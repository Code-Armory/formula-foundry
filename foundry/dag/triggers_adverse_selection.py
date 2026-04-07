# foundry/dag/triggers_adverse_selection.py
#
# Additive module — adverse selection trigger additions for triggers.py.
#
# INTEGRATION INSTRUCTIONS:
#   Copy the contents of this file into foundry/dag/triggers.py, appended
#   after the existing EntropyTrigger / detect_entropy_collapse section.
#   Then add the following imports to databento_ingest.py:
#
#     from foundry.dag.triggers import (
#         AdverseSelectionTrigger,
#         LambdaBaseline,
#         detect_adverse_selection,
#     )
#
# This file is kept separate so the diff against triggers.py is reviewable
# before merging. Do not import this module directly — merge it into triggers.py.
#
# New additions:
#   LambdaBaseline         — rolling 20-day bar-level lambda history per instrument
#   AdverseSelectionTrigger — trigger dataclass with .to_agent_input()
#   detect_adverse_selection — OLS regression, percentile check, R² gate
#   _compute_lambda_ols    — internal: OLS slope + R² from bar series
#   _compute_lambda_percentile — internal: percentile rank in history
#
# Constants (add alongside existing OFI_SIGMA_THRESHOLD etc.):
#   LAMBDA_PERCENTILE_THRESHOLD = 0.95   # top 5% of historical lambda
#   R2_TRIGGER_THRESHOLD        = 0.40   # minimum R² to fire trigger
#   R2_NOISE_FLOOR              = 0.20   # hard suppress below this
#   LAMBDA_MIN_BARS             = 10     # minimum bars for meaningful OLS
#   LAMBDA_HISTORY_DAYS         = 20     # rolling baseline window

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

# ---------------------------------------------------------------------------
# Constants — paste into triggers.py alongside existing threshold constants
# ---------------------------------------------------------------------------

LAMBDA_PERCENTILE_THRESHOLD: float = 0.95   # top 5% of historical lambda → trigger
R2_TRIGGER_THRESHOLD:        float = 0.40   # minimum R² to declare informed flow
R2_NOISE_FLOOR:              float = 0.20   # hard suppress: below this = noise, not strategy
LAMBDA_MIN_BARS:             int   = 10     # minimum 30-min bars required for OLS
LAMBDA_HISTORY_DAYS:         int   = 20     # rolling baseline window in trading days


# ---------------------------------------------------------------------------
# LambdaBaseline
# ---------------------------------------------------------------------------

@dataclass
class LambdaBaseline:
    """
    Rolling history of bar-level Kyle's Lambda estimates for one instrument.

    Updated at each 30-minute bar close by the ingest pipeline.
    Stored in Postgres (same table as OFI baselines) and loaded on startup.

    lambda_history: deque of (lambda_coefficient, r_squared) tuples,
        ordered oldest-first. Maximum length = LAMBDA_HISTORY_DAYS * bars_per_day.
        A full trading day of 30-minute bars: ~13 bars (6.5 hour session).
        20 days × 13 bars = 260 bar history.

    Baseline is invalid until at least LAMBDA_MIN_BARS values are present.
    """
    instrument: str
    lambda_history: deque = field(
        default_factory=lambda: deque(maxlen=LAMBDA_HISTORY_DAYS * 14)
    )

    @property
    def is_valid(self) -> bool:
        """True when enough history exists for a meaningful percentile rank."""
        return len(self.lambda_history) >= LAMBDA_MIN_BARS

    def add_bar(self, lambda_coeff: float, r_squared: float) -> None:
        """Record one completed 30-minute bar's lambda estimate."""
        self.lambda_history.append((lambda_coeff, r_squared))

    def lambda_values(self) -> List[float]:
        """All historical lambda coefficients (excludes R² values)."""
        return [lc for lc, _ in self.lambda_history]


# ---------------------------------------------------------------------------
# AdverseSelectionTrigger
# ---------------------------------------------------------------------------

@dataclass
class AdverseSelectionTrigger:
    """
    Fired when Kyle's Lambda exceeds its 95th percentile with R² ≥ 0.40.

    Carries the full bar series so Agent 050 can propose nonlinear
    extensions (λ²·vol² terms) or decay models (λ₀·exp(-δt)).
    Passing only summary statistics would make the nonlinear formulas
    impossible to ground in the actual observed relationship.
    """
    triggered:            bool
    instrument:           str
    window_start:         datetime
    window_end:           datetime
    lambda_coefficient:   float = 0.0     # OLS slope (Δp per unit signed_vol)
    lambda_percentile:    float = 0.0     # rank in 20-day history [0, 1]
    regression_r2:        float = 0.0     # goodness of fit [0, 1]
    signed_volume_series: List[float] = field(default_factory=list)
    price_change_series:  List[float] = field(default_factory=list)
    suppressed:           bool = False    # True if R² < R2_NOISE_FLOOR
    trigger_conditions:   Dict[str, Any] = field(default_factory=dict)
    event_type:           str = "adverse_selection_regime"

    def to_agent_input(self) -> Dict[str, Any]:
        """Format trigger data for Agent 050's build_initial_message()."""
        return {
            "event_type":            self.event_type,
            "instrument":            self.instrument,
            "window_start":          self.window_start.isoformat(),
            "window_end":            self.window_end.isoformat(),
            "lambda_coefficient":    round(self.lambda_coefficient, 8),
            "lambda_percentile":     round(self.lambda_percentile, 4),
            "regression_r2":         round(self.regression_r2, 4),
            "signed_volume_series":  [round(v, 2) for v in self.signed_volume_series],
            "price_change_series":   [round(v, 6) for v in self.price_change_series],
            "trigger_conditions":    self.trigger_conditions,
        }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _compute_lambda_ols(
    signed_volume_series: List[float],
    price_change_series: List[float],
) -> Optional[Tuple[float, float]]:
    """
    OLS regression: Δp = λ · signed_vol + ε

    Returns (lambda_coefficient, r_squared) or None if degenerate.

    Uses numpy for numerical stability. Both series must be equal length
    and contain at least LAMBDA_MIN_BARS observations.

    Sign convention: signed_vol is negative during selling panics (OFI
    convention — buy_vol - sell_vol, selling = negative). A positive lambda
    means price falls when selling pressure is high (correct interpretation).
    """
    if len(signed_volume_series) != len(price_change_series):
        return None
    if len(signed_volume_series) < LAMBDA_MIN_BARS:
        return None

    sv = np.array(signed_volume_series, dtype=float)
    dp = np.array(price_change_series, dtype=float)

    sv_mean = sv.mean()
    dp_mean = dp.mean()

    sv_centered = sv - sv_mean
    dp_centered = dp - dp_mean

    var_sv = (sv_centered ** 2).mean()
    if var_sv < 1e-10:
        # Degenerate: no variation in signed volume (flat market)
        return None

    cov = (sv_centered * dp_centered).mean()
    lambda_coeff = cov / var_sv

    # R²
    dp_pred = dp_mean + lambda_coeff * sv_centered
    ss_res = ((dp - dp_pred) ** 2).sum()
    ss_tot = ((dp_centered) ** 2).sum()
    r_squared = 1.0 - (ss_res / ss_tot) if ss_tot > 1e-10 else 0.0
    r_squared = float(max(0.0, min(1.0, r_squared)))

    return float(lambda_coeff), r_squared


def _compute_lambda_percentile(
    lambda_coeff: float,
    lambda_history: List[float],
) -> float:
    """
    Compute the percentile rank of lambda_coeff within lambda_history.

    Returns a value in [0, 1]. 0.95 means lambda_coeff exceeds 95% of
    historical observations.

    Uses the same approach as spread_percentile in the existing panic trigger:
    fraction of history values ≤ current value.
    """
    if not lambda_history:
        return 0.0
    arr = np.array(lambda_history, dtype=float)
    return float(np.mean(arr <= lambda_coeff))


# ---------------------------------------------------------------------------
# Public detection function
# ---------------------------------------------------------------------------

def detect_adverse_selection(
    instrument: str,
    window_start: datetime,
    window_end: datetime,
    signed_volume_series: List[float],
    price_change_series: List[float],
    baseline: LambdaBaseline,
    lambda_percentile_threshold: float = LAMBDA_PERCENTILE_THRESHOLD,
    r2_trigger_threshold: float = R2_TRIGGER_THRESHOLD,
    r2_noise_floor: float = R2_NOISE_FLOOR,
) -> AdverseSelectionTrigger:
    """
    Detect an adverse selection regime shift via Kyle's Lambda.

    Trigger fires when ALL of the following hold:
      1. OLS lambda coefficient is in the top 5% of its 20-day history
      2. Regression R² ≥ 0.40 (informed flow, not noise)
      3. Baseline has sufficient history (≥ LAMBDA_MIN_BARS observations)

    Hard suppress when:
      - R² < 0.20 (noise floor) — logs SUPPRESSED, returns triggered=False
      - Insufficient history — returns triggered=False silently

    Returns AdverseSelectionTrigger with triggered=False for any non-firing
    case. The suppressed flag distinguishes noise-floor suppression from
    simple below-threshold cases.
    """
    import logging
    log = logging.getLogger(__name__)

    null_trigger = AdverseSelectionTrigger(
        triggered=False,
        instrument=instrument,
        window_start=window_start,
        window_end=window_end,
        signed_volume_series=signed_volume_series,
        price_change_series=price_change_series,
    )

    # Baseline validity gate
    if not baseline.is_valid:
        return null_trigger

    # OLS computation
    ols_result = _compute_lambda_ols(signed_volume_series, price_change_series)
    if ols_result is None:
        return null_trigger

    lambda_coeff, r_squared = ols_result

    # Hard noise floor suppress — R² too low to interpret lambda
    if r_squared < r2_noise_floor:
        log.debug(
            "[AdverseSelection] SUPPRESSED %s: R²=%.3f < %.2f (noise floor). "
            "Lambda=%.6f meaningless.",
            instrument, r_squared, r2_noise_floor, lambda_coeff,
        )
        null_trigger.suppressed = True
        null_trigger.lambda_coefficient = lambda_coeff
        null_trigger.regression_r2 = r_squared
        return null_trigger

    # Percentile rank in 20-day history
    lambda_history = baseline.lambda_values()
    lambda_percentile = _compute_lambda_percentile(lambda_coeff, lambda_history)

    # Update baseline with this bar's estimate (regardless of trigger outcome)
    baseline.add_bar(lambda_coeff, r_squared)

    # Trigger conditions
    percentile_triggered = lambda_percentile >= lambda_percentile_threshold
    r2_triggered = r_squared >= r2_trigger_threshold
    triggered = percentile_triggered and r2_triggered

    trigger_conditions = {
        "lambda_coefficient":        lambda_coeff,
        "lambda_percentile":         lambda_percentile,
        "lambda_percentile_threshold": lambda_percentile_threshold,
        "regression_r2":             r_squared,
        "r2_trigger_threshold":      r2_trigger_threshold,
        "r2_noise_floor":            r2_noise_floor,
        "percentile_triggered":      percentile_triggered,
        "r2_triggered":              r2_triggered,
        "n_bars":                    len(signed_volume_series),
        "n_history":                 len(lambda_history),
        "description": (
            f"Adverse selection regime: λ={lambda_coeff:.6f} "
            f"(pctile={lambda_percentile:.3f}), R²={r_squared:.3f}"
        ),
    }

    if triggered:
        log.warning(
            "[AdverseSelection] REGIME SHIFT: %s | λ=%.6f (pctile=%.3f) | R²=%.3f",
            instrument, lambda_coeff, lambda_percentile, r_squared,
        )

    return AdverseSelectionTrigger(
        triggered=triggered,
        instrument=instrument,
        window_start=window_start,
        window_end=window_end,
        lambda_coefficient=lambda_coeff,
        lambda_percentile=lambda_percentile,
        regression_r2=r_squared,
        signed_volume_series=signed_volume_series,
        price_change_series=price_change_series,
        suppressed=False,
        trigger_conditions=trigger_conditions,
    )
