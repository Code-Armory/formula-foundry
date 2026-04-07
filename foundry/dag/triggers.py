"""
Panic Fingerprint Trigger — OFI + Spread Detection

Trigger fires when BOTH conditions are simultaneously true:
  1. OFI Z-score > OFI_SIGMA_THRESHOLD (default: 3.0)
  2. Spread percentile > SPREAD_PERCENTILE_THRESHOLD (default: 95.0)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import numpy as np

logger = logging.getLogger(__name__)

OFI_SIGMA_THRESHOLD: float = 3.0
SPREAD_PERCENTILE_THRESHOLD: float = 95.0
LOOKBACK_WINDOW_SECONDS: int = 30
BASELINE_WINDOW_DAYS: int = 20


@dataclass
class MBOEvent:
    timestamp_ns: int
    instrument: str
    action: str
    side: str
    price: float
    size: int
    order_id: int
    is_aggressive: bool


@dataclass
class OrderBookSnapshot:
    timestamp_ns: int
    instrument: str
    best_bid: float
    best_ask: float
    best_bid_size: int
    best_ask_size: int

    @property
    def mid_price(self) -> float:
        return (self.best_bid + self.best_ask) / 2.0

    @property
    def spread(self) -> float:
        return self.best_ask - self.best_bid

    @property
    def spread_bps(self) -> float:
        return (self.spread / self.mid_price) * 10_000.0 if self.mid_price > 0 else 0.0


@dataclass
class MarketDataSlice:
    instrument: str
    window_start_ns: int
    window_end_ns: int
    events: List[MBOEvent] = field(default_factory=list)
    snapshots: List[OrderBookSnapshot] = field(default_factory=list)
    baseline_ofi_mean: float = 0.0
    baseline_ofi_std: float = 1.0
    spread_percentiles: List[float] = field(default_factory=list)


@dataclass
class PanicTrigger:
    triggered: bool
    instrument: str
    timestamp: datetime
    ofi_zscore: float = 0.0
    ofi_raw: float = 0.0
    ofi_acceleration: float = 0.0
    spread_percentile: float = 0.0
    current_spread_bps: float = 0.0
    baseline_sell_rate_hz: float = 0.0
    current_sell_rate_hz: float = 0.0
    mbo_event_count: int = 0
    aggressive_sell_count: int = 0
    aggressive_buy_count: int = 0
    event_type: str = "panic_liquidity_withdrawal"
    trigger_conditions: Dict[str, Any] = field(default_factory=dict)

    def to_agent_input(self) -> Dict[str, Any]:
        return {
            "event_type": self.event_type,
            "instrument": self.instrument,
            "timestamp": self.timestamp.isoformat(),
            "ofi_zscore": self.ofi_zscore,
            "spread_percentile": self.spread_percentile,
            "ofi_acceleration": round(self.ofi_acceleration, 6),
            "baseline_sell_rate_hz": round(self.baseline_sell_rate_hz, 4),
            "current_sell_rate_hz": round(self.current_sell_rate_hz, 4),
            "mbo_event_count": self.mbo_event_count,
            "trigger_conditions": self.trigger_conditions,
        }


def detect_panic_fingerprint(
    data_slice: MarketDataSlice,
    ofi_sigma_threshold: float = OFI_SIGMA_THRESHOLD,
    spread_pctile_threshold: float = SPREAD_PERCENTILE_THRESHOLD,
) -> PanicTrigger:
    timestamp = datetime.fromtimestamp(data_slice.window_end_ns / 1e9, tz=timezone.utc)

    if not data_slice.events:
        return PanicTrigger(triggered=False, instrument=data_slice.instrument, timestamp=timestamp)

    ofi_raw = _compute_ofi(data_slice.events)
    ofi_zscore = _compute_ofi_zscore(ofi_raw, data_slice.baseline_ofi_mean, data_slice.baseline_ofi_std)
    ofi_acceleration = _compute_ofi_acceleration(data_slice.events)
    current_spread_bps = _get_current_spread_bps(data_slice.snapshots)
    spread_percentile = _compute_spread_percentile(current_spread_bps, data_slice.spread_percentiles)

    window_seconds = max((data_slice.window_end_ns - data_slice.window_start_ns) / 1e9, 1.0)
    aggressive_sells = [e for e in data_slice.events if e.is_aggressive and e.side == "A"]
    aggressive_buys = [e for e in data_slice.events if e.is_aggressive and e.side == "B"]
    current_sell_rate = len(aggressive_sells) / window_seconds
    baseline_sell_rate = data_slice.baseline_ofi_mean / window_seconds if data_slice.baseline_ofi_mean > 0 else 0.0

    # Panic is a SELLING event: OFI z-score must be negative (net selling pressure).
    # A positive z-score (net buying) is not a liquidity withdrawal panic —
    # it is a short-squeeze or aggressive accumulation. Different phenomenon,
    # different agent. Agent 089 models selling cascades only.
    ofi_triggered = ofi_zscore <= -ofi_sigma_threshold
    spread_triggered = spread_percentile >= spread_pctile_threshold
    triggered = ofi_triggered and spread_triggered

    return PanicTrigger(
        triggered=triggered,
        instrument=data_slice.instrument,
        timestamp=timestamp,
        ofi_zscore=round(ofi_zscore, 4),
        ofi_raw=round(ofi_raw, 2),
        ofi_acceleration=round(ofi_acceleration, 6),
        spread_percentile=round(spread_percentile, 2),
        current_spread_bps=round(current_spread_bps, 4),
        baseline_sell_rate_hz=round(baseline_sell_rate, 4),
        current_sell_rate_hz=round(current_sell_rate, 4),
        mbo_event_count=len(data_slice.events),
        aggressive_sell_count=len(aggressive_sells),
        aggressive_buy_count=len(aggressive_buys),
        trigger_conditions={
            "ofi_zscore": ofi_zscore,
            "ofi_sigma_threshold": ofi_sigma_threshold,
            "spread_percentile": spread_percentile,
            "spread_pctile_threshold": spread_pctile_threshold,
            "ofi_triggered": ofi_triggered,
            "spread_triggered": spread_triggered,
            "description": (
                f"Panic fingerprint: OFI ≤ -{ofi_sigma_threshold}σ (selling) "
                f"+ spread ≥ {spread_pctile_threshold}th pct"
            ),
        },
    )


def _compute_ofi(events: List[MBOEvent]) -> float:
    buy_volume = sum(e.size for e in events if e.is_aggressive and e.side == "B")
    sell_volume = sum(e.size for e in events if e.is_aggressive and e.side == "A")
    return float(buy_volume - sell_volume)


def _compute_ofi_zscore(ofi: float, baseline_mean: float, baseline_std: float) -> float:
    """
    Return the SIGNED OFI z-score.

    Sign convention (critical for directional correctness):
      Negative z-score → net selling (aggressive sells > buys)
      Positive z-score → net buying  (aggressive buys > sells)

    A liquidity withdrawal panic is a SELLING event: OFI < 0, z-score < 0.
    Using abs() here would make a selling cascade and a buying cascade
    indistinguishable, destroying the directional semantics that
    Agent 089 and Agent 060 rely on.
    """
    if baseline_std <= 0:
        return 0.0
    return (ofi - baseline_mean) / baseline_std


def _compute_ofi_acceleration(events: List[MBOEvent]) -> float:
    if len(events) < 2:
        return 0.0
    midpoint_ns = (events[0].timestamp_ns + events[-1].timestamp_ns) / 2
    first_half = [e for e in events if e.timestamp_ns <= midpoint_ns]
    second_half = [e for e in events if e.timestamp_ns > midpoint_ns]
    if not first_half or not second_half:
        return 0.0
    ofi_first = _compute_ofi(first_half)
    ofi_second = _compute_ofi(second_half)
    time_span = max((events[-1].timestamp_ns - events[0].timestamp_ns) / 1e9, 0.001)
    return (ofi_second - ofi_first) / (time_span / 2)


def _get_current_spread_bps(snapshots: List[OrderBookSnapshot]) -> float:
    if not snapshots:
        return 0.0
    return max(snapshots, key=lambda s: s.timestamp_ns).spread_bps


def _compute_spread_percentile(current_spread_bps: float, historical_spreads: List[float]) -> float:
    if not historical_spreads or current_spread_bps <= 0:
        return 0.0
    arr = np.array(historical_spreads)
    return float(np.mean(arr <= current_spread_bps) * 100.0)


def build_test_panic_slice(instrument: str = "ES") -> MarketDataSlice:
    import time
    now_ns = int(time.time() * 1e9)
    events: List[MBOEvent] = []
    for i in range(80):
        events.append(MBOEvent(
            timestamp_ns=now_ns + (i * 100_000_000),
            instrument=instrument, action="T", side="A",
            price=4500.0 - (i * 0.25), size=10,
            order_id=1000 + i, is_aggressive=True,
        ))
    for i in range(10):
        events.append(MBOEvent(
            timestamp_ns=now_ns + (i * 500_000_000),
            instrument=instrument, action="T", side="B",
            price=4502.0, size=2, order_id=2000 + i, is_aggressive=True,
        ))
    snapshots = [
        OrderBookSnapshot(
            timestamp_ns=now_ns + (i * 1_000_000_000),
            instrument=instrument,
            best_bid=4498.0 - (i * 0.5),
            best_ask=4502.0 + (i * 0.5),
            best_bid_size=5, best_ask_size=5,
        )
        for i in range(10)
    ]
    return MarketDataSlice(
        instrument=instrument,
        window_start_ns=now_ns,
        window_end_ns=now_ns + 10_000_000_000,
        events=events,
        snapshots=snapshots,
        baseline_ofi_mean=0.0,
        baseline_ofi_std=100.0,
        spread_percentiles=[0.5, 0.8, 1.0, 1.2, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0] * 100,
    )


# ===========================================================================
# Cross-Asset Trigger — ES/ZN Decoupling Detection
# ===========================================================================
#
# Fires on two empirically distinct regime signatures:
#
#   Regime A — FLIGHT_TO_QUALITY:
#     ρ(ES_OFI_z, ZN_OFI_z) < -0.65 (capital exiting equities, entering bonds)
#     AND |ES_OFI_z| > 2.5  (equity side is actively panicking)
#     AND ZN_OFI_z > 2.0   (treasury side is aggressively bid)
#
#   Regime B — COORDINATED_LIQUIDATION:
#     ρ(ES_OFI_z, ZN_OFI_z) > +0.60 (both selling simultaneously)
#     AND ES_OFI_z < -2.0  (aggressive equity selling)
#     AND ZN_OFI_z < -2.0  (aggressive treasury selling — no safe haven)
#
# The behavioral distinction matters for Agent 060's formula:
#   Regime A: off-diagonal A_ZN→ES < 0 (Treasury buying dampens equity panic)
#   Regime B: off-diagonal A_ZN→ES > 0 (Treasury selling amplifies equity panic)
# ===========================================================================

# Regime thresholds — all directionally explicit (signed OFI z-scores)
_FTQ_CORR_THRESHOLD: float = -0.65    # Regime A: correlation below this
_FTQ_ES_Z_MIN: float = 2.5            # Regime A: ES z-score must be < -2.5 (selling)
_FTQ_ZN_Z_MIN: float = 2.0            # Regime A: ZN z-score must be > +2.0 (buying)
_CL_CORR_THRESHOLD: float = 0.60      # Regime B: correlation above this
_CL_ES_Z_MAX: float = -2.0            # Regime B: ES z-score must be < -2.0 (selling)
_CL_ZN_Z_MAX: float = -2.0            # Regime B: ZN z-score must be < -2.0 (selling)
_MIN_WINDOWS_REQUIRED: int = 10       # Minimum samples before evaluating


@dataclass
class CrossAssetWindowSample:
    """One paired observation of OFI z-scores across two instruments."""
    timestamp_ns: int
    ofi_z_a: float   # instrument_a (e.g. ES) z-score at this window
    ofi_z_b: float   # instrument_b (e.g. ZN) z-score at this window


@dataclass
class CrossAssetTrigger:
    """
    Output of detect_cross_asset_decoupling().
    Carries all context needed by Agent 060's build_initial_message().
    """
    triggered: bool
    regime: str          # "flight_to_quality" | "coordinated_liquidation" | ""
    instrument_a: str    # e.g. "ES.c.0"
    instrument_b: str    # e.g. "ZN.c.0"
    timestamp: datetime

    # Correlation metrics
    correlation: float = 0.0
    n_windows: int = 0

    # Individual z-scores at trigger time
    ofi_z_a: float = 0.0
    ofi_z_b: float = 0.0

    # Trigger conditions for Blackboard logging
    trigger_conditions: Dict[str, Any] = field(default_factory=dict)

    def to_agent_input(self) -> Dict[str, Any]:
        """Convert to the dict format expected by Agent 060's build_initial_message."""
        return {
            "event_type": f"cross_asset_{self.regime}",
            "instrument_a": self.instrument_a,
            "instrument_b": self.instrument_b,
            "timestamp": self.timestamp.isoformat(),
            "regime": self.regime,
            "correlation": round(self.correlation, 4),
            "n_windows": self.n_windows,
            f"ofi_zscore_{self.instrument_a.split('.')[0].lower()}": round(self.ofi_z_a, 4),
            f"ofi_zscore_{self.instrument_b.split('.')[0].lower()}": round(self.ofi_z_b, 4),
            "trigger_conditions": self.trigger_conditions,
        }


def detect_cross_asset_decoupling(
    samples: List[CrossAssetWindowSample],
    instrument_a: str,
    instrument_b: str,
) -> CrossAssetTrigger:
    """
    Evaluate cross-asset OFI correlation for regime detection.

    Takes the rolling list of (ofi_z_a, ofi_z_b) window samples and computes
    Pearson correlation. Returns a CrossAssetTrigger with triggered=True if
    either Regime A or Regime B threshold conditions are met.

    Requires at least _MIN_WINDOWS_REQUIRED samples. With fewer samples,
    the correlation estimate is not statistically meaningful — returns
    triggered=False regardless of the computed value.

    Args:
        samples: List of CrossAssetWindowSample, ordered oldest→newest.
                 Typically the last 60 samples (30 minutes at 30s intervals).
        instrument_a: First instrument symbol (e.g. "ES.c.0")
        instrument_b: Second instrument symbol (e.g. "ZN.c.0")

    Returns:
        CrossAssetTrigger with triggered=True if regime threshold met.
    """
    timestamp = datetime.now(timezone.utc)
    n = len(samples)

    no_trigger = CrossAssetTrigger(
        triggered=False, regime="", instrument_a=instrument_a,
        instrument_b=instrument_b, timestamp=timestamp, n_windows=n,
    )

    if n < _MIN_WINDOWS_REQUIRED:
        return no_trigger

    zs_a = np.array([s.ofi_z_a for s in samples])
    zs_b = np.array([s.ofi_z_b for s in samples])

    # Pearson correlation (std=0 guard — returns 0 if one series is constant)
    std_a, std_b = np.std(zs_a), np.std(zs_b)
    if std_a < 1e-8 or std_b < 1e-8:
        return no_trigger

    correlation = float(np.corrcoef(zs_a, zs_b)[0, 1])
    if not np.isfinite(correlation):
        return no_trigger

    # Use most recent sample's z-scores for per-instrument checks
    latest = samples[-1]
    ofi_z_a = latest.ofi_z_a
    ofi_z_b = latest.ofi_z_b

    # Regime A — Flight-to-Quality Panic
    # Directionality is mandatory — using abs() would fire on a buying surge,
    # which would invert the A_ZN→ES cross-excitation sign in Agent 060's matrix.
    # ES must be SELLING (z_a < 0) and ZN must be BUYING (z_b > 0).
    regime_a = (
        correlation < _FTQ_CORR_THRESHOLD
        and ofi_z_a < -_FTQ_ES_Z_MIN      # equity selling (MUST be negative)
        and ofi_z_b > _FTQ_ZN_Z_MIN       # treasury buying (MUST be positive)
    )

    # Regime B — Coordinated Liquidation
    # Both instruments selling simultaneously — signs are already correct here
    # (both negative), and the original conditions were directionally sound.
    regime_b = (
        correlation > _CL_CORR_THRESHOLD
        and ofi_z_a < _CL_ES_Z_MAX        # equity selling
        and ofi_z_b < _CL_ZN_Z_MAX        # treasury selling (no safe haven)
    )

    if not regime_a and not regime_b:
        return CrossAssetTrigger(
            triggered=False, regime="", instrument_a=instrument_a,
            instrument_b=instrument_b, timestamp=timestamp,
            correlation=round(correlation, 4), n_windows=n,
            ofi_z_a=round(ofi_z_a, 4), ofi_z_b=round(ofi_z_b, 4),
        )

    regime = "flight_to_quality" if regime_a else "coordinated_liquidation"
    logger.info(
        "CROSS-ASSET TRIGGER [%s]: %s↔%s | ρ=%.3f | z_a=%.2f | z_b=%.2f | n=%d",
        regime.upper(), instrument_a, instrument_b,
        correlation, ofi_z_a, ofi_z_b, n,
    )

    return CrossAssetTrigger(
        triggered=True,
        regime=regime,
        instrument_a=instrument_a,
        instrument_b=instrument_b,
        timestamp=timestamp,
        correlation=round(correlation, 4),
        n_windows=n,
        ofi_z_a=round(ofi_z_a, 4),
        ofi_z_b=round(ofi_z_b, 4),
        trigger_conditions={
            "regime": regime,
            "correlation": correlation,
            "correlation_threshold": _FTQ_CORR_THRESHOLD if regime_a else _CL_CORR_THRESHOLD,
            "ofi_z_a": ofi_z_a,
            "ofi_z_b": ofi_z_b,
            "n_windows": n,
            "min_windows_required": _MIN_WINDOWS_REQUIRED,
        },
    )


# ===========================================================================
# Entropy Collapse Trigger — MBP-10 Order Book Fragility Detection
# ===========================================================================
#
# Fires when BOTH conditions hold simultaneously:
#
#   Condition 1 — Entropy collapse:
#     H(bid) < 5th percentile of 20-day entropy distribution
#     (bid-side volume distribution has become dangerously concentrated)
#
#   Condition 2 — Volume reduction (anti-redistribution guard):
#     total_bid_volume < 30th percentile of 20-day volume distribution
#     (prevents false positives from volume-neutral level migrations)
#
# Why both conditions:
#   A market maker quote refresh can redistribute volume from deep levels
#   to the best bid, causing entropy to collapse WITHOUT total volume dropping.
#   This is NOT a fragility signal — it is normal quoting behavior.
#   Only when entropy collapses AND total volume also drops are market makers
#   actually withdrawing from the book.
#
# MBP-10 schema:
#   Databento MBP-10 delivers the top 10 bid levels and top 10 ask levels
#   on every book change. This trigger operates only on the BID side.
#   The ask side is symmetric and not used here — Agent 051 models selling
#   pressure liquidity fragility specifically.
#
# Synthesis target:
#   Hawkes intensity approaching criticality (Agent 089: α/β → 1)
#   AND entropy approaching zero (Agent 051: H → 0)
#   describe the SAME psychological event — market fragility — through
#   different mathematical lenses. Agent 105 will find a Tier 3 behavioral
#   isomorphism between these two formulas.
# ===========================================================================

import math as _math

_ENTROPY_PCTILE_THRESHOLD: float = 5.0    # entropy below this percentile → fragile
_VOLUME_PCTILE_THRESHOLD:  float = 30.0   # total bid volume below this pct → depleted
_ENTROPY_MIN_LEVELS: int = 2              # minimum non-zero levels for meaningful entropy


@dataclass
class MBP10Level:
    """One price level from a Databento MBP-10 snapshot."""
    price: float
    size: int   # aggregate size at this level


@dataclass
class MBP10Snapshot:
    """
    Top-10-level order book snapshot from Databento MBP-10 schema.

    bids: top 10 bid levels, best (highest price) first — bids[0] = best bid
    asks: top 10 ask levels, best (lowest price) first — asks[0] = best ask

    All sizes are in contract units matching the instrument's lot size.
    """
    timestamp_ns: int
    instrument: str
    bids: List[MBP10Level]   # len ≤ 10, best first
    asks: List[MBP10Level]   # len ≤ 10, best first


@dataclass
class EntropyBaseline:
    """
    20-day baseline statistics for bid entropy and total bid volume.
    Loaded from Postgres at startup. Never mutated in the hot loop.

    Both lists are sorted ascending for numpy searchsorted percentile lookup.
    Samples are taken at every MBP-10 update during the bootstrap window.
    Typical size: 500K–2M samples per instrument per 20 trading days.
    """
    entropy_samples: List[float]    # sorted bid entropy values (bits)
    volume_samples:  List[float]    # sorted total bid volume values (contracts)


@dataclass
class EntropyTrigger:
    """
    Output of detect_entropy_collapse().
    Carries the book state at trigger time for Agent 051's initial message.
    """
    triggered: bool
    instrument: str
    timestamp: datetime

    # Entropy metrics
    bid_entropy: float = 0.0            # Shannon entropy in bits, range [0, log2(10)]
    entropy_percentile: float = 50.0    # what percentile this entropy sits at historically
                                        # LOW percentile = LOW entropy = HIGH fragility

    # Volume metrics
    total_bid_volume: int = 0
    volume_percentile: float = 50.0     # what percentile this volume sits at historically

    # Book structure at trigger time
    best_bid_fraction: float = 0.0      # vol@best_bid / total_bid_volume
    n_nonzero_levels: int = 0           # number of bid levels with any volume

    # Level-by-level snapshot for Agent 051
    bid_levels: List[MBP10Level] = field(default_factory=list)

    trigger_conditions: Dict[str, Any] = field(default_factory=dict)

    def to_agent_input(self) -> Dict[str, Any]:
        """Convert to the dict format expected by Agent 051's build_initial_message."""
        return {
            "event_type": "entropy_collapse",
            "instrument": self.instrument,
            "timestamp": self.timestamp.isoformat(),
            "bid_entropy_bits": round(self.bid_entropy, 4),
            "entropy_percentile": round(self.entropy_percentile, 2),
            "total_bid_volume": self.total_bid_volume,
            "volume_percentile": round(self.volume_percentile, 2),
            "best_bid_fraction": round(self.best_bid_fraction, 4),
            "n_nonzero_levels": self.n_nonzero_levels,
            "bid_levels": [
                {"price": lvl.price, "size": lvl.size}
                for lvl in self.bid_levels
            ],
            "trigger_conditions": self.trigger_conditions,
        }


def _compute_bid_entropy(bids: List[MBP10Level]) -> tuple:
    """
    Compute Shannon entropy of the bid-side volume distribution.

    H(bid) = -Σᵢ p(level_i) · log₂(p(level_i))
    where p(level_i) = size_at_level_i / total_bid_volume

    Returns:
        (entropy_bits, total_volume, best_bid_fraction, n_nonzero_levels)

    Edge cases:
        total_volume = 0 → entropy = 0 (undefined, treat as maximally fragile)
        only one nonzero level → entropy = 0 (fully concentrated)
        all levels equal → entropy = log₂(n_nonzero) (maximum)
    """
    sizes = np.array([b.size for b in bids], dtype=float)
    total = float(sizes.sum())

    if total <= 0:
        return 0.0, 0, 0.0, 0

    n_nonzero = int(np.count_nonzero(sizes))
    best_bid_fraction = float(sizes[0] / total) if len(sizes) > 0 else 0.0

    # Shannon entropy: only sum over nonzero levels (0·log(0) = 0 by convention)
    p = sizes[sizes > 0] / total
    entropy = float(-np.sum(p * np.log2(p)))

    return entropy, int(total), best_bid_fraction, n_nonzero


def _compute_percentile(value: float, sorted_samples: List[float]) -> float:
    """
    Return the percentile rank of `value` within `sorted_samples`.
    0.0 = below all samples (minimum), 100.0 = above all samples (maximum).
    """
    if not sorted_samples:
        return 50.0
    n = len(sorted_samples)
    pos = int(np.searchsorted(sorted_samples, value, side="right"))
    return float(pos / n * 100.0)


def detect_entropy_collapse(
    snapshot: MBP10Snapshot,
    baseline: EntropyBaseline,
    entropy_pctile_threshold: float = _ENTROPY_PCTILE_THRESHOLD,
    volume_pctile_threshold: float = _VOLUME_PCTILE_THRESHOLD,
) -> EntropyTrigger:
    """
    Evaluate bid-side Shannon entropy collapse for one MBP-10 snapshot.

    Requires BOTH conditions:
      1. Entropy percentile < entropy_pctile_threshold  (entropy historically low)
      2. Volume percentile < volume_pctile_threshold    (volume also reduced)

    The dual condition prevents false positives from quote refreshes that
    redistribute volume without removing it.

    Args:
        snapshot: MBP10Snapshot from Databento MBP-10 feed
        baseline: EntropyBaseline loaded from Postgres (20-day history)
        entropy_pctile_threshold: fire if entropy is below this percentile
        volume_pctile_threshold:  fire if volume is below this percentile

    Returns:
        EntropyTrigger with triggered=True if both conditions met.
    """
    timestamp = datetime.now(timezone.utc)

    no_trigger = EntropyTrigger(
        triggered=False, instrument=snapshot.instrument, timestamp=timestamp,
    )

    if not snapshot.bids:
        return no_trigger

    entropy, total_vol, best_frac, n_nonzero = _compute_bid_entropy(snapshot.bids)

    # Require at least 2 non-zero levels for entropy to be meaningful
    # (1 level → entropy always 0, not informative)
    if n_nonzero < _ENTROPY_MIN_LEVELS and total_vol > 0:
        # Single-level book: extremely fragile, treat as triggered
        # only if volume is also collapsing
        entropy = 0.0

    entropy_pctile = _compute_percentile(entropy, baseline.entropy_samples)
    volume_pctile  = _compute_percentile(float(total_vol), baseline.volume_samples)

    entropy_triggered = entropy_pctile < entropy_pctile_threshold
    volume_triggered  = volume_pctile  < volume_pctile_threshold
    triggered = entropy_triggered and volume_triggered

    if triggered:
        logger.warning(
            "ENTROPY COLLAPSE: %s | H=%.3f bits | entropy_pct=%.1f | "
            "vol=%d | vol_pct=%.1f | best_frac=%.3f | levels=%d",
            snapshot.instrument, entropy, entropy_pctile,
            total_vol, volume_pctile, best_frac, n_nonzero,
        )

    return EntropyTrigger(
        triggered=triggered,
        instrument=snapshot.instrument,
        timestamp=timestamp,
        bid_entropy=round(entropy, 4),
        entropy_percentile=round(entropy_pctile, 2),
        total_bid_volume=total_vol,
        volume_percentile=round(volume_pctile, 2),
        best_bid_fraction=round(best_frac, 4),
        n_nonzero_levels=n_nonzero,
        bid_levels=list(snapshot.bids),
        trigger_conditions={
            "entropy_bits": entropy,
            "entropy_percentile": entropy_pctile,
            "entropy_threshold_percentile": entropy_pctile_threshold,
            "total_bid_volume": total_vol,
            "volume_percentile": volume_pctile,
            "volume_threshold_percentile": volume_pctile_threshold,
            "entropy_triggered": entropy_triggered,
            "volume_triggered": volume_triggered,
            "n_nonzero_levels": n_nonzero,
            "best_bid_fraction": best_frac,
        },
    )
