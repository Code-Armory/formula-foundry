"""
Regression tests for COVID crucible — Black Thursday OFI replay.

These tests validate durable properties of the OFI signal against
BTCUSDT 2020-03-12 aggTrades. They intentionally avoid brittle exact
counts; instead they assert structural facts that should hold unless
the OFI formula, side convention, or ingest adapter changes.

Requires: data/binance/BTCUSDT-aggTrades-2020-03-12.zip
Skip gracefully if the data file is absent (CI without large test data).
"""

from __future__ import annotations

from pathlib import Path

import pytest

DATA_PATH = Path(__file__).resolve().parent.parent / "data" / "binance" / "BTCUSDT-aggTrades-2020-03-12.zip"

skip_no_data = pytest.mark.skipif(
    not DATA_PATH.exists(),
    reason=f"Black Thursday data not found: {DATA_PATH}",
)


@pytest.fixture(scope="module")
def crucible_results():
    """Run the full crucible replay once, share across all tests in this module."""
    from scripts.run_crucible import run_replay

    return run_replay(
        data_path=str(DATA_PATH),
        window_secs=30,
        threshold=3.0,
        warmup=10,
    )


@skip_no_data
def test_parses_all_trades(crucible_results):
    """All 1.6M+ trades should produce a meaningful number of windows."""
    assert len(crucible_results) > 2000, (
        f"Expected >2000 windows from 1.6M trades, got {len(crucible_results)}"
    )


@skip_no_data
def test_sell_pressure_breaches_dominate(crucible_results):
    """
    During a 50% crash, sell-pressure breaches must significantly outnumber
    buy-pressure breaches. This validates directional correctness of the
    OFI sign convention (negative z = selling).
    """
    sell_breaches = [w for w in crucible_results if w.breached and w.z_score < 0]
    buy_breaches = [w for w in crucible_results if w.breached and w.z_score > 0]
    assert len(sell_breaches) >= 40, (
        f"Expected >=40 sell-pressure breaches, got {len(sell_breaches)}"
    )
    assert len(sell_breaches) > len(buy_breaches) * 2, (
        f"Sell breaches ({len(sell_breaches)}) should be >2x buy breaches "
        f"({len(buy_breaches)}) during a crash"
    )


@skip_no_data
def test_peak_z_score_strongly_negative(crucible_results):
    """
    The most extreme window must be a sell-pressure event with z < -10σ.
    This validates that the OFI formula produces extreme readings during
    known panic cascades.
    """
    min_z = min(w.z_score for w in crucible_results)
    assert min_z < -10.0, (
        f"Expected peak sell z-score < -10σ, got {min_z:.2f}σ"
    )


@skip_no_data
def test_no_sign_inversion(crucible_results):
    """
    The VERIFY_ME question from triggers.py: during the crash, the most
    extreme windows should have NEGATIVE z-scores (not positive).
    Positive would indicate the sign convention is inverted.
    """
    top5_by_magnitude = sorted(crucible_results, key=lambda w: abs(w.z_score), reverse=True)[:5]
    negative_count = sum(1 for w in top5_by_magnitude if w.z_score < 0)
    assert negative_count >= 4, (
        f"Expected >=4 of top-5 extreme windows to be negative (sell), "
        f"got {negative_count}. Sign convention may be inverted."
    )


@skip_no_data
def test_first_breach_before_daily_low(crucible_results):
    """
    The OFI signal should fire BEFORE the price reaches its daily low.
    This validates that the signal has predictive/early-warning value,
    not just coincident detection.
    """
    daily_low_price = min(w.last_price for w in crucible_results)
    first_sell_breach = next(
        (w for w in crucible_results if w.breached and w.z_score < 0),
        None,
    )
    assert first_sell_breach is not None, "No sell-pressure breach found"
    assert first_sell_breach.last_price > daily_low_price * 1.05, (
        f"First sell breach price (${first_sell_breach.last_price:,.2f}) should be "
        f">5% above daily low (${daily_low_price:,.2f}) for early-warning value"
    )


@skip_no_data
def test_satoshi_conversion_produces_nonzero_sizes(crucible_results):
    """
    Validate that the satoshi conversion didn't produce degenerate OFI values.
    Every window should have nonzero buy_vol or sell_vol.
    """
    degenerate = [w for w in crucible_results if w.buy_vol == 0 and w.sell_vol == 0]
    assert len(degenerate) == 0, (
        f"Found {len(degenerate)} windows with zero buy AND sell volume — "
        f"satoshi conversion may be broken"
    )


@skip_no_data
def test_price_range_matches_known_crash(crucible_results):
    """
    Sanity check: BTC opened ~$7,900 and hit ~$4,400-4,800 on March 12.
    If prices are wildly different, the ingest adapter is misreading columns.
    """
    prices = [w.last_price for w in crucible_results]
    assert max(prices) > 7000, f"Max price ${max(prices):,.2f} too low — column mismatch?"
    assert min(prices) < 5500, f"Min price ${min(prices):,.2f} too high — column mismatch?"
