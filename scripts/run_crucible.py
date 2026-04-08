#!/usr/bin/env python3
"""
COVID Crucible — Standalone OFI Replay (Trade-Only)

Replays Binance aggTrades through the same OFI window math as the production
trigger in triggers.py, but WITHOUT the compound spread gate (no book data).

Computes per-window:
  - OFI (buy_vol - sell_vol, in satoshis)
  - Rolling mean / std over prior windows
  - Z-score (signed: negative = net selling)
  - Last trade price in window
  - Direction label
  - Threshold breach flag

OFI definition matches production exactly:
  OFI = sum(size for side=='B' aggressive) - sum(size for side=='A' aggressive)
  z   = (ofi - rolling_mean) / rolling_std

Window duration matches TRIGGER_WINDOW_SECS (default 30s).

Usage:
  python3 scripts/run_crucible.py [OPTIONS]

  --data PATH       aggTrades CSV/zip (default: data/binance/BTCUSDT-aggTrades-2020-03-12.zip)
  --window SECS     OFI window duration (default: 30, matches production)
  --threshold SIGMA Z-score threshold for flagging (default: 3.0)
  --warmup N        Minimum windows before Z-scoring (default: 10)
  --csv PATH        Write results to CSV (optional)
"""

from __future__ import annotations

import argparse
import csv as csv_mod
import logging
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

import numpy as np

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from foundry.ingest.binance_ingest import iter_aggtrades_csv

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Defaults — match production triggers.py
# ---------------------------------------------------------------------------
DEFAULT_WINDOW_SECS: int = 30
DEFAULT_THRESHOLD: float = 3.0
DEFAULT_WARMUP: int = 10
DEFAULT_DATA_PATH: str = "data/binance/BTCUSDT-aggTrades-2020-03-12.zip"


@dataclass
class OFIWindow:
    """One completed OFI window."""
    window_start_ns: int
    window_end_ns: int
    buy_vol: int          # satoshis
    sell_vol: int         # satoshis
    ofi: float            # buy_vol - sell_vol
    trade_count: int
    last_price: float
    rolling_mean: float
    rolling_std: float
    z_score: float
    breached: bool
    direction: str        # "sell_pressure" | "buy_pressure" | "neutral"

    @property
    def start_utc(self) -> str:
        return datetime.fromtimestamp(
            self.window_start_ns / 1e9, tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S")

    @property
    def end_utc(self) -> str:
        return datetime.fromtimestamp(
            self.window_end_ns / 1e9, tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S")


def run_replay(
    data_path: str,
    window_secs: int = DEFAULT_WINDOW_SECS,
    threshold: float = DEFAULT_THRESHOLD,
    warmup: int = DEFAULT_WARMUP,
) -> List[OFIWindow]:
    """
    Replay aggTrades through windowed OFI computation.

    Uses the same OFI definition as production triggers.py:
      OFI = sum(size | side=='B', aggressive) - sum(size | side=='A', aggressive)

    Z-score is self-bootstrapped from a rolling window of all prior OFI values
    (no external baseline available for historical Binance data).
    """
    window_ns = int(window_secs * 1e9)
    results: List[OFIWindow] = []
    ofi_history: List[float] = []

    # Current window accumulators
    window_start_ns: Optional[int] = None
    buy_vol = 0
    sell_vol = 0
    trade_count = 0
    last_price = 0.0

    def close_window(end_ns: int) -> None:
        """Close the current window, compute Z-score, append to results."""
        nonlocal buy_vol, sell_vol, trade_count, last_price, window_start_ns

        # OFI: exact production formula
        ofi = float(buy_vol - sell_vol)
        ofi_history.append(ofi)

        # Rolling Z-score (self-bootstrapped)
        if len(ofi_history) >= warmup:
            arr = np.array(ofi_history[:-1])  # exclude current window
            rolling_mean = float(arr.mean())
            rolling_std = float(arr.std(ddof=1)) if len(arr) > 1 else 0.0
            if rolling_std > 0:
                z_score = (ofi - rolling_mean) / rolling_std
            else:
                z_score = 0.0
        else:
            rolling_mean = 0.0
            rolling_std = 0.0
            z_score = 0.0

        breached = abs(z_score) >= threshold
        if z_score < -1.0:
            direction = "sell_pressure"
        elif z_score > 1.0:
            direction = "buy_pressure"
        else:
            direction = "neutral"

        results.append(OFIWindow(
            window_start_ns=window_start_ns,
            window_end_ns=end_ns,
            buy_vol=buy_vol,
            sell_vol=sell_vol,
            ofi=ofi,
            trade_count=trade_count,
            last_price=last_price,
            rolling_mean=rolling_mean,
            rolling_std=rolling_std,
            z_score=z_score,
            breached=breached,
            direction=direction,
        ))

        # Reset accumulators
        buy_vol = 0
        sell_vol = 0
        trade_count = 0
        window_start_ns = None

    # Stream trades
    for event, _inst in iter_aggtrades_csv(data_path):
        ts_ns = event.timestamp_ns

        if window_start_ns is None:
            window_start_ns = ts_ns

        # Close window if duration exceeded
        if ts_ns - window_start_ns >= window_ns:
            close_window(ts_ns)
            window_start_ns = ts_ns

        # Accumulate — matches production _compute_ofi exactly
        if event.is_aggressive:
            if event.side == "B":
                buy_vol += event.size
            elif event.side == "A":
                sell_vol += event.size
        trade_count += 1
        last_price = event.price

    # Close final partial window
    if window_start_ns is not None and trade_count > 0:
        close_window(window_start_ns + window_ns)

    return results


def print_report(results: List[OFIWindow], threshold: float) -> None:
    """Print summary to stdout."""
    breaches = [w for w in results if w.breached]
    sell_breaches = [w for w in breaches if w.z_score < 0]
    buy_breaches = [w for w in breaches if w.z_score > 0]

    print("=" * 90)
    print("COVID CRUCIBLE — OFI Replay Report")
    print(f"BTCUSDT | 2020-03-12 (Black Thursday) | {len(results)} windows")
    print("=" * 90)
    print(f"Threshold: |z| >= {threshold:.1f}σ")
    print(f"Total breaches: {len(breaches)}")
    print(f"  Sell pressure (z < -{threshold:.1f}): {len(sell_breaches)}")
    print(f"  Buy pressure  (z > +{threshold:.1f}): {len(buy_breaches)}")
    print()

    if results:
        prices = [w.last_price for w in results]
        print(f"Price range: ${min(prices):,.2f} → ${max(prices):,.2f}")
        print(f"  Open:  ${results[0].last_price:,.2f}")
        print(f"  Close: ${results[-1].last_price:,.2f}")
        print()

    if breaches:
        print("-" * 90)
        print(f"{'Time (UTC)':<22} {'Price':>10} {'OFI (sats)':>16} "
              f"{'Z-score':>9} {'Direction':<16} {'Trades':>7}")
        print("-" * 90)
        for w in breaches:
            flag = "*** " if abs(w.z_score) >= threshold * 1.5 else "    "
            print(f"{flag}{w.start_utc:<18} ${w.last_price:>9,.2f} "
                  f"{w.ofi:>16,.0f} {w.z_score:>+9.2f}σ "
                  f"{w.direction:<16} {w.trade_count:>7,}")
        print("-" * 90)

    # Top 10 most extreme windows
    print()
    print("Top 10 most extreme windows (by |z-score|):")
    print("-" * 90)
    top10 = sorted(results, key=lambda w: abs(w.z_score), reverse=True)[:10]
    for i, w in enumerate(top10, 1):
        print(f"  {i:>2}. {w.start_utc}  ${w.last_price:>9,.2f}  "
              f"z={w.z_score:>+8.2f}σ  OFI={w.ofi:>16,.0f}  "
              f"trades={w.trade_count:,}")
    print()


def write_csv(results: List[OFIWindow], path: str) -> None:
    """Write full results to CSV."""
    with open(path, "w", newline="") as f:
        writer = csv_mod.writer(f)
        writer.writerow([
            "window_start_utc", "window_end_utc", "last_price",
            "buy_vol_sats", "sell_vol_sats", "ofi_sats",
            "rolling_mean", "rolling_std", "z_score",
            "breached", "direction", "trade_count",
        ])
        for w in results:
            writer.writerow([
                w.start_utc, w.end_utc, f"{w.last_price:.2f}",
                w.buy_vol, w.sell_vol, f"{w.ofi:.0f}",
                f"{w.rolling_mean:.2f}", f"{w.rolling_std:.2f}",
                f"{w.z_score:.4f}",
                w.breached, w.direction, w.trade_count,
            ])
    print(f"CSV written: {path} ({len(results)} rows)")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="COVID Crucible — Standalone OFI Replay"
    )
    parser.add_argument(
        "--data", default=DEFAULT_DATA_PATH,
        help=f"aggTrades CSV/zip path (default: {DEFAULT_DATA_PATH})",
    )
    parser.add_argument(
        "--window", type=int, default=DEFAULT_WINDOW_SECS,
        help=f"OFI window duration in seconds (default: {DEFAULT_WINDOW_SECS})",
    )
    parser.add_argument(
        "--threshold", type=float, default=DEFAULT_THRESHOLD,
        help=f"Z-score threshold for flagging (default: {DEFAULT_THRESHOLD})",
    )
    parser.add_argument(
        "--warmup", type=int, default=DEFAULT_WARMUP,
        help=f"Minimum windows before Z-scoring (default: {DEFAULT_WARMUP})",
    )
    parser.add_argument(
        "--csv", default=None,
        help="Write full results to CSV file (optional)",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true",
        help="Enable DEBUG logging",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-5s %(message)s",
    )

    results = run_replay(
        data_path=args.data,
        window_secs=args.window,
        threshold=args.threshold,
        warmup=args.warmup,
    )

    print_report(results, threshold=args.threshold)

    if args.csv:
        write_csv(results, args.csv)


if __name__ == "__main__":
    main()
