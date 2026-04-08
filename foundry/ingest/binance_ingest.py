"""
Binance aggTrades CSV → MBOEvent adapter.

Reads Binance Vision aggTrades CSV files (optionally zipped) and yields
MBOEvent objects compatible with the IngestPipeline.

IMPORTANT — size semantics:
    Binance spot trade quantities arrive as fractional BTC (e.g. 0.03150700).
    MBOEvent.size is int (contract units). This adapter converts BTC → satoshis:
        size = int(round(float(qty_str) * 100_000_000))
    All downstream OFI accumulators therefore operate in satoshi units.
    Since OFI is Z-scored, the absolute scale is irrelevant — only relative
    magnitude within a window matters.

CSV format (Binance Vision aggTrades, no header):
    Column 0: agg_trade_id      (int)
    Column 1: price              (string/float)
    Column 2: qty                (string/float, BTC)
    Column 3: first_trade_id     (int)
    Column 4: last_trade_id      (int)
    Column 5: timestamp          (epoch milliseconds)
    Column 6: is_buyer_maker     (True/False string)
    Column 7: is_best_match      (True/False string)

Aggressor convention:
    is_buyer_maker=True  → the *maker* was the buyer, so the *taker* is a seller
                         → seller aggressive → side="A", is_aggressive=True
    is_buyer_maker=False → the *maker* was the seller, so the *taker* is a buyer
                         → buyer aggressive  → side="B", is_aggressive=True

    This matches the Databento mbp-10 convention used by LiveMBOStreamer:
        side='B' = buyer aggressive
        side='A' = seller aggressive
"""

from __future__ import annotations

import asyncio
import csv
import io
import logging
import zipfile
from pathlib import Path
from typing import AsyncIterator, Iterator, Tuple

from foundry.dag.triggers import MBOEvent

logger = logging.getLogger(__name__)

# 1 BTC = 100,000,000 satoshis
_SATS_PER_BTC: int = 100_000_000


def _parse_row(row: list[str], instrument: str) -> MBOEvent | None:
    """
    Parse one aggTrades CSV row into an MBOEvent.

    Returns None for malformed rows (logged at DEBUG level).
    """
    try:
        # Column indices — Binance Vision aggTrades, no header
        price_str = row[1]
        qty_str = row[2]
        ts_ms_str = row[5]
        is_buyer_maker_str = row[6].strip()

        # --- Validate and convert price ---
        price = float(price_str)
        if price <= 0.0:
            logger.debug("[BinanceIngest] Skipping non-positive price: %s", price_str)
            return None

        # --- Validate and convert quantity → satoshis ---
        qty_btc = float(qty_str)
        if qty_btc <= 0.0:
            logger.debug("[BinanceIngest] Skipping non-positive qty: %s", qty_str)
            return None
        # Binance spot trade qty arrives in BTC; store as satoshis to preserve
        # integer semantics in MBOEvent.size
        size = int(round(qty_btc * _SATS_PER_BTC))
        if size == 0:
            # Sub-satoshi trade (< 0.000000005 BTC) — skip
            logger.debug("[BinanceIngest] Sub-satoshi trade rounded to 0, skipping")
            return None

        # --- Timestamp: ms → ns ---
        timestamp_ns = int(ts_ms_str) * 1_000_000

        # --- Aggressor side ---
        # is_buyer_maker=True  → taker is seller → side "A"
        # is_buyer_maker=False → taker is buyer  → side "B"
        if is_buyer_maker_str == "True":
            side = "A"
        elif is_buyer_maker_str == "False":
            side = "B"
        else:
            logger.debug(
                "[BinanceIngest] Unrecognized is_buyer_maker: %r",
                is_buyer_maker_str,
            )
            return None

        return MBOEvent(
            timestamp_ns=timestamp_ns,
            instrument=instrument,
            action="T",
            side=side,
            price=price,
            size=size,
            order_id=0,  # Not available in aggTrades
            is_aggressive=True,  # All aggTrades are aggressive by definition
        )

    except (IndexError, ValueError) as exc:
        logger.debug("[BinanceIngest] Malformed row: %s — %s", row[:3], exc)
        return None


def iter_aggtrades_csv(
    path: str | Path,
    instrument: str = "BTCUSDT",
) -> Iterator[Tuple[MBOEvent, str]]:
    """
    Synchronous iterator over a Binance aggTrades CSV (or .zip containing one).

    Yields (MBOEvent, instrument) tuples in file order (timestamp-ascending).
    Malformed rows are silently skipped (logged at DEBUG).

    Parameters
    ----------
    path : str or Path
        Path to a .csv or .zip file. If .zip, the first CSV inside is used.
    instrument : str
        Symbol string attached to each MBOEvent (default: "BTCUSDT").
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"aggTrades file not found: {path}")

    parsed = 0
    skipped = 0

    if path.suffix == ".zip":
        with zipfile.ZipFile(path, "r") as zf:
            csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
            if not csv_names:
                raise ValueError(f"No CSV found inside {path}")
            csv_name = csv_names[0]
            logger.info("[BinanceIngest] Reading %s from %s", csv_name, path.name)
            with zf.open(csv_name) as f:
                reader = csv.reader(io.TextIOWrapper(f, encoding="utf-8"))
                for row in reader:
                    event = _parse_row(row, instrument)
                    if event is not None:
                        parsed += 1
                        yield (event, instrument)
                    else:
                        skipped += 1
    else:
        logger.info("[BinanceIngest] Reading %s", path.name)
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            for row in reader:
                event = _parse_row(row, instrument)
                if event is not None:
                    parsed += 1
                    yield (event, instrument)
                else:
                    skipped += 1

    logger.info(
        "[BinanceIngest] Complete. Parsed: %d, Skipped: %d", parsed, skipped
    )


async def aiter_aggtrades_csv(
    path: str | Path,
    instrument: str = "BTCUSDT",
    yield_interval: int = 10_000,
) -> AsyncIterator[Tuple[MBOEvent, str]]:
    """
    Async wrapper over iter_aggtrades_csv for IngestPipeline compatibility.

    Yields control to the event loop every `yield_interval` records to avoid
    starving other coroutines (matches HistoricalMBOStreamer pattern).

    This is the entry point for plugging Binance data into IngestPipeline.run().
    """
    count = 0
    for event, inst in iter_aggtrades_csv(path, instrument):
        yield (event, inst)
        count += 1
        if count % yield_interval == 0:
            await asyncio.sleep(0)
