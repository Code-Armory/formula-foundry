"""
Databento MBO Ingest Pipeline — Formula Foundry Live Data Layer

Replaces build_test_panic_slice() with production-grade market data.
Connects the Formula Foundry to Databento's real nanosecond MBO feed.

Architecture:
  BaseMBOStreamer (ABC)
    ├── LiveMBOStreamer     — databento.Live websocket (production)
    └── HistoricalMBOStreamer — databento.Historical DBN replay (testing)

  InstrumentState (per-instrument)
    ├── event_buffer        — time-keyed deque, 30s rolling window of MBOEvents
    ├── snapshot_buffer     — time-keyed deque of OrderBookSnapshots
    ├── baseline_ofi_mean   — 20-day window-level OFI mean (loaded from Postgres)
    ├── baseline_ofi_std    — 20-day window-level OFI std  (loaded from Postgres)
    ├── spread_percentiles  — historical spread distribution (loaded from Postgres)
    └── circuit_breaker     — per-instrument lockout after trigger fires

  BaselineManager
    ├── load_baseline()     — Postgres read on startup
    ├── save_baseline()     — Postgres write after bootstrap
    └── is_stale()          — True if baseline older than 1 trading day

  HistoricalBootstrap
    └── run()               — pulls 20 trading days, computes window-level stats,
                              writes to Postgres via BaselineManager

  IngestPipeline
    └── run()               — the main loop:
                              tick → parse → buffer → evaluate → [trigger → flow]

Databento MBO aggressor convention:
  action == 'T' (trade) → aggressive order
  side field on a trade == the RESTING side that was hit
    side == 'A' (ask resting, hit by buyer)  → aggressive BUY  → MBOEvent side='B'
    side == 'B' (bid resting, hit by seller) → aggressive SELL → MBOEvent side='A'

Postgres baseline schema (created on startup):
  instrument_baselines: instrument, ofi_mean, ofi_std,
                        spread_percentiles_json, computed_at, trading_days_used
  ingest_log: instrument, event_type, details_json, occurred_at

Environment variables:
  DATABENTO_API_KEY     — required
  LIVE_MODE             — "true" for live feed, "false" for historical replay (default: false)
  INSTRUMENTS           — comma-separated, e.g. "ES.c.0,NQ.c.0" (default: "ES.c.0")
  DATABENTO_DATASET     — e.g. "GLBX.MDP3" for CME Globex (default: GLBX.MDP3)
  REPLAY_START          — ISO date for historical replay start (default: 20 trading days ago)
  REPLAY_END            — ISO date for historical replay end (default: yesterday)
  POSTGRES_DSN          — PostgreSQL connection string
  BLACKBOARD_API_URL    — Blackboard API (default: http://localhost:8000)
  CIRCUIT_BREAKER_SECS  — lockout after trigger (default: 300)
  TRIGGER_WINDOW_SECS   — rolling OFI window (default: 30)
  BOOTSTRAP_TRADING_DAYS — days of history for baseline (default: 20)
  OFI_SIGMA_THRESHOLD   — z-score trigger (default: 3.0)
  SPREAD_PCTILE_THRESHOLD — spread percentile trigger (default: 95.0)

To run in replay mode (development):
  PYTHONPATH=. python -m foundry.ingest.databento_ingest

To run in live mode (production):
  LIVE_MODE=true PYTHONPATH=. python -m foundry.ingest.databento_ingest
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator, Dict, FrozenSet, List, Optional, Tuple

import asyncpg
import numpy as np

from foundry.dag.triggers import (
    MBOEvent,
    MarketDataSlice,
    OrderBookSnapshot,
    CrossAssetWindowSample,
    CrossAssetTrigger,
    MBP10Level,
    MBP10Snapshot,
    EntropyBaseline,
    EntropyTrigger,
    detect_panic_fingerprint,
    detect_cross_asset_decoupling,
    detect_entropy_collapse,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DATABENTO_API_KEY     = os.environ.get("DATABENTO_API_KEY", "")
LIVE_MODE             = os.environ.get("LIVE_MODE", "false").lower() == "true"
INSTRUMENTS_RAW       = os.environ.get("INSTRUMENTS", "ES.c.0")
INSTRUMENTS           = [s.strip() for s in INSTRUMENTS_RAW.split(",") if s.strip()]
DATABENTO_DATASET     = os.environ.get("DATABENTO_DATASET", "GLBX.MDP3")
POSTGRES_DSN          = os.environ.get("POSTGRES_DSN", "postgresql://foundry:foundry_state@localhost:5432/foundry_dag")
BLACKBOARD_API_URL    = os.environ.get("BLACKBOARD_API_URL", "http://localhost:8000")
CIRCUIT_BREAKER_SECS  = int(os.environ.get("CIRCUIT_BREAKER_SECS", "300"))
TRIGGER_WINDOW_SECS   = int(os.environ.get("TRIGGER_WINDOW_SECS", "30"))
BOOTSTRAP_TRADING_DAYS = int(os.environ.get("BOOTSTRAP_TRADING_DAYS", "20"))
OFI_SIGMA_THRESHOLD   = float(os.environ.get("OFI_SIGMA_THRESHOLD", "3.0"))
SPREAD_PCTILE_THRESHOLD = float(os.environ.get("SPREAD_PCTILE_THRESHOLD", "95.0"))

# Databento price divisor — MBO prices are in fixed-point 1e-9 units
_PRICE_DIVISOR = 1_000_000_000.0

# Trading days to calendar days multiplier (approximate, conservative)
_TRADING_TO_CALENDAR_DAYS = 1.45


# ---------------------------------------------------------------------------
# Per-instrument state
# ---------------------------------------------------------------------------


@dataclass
class CircuitBreaker:
    """
    Per-instrument lockout after a trigger fires.
    Prevents the DAG from being flooded during a sustained panic event.
    """
    lockout_seconds: int = CIRCUIT_BREAKER_SECS
    _last_triggered_at: Optional[datetime] = field(default=None, repr=False)

    def is_locked(self) -> bool:
        if self._last_triggered_at is None:
            return False
        elapsed = (datetime.now(timezone.utc) - self._last_triggered_at).total_seconds()
        return elapsed < self.lockout_seconds

    def trip(self) -> None:
        self._last_triggered_at = datetime.now(timezone.utc)
        logger.info(
            "[CircuitBreaker] Tripped. Locked for %ds until %s.",
            self.lockout_seconds,
            (datetime.now(timezone.utc) + timedelta(seconds=self.lockout_seconds)).isoformat(),
        )

    def remaining_seconds(self) -> float:
        if self._last_triggered_at is None:
            return 0.0
        elapsed = (datetime.now(timezone.utc) - self._last_triggered_at).total_seconds()
        return max(0.0, self.lockout_seconds - elapsed)


@dataclass
class InstrumentState:
    """
    All mutable state for one instrument.

    event_buffer: rolling deque of MBOEvents within TRIGGER_WINDOW_SECS.
        Time-keyed, not count-keyed. Event rate during ES panic: 10K+/sec.
        A fixed maxlen would be wrong. We trim by timestamp instead.

    snapshot_buffer: rolling deque of OrderBookSnapshots for spread tracking.
        Top-of-book updates arrive less frequently than aggressive trades.

    baseline_*: loaded from Postgres on startup. Never mutated in the hot loop.
        Updated once per trading day by the bootstrap process.

    spread_percentiles: the empirical CDF of historical spread values.
        Used by _compute_spread_percentile in triggers.py.
        Stored sorted for numpy searchsorted efficiency.
    """
    instrument: str
    event_buffer: deque = field(default_factory=deque)
    snapshot_buffer: deque = field(default_factory=deque)
    baseline_ofi_mean: float = 0.0
    baseline_ofi_std: float = 100.0       # conservative default, overwritten by bootstrap
    spread_percentiles: List[float] = field(default_factory=list)
    circuit_breaker: CircuitBreaker = field(default_factory=CircuitBreaker)
    window_start_ns: int = 0
    # Cross-asset feed: updated after every trigger evaluation
    last_computed_ofi_z: float = 0.0      # most recent OFI z-score from this instrument
    last_evaluated_ns: int = 0             # timestamp_ns of last trigger evaluation
    # Entropy state: loaded from Postgres at startup, used by MBP-10 entropy trigger
    entropy_baseline: Optional[EntropyBaseline] = None
    entropy_circuit_breaker: CircuitBreaker = field(default_factory=CircuitBreaker)
    # MBP-10 snapshot counter for entropy evaluation throttling
    _snapshot_count: int = field(default=0, repr=False)

    def trim_to_window(self, now_ns: int) -> None:
        """Remove events older than TRIGGER_WINDOW_SECS from both buffers."""
        cutoff_ns = now_ns - int(TRIGGER_WINDOW_SECS * 1e9)
        while self.event_buffer and self.event_buffer[0].timestamp_ns < cutoff_ns:
            self.event_buffer.popleft()
        while self.snapshot_buffer and self.snapshot_buffer[0].timestamp_ns < cutoff_ns:
            self.snapshot_buffer.popleft()
        self.window_start_ns = cutoff_ns

    def to_market_data_slice(self, now_ns: int) -> MarketDataSlice:
        """Snapshot current buffer state into a MarketDataSlice for trigger evaluation."""
        return MarketDataSlice(
            instrument=self.instrument,
            window_start_ns=self.window_start_ns,
            window_end_ns=now_ns,
            events=list(self.event_buffer),
            snapshots=list(self.snapshot_buffer),
            baseline_ofi_mean=self.baseline_ofi_mean,
            baseline_ofi_std=self.baseline_ofi_std,
            spread_percentiles=self.spread_percentiles,
        )


@dataclass
class CrossAssetState:
    """
    Mutable state for one instrument pair (e.g. ES↔ZN).

    Maintains a rolling deque of CrossAssetWindowSample objects —
    one sample per single-instrument trigger evaluation that has fresh
    z-scores for BOTH instruments in the pair.

    max_samples = 60: keeps ~30 minutes of data at 30-second windows.
    min_samples for trigger evaluation: _MIN_WINDOWS_REQUIRED (10).

    Staleness threshold: if either instrument hasn't been evaluated
    within STALE_NS nanoseconds, don't add a new sample (the z-score
    from a stale window is not representative of current market state).
    """
    instrument_a: str
    instrument_b: str
    samples: deque = field(default_factory=lambda: deque(maxlen=60))
    circuit_breaker: CircuitBreaker = field(default_factory=CircuitBreaker)

    # An instrument z-score is considered stale after 2 trigger windows
    STALE_NS: int = field(default=int(TRIGGER_WINDOW_SECS * 2e9), init=False)

    def try_add_sample(
        self,
        state_a: InstrumentState,
        state_b: InstrumentState,
        now_ns: int,
    ) -> bool:
        """
        Add a paired (z_a, z_b) sample if both instruments have fresh z-scores.

        Returns True if a sample was added, False if either instrument's
        z-score was stale (evaluated too long ago to be meaningful).
        """
        stale_a = (now_ns - state_a.last_evaluated_ns) > self.STALE_NS
        stale_b = (now_ns - state_b.last_evaluated_ns) > self.STALE_NS
        if stale_a or stale_b:
            return False

        self.samples.append(CrossAssetWindowSample(
            timestamp_ns=now_ns,
            ofi_z_a=state_a.last_computed_ofi_z,
            ofi_z_b=state_b.last_computed_ofi_z,
        ))
        return True


# ---------------------------------------------------------------------------
# Postgres baseline persistence
# ---------------------------------------------------------------------------


class BaselineManager:
    """
    Persists and retrieves 20-day OFI baseline statistics from Postgres.

    Schema (created on first call to initialize_schema):
      instrument_baselines: one row per instrument, upserted on bootstrap.
      ingest_log: audit trail of trigger events and bootstrap runs.
    """

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._pool: Optional[asyncpg.Pool] = None

    async def connect(self) -> None:
        self._pool = await asyncpg.create_pool(self._dsn, min_size=1, max_size=5)
        await self._initialize_schema()
        logger.info("[BaselineManager] Connected to Postgres.")

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()

    async def _initialize_schema(self) -> None:
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS instrument_baselines (
                    instrument          TEXT PRIMARY KEY,
                    ofi_mean            DOUBLE PRECISION NOT NULL,
                    ofi_std             DOUBLE PRECISION NOT NULL,
                    spread_percentiles_json TEXT NOT NULL,
                    computed_at         TIMESTAMPTZ NOT NULL,
                    trading_days_used   INTEGER NOT NULL
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS entropy_baselines (
                    instrument              TEXT PRIMARY KEY,
                    entropy_percentiles_json TEXT NOT NULL,
                    volume_percentiles_json  TEXT NOT NULL,
                    computed_at             TIMESTAMPTZ NOT NULL,
                    trading_days_used       INTEGER NOT NULL
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS ingest_log (
                    id          SERIAL PRIMARY KEY,
                    instrument  TEXT NOT NULL,
                    event_type  TEXT NOT NULL,
                    details_json TEXT,
                    occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
        logger.info("[BaselineManager] Schema initialized (OFI + entropy tables).")

    async def load_entropy_baseline(self, instrument: str) -> Optional[EntropyBaseline]:
        """Load entropy baseline from Postgres. Returns None if missing."""
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM entropy_baselines WHERE instrument = $1", instrument,
            )
        if row is None:
            return None
        return EntropyBaseline(
            entropy_samples=json.loads(row["entropy_percentiles_json"]),
            volume_samples=json.loads(row["volume_percentiles_json"]),
        )

    async def save_entropy_baseline(
        self,
        instrument: str,
        entropy_samples: List[float],
        volume_samples: List[float],
        trading_days_used: int,
    ) -> None:
        """Upsert entropy baseline after bootstrap."""
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO entropy_baselines
                    (instrument, entropy_percentiles_json, volume_percentiles_json,
                     computed_at, trading_days_used)
                VALUES ($1, $2, $3, NOW(), $4)
                ON CONFLICT (instrument) DO UPDATE SET
                    entropy_percentiles_json = EXCLUDED.entropy_percentiles_json,
                    volume_percentiles_json  = EXCLUDED.volume_percentiles_json,
                    computed_at              = EXCLUDED.computed_at,
                    trading_days_used        = EXCLUDED.trading_days_used
                """,
                instrument,
                json.dumps(entropy_samples),
                json.dumps(volume_samples),
                trading_days_used,
            )
        logger.info(
            "[BaselineManager] Saved entropy baseline for %s: "
            "%d entropy samples, %d volume samples.",
            instrument, len(entropy_samples), len(volume_samples),
        )

    async def is_entropy_stale(self, instrument: str) -> bool:
        """True if entropy baseline is missing or older than 1 trading day."""
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT computed_at FROM entropy_baselines WHERE instrument = $1", instrument,
            )
        if row is None:
            return True
        age_hours = (datetime.now(timezone.utc) - row["computed_at"]).total_seconds() / 3600
        return age_hours > (24 * _TRADING_TO_CALENDAR_DAYS)

    async def load_baseline(self, instrument: str) -> Optional[Dict]:
        """
        Load baseline from Postgres. Returns None if no baseline exists.
        Caller must run historical bootstrap if None is returned.
        """
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM instrument_baselines WHERE instrument = $1",
                instrument,
            )
        if row is None:
            return None
        return {
            "ofi_mean": row["ofi_mean"],
            "ofi_std": row["ofi_std"],
            "spread_percentiles": json.loads(row["spread_percentiles_json"]),
            "computed_at": row["computed_at"],
            "trading_days_used": row["trading_days_used"],
        }

    async def save_baseline(
        self,
        instrument: str,
        ofi_mean: float,
        ofi_std: float,
        spread_percentiles: List[float],
        trading_days_used: int,
    ) -> None:
        """Upsert baseline. Called after bootstrap completes."""
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO instrument_baselines
                    (instrument, ofi_mean, ofi_std, spread_percentiles_json,
                     computed_at, trading_days_used)
                VALUES ($1, $2, $3, $4, NOW(), $5)
                ON CONFLICT (instrument) DO UPDATE SET
                    ofi_mean = EXCLUDED.ofi_mean,
                    ofi_std  = EXCLUDED.ofi_std,
                    spread_percentiles_json = EXCLUDED.spread_percentiles_json,
                    computed_at = EXCLUDED.computed_at,
                    trading_days_used = EXCLUDED.trading_days_used
                """,
                instrument, ofi_mean, ofi_std,
                json.dumps(spread_percentiles), trading_days_used,
            )
        logger.info(
            "[BaselineManager] Saved baseline for %s: mean=%.4f std=%.4f days=%d",
            instrument, ofi_mean, ofi_std, trading_days_used,
        )

    async def is_stale(self, instrument: str) -> bool:
        """
        Returns True if baseline is missing or older than 1 trading day (~1.45 calendar days).
        Triggers re-bootstrap on worker restart after a session gap.
        """
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT computed_at FROM instrument_baselines WHERE instrument = $1",
                instrument,
            )
        if row is None:
            return True
        age_hours = (datetime.now(timezone.utc) - row["computed_at"]).total_seconds() / 3600
        return age_hours > (24 * _TRADING_TO_CALENDAR_DAYS)

    async def log_event(
        self,
        instrument: str,
        event_type: str,
        details: Optional[Dict] = None,
    ) -> None:
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO ingest_log (instrument, event_type, details_json)
                VALUES ($1, $2, $3)
                """,
                instrument, event_type,
                json.dumps(details) if details else None,
            )


# ---------------------------------------------------------------------------
# Historical bootstrap
# ---------------------------------------------------------------------------


class HistoricalBootstrap:
    """
    Pulls BOOTSTRAP_TRADING_DAYS of MBO data from Databento Historical API,
    computes window-level OFI statistics, and writes to Postgres.

    Window-level computation rationale:
        The trigger evaluates OFI over a 30-second window and computes a z-score
        against the baseline. For the z-score to be statistically meaningful,
        the baseline must be computed at the same time resolution.

        Window-level OFI = Σ(aggressive buy volume) - Σ(aggressive sell volume)
        over each 30-second window in the historical dataset.

        We take mean and std of ~7,800 such window values per instrument
        (260 windows/day × 30 trading days).

    Spread percentile computation:
        We sample the bid-ask spread at each top-of-book update, collect
        all samples across the bootstrap period, and store the sorted
        distribution for percentile lookup during live trading.
    """

    def __init__(self, api_key: str, baseline_manager: BaselineManager) -> None:
        self._api_key = api_key
        self._baseline = baseline_manager

    async def run(self, instrument: str, dataset: str) -> Tuple[float, float, List[float]]:
        """
        Execute bootstrap for one instrument by pulling one calendar day at a time.
        Returns (ofi_mean, ofi_std, spread_percentiles). Writes to Postgres.

        WHY DAILY CHUNKS (not one large get_range call):
            20 days of ES MBO data is 50–100 GB during normal markets,
            500+ GB during crisis periods (March 2020: ~800 GB/day for ES).
            A single get_range call times out before the Databento server
            finishes packaging the response. Daily chunks:
              - Each request is 2–50 GB — manageable with a 10-minute timeout
              - Market holidays and data gaps are skipped gracefully
              - A failed day does not abort the entire bootstrap
              - Requires ≥ MIN_VALID_DAYS to accept the baseline

        Progress: logged per day. Typical run time: 2–15 min/day depending
        on data volume and network throughput.
        """
        logger.info(
            "[Bootstrap] Starting daily-chunked historical pull for %s (%d trading days)...",
            instrument, BOOTSTRAP_TRADING_DAYS,
        )

        calendar_days = int(BOOTSTRAP_TRADING_DAYS * _TRADING_TO_CALENDAR_DAYS) + 5
        end_dt = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        start_dt = end_dt - timedelta(days=calendar_days)

        try:
            import databento as db
        except ImportError:
            raise ImportError(
                "databento package not installed. "
                "Add 'databento = \"^0.40.0\"' to pyproject.toml and reinstall."
            )

        client = db.Historical(key=self._api_key)

        # Build list of calendar days to try
        days_to_try: List[datetime] = []
        current = start_dt
        while current < end_dt:
            days_to_try.append(current)
            current += timedelta(days=1)

        window_ofis: List[float] = []
        spread_samples: List[float] = []
        entropy_samples: List[float] = []
        volume_samples: List[float] = []
        valid_days = 0
        failed_days = 0

        # State that persists across day boundaries — partial windows at midnight
        # are discarded (clean boundary per day is acceptable at this resolution)
        for day in days_to_try:
            day_start_str = day.strftime("%Y-%m-%dT%H:%M:%S")
            day_end_str   = (day + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S")
            day_label     = day.strftime("%Y-%m-%d")

            def _fetch_day(s=day_start_str, e=day_end_str):
                return client.timeseries.get_range(
                    dataset=dataset,
                    symbols=[instrument],
                    schema="mbp-10",    # unified schema: trades + 10-level depth
                    start=s,
                    end=e,
                )

            try:
                logger.info(
                    "[Bootstrap] Fetching %s %s (%d/%d)...",
                    instrument, day_label,
                    len(days_to_try) - days_to_try.index(day),
                    len(days_to_try),
                )
                loop = asyncio.get_event_loop()
                # 10 minutes per day — generous even for crisis-period data
                store = await asyncio.wait_for(
                    loop.run_in_executor(None, _fetch_day),
                    timeout=600,
                )
                day_ofis, day_spreads, day_entropies, day_volumes = \
                    self._process_day_records(store)
                window_ofis.extend(day_ofis)
                spread_samples.extend(day_spreads)
                entropy_samples.extend(day_entropies)
                volume_samples.extend(day_volumes)
                valid_days += 1
                logger.info(
                    "[Bootstrap] %s %s: %d OFI windows | %d spread | "
                    "%d entropy | %d volume samples accumulated.",
                    instrument, day_label,
                    len(day_ofis), len(day_spreads),
                    len(day_entropies), len(day_volumes),
                )

            except asyncio.TimeoutError:
                logger.warning(
                    "[Bootstrap] %s %s: request timed out (>600s). "
                    "Possible causes: very large data volume or Databento API latency. Skipping.",
                    instrument, day_label,
                )
                failed_days += 1

            except Exception as exc:
                # Market holidays, instrument not trading, data gaps, API errors
                logger.warning(
                    "[Bootstrap] %s %s: %s. Skipping.",
                    instrument, day_label, exc,
                )
                failed_days += 1

        # Require a minimum number of valid trading days
        MIN_VALID_DAYS = 10
        if valid_days < MIN_VALID_DAYS:
            raise RuntimeError(
                f"Bootstrap failed for {instrument}: only {valid_days}/{len(days_to_try)} "
                f"calendar days succeeded (minimum {MIN_VALID_DAYS} required). "
                f"Failures: {failed_days}. "
                "Check DATABENTO_API_KEY, instrument symbol, and date range."
            )

        if not window_ofis:
            raise RuntimeError(
                f"Bootstrap produced zero OFI windows for {instrument} "
                f"despite {valid_days} valid days. Check data quality."
            )

        ofi_arr = np.array(window_ofis)
        ofi_mean = float(np.mean(ofi_arr))
        ofi_std  = float(np.std(ofi_arr))

        if ofi_std < 1.0:
            logger.warning(
                "[Bootstrap] OFI std=%.6f is suspiciously low for %s "
                "(%d windows from %d valid days). Using minimum std=1.0.",
                ofi_std, instrument, len(window_ofis), valid_days,
            )
            ofi_std = 1.0

        # Control memory: downsample spread samples if very large
        if len(spread_samples) > 200_000:
            spread_samples = spread_samples[::2]
        spread_percentiles = sorted(spread_samples)

        logger.info(
            "[Bootstrap] %s complete: %d OFI windows from %d valid days "
            "(%d skipped) | ofi_mean=%.4f ofi_std=%.4f | "
            "%d spread | %d entropy | %d volume samples",
            instrument, len(window_ofis), valid_days, failed_days,
            ofi_mean, ofi_std,
            len(spread_percentiles), len(entropy_samples), len(volume_samples),
        )

        await self._baseline.save_baseline(
            instrument=instrument,
            ofi_mean=ofi_mean,
            ofi_std=ofi_std,
            spread_percentiles=spread_percentiles,
            trading_days_used=valid_days,
        )

        # Save entropy baseline (sorted for percentile lookup)
        entropy_sorted = sorted(entropy_samples) if entropy_samples else []
        volume_sorted  = sorted(volume_samples)  if volume_samples  else []
        if entropy_sorted:
            await self._baseline.save_entropy_baseline(
                instrument=instrument,
                entropy_samples=entropy_sorted,
                volume_samples=volume_sorted,
                trading_days_used=valid_days,
            )
        else:
            logger.warning(
                "[Bootstrap] No entropy samples collected for %s. "
                "MBP-10 levels may not be present in the data. "
                "Entropy trigger will not be available for this instrument.",
                instrument,
            )

        await self._baseline.log_event(
            instrument=instrument,
            event_type="baseline_bootstrap",
            details={
                "ofi_mean": ofi_mean,
                "ofi_std": ofi_std,
                "windows_computed": len(window_ofis),
                "spread_samples": len(spread_percentiles),
                "entropy_samples": len(entropy_sorted),
                "volume_samples": len(volume_sorted),
                "valid_days": valid_days,
                "failed_days": failed_days,
            },
        )
        return ofi_mean, ofi_std, spread_percentiles

    def _process_day_records(
        self,
        store,
    ) -> Tuple[List[float], List[float], List[float], List[float]]:
        """
        Process one day's DBNStore records (mbp-10 schema) into:
          - OFI windows and spread samples (for OFI baseline)
          - Entropy samples and bid volume samples (for entropy baseline)

        Returns:
            (window_ofis, spread_samples, entropy_samples, volume_samples)

        mbp-10 schema provides:
          - Trade records (action='T') with aggressor side → used for OFI
          - All records have `levels`: list of 10 BidAskPair objects
            → used for entropy and spread

        Aggressor convention (VERIFY_ME on first replay):
            action='T', side = aggressor side convention must match MBO.
            side='B' → buyer aggressive → buy volume (bid resting, buyer hit ask)
            side='A' → seller aggressive → sell volume (ask resting, seller hit bid)

        VERIFY_ME: If March 9, 2020 ES replay produces positive OFI z-scores
        during a limit-down cascade, the side convention is inverted here.
        Check: sell_vol should be >> buy_vol during limit-down events.
        """
        window_ofis: List[float] = []
        spread_samples: List[float] = []
        entropy_samples: List[float] = []
        volume_samples: List[float] = []

        window_ns = int(TRIGGER_WINDOW_SECS * 1e9)
        current_window_start_ns: Optional[int] = None
        window_buy_vol  = 0
        window_sell_vol = 0

        for record in store:
            ts_ns = record.ts_event

            if current_window_start_ns is None:
                current_window_start_ns = ts_ns

            if ts_ns - current_window_start_ns >= window_ns:
                window_ofis.append(float(window_buy_vol - window_sell_vol))
                current_window_start_ns = ts_ns
                window_buy_vol  = 0
                window_sell_vol = 0

            action    = chr(record.action) if isinstance(record.action, int) else record.action
            side_char = chr(record.side)   if isinstance(record.side,   int) else record.side

            # OFI: trade records only
            if action == 'T':
                size = record.size
                if side_char == 'B':
                    window_buy_vol  += size
                elif side_char == 'A':
                    window_sell_vol += size

            # Entropy + spread: every record has levels in mbp-10
            if hasattr(record, 'levels') and record.levels:
                levels = record.levels

                # Spread from best bid/ask
                try:
                    best_bid_px  = levels[0].bid_px / _PRICE_DIVISOR
                    best_ask_px  = levels[0].ask_px / _PRICE_DIVISOR
                    if best_ask_px > best_bid_px > 0:
                        mid = (best_bid_px + best_ask_px) / 2.0
                        spread_bps = ((best_ask_px - best_bid_px) / mid) * 10_000.0
                        spread_samples.append(spread_bps)
                        if len(spread_samples) > 100_000:
                            spread_samples = spread_samples[::2]
                except (AttributeError, ZeroDivisionError):
                    pass

                # Entropy from bid-side volume distribution
                try:
                    import numpy as _np
                    bid_sizes = _np.array(
                        [lvl.bid_sz for lvl in levels if lvl.bid_sz > 0], dtype=float
                    )
                    total_bid = float(bid_sizes.sum())
                    if total_bid > 0:
                        p = bid_sizes / total_bid
                        H = float(-_np.sum(p * _np.log2(p)))
                        entropy_samples.append(H)
                        volume_samples.append(total_bid)
                        if len(entropy_samples) > 500_000:
                            entropy_samples = entropy_samples[::2]
                            volume_samples  = volume_samples[::2]
                except Exception:
                    pass

        # Emit final partial window
        if window_buy_vol != 0 or window_sell_vol != 0:
            window_ofis.append(float(window_buy_vol - window_sell_vol))

        return window_ofis, spread_samples, entropy_samples, volume_samples


# ---------------------------------------------------------------------------
# MBO streamer interface
# ---------------------------------------------------------------------------


class BaseMBOStreamer(ABC):
    """
    Common interface for live and historical MBO data sources.

    Both implementations yield identical MBOEvent and OrderBookSnapshot
    objects. The pipeline never knows which one it is using.
    """

    def __init__(self, api_key: str, dataset: str, instruments: List[str]) -> None:
        self._api_key = api_key
        self._dataset = dataset
        self._instruments = instruments

    @abstractmethod
    async def stream(self) -> AsyncIterator[Tuple[MBOEvent | OrderBookSnapshot, str]]:
        """
        Yield (event_or_snapshot, instrument) tuples in timestamp order.
        instrument is the symbol string (e.g. 'ES.c.0').
        """
        ...


class LiveMBOStreamer(BaseMBOStreamer):
    """
    Databento Live websocket client — unified mbp-10 schema.

    mbp-10 delivers: trade events (with aggressor side) AND 10-level depth
    on every book change. This unified schema replaces the separate mbo
    subscription, eliminating dual-stream synchronization risk.

    Yields per record:
      - For action='T' (trade): MBOEvent (is_aggressive=True) for OFI
                                + MBP10Snapshot for entropy evaluation
      - For all other actions:  MBP10Snapshot only

    Aggressor convention (mbp-10 trades):
      VERIFY_ME: Confirm side='B' = buyer aggressive, side='A' = seller aggressive
      on first live run against a known event. See triggers.py VERIFY_ME note.
    """

    async def stream(self) -> AsyncIterator[Tuple[MBOEvent | MBP10Snapshot, str]]:
        try:
            import databento as db
        except ImportError:
            raise ImportError("databento package not installed.")

        logger.info(
            "[LiveMBOStreamer] Connecting to %s (mbp-10), instruments: %s",
            self._dataset, self._instruments,
        )

        client = db.Live(key=self._api_key)
        client.subscribe(
            dataset=self._dataset,
            schema="mbp-10",
            symbols=self._instruments,
        )

        logger.info("[LiveMBOStreamer] Subscribed (mbp-10). Streaming...")

        async for record in client:
            for event, instrument in self._parse_records(record):
                yield event, instrument

    def _parse_records(
        self, record
    ) -> List[Tuple[MBOEvent | MBP10Snapshot, str]]:
        """
        Translate one mbp-10 record into 1 or 2 events.

        Trade record → [MBOEvent, MBP10Snapshot] (OFI + entropy)
        Other record → [MBP10Snapshot] (entropy + spread only)

        Returns empty list for records we cannot parse (system msgs, errors).
        """
        results: List[Tuple[MBOEvent | MBP10Snapshot, str]] = []
        try:
            instrument = record.symbol if hasattr(record, 'symbol') else self._instruments[0]
            ts_ns      = record.ts_event
            action     = chr(record.action) if isinstance(record.action, int) else str(record.action)
            side_char  = chr(record.side)   if isinstance(record.side,   int) else str(record.side)

            # Build MBP10Snapshot from record levels (present in all mbp-10 records)
            snapshot = self._build_snapshot(record, instrument, ts_ns)

            if action == 'T':
                # Emit MBOEvent for OFI accumulation
                # mbp-10 trade convention: side = aggressor side
                # 'B' = buyer aggressive, 'A' = seller aggressive
                # (Differs from MBO where side = resting side! Verify empirically.)
                results.append((
                    MBOEvent(
                        timestamp_ns=ts_ns,
                        instrument=instrument,
                        action='T',
                        side=side_char,
                        price=record.price / _PRICE_DIVISOR,
                        size=record.size,
                        order_id=0,
                        is_aggressive=True,
                    ),
                    instrument,
                ))

            # Always emit MBP10Snapshot for entropy and spread tracking
            if snapshot is not None:
                results.append((snapshot, instrument))

        except Exception as exc:
            logger.debug("[LiveMBOStreamer] Failed to parse record: %s", exc)

        return results

    def _build_snapshot(
        self, record, instrument: str, ts_ns: int
    ) -> Optional[MBP10Snapshot]:
        """Extract MBP10Snapshot from record.levels."""
        if not hasattr(record, 'levels') or not record.levels:
            return None
        try:
            bids = [
                MBP10Level(price=lvl.bid_px / _PRICE_DIVISOR, size=lvl.bid_sz)
                for lvl in record.levels
                if lvl.bid_sz > 0
            ]
            asks = [
                MBP10Level(price=lvl.ask_px / _PRICE_DIVISOR, size=lvl.ask_sz)
                for lvl in record.levels
                if lvl.ask_sz > 0
            ]
            return MBP10Snapshot(
                timestamp_ns=ts_ns,
                instrument=instrument,
                bids=bids,
                asks=asks,
            )
        except Exception:
            return None


class HistoricalMBOStreamer(BaseMBOStreamer):
    """
    Databento Historical replay client.

    Replays a downloaded DBN file or a historical date range through the
    exact same parsing logic as LiveMBOStreamer. Used for:
      - Testing trigger logic against known panic events
      - Verifying circuit breaker behavior
      - Deterministic regression testing

    Replay mode is significantly slower than live (disk I/O bound).
    For CI/testing, use a small date range (1-2 days).
    """

    def __init__(
        self,
        api_key: str,
        dataset: str,
        instruments: List[str],
        start: Optional[str] = None,
        end: Optional[str] = None,
        dbn_file_path: Optional[str] = None,
    ) -> None:
        super().__init__(api_key, dataset, instruments)
        self._start = start
        self._end = end
        self._dbn_file_path = dbn_file_path

    async def stream(self) -> AsyncIterator[Tuple[MBOEvent | MBP10Snapshot, str]]:
        try:
            import databento as db
        except ImportError:
            raise ImportError("databento package not installed.")

        if self._dbn_file_path:
            logger.info("[HistoricalMBOStreamer] Replaying from file: %s", self._dbn_file_path)
            store = db.DBNStore.from_file(self._dbn_file_path)
        else:
            end_dt = datetime.now(timezone.utc).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            start_dt = end_dt - timedelta(
                days=int(BOOTSTRAP_TRADING_DAYS * _TRADING_TO_CALENDAR_DAYS)
            )
            start_str = self._start or start_dt.strftime("%Y-%m-%dT%H:%M:%S")
            end_str   = self._end   or end_dt.strftime("%Y-%m-%dT%H:%M:%S")

            logger.info(
                "[HistoricalMBOStreamer] Fetching %s (mbp-10): %s → %s",
                self._instruments, start_str, end_str,
            )
            client = db.Historical(key=self._api_key)
            store = client.timeseries.get_range(
                dataset=self._dataset,
                symbols=self._instruments,
                schema="mbp-10",
                start=start_str,
                end=end_str,
            )

        loop = asyncio.get_event_loop()
        records = await loop.run_in_executor(None, lambda: list(store))

        logger.info(
            "[HistoricalMBOStreamer] Loaded %d records. Replaying...", len(records)
        )

        live_parser = LiveMBOStreamer(self._api_key, self._dataset, self._instruments)
        for record in records:
            for event_tuple in live_parser._parse_records(record):
                yield event_tuple
            # Yield control to event loop between records
            # (avoids starving other coroutines during replay)
            await asyncio.sleep(0)


# ---------------------------------------------------------------------------
# Main ingest pipeline
# ---------------------------------------------------------------------------


class IngestPipeline:
    """
    The main orchestrator. Connects everything:
      baseline loading → streamer → per-instrument buffer → trigger → DAG call

    The hot loop (inside _process_event) is intentionally minimal:
      1. Route event to correct InstrumentState by instrument key
      2. Append to time-keyed buffer
      3. Trim events older than TRIGGER_WINDOW_SECS
      4. Every N events, evaluate the trigger
      5. If trigger fires and circuit breaker is open: invoke the DAG

    No Postgres I/O, no logging, no heavy computation in the hot loop.
    Postgres writes happen only in the circuit-breaker trip handler,
    which is outside the tight inner loop.
    """

    # Evaluate trigger every N new aggressive events (not every tick)
    # Reduces CPU usage while keeping trigger latency < 1 second on ES
    _EVAL_INTERVAL_EVENTS = 50

    def __init__(
        self,
        streamer: BaseMBOStreamer,
        baseline_manager: BaselineManager,
        instrument_states: Dict[str, InstrumentState],
        cross_asset_states: Optional[Dict[FrozenSet, CrossAssetState]] = None,
    ) -> None:
        self._streamer = streamer
        self._baseline = baseline_manager
        self._states = instrument_states
        self._cross_asset_states: Dict[FrozenSet, CrossAssetState] = cross_asset_states or {}
        self._event_counts: Dict[str, int] = {sym: 0 for sym in instrument_states}

    async def run(self) -> None:
        """Main loop. Runs until the stream is exhausted or KeyboardInterrupt."""
        logger.info(
            "[IngestPipeline] Starting. Instruments: %s | Mode: %s",
            list(self._states.keys()),
            "LIVE" if LIVE_MODE else "HISTORICAL REPLAY",
        )

        async for event_or_snapshot, instrument in self._streamer.stream():
            await self._process_event(event_or_snapshot, instrument)

        logger.info("[IngestPipeline] Stream exhausted. Pipeline complete.")

    # Evaluate entropy every N MBP10Snapshots — less frequent than OFI
    # (book structure changes more slowly than aggressive order flow)
    _ENTROPY_EVAL_INTERVAL = 20

    async def _process_event(
        self,
        event_or_snapshot: MBOEvent | MBP10Snapshot | OrderBookSnapshot,
        instrument: str,
    ) -> None:
        state = self._states.get(instrument)
        if state is None:
            return

        now_ns = event_or_snapshot.timestamp_ns
        state.trim_to_window(now_ns)

        if isinstance(event_or_snapshot, MBOEvent):
            state.event_buffer.append(event_or_snapshot)
            if event_or_snapshot.is_aggressive:
                self._event_counts[instrument] = self._event_counts.get(instrument, 0) + 1
                if self._event_counts[instrument] % self._EVAL_INTERVAL_EVENTS == 0:
                    await self._evaluate_trigger(state, now_ns)

        elif isinstance(event_or_snapshot, MBP10Snapshot):
            # Extract top-of-book into OrderBookSnapshot for spread tracking (backwards compat)
            if event_or_snapshot.bids and event_or_snapshot.asks:
                tob = OrderBookSnapshot(
                    timestamp_ns=now_ns,
                    instrument=instrument,
                    best_bid=event_or_snapshot.bids[0].price,
                    best_ask=event_or_snapshot.asks[0].price,
                    best_bid_size=event_or_snapshot.bids[0].size,
                    best_ask_size=event_or_snapshot.asks[0].size,
                )
                state.snapshot_buffer.append(tob)

            # Throttled entropy evaluation using full depth
            state._snapshot_count += 1
            if state._snapshot_count % self._ENTROPY_EVAL_INTERVAL == 0:
                await self._evaluate_entropy_trigger(state, event_or_snapshot)

        else:
            # Legacy OrderBookSnapshot (from non-mbp10 path)
            state.snapshot_buffer.append(event_or_snapshot)

    async def _evaluate_entropy_trigger(
        self, state: InstrumentState, snapshot: MBP10Snapshot
    ) -> None:
        """
        Evaluate bid-side Shannon entropy collapse trigger.
        Only fires when entropy_baseline is loaded (requires bootstrap to have run).
        """
        if state.entropy_baseline is None:
            return  # entropy bootstrap not yet complete for this instrument

        if state.entropy_circuit_breaker.is_locked():
            return

        entropy_trigger = detect_entropy_collapse(
            snapshot=snapshot,
            baseline=state.entropy_baseline,
        )

        if entropy_trigger.triggered:
            state.entropy_circuit_breaker.trip()
            await self._baseline.log_event(
                instrument=state.instrument,
                event_type="entropy_collapse",
                details={
                    "bid_entropy": entropy_trigger.bid_entropy,
                    "entropy_percentile": entropy_trigger.entropy_percentile,
                    "total_bid_volume": entropy_trigger.total_bid_volume,
                    "volume_percentile": entropy_trigger.volume_percentile,
                    "best_bid_fraction": entropy_trigger.best_bid_fraction,
                    "n_nonzero_levels": entropy_trigger.n_nonzero_levels,
                },
            )
            await self._invoke_entropy_dag(entropy_trigger)

    async def _invoke_entropy_dag(self, trigger: EntropyTrigger) -> None:
        """Fire entropy_flow for Agent 051 (non-blocking task)."""
        logger.info(
            "[Pipeline] Invoking entropy_flow for %s | H=%.3f bits | pctile=%.1f (non-blocking).",
            trigger.instrument, trigger.bid_entropy, trigger.entropy_percentile,
        )

        async def _run_flow():
            try:
                from foundry.dag.entropy_flow import entropy_flow
                result = await entropy_flow(trigger.to_agent_input())
                logger.info("[Pipeline] entropy_flow result: %s", result)
            except ImportError:
                logger.warning("[Pipeline] entropy_flow not yet built. Trigger captured.")
            except Exception as exc:
                logger.error("[Pipeline] entropy_flow failed: %s", exc, exc_info=True)

        asyncio.create_task(_run_flow())

    async def _evaluate_trigger(self, state: InstrumentState, now_ns: int) -> None:
        """
        Evaluate the single-instrument panic fingerprint trigger.
        Stores the computed z-score on InstrumentState for cross-asset feeding.
        After evaluating, checks all cross-asset pairs that include this instrument.
        """
        if state.circuit_breaker.is_locked():
            logger.debug(
                "[Pipeline] %s circuit breaker locked (%.0fs remaining).",
                state.instrument, state.circuit_breaker.remaining_seconds(),
            )
            return

        data_slice = state.to_market_data_slice(now_ns)
        trigger = detect_panic_fingerprint(
            data_slice,
            ofi_sigma_threshold=OFI_SIGMA_THRESHOLD,
            spread_pctile_threshold=SPREAD_PCTILE_THRESHOLD,
        )

        # Feed cross-asset state — always store, regardless of trigger outcome
        state.last_computed_ofi_z = trigger.ofi_zscore
        state.last_evaluated_ns = now_ns

        if trigger.triggered:
            logger.warning(
                "[Pipeline] PANIC FINGERPRINT: %s | OFI z=%.2fσ (selling) | Spread pctile=%.1f",
                state.instrument, trigger.ofi_zscore, trigger.spread_percentile,
            )
            state.circuit_breaker.trip()
            await self._baseline.log_event(
                instrument=state.instrument,
                event_type="trigger_fired",
                details={
                    "ofi_zscore": trigger.ofi_zscore,
                    "spread_percentile": trigger.spread_percentile,
                    "ofi_acceleration": trigger.ofi_acceleration,
                    "mbo_event_count": trigger.mbo_event_count,
                },
            )
            await self._invoke_dag(trigger, data_slice)

        # Cross-asset evaluation: check all pairs containing this instrument
        for pair_key, ca_state in self._cross_asset_states.items():
            if state.instrument not in pair_key:
                continue
            other_instrument = next(s for s in pair_key if s != state.instrument)
            other_state = self._states.get(other_instrument)
            if other_state is None:
                continue
            added = ca_state.try_add_sample(
                state_a=self._states[ca_state.instrument_a],
                state_b=self._states[ca_state.instrument_b],
                now_ns=now_ns,
            )
            if added:
                await self._evaluate_cross_asset_trigger(ca_state, now_ns)

    async def _evaluate_cross_asset_trigger(
        self, ca_state: CrossAssetState, now_ns: int
    ) -> None:
        """
        Evaluate cross-asset correlation regime trigger for one instrument pair.
        Only fires if enough samples have been collected and circuit breaker is open.
        """
        if ca_state.circuit_breaker.is_locked():
            return

        ca_trigger = detect_cross_asset_decoupling(
            samples=list(ca_state.samples),
            instrument_a=ca_state.instrument_a,
            instrument_b=ca_state.instrument_b,
        )

        if ca_trigger.triggered:
            ca_state.circuit_breaker.trip()
            await self._baseline.log_event(
                instrument=f"{ca_state.instrument_a}_{ca_state.instrument_b}",
                event_type=f"cross_asset_{ca_trigger.regime}",
                details={
                    "correlation": ca_trigger.correlation,
                    "ofi_z_a": ca_trigger.ofi_z_a,
                    "ofi_z_b": ca_trigger.ofi_z_b,
                    "n_windows": ca_trigger.n_windows,
                    "regime": ca_trigger.regime,
                },
            )
            await self._invoke_cross_asset_dag(ca_trigger)

    async def _invoke_cross_asset_dag(self, ca_trigger: CrossAssetTrigger) -> None:
        """
        Fire Agent 060 with the cross-asset trigger data (non-blocking task).
        Agent 060 can take 30-120 seconds — the ingest loop must keep running.
        """
        logger.info(
            "[Pipeline] Invoking Agent 060 for %s regime: %s↔%s (non-blocking).",
            ca_trigger.regime, ca_trigger.instrument_a, ca_trigger.instrument_b,
        )

        async def _run_flow():
            try:
                from foundry.dag.cross_asset_flow import cross_asset_flow
                result = await cross_asset_flow(ca_trigger.to_agent_input())
                logger.info("[Pipeline] cross_asset_flow result: %s", result)
            except ImportError:
                logger.warning(
                    "[Pipeline] cross_asset_flow not yet built. "
                    "Agent 060 trigger captured but DAG not invoked. "
                    "Build foundry/dag/cross_asset_flow.py."
                )
            except Exception as exc:
                logger.error(
                    "[Pipeline] cross_asset_flow failed: %s", exc, exc_info=True
                )

        asyncio.create_task(_run_flow())

    async def _invoke_dag(self, trigger, data_slice: MarketDataSlice) -> None:
        """
        Fire the panic_liquidity_flow DAG with the triggering data slice.

        Runs in a separate task so the ingest loop is not blocked waiting
        for Agent 089 to complete its multi-iteration reasoning loop.
        Agent 089 can take 30-120 seconds. The stream must keep processing.
        """
        logger.info(
            "[Pipeline] Invoking panic_liquidity_flow for %s (non-blocking).",
            trigger.instrument,
        )

        async def _run_flow():
            try:
                from foundry.dag.flows import panic_liquidity_flow
                result = await panic_liquidity_flow(data_slice)
                logger.info(
                    "[Pipeline] panic_liquidity_flow completed: %s", result
                )
            except Exception as exc:
                logger.error(
                    "[Pipeline] panic_liquidity_flow failed for %s: %s",
                    trigger.instrument, exc, exc_info=True,
                )

        asyncio.create_task(_run_flow())


# ---------------------------------------------------------------------------
# Startup orchestration
# ---------------------------------------------------------------------------


async def _load_or_bootstrap_baseline(
    instrument: str,
    dataset: str,
    baseline_manager: BaselineManager,
    api_key: str,
) -> Tuple[float, float, List[float]]:
    """
    Load baseline from Postgres if fresh, otherwise run historical bootstrap.

    This is the cold-start gate: no live stream opens until all instruments
    have valid baselines. A stale or missing baseline triggers a fresh pull.
    """
    stale = await baseline_manager.is_stale(instrument)
    if not stale:
        data = await baseline_manager.load_baseline(instrument)
        if data is not None:
            age_hours = (
                datetime.now(timezone.utc) - data["computed_at"]
            ).total_seconds() / 3600
            logger.info(
                "[Startup] Loaded baseline for %s from Postgres "
                "(age=%.1fh, mean=%.4f, std=%.4f).",
                instrument, age_hours, data["ofi_mean"], data["ofi_std"],
            )
            return data["ofi_mean"], data["ofi_std"], data["spread_percentiles"]

    logger.info(
        "[Startup] Baseline for %s is missing or stale. Running historical bootstrap...",
        instrument,
    )
    bootstrap = HistoricalBootstrap(api_key=api_key, baseline_manager=baseline_manager)
    return await bootstrap.run(instrument=instrument, dataset=dataset)


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stdout,
    )

    # Validate required config
    if not DATABENTO_API_KEY:
        logger.error("DATABENTO_API_KEY not set. Cannot connect to Databento.")
        sys.exit(1)
    if not INSTRUMENTS:
        logger.error("INSTRUMENTS not set. Specify at least one symbol, e.g. ES.c.0")
        sys.exit(1)

    logger.info("Formula Foundry — Databento MBO Ingest Pipeline")
    logger.info("Mode: %s | Dataset: %s | Instruments: %s",
                "LIVE" if LIVE_MODE else "HISTORICAL REPLAY",
                DATABENTO_DATASET, INSTRUMENTS)

    # Connect to Postgres
    baseline_manager = BaselineManager(dsn=POSTGRES_DSN)
    await baseline_manager.connect()

    # Load or bootstrap baselines for all instruments (sequentially — API rate limits)
    instrument_states: Dict[str, InstrumentState] = {}
    for instrument in INSTRUMENTS:
        ofi_mean, ofi_std, spread_pctiles = await _load_or_bootstrap_baseline(
            instrument=instrument,
            dataset=DATABENTO_DATASET,
            baseline_manager=baseline_manager,
            api_key=DATABENTO_API_KEY,
        )
        # Load entropy baseline (built alongside OFI in the unified mbp-10 bootstrap)
        entropy_bl = await baseline_manager.load_entropy_baseline(instrument)
        if entropy_bl is None:
            logger.warning(
                "[Startup] No entropy baseline for %s. "
                "Entropy trigger disabled until bootstrap completes. "
                "This resolves automatically on the next bootstrap run.",
                instrument,
            )

        instrument_states[instrument] = InstrumentState(
            instrument=instrument,
            baseline_ofi_mean=ofi_mean,
            baseline_ofi_std=ofi_std,
            spread_percentiles=spread_pctiles,
            circuit_breaker=CircuitBreaker(lockout_seconds=CIRCUIT_BREAKER_SECS),
            entropy_baseline=entropy_bl,
            entropy_circuit_breaker=CircuitBreaker(lockout_seconds=CIRCUIT_BREAKER_SECS),
        )
        logger.info(
            "[Startup] %s ready: ofi_mean=%.4f ofi_std=%.4f "
            "spread=%d samples | entropy=%s",
            instrument, ofi_mean, ofi_std, len(spread_pctiles),
            f"{len(entropy_bl.entropy_samples)} samples" if entropy_bl else "NOT LOADED",
        )

    # Build cross-asset pair states for instruments that share a pair
    # CROSS_ASSET_PAIRS env: comma-separated pairs, each pair is "A:B"
    # Default: "ES.c.0:ZN.c.0" if both instruments are being monitored
    cross_asset_states: Dict[FrozenSet, CrossAssetState] = {}
    pairs_raw = os.environ.get("CROSS_ASSET_PAIRS", "ES.c.0:ZN.c.0")
    for pair_str in pairs_raw.split(","):
        pair_str = pair_str.strip()
        if ":" not in pair_str:
            continue
        inst_a, inst_b = [s.strip() for s in pair_str.split(":", 1)]
        if inst_a not in instrument_states or inst_b not in instrument_states:
            logger.warning(
                "[Startup] Cross-asset pair %s↔%s skipped: "
                "one or both instruments not in INSTRUMENTS list.",
                inst_a, inst_b,
            )
            continue
        pair_key = frozenset({inst_a, inst_b})
        cross_asset_states[pair_key] = CrossAssetState(
            instrument_a=inst_a,
            instrument_b=inst_b,
            circuit_breaker=CircuitBreaker(lockout_seconds=CIRCUIT_BREAKER_SECS),
        )
        logger.info("[Startup] Cross-asset pair registered: %s ↔ %s", inst_a, inst_b)

    # Build streamer
    if LIVE_MODE:
        streamer = LiveMBOStreamer(
            api_key=DATABENTO_API_KEY,
            dataset=DATABENTO_DATASET,
            instruments=INSTRUMENTS,
        )
    else:
        replay_start = os.environ.get("REPLAY_START")
        replay_end = os.environ.get("REPLAY_END")
        dbn_file = os.environ.get("REPLAY_DBN_FILE")
        streamer = HistoricalMBOStreamer(
            api_key=DATABENTO_API_KEY,
            dataset=DATABENTO_DATASET,
            instruments=INSTRUMENTS,
            start=replay_start,
            end=replay_end,
            dbn_file_path=dbn_file,
        )

    # Run pipeline
    pipeline = IngestPipeline(
        streamer=streamer,
        baseline_manager=baseline_manager,
        instrument_states=instrument_states,
        cross_asset_states=cross_asset_states,
    )
    try:
        await pipeline.run()
    except KeyboardInterrupt:
        logger.info("[IngestPipeline] Interrupted by user.")
    finally:
        await baseline_manager.close()
        logger.info("[IngestPipeline] Shutdown complete.")


if __name__ == "__main__":
    asyncio.run(main())
