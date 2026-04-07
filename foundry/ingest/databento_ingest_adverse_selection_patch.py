# databento_ingest_adverse_selection_patch.py
#
# Surgical integration patch for foundry/ingest/databento_ingest.py
#
# Adds the 30-minute bar accumulator, LambdaBaseline, and adverse selection
# trigger evaluation to the existing IngestPipeline without modifying any
# existing logic. Every change is additive except for two small insertions
# inside _process_event() and the InstrumentState constructor call in main().
#
# Verification after applying all changes:
#   python3 -m py_compile foundry/ingest/databento_ingest.py
#
# Sections:
#   A. New imports
#   B. New constant (BAR_DURATION_NS)
#   C. New fields on InstrumentState
#   D. New method on InstrumentState: try_close_bar()
#   E. Two insertions in IngestPipeline._process_event()
#   F. New method: IngestPipeline._evaluate_adverse_selection_bar()
#   G. New method: IngestPipeline._invoke_adverse_selection_dag()
#   H. InstrumentState constructor call in main(): add lambda_baseline field
#
# ============================================================
# A. IMPORTS — add alongside existing trigger imports
# ============================================================
#
# In the existing block:
#   from foundry.dag.triggers import (
#       MBOEvent,
#       MarketDataSlice,
#       OrderBookSnapshot,
#       CrossAssetWindowSample,
#       CrossAssetTrigger,
#       MBP10Level,
#       MBP10Snapshot,
#       EntropyBaseline,
#       EntropyTrigger,
#       detect_panic_fingerprint,
#       detect_cross_asset_decoupling,
#       detect_entropy_collapse,
#   )
#
# ADD to that same block:
#       AdverseSelectionTrigger,
#       LambdaBaseline,
#       detect_adverse_selection,
#
# ============================================================
# B. NEW CONSTANT — add alongside OFI_SIGMA_THRESHOLD, etc.
# ============================================================
#
# ADD after the existing threshold constants:
#
#   # Duration of one Kyle's Lambda accumulation bar: 30 minutes in nanoseconds.
#   # Lambda is an OLS estimate over this window; shorter windows increase noise,
#   # longer windows miss intraday regime shifts.
#   BAR_DURATION_NS: int = int(30 * 60 * 1e9)
#
# ============================================================
# C. NEW FIELDS ON InstrumentState
# ============================================================
#
# The existing InstrumentState dataclass ends with:
#   entropy_circuit_breaker: CircuitBreaker = field(default_factory=CircuitBreaker)
#   _snapshot_count: int = field(default=0, repr=False)
#
# ADD these fields immediately after _snapshot_count:
#
#   # Adverse selection / Kyle's Lambda state
#   # lambda_baseline starts empty; is_valid gate prevents trigger until
#   # LAMBDA_MIN_BARS (10) completed bars have been observed.
#   # No bootstrap required — accumulates from live/replay bars automatically.
#   lambda_baseline: LambdaBaseline = field(
#       default_factory=lambda: LambdaBaseline(instrument=""),  # instrument set in __post_init__
#       repr=False,
#   )
#   lambda_circuit_breaker: CircuitBreaker = field(
#       default_factory=CircuitBreaker, repr=False
#   )
#   # 30-minute bar accumulator — reset on each bar close
#   _bar_open_ns: int = field(default=0, repr=False)
#   _bar_open_mid: float = field(default=0.0, repr=False)
#   _bar_signed_vol: float = field(default=0.0, repr=False)
#   # Rolling series of completed bars for OLS (last 3×LAMBDA_MIN_BARS bars)
#   _bar_sv_series: deque = field(
#       default_factory=lambda: deque(maxlen=30), repr=False
#   )
#   _bar_dp_series: deque = field(
#       default_factory=lambda: deque(maxlen=30), repr=False
#   )
#
# ALSO ADD a __post_init__ method to fix the lambda_baseline.instrument field,
# since dataclass default_factory cannot reference self.instrument:
#
#   def __post_init__(self) -> None:
#       if not self.lambda_baseline.instrument:
#           self.lambda_baseline.instrument = self.instrument
#
# ============================================================
# D. NEW METHOD ON InstrumentState: try_close_bar()
# ============================================================
#
# Add this method to the InstrumentState class, after trim_to_window():
#
#   def try_close_bar(
#       self, now_ns: int, current_mid: float
#   ) -> Optional[tuple]:
#       """
#       Check if the current 30-minute bar has elapsed. If so, close it.
#
#       Returns (signed_vol, delta_mid) if the bar is complete and has
#       a valid mid price at both open and close, otherwise None.
#
#       Resets bar accumulator state for the next bar on close.
#
#       Must be called on each MBP10Snapshot arrival (where mid price
#       is reliably available from best bid/ask).
#       """
#       if self._bar_open_ns == 0:
#           # First bar — initialize rather than close
#           self._bar_open_ns = now_ns
#           self._bar_open_mid = current_mid
#           self._bar_signed_vol = 0.0
#           return None
#
#       if now_ns - self._bar_open_ns < BAR_DURATION_NS:
#           # Bar still open — accumulate
#           return None
#
#       # Bar complete
#       if self._bar_open_mid <= 0.0 or current_mid <= 0.0:
#           # Degenerate mid price — skip this bar
#           self._bar_open_ns = now_ns
#           self._bar_open_mid = current_mid
#           self._bar_signed_vol = 0.0
#           return None
#
#       signed_vol = self._bar_signed_vol
#       delta_mid  = current_mid - self._bar_open_mid
#
#       # Record in rolling series
#       self._bar_sv_series.append(signed_vol)
#       self._bar_dp_series.append(delta_mid)
#
#       # Reset for next bar
#       self._bar_open_ns    = now_ns
#       self._bar_open_mid   = current_mid
#       self._bar_signed_vol = 0.0
#
#       return (signed_vol, delta_mid)
#
# ============================================================
# E. INSERTIONS IN IngestPipeline._process_event()
# ============================================================
#
# The existing _process_event() body handles three cases:
#   if isinstance(event_or_snapshot, MBOEvent): ...
#   elif isinstance(event_or_snapshot, MBP10Snapshot): ...
#   else: ...
#
# INSERTION 1 — Inside the MBOEvent branch, after:
#   state.event_buffer.append(event_or_snapshot)
#
# ADD (accumulate signed volume for the current 30-min bar):
#
#   # Accumulate signed volume using the same convention as OFI:
#   #   side == 'B' → seller hit resting bid → selling → negative contribution
#   #   side == 'A' → buyer hit resting ask  → buying  → positive contribution
#   if event_or_snapshot.is_aggressive:
#       if event_or_snapshot.side == "B":
#           state._bar_signed_vol -= event_or_snapshot.size
#       else:
#           state._bar_signed_vol += event_or_snapshot.size
#
# INSERTION 2 — Inside the MBP10Snapshot branch, after the tob extraction
# and snapshot_buffer.append(tob) call, and after _snapshot_count increment,
# but BEFORE the entropy evaluation block. Insert:
#
#   # 30-minute bar close check (adverse selection trigger)
#   if tob is not None:
#       bar_result = state.try_close_bar(now_ns, tob.mid_price)
#       if bar_result is not None:
#           await self._evaluate_adverse_selection_bar(state, now_ns)
#
# The full MBP10Snapshot branch after both insertions should look like:
#
#   elif isinstance(event_or_snapshot, MBP10Snapshot):
#       tob = None
#       if event_or_snapshot.bids and event_or_snapshot.asks:
#           tob = OrderBookSnapshot(
#               timestamp_ns=now_ns,
#               instrument=instrument,
#               best_bid=event_or_snapshot.bids[0].price,
#               best_ask=event_or_snapshot.asks[0].price,
#               best_bid_size=event_or_snapshot.bids[0].size,
#               best_ask_size=event_or_snapshot.asks[0].size,
#           )
#           state.snapshot_buffer.append(tob)
#
#       # [INSERTION 2 — bar close check]
#       if tob is not None:
#           bar_result = state.try_close_bar(now_ns, tob.mid_price)
#           if bar_result is not None:
#               await self._evaluate_adverse_selection_bar(state, now_ns)
#
#       state._snapshot_count += 1
#       if state._snapshot_count % self._ENTROPY_EVAL_INTERVAL == 0:
#           await self._evaluate_entropy_trigger(state, event_or_snapshot)
#
# NOTE: bar_result is not used directly — try_close_bar() already appended
# the (sv, dp) values to state._bar_sv_series and state._bar_dp_series.
# _evaluate_adverse_selection_bar() reads those series from state.
#
# ============================================================
# F. NEW METHOD: IngestPipeline._evaluate_adverse_selection_bar()
# ============================================================
#
# Add this method to IngestPipeline, alongside _evaluate_entropy_trigger():
#
#   async def _evaluate_adverse_selection_bar(
#       self, state: InstrumentState, now_ns: int
#   ) -> None:
#       """
#       Evaluate Kyle's Lambda adverse selection trigger at 30-minute bar close.
#
#       Only fires when:
#         - Lambda baseline has sufficient history (>= LAMBDA_MIN_BARS bars)
#         - Lambda percentile >= 0.95 (top 5% of historical lambda)
#         - Regression R² >= 0.40 (informed flow, not noise)
#
#       Hard suppressed (no dispatch) when R² < 0.20 (noise floor).
#       Circuit breaker prevents re-firing within CIRCUIT_BREAKER_SECS.
#       """
#       if state.lambda_circuit_breaker.is_locked():
#           logger.debug(
#               "[Pipeline] %s lambda circuit breaker locked (%.0fs remaining).",
#               state.instrument, state.lambda_circuit_breaker.remaining_seconds(),
#           )
#           return
#
#       sv_series = list(state._bar_sv_series)
#       dp_series = list(state._bar_dp_series)
#
#       if len(sv_series) < 2:
#           # Not enough bars for OLS — wait silently
#           return
#
#       window_start = datetime.fromtimestamp(
#           (now_ns - BAR_DURATION_NS) / 1e9, tz=timezone.utc
#       )
#       window_end = datetime.fromtimestamp(now_ns / 1e9, tz=timezone.utc)
#
#       trigger = detect_adverse_selection(
#           instrument=state.instrument,
#           window_start=window_start,
#           window_end=window_end,
#           signed_volume_series=sv_series,
#           price_change_series=dp_series,
#           baseline=state.lambda_baseline,
#       )
#
#       if trigger.suppressed:
#           logger.debug(
#               "[Pipeline] Adverse selection SUPPRESSED %s: R²=%.3f below noise floor.",
#               state.instrument, trigger.regression_r2,
#           )
#           return
#
#       if trigger.triggered:
#           logger.warning(
#               "[Pipeline] ADVERSE SELECTION REGIME: %s | λ=%.6f (pctile=%.3f) | R²=%.3f",
#               state.instrument,
#               trigger.lambda_coefficient,
#               trigger.lambda_percentile,
#               trigger.regression_r2,
#           )
#           state.lambda_circuit_breaker.trip()
#           await self._baseline.log_event(
#               instrument=state.instrument,
#               event_type="adverse_selection_regime",
#               details={
#                   "lambda_coefficient": trigger.lambda_coefficient,
#                   "lambda_percentile":  trigger.lambda_percentile,
#                   "regression_r2":      trigger.regression_r2,
#                   "n_bars":             len(sv_series),
#               },
#           )
#           await self._invoke_adverse_selection_dag(trigger)
#
# ============================================================
# G. NEW METHOD: IngestPipeline._invoke_adverse_selection_dag()
# ============================================================
#
# Add this method alongside _invoke_entropy_dag() and _invoke_cross_asset_dag():
#
#   async def _invoke_adverse_selection_dag(
#       self, trigger: AdverseSelectionTrigger
#   ) -> None:
#       """
#       Fire adverse_selection_flow for Agent 050 (non-blocking task).
#
#       Agent 050 can take 30-120 seconds (LLM reasoning loop).
#       The ingest pipeline must not block — same pattern as entropy and
#       cross-asset flows.
#       """
#       logger.info(
#           "[Pipeline] Invoking adverse_selection_flow for %s | "
#           "λ=%.6f (pctile=%.3f) | R²=%.3f (non-blocking).",
#           trigger.instrument,
#           trigger.lambda_coefficient,
#           trigger.lambda_percentile,
#           trigger.regression_r2,
#       )
#
#       async def _run_flow():
#           try:
#               from foundry.dag.adverse_selection_flow import adverse_selection_flow
#               result = await adverse_selection_flow(trigger.to_agent_input())
#               logger.info("[Pipeline] adverse_selection_flow result: %s", result)
#           except ImportError:
#               logger.warning(
#                   "[Pipeline] adverse_selection_flow not yet importable. "
#                   "Trigger captured but DAG not invoked."
#               )
#           except Exception as exc:
#               logger.error(
#                   "[Pipeline] adverse_selection_flow failed: %s", exc, exc_info=True
#               )
#
#       asyncio.create_task(_run_flow())
#
# ============================================================
# H. InstrumentState CONSTRUCTOR CALL in main()
# ============================================================
#
# The existing constructor call in main() (inside the for instrument loop):
#
#   instrument_states[instrument] = InstrumentState(
#       instrument=instrument,
#       baseline_ofi_mean=ofi_mean,
#       baseline_ofi_std=ofi_std,
#       spread_percentiles=spread_pctiles,
#       circuit_breaker=CircuitBreaker(lockout_seconds=CIRCUIT_BREAKER_SECS),
#       entropy_baseline=entropy_bl,
#       entropy_circuit_breaker=CircuitBreaker(lockout_seconds=CIRCUIT_BREAKER_SECS),
#   )
#
# REPLACE WITH (add lambda fields; __post_init__ will set lambda_baseline.instrument):
#
#   instrument_states[instrument] = InstrumentState(
#       instrument=instrument,
#       baseline_ofi_mean=ofi_mean,
#       baseline_ofi_std=ofi_std,
#       spread_percentiles=spread_pctiles,
#       circuit_breaker=CircuitBreaker(lockout_seconds=CIRCUIT_BREAKER_SECS),
#       entropy_baseline=entropy_bl,
#       entropy_circuit_breaker=CircuitBreaker(lockout_seconds=CIRCUIT_BREAKER_SECS),
#       lambda_circuit_breaker=CircuitBreaker(lockout_seconds=CIRCUIT_BREAKER_SECS),
#       # lambda_baseline instrument is set by __post_init__ from self.instrument
#   )
#
# NOTE: No bootstrap load for lambda_baseline — it accumulates from the first
# 30-minute bars of each session. The is_valid gate (LAMBDA_MIN_BARS=10 bars,
# ~5 hours of data) prevents premature trigger firing during cold start.
# A Postgres persistence layer can be added in a future sprint alongside the
# existing entropy_baseline save/load pattern.
#
# ============================================================
# STARTUP LOG UPDATE — optional but recommended
# ============================================================
#
# Update the [Startup] log line to report lambda_baseline status:
#
# BEFORE:
#   logger.info(
#       "[Startup] %s ready: ofi_mean=%.4f ofi_std=%.4f "
#       "spread=%d samples | entropy=%s",
#       instrument, ofi_mean, ofi_std, len(spread_pctiles),
#       f"{len(entropy_bl.entropy_samples)} samples" if entropy_bl else "NOT LOADED",
#   )
#
# AFTER:
#   logger.info(
#       "[Startup] %s ready: ofi_mean=%.4f ofi_std=%.4f "
#       "spread=%d samples | entropy=%s | lambda=accumulating (cold start)",
#       instrument, ofi_mean, ofi_std, len(spread_pctiles),
#       f"{len(entropy_bl.entropy_samples)} samples" if entropy_bl else "NOT LOADED",
#   )
#
# ============================================================
# COMPLETE CHANGE SUMMARY
# ============================================================
#
# Files modified:    1 (databento_ingest.py)
# Files unchanged:   triggers.py, agent_050.py, adverse_selection_flow.py
#                    synthesis_flow.py (routing patch applied separately)
#
# Changes in databento_ingest.py:
#   +3  imports (AdverseSelectionTrigger, LambdaBaseline, detect_adverse_selection)
#   +1  constant (BAR_DURATION_NS)
#   +8  fields on InstrumentState
#   +1  __post_init__ method on InstrumentState
#   +20 try_close_bar() method on InstrumentState
#   +6  lines in _process_event() (2 insertions)
#   +35 _evaluate_adverse_selection_bar() method on IngestPipeline
#   +25 _invoke_adverse_selection_dag() method on IngestPipeline
#   +1  field in InstrumentState constructor call in main()
#   +1  startup log line update
#
# No existing methods are modified except _process_event() (2 small insertions).
# All new logic is additive. The OFI, entropy, and cross-asset pipelines
# are entirely unaffected.
#
# ============================================================
# POST-APPLY VERIFICATION SEQUENCE
# ============================================================
#
# 1. python3 -m py_compile foundry/ingest/databento_ingest.py
#    → must exit 0
#
# 2. python3 -c "
#    from foundry.ingest.databento_ingest import InstrumentState, CircuitBreaker
#    s = InstrumentState(instrument='ES.c.0')
#    assert s.lambda_baseline.instrument == 'ES.c.0', 'post_init failed'
#    assert not s.lambda_baseline.is_valid, 'baseline should be invalid on cold start'
#    print('InstrumentState invariants: PASS')
#    "
#    → must print PASS
#
# 3. python3 -c "
#    from foundry.dag.triggers_adverse_selection import (
#        LambdaBaseline, detect_adverse_selection
#    )
#    from datetime import datetime, timezone
#    bl = LambdaBaseline(instrument='ES.c.0')
#    for i in range(12):
#        bl.add_bar(float(i) * 0.0001, 0.5)
#    sv = [float(i * 100 - 500) for i in range(12)]
#    dp = [v * 0.0003 for v in sv]
#    t = detect_adverse_selection(
#        'ES.c.0',
#        datetime.now(timezone.utc),
#        datetime.now(timezone.utc),
#        sv, dp, bl,
#        lambda_percentile_threshold=0.0,  # force trigger for test
#        r2_trigger_threshold=0.0,
#    )
#    assert t.triggered, f'expected trigger, got: {t}'
#    print('detect_adverse_selection smoke test: PASS')
#    "
#    → must print PASS
#
# ============================================================
