"""
Agent 003 — Evolution Trigger Monitor

Layer 0 — Orchestration. NOT a BaseAgent subclass. No Claude calls.

Mission: Poll the Blackboard for unresolved REJECTED_ISOMORPHISM edges and
         dispatch the correct Evolutionary Gardener for each.

Resolution check (Option B — Lineage Check):
    A rejection is considered resolved if any formula exists in the Blackboard
    tagged with "resolves_<rejection_id[:8]>". This tag is written by Evolutionary
    Gardeners (e.g. Agent 201) when they successfully produce Formula C.

    Option B is preferred over a "resolved" flag on the edge because:
      - It does not mutate historical records (REJECTED_ISOMORPHISM edges are
        a snapshot of Agent 105's reasoning at a point in time).
      - The graph's resolution state is derived from the PRESENCE of Formula C,
        not from a mutable flag that could be incorrectly set.
      - It composes naturally with the Librarian: once Formula C exists,
        the Librarian pairs it with the parent formulas autonomously.

Routing:
    failure_mode field on the REJECTED_ISOMORPHISM edge (set by Agent 105)
    maps deterministically to the correct Evolutionary Gardener flow.
    No LLM interpretation. No keyword parsing. One dict lookup.

    EVOLUTIONARY_ROUTERS maps RejectionFailureMode → coroutine.
    Unbuilt flows use _stub_flow which logs and returns without crashing.
    Replace the stub with the real flow when Agent 202/203/204 is built.

In-flight protection:
    _in_flight set tracks rejection_ids currently being processed.
    Prevents double-dispatch if a Gardener run outlasts the poll interval.

Dead letter queue:
    UNCLASSIFIED rejections are logged and skipped. No Gardener handles them.
    A future human-review queue (webhook, Slack alert, etc.) can be wired
    into _route_to_human_queue() without changing any other logic.

Environment variables:
    BLACKBOARD_API_URL      — default: http://localhost:8000
    EVOLUTION_POLL_SECONDS  — polling interval (default: 60)
    EVOLUTION_MAX_CONCURRENT — max simultaneous Gardener flows (default: 3)

To run as a daemon:
    PYTHONPATH=. python -m foundry.agents.orchestration.agent_003

To run one poll cycle (for testing):
    PYTHONPATH=. python -m foundry.agents.orchestration.agent_003 --once
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from typing import Callable, Dict, Optional, Set

import httpx

from foundry.core.schema.isomorphism import RejectionFailureMode

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BLACKBOARD_API_URL     = os.environ.get("BLACKBOARD_API_URL", "http://localhost:8000")
EVOLUTION_POLL_SECONDS = int(os.environ.get("EVOLUTION_POLL_SECONDS", "60"))
EVOLUTION_MAX_CONCURRENT = int(os.environ.get("EVOLUTION_MAX_CONCURRENT", "3"))


# ---------------------------------------------------------------------------
# Stub flow for unbuilt Gardeners
# ---------------------------------------------------------------------------


async def _stub_flow(rejection_id: str, agent_label: str) -> str:
    """
    Placeholder for Evolutionary Gardener flows not yet implemented.

    Logs a warning and returns without crashing. Replace this in
    EVOLUTIONARY_ROUTERS when the real flow is built.

    Returns a "not_built" result code so the caller can distinguish
    stubs from real flow outcomes.
    """
    logger.warning(
        "[Agent003] %s not yet built. Rejection %s queued for future implementation. "
        "Build the flow and update EVOLUTIONARY_ROUTERS.",
        agent_label, rejection_id[:8],
    )
    return f"not_built:{agent_label}:{rejection_id[:8]}"


# All four Gardeners (202-205) are live — no stubs remaining.


# ---------------------------------------------------------------------------
# Routing table
# ---------------------------------------------------------------------------
#
# Maps RejectionFailureMode → async callable(rejection_id: str) -> str
#
# To wire a new Gardener:
#   1. Import its flow function
#   2. Replace the stub entry with the real function
#   3. No other code changes needed
#
# Import is deferred inside the function to avoid circular import issues
# at module load time (evolutionary_flow imports from agents which imports
# from schema which imports from here via isomorphism.py).

def _build_routing_table() -> Dict[RejectionFailureMode, Optional[Callable]]:
    """
    Build the routing table with lazy imports of all evolutionary flows.
    Called once at daemon startup, not at module import time.

    All five failure modes are live. UNCLASSIFIED routes to human review.
    """
    from foundry.dag.evolutionary_flow import evolutionary_flow as _flow_201
    from foundry.dag.evolutionary_flow_202 import evolutionary_flow_202 as _flow_202
    from foundry.dag.evolutionary_flow_203 import evolutionary_flow_203 as _flow_203
    from foundry.dag.evolutionary_flow_204 import evolutionary_flow_204 as _flow_204
    from foundry.dag.evolutionary_flow_205 import evolutionary_flow_205 as _flow_205

    async def _run_agent_201(rejection_id: str) -> str:
        return await _flow_201(rejection_id=rejection_id)

    async def _run_agent_202(rejection_id: str) -> str:
        return await _flow_202(rejection_id)

    async def _run_agent_203(rejection_id: str) -> str:
        return await _flow_203(rejection_id)

    async def _run_agent_204(rejection_id: str) -> str:
        return await _flow_204(rejection_id)

    async def _run_agent_205(rejection_id: str) -> str:
        return await _flow_205(rejection_id)

    return {
        RejectionFailureMode.TEMPORAL_SCALE_MISMATCH:           _run_agent_201,
        RejectionFailureMode.DIMENSIONALITY_MISMATCH:           _run_agent_202,
        RejectionFailureMode.STOCHASTIC_DETERMINISTIC_MISMATCH: _run_agent_203,
        RejectionFailureMode.MICRO_MACRO_MISMATCH:              _run_agent_204,
        RejectionFailureMode.INFORMATION_GEOMETRY_MISMATCH:     _run_agent_205,
        RejectionFailureMode.UNCLASSIFIED:                      None,  # human review
    }


# ---------------------------------------------------------------------------
# Human review queue
# ---------------------------------------------------------------------------


async def _route_to_human_queue(rejection: Dict) -> None:
    """
    Handle UNCLASSIFIED rejections.

    Currently logs only. Wire a webhook, Slack alert, or Postgres queue
    here when human-review infrastructure is ready. The signature is
    intentionally side-effectful — it should never raise, only log.
    """
    logger.warning(
        "[Agent003] UNCLASSIFIED rejection %s (%s ↔ %s). "
        "No Gardener assigned. Human review required. "
        "Bridging concept: %s",
        rejection.get("rejection_id", "?")[:8],
        rejection.get("uuid_a", "?")[:8],
        rejection.get("uuid_b", "?")[:8],
        rejection.get("bridging_concept", "")[:120],
    )


# ---------------------------------------------------------------------------
# Evolution Trigger Monitor
# ---------------------------------------------------------------------------


class EvolutionTriggerMonitor:
    """
    Layer 0 polling daemon. Not a BaseAgent. No Claude.

    Poll cycle (every EVOLUTION_POLL_SECONDS):
      1. Fetch all REJECTED_ISOMORPHISM edges from Blackboard
      2. For each rejection:
         a. Skip if already in-flight (being processed right now)
         b. Skip if resolved (Formula C with resolves_<id> tag exists)
         c. Dispatch to the correct Gardener flow (non-blocking asyncio.Task)
    """

    def __init__(self, api_url: str = BLACKBOARD_API_URL) -> None:
        self._api_url = api_url.rstrip("/")
        self._in_flight: Set[str] = set()      # rejection_ids currently dispatched
        self._routers: Dict[RejectionFailureMode, Optional[Callable]] = {}
        self._semaphore = asyncio.Semaphore(EVOLUTION_MAX_CONCURRENT)
        self._poll_count = 0

    def _initialize_routers(self) -> None:
        """Called once before the first poll cycle."""
        self._routers = _build_routing_table()
        logger.info(
            "[Agent003] Routing table initialized: %d modes registered.",
            len(self._routers),
        )
        for mode, fn in self._routers.items():
            status = fn.__name__ if fn else "→ human_queue"
            logger.info("  %s → %s", mode.value, status)

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def run_forever(self) -> None:
        """Polling daemon. Runs until KeyboardInterrupt."""
        self._initialize_routers()
        logger.info(
            "[Agent003] Evolution Trigger Monitor started. "
            "Poll interval: %ds | Max concurrent: %d",
            EVOLUTION_POLL_SECONDS, EVOLUTION_MAX_CONCURRENT,
        )

        while True:
            try:
                await self._poll_cycle()
            except Exception as exc:
                # Never let a poll error crash the daemon
                logger.error("[Agent003] Poll cycle error: %s", exc, exc_info=True)

            await asyncio.sleep(EVOLUTION_POLL_SECONDS)

    async def run_once(self) -> None:
        """Single poll cycle for testing."""
        self._initialize_routers()
        logger.info("[Agent003] Running single poll cycle.")
        await self._poll_cycle()

    # ------------------------------------------------------------------
    # Poll cycle
    # ------------------------------------------------------------------

    async def _poll_cycle(self) -> None:
        self._poll_count += 1
        logger.debug("[Agent003] Poll cycle %d starting.", self._poll_count)

        rejections = await self._fetch_rejections()
        if not rejections:
            logger.debug("[Agent003] No rejection edges in graph.")
            return

        dispatched = 0
        skipped_in_flight = 0
        skipped_resolved = 0
        skipped_unclassified = 0

        for rejection in rejections:
            rejection_id = rejection.get("rejection_id", "")
            if not rejection_id:
                continue

            # In-flight check: already being processed by a running task
            if rejection_id in self._in_flight:
                skipped_in_flight += 1
                continue

            # Lineage check: Formula C already exists (Option B)
            if await self._is_resolved(rejection_id):
                logger.debug(
                    "[Agent003] Rejection %s already resolved (Formula C exists).",
                    rejection_id[:8],
                )
                skipped_resolved += 1
                continue

            # Route
            failure_mode = self._parse_failure_mode(rejection)

            if failure_mode == RejectionFailureMode.UNCLASSIFIED:
                await _route_to_human_queue(rejection)
                skipped_unclassified += 1
                continue

            flow_fn = self._routers.get(failure_mode)
            if flow_fn is None:
                await _route_to_human_queue(rejection)
                skipped_unclassified += 1
                continue

            # Dispatch as a non-blocking background task
            self._in_flight.add(rejection_id)
            asyncio.create_task(
                self._run_with_cleanup(rejection_id, failure_mode, flow_fn)
            )
            dispatched += 1
            logger.info(
                "[Agent003] Dispatched rejection %s (mode: %s) → %s",
                rejection_id[:8], failure_mode.value, flow_fn.__name__,
            )

        logger.info(
            "[Agent003] Poll cycle %d complete: %d dispatched | "
            "%d in-flight | %d resolved | %d unclassified",
            self._poll_count, dispatched,
            skipped_in_flight, skipped_resolved, skipped_unclassified,
        )

    # ------------------------------------------------------------------
    # Task execution with cleanup
    # ------------------------------------------------------------------

    async def _run_with_cleanup(
        self,
        rejection_id: str,
        failure_mode: RejectionFailureMode,
        flow_fn: Callable,
    ) -> None:
        """
        Run a Gardener flow under the concurrency semaphore.
        Always removes rejection_id from _in_flight on completion.
        """
        async with self._semaphore:
            try:
                result = await flow_fn(rejection_id=rejection_id)
                logger.info(
                    "[Agent003] Rejection %s (mode: %s) → flow result: %s",
                    rejection_id[:8], failure_mode.value, result,
                )
            except Exception as exc:
                logger.error(
                    "[Agent003] Flow failed for rejection %s (mode: %s): %s",
                    rejection_id[:8], failure_mode.value, exc, exc_info=True,
                )
            finally:
                self._in_flight.discard(rejection_id)

    # ------------------------------------------------------------------
    # Lineage check (Option B)
    # ------------------------------------------------------------------

    async def _is_resolved(self, rejection_id: str) -> bool:
        """
        Check whether a Gardener has already produced Formula C for this rejection.

        Evolutionary Gardeners tag their output with "resolves_<rejection_id[:8]>".
        Query the Blackboard for any formula carrying this tag. Empty result = unresolved.

        Uses rejection_id[:8] (first 8 chars of UUID4) matching the tag format
        written by agent_201.py's extract_formula(). UUID4 prefix collision
        probability is ~1 in 4 billion — acceptable for this use case.
        """
        tag = f"resolves_{rejection_id[:8]}"
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(
                    f"{self._api_url}/v1/formulas",
                    params={"tag": tag},
                )
                if resp.status_code == 400:
                    # API returns 400 when no results (no formulas with this tag)
                    return False
                resp.raise_for_status()
                formulas = resp.json()
                return len(formulas) > 0
        except httpx.RequestError as exc:
            logger.error(
                "[Agent003] Lineage check failed for %s: %s. Treating as unresolved.",
                rejection_id[:8], exc,
            )
            # Fail open: if we can't check, treat as unresolved
            # Better to re-dispatch than to silently skip
            return False

    # ------------------------------------------------------------------
    # Blackboard queries
    # ------------------------------------------------------------------

    async def _fetch_rejections(self) -> list:
        """Fetch all REJECTED_ISOMORPHISM edges from the Blackboard."""
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.get(f"{self._api_url}/v1/rejections")
                resp.raise_for_status()
                return resp.json()
        except httpx.RequestError as exc:
            logger.error("[Agent003] Blackboard unreachable: %s", exc)
            return []

    # ------------------------------------------------------------------
    # Failure mode parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_failure_mode(rejection: dict) -> RejectionFailureMode:
        """
        Parse failure_mode from a rejection record.

        Handles three cases:
          1. Valid enum value (from updated Agent 105)         → correct Gardener
          2. None / missing (from pre-cascade seed records)    → UNCLASSIFIED
          3. Unknown string (model drift, future enum values)  → UNCLASSIFIED + warning
        """
        raw = rejection.get("failure_mode")
        if raw is None:
            # Pre-cascade record: no failure_mode stored on the Neo4j edge
            logger.debug(
                "[Agent003] Rejection %s has no failure_mode (pre-cascade record). "
                "Treating as UNCLASSIFIED.",
                rejection.get("rejection_id", "?")[:8],
            )
            return RejectionFailureMode.UNCLASSIFIED

        try:
            return RejectionFailureMode(raw)
        except ValueError:
            logger.warning(
                "[Agent003] Unknown failure_mode '%s' on rejection %s. "
                "Treating as UNCLASSIFIED. Add to RejectionFailureMode enum if legitimate.",
                raw, rejection.get("rejection_id", "?")[:8],
            )
            return RejectionFailureMode.UNCLASSIFIED


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stdout,
    )

    monitor = EvolutionTriggerMonitor(api_url=BLACKBOARD_API_URL)

    if "--once" in sys.argv:
        await monitor.run_once()
    else:
        try:
            await monitor.run_forever()
        except KeyboardInterrupt:
            logger.info("[Agent003] Shutdown by user.")


if __name__ == "__main__":
    asyncio.run(main())
