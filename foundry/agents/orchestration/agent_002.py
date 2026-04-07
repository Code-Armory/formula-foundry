"""
Agent 002 — Synthesis Trigger Monitor

Layer 0 — Orchestration. NOT a BaseAgent subclass. No Claude calls.

Mission: Watch the Blackboard for new SYNTACTICALLY_CORRECT or FORMALLY_VERIFIED
         formulas and trigger synthesis_flow when new ones arrive.

This is the last piece of the autonomous loop:

  databento_ingest.py → Agent 089 → Formula A (syntactically_correct)
         ↓
  [Agent 002 detects Formula A]
         ↓
  synthesis_flow() → Librarian pairs A+B → Agent 105 synthesizes
         ↓
  Agent 105 rejects → REJECTED_ISOMORPHISM
         ↓
  Agent 003 detects rejection → Agent 201 builds Formula C
         ↓
  [Agent 002 detects Formula C]
         ↓
  synthesis_flow() → Librarian pairs C+A, C+B → Agent 105 synthesizes
         ↓
  Agent 151 formally verifies → IP Library

Without Agent 002, every synthesis_flow invocation is manual. With it,
the machine runs itself from Databento tick to formally verified IP.

Design:
  UUID-set tracking (not count-based):
    On startup: load all current eligible formula UUIDs as the "seen" baseline.
    On each poll: fetch eligible formulas, compare UUIDs to seen set.
    New UUID detected → fire synthesis_flow() once → add all new UUIDs to seen set.

  "New" means: a UUID present in the Blackboard that was not present on the
  previous poll cycle. This is precise and never double-fires on the same formula.

  If N new formulas arrive in one poll cycle, synthesis_flow fires exactly once.
  The Librarian selects the best pair. On the next poll, if unprocessed pairs
  remain, synthesis_flow fires again.

  synthesis_flow is always called with no UUID override (uuid_a=None, uuid_b=None).
  The Librarian autonomously selects the pair. This preserves the strict separation
  between orchestration (which formulas exist) and curation (which pair to synthesize).

In-flight protection:
  If a synthesis_flow task is still running when the next poll fires,
  Agent 002 skips the dispatch. This prevents synthesis queue buildup
  when Agent 105 or Agent 151 takes longer than the poll interval.

Eligible statuses:
  syntactically_correct — passed SymPy gate, ready for synthesis pairing
  formally_verified     — passed Lean 4 gate (already in IP library, but new
                          formally_verified formulas are prime synthesis targets
                          and may open new pairing opportunities)

Environment variables:
  BLACKBOARD_API_URL      — default: http://localhost:8000
  SYNTHESIS_POLL_SECONDS  — polling interval (default: 60)

To run as a daemon:
  PYTHONPATH=. python -m foundry.agents.orchestration.agent_002

To run one poll cycle (for testing):
  PYTHONPATH=. python -m foundry.agents.orchestration.agent_002 --once
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from typing import Optional, Set

import httpx

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BLACKBOARD_API_URL     = os.environ.get("BLACKBOARD_API_URL", "http://localhost:8000")
SYNTHESIS_POLL_SECONDS = int(os.environ.get("SYNTHESIS_POLL_SECONDS", "60"))

# Statuses that make a formula eligible as a synthesis candidate
_ELIGIBLE_STATUSES = ("syntactically_correct", "formally_verified")


# ---------------------------------------------------------------------------
# Synthesis Trigger Monitor
# ---------------------------------------------------------------------------


class SynthesisTriggerMonitor:
    """
    Layer 0 polling daemon. Not a BaseAgent. No Claude.

    Startup:
      Load all current eligible formula UUIDs as the "seen" baseline.
      This prevents re-triggering synthesis for formulas that existed
      before Agent 002 started (they have presumably already been paired
      or are in an evaluated state in the graph).

    Poll cycle (every SYNTHESIS_POLL_SECONDS):
      1. Fetch all eligible formulas from Blackboard
      2. Diff against seen set to find new UUIDs
      3. If new formulas found AND no synthesis task in-flight:
         → fire synthesis_flow() as a background task
         → add new UUIDs to seen set
      4. If new formulas found but synthesis in-flight:
         → add new UUIDs to seen set (will be paired on next synthesis run)
         → log that new formulas are queued
    """

    def __init__(self, api_url: str = BLACKBOARD_API_URL) -> None:
        self._api_url = api_url.rstrip("/")
        self._seen_uuids: Set[str] = set()
        self._synthesis_in_flight: bool = False
        self._poll_count: int = 0
        self._total_dispatched: int = 0

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def run_forever(self) -> None:
        """Polling daemon. Runs until KeyboardInterrupt."""
        await self._initialize_seen_set()
        logger.info(
            "[Agent002] Synthesis Trigger Monitor started. "
            "Poll interval: %ds | Baseline: %d eligible formulas already seen.",
            SYNTHESIS_POLL_SECONDS, len(self._seen_uuids),
        )

        while True:
            try:
                await self._poll_cycle()
            except Exception as exc:
                logger.error("[Agent002] Poll cycle error: %s", exc, exc_info=True)

            await asyncio.sleep(SYNTHESIS_POLL_SECONDS)

    async def run_once(self) -> None:
        """Single poll cycle for testing. Does NOT load startup baseline."""
        logger.info(
            "[Agent002] Running single poll cycle (no baseline load — all formulas treated as new)."
        )
        await self._poll_cycle()

    # ------------------------------------------------------------------
    # Poll cycle
    # ------------------------------------------------------------------

    async def _poll_cycle(self) -> None:
        self._poll_count += 1
        logger.debug("[Agent002] Poll cycle %d starting.", self._poll_count)

        eligible = await self._fetch_eligible_formulas()
        if not eligible:
            logger.debug("[Agent002] No eligible formulas in Blackboard.")
            return

        current_uuids = {f["uuid"] for f in eligible}
        new_uuids = current_uuids - self._seen_uuids

        if not new_uuids:
            logger.debug(
                "[Agent002] No new formulas (total eligible: %d, all previously seen).",
                len(current_uuids),
            )
            return

        # Log new arrivals
        new_names = [
            next(
                (f.get("name") or f["uuid"][:8] for f in eligible if f["uuid"] == uid),
                uid[:8],
            )
            for uid in list(new_uuids)[:5]
        ]
        suffix = f" +{len(new_uuids) - 5} more" if len(new_uuids) > 5 else ""
        logger.info(
            "[Agent002] %d new formula(s) detected: %s%s",
            len(new_uuids), ", ".join(new_names), suffix,
        )

        # Always mark new UUIDs as seen, regardless of dispatch outcome.
        # This prevents re-triggering for the same formula on every poll
        # cycle if synthesis is slow or failing.
        self._seen_uuids.update(new_uuids)

        # Dispatch synthesis — but only if not already in-flight
        if self._synthesis_in_flight:
            logger.info(
                "[Agent002] Synthesis already in-flight. "
                "%d new formula(s) added to seen set — Librarian will "
                "consider them on the next synthesis run.",
                len(new_uuids),
            )
            return

        self._synthesis_in_flight = True
        asyncio.create_task(self._run_synthesis())
        self._total_dispatched += 1
        logger.info(
            "[Agent002] Dispatched synthesis_flow (dispatch #%d). "
            "Librarian will select the best pair.",
            self._total_dispatched,
        )

    # ------------------------------------------------------------------
    # Synthesis flow execution
    # ------------------------------------------------------------------

    async def _run_synthesis(self) -> None:
        """
        Run synthesis_flow() as a background task.
        Always clears _synthesis_in_flight on completion.

        synthesis_flow is called with no UUID override — the Librarian
        autonomously selects the highest-scoring unevaluated pair.

        Possible return values (from synthesis_flow):
          "no_pairs"                  — Librarian found nothing to pair
          "rejected:<id>"             — Agent 105 rejected the pair (new rejection
                                        edge written; Agent 003 will pick it up)
          "formally_verified:<uuid>"  — full pipeline success
          "proof_deferred:<uuid>"     — Agent 151 budget exhausted
          "agent_105_failure"         — Agent 105 produced no terminal action
          "agent_151_failure"         — Agent 151 produced no terminal action
          "blackboard_write_failure"  — Blackboard POST failed
        """
        try:
            from foundry.dag.synthesis_flow import synthesis_flow
            result = await synthesis_flow(uuid_a=None, uuid_b=None)

            if result.startswith("formally_verified:"):
                uuid = result.split(":", 1)[1]
                logger.info("[Agent002] ✓ FORMALLY VERIFIED: %s", uuid)
            elif result.startswith("rejected:"):
                rejection_id = result.split(":", 1)[1]
                logger.info(
                    "[Agent002] Agent 105 rejected the pair (rejection: %s). "
                    "Agent 003 will dispatch the Evolutionary Gardener.",
                    rejection_id[:8],
                )
            elif result == "no_pairs":
                logger.info(
                    "[Agent002] Librarian found no eligible pairs. "
                    "Waiting for more formulas or for existing evaluations to complete."
                )
            elif result.startswith("proof_deferred:"):
                uuid = result.split(":", 1)[1]
                logger.info(
                    "[Agent002] Proof deferred for %s (Agent 151 budget exhausted). "
                    "Formula remains SYNTACTICALLY_CORRECT.", uuid[:8],
                )
            else:
                logger.warning("[Agent002] synthesis_flow returned: %s", result)

        except ImportError as exc:
            logger.error(
                "[Agent002] Could not import synthesis_flow: %s. "
                "Ensure foundry/dag/synthesis_flow.py exists.", exc,
            )
        except Exception as exc:
            logger.error("[Agent002] synthesis_flow raised: %s", exc, exc_info=True)
        finally:
            self._synthesis_in_flight = False
            logger.debug("[Agent002] synthesis_flow task complete. In-flight flag cleared.")

    # ------------------------------------------------------------------
    # Startup baseline
    # ------------------------------------------------------------------

    async def _initialize_seen_set(self) -> None:
        """
        Load all currently eligible formula UUIDs as the startup baseline.

        This prevents Agent 002 from re-triggering synthesis for formulas
        that existed before it started. Those formulas have already been
        through at least one synthesis cycle (or are waiting in an evaluated
        state the Librarian already knows about).

        If the Blackboard is unreachable at startup, _seen_uuids stays empty.
        Agent 002 will treat all formulas as "new" on the first poll cycle,
        which may trigger a synthesis run. This is acceptable — synthesis_flow
        is idempotent and the Librarian will simply score and pick the best pair.
        """
        logger.info("[Agent002] Loading startup baseline from Blackboard...")
        formulas = await self._fetch_eligible_formulas()
        self._seen_uuids = {f["uuid"] for f in formulas}
        logger.info(
            "[Agent002] Baseline loaded: %d eligible formulas pre-existing "
            "(will not trigger synthesis for these).",
            len(self._seen_uuids),
        )

    # ------------------------------------------------------------------
    # Blackboard query
    # ------------------------------------------------------------------

    async def _fetch_eligible_formulas(self) -> list:
        """
        Fetch all formulas in eligible statuses from the Blackboard.
        Returns combined list across all eligible statuses, deduplicated.
        """
        collected = []
        seen_in_fetch: Set[str] = set()

        async with httpx.AsyncClient(timeout=15.0) as client:
            for status in _ELIGIBLE_STATUSES:
                try:
                    resp = await client.get(
                        f"{self._api_url}/v1/formulas",
                        params={"status": status},
                    )
                    if resp.status_code == 400:
                        # API returns 400 when no formulas match (not an error)
                        continue
                    resp.raise_for_status()
                    for formula in resp.json():
                        if formula["uuid"] not in seen_in_fetch:
                            seen_in_fetch.add(formula["uuid"])
                            collected.append(formula)
                except httpx.HTTPStatusError as exc:
                    logger.error(
                        "[Agent002] HTTP error fetching status=%s: %s",
                        status, exc,
                    )
                except httpx.RequestError as exc:
                    logger.error(
                        "[Agent002] Blackboard unreachable (status=%s): %s",
                        status, exc,
                    )

        return collected


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stdout,
    )

    monitor = SynthesisTriggerMonitor(api_url=BLACKBOARD_API_URL)

    if "--once" in sys.argv:
        await monitor.run_once()
    else:
        try:
            await monitor.run_forever()
        except KeyboardInterrupt:
            logger.info("[Agent002] Shutdown by user.")


if __name__ == "__main__":
    asyncio.run(main())
