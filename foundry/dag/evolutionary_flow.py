"""
Evolutionary Flow — The Third Production DAG

Agent 201 (Temporal Scale Bridger) constructs Formula C from a rejection record.

Flow: Rejection selection → Agent 201 → Blackboard write
      Formula C lands as SYNTACTICALLY_CORRECT.
      Librarian Router handles C↔A and C↔B pairing on subsequent ticks.

This is the passive handoff pattern (Option 2a):
  Agent 201 does NOT trigger Agent 105 or Agent 151.
  It proposes Formula C and goes to sleep.
  The existing synthesis_flow handles verification when the Librarian
  routes C against its parents.

Entry:
  evolutionary_flow()                    — selects oldest unresolved rejection
  evolutionary_flow(rejection_id=<uuid>) — targets a specific rejection

Exit conditions:
  "no_rejections"                 — no REJECTED_ISOMORPHISM edges in graph
  "rejection_not_found:<id>"      — specified rejection_id does not exist
  "agent_201_failure"             — Agent 201 failed to produce a terminal action
  "blackboard_write_failure"      — Formula C write to Blackboard failed
  "formula_c_written:<uuid>"      — Formula C in Blackboard, Librarian will pick it up

To run locally:
  # Process oldest rejection (automatic selection):
  python -m foundry.dag.evolutionary_flow --test

  # Target a specific rejection:
  python -m foundry.dag.evolutionary_flow --rejection-id <uuid>
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from typing import Any, Dict, Optional

import httpx

from foundry.agents.base import AgentConfig, AgentRunResult
from foundry.agents.evolutionary.agent_201 import TemporalScaleBridger
from foundry.core.schema.formula import AgentLayer

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


def _get_agent_config() -> AgentConfig:
    return AgentConfig(
        anthropic_api_key=os.environ["ANTHROPIC_API_KEY"],
        blackboard_api_url=os.environ.get("BLACKBOARD_API_URL", "http://localhost:8000"),
        lean_worker_url=os.environ.get("LEAN_WORKER_URL", "http://lean_worker:8080"),
        max_iterations=int(os.environ.get("AGENT_MAX_ITERATIONS", "8")),
        sympy_timeout_seconds=int(os.environ.get("SYMPY_TIMEOUT", "10")),
    )


# ---------------------------------------------------------------------------
# Prefect compatibility shim
# ---------------------------------------------------------------------------

try:
    from prefect import task, flow, get_run_logger
    _USE_PREFECT = True
except ImportError:
    def task(fn=None, **kwargs):  # type: ignore
        return fn if fn is not None else lambda f: f

    def flow(fn=None, **kwargs):  # type: ignore
        return fn if fn is not None else lambda f: f

    def get_run_logger():  # type: ignore
        return logging.getLogger(__name__)

    _USE_PREFECT = False


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task(name="select-rejection-target", retries=2, retry_delay_seconds=2)
async def select_rejection_task(
    rejection_id: Optional[str],
) -> Optional[Dict[str, Any]]:
    """
    Resolve which rejection record to process.

    If rejection_id is provided: verify it exists and return it.
    If rejection_id is None: return the oldest unresolved rejection
      (last entry in DESC-sorted list — inserted first, least recently worked on).

    Returns the full rejection record dict, or None if none available.
    """
    log = get_run_logger()
    config = _get_agent_config()

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(f"{config.blackboard_api_url}/v1/rejections")
            resp.raise_for_status()
            rejections = resp.json()
    except httpx.RequestError as exc:
        log.error("Blackboard API unreachable when selecting rejection: %s", exc)
        return None

    if not rejections:
        log.info("No REJECTED_ISOMORPHISM edges in graph.")
        return None

    if rejection_id:
        matching = [r for r in rejections if r.get("rejection_id") == rejection_id]
        if not matching:
            log.error("Rejection %s not found in graph.", rejection_id)
            return None
        record = matching[0]
        log.info("Targeting specified rejection: %s", rejection_id[:8])
        return record

    # Automatic selection: oldest rejection (DESC list → last entry = oldest)
    record = rejections[-1]
    log.info(
        "Auto-selected oldest rejection: %s (agent_version=%s)",
        record.get("rejection_id", "")[:8],
        record.get("agent_version", "unknown"),
    )
    return record


@task(name="run-agent-201", retries=1, retry_delay_seconds=5, timeout_seconds=600)
async def run_agent_201_task(rejection_record: Dict[str, Any]) -> AgentRunResult:
    """
    Invoke the Temporal Scale Bridger (Agent 201) to construct Formula C.

    Agent 201 always produces a terminal action (propose_formula_to_blackboard).
    Timeout: 10 minutes (fetch rejection + fetch 2 parents + SymPy validation + proposal).
    """
    log = get_run_logger()
    rejection_id = rejection_record.get("rejection_id", "unknown")
    log.info("Invoking Agent 201 on rejection: %s", rejection_id[:8])

    config = _get_agent_config()
    agent = TemporalScaleBridger(config)

    trigger_data = {
        "rejection_id": rejection_id,
        "uuid_a": rejection_record.get("uuid_a", ""),
        "uuid_b": rejection_record.get("uuid_b", ""),
        "bridging_concept_preview": rejection_record.get("bridging_concept", "")[:200],
    }

    result = await agent.run(trigger_data)

    log.info(
        "Agent 201 complete: success=%s | was_synthesized=%s | iterations=%d",
        result.success,
        result.was_synthesized,
        result.iterations_used,
    )
    if not result.success:
        log.error("Agent 201 failure: %s", result.failure_reason)

    return result


@task(name="write-formula-c-to-blackboard", retries=3, retry_delay_seconds=2)
async def write_formula_c_task(run_result: AgentRunResult) -> Optional[str]:
    """
    POST Formula C to the Blackboard with LAYER_2 authority.
    Returns the formula UUID on success, or None.

    Layer 2 authority allows writing SYNTACTICALLY_CORRECT status.
    Formula C lands in the synthesis pool immediately after this write.
    """
    if not run_result.was_synthesized or run_result.formula is None:
        return None

    log = get_run_logger()
    formula = run_result.formula
    config = _get_agent_config()

    async with httpx.AsyncClient(timeout=30.0) as client:
        payload = {
            "formula": json.loads(formula.model_dump_json()),
            "proposing_agent_id": run_result.agent_id,
            "agent_layer": AgentLayer.LAYER_2.value,
        }
        response = await client.post(
            f"{config.blackboard_api_url}/v1/formulas",
            json=payload,
        )
        response.raise_for_status()
        result = response.json()
        uuid = result["uuid"]

    log.info(
        "Formula C written to Blackboard: %s ('%s') | tags: %s",
        uuid,
        formula.name or "unnamed",
        formula.tags[:5],
    )
    return uuid


@task(name="log-evolutionary-trace")
async def log_evolutionary_trace_task(
    run_result: AgentRunResult,
    rejection_id: str,
) -> None:
    """Structured audit log of the Agent 201 run."""
    log = get_run_logger()
    trace_summary = {
        "stage": "agent_201",
        "agent_id": run_result.agent_id,
        "source_rejection_id": rejection_id,
        "triggered_at": run_result.triggered_at.isoformat(),
        "completed_at": run_result.completed_at.isoformat(),
        "iterations_used": run_result.iterations_used,
        "success": run_result.success,
        "formula_uuid": run_result.formula.uuid if run_result.formula else None,
        "formula_name": run_result.formula.name if run_result.formula else None,
        "steps": [
            {
                "iteration": step.iteration,
                "tool_name": step.tool_name,
                "result_keys": list(step.tool_result.keys()),
                "error": step.tool_result.get("error"),
            }
            for step in run_result.reasoning_trace
        ],
    }
    log.info("EVOLUTIONARY TRACE: %s", json.dumps(trace_summary, indent=2))


# ---------------------------------------------------------------------------
# The Flow
# ---------------------------------------------------------------------------


@flow(
    name="evolutionary-foundry",
    description=(
        "Evolutionary Gardening: Agent 201 resolves a REJECTED_ISOMORPHISM edge "
        "by constructing Formula C (the Missing Link). "
        "Formula C is written as SYNTACTICALLY_CORRECT. "
        "The Librarian Router pairs it with the parent formulas on subsequent ticks."
    ),
    version="0.1.0",
)
async def evolutionary_flow(rejection_id: Optional[str] = None) -> str:
    """
    The third production flow of the Formula Foundry.

    Entry: optional rejection_id override. If None, selects the oldest
    unresolved rejection from the graph.
    """
    log = get_run_logger()

    # Stage 1: Identify target rejection
    rejection_record = await select_rejection_task(rejection_id)

    if rejection_record is None:
        if rejection_id:
            return f"rejection_not_found:{rejection_id}"
        return "no_rejections"

    target_id = rejection_record.get("rejection_id", "unknown")
    log.info(
        "Evolutionary pipeline starting: rejection=%s | %s ↔ %s",
        target_id[:8],
        rejection_record.get("uuid_a", "")[:8],
        rejection_record.get("uuid_b", "")[:8],
    )

    # Stage 2: Agent 201 — construct Formula C
    run_201 = await run_agent_201_task(rejection_record)
    await log_evolutionary_trace_task(run_201, rejection_id=target_id)

    if not run_201.was_synthesized:
        log.error("Agent 201 produced no formula from rejection %s.", target_id[:8])
        return "agent_201_failure"

    # Stage 3: Write Formula C to Blackboard
    formula_uuid = await write_formula_c_task(run_201)

    if not formula_uuid:
        log.error("Blackboard write failed for Formula C.")
        return "blackboard_write_failure"

    log.info(
        "Formula C in Blackboard: %s ('%s')\n"
        "Next: Librarian Router will pair it with the parent formulas. "
        "Run synthesis_flow() to trigger Agent 105 + Agent 151.",
        formula_uuid,
        run_201.formula.name or "unnamed",
    )
    return f"formula_c_written:{formula_uuid}"


# ---------------------------------------------------------------------------
# Local test runner
# ---------------------------------------------------------------------------


async def _run_test(rejection_id: Optional[str] = None) -> None:
    logging.basicConfig(level=logging.INFO)
    logger.info("Starting evolutionary flow test run...")
    if rejection_id:
        logger.info("Targeting rejection: %s", rejection_id)
    else:
        logger.info("Auto-selecting oldest unresolved rejection.")

    result = await evolutionary_flow(rejection_id=rejection_id)
    logger.info("Flow result: %s", result)

    if result.startswith("formula_c_written:"):
        uuid = result.split(":", 1)[1]
        logger.info("SUCCESS: Formula C = %s", uuid)
        logger.info("Verify: GET /v1/formulas/%s", uuid)
        logger.info("Next:   Run synthesis_flow() — Librarian will pair C with parents.")
    elif result == "no_rejections":
        logger.info("No rejections in graph. Run: PYTHONPATH=. python scripts/seed_rejection.py")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run evolutionary_flow locally")
    parser.add_argument("--test", action="store_true", help="Auto-select oldest rejection")
    parser.add_argument("--rejection-id", default=None, help="Target a specific rejection UUID")
    args = parser.parse_args()
    asyncio.run(_run_test(rejection_id=args.rejection_id))
