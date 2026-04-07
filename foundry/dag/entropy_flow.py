"""
Entropy Flow — The Order Book Fragility DAG

Triggered by databento_ingest.py (MBP-10 stream) when bid-side Shannon
entropy collapses below the 5th historical percentile AND total bid volume
drops below the 30th historical percentile simultaneously.

Invokes Agent 051 to formally express the pre-crisis order book structure
as a Shannon entropy fragility index.

Flow: EntropyTrigger → Agent 051 → Blackboard Write (SYNTACTICALLY_CORRECT)

Exit conditions:
  "agent_051_failure"        — Agent 051 failed to produce a formula
  "blackboard_write_failure" — Blackboard POST failed
  "formula_written:<uuid>"   — Formula in Blackboard, Librarian will pair it
                               (expected synthesis target: Agent 089's Hawkes)

To test locally:
  PYTHONPATH=. python -m foundry.dag.entropy_flow --test
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
from foundry.agents.specialist.agent_051 import OrderBookEntropySpecialist
from foundry.core.schema.formula import AgentLayer

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


def _get_agent_config() -> AgentConfig:
    return AgentConfig(
        anthropic_api_key=os.environ["ANTHROPIC_API_KEY"],
        blackboard_api_url=os.environ.get("BLACKBOARD_API_URL", "http://localhost:8000"),
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


@task(name="run-agent-051", retries=1, retry_delay_seconds=5, timeout_seconds=600)
async def run_agent_051_task(trigger_data: Dict[str, Any]) -> AgentRunResult:
    """
    Invoke Agent 051 (Order Book Entropy Specialist) with the collapse snapshot.
    """
    log = get_run_logger()

    instrument    = trigger_data.get("instrument", "UNKNOWN")
    entropy       = trigger_data.get("bid_entropy_bits", 0.0)
    entropy_pctile = trigger_data.get("entropy_percentile", 0.0)
    n_levels      = trigger_data.get("n_nonzero_levels", 0)

    log.info(
        "Invoking Agent 051: %s | H=%.4f bits | pctile=%.1f | levels=%d",
        instrument, entropy, entropy_pctile, n_levels,
    )

    config = _get_agent_config()
    agent  = OrderBookEntropySpecialist(config)
    result = await agent.run(trigger_data)

    log.info(
        "Agent 051 complete: success=%s | was_synthesized=%s | iterations=%d",
        result.success, result.was_synthesized, result.iterations_used,
    )
    if not result.success:
        log.error("Agent 051 failure: %s", result.failure_reason)

    return result


@task(name="write-entropy-formula", retries=3, retry_delay_seconds=2)
async def write_formula_to_blackboard_task(run_result: AgentRunResult) -> Optional[str]:
    """
    POST the entropy fragility formula to the Blackboard with LAYER_1 authority.
    Agent 051 clears the SymPy gate internally; formula lands as SYNTACTICALLY_CORRECT.
    """
    if not run_result.was_synthesized or run_result.formula is None:
        return None

    log     = get_run_logger()
    formula = run_result.formula
    config  = _get_agent_config()

    async with httpx.AsyncClient(timeout=30.0) as client:
        payload = {
            "formula": json.loads(formula.model_dump_json()),
            "proposing_agent_id": run_result.agent_id,
            "agent_layer": AgentLayer.LAYER_1.value,
        }
        response = await client.post(
            f"{config.blackboard_api_url}/v1/formulas",
            json=payload,
        )
        response.raise_for_status()
        uuid = response.json()["uuid"]

    log.info(
        "Entropy formula written to Blackboard: %s ('%s') | tags: %s",
        uuid,
        formula.name or "unnamed",
        formula.tags[:6],
    )
    return uuid


@task(name="log-entropy-trace")
async def log_trace_task(run_result: AgentRunResult) -> None:
    """Structured audit log for Agent 051's reasoning trace."""
    log = get_run_logger()
    trace = {
        "agent_id":     run_result.agent_id,
        "success":      run_result.success,
        "iterations":   run_result.iterations_used,
        "formula_uuid": run_result.formula.uuid if run_result.formula else None,
        "steps": [
            {
                "iteration": step.iteration,
                "tool_name": step.tool_name,
                "error":     step.tool_result.get("error"),
            }
            for step in run_result.reasoning_trace
        ],
    }
    log.info("ENTROPY TRACE: %s", json.dumps(trace, indent=2))


# ---------------------------------------------------------------------------
# The Flow
# ---------------------------------------------------------------------------


@flow(
    name="entropy-foundry",
    description=(
        "Transforms a bid-side Shannon entropy collapse event into an "
        "information-theoretic fragility formula via Agent 051. "
        "Expected synthesis: Tier 3 behavioral isomorphism with Agent 089's "
        "Hawkes process (entropy collapse ↔ branching ratio approaching criticality)."
    ),
    version="0.1.0",
)
async def entropy_flow(trigger_data: Dict[str, Any]) -> str:
    """
    Entry: trigger_data dict from EntropyTrigger.to_agent_input().
    Required fields: instrument, bid_entropy_bits, entropy_percentile,
                     total_bid_volume, volume_percentile,
                     best_bid_fraction, n_nonzero_levels, bid_levels, timestamp.
    """
    log = get_run_logger()
    log.info(
        "entropy_flow: %s | H=%.4f bits | pctile=%.1f | vol_pctile=%.1f",
        trigger_data.get("instrument"),
        trigger_data.get("bid_entropy_bits", 0.0),
        trigger_data.get("entropy_percentile", 0.0),
        trigger_data.get("volume_percentile", 0.0),
    )

    # Stage 1: Agent 051 reasoning loop
    run_result = await run_agent_051_task(trigger_data)
    await log_trace_task(run_result)

    if not run_result.was_synthesized or run_result.formula is None:
        log.error("Agent 051 produced no formula.")
        return "agent_051_failure"

    # Stage 2: Blackboard write
    formula_uuid = await write_formula_to_blackboard_task(run_result)
    if not formula_uuid:
        log.error("Blackboard write failed for entropy formula.")
        return "blackboard_write_failure"

    log.info(
        "✓ Entropy fragility formula in Blackboard: %s\n"
        "  Next: Agent 002 will detect it → Agent 105 will attempt Tier 3\n"
        "  isomorphism with Agent 089's Hawkes process\n"
        "  (entropy collapse ↔ branching ratio approaching criticality).",
        formula_uuid,
    )
    return f"formula_written:{formula_uuid}"


# ---------------------------------------------------------------------------
# Local test runner
# ---------------------------------------------------------------------------


async def _run_test() -> None:
    logging.basicConfig(level=logging.INFO)
    logger.info("Starting entropy_flow test run...")

    # Synthetic payload matching March 9, 2020 book structure at circuit breaker
    # H ≈ 0.6 bits (2nd percentile historically) — extreme concentration
    # 87% of remaining bid volume at best bid only
    synthetic_trigger = {
        "event_type":         "entropy_collapse",
        "instrument":         "ES.c.0",
        "timestamp":          "2020-03-09T18:32:15.000000000Z",
        "bid_entropy_bits":   0.5921,       # 1.8th historical percentile
        "entropy_percentile": 1.8,
        "total_bid_volume":   312,          # 8.4th volume percentile (depleted)
        "volume_percentile":  8.4,
        "best_bid_fraction":  0.874,        # 87.4% at best bid — extreme concentration
        "n_nonzero_levels":   3,            # only 3 of 10 levels have any volume
        "bid_levels": [
            {"price": 2729.00, "size": 273},   # 87.5% — best bid
            {"price": 2728.75, "size": 28},    # 9.0%
            {"price": 2728.50, "size": 11},    # 3.5%
            {"price": 2728.25, "size": 0},
            {"price": 2728.00, "size": 0},
            {"price": 2727.75, "size": 0},
            {"price": 2727.50, "size": 0},
            {"price": 2727.25, "size": 0},
            {"price": 2727.00, "size": 0},
            {"price": 2726.75, "size": 0},
        ],
        "trigger_conditions": {
            "entropy_bits": 0.5921,
            "entropy_percentile": 1.8,
            "entropy_threshold_percentile": 5.0,
            "total_bid_volume": 312,
            "volume_percentile": 8.4,
            "volume_threshold_percentile": 30.0,
            "entropy_triggered": True,
            "volume_triggered": True,
            "n_nonzero_levels": 3,
            "best_bid_fraction": 0.874,
        },
    }

    result = await entropy_flow(synthetic_trigger)
    logger.info("Flow exit: %s", result)

    if result.startswith("formula_written:"):
        uuid = result.split(":", 1)[1]
        logger.info("SUCCESS: GET /v1/formulas/%s", uuid)
        logger.info("Verify tags include 'entropy', 'leading_indicator', 'agent_051'.")
        logger.info(
            "Expected synthesis: Agent 105 pairs this with Agent 089's "
            "Hawkes formula → Tier 3 isomorphism."
        )
    elif result == "agent_051_failure":
        logger.error("Agent 051 failed. Check ANTHROPIC_API_KEY and Blackboard.")
    elif result == "blackboard_write_failure":
        logger.error("Blackboard unreachable. Is docker-compose up api running?")


if __name__ == "__main__":
    asyncio.run(_run_test())
