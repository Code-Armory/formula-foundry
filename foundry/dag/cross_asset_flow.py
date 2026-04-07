"""
Cross-Asset Flow — The Macro Specialist DAG

Triggered by databento_ingest.py when a structural decoupling occurs between
two instruments (e.g., ES and ZN).
Invokes Agent 060 to parameterize the regime (Flight-to-Quality or
Coordinated Liquidation) as a bivariate Vector Hawkes process.

Flow: Trigger → Agent 060 → Blackboard Write (SYNTACTICALLY_CORRECT)

Exit conditions:
  "agent_060_failure"        — Agent 060 failed to produce a formula
  "blackboard_write_failure" — Blackboard POST failed
  "formula_written:<uuid>"   — Formula in Blackboard, Librarian will pair it

To run locally:
  python -m foundry.dag.cross_asset_flow --test
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
from foundry.agents.specialist.agent_060 import MacroCrossAssetSpecialist
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


@task(name="run-agent-060", retries=1, retry_delay_seconds=5, timeout_seconds=600)
async def run_agent_060_task(trigger_data: Dict[str, Any]) -> AgentRunResult:
    """
    Invoke Agent 060 (Macro Cross-Asset Specialist) with the cross-asset trigger.
    Timeout: 10 minutes — Agent 060 validates a bivariate SymPy expression and
    constructs a detailed behavioral mapping across 4+ matrix entries.
    """
    log = get_run_logger()

    inst_a  = trigger_data.get("instrument_a", "UNKNOWN")
    inst_b  = trigger_data.get("instrument_b", "UNKNOWN")
    regime  = trigger_data.get("regime", "unknown_regime")
    corr    = trigger_data.get("correlation", 0.0)

    log.info(
        "Invoking Agent 060: %s ↔ %s | regime=%s | ρ=%.4f",
        inst_a, inst_b, regime.upper(), corr,
    )

    config = _get_agent_config()
    agent  = MacroCrossAssetSpecialist(config)
    result = await agent.run(trigger_data)

    log.info(
        "Agent 060 complete: success=%s | was_synthesized=%s | iterations=%d",
        result.success, result.was_synthesized, result.iterations_used,
    )
    if not result.success:
        log.error("Agent 060 failure: %s", result.failure_reason)

    return result


@task(name="write-cross-asset-formula", retries=3, retry_delay_seconds=2)
async def write_formula_to_blackboard_task(run_result: AgentRunResult) -> Optional[str]:
    """
    POST the Vector Hawkes formula to the Blackboard with LAYER_1 authority.

    Agent 060 operates at Layer 1 and has cleared the SymPy gate internally,
    so the formula lands as SYNTACTICALLY_CORRECT. Agent 002 will detect it
    and schedule Agent 105 to find the isomorphism with Agent 089's output.
    """
    if not run_result.was_synthesized or run_result.formula is None:
        return None

    log    = get_run_logger()
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
        "Cross-asset formula written to Blackboard: %s ('%s') | tags: %s",
        uuid,
        formula.name or "unnamed",
        formula.tags[:6],
    )
    return uuid


@task(name="log-cross-asset-trace")
async def log_trace_task(run_result: AgentRunResult) -> None:
    """Structured audit log for Agent 060's reasoning trace."""
    log = get_run_logger()
    trace = {
        "agent_id":    run_result.agent_id,
        "success":     run_result.success,
        "iterations":  run_result.iterations_used,
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
    log.info("CROSS-ASSET TRACE: %s", json.dumps(trace, indent=2))


# ---------------------------------------------------------------------------
# The Flow
# ---------------------------------------------------------------------------


@flow(
    name="cross-asset-foundry",
    description=(
        "Transforms a live Databento cross-asset decoupling event into a "
        "bivariate Vector Hawkes formula via Agent 060. "
        "Formula lands as SYNTACTICALLY_CORRECT; Agent 002 schedules synthesis."
    ),
    version="0.1.0",
)
async def cross_asset_flow(trigger_data: Dict[str, Any]) -> str:
    """
    Entry: trigger_data dict from CrossAssetTrigger.to_agent_input().
    Required fields: regime, instrument_a, instrument_b, correlation,
                     ofi_zscore_es (or ofi_z_a), ofi_zscore_zn (or ofi_z_b),
                     n_windows, timestamp.
    """
    log = get_run_logger()
    log.info(
        "cross_asset_flow: regime=%s | %s↔%s | ρ=%.4f",
        trigger_data.get("regime"),
        trigger_data.get("instrument_a"),
        trigger_data.get("instrument_b"),
        trigger_data.get("correlation", 0.0),
    )

    # Stage 1: Agent 060 reasoning loop
    run_result = await run_agent_060_task(trigger_data)
    await log_trace_task(run_result)

    if not run_result.was_synthesized or run_result.formula is None:
        log.error("Agent 060 produced no formula.")
        return "agent_060_failure"

    # Stage 2: Blackboard write
    formula_uuid = await write_formula_to_blackboard_task(run_result)
    if not formula_uuid:
        log.error("Blackboard write failed for cross-asset formula.")
        return "blackboard_write_failure"

    log.info(
        "✓ Vector Hawkes formula in Blackboard: %s\n"
        "  Next: Agent 002 will detect it → Agent 105 will find isomorphism "
        "with Agent 089 → Agent 151 proves vector_hawkes_subcritical_2x2.",
        formula_uuid,
    )
    return f"formula_written:{formula_uuid}"


# ---------------------------------------------------------------------------
# Local test runner
# ---------------------------------------------------------------------------


async def _run_test() -> None:
    logging.basicConfig(level=logging.INFO)
    logger.info("Starting cross_asset_flow test run...")

    # Synthetic payload matching Regime A output from detect_cross_asset_decoupling()
    # Sign convention: ES is SELLING (negative z), ZN is BUYING (positive z)
    # March 9, 2020 limit-down: ES sell_vol >> buy_vol → net OFI << 0 → z << 0
    synthetic_trigger = {
        "event_type":    "cross_asset_flight_to_quality",
        "regime":        "flight_to_quality",
        "instrument_a":  "ES.c.0",
        "instrument_b":  "ZN.c.0",
        "timestamp":     "2026-04-05T14:30:00Z",
        "correlation":   -0.72,
        "n_windows":     12,
        "ofi_zscore_es": -3.10,   # NEGATIVE: ES aggressive selling (z < -2.5 threshold)
        "ofi_zscore_zn":  2.80,   # POSITIVE: ZN aggressive buying  (z > +2.0 threshold)
        "trigger_conditions": {
            "regime": "flight_to_quality",
            "correlation": -0.72,
            "correlation_threshold": -0.65,
            "ofi_z_a": -3.10,    # negative = selling
            "ofi_z_b":  2.80,    # positive = buying
            "n_windows": 12,
            "min_windows_required": 10,
        },
    }

    result = await cross_asset_flow(synthetic_trigger)
    logger.info("Flow exit: %s", result)

    if result.startswith("formula_written:"):
        uuid = result.split(":", 1)[1]
        logger.info("SUCCESS: GET /v1/formulas/%s", uuid)
        logger.info("Verify tags include 'vector_hawkes' and 'flight_to_quality'.")
    elif result == "agent_060_failure":
        logger.error("Agent 060 failed. Check ANTHROPIC_API_KEY and Blackboard connectivity.")
    elif result == "blackboard_write_failure":
        logger.error("Blackboard unreachable. Is docker-compose up api running?")


if __name__ == "__main__":
    asyncio.run(_run_test())
