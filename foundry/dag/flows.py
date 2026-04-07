"""
Panic Liquidity Flow — The first live DAG in the Formula Foundry.

Flow: MarketDataSlice → Trigger Detection → Agent 089 → Blackboard write

Exit conditions:
  "no_trigger"              — OFI/spread conditions not met
  "agent_failure"           — trigger fired but Agent 089 failed
  "blackboard_write_failure"— Agent 089 succeeded but Blackboard write failed
  "<uuid>"                  — formula written to Blackboard (goal state)

To run locally:
  python -m foundry.dag.flows --test
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional

import httpx

from foundry.agents.base import AgentConfig, AgentRunResult
from foundry.agents.specialist.agent_089 import HawkesSpecialist
from foundry.dag.triggers import (
    MarketDataSlice,
    PanicTrigger,
    build_test_panic_slice,
    detect_panic_fingerprint,
)
from foundry.core.schema.formula import FormulaDNA, AgentLayer, EmpiricalTrace

logger = logging.getLogger(__name__)


def _get_agent_config() -> AgentConfig:
    return AgentConfig(
        anthropic_api_key=os.environ["ANTHROPIC_API_KEY"],
        blackboard_api_url=os.environ.get("BLACKBOARD_API_URL", "http://localhost:8000"),
        max_iterations=int(os.environ.get("AGENT_089_MAX_ITERATIONS", "8")),
        sympy_timeout_seconds=int(os.environ.get("SYMPY_TIMEOUT", "10")),
    )


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


@task(name="detect-panic-fingerprint", retries=2, retry_delay_seconds=1)
def detect_trigger_task(data_slice: MarketDataSlice) -> PanicTrigger:
    log = get_run_logger()
    trigger = detect_panic_fingerprint(data_slice)
    if trigger.triggered:
        log.warning(
            "PANIC FINGERPRINT DETECTED: %s | OFI z=%.2fσ | Spread pctile=%.1f",
            trigger.instrument, trigger.ofi_zscore, trigger.spread_percentile,
        )
    else:
        log.info(
            "No trigger: %s | OFI z=%.2f | Spread pctile=%.1f",
            trigger.instrument, trigger.ofi_zscore, trigger.spread_percentile,
        )
    return trigger


@task(name="run-agent-089", retries=1, retry_delay_seconds=5, timeout_seconds=300)
async def run_agent_089_task(trigger: PanicTrigger) -> AgentRunResult:
    log = get_run_logger()
    log.info("Invoking Agent 089 for %s trigger", trigger.instrument)

    config = _get_agent_config()
    agent = HawkesSpecialist(config)
    result = await agent.run(trigger.to_agent_input())

    log.info(
        "Agent 089 complete: success=%s | iterations=%d | formula=%s",
        result.success, result.iterations_used,
        result.formula.uuid if result.formula else "None",
    )
    if not result.success:
        log.error("Agent 089 failed: %s", result.failure_reason)
    return result


@task(name="write-formula-to-blackboard", retries=3, retry_delay_seconds=2)
async def write_to_blackboard_task(
    run_result: AgentRunResult,
    trigger: PanicTrigger,
) -> Optional[str]:
    if not run_result.success or not run_result.formula:
        return None

    log = get_run_logger()
    formula = run_result.formula
    config = _get_agent_config()

    trace = EmpiricalTrace(
        data_source="databento",
        instrument=trigger.instrument,
        schema_type="mbo",
        time_range_start=trigger.timestamp,
        time_range_end=trigger.timestamp,
        trigger_conditions=trigger.trigger_conditions,
        sample_count=trigger.mbo_event_count,
    )
    formula.empirical_traces.append(trace)

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

    log.info("Formula written to Blackboard: %s", uuid)
    return uuid


@task(name="log-reasoning-trace")
async def log_reasoning_trace_task(run_result: AgentRunResult) -> None:
    log = get_run_logger()
    trace_summary = {
        "agent_id": run_result.agent_id,
        "triggered_at": run_result.triggered_at.isoformat(),
        "completed_at": run_result.completed_at.isoformat(),
        "iterations_used": run_result.iterations_used,
        "success": run_result.success,
        "formula_uuid": run_result.formula.uuid if run_result.formula else None,
        "steps": [
            {"iteration": step.iteration, "tool_name": step.tool_name,
             "valid": step.tool_result.get("valid"), "error": step.tool_result.get("error")}
            for step in run_result.reasoning_trace
        ],
    }
    log.info("REASONING TRACE: %s", json.dumps(trace_summary, indent=2))


@flow(
    name="panic-liquidity-foundry",
    description="OFI+Spread trigger → Agent 089 → Blackboard formula write",
    version="0.1.0",
)
async def panic_liquidity_flow(data_slice: MarketDataSlice) -> str:
    trigger = detect_trigger_task(data_slice)
    if not trigger.triggered:
        return "no_trigger"

    run_result = await run_agent_089_task(trigger)
    await log_reasoning_trace_task(run_result)

    if not run_result.success:
        return "agent_failure"

    formula_uuid = await write_to_blackboard_task(run_result, trigger)
    if not formula_uuid:
        return "blackboard_write_failure"

    return formula_uuid


async def _run_test() -> None:
    logging.basicConfig(level=logging.INFO)
    logger.info("Starting Formula Foundry test run...")
    test_slice = build_test_panic_slice(instrument="ES")
    result = await panic_liquidity_flow(test_slice)
    logger.info("Flow result: %s", result)
    if result not in ("no_trigger", "agent_failure", "blackboard_write_failure"):
        logger.info("SUCCESS: Formula UUID = %s", result)


if __name__ == "__main__":
    asyncio.run(_run_test())
