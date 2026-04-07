"""
Adverse Selection Flow — Game Theory Wing DAG

Triggered by detect_adverse_selection() in databento_ingest.py when
Kyle's Lambda exceeds its 95th percentile with R² ≥ 0.40.

Flow: AdverseSelectionTrigger → Agent 050 → Blackboard write

Exit conditions:
  "no_trigger"               — trigger fired but suppressed in ingest (R² < 0.20)
  "agent_050_failure"        — Agent 050 did not produce a formula
  "blackboard_write_failure" — Blackboard POST failed
  "formula_written:<uuid>"   — formula in Blackboard (goal state)

Agent 050 writes Extension A (nonlinear_impact tag) or Extension B (exp_decay tag).
synthesis_flow.py routing logic:
  - exp_decay → Agent 151 (Real.exp_pos, verifiable today)
  - nonlinear_impact → Agent 151 (Seed Proof 7: linarith, 0 ≤ 2 * γ)

To test manually:
  PYTHONPATH=. python -m foundry.dag.adverse_selection_flow --test
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Optional

import httpx

from foundry.agents.base import AgentConfig, AgentRunResult
from foundry.agents.specialist.agent_050 import AdverseSelectionSpecialist
from foundry.core.schema.formula import AgentLayer, EmpiricalTrace, FormulaDNA

logger = logging.getLogger(__name__)

try:
    from prefect import flow, get_run_logger, task
    _USE_PREFECT = True
except ImportError:
    def task(fn=None, **kwargs):  # type: ignore
        return fn if fn is not None else lambda f: f

    def flow(fn=None, **kwargs):  # type: ignore
        return fn if fn is not None else lambda f: f

    def get_run_logger():  # type: ignore
        return logging.getLogger(__name__)

    _USE_PREFECT = False


def _get_agent_config() -> AgentConfig:
    return AgentConfig(
        anthropic_api_key=os.environ["ANTHROPIC_API_KEY"],
        blackboard_api_url=os.environ.get("BLACKBOARD_API_URL", "http://localhost:8000"),
        max_iterations=int(os.environ.get("AGENT_MAX_ITERATIONS", "8")),
        sympy_timeout_seconds=int(os.environ.get("SYMPY_TIMEOUT", "10")),
    )


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(name="run-agent-050", retries=1, retry_delay_seconds=5, timeout_seconds=300)
async def run_agent_050_task(trigger_data: dict) -> AgentRunResult:
    log = get_run_logger()
    log.info(
        "Invoking Agent 050 for %s | λ=%.6f (pctile=%.3f) | R²=%.3f",
        trigger_data.get("instrument"),
        trigger_data.get("lambda_coefficient", 0.0),
        trigger_data.get("lambda_percentile", 0.0),
        trigger_data.get("regression_r2", 0.0),
    )
    config = _get_agent_config()
    agent = AdverseSelectionSpecialist(config)
    result = await agent.run(trigger_data)
    log.info(
        "Agent 050 complete: success=%s | iterations=%d | formula=%s",
        result.success, result.iterations_used,
        result.formula.uuid if result.formula else "None",
    )
    if not result.success:
        log.error("Agent 050 failed: %s", result.failure_reason)
    return result


@task(name="write-adverse-selection-formula", retries=3, retry_delay_seconds=2)
async def write_to_blackboard_task(
    run_result: AgentRunResult,
    trigger_data: dict,
) -> Optional[str]:
    if not run_result.was_synthesized:
        return None

    log = get_run_logger()
    formula = run_result.formula
    config = _get_agent_config()

    # Attach empirical trace so the Blackboard records the trigger provenance
    trace = EmpiricalTrace(
        data_source="databento",
        instrument=trigger_data.get("instrument", "unknown"),
        schema_type="mbp-10",
        time_range_start=trigger_data.get("window_start", datetime.now(timezone.utc).isoformat()),
        time_range_end=trigger_data.get("window_end", datetime.now(timezone.utc).isoformat()),
        trigger_conditions=trigger_data.get("trigger_conditions", {}),
        sample_count=len(trigger_data.get("signed_volume_series", [])),
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

    extension_type = "exp_decay" if "exp_decay" in formula.tags else "nonlinear_impact"
    log.info(
        "Formula written to Blackboard: %s (%s) | %s",
        uuid, formula.name, extension_type,
    )
    return uuid


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(
    name="adverse-selection-foundry",
    description="Kyle's Lambda regime shift → Agent 050 → Game Theory IP",
)
async def adverse_selection_flow(trigger_data: dict) -> str:
    log = get_run_logger()

    run_result = await run_agent_050_task(trigger_data)

    if not run_result.was_synthesized:
        log.error(
            "Agent 050 failed for %s. Reason: %s",
            trigger_data.get("instrument"), run_result.failure_reason,
        )
        return "agent_050_failure"

    formula_uuid = await write_to_blackboard_task(run_result, trigger_data)
    if not formula_uuid:
        return "blackboard_write_failure"

    extension = "exp_decay" if "exp_decay" in run_result.formula.tags else "nonlinear_impact"
    log.info(
        "Adverse selection flow complete: %s (%s). "
        "Agent 002 will detect and fire synthesis_flow.",
        formula_uuid, extension,
    )
    return f"formula_written:{formula_uuid}"


# ---------------------------------------------------------------------------
# Test harness
# ---------------------------------------------------------------------------

def _build_test_trigger() -> dict:
    """
    Synthetic adverse selection trigger for --test mode.

    Simulates a nonlinear impact scenario: price change accelerates
    with volume (Extension A territory).
    """
    import math
    n_bars = 12
    sv = [float(i * 50 - 300) for i in range(n_bars)]   # signed vol: -300 to +250
    # Nonlinear: Δp = 0.0003·sv + 0.000001·sv² + noise
    dp = [
        0.0003 * v + 0.000001 * v ** 2 + (i % 3 - 1) * 0.00005
        for i, v in enumerate(sv)
    ]
    now = datetime.now(timezone.utc)
    return {
        "event_type":           "adverse_selection_regime",
        "instrument":           "ES.c.0",
        "window_start":         now.isoformat(),
        "window_end":           now.isoformat(),
        "lambda_coefficient":   0.000312,
        "lambda_percentile":    0.972,
        "regression_r2":        0.651,
        "signed_volume_series": sv,
        "price_change_series":  dp,
        "trigger_conditions": {
            "lambda_coefficient":        0.000312,
            "lambda_percentile":         0.972,
            "lambda_percentile_threshold": 0.95,
            "regression_r2":             0.651,
            "r2_trigger_threshold":      0.40,
            "n_bars":                    n_bars,
            "description":               "Test: nonlinear price impact scenario",
        },
    }


async def _run_test() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    trigger_data = _build_test_trigger()
    logger.info("Starting adverse selection flow test...")
    result = await adverse_selection_flow(trigger_data)
    logger.info("Flow result: %s", result)


if __name__ == "__main__":
    if "--test" in sys.argv:
        asyncio.run(_run_test())
    else:
        print("Usage: python -m foundry.dag.adverse_selection_flow --test")
        sys.exit(1)
