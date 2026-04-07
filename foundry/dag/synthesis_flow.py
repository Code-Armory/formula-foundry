"""
Synthesis Flow — The Second Production DAG

Librarian pair selection → Agent 105 synthesis → [if synthesized] Agent 151 audit.

Flow exit conditions:
  "no_pairs"                 — Librarian: <2 eligible formulas, or all pairs evaluated
  "rejected:<rejection_id>"  — Agent 105 PATH B: no isomorphism found
  "agent_105_failure"        — Agent 105 failed to produce any terminal action
  "blackboard_write_failure" — Blackboard POST failed after Agent 105 synthesis
  "formally_verified:<uuid>" — Full pipeline success
  "falsified:<uuid>"         — Agent 151 falsified the formula
  "proof_deferred:<uuid>"    — Agent 151: proof budget exhausted, no counterexample
  "agent_151_failure"        — Agent 151 failed to produce any terminal action

Entry:
  synthesis_flow()              — Librarian selects pair autonomously
  synthesis_flow(uuid_a, uuid_b) — Manual UUID override (dev/testing)

To run locally:
  python -m foundry.dag.synthesis_flow --test
  python -m foundry.dag.synthesis_flow --uuid-a <uuid> --uuid-b <uuid>
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from typing import Optional, Tuple

import httpx

from foundry.agents.base import AgentConfig, AgentRunResult
from foundry.agents.orchestration.librarian import LibrarianRouter, RoutingDecision
from foundry.agents.specialist.agent_105 import IsomorphismSynthesizer
from foundry.agents.specialist.agent_151 import Lean4Auditor
from foundry.agents.specialist.agent_152 import EntropyAuditor
from foundry.core.schema.formula import AgentLayer

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Auditor routing constants
# ---------------------------------------------------------------------------

# Game theory formula tags — controls auditor routing.
# Formulas with these tags are held for Agent 153 (PHASE_3)
# UNLESS they also carry a tag in _AGENT_151_VERIFIABLE_TAGS.
_GAME_THEORY_TAGS: frozenset = frozenset({
    "game_theory", "adverse_selection", "kyle_lambda",
})

# Entropy / information theory tags — routed to Agent 152.
# Does NOT include "entropy_lambda" (Agent 205 output) — those carry
# adverse_selection + information_geometry and route to Agent 151 via SP6.
_ENTROPY_TAGS: frozenset = frozenset({
    "entropy", "information_theory", "shannon",
})

# Game-theory sub-tags that Agent 151 can verify today (no Agent 153 needed).
#   exp_decay:            λ(t) = λ₀·exp(-δt)       — Real.exp_pos (SP5)
#   information_geometry: λ_info = λ₀·exp(-H_OFI)  — Real.exp_pos (SP6)
#   nonlinear_impact:     d²(Δp)/dx² = 2γ ≥ 0      — linarith     (SP7)
_AGENT_151_VERIFIABLE_TAGS: frozenset = frozenset({
    "exp_decay",
    "information_geometry",
    "nonlinear_impact",
})


def _select_auditor(formula_tags: set) -> Optional[str]:
    """
    Determine which auditor to dispatch for a synthesized formula.

    Returns:
      "agent_151" — route to Agent 151 (Real Analysis / Info Geometry)
      "agent_152" — route to Agent 152 (Entropy Auditor, Finset.sum_nonpos)
      None        — hold at SYNTACTICALLY_CORRECT (Agent 153 queue, PHASE_3)

    Routing rules (evaluated in order):
      Entropy tags present          → "agent_152" (Agent 051 output)
      Game theory + verifiable tag  → "agent_151" (exp_decay / info_geo / nonlinear)
      Game theory, no verifiable    → None (convexity — hold for Agent 153)
      All other formulas            → "agent_151" (default)
    """
    if formula_tags & _ENTROPY_TAGS:
        return "agent_152"
    if formula_tags & _GAME_THEORY_TAGS:
        if formula_tags & _AGENT_151_VERIFIABLE_TAGS:
            return "agent_151"
        return None
    return "agent_151"


def _get_agent_config() -> AgentConfig:
    return AgentConfig(
        anthropic_api_key=os.environ["ANTHROPIC_API_KEY"],
        blackboard_api_url=os.environ.get("BLACKBOARD_API_URL", "http://localhost:8000"),
        lean_worker_url=os.environ.get("LEAN_WORKER_URL", "http://lean_worker:8080"),
        max_iterations=int(os.environ.get("AGENT_MAX_ITERATIONS", "8")),
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


@task(name="librarian-select-pair", retries=2, retry_delay_seconds=2)
async def librarian_select_pair_task(
    uuid_a: Optional[str],
    uuid_b: Optional[str],
) -> Optional[Tuple[str, str]]:
    log = get_run_logger()
    if uuid_a and uuid_b:
        log.info("Manual pair override: %s ↔ %s", uuid_a[:8], uuid_b[:8])
        return (uuid_a, uuid_b)

    config = _get_agent_config()
    router = LibrarianRouter(blackboard_api_url=config.blackboard_api_url)
    decision: Optional[RoutingDecision] = await router.select_next_pair()

    if decision is None:
        log.info("Librarian: no eligible pairs available.")
        return None

    log.info(
        "Librarian selected: '%s' (%s) ↔ '%s' (%s) | score=%.3f",
        decision.formula_a_name, decision.uuid_a[:8],
        decision.formula_b_name, decision.uuid_b[:8],
        decision.score,
    )
    return (decision.uuid_a, decision.uuid_b)


@task(name="run-agent-105", retries=1, retry_delay_seconds=5, timeout_seconds=600)
async def run_agent_105_task(uuid_a: str, uuid_b: str) -> AgentRunResult:
    log = get_run_logger()
    log.info("Invoking Agent 105: %s ↔ %s", uuid_a[:8], uuid_b[:8])

    config = _get_agent_config()
    agent = IsomorphismSynthesizer(config)
    result = await agent.run({
        "uuid_a": uuid_a,
        "uuid_b": uuid_b,
        "synthesis_context": "Librarian-directed synthesis",
    })

    log.info(
        "Agent 105 complete: success=%s | was_synthesized=%s | was_rejected=%s | iterations=%d",
        result.success, result.was_synthesized, result.was_rejected, result.iterations_used,
    )
    if not result.success:
        log.error("Agent 105 failure: %s", result.failure_reason)
    return result


@task(name="write-synthesis-to-blackboard", retries=3, retry_delay_seconds=2)
async def write_synthesis_to_blackboard_task(run_result: AgentRunResult) -> Optional[str]:
    if not run_result.was_synthesized or run_result.formula is None:
        return None

    log = get_run_logger()
    config = _get_agent_config()
    formula = run_result.formula

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
        uuid = response.json()["uuid"]

    log.info("Synthesis formula written to Blackboard: %s ('%s')", uuid, formula.name or "unnamed")
    return uuid


@task(name="run-agent-151", retries=1, retry_delay_seconds=10, timeout_seconds=900)
async def run_agent_151_task(formula_uuid: str, formula_name: str) -> AgentRunResult:
    log = get_run_logger()
    log.info("Invoking Agent 151 on: %s ('%s')", formula_uuid[:8], formula_name)

    config = _get_agent_config()
    agent = Lean4Auditor(config)
    result = await agent.run({
        "uuid": formula_uuid,
        "formula_name": formula_name,
        "context": "Synthesis pipeline: Agent 105 output → Agent 151 audit",
    })

    log.info(
        "Agent 151 complete: success=%s | audit_outcome=%s | iterations=%d",
        result.success, result.audit_outcome, result.iterations_used,
    )
    return result


@task(name="run-agent-152", retries=1, retry_delay_seconds=10, timeout_seconds=900)
async def run_agent_152_task(formula_uuid: str, formula_name: str) -> AgentRunResult:
    log = get_run_logger()
    log.info("Invoking Agent 152 on: %s ('%s')", formula_uuid[:8], formula_name)

    config = _get_agent_config()
    agent = EntropyAuditor(config)
    result = await agent.run({
        "uuid": formula_uuid,
        "formula_name": formula_name,
        "context": "Synthesis pipeline: Agent 105 output → Agent 152 entropy audit",
    })

    log.info(
        "Agent 152 complete: success=%s | audit_outcome=%s | iterations=%d",
        result.success, result.audit_outcome, result.iterations_used,
    )
    return result


@task(name="log-synthesis-trace")
async def log_synthesis_trace_task(run_result: AgentRunResult, stage: str) -> None:
    log = get_run_logger()
    trace = {
        "stage": stage,
        "agent_id": run_result.agent_id,
        "triggered_at": run_result.triggered_at.isoformat(),
        "completed_at": run_result.completed_at.isoformat(),
        "iterations_used": run_result.iterations_used,
        "success": run_result.success,
        "formula_uuid": run_result.formula.uuid if run_result.formula else None,
        "steps": [
            {"iteration": s.iteration, "tool_name": s.tool_name,
             "error": s.tool_result.get("error")}
            for s in run_result.reasoning_trace
        ],
    }
    log.info("SYNTHESIS TRACE [%s]: %s", stage, json.dumps(trace, indent=2))


@flow(
    name="synthesis-foundry",
    description="Librarian → Agent 105 → Agent 151. Self-directing synthesis pipeline.",
    version="0.1.0",
)
async def synthesis_flow(
    uuid_a: Optional[str] = None,
    uuid_b: Optional[str] = None,
) -> str:
    log = get_run_logger()

    pair = await librarian_select_pair_task(uuid_a, uuid_b)
    if pair is None:
        return "no_pairs"

    selected_uuid_a, selected_uuid_b = pair
    log.info("Synthesis pipeline: %s ↔ %s", selected_uuid_a[:8], selected_uuid_b[:8])

    run_105 = await run_agent_105_task(selected_uuid_a, selected_uuid_b)
    await log_synthesis_trace_task(run_105, stage="agent_105")

    if run_105.was_rejected:
        rejection_id = (run_105.output_data or {}).get("rejection_id", "unknown")
        return f"rejected:{rejection_id}"

    if not run_105.was_synthesized:
        return "agent_105_failure"

    formula_uuid = await write_synthesis_to_blackboard_task(run_105)
    if not formula_uuid:
        return "blackboard_write_failure"

    formula_name = run_105.formula.name or formula_uuid[:8]
    formula_tags = set(run_105.formula.tags) if run_105.formula else set()

    auditor = _select_auditor(formula_tags)

    if auditor is None:
        log.info(
            "Formula %s has game_theory tags without a verifiable sub-tag. "
            "Holding at SYNTACTICALLY_CORRECT for Agent 153 (PHASE_3).",
            formula_uuid,
        )
        return f"proof_deferred:{formula_uuid}"

    if auditor == "agent_152":
        run_152 = await run_agent_152_task(formula_uuid, formula_name)
        await log_synthesis_trace_task(run_152, stage="agent_152")
        outcome = run_152.audit_outcome
        if outcome == "formally_verified":
            return f"formally_verified:{formula_uuid}"
        elif outcome == "falsified":
            return f"falsified:{formula_uuid}"
        elif outcome == "syntactically_correct":
            return f"proof_deferred:{formula_uuid}"
        else:
            return "agent_152_failure"

    # Default: agent_151 (Hawkes, cross-asset, info-geometry, exp-decay, nonlinear)
    run_151 = await run_agent_151_task(formula_uuid, formula_name)
    await log_synthesis_trace_task(run_151, stage="agent_151")

    outcome = run_151.audit_outcome
    if outcome == "formally_verified":
        return f"formally_verified:{formula_uuid}"
    elif outcome == "falsified":
        return f"falsified:{formula_uuid}"
    elif outcome == "syntactically_correct":
        return f"proof_deferred:{formula_uuid}"
    else:
        return "agent_151_failure"


async def _run_test(uuid_a: Optional[str] = None, uuid_b: Optional[str] = None) -> None:
    logging.basicConfig(level=logging.INFO)
    logger.info("Starting synthesis flow test run...")
    result = await synthesis_flow(uuid_a=uuid_a, uuid_b=uuid_b)
    logger.info("Flow result: %s", result)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true")
    parser.add_argument("--uuid-a", default=None)
    parser.add_argument("--uuid-b", default=None)
    args = parser.parse_args()
    asyncio.run(_run_test(uuid_a=args.uuid_a, uuid_b=args.uuid_b))
