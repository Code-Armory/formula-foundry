"""
Evolutionary Flow 204 — Micro/Macro Bridge

Triggered by Agent 003 when failure_mode == MICRO_MACRO_MISMATCH.
Passes rejection context to Agent 204. Agent 204 fetches parents via tools,
constructs Formula C, and writes it to the Blackboard.

Bugs fixed vs. delivered version:
  1. run_result.formula was always None because propose_micro_macro_bridge was
     not in _PROPOSAL_TOOL_NAMES. Now that agent_204 uses propose_formula_to_blackboard
     (in _PROPOSAL_TOOL_NAMES), run_result.formula is correctly populated.
     Check changed from run_result.formula to run_result.was_synthesized accordingly.
  2. Pre-fetching of parent formulas removed — agent fetches them itself via tools,
     matching the Agent 201 contract and keeping flow/agent responsibilities clean.
  3. Prefect compatibility shim added (consistent with all other flow files).

Exit conditions:
  "fetch_rejection_failed"   — Blackboard unreachable or rejection missing
  "agent_204_failure"        — Agent 204 did not produce a formula
  "blackboard_write_failure" — POST to Blackboard failed
  "formula_written:<uuid>"   — Formula C in Blackboard (goal state)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os

import httpx

from foundry.agents.base import AgentConfig
from foundry.agents.evolutionary.agent_204 import MicroMacroGardener
from foundry.core.schema.formula import AgentLayer

logger = logging.getLogger(__name__)

try:
    from prefect import flow, get_run_logger
    _USE_PREFECT = True
except ImportError:
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


@flow(name="evolutionary-flow-204")
async def evolutionary_flow_204(rejection_id: str) -> str:
    log = get_run_logger()
    config = _get_agent_config()

    # Fetch rejection metadata to build the initial message preview.
    # Agent 204 will re-fetch the full record itself via fetch_rejection_data tool.
    async with httpx.AsyncClient(timeout=15.0) as client:
        rej_resp = await client.get(
            f"{config.blackboard_api_url}/v1/rejections/{rejection_id}"
        )
        if rej_resp.status_code != 200:
            log.error(
                "Failed to fetch rejection %s: HTTP %s",
                rejection_id[:8], rej_resp.status_code,
            )
            return "fetch_rejection_failed"
        rejection = rej_resp.json()

    trigger_data = {
        "rejection_id": rejection_id,
        "uuid_a": rejection.get("uuid_a", ""),
        "uuid_b": rejection.get("uuid_b", ""),
        # Preview only — agent reads the full text via fetch_rejection_data tool
        "bridging_concept_preview": rejection.get("suggested_bridging_formula", "")[:200],
    }

    agent = MicroMacroGardener(config)
    run_result = await agent.run(trigger_data)

    if not run_result.was_synthesized:
        log.error(
            "Agent 204 failed for rejection %s. Reason: %s",
            rejection_id[:8],
            run_result.failure_reason,
        )
        return "agent_204_failure"

    # Write Formula C to the Blackboard.
    async with httpx.AsyncClient(timeout=30.0) as client:
        payload = {
            "formula": json.loads(run_result.formula.model_dump_json()),
            "proposing_agent_id": run_result.agent_id,
            "agent_layer": AgentLayer.LAYER_2.value,
        }
        post_resp = await client.post(
            f"{config.blackboard_api_url}/v1/formulas",
            json=payload,
        )
        if post_resp.status_code not in (200, 201):
            log.error(
                "Blackboard write failed for rejection %s: HTTP %s",
                rejection_id[:8], post_resp.status_code,
            )
            return "blackboard_write_failure"
        uuid = post_resp.json()["uuid"]

    log.info(
        "Agent 204 SUCCESS: Formula C written = %s (resolves rejection %s)",
        uuid, rejection_id[:8],
    )
    return f"formula_written:{uuid}"


# ---------------------------------------------------------------------------
# CLI entry point for manual testing
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    if len(sys.argv) < 2:
        print("Usage: python -m foundry.dag.evolutionary_flow_204 <rejection_id>")
        sys.exit(1)

    result = asyncio.run(evolutionary_flow_204(sys.argv[1]))
    print(f"Flow result: {result}")
