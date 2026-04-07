"""
Evolutionary Flow 205 — Information Geometry Bridge

Triggered by Agent 003 when failure_mode == INFORMATION_GEOMETRY_MISMATCH.
Passes rejection context to Agent 205 (InformationGeometryGardener).

Flow contract:
  Input:  rejection_id (str) — UUID of the REJECTED_ISOMORPHISM edge
  Output: one of:
    "formula_written:<uuid>"    — Formula C committed to Blackboard
    "fetch_rejection_failed"    — Rejection record not found or API error
    "agent_205_failure"         — Agent ran but produced no formula
    "blackboard_write_failure"  — Blackboard POST returned non-2xx

The flow does NOT raise. All failures return a string status code so
Agent 003 can log and continue without crashing the daemon.
"""

from __future__ import annotations

import asyncio
import json
import logging

import httpx

from foundry.agents.base import AgentConfig
from foundry.agents.evolutionary.agent_205 import InformationGeometryGardener
from foundry.core.schema.formula import AgentLayer

log = logging.getLogger(__name__)


async def evolutionary_flow_205(rejection_id: str) -> str:
    """
    Run Agent 205 for a single INFORMATION_GEOMETRY_MISMATCH rejection.

    Args:
        rejection_id: UUID of the REJECTED_ISOMORPHISM edge in Neo4j.

    Returns:
        Status string (see module docstring for contract).
    """
    config = AgentConfig()

    # ------------------------------------------------------------------
    # Fetch the rejection record to populate trigger_data.
    # Agent 205 re-fetches the full record itself via fetch_rejection_data,
    # but we need uuid_a / uuid_b / bridging_concept for build_initial_message.
    # ------------------------------------------------------------------
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
        "rejection_id":           rejection_id,
        "uuid_a":                 rejection.get("uuid_a", ""),
        "uuid_b":                 rejection.get("uuid_b", ""),
        "bridging_concept_preview": rejection.get("suggested_bridging_formula", "")[:200],
    }

    # ------------------------------------------------------------------
    # Run Agent 205
    # ------------------------------------------------------------------
    agent = InformationGeometryGardener(config)
    run_result = await agent.run(trigger_data)

    if not run_result.was_synthesized:
        log.error(
            "Agent 205 failed for rejection %s. Reason: %s",
            rejection_id[:8],
            run_result.failure_reason,
        )
        return "agent_205_failure"

    # ------------------------------------------------------------------
    # Write Formula C to the Blackboard
    # ------------------------------------------------------------------
    async with httpx.AsyncClient(timeout=30.0) as client:
        payload = {
            "formula":            json.loads(run_result.formula.model_dump_json()),
            "proposing_agent_id": run_result.agent_id,
            "agent_layer":        AgentLayer.LAYER_2.value,
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
        "Agent 205 SUCCESS: Formula C written = %s (resolves rejection %s)",
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
        print("Usage: python -m foundry.dag.evolutionary_flow_205 <rejection_id>")
        sys.exit(1)

    result = asyncio.run(evolutionary_flow_205(sys.argv[1]))
    print(f"Flow result: {result}")
