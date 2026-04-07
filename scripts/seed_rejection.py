"""
Seed Rejection — Synthetic REJECTED_ISOMORPHISM Edge

Simulates Agent 105 (v0.0.0-synthetic) formally rejecting the isomorphism
between the Hawkes Process Intensity Baseline and the Amihud Illiquidity Ratio.

Why this rejection is scientifically correct:
  Hawkes λ(t): continuous-time conditional intensity function over a point
    process of individual order events (nanosecond resolution).
  Amihud ILLIQ_t: discrete ratio of |return| / dollar_volume, averaged over
    D calendar days (OHLCV resolution).

  They model the same underlying phenomenon — liquidity-provider fear of
  informed or panicked order flow — but at incompatible temporal scales.
  No bijective variable substitution exists (Tier 1 failure), no corresponding
  mathematical objects exist between a kernel-driven intensity and a ratio
  statistic (Tier 2 failure), and despite describing the same psychology, the
  mechanisms cannot be unified without an explicit integration operator that
  aggregates instantaneous rates into daily quantities (Tier 3 failure).

  The bridge is Formula C: a Cumulative Integrated Hawkes Intensity operator.

What this script does:
  1. Verifies the Blackboard API is reachable
  2. Confirms both seed formulas exist (requires seed_corpus.py to have run)
  3. Checks whether the rejection edge already exists (idempotent)
  4. POSTs to /v1/rejections to create the REJECTED_ISOMORPHISM edge
  5. Verifies the edge is readable via GET /v1/rejections
  6. Prints the exact suggested_bridging_formula that Agent 201 will read

Run with:
  PYTHONPATH=. python scripts/seed_rejection.py

Requires:
  - Blackboard API running (docker-compose up api)
  - Seed corpus loaded (PYTHONPATH=. python scripts/seed_corpus.py)
"""

from __future__ import annotations

import asyncio
import sys
import uuid as uuid_lib
from typing import Optional

import httpx

# ---------------------------------------------------------------------------
# Rejection constants
# ---------------------------------------------------------------------------

# Identifies this as synthetic — Evolutionary Gardeners can filter real vs.
# seeded rejections using agent_version. Real Agent 105 uses "0.1.0".
SYNTHETIC_AGENT_VERSION = "0.0.0-synthetic"
AGENT_ID = "agent_105"

# The name substrings used to locate seed formulas in the Blackboard.
# Must match the 'name' field in seed_corpus.py.
HAWKES_NAME_SUBSTRING = "Hawkes Process Intensity (Baseline)"
AMIHUD_NAME_SUBSTRING = "Amihud Illiquidity Ratio"

# ---------------------------------------------------------------------------
# Rejection content — the scientific finding
# ---------------------------------------------------------------------------

TIER_1_REASONING = (
    "No bijective variable substitution exists between the two formulas. "
    "Hawkes λ(t) sums over a set of discrete event times {t_i < t} — an "
    "index over individual order arrivals at nanosecond resolution. "
    "Amihud ILLIQ_t averages over a set of calendar days {d = 1..D} — an "
    "index over trading sessions at daily resolution. "
    "These index sets are structurally incompatible: one is a random point "
    "process, the other is a deterministic calendar. No substitution φ: "
    "Var_Hawkes → Var_Amihud can make f_A(φ(x)) = f_B(x)."
)

TIER_2_REASONING = (
    "The mathematical objects in each formula play no analogous roles. "
    "Hawkes possesses: (1) a conditional intensity function λ(t|H_t), "
    "(2) an exponential decay kernel e^{-β(t-t_i)}, (3) a branching ratio "
    "α/β governing subcriticality, (4) a history-dependent feedback structure. "
    "Amihud possesses none of these — it is a ratio statistic |R_d|/VOL_d "
    "with a sample mean over a fixed window D. "
    "Hawkes exhibits self-excitation and critical phase transitions; Amihud "
    "is a descriptive aggregate with no memory kernel and no dynamical "
    "structure. The mathematical objects do not correspond."
)

TIER_3_REASONING = (
    "Both formulas map to the same behavioral mechanism: liquidity-provider "
    "fear of informed or panicked order flow. In Hawkes, α represents "
    "'contagion coefficient — how strongly one sell triggers the next.' "
    "In Amihud, |R_id|/VOL_id represents 'price conviction — how far "
    "participants pushed price per unit of capital committed.' "
    "These are descriptions of the same underlying psychology at incompatible "
    "temporal resolutions. Hawkes captures the instantaneous cascade dynamics "
    "of individual events (the derivative of fear). Amihud captures the "
    "aggregate daily illiquidity signal (the integral of fear). "
    "They describe the same river from different altitudes. The connection "
    "is real but cannot be formalized without an explicit temporal aggregation "
    "operator that bridges the two scales — which does not exist in the "
    "current Blackboard."
)

CONCLUSION = (
    "Synthesis is not possible for this pair in their current form. "
    "Hawkes λ(t) and Amihud ILLIQ_t are mathematically incommensurable: "
    "a continuous-time stochastic intensity function cannot be directly "
    "unified with a discrete daily ratio statistic without an intermediate "
    "aggregation step. The connection between them is scientifically real "
    "and behaviorally meaningful, but requires Formula C — a dedicated "
    "temporal aggregation operator — before Agent 105 can declare any "
    "tier of isomorphism. This rejection is not a dead end; it is a "
    "precise specification of the missing mathematical object."
)

# This is the research directive Agent 201 will read verbatim.
# It must name: (1) the mathematical structure, (2) the operation,
# (3) what it produces, (4) which Blackboard formulas it bridges.
SUGGESTED_BRIDGING_FORMULA = (
    "Cumulative Integrated Hawkes Intensity (CIHI): "
    "Λ_d = ∫_{t_open}^{t_close} λ(t | H_t) dt, "
    "where the integral aggregates the Hawkes conditional intensity over "
    "the full intraday trading session [t_open, t_close] for day d. "
    "Normalized form: CIHI_d = Λ_d / VOL_d, "
    "where VOL_d is the dollar volume of aggressive sell orders on day d. "
    "This produces a daily illiquidity measure that is structurally analogous "
    "to Amihud ILLIQ_d but derived from first-principles Hawkes dynamics. "
    "Mathematical wing: Measure Theory + Ergodic Theory (integration of "
    "stochastic processes over fixed intervals). "
    "When Formula C (CIHI) exists in the Blackboard, Agent 105 can declare "
    "a Tier 2 isomorphism between CIHI and Amihud (same mathematical role: "
    "daily aggregated illiquidity ratio) and a Tier 2 isomorphism between "
    "CIHI and Hawkes (same stochastic intensity, different temporal view). "
    "The Hawkes ↔ Amihud isomorphism then follows by transitivity via CIHI."
)


# ---------------------------------------------------------------------------
# Script
# ---------------------------------------------------------------------------


async def seed_rejection(api_base_url: str = "http://localhost:8000") -> None:
    async with httpx.AsyncClient(timeout=30.0) as client:

        # ------------------------------------------------------------------
        # 1. Health check
        # ------------------------------------------------------------------
        try:
            health = await client.get(f"{api_base_url}/health")
            health.raise_for_status()
        except (httpx.RequestError, httpx.HTTPStatusError) as exc:
            print(f"✗ Blackboard API unreachable at {api_base_url}: {exc}")
            print("  Run: docker-compose up -d api")
            sys.exit(1)
        print(f"✓ Blackboard API online: {health.json()['status']}")

        # ------------------------------------------------------------------
        # 2. Locate Hawkes Baseline UUID
        # ------------------------------------------------------------------
        hawkes_uuid = await _find_formula_uuid(client, api_base_url, HAWKES_NAME_SUBSTRING)
        if hawkes_uuid is None:
            print(f"✗ Could not find formula: '{HAWKES_NAME_SUBSTRING}'")
            print("  Run: PYTHONPATH=. python scripts/seed_corpus.py")
            sys.exit(1)
        print(f"✓ Hawkes Baseline: {hawkes_uuid}")

        # ------------------------------------------------------------------
        # 3. Locate Amihud UUID
        # ------------------------------------------------------------------
        amihud_uuid = await _find_formula_uuid(client, api_base_url, AMIHUD_NAME_SUBSTRING)
        if amihud_uuid is None:
            print(f"✗ Could not find formula: '{AMIHUD_NAME_SUBSTRING}'")
            print("  Run: PYTHONPATH=. python scripts/seed_corpus.py")
            sys.exit(1)
        print(f"✓ Amihud Illiquidity Ratio: {amihud_uuid}")

        # ------------------------------------------------------------------
        # 4. Idempotency check — skip if rejection already exists
        # ------------------------------------------------------------------
        existing = await client.get(f"{api_base_url}/v1/rejections")
        existing.raise_for_status()
        for r in existing.json():
            pair = {r.get("uuid_a"), r.get("uuid_b")}
            if pair == {hawkes_uuid, amihud_uuid}:
                print(
                    f"\n~ Rejection already exists for this pair "
                    f"(rejection_id: {r.get('rejection_id', 'unknown')})."
                )
                print("  Skipping write. Use GET /v1/rejections to inspect.")
                _print_agent_201_directive()
                return

        # ------------------------------------------------------------------
        # 5. POST the rejection
        # ------------------------------------------------------------------
        rejection_id = str(uuid_lib.uuid4())
        payload = {
            "uuid_a": hawkes_uuid,
            "uuid_b": amihud_uuid,
            "rejection_id": rejection_id,
            "agent_id": AGENT_ID,
            "agent_version": SYNTHETIC_AGENT_VERSION,
            "tier_1_result": "no_match",
            "tier_2_result": "no_match",
            "tier_3_result": "no_match",
            "conclusion": CONCLUSION,
            "suggested_bridging_formula": SUGGESTED_BRIDGING_FORMULA,
            "failure_mode": "temporal_scale_mismatch",
        }

        response = await client.post(f"{api_base_url}/v1/rejections", json=payload)

        if response.status_code == 201:
            result = response.json()
            print(f"\n✓ REJECTED_ISOMORPHISM edge created")
            print(f"  rejection_id: {result['rejection_id']}")
            print(f"  uuid_a (Hawkes):  {result['uuid_a']}")
            print(f"  uuid_b (Amihud):  {result['uuid_b']}")
        else:
            print(f"✗ POST /v1/rejections failed: {response.status_code}")
            print(f"  {response.text}")
            sys.exit(1)

        # ------------------------------------------------------------------
        # 6. Verify readability
        # ------------------------------------------------------------------
        verify = await client.get(f"{api_base_url}/v1/rejections")
        verify.raise_for_status()
        rejections = verify.json()

        matching = [
            r for r in rejections
            if {r.get("uuid_a"), r.get("uuid_b")} == {hawkes_uuid, amihud_uuid}
        ]

        if matching:
            print(f"\n✓ GET /v1/rejections confirms edge is readable")
            print(f"  Total rejections in graph: {len(rejections)}")
            print(f"  failure_mode: {matching[0].get('failure_mode', 'MISSING — schema not updated')}")
            print(f"  bridging_concept (truncated): {matching[0].get('bridging_concept', '')[:80]}...")
            if matching[0].get("failure_mode") != "temporal_scale_mismatch":
                print("✗ WARNING: failure_mode not stored correctly. Check neo4j_client.py update.")
                sys.exit(1)
        else:
            print("✗ Edge written but not readable. Check Neo4j connectivity.")
            sys.exit(1)

        # ------------------------------------------------------------------
        # 7. Agent 201 directive
        # ------------------------------------------------------------------
        _print_agent_201_directive()


async def _find_formula_uuid(
    client: httpx.AsyncClient,
    api_base_url: str,
    name_substring: str,
) -> Optional[str]:
    """
    Find a formula UUID by name substring, searching across all statuses.
    Returns None if not found.
    """
    for status in ("hypothesis", "syntactically_correct", "formally_verified"):
        try:
            resp = await client.get(
                f"{api_base_url}/v1/formulas",
                params={"status": status},
            )
            if resp.status_code == 400:
                continue
            resp.raise_for_status()
            for formula in resp.json():
                if name_substring.lower() in (formula.get("name") or "").lower():
                    return formula["uuid"]
        except (httpx.RequestError, httpx.HTTPStatusError):
            continue
    return None


def _print_agent_201_directive() -> None:
    print("\n" + "=" * 70)
    print("AGENT 201 RESEARCH DIRECTIVE")
    print("=" * 70)
    print("The following suggested_bridging_formula is now in the Blackboard.")
    print("Agent 201 (Temporal Scale Bridger) will read this verbatim:\n")
    print(SUGGESTED_BRIDGING_FORMULA)
    print("\n" + "=" * 70)
    print("Next step: Build Agent 201 to operationalize this into Formula C.")
    print("Query: GET /v1/rejections?agent_version=0.0.0-synthetic")
    print("=" * 70)


if __name__ == "__main__":
    url = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"
    asyncio.run(seed_rejection(url))
