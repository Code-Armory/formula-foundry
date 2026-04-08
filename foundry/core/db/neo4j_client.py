"""
Blackboard Client — Neo4j implementation of the Consensus Mesh.

Authority model enforced here:
  Layer 1 (Specialists)  → PROPOSE  → sets HYPOTHESIS or SYNTACTICALLY_CORRECT
                                       (SYNTACTICALLY_CORRECT earned via SymPy gate)
  Layer 2 (Synthesizers) → MERGE    → sets SYNTACTICALLY_CORRECT, creates CROSS_LINKED edges
  Layer 3 (Auditors)     → VERIFY   → sets FORMALLY_VERIFIED or FALSIFIED (MASTER_LOCK)
  SYSTEM                 → SEED     → bypasses authority (corpus injection only)

Graph Schema
------------
Nodes:
  (:Formula)  — one node per UUID (formula version)
  (:Agent)    — one node per agent_id
  (:Tag)      — one node per tag string

Relationships:
  (Agent)-[:PROPOSED  {timestamp}]→(Formula)
  (Agent)-[:MERGED    {timestamp}]→(Formula)
  (Agent)-[:VERIFIED  {timestamp, status}]→(Formula)
  (Formula)-[:EVOLVED_FROM]→(Formula)               — lineage chain
  (Formula)-[:CROSS_LINKED {description}]→(Formula) — isomorphism
  (Formula)-[:REJECTED_ISOMORPHISM {...}]→(Formula)  — null hypothesis record
  (Formula)-[:TAGGED]→(Tag)
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

from neo4j import AsyncGraphDatabase, AsyncDriver
from neo4j.time import DateTime as Neo4jDateTime

from foundry.core.schema.formula import FormulaDNA, ProofStatus, AgentLayer

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Authority table
# ---------------------------------------------------------------------------
#
# SYNTACTICALLY_CORRECT includes LAYER_1 because Layer 1 Specialist agents
# run the SymPy validation gate INSIDE their own tool loop before proposing.
# Passing SymPy earns SYNTACTICALLY_CORRECT regardless of layer.
# The meaningful restriction is FORMALLY_VERIFIED, which requires LAYER_3 only.

_WRITE_AUTHORITY: Dict[ProofStatus, List[AgentLayer]] = {
    ProofStatus.HYPOTHESIS: [
        AgentLayer.LAYER_1, AgentLayer.LAYER_2, AgentLayer.LAYER_3, AgentLayer.SYSTEM,
    ],
    ProofStatus.SYNTACTICALLY_CORRECT: [
        AgentLayer.LAYER_1, AgentLayer.LAYER_2, AgentLayer.LAYER_3,
    ],
    ProofStatus.FORMALLY_VERIFIED: [AgentLayer.LAYER_3],
    ProofStatus.FALSIFIED: [AgentLayer.LAYER_3],
    ProofStatus.ARCHIVED: [AgentLayer.LAYER_2, AgentLayer.LAYER_3],
}


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class BlackboardClient:
    """
    Async Neo4j client for the Formula Foundry Blackboard.
    One instance shared across the FastAPI application lifespan.
    """

    def __init__(self, uri: str, user: str, password: str) -> None:
        self._driver: AsyncDriver = AsyncGraphDatabase.driver(
            uri, auth=(user, password), max_connection_pool_size=50
        )

    async def verify_connectivity(self) -> None:
        await self._driver.verify_connectivity()
        logger.info("Blackboard: Neo4j connectivity verified")

    async def close(self) -> None:
        await self._driver.close()
        logger.info("Blackboard: Neo4j connection closed")

    # -------------------------------------------------------------------------
    # Schema initialization (idempotent)
    # -------------------------------------------------------------------------

    async def initialize_schema(self) -> None:
        """Create uniqueness constraints and performance indexes."""
        async with self._driver.session() as session:
            constraints = [
                "CREATE CONSTRAINT formula_uuid IF NOT EXISTS FOR (f:Formula) REQUIRE f.uuid IS UNIQUE",
                "CREATE CONSTRAINT agent_id IF NOT EXISTS FOR (a:Agent) REQUIRE a.agent_id IS UNIQUE",
                "CREATE CONSTRAINT tag_name IF NOT EXISTS FOR (t:Tag) REQUIRE t.name IS UNIQUE",
            ]
            indexes = [
                "CREATE INDEX formula_status IF NOT EXISTS FOR (f:Formula) ON (f.proof_status)",
                "CREATE INDEX formula_created IF NOT EXISTS FOR (f:Formula) ON (f.created_at)",
                "CREATE INDEX formula_version IF NOT EXISTS FOR (f:Formula) ON (f.version)",
            ]
            for stmt in constraints + indexes:
                await session.run(stmt)
        logger.info("Blackboard: Schema initialized")

    # -------------------------------------------------------------------------
    # Write operations
    # -------------------------------------------------------------------------

    async def seed_formula(self, formula: FormulaDNA) -> str:
        """SYSTEM authority: inject a known formula into the seed corpus."""
        async with self._driver.session() as session:
            await session.execute_write(_write_formula_tx, formula, "SYSTEM")
        logger.info("Blackboard: Seeded formula %s (%s)", formula.uuid, formula.name)
        return formula.uuid

    async def propose_formula(
        self,
        formula: FormulaDNA,
        proposing_agent_id: str,
        agent_layer: AgentLayer = AgentLayer.LAYER_1,
    ) -> str:
        """Layer 1/2 operation: agent proposes a new formula."""
        _assert_write_authority(formula.proof_status, agent_layer, proposing_agent_id)
        async with self._driver.session() as session:
            await session.execute_write(_write_formula_tx, formula, proposing_agent_id)
        logger.info(
            "Blackboard: Agent %s proposed formula %s", proposing_agent_id, formula.uuid
        )
        return formula.uuid

    async def update_proof_status(
        self,
        uuid: str,
        new_status: ProofStatus,
        agent_id: str,
        agent_layer: AgentLayer = AgentLayer.LAYER_3,
        lean4_encoding: str | None = None,
    ) -> None:
        """Update proof status with authority enforcement."""
        _assert_write_authority(new_status, agent_layer, agent_id)
        async with self._driver.session() as session:
            # Conditionally SET lean4_encoding when provided (FORMALLY_VERIFIED)
            lean4_clause = ", f.lean4_encoding = $lean4" if lean4_encoding else ""
            query = f"""
                MATCH (f:Formula {{uuid: $uuid}})
                SET f.proof_status = $status, f.updated_at = $now{lean4_clause}
                WITH f
                MERGE (a:Agent {{agent_id: $agent_id}})
                MERGE (a)-[:VERIFIED {{timestamp: $now, status: $status}}]->(f)
                RETURN f.uuid AS uuid
                """
            params = dict(
                uuid=uuid,
                status=new_status.value,
                agent_id=agent_id,
                now=_utcnow_iso(),
            )
            if lean4_encoding:
                params["lean4"] = lean4_encoding
            result = await session.run(query, **params)
            record = await result.single()
            if not record:
                raise KeyError(f"Formula {uuid} not found in Blackboard")
        logger.info(
            "Blackboard: Agent %s updated %s → %s", agent_id, uuid, new_status.value
        )
    async def cross_link(
        self,
        uuid_a: str,
        uuid_b: str,
        agent_id: str,
        isomorphism_description: str,
        agent_layer: AgentLayer = AgentLayer.LAYER_2,
    ) -> None:
        """Layer 2 operation: declare structural isomorphism between two formulas."""
        if agent_layer == AgentLayer.LAYER_1:
            raise PermissionError(
                f"Agent {agent_id} (layer1) cannot create cross-links. Requires layer2+."
            )
        async with self._driver.session() as session:
            await session.run(
                """
                MATCH (a:Formula {uuid: $uuid_a})
                MATCH (b:Formula {uuid: $uuid_b})
                MERGE (a)-[:CROSS_LINKED {
                    agent_id: $agent_id,
                    description: $description,
                    timestamp: $now
                }]->(b)
                MERGE (b)-[:CROSS_LINKED {
                    agent_id: $agent_id,
                    description: $description,
                    timestamp: $now
                }]->(a)
                """,
                uuid_a=uuid_a,
                uuid_b=uuid_b,
                agent_id=agent_id,
                description=isomorphism_description,
                now=_utcnow_iso(),
            )
        logger.info("Blackboard: %s cross-linked %s ↔ %s", agent_id, uuid_a, uuid_b)

    async def append_verification_attempt(
        self,
        uuid: str,
        attempt_json: Dict[str, Any],
    ) -> None:
        """Append one VerificationAttempt record to a formula."""
        async with self._driver.session() as session:
            result = await session.run(
                "MATCH (f:Formula {uuid: $uuid}) RETURN f.verification_attempts AS va",
                uuid=uuid,
            )
            record = await result.single()
            if not record:
                raise KeyError(f"Formula {uuid} not found")
            existing = json.loads(record["va"] or "[]")
            existing.append(attempt_json)
            await session.run(
                """
                MATCH (f:Formula {uuid: $uuid})
                SET f.verification_attempts = $attempts, f.updated_at = $now
                """,
                uuid=uuid,
                attempts=json.dumps(existing),
                now=_utcnow_iso(),
            )

    # -------------------------------------------------------------------------
    # Read operations
    # -------------------------------------------------------------------------

    async def get_formula(self, uuid: str) -> Optional[FormulaDNA]:
        async with self._driver.session() as session:
            result = await session.run(
                "MATCH (f:Formula {uuid: $uuid}) RETURN f", uuid=uuid
            )
            record = await result.single()
            if not record:
                return None
            return _deserialize_formula(dict(record["f"]))

    async def get_lineage(self, uuid: str) -> List[FormulaDNA]:
        async with self._driver.session() as session:
            result = await session.run(
                """
                MATCH (target:Formula {uuid: $uuid})
                MATCH path = (target)-[:EVOLVED_FROM*0..50]->(ancestor:Formula)
                UNWIND nodes(path) AS node
                RETURN DISTINCT node AS f
                ORDER BY node.version ASC
                """,
                uuid=uuid,
            )
            records = await result.data()
            return [_deserialize_formula(dict(r["f"])) for r in records]

    async def get_cross_links(self, uuid: str) -> List[FormulaDNA]:
        async with self._driver.session() as session:
            result = await session.run(
                """
                MATCH (f:Formula {uuid: $uuid})-[:CROSS_LINKED]-(linked:Formula)
                RETURN DISTINCT linked AS f
                """,
                uuid=uuid,
            )
            records = await result.data()
            return [_deserialize_formula(dict(r["f"])) for r in records]

    async def get_pair_relationship(self, uuid_a: str, uuid_b: str) -> Optional[str]:
        """Returns 'CROSS_LINKED', 'REJECTED_ISOMORPHISM', or None."""
        async with self._driver.session() as session:
            result = await session.run(
                """
                MATCH (a:Formula {uuid: $uuid_a})-[r]-(b:Formula {uuid: $uuid_b})
                WHERE type(r) IN ['CROSS_LINKED', 'REJECTED_ISOMORPHISM']
                RETURN type(r) AS rel_type
                LIMIT 1
                """,
                uuid_a=uuid_a,
                uuid_b=uuid_b,
            )
            record = await result.single()
            return record["rel_type"] if record else None

    async def query_by_status(self, status: ProofStatus) -> List[FormulaDNA]:
        async with self._driver.session() as session:
            result = await session.run(
                "MATCH (f:Formula {proof_status: $status}) RETURN f ORDER BY f.created_at DESC",
                status=status.value,
            )
            records = await result.data()
            return [_deserialize_formula(dict(r["f"])) for r in records]

    async def query_by_tag(self, tag: str) -> List[FormulaDNA]:
        async with self._driver.session() as session:
            result = await session.run(
                """
                MATCH (f:Formula)-[:TAGGED]->(:Tag {name: $tag})
                RETURN f ORDER BY f.created_at DESC
                """,
                tag=tag,
            )
            records = await result.data()
            return [_deserialize_formula(dict(r["f"])) for r in records]

    async def query_by_agent(self, agent_id: str) -> List[FormulaDNA]:
        async with self._driver.session() as session:
            result = await session.run(
                """
                MATCH (:Agent {agent_id: $agent_id})-[:PROPOSED|:MERGED]->(f:Formula)
                RETURN DISTINCT f ORDER BY f.created_at DESC
                """,
                agent_id=agent_id,
            )
            records = await result.data()
            return [_deserialize_formula(dict(r["f"])) for r in records]

    async def get_ip_library(self) -> List[FormulaDNA]:
        return await self.query_by_status(ProofStatus.FORMALLY_VERIFIED)

    async def record_rejection(
        self,
        uuid_a: str,
        uuid_b: str,
        rejection_id: str,
        agent_id: str,
        agent_version: str,
        tier_1_result: str,
        tier_2_result: str,
        tier_3_result: str,
        conclusion: str,
        bridging_concept: str,
        failure_mode: str = "unclassified",
    ) -> str:
        """Persist a formal isomorphism rejection as a REJECTED_ISOMORPHISM edge."""
        async with self._driver.session() as session:
            result = await session.run(
                """
                MATCH (a:Formula {uuid: $uuid_a})
                MATCH (b:Formula {uuid: $uuid_b})
                MERGE (a)-[r:REJECTED_ISOMORPHISM {rejection_id: $rejection_id}]->(b)
                SET r.evaluated_by    = $agent_id,
                    r.agent_version   = $agent_version,
                    r.timestamp       = $now,
                    r.tier_1_result   = $tier_1_result,
                    r.tier_2_result   = $tier_2_result,
                    r.tier_3_result   = $tier_3_result,
                    r.conclusion      = $conclusion,
                    r.bridging_concept = $bridging_concept,
                    r.failure_mode    = $failure_mode
                RETURN r.rejection_id AS rid
                """,
                uuid_a=uuid_a,
                uuid_b=uuid_b,
                rejection_id=rejection_id,
                agent_id=agent_id,
                agent_version=agent_version,
                now=_utcnow_iso(),
                tier_1_result=tier_1_result,
                tier_2_result=tier_2_result,
                tier_3_result=tier_3_result,
                conclusion=conclusion,
                bridging_concept=bridging_concept,
                failure_mode=failure_mode,
            )
            record = await result.single()
            if not record:
                raise KeyError(
                    f"Could not create REJECTED_ISOMORPHISM edge between {uuid_a} and {uuid_b}. "
                    "Both Formula nodes must exist in the Blackboard."
                )
        logger.info(
            "Blackboard: Rejection recorded %s ↔ %s by %s v%s (mode: %s)",
            uuid_a, uuid_b, agent_id, agent_version, failure_mode,
        )
        return rejection_id

    async def get_unresolved_rejections(
        self,
        agent_version: Optional[str] = None,
        failure_mode: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Return all REJECTED_ISOMORPHISM edges.
        Optionally filtered by agent_version and/or failure_mode.

        Agent 003 (Evolution Trigger Monitor) queries with failure_mode to
        fetch only the rejections it is responsible for routing.
        """
        async with self._driver.session() as session:
            if agent_version and failure_mode:
                result = await session.run(
                    """
                    MATCH (a:Formula)-[r:REJECTED_ISOMORPHISM]->(b:Formula)
                    WHERE r.agent_version = $agent_version
                      AND r.failure_mode  = $failure_mode
                    RETURN a.uuid AS uuid_a, b.uuid AS uuid_b,
                           r.rejection_id AS rejection_id,
                           r.agent_version AS agent_version,
                           r.bridging_concept AS bridging_concept,
                           r.failure_mode AS failure_mode,
                           r.timestamp AS timestamp
                    ORDER BY r.timestamp DESC
                    """,
                    agent_version=agent_version,
                    failure_mode=failure_mode,
                )
            elif agent_version:
                result = await session.run(
                    """
                    MATCH (a:Formula)-[r:REJECTED_ISOMORPHISM {agent_version: $agent_version}]->(b:Formula)
                    RETURN a.uuid AS uuid_a, b.uuid AS uuid_b,
                           r.rejection_id AS rejection_id,
                           r.agent_version AS agent_version,
                           r.bridging_concept AS bridging_concept,
                           r.failure_mode AS failure_mode,
                           r.timestamp AS timestamp
                    ORDER BY r.timestamp DESC
                    """,
                    agent_version=agent_version,
                )
            elif failure_mode:
                result = await session.run(
                    """
                    MATCH (a:Formula)-[r:REJECTED_ISOMORPHISM {failure_mode: $failure_mode}]->(b:Formula)
                    RETURN a.uuid AS uuid_a, b.uuid AS uuid_b,
                           r.rejection_id AS rejection_id,
                           r.agent_version AS agent_version,
                           r.bridging_concept AS bridging_concept,
                           r.failure_mode AS failure_mode,
                           r.timestamp AS timestamp
                    ORDER BY r.timestamp DESC
                    """,
                    failure_mode=failure_mode,
                )
            else:
                result = await session.run(
                    """
                    MATCH (a:Formula)-[r:REJECTED_ISOMORPHISM]->(b:Formula)
                    RETURN a.uuid AS uuid_a, b.uuid AS uuid_b,
                           r.rejection_id AS rejection_id,
                           r.agent_version AS agent_version,
                           r.bridging_concept AS bridging_concept,
                           r.failure_mode AS failure_mode,
                           r.timestamp AS timestamp
                    ORDER BY r.timestamp DESC
                    """
                )
            records = await result.data()
            return [dict(r) for r in records]


# ---------------------------------------------------------------------------
# Transaction functions
# ---------------------------------------------------------------------------


async def _write_formula_tx(tx: Any, formula: FormulaDNA, agent_id: str) -> None:
    data = formula.model_dump(mode="json")
    json_fields = [
        "behavioral_mappings", "empirical_traces", "verification_attempts",
        "axiomatic_origin", "mathematical_wing", "isomorphic_to", "tags",
    ]
    for field in json_fields:
        data[field] = json.dumps(data[field])
    data["created_at"] = _to_iso(data.get("created_at"))
    data["updated_at"] = _to_iso(data.get("updated_at"))
    await tx.run(
        """
        MERGE (f:Formula {uuid: $uuid})
        SET f += $props
        WITH f
        MERGE (a:Agent {agent_id: $agent_id})
        MERGE (a)-[:PROPOSED {timestamp: $now}]->(f)
        """,
        uuid=formula.uuid,
        props=data,
        agent_id=agent_id,
        now=_utcnow_iso(),
    )
    if formula.parent_uuid:
        await tx.run(
            """
            MATCH (parent:Formula {uuid: $parent_uuid})
            MATCH (child:Formula {uuid: $child_uuid})
            MERGE (child)-[:EVOLVED_FROM]->(parent)
            """,
            parent_uuid=formula.parent_uuid,
            child_uuid=formula.uuid,
        )
    for tag in formula.tags:
        await tx.run(
            """
            MERGE (t:Tag {name: $tag})
            WITH t
            MATCH (f:Formula {uuid: $uuid})
            MERGE (f)-[:TAGGED]->(t)
            """,
            tag=tag,
            uuid=formula.uuid,
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _assert_write_authority(
    target_status: ProofStatus,
    agent_layer: AgentLayer,
    agent_id: str,
) -> None:
    allowed = _WRITE_AUTHORITY.get(target_status, [])
    if agent_layer not in allowed:
        raise PermissionError(
            f"Agent '{agent_id}' ({agent_layer.value}) is not authorized to write "
            f"status '{target_status}'. Required layers: {[l.value for l in allowed]}"
        )


def _utcnow_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


def _to_iso(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, Neo4jDateTime):
        return value.isoformat()
    if isinstance(value, str):
        return value
    return str(value)


def _deserialize_formula(data: Dict[str, Any]) -> FormulaDNA:
    json_fields = [
        "behavioral_mappings", "empirical_traces", "verification_attempts",
        "axiomatic_origin", "mathematical_wing", "isomorphic_to", "tags",
    ]
    for field in json_fields:
        if field in data and isinstance(data[field], str):
            data[field] = json.loads(data[field])
    for field in ("created_at", "updated_at"):
        if field in data:
            data[field] = _to_iso(data[field])
    return FormulaDNA(**data)
