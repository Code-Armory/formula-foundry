"""
Blackboard API — FastAPI layer over the Neo4j Consensus Mesh.

Endpoints enforce the three-layer authority model:
  Layer 0 (Librarian)    — GET /v1/librarian/next-pair (orchestration)
  Layer 1 (Specialists)  — POST /v1/formulas (HYPOTHESIS / SYNTACTICALLY_CORRECT)
  Layer 2 (Synthesizers) — POST /v1/formulas/{uuid}/cross-link
  Layer 3 (Auditors)     — PATCH /v1/formulas/{uuid}/status (FORMALLY_VERIFIED)
  SYSTEM                 — POST /v1/formulas/seed
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from foundry.agents.orchestration.librarian import LibrarianRouter, RoutingDecision
from foundry.core.db.neo4j_client import BlackboardClient
from foundry.core.schema.formula import AgentLayer, FormulaDNA, ProofStatus

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Application state
# ---------------------------------------------------------------------------

_blackboard: Optional[BlackboardClient] = None


def get_blackboard() -> BlackboardClient:
    if _blackboard is None:
        raise RuntimeError("Blackboard not initialized. Application startup incomplete.")
    return _blackboard


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _blackboard
    _blackboard = BlackboardClient(
        uri=os.environ["NEO4J_URI"],
        user=os.environ["NEO4J_USER"],
        password=os.environ["NEO4J_PASSWORD"],
    )
    await _blackboard.verify_connectivity()
    await _blackboard.initialize_schema()
    logger.info("Formula Foundry Blackboard API: ready")
    yield
    await _blackboard.close()
    logger.info("Formula Foundry Blackboard API: shutdown complete")


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------


app = FastAPI(
    title="Formula Foundry — Blackboard API",
    description=(
        "The IP ledger and Consensus Mesh for the Mathematical Think Tank. "
        "Every formula ever generated, verified, or falsified lives here."
    ),
    version="0.2.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Request / Response contracts
# ---------------------------------------------------------------------------


class ProposeFormulaRequest(BaseModel):
    formula: FormulaDNA
    proposing_agent_id: str
    agent_layer: AgentLayer = AgentLayer.LAYER_1


class UpdateStatusRequest(BaseModel):
    new_status: ProofStatus
    agent_id: str
    agent_layer: AgentLayer = AgentLayer.LAYER_3
    lean4_encoding: Optional[str] = None


class CrossLinkRequest(BaseModel):
    uuid_b: str
    agent_id: str
    isomorphism_description: str
    agent_layer: AgentLayer = AgentLayer.LAYER_2
    tier_level: Optional[int] = None
    transformation_map: Optional[str] = None


class RejectionRequest(BaseModel):
    uuid_a: str
    uuid_b: str
    rejection_id: str
    agent_id: str
    agent_version: str
    tier_1_result: str
    tier_2_result: str
    tier_3_result: str
    conclusion: str
    suggested_bridging_formula: str
    failure_mode: str = "unclassified"   # RejectionFailureMode value; default for backward compat


class FormulaResponse(BaseModel):
    uuid: str
    proof_status: str
    message: str


class LibrarianPairResponse(BaseModel):
    available: bool
    uuid_a: Optional[str] = None
    uuid_b: Optional[str] = None
    formula_a_name: Optional[str] = None
    formula_b_name: Optional[str] = None
    score: Optional[float] = None
    reasoning: Optional[str] = None
    candidates_evaluated: Optional[int] = None
    pairs_excluded: Optional[int] = None
    reason: Optional[str] = None


# ---------------------------------------------------------------------------
# Routes — Infrastructure
# ---------------------------------------------------------------------------


@app.get("/health", tags=["Infrastructure"])
async def health_check():
    return {"status": "alive", "component": "blackboard", "version": "0.2.0"}


@app.get("/schema", tags=["Infrastructure"])
async def describe_schema():
    return {
        "formula_dna_fields": {
            "uuid": "Unique identifier per formula version",
            "version": "Monotonically increasing per lineage chain",
            "parent_uuid": "UUID of formula this evolved from",
            "symbolic_expression_latex": "The formula in LaTeX (always required)",
            "symbolic_expression_sympy": "SymPy string — set after Stage 1 verification",
            "lean4_encoding": "Lean 4 proof — set only after FORMALLY_VERIFIED",
            "behavioral_claim": "Natural language psychological mechanism statement",
            "behavioral_mappings": "Variable → psychological state semantic bridge",
            "axiomatic_origin": "Agent IDs that contributed",
            "mathematical_wing": "Mathematical domains drawn from",
            "proof_status": "Pipeline state: hypothesis → formally_verified",
        },
        "proof_status_lifecycle": [
            "hypothesis → syntactically_correct → formally_verified",
            "hypothesis → falsified → archived",
        ],
        "authority_model": {
            "layer0_librarian": "Orchestration — selects pairs for synthesis",
            "layer1_specialists": "Can propose (write hypothesis or syntactically_correct via SymPy gate)",
            "layer2_synthesizers": "Can merge, cross-link; Evolutionary agents propose bridging formulas",
            "layer3_auditors": "Master lock — can formally verify or falsify",
            "system": "Seed corpus injection only",
        },
    }


# ---------------------------------------------------------------------------
# Routes — Seed Corpus
# ---------------------------------------------------------------------------


@app.post(
    "/v1/formulas/seed",
    response_model=FormulaResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["Seed Corpus"],
)
async def seed_formula(formula: FormulaDNA):
    bb = get_blackboard()
    uuid = await bb.seed_formula(formula)
    return FormulaResponse(uuid=uuid, proof_status=formula.proof_status, message="seeded")


# ---------------------------------------------------------------------------
# Routes — Formula Lifecycle
# ---------------------------------------------------------------------------


@app.post(
    "/v1/formulas",
    response_model=FormulaResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["Formulas"],
)
async def propose_formula(req: ProposeFormulaRequest):
    bb = get_blackboard()
    try:
        uuid = await bb.propose_formula(
            formula=req.formula,
            proposing_agent_id=req.proposing_agent_id,
            agent_layer=req.agent_layer,
        )
    except PermissionError as e:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(e))
    return FormulaResponse(uuid=uuid, proof_status=req.formula.proof_status, message="proposed")


@app.get("/v1/formulas/{uuid}", response_model=FormulaDNA, tags=["Formulas"])
async def get_formula(uuid: str):
    bb = get_blackboard()
    formula = await bb.get_formula(uuid)
    if not formula:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Formula '{uuid}' not found")
    return formula


@app.get("/v1/formulas/{uuid}/lineage", response_model=List[FormulaDNA], tags=["Formulas"])
async def get_lineage(uuid: str):
    bb = get_blackboard()
    chain = await bb.get_lineage(uuid)
    if not chain:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Formula '{uuid}' not found")
    return chain


@app.get("/v1/formulas/{uuid}/cross-links", response_model=List[FormulaDNA], tags=["Formulas"])
async def get_cross_links(uuid: str):
    bb = get_blackboard()
    return await bb.get_cross_links(uuid)


@app.patch("/v1/formulas/{uuid}/status", response_model=FormulaResponse, tags=["Formulas"])
async def update_status(uuid: str, req: UpdateStatusRequest):
    bb = get_blackboard()
    try:
        await bb.update_proof_status(
            uuid=uuid,
            new_status=req.new_status,
            agent_id=req.agent_id,
            agent_layer=req.agent_layer,
            lean4_encoding=req.lean4_encoding,
        )
    except PermissionError as e:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(e))
    except KeyError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    return FormulaResponse(uuid=uuid, proof_status=req.new_status.value, message="updated")


@app.post("/v1/formulas/{uuid}/cross-link", response_model=dict, tags=["Formulas"])
async def cross_link(uuid: str, req: CrossLinkRequest):
    bb = get_blackboard()
    try:
        await bb.cross_link(
            uuid_a=uuid,
            uuid_b=req.uuid_b,
            agent_id=req.agent_id,
            isomorphism_description=req.isomorphism_description,
            agent_layer=req.agent_layer,
        )
    except PermissionError as e:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(e))
    return {"linked": [uuid, req.uuid_b], "agent": req.agent_id,
            "description": req.isomorphism_description}


# ---------------------------------------------------------------------------
# Routes — Query
# ---------------------------------------------------------------------------


@app.get("/v1/formulas", response_model=List[FormulaDNA], tags=["Query"])
async def query_formulas(
    proof_status: Optional[ProofStatus] = Query(None, alias="status"),
    tag: Optional[str] = Query(None),
    agent_id: Optional[str] = Query(None),
):
    bb = get_blackboard()
    if not any([proof_status, tag, agent_id]):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Provide at least one filter: status, tag, or agent_id")
    if proof_status:
        return await bb.query_by_status(proof_status)
    if tag:
        return await bb.query_by_tag(tag)
    if agent_id:
        return await bb.query_by_agent(agent_id)
    return []


@app.get("/v1/ip-library", response_model=List[FormulaDNA], tags=["Query"])
async def get_ip_library():
    bb = get_blackboard()
    return await bb.get_ip_library()


# ---------------------------------------------------------------------------
# Routes — Rejection Records
# ---------------------------------------------------------------------------


@app.post("/v1/rejections", response_model=dict, status_code=status.HTTP_201_CREATED,
          tags=["Rejections"])
async def record_rejection(req: RejectionRequest):
    bb = get_blackboard()
    try:
        rejection_id = await bb.record_rejection(
            uuid_a=req.uuid_a,
            uuid_b=req.uuid_b,
            rejection_id=req.rejection_id,
            agent_id=req.agent_id,
            agent_version=req.agent_version,
            tier_1_result=req.tier_1_result,
            tier_2_result=req.tier_2_result,
            tier_3_result=req.tier_3_result,
            conclusion=req.conclusion,
            bridging_concept=req.suggested_bridging_formula,
            failure_mode=req.failure_mode,
        )
    except KeyError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    return {"rejection_id": rejection_id, "uuid_a": req.uuid_a,
            "uuid_b": req.uuid_b, "message": "rejection_recorded"}


@app.get("/v1/rejections", response_model=List[dict], tags=["Rejections"])
async def get_rejections(
    agent_version: Optional[str] = Query(None),
    failure_mode: Optional[str] = Query(None),
):
    """
    Returns all REJECTED_ISOMORPHISM edges.
    Filter by agent_version, failure_mode, or both.
    Agent 003 uses failure_mode to fetch only the rejections it is routing.
    """
    bb = get_blackboard()
    return await bb.get_unresolved_rejections(
        agent_version=agent_version,
        failure_mode=failure_mode,
    )


# ---------------------------------------------------------------------------
# Routes — Librarian Router (Layer 0 Orchestration)
# ---------------------------------------------------------------------------


@app.get("/v1/librarian/next-pair", response_model=LibrarianPairResponse, tags=["Librarian"])
async def librarian_next_pair():
    """
    Layer 0 — Orchestration inspection endpoint.
    Invokes the Librarian Router and returns the highest-scoring unevaluated
    formula pair. Read-only — does not modify any state.

    BLACKBOARD_SELF_URL: override when localhost does not resolve correctly
    inside Docker (e.g. in multi-host deployments).
    """
    self_url = os.environ.get("BLACKBOARD_SELF_URL", "http://localhost:8000")
    router = LibrarianRouter(blackboard_api_url=self_url)
    decision = await router.select_next_pair()

    if decision is None:
        return LibrarianPairResponse(
            available=False,
            reason=(
                "No eligible pairs available. Either fewer than 2 formulas exist "
                "with status syntactically_correct or formally_verified, or all "
                "pairs have already been evaluated."
            ),
        )

    return LibrarianPairResponse(
        available=True,
        uuid_a=decision.uuid_a,
        uuid_b=decision.uuid_b,
        formula_a_name=decision.formula_a_name,
        formula_b_name=decision.formula_b_name,
        score=decision.score,
        reasoning=decision.reasoning,
        candidates_evaluated=decision.candidates_evaluated,
        pairs_excluded=decision.pairs_excluded,
    )
