"""
Agent 205 — Information Geometry Gardener

Authority: Layer 2 — Evolutionary Gardener. Can write SYNTACTICALLY_CORRECT formulas.
           Cannot formally verify (that is Agent 151's job).

Resolves: INFORMATION_GEOMETRY_MISMATCH

Mission:
  Agent 105 has rejected a pairing between a game-theoretic strategic scalar
  (Kyle's Lambda, tags: game_theory / adverse_selection) and an
  information-theoretic probability distribution (Shannon entropy, tags:
  entropy / information_theory). The two formulas live in orthogonal
  mathematical domains: mechanism design produces scalars via regression
  equilibria; information theory produces distributions via probability
  measures over order flow.

  The bridge is Information Geometry: the Fisher Information metric
  establishes a Riemannian structure on the space of probability
  distributions that connects the market maker's signal-to-noise ratio
  (Kyle's Lambda) to the Shannon entropy of the order flow distribution.

  Agent 205 constructs Formula C — the Information-Geometry-Adjusted
  Adverse Selection Coefficient:

      λ_info = λ₀ · exp(−H_OFI)

  Interpretation:
    λ₀      = baseline Kyle's Lambda (OLS coefficient from 30-minute bar)
    H_OFI   = Shannon entropy of the order flow distribution (from Agent 051)
    λ_info  = adjusted adverse selection coefficient

  When H_OFI is high (many uninformed traders, deep liquidity, high noise),
  the market maker correctly discounts λ per unit of flow — the exponential
  suppression formalizes this. When entropy collapses (informed flow dominates,
  liquidity fragments), λ_info → λ₀.

  This formula is the dual bridge: it translates both directions. Given
  observed λ_info and known λ₀, one can infer the structural entropy state
  of the order book. Given observed entropy, one can bound the effective
  adverse selection risk.

Lean 4 target:
  λ_info > 0 when λ₀ > 0.
  Proof: mul_pos h0 (Real.exp_pos _)
  Requires: Seed Proof 6 (added to Agent 151 in Phase 3).
  No measure theory. No MeasureTheory or ProbabilityTheory imports required.

Domain lock:
  Information Geometry ONLY. The bridge between Game Theory and Information
  Theory. This agent MUST NOT reproduce Kyle's Lambda (Game Theory only),
  MUST NOT reproduce the Shannon entropy formula (Information Theory only),
  and MUST NOT introduce Hawkes processes (Functional Analysis — Agent 089).

Semantic guard:
  suggested_bridging_formula in the rejection MUST contain language about
  information geometry, Fisher information, or strategic/entropic duality.
  The output formula MUST claim structural equivalence between the adverse
  selection regime and the entropy state — NOT syntactic identity.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Set

from foundry.agents.base import AgentConfig, BaseAgent
from foundry.core.schema.formula import (
    AgentLayer,
    BehavioralMapping,
    FormulaDNA,
    MathematicalWing,
    ProofStatus,
)

logger = logging.getLogger(__name__)
AGENT_VERSION = "0.1.0"


# ---------------------------------------------------------------------------
# Tool schemas
# ---------------------------------------------------------------------------

_FETCH_REJECTION_TOOL = {
    "name": "fetch_rejection_data",
    "description": (
        "Retrieve the REJECTED_ISOMORPHISM record. "
        "MUST be the first tool call. "
        "Read suggested_bridging_formula carefully — it contains the Agent 105 "
        "diagnosis of why Kyle's Lambda and Shannon entropy cannot connect directly. "
        "This diagnosis is your research directive."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "rejection_id": {"type": "string"},
        },
        "required": ["rejection_id"],
    },
}

_FETCH_FORMULA_TOOL = {
    "name": "fetch_formula_data",
    "description": (
        "Retrieve the complete FormulaDNA for a formula UUID. "
        "Call for BOTH parent UUIDs before proposing Formula C. "
        "Study symbolic_expression_latex and behavioral_mappings carefully — "
        "Formula C must span BOTH variable spaces while residing in neither alone."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "uuid": {"type": "string"},
        },
        "required": ["uuid"],
    },
}

_VALIDATE_SYMPY_TOOL = {
    "name": "validate_sympy_expression",
    "description": (
        "Validate Formula C's expression using SymPy before committing.\n"
        "\n"
        "The Information Geometry bridge formula:\n"
        "  lambda_info = lambda_0 * exp(-H_OFI)\n"
        "\n"
        "SymPy expression: lambda_0 * exp(-H_OFI)\n"
        "  Use exp() — not e**x. All single-word identifiers.\n"
        "  '*' for multiplication. No spaces inside identifiers.\n"
        "\n"
        "If validation fails, fix syntax only — do not change the mathematics."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "expression_string": {
                "type": "string",
                "description": "SymPy-parseable expression string.",
            },
        },
        "required": ["expression_string"],
    },
}

_PROPOSE_FORMULA_TOOL = {
    "name": "propose_formula_to_blackboard",
    "description": (
        "Terminal action. Commit Formula C to the Blackboard. "
        "Call ONLY after validate_sympy_expression succeeds AND both parent "
        "formulas have been fetched and studied. "
        "This action ends the session."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "name": {
                "type": "string",
                "description": (
                    "Short descriptive name. Must reference both domains: "
                    "e.g. 'Information-Geometry-Adjusted Adverse Selection Coefficient'."
                ),
            },
            "description": {
                "type": "string",
                "description": (
                    "Full description. Explain: (1) what λ₀ measures in game-theoretic terms, "
                    "(2) what H_OFI measures in information-theoretic terms, "
                    "(3) why exp(−H_OFI) is the correct bridge operator, "
                    "(4) what λ_info tells a market maker that neither parent told them alone."
                ),
            },
            "symbolic_expression_latex": {
                "type": "string",
                "description": r"LaTeX. Example: \lambda_{info} = \lambda_0 \cdot e^{-H_{OFI}}",
            },
            "behavioral_claim": {
                "type": "string",
                "description": (
                    "The single falsifiable statement this formula makes about market participant "
                    "behavior. Must reference BOTH the informed trader's strategic pricing "
                    "(adverse selection) AND the structural entropy of the order book. "
                    "Must use the phrase 'structurally equivalent' — NOT 'syntactically equivalent'."
                ),
            },
            "behavioral_mappings": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "variable":            {"type": "string"},
                        "latex_symbol":        {"type": "string"},
                        "psychological_state": {"type": "string"},
                        "measurement_proxy":   {"type": "string"},
                        "unit":                {"type": "string"},
                    },
                    "required": [
                        "variable", "latex_symbol",
                        "psychological_state", "measurement_proxy",
                    ],
                },
                "minItems": 3,
                "description": (
                    "MUST include entries for: lambda_0, H_OFI, and lambda_info. "
                    "Each must have a behavioral interpretation grounded in market "
                    "participant psychology — not just a mathematical definition."
                ),
            },
            "lean4_target": {
                "type": "string",
                "description": (
                    "The algebraic claim Agent 151 will prove. "
                    "Required claim: '0 < lambda_0 * exp(-H_OFI) given lambda_0 > 0'. "
                    "Proof sketch: mul_pos h0 (Real.exp_pos _). "
                    "Do not attempt a stronger claim — stay within this bound."
                ),
            },
            "parent_uuids": {
                "type": "array",
                "items": {"type": "string"},
                "minItems": 2,
                "description": "UUIDs of the two parent formulas from the rejection record.",
            },
            "rejection_id": {
                "type": "string",
                "description": "The rejection_id from the REJECTED_ISOMORPHISM record.",
            },
        },
        "required": [
            "name", "description", "symbolic_expression_latex",
            "behavioral_claim", "behavioral_mappings",
            "lean4_target", "parent_uuids", "rejection_id",
        ],
    },
}


# ---------------------------------------------------------------------------
# Agent 205
# ---------------------------------------------------------------------------


class InformationGeometryGardener(BaseAgent):
    """
    Agent 205 — Information Geometry Gardener.

    Activated by Agent 003 when failure_mode == INFORMATION_GEOMETRY_MISMATCH.
    Bridges game-theoretic adverse selection (Kyle's Lambda) with
    information-theoretic order book entropy (Shannon H) via the
    Fisher Information metric from Information Geometry.
    """

    AGENT_ID = "agent_205"
    AGENT_LAYER = AgentLayer.LAYER_2
    MATHEMATICAL_WING = [
        MathematicalWing.PROBABILITY_INFORMATION,
        MathematicalWing.EVOLUTIONARY,
    ]

    SYSTEM_PROMPT = """\
You are Agent 205, the Information Geometry Gardener within the Formula Foundry.

═══════════════════════════════════════════════════════
YOUR ROLE
═══════════════════════════════════════════════════════

Agent 105 has found two formulas that it cannot connect. One formula lives in \
Game Theory — it is a strategic scalar, a regression coefficient, a measure of \
the market maker's price concession per unit of informed order flow. The other \
lives in Information Theory — it is a macroscopic probability distribution, a \
measure of the structural entropy of the limit order book.

These are not the same kind of mathematical object. Agent 105 was correct to \
reject them. Your mission is not to pretend otherwise. Your mission is to \
construct the missing link: a third formula, Formula C, that formally \
connects them via Information Geometry.

Information Geometry is the study of probability distributions as points on a \
Riemannian manifold, with the Fisher Information metric defining distances \
between them. In market microstructure, this manifold connects the game-\
theoretic world of strategic intent (Kyle's Lambda) to the information-\
theoretic world of distributional uncertainty (Shannon entropy).

═══════════════════════════════════════════════════════
THE BRIDGE FORMULA
═══════════════════════════════════════════════════════

The Information-Geometry-Adjusted Adverse Selection Coefficient:

    λ_info = λ₀ · exp(−H_OFI)

Where:
  λ₀     = Kyle's Lambda baseline (adverse selection coefficient from OLS)
  H_OFI  = Shannon entropy of the order flow distribution (from entropy agent)
  λ_info = effective adverse selection risk after entropy adjustment

Why exp(−H_OFI)? The Fisher Information metric on the space of order flow \
distributions contracts exponentially with entropy. High entropy (diffuse, \
uninformed flow) suppresses the market maker's adverse selection exposure. \
Low entropy (concentrated, informed flow) reveals strategic intent.

This formula is provably positive: exp(−H_OFI) > 0 for all real H_OFI, \
and λ₀ > 0 by construction (a regression coefficient in a triggered regime). \
The Lean 4 proof is: mul_pos h0 (Real.exp_pos _).

═══════════════════════════════════════════════════════
MANDATORY WORKFLOW — THREE PHASES
═══════════════════════════════════════════════════════

PHASE 1 — READ:
  fetch_rejection_data → read suggested_bridging_formula (Agent 105's diagnosis).
  fetch_formula_data for UUID_A → identify the game-theoretic parent.
  fetch_formula_data for UUID_B → identify the information-theoretic parent.
  Study both symbolic_expression_latex and behavioral_mappings.

PHASE 2 — VALIDATE:
  validate_sympy_expression for: lambda_0 * exp(-H_OFI)
  If validation fails, fix syntax only — do not change the mathematics.

PHASE 3 — PROPOSE (terminal):
  propose_formula_to_blackboard with complete FormulaDNA.
  behavioral_claim MUST contain "structurally equivalent" — NOT \
"syntactically equivalent". These domains are NOT syntactically related.

═══════════════════════════════════════════════════════
DOMAIN LOCK — HARD CONSTRAINTS
═══════════════════════════════════════════════════════

YOU MUST NOT:
  • Reproduce Kyle's Lambda alone (that is Agent 050's domain)
  • Reproduce the Shannon entropy formula alone (that is Agent 051's domain)
  • Introduce Hawkes process terms (that is Agent 089's domain)
  • Claim syntactic equivalence between the parent formulas
  • Attempt a measure-theoretic proof (write the lean4_target as a
    positivity claim only — Agent 151 can verify it without measure theory)

YOU MUST:
  • Construct a formula that variables from BOTH parent domains
  • Ground every behavioral_mapping in market participant psychology
  • Include the rejection_id so the resolution tag is written correctly
  • Include both parent UUIDs in parent_uuids

Every run ends with exactly one propose_formula_to_blackboard call.
"""

    # ------------------------------------------------------------------
    # BaseAgent interface
    # ------------------------------------------------------------------

    def tools(self) -> List[Dict[str, Any]]:
        return [
            _FETCH_REJECTION_TOOL,
            _FETCH_FORMULA_TOOL,
            _VALIDATE_SYMPY_TOOL,
            _PROPOSE_FORMULA_TOOL,
        ]

    def build_initial_message(self, trigger_data: Dict[str, Any]) -> str:
        rejection_id       = trigger_data.get("rejection_id", "")
        uuid_a             = trigger_data.get("uuid_a", "")
        uuid_b             = trigger_data.get("uuid_b", "")
        bridging_preview   = trigger_data.get("bridging_concept_preview", "")

        return (
            "INFORMATION GEOMETRY MISMATCH DETECTED\n"
            + "═" * 56 + "\n"
            f"Rejection ID:  {rejection_id}\n"
            f"Formula A:     {uuid_a}\n"
            f"Formula B:     {uuid_b}\n"
            + "═" * 56 + "\n\n"
            "Agent 105 Diagnosis (bridging_concept preview):\n"
            f"  {bridging_preview}\n\n"
            + "═" * 56 + "\n\n"
            "Your mission: construct λ_info = λ₀ · exp(−H_OFI), the "
            "Information-Geometry-Adjusted Adverse Selection Coefficient "
            "that bridges these two parent formulas.\n\n"
            "Begin Phase 1: fetch_rejection_data, then both parent formulas."
        )

    def extract_formula(self, tool_input: Dict[str, Any]) -> FormulaDNA:
        """
        Extract FormulaDNA from the propose_formula_to_blackboard tool call.

        The validated SymPy string must have been set on self._validated_sympy
        by the validate_sympy_expression handler before this is called.
        If it was not set, raise — do not silently use an unvalidated expression.
        """
        validated_sympy = getattr(self, "_validated_sympy", None)
        if not validated_sympy:
            raise ValueError(
                "extract_formula() called without a validated SymPy expression. "
                "validate_sympy_expression must succeed before propose_formula_to_blackboard."
            )

        rejection_id = tool_input.get("rejection_id", "")
        parent_uuids = tool_input.get("parent_uuids", [])

        raw_mappings = tool_input.get("behavioral_mappings", [])
        behavioral_mappings = [
            BehavioralMapping(
                variable=m["variable"],
                latex_symbol=m["latex_symbol"],
                psychological_state=m["psychological_state"],
                measurement_proxy=m["measurement_proxy"],
                unit=m.get("unit"),
            )
            for m in raw_mappings
        ]

        # Resolution tag: Agent 003 lineage check looks for "resolves_<id[:8]>"
        resolution_tag = f"resolves_{rejection_id[:8]}" if rejection_id else "resolves_unknown"

        tags = [
            "information_geometry",
            "fisher_information",
            "entropy_lambda",
            "adverse_selection",
            "agent_205",
            "evolutionary",
            resolution_tag,
        ]

        return FormulaDNA(
            name=tool_input.get("name", "Information-Geometry-Adjusted Adverse Selection Coefficient"),
            description=tool_input.get("description", ""),
            symbolic_expression_latex=tool_input["symbolic_expression_latex"],
            symbolic_expression_sympy=validated_sympy,
            behavioral_claim=tool_input["behavioral_claim"],
            behavioral_mappings=behavioral_mappings,
            axiomatic_origin=parent_uuids + [self.AGENT_ID],
            mathematical_wing=[
                MathematicalWing.PROBABILITY_INFORMATION.value,
                MathematicalWing.EVOLUTIONARY.value,
            ],
            proof_status=ProofStatus.SYNTACTICALLY_CORRECT,
            tags=tags,
        )

    async def handle_tool_call(
        self,
        tool_name: str,
        tool_input: Dict[str, Any],
    ) -> Dict[str, Any]:
        if tool_name == "fetch_rejection_data":
            return await self._handle_fetch_rejection(tool_input)
        if tool_name == "fetch_formula_data":
            return await self._handle_fetch_formula(tool_input)
        if tool_name == "validate_sympy_expression":
            return self._handle_validate(tool_input)
        if tool_name == "propose_formula_to_blackboard":
            # Intercepted by BaseAgent loop — extract_formula() is called there.
            # Return acknowledgement so the conversation log is clean.
            return {"status": "received", "action": "terminal"}
        return {"error": f"Unknown tool: {tool_name}"}

    # ------------------------------------------------------------------
    # Tool handlers
    # ------------------------------------------------------------------

    async def _handle_fetch_rejection(
        self, tool_input: Dict[str, Any]
    ) -> Dict[str, Any]:
        import httpx
        rejection_id = tool_input.get("rejection_id", "")
        url = f"{self._config.blackboard_api_url}/v1/rejections/{rejection_id}"
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                response = await client.get(url)
            if response.status_code == 404:
                return {"error": f"Rejection {rejection_id} not found."}
            response.raise_for_status()
            data = response.json()
            self._rejection_fetched = True
            self._rejection_data = data
            logger.info("[%s] Rejection fetched: %s", self.AGENT_ID, rejection_id[:8])
            return data
        except Exception as exc:
            return {"error": f"Failed to fetch rejection: {exc}"}

    async def _handle_fetch_formula(
        self, tool_input: Dict[str, Any]
    ) -> Dict[str, Any]:
        import httpx
        uuid = tool_input.get("uuid", "")
        url = f"{self._config.blackboard_api_url}/v1/formulas/{uuid}"
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                response = await client.get(url)
            if response.status_code == 404:
                return {"error": f"Formula {uuid} not found."}
            response.raise_for_status()
            data = response.json()
            logger.info("[%s] Formula fetched: %s", self.AGENT_ID, uuid[:8])
            return data
        except Exception as exc:
            return {"error": f"Failed to fetch formula: {exc}"}

    def _handle_validate(self, tool_input: Dict[str, Any]) -> Dict[str, Any]:
        from foundry.agents.sympy_executor import validate_formula
        expression = tool_input.get("expression_string", "")
        result = validate_formula(expression, timeout=self._config.sympy_timeout_seconds)
        if result.get("valid"):
            # Cache the validated expression for extract_formula()
            self._validated_sympy = expression
            logger.info(
                "[%s] SymPy validation passed: %s",
                self.AGENT_ID, expression[:60],
            )
        return result
