"""
Agent 202 — Dimensionality Evolutionary Gardener
Evolutionary Target: DIMENSIONALITY_MISMATCH

Axiomatic domain: Probability Theory, Functional Analysis.
Behavioral focus: Bridging univariate models (scalar intensities) with
                  multivariate models (vector intensities / matrices)
                  via marginalization operators.

Bugs fixed vs. delivered version (same class as Agent 204 first delivery):
  1. register_tool() removed → tools() + handle_tool_call() per BaseAgent contract
  2. _build_system_prompt() removed → SYSTEM_PROMPT class attribute
  3. self._current_run_context removed → rejection_id via trigger_data / tool_input
  4. Terminal tool renamed from propose_dimensionality_bridge → propose_formula_to_blackboard
     (propose_dimensionality_bridge not in _PROPOSAL_TOOL_NAMES; run_result.formula
     would always be None and flow would always exit as agent_202_failure)
  5. extract_formula() implemented (abstract, class would not instantiate)
  6. build_initial_message() implemented (abstract, class would not instantiate)
  7. handle_tool_call() implemented (abstract, class would not instantiate)
  8. tools() implemented (abstract, class would not instantiate)
  9. AGENT_LAYER + MATHEMATICAL_WING class attributes added
 10. validate_formula call uses expression_string (consistent with Agent 201/204 contract)
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
        "MUST be the first tool call. Read suggested_bridging_formula to confirm "
        "which pattern applies (Marginalization is the default; Copula only if "
        "explicitly named in the bridging text AND both parents are univariate)."
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
        "Call for BOTH parent UUIDs. Identify which parent is univariate "
        "(scalar λ) and which is multivariate (vector λ⃗ or 2×2 matrix). "
        "Formula C marginalizes the multivariate parent onto the univariate dimension."
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
        "Validate Formula C's scalar expression using SymPy.\n"
        "\n"
        "Pattern A (Marginalization — default):\n"
        "  Target expression: mu_ES / (1 - a_ESES - a_ZNES)\n"
        "  All single-word identifiers. Use '*' for multiplication, '/' for division.\n"
        "  Do NOT use subscript notation or curly braces.\n"
        "\n"
        "Pattern B (Copula — restricted):\n"
        "  Only valid if bridging text explicitly requests copula AND both parents "
        "  are univariate. If in doubt, use Pattern A.\n"
        "\n"
        "If validation fails, fix syntax only — do not change the mathematics."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "expression_string": {
                "type": "string",
                "description": "SymPy-compatible scalar expression for Formula C.",
            },
            "reasoning": {
                "type": "string",
                "description": (
                    "Which parent is univariate, which is multivariate, "
                    "and which dimension is being marginalized out."
                ),
            },
        },
        "required": ["expression_string", "reasoning"],
    },
}

_PROPOSE_FORMULA_TOOL = {
    "name": "propose_formula_to_blackboard",
    "description": (
        "TERMINAL ACTION. Submit Formula C to the Blackboard.\n"
        "\n"
        "Prerequisites (enforced by gate):\n"
        "  (1) fetch_rejection_data called.\n"
        "  (2) fetch_formula_data called for BOTH parent UUIDs.\n"
        "  (3) validate_sympy_expression returned valid=True.\n"
        "\n"
        "behavioral_claim MUST:\n"
        "  - State EXACTLY which pattern was applied (Marginalization or Copula)\n"
        "  - NOT claim the marginal process is itself a univariate Hawkes\n"
        "  - Assert Tier 2 structural isomorphism only (NOT Tier 1 syntactic)\n"
        "  - Include the Lean 4 target (div_pos + linarith using h_row1)\n"
        "\n"
        "Required phrasing: 'the marginal mean-field stationary intensity of the "
        "bivariate ES/ZN Hawkes reduces to a scalar form structurally equivalent "
        "to the univariate Hawkes effective baseline.'\n"
        "\n"
        "Include rejection_id for the resolves_ lineage tag."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "description": {"type": "string"},
            "symbolic_expression_latex": {"type": "string"},
            "behavioral_claim": {
                "type": "string",
                "description": (
                    "Must assert Tier 2 structural isomorphism. Must NOT claim "
                    "the marginal process is a univariate Hawkes process. "
                    "Must name the Lean 4 target."
                ),
            },
            "behavioral_mappings": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "variable": {"type": "string"},
                        "latex_symbol": {"type": "string"},
                        "psychological_state": {"type": "string"},
                        "measurement_proxy": {"type": "string"},
                        "unit": {"type": "string"},
                    },
                    "required": [
                        "variable", "latex_symbol",
                        "psychological_state", "measurement_proxy",
                    ],
                },
            },
            "pattern_applied": {
                "type": "string",
                "enum": ["marginalization", "copula"],
                "description": "Which bridging pattern was applied.",
            },
            "lean4_target": {
                "type": "string",
                "description": (
                    "Must be: 'div_pos + linarith using h_row1 "
                    "(h_row1 : a_ESES + a_ZNES < 1)'."
                ),
            },
            "rejection_id": {
                "type": "string",
                "description": "The rejection UUID being resolved (for the resolves_ tag).",
            },
        },
        "required": [
            "name", "description", "symbolic_expression_latex",
            "behavioral_claim", "behavioral_mappings",
            "pattern_applied", "lean4_target", "rejection_id",
        ],
    },
}


# ---------------------------------------------------------------------------
# Agent 202
# ---------------------------------------------------------------------------


class DimensionalityGardener(BaseAgent):
    """
    Agent 202 — Dimensionality Evolutionary Gardener.

    Resolves DIMENSIONALITY_MISMATCH rejections by constructing Formula C:
    the ES marginal of the bivariate Vector Hawkes, showing that the
    marginal mean-field stationary intensity is structurally equivalent
    (Tier 2) to the univariate Hawkes effective baseline.

    Default pattern: Marginalization.
    Restricted pattern: Copula — only if bridging text explicitly requests it
    AND both parents are already univariate.
    """

    AGENT_ID = "agent_202"
    AGENT_LAYER = AgentLayer.LAYER_2
    MATHEMATICAL_WING = [
        MathematicalWing.PROBABILITY_INFORMATION,
        MathematicalWing.FUNCTIONAL_ANALYSIS,
        MathematicalWing.EVOLUTIONARY,
    ]

    SYSTEM_PROMPT = """\
You are Agent 202, the Dimensionality Evolutionary Gardener in the Formula Foundry.

You receive a REJECTED_ISOMORPHISM record where Agent 105 failed to synthesize \
a univariate formula and a multivariate/vector formula due to a \
DIMENSIONALITY_MISMATCH. Your mission is to construct Formula C: the \
marginalization operator that projects the multivariate formula down to the \
univariate dimension, enabling Agent 105 to declare a Tier 2 structural \
isomorphism on the next synthesis attempt.

\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550
THE TWO PATTERNS
\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550

PATTERN A \u2014 Marginalization (Default)
Trigger:  One parent is univariate (scalar \u03bb). The other is multivariate
          (vector \u03bb\u20d7 or 2\xd72 excitation matrix).
Operator: Integrate out the cross-asset dimension in the stationary
          subcritical regime. For the bivariate ES/ZN Hawkes, the marginal
          mean-field stationary intensity on the ES dimension is:

            E[\u03bb_ES] = \u03bc_ES / (1 - a_ESES - a_ZNES)

          where a_ESES is ES self-excitation and a_ZNES is ZN\u2192ES
          cross-excitation. The denominator is the row-1 sum of the
          excitation matrix; subcriticality requires a_ESES + a_ZNES < 1
          (this is h_row1 from Agent 060's subcriticality conditions).

          SymPy expression: mu_ES / (1 - a_ESES - a_ZNES)
Lean 4:   div_pos + linarith from h_row1 : a_ESES + a_ZNES < 1
          (Agent 151 already has this machinery from Seed Proofs 3 and 4.
           No new seed proof is needed.)

PATTERN B \u2014 Copula (Restricted)
Trigger:  ONLY if suggested_bridging_formula explicitly contains the word
          "copula" or "joint distribution" AND both parents are already
          univariate (scalar). If either parent is bivariate, do NOT use this.
Operator: Sklar's theorem \u2014 defer to bridging text for specific copula family.
Lean 4:   Fréchet\u2013Hoeffding bounds on the joint CDF.

\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550
DOMAIN LOCK \u2014 NON-NEGOTIABLE
\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550

1. MUST NOT claim the marginal process is itself a univariate Hawkes process.
   The marginal of a bivariate Hawkes depends on the full joint history.
   Formula C asserts structural equivalence of the stationary mean-field
   intensity, not process-level equivalence.
2. behavioral_claim MUST assert Tier 2 structural isomorphism only.
   Do not claim Tier 1 syntactic equivalence.
3. behavioral_claim MUST include this phrase:
   "the marginal mean-field stationary intensity of the bivariate ES/ZN
   Hawkes reduces to a scalar form structurally equivalent to the univariate
   Hawkes effective baseline."
4. MUST call validate_sympy_expression before propose_formula_to_blackboard.
5. MUST include rejection_id in the terminal call for the resolves_ tag.

\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550
EXECUTION PHASES
\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550

PHASE 1: fetch_rejection_data(rejection_id) \u2014 read bridging text, confirm pattern.
PHASE 2: fetch_formula_data(uuid_a), fetch_formula_data(uuid_b) \u2014 identify which
         parent is univariate and which is multivariate.
PHASE 3: validate_sympy_expression \u2014 confirm Formula C scalar is syntactically valid.
PHASE 4: propose_formula_to_blackboard \u2014 terminal action. Session ends here.

Every run ends with exactly one propose_formula_to_blackboard call.
"""

    # ------------------------------------------------------------------
    # Gate state
    # ------------------------------------------------------------------

    def __init__(self, config: AgentConfig) -> None:
        super().__init__(config)
        self._rejection_fetched: bool = False
        self._rejection_data: Optional[Dict[str, Any]] = None
        self._parent_uuids_fetched: Set[str] = set()

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
        rejection_id = trigger_data.get("rejection_id", "UNKNOWN")
        uuid_a = trigger_data.get("uuid_a", "unknown")
        uuid_b = trigger_data.get("uuid_b", "unknown")
        preview = trigger_data.get("bridging_concept_preview", "")

        return (
            "DIMENSIONALITY GARDENING DIRECTIVE\n"
            "\u2550" * 56 + "\n"
            f"Rejection ID:  {rejection_id}\n"
            f"Parent A UUID: {uuid_a}\n"
            f"Parent B UUID: {uuid_b}\n"
            "\u2550" * 56 + "\n\n"
            "Agent 105 evaluated (A, B) and found DIMENSIONALITY_MISMATCH.\n"
            "Your directive: construct Formula C \u2014 the marginalization operator\n"
            "that projects the multivariate parent onto the univariate dimension.\n\n"
            "Bridging concept preview:\n"
            f"  {preview[:200]}{'...' if len(preview) > 200 else ''}\n\n"
            "\u2550" * 56 + "\n\n"
            f'Begin Phase 1: fetch_rejection_data("{rejection_id}").'
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
            return {"accepted": True}
        return {"error": f"Unknown tool: {tool_name}"}

    def extract_formula(
        self,
        tool_input: Dict[str, Any],
        validated_sympy_str: str,
    ) -> FormulaDNA:
        """Build Formula C from the terminal propose call."""
        if not self._rejection_fetched:
            raise ValueError(
                "Gate violation: propose_formula_to_blackboard requires "
                "fetch_rejection_data to have been called first."
            )
        if len(self._parent_uuids_fetched) < 2:
            raise ValueError(
                f"Gate violation: propose_formula_to_blackboard requires "
                f"fetch_formula_data for both parent UUIDs. "
                f"Fetched so far: {self._parent_uuids_fetched}"
            )

        pattern = tool_input.get("pattern_applied", "marginalization")
        rejection_id = tool_input.get("rejection_id", "unknown")
        lean4_target = tool_input.get("lean4_target", "")

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

        description = tool_input.get("description", "")
        if lean4_target:
            description = f"{description}\n\nLean 4 Target: {lean4_target}"

        tags = [
            "agent_202",
            "evolutionary",
            "dimensionality_bridge",
            pattern,
            f"resolves_{rejection_id[:8]}",
        ]

        return FormulaDNA(
            name=tool_input.get("name"),
            description=description,
            symbolic_expression_latex=tool_input["symbolic_expression_latex"],
            symbolic_expression_sympy=validated_sympy_str,
            behavioral_claim=tool_input["behavioral_claim"],
            behavioral_mappings=behavioral_mappings,
            axiomatic_origin=[self.AGENT_ID],
            mathematical_wing=[
                MathematicalWing.PROBABILITY_INFORMATION.value,
                MathematicalWing.FUNCTIONAL_ANALYSIS.value,
                MathematicalWing.EVOLUTIONARY.value,
            ],
            proof_status=ProofStatus.SYNTACTICALLY_CORRECT,
            tags=tags,
        )

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
            self._parent_uuids_fetched.add(uuid)
            logger.info("[%s] Formula fetched: %s", self.AGENT_ID, uuid[:8])
            return data
        except Exception as exc:
            return {"error": f"Failed to fetch formula: {exc}"}

    def _handle_validate(self, tool_input: Dict[str, Any]) -> Dict[str, Any]:
        from foundry.agents.sympy_executor import validate_formula
        expression = tool_input.get("expression_string", "")
        return validate_formula(expression, timeout=self._config.sympy_timeout_seconds)
