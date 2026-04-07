"""
Agent 204 — Micro/Macro Evolutionary Gardener
Evolutionary Target: MICRO_MACRO_MISMATCH

Axiomatic domain: Statistical Mechanics, Measure Theory, Mean-Field Theory.
Behavioral focus: Bridging high-frequency, discrete order-by-order phenomena
                  (micro-states) to systemic, macroscopic market observables.

Bugs fixed vs. delivered version:
  1. MathematicalWing.STATISTICAL_MECHANICS → MEASURE_THEORY + PROBABILITY_INFORMATION
     (STATISTICAL_MECHANICS does not exist in the enum; AttributeError on import)
  2. register_tool() pattern removed → tools() + handle_tool_call() per BaseAgent contract
  3. _build_system_prompt() removed → SYSTEM_PROMPT class attribute (base loop reads this)
  4. self._current_run_context removed → rejection_id passed via trigger_data / tool_input
  5. extract_formula() implemented (was abstract, class would not instantiate)
  6. build_initial_message() implemented (was abstract, class would not instantiate)
  7. handle_tool_call() implemented (was abstract, class would not instantiate)
  8. Terminal tool renamed to propose_formula_to_blackboard (in _PROPOSAL_TOOL_NAMES)
     — propose_micro_macro_bridge was not intercepted by base loop, so run_result.formula
     was always None and the flow always exited as agent_204_failure
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
        "MUST be the first tool call. Read suggested_bridging_formula carefully — "
        "it specifies which of the two bridging patterns (Mean-Field or "
        "Partition Function) to apply."
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
        "Study symbolic_expression_latex and behavioral_mappings — "
        "Formula C must map between both variable spaces."
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
        "Pattern A (Mean-Field): use a ratio expression.\n"
        "  Example: mu / (1 - alpha / beta)\n"
        "  All single-word identifiers. '*' for multiplication.\n"
        "\n"
        "Pattern B (Partition Function): use a Sum expression.\n"
        "  Example: Sum(exp(-beta * H_i), (i, 0, n-1))\n"
        "  'Sum' is capital S. 'exp' from sympy.\n"
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
                    "Which pattern (Mean-Field or Partition Function) and why, "
                    "based on the suggested_bridging_formula text."
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
        "behavioral_claim MUST explicitly state:\n"
        "  - Which pattern was applied (Mean-Field LLN or Partition Function)\n"
        "  - Why that pattern was selected based on the bridging text\n"
        "  - The Lean 4 algebraic target (div_pos or sum_pos)\n"
        "\n"
        "Include rejection_id in the call so the resolves_ tag is set correctly."
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
                    "Must name the pattern applied, the Lean 4 target, "
                    "and the market-behavioral interpretation."
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
                "enum": ["mean_field", "statistical_mechanics"],
                "description": "Which bridging pattern was applied.",
            },
            "lean4_target": {
                "type": "string",
                "description": (
                    "Algebraic Lean 4 target. "
                    "Pattern A: 'div_pos, sub_pos, div_lt_one — positivity of mu/(1-n)'. "
                    "Pattern B: 'Finset.sum_pos, Real.exp_pos — partition function Z > 0'."
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
# Agent 204
# ---------------------------------------------------------------------------


class MicroMacroGardener(BaseAgent):
    """
    Agent 204 — Micro/Macro Evolutionary Gardener.

    Resolves MICRO_MACRO_MISMATCH rejections by constructing Formula C:
    either a Mean-Field limit (Pattern A) or a Boltzmann Partition Function
    (Pattern B), selected deterministically from the rejection bridging text.
    """

    AGENT_ID = "agent_204"
    AGENT_LAYER = AgentLayer.LAYER_2
    MATHEMATICAL_WING = [
        MathematicalWing.MEASURE_THEORY,
        MathematicalWing.PROBABILITY_INFORMATION,
        MathematicalWing.EVOLUTIONARY,
    ]

    SYSTEM_PROMPT = """\
You are Agent 204, the Micro/Macro Evolutionary Gardener in the Formula Foundry.

You receive a REJECTED_ISOMORPHISM record where Agent 105 failed to synthesize \
two formulas due to a MICRO_MACRO_MISMATCH. Your mission is to construct \
Formula C: the bridging operator that projects the micro-structural formula \
into the macro-structural domain, allowing subsequent synthesis to succeed.

\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550
THE TWO BRIDGING PATTERNS
\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550

Read suggested_bridging_formula. Select EXACTLY ONE pattern.

PATTERN A \u2014 Mean-Field Limit (Point Process \u2192 Aggregate State)
Trigger:  Formula A models sequential trade arrivals (e.g., Hawkes point process
          at tick resolution). Formula B models a continuous aggregate state
          (e.g., regime intensity, daily volume).
Operator: Apply the Law of Large Numbers for point processes. As the number
          of participants N\u2192\u221e, stochastic fluctuations vanish. Project discrete
          arrivals N(t) into the deterministic mean-field stationary intensity:
            E[\u03bb] = \u03bc / (1 - n)   where n = \u03b1/\u03b2 (branching ratio, n < 1)
          This is the Palm calculus result stated as a behavioral axiom.
Lean 4:   div_pos, sub_pos, div_lt_one \u2014 verify algebraic positivity and
          boundedness of \u03bc/(1-n) under subcriticality (n < 1).
          The stochastic derivation is NOT proved in Lean 4.

PATTERN B \u2014 Partition Function (Discrete Queue \u2192 Regime Flag)
Trigger:  Formula A models discrete individual queue positions or categorical
          order states. Formula B models a macroscopic regime flag.
Operator: Statistical Mechanics formulation. Treat discrete order variables
          as micro-states x\u1d62. Define energy function H(x\u1d62) representing
          urgency or panic. The macro-observable (regime flag) emerges as the
          Boltzmann-weighted expected value:
            Z = \u03a3\u1d62 exp(-\u03b2 \u00b7 H(x\u1d62))
            \u27e8O\u27e9 = (1/Z) \u00b7 \u03a3\u1d62 O(x\u1d62) \u00b7 exp(-\u03b2 \u00b7 H(x\u1d62))
Lean 4:   Finset.sum_pos + Real.exp_pos \u2014 verify Z > 0 (all terms positive).

\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550
DOMAIN LOCK \u2014 NON-NEGOTIABLE
\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550

1. MUST include \u222b or \u03a3 over micro-states in Formula C. No bare substitution.
2. MUST state which pattern and why in behavioral_claim.
3. MUST call validate_sympy_expression before propose_formula_to_blackboard.
4. MUST NOT alter the physics of Formula A or B. Build the wrapper only.
5. MUST include rejection_id in the terminal call for the resolves_ tag.

\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550
EXECUTION PHASES
\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550

PHASE 1: fetch_rejection_data(rejection_id) \u2014 read bridging text, detect pattern.
PHASE 2: fetch_formula_data(uuid_a), fetch_formula_data(uuid_b) \u2014 study variable spaces.
PHASE 3: validate_sympy_expression \u2014 confirm Formula C is syntactically valid.
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
            "MICRO/MACRO GARDENING DIRECTIVE\n"
            "\u2550" * 56 + "\n"
            f"Rejection ID:  {rejection_id}\n"
            f"Parent A UUID: {uuid_a}\n"
            f"Parent B UUID: {uuid_b}\n"
            "\u2550" * 56 + "\n\n"
            "Agent 105 evaluated (A, B) and found MICRO_MACRO_MISMATCH.\n"
            "Your directive: construct Formula C \u2014 the mean-field or partition-function\n"
            "bridge that projects the micro-structural formula into the macro domain.\n\n"
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

        pattern = tool_input.get("pattern_applied", "mean_field")
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
            "agent_204",
            "evolutionary",
            "micro_macro_bridge",
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
                MathematicalWing.MEASURE_THEORY.value,
                MathematicalWing.PROBABILITY_INFORMATION.value,
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
