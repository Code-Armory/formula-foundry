"""
Agent 203 — Stochastic/Deterministic Evolutionary Gardener
Evolutionary Target: STOCHASTIC_DETERMINISTIC_MISMATCH

Axiomatic domain: Probability Theory, Ergodic Theory, Functional Analysis.
Behavioral focus: Bridging probabilistic point processes or stochastic variables
                  to deterministic coefficients or static ratios via explicit
                  expectation operators or ergodic limits.

Bugs fixed vs. delivered version (identical class to Agent 202/204 first deliveries):
  1. register_tool() removed → tools() + handle_tool_call() per BaseAgent contract
  2. _build_system_prompt() removed → SYSTEM_PROMPT class attribute
  3. self._current_run_context removed → rejection_id via trigger_data / tool_input
  4. self.config → self._config (BaseAgent stores config as _config)
  5. extract_formula() implemented (abstract, class would not instantiate)
  6. build_initial_message() implemented (abstract, class would not instantiate)
  7. handle_tool_call() implemented (abstract, class would not instantiate)
  8. tools() implemented (abstract, class would not instantiate)
  9. AGENT_LAYER + MATHEMATICAL_WING class attributes added
 10. validate tool uses expression_string (consistent with Agent 201/202/204 contract)
 11. EVOLUTIONARY added to mathematical_wing (consistent with other Gardeners)

Note: propose_formula_to_blackboard is already correctly named in the delivered
version (in _PROPOSAL_TOOL_NAMES). Bug count is one fewer than previous deliveries.
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
        "which pattern applies: expectation_operator (default) or ergodic_limit "
        "(only if bridging text explicitly references time-average or ergodic theorem)."
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
        "Call for BOTH parent UUIDs. Identify which parent is stochastic "
        "(e.g., Hawkes λ(t), entropy H) and which is deterministic "
        "(e.g., Kyle's Lambda, Amihud ILLIQ ratio). "
        "Formula C bridges them via E[·] or the ergodic time-average."
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
        "Pattern A (Expectation Operator — default):\n"
        "  Hawkes stationary mean: mu / (1 - alpha / beta)\n"
        "  For vector Hawkes row marginal: mu_ES / (1 - a_ESES - a_ZNES)\n"
        "  All single-word identifiers. '*' for multiplication, '/' for division.\n"
        "\n"
        "Pattern B (Ergodic Limit):\n"
        "  Time-average of the stochastic process over session [t_open, t_close].\n"
        "  Example: Integral(lambda_t, (t, t_open, t_close)) / (t_close - t_open)\n"
        "  'Integral' is capital I. lambda_t represents λ(t) as a SymPy symbol.\n"
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
                    "Which parent is stochastic, which is deterministic, "
                    "and which operator (expectation or ergodic limit) bridges them."
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
        "  - Name the pattern (expectation_operator or ergodic_limit)\n"
        "  - NOT claim to have derived the full probability measure in Lean 4\n"
        "  - State the expectation as a behavioral axiom (Palm calculus result)\n"
        "  - Include the Lean 4 target (div_pos + linarith, Seed Proof 4 pattern)\n"
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
                    "Must name the operator applied. Must NOT claim Lean 4 "
                    "proves the full measure-theoretic derivation. "
                    "Must include the Lean 4 algebraic target."
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
                "enum": ["expectation_operator", "ergodic_limit"],
                "description": "Which bridging pattern was applied.",
            },
            "lean4_target": {
                "type": "string",
                "description": (
                    "Must be: 'div_pos + linarith (Seed Proof 4 pattern) — "
                    "algebraic positivity of E[λ] = μ/(1-n) under subcriticality'."
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
# Agent 203
# ---------------------------------------------------------------------------


class StochasticDeterministicGardener(BaseAgent):
    """
    Agent 203 — Stochastic/Deterministic Evolutionary Gardener.

    Resolves STOCHASTIC_DETERMINISTIC_MISMATCH rejections by constructing
    Formula C: the expectation operator E[λ] or ergodic time-average that
    projects a stochastic point process into a deterministic scalar,
    enabling Agent 105 to declare a Tier 2 or Tier 3 isomorphism with a
    deterministic target formula (Kyle's Lambda, Amihud ILLIQ, etc.).
    """

    AGENT_ID = "agent_203"
    AGENT_LAYER = AgentLayer.LAYER_2
    MATHEMATICAL_WING = [
        MathematicalWing.PROBABILITY_INFORMATION,
        MathematicalWing.ERGODIC_THEORY,
        MathematicalWing.EVOLUTIONARY,
    ]

    SYSTEM_PROMPT = """\
You are Agent 203, the Stochastic/Deterministic Evolutionary Gardener in the \
Formula Foundry.

You receive a REJECTED_ISOMORPHISM record where Agent 105 failed to synthesize \
a probabilistic formula (e.g., Hawkes intensity λ(t)) and a deterministic \
formula (e.g., Kyle's Lambda regression coefficient, Amihud ILLIQ ratio) due \
to a STOCHASTIC_DETERMINISTIC_MISMATCH. Your mission is to construct Formula C: \
the expectation operator or ergodic limit that projects the stochastic variable \
into a deterministic scalar, bridging the two parents.

\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550
THE TWO PATTERNS
\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550

PATTERN A \u2014 Expectation Operator (Default)
Trigger:  One parent is a stochastic process (path-dependent, random). The
          other is a deterministic scalar or ratio (fixed, observable).
Operator: Apply E[\u00b7] under stationarity. For a subcritical Hawkes process
          (branching ratio n = \u03b1/\u03b2 < 1), the Palm calculus result gives the
          stationary mean intensity as a behavioral axiom:

            E[\u03bb] = \u03bc / (1 - n)   where n = \u03b1/\u03b2

          For a bivariate Hawkes, use the row-sum marginal:
            E[\u03bb_ES] = \u03bc_ES / (1 - a_ESES - a_ZNES)

          The stochastic derivation is stated as a behavioral axiom.
          Lean 4 proves algebraic consequences only.
Lean 4:   div_pos + linarith \u2014 positivity of \u03bc/(1-n) under subcriticality.
          Uses Seed Proof 4 pattern. No new seed proof needed.

PATTERN B \u2014 Ergodic Limit (Restricted)
Trigger:  ONLY if suggested_bridging_formula explicitly references "time average",
          "ergodic", or "session average". Both patterns describe the same
          underlying physics from different angles — prefer Pattern A unless
          the bridging text specifically demands temporal averaging.
Operator: Ergodic time-average over a trading session:
            \u03bb\u0304 = (1/T) \u222b\u2080\u1d40 \u03bb(t | \u210c_t) dt
          This converges to E[\u03bb] in the stationary subcritical limit.
Lean 4:   Same div_pos + linarith target after the limit is stated axiomatically.

\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550
DOMAIN LOCK \u2014 NON-NEGOTIABLE
\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550

1. MUST NOT claim to derive the expectation from first principles in Lean 4.
   The Palm calculus result E[\u03bb] = \u03bc/(1-n) is stated as a behavioral axiom.
   Agent 151 verifies algebraic positivity only.
2. behavioral_claim MUST explicitly name the operator applied (E[\u00b7] or \u03bb\u0304).
3. MUST prove stationarity prerequisite implicitly: the subcriticality condition
   (n < 1 or h_row1) must appear as a hypothesis in the behavioral_mappings.
4. MUST call validate_sympy_expression before propose_formula_to_blackboard.
5. MUST include rejection_id in the terminal call for the resolves_ tag.

\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550
EXECUTION PHASES
\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550

PHASE 1: fetch_rejection_data(rejection_id) \u2014 read bridging text, confirm pattern.
PHASE 2: fetch_formula_data(uuid_a), fetch_formula_data(uuid_b) \u2014 identify which
         parent is stochastic and which is deterministic.
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
            "STOCHASTIC/DETERMINISTIC GARDENING DIRECTIVE\n"
            "\u2550" * 56 + "\n"
            f"Rejection ID:  {rejection_id}\n"
            f"Parent A UUID: {uuid_a}\n"
            f"Parent B UUID: {uuid_b}\n"
            "\u2550" * 56 + "\n\n"
            "Agent 105 evaluated (A, B) and found STOCHASTIC_DETERMINISTIC_MISMATCH.\n"
            "Your directive: construct Formula C \u2014 the expectation operator or\n"
            "ergodic limit that projects the stochastic parent into a deterministic\n"
            "scalar bridging the two parents.\n\n"
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

        pattern = tool_input.get("pattern_applied", "expectation_operator")
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
            "agent_203",
            "evolutionary",
            "stochastic_deterministic_bridge",
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
                MathematicalWing.ERGODIC_THEORY.value,
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
