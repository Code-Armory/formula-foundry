"""
Agent 201 — Temporal Scale Bridger

Authority: Layer 2 — can write SYNTACTICALLY_CORRECT formulas.
           Cannot formally verify (that is Agent 151's job).

Mission: Read a REJECTED_ISOMORPHISM record, extract the suggested_bridging_formula,
         fetch both parent formulas to understand their variable spaces, and
         construct Formula C — the Missing Link that makes the bridge real.

This agent is the first of the Evolutionary Gardeners (Agents 201-250).
Its specific evolutionary mechanic: resolving temporal mismatch rejections
by constructing integral aggregation operators that translate between
instantaneous rates (point process) and discrete daily aggregates.

Mathematical Wing: Measure Theory + Ergodic Theory
Evolutionary trick: ∫ (instantaneous rate) dt over session window → daily aggregate

Workflow:
  Phase 1: fetch_rejection_data(rejection_id)
           → reads bridging_concept, uuid_a, uuid_b

  Phase 2: fetch_formula_data(uuid_a) + fetch_formula_data(uuid_b)
           → understands parent variable spaces and behavioral mappings

  Phase 3: Reason about the bridging formula. Draft LaTeX.
           Translate to SymPy integral expression. validate_sympy_expression.

  Phase 4: propose_formula_to_blackboard (terminal action)
           → Formula C written as SYNTACTICALLY_CORRECT
           → Librarian Router will autonomously pair C with A and C with B

Agent 201 never pairs formulas, never runs Agent 105, never triggers Agent 151.
It produces ONE formula and goes to sleep. The pipeline handles the rest.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Set

import httpx

from foundry.agents.base import AgentConfig, BaseAgent
from foundry.agents.sympy_executor import validate_formula
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
        "Retrieve a REJECTED_ISOMORPHISM record by rejection_id from the Blackboard. "
        "MUST be your first tool call. "
        "Read the bridging_concept field carefully — it is your complete research directive. "
        "Also note uuid_a and uuid_b: you must fetch both parent formulas next."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "rejection_id": {
                "type": "string",
                "description": "The rejection UUID to retrieve.",
            },
        },
        "required": ["rejection_id"],
    },
}

_FETCH_FORMULA_TOOL = {
    "name": "fetch_formula_data",
    "description": (
        "Retrieve the complete FormulaDNA for a formula UUID from the Blackboard. "
        "Call this for BOTH parent UUIDs (uuid_a and uuid_b from the rejection record). "
        "Study the symbolic_expression_latex and behavioral_mappings carefully — "
        "Formula C must map between the variable spaces of both parents."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "uuid": {
                "type": "string",
                "description": "The formula UUID to retrieve.",
            },
        },
        "required": ["uuid"],
    },
}

_VALIDATE_SYMPY_TOOL = {
    "name": "validate_sympy_expression",
    "description": (
        "Validate a mathematical expression string using SymPy. "
        "Use this to confirm Formula C translates to a valid algebraic object. "
        "\n"
        "CRITICAL for integral expressions (your primary tool):\n"
        "  Use: Integral(integrand, (variable, lower_bound, upper_bound))\n"
        "  Example: Integral(lambda_t, (t, t_open, t_close)) / VOL_d\n"
        "  - 'Integral' is capital I\n"
        "  - lambda_t represents the Hawkes intensity λ(t) as a SymPy symbol\n"
        "  - Do NOT try to expand the Hawkes sum inside the integral — "
        "    the stochastic sum over {t_i} cannot be written as a closed-form SymPy expression. "
        "    Represent λ(t) as a single symbol 'lambda_t'.\n"
        "  - All variable names must be single words: 't_open' not 't open'\n"
        "  - Use '*' for multiplication, '**' for powers\n"
        "\n"
        "If validation fails, read the error and adjust syntax only — "
        "do not change the mathematical content."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "expression_string": {
                "type": "string",
                "description": (
                    "SymPy-compatible expression. "
                    "For Formula C: Integral(lambda_t, (t, t_open, t_close)) / VOL_d"
                ),
            },
            "reasoning": {
                "type": "string",
                "description": "What this expression represents and how it bridges the two parents.",
            },
        },
        "required": ["expression_string", "reasoning"],
    },
}

_PROPOSE_BRIDGING_FORMULA_TOOL = {
    "name": "propose_formula_to_blackboard",
    "description": (
        "TERMINAL ACTION. Submit Formula C — the temporal bridging formula — to the Blackboard. "
        "\n"
        "Prerequisites (strictly enforced):\n"
        "  (1) fetch_rejection_data called and bridging_concept read.\n"
        "  (2) fetch_formula_data called for both parent UUIDs.\n"
        "  (3) validate_sympy_expression returned valid=True.\n"
        "\n"
        "Formula C will be written as SYNTACTICALLY_CORRECT with LAYER_2 authority. "
        "The Librarian Router will automatically pair it with the parent formulas "
        "on subsequent ticks. Agent 105 will then declare the isomorphisms. "
        "Do NOT trigger Agent 105 yourself — the pipeline handles it."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "name": {
                "type": "string",
                "description": "Human-readable name for Formula C, e.g. 'Cumulative Integrated Hawkes Intensity (CIHI)'",
            },
            "description": {
                "type": "string",
                "description": "One paragraph describing what Formula C models and how it bridges the two parents.",
            },
            "symbolic_expression_latex": {
                "type": "string",
                "description": (
                    "Formula C in LaTeX. Must be the direct operationalization of the "
                    "bridging_concept from the rejection record. "
                    "E.g. r'\\Lambda_d = \\int_{t_{open}}^{t_{close}} \\lambda(t | H_t)\\, dt'"
                ),
            },
            "behavioral_claim": {
                "type": "string",
                "description": (
                    "The psychological mechanism Formula C captures. "
                    "Must explain how it translates the parent A mechanism into parent B's domain. "
                    "E.g. 'Aggregates the instantaneous panic cascade rate λ(t) over a full "
                    "trading session, producing a daily illiquidity signal comparable to Amihud ILLIQ.'"
                ),
            },
            "behavioral_mappings": {
                "type": "array",
                "description": (
                    "One entry per variable in Formula C. "
                    "Include all new variables introduced (e.g. Λ_d, t_open, t_close) "
                    "AND inherited variables from parents (e.g. λ(t), VOL_d) with their "
                    "original behavioral interpretations preserved."
                ),
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
                "minItems": 1,
            },
            "tags": {
                "type": "array",
                "items": {"type": "string"},
                "description": (
                    "Domain-specific tags. 'temporal_bridge' and 'evolutionary' "
                    "will be added automatically. Add mathematical descriptors: "
                    "e.g. ['integral_operator', 'daily_aggregate', 'cihi']"
                ),
            },
        },
        "required": [
            "name", "description", "symbolic_expression_latex",
            "behavioral_claim", "behavioral_mappings",
        ],
    },
}


# ---------------------------------------------------------------------------
# Agent 201
# ---------------------------------------------------------------------------


class TemporalScaleBridger(BaseAgent):
    """
    Agent 201 — Temporal Scale Bridger.
    First Evolutionary Gardener. Resolves temporal mismatch rejections
    by constructing integral aggregation operators (Formula C).
    """

    AGENT_ID = "agent_201"
    AGENT_LAYER = AgentLayer.LAYER_2
    MATHEMATICAL_WING = [
        MathematicalWing.MEASURE_THEORY,
        MathematicalWing.ERGODIC_THEORY,
        MathematicalWing.EVOLUTIONARY,
    ]

    SYSTEM_PROMPT = """\
You are Agent 201, the Temporal Scale Bridger — the first of the Evolutionary \
Gardeners within the Formula Foundry. Your existence is dedicated to resolving \
one specific class of mathematical incompatibility: the temporal scale mismatch.

═══════════════════════════════════════════════════════
YOUR IDENTITY AND PURPOSE
═══════════════════════════════════════════════════════

When Agent 105 (the Isomorphism Synthesizer) determines that two formulas \
cannot be connected, it records a formal rejection with a suggested_bridging_formula. \
Your job is to read that rejection and construct the Missing Link — Formula C.

Formula C is not a compromise. It is not a weighted average of the parents. \
It is a new mathematical object that, once it exists in the Blackboard, \
makes both connections (C↔A and C↔B) obvious to Agent 105.

You are a MATHEMATICAL COMPILER, not a speculator. The bridging_concept field \
in your rejection record is not a vague suggestion. It is a specification. \
Your job is to operationalize it as a rigorous formula, validate it in SymPy, \
and commit it to the Blackboard.

═══════════════════════════════════════════════════════
AXIOMATIC DOMAIN: MEASURE THEORY + ERGODIC THEORY
═══════════════════════════════════════════════════════

Your domain is temporal aggregation: translating between instantaneous \
rates and accumulated measures over time windows.

Your vocabulary:
  Λ(T) = ∫₀ᵀ λ(t) dt        — Cumulative intensity (Poisson process theory)
  Λ_d = ∫_{t_open}^{t_close} λ(t) dt  — Session-aggregated intensity
  E_T[λ(t)] = (1/T) ∫₀ᵀ λ(t) dt      — Time-average intensity (Ergodic)
  μ_d = Λ_d / VOL_d                    — Volume-normalized aggregate

Your bridge pattern:
  A → instantaneous rate (point process, microsecond resolution)
  B → discrete daily aggregate (ratio statistic, session resolution)
  C → integral operator that maps A's domain into B's domain

DOMAIN LOCK — HARD CONSTRAINTS:

1. You ONLY use mathematics from: measure theory, Lebesgue integration, \
   ergodic theory, temporal aggregation, expected value over time windows, \
   measure-preserving transformations.

2. You DO NOT invent new psychological mechanisms. Formula C inherits the \
   behavioral semantics from its parents. A daily integral of panic intensity \
   is still panic intensity — just aggregated. Preserve the psychological \
   interpretation from the parent behavioral_mappings.

3. You DO NOT attempt to expand λ(t) in SymPy. The Hawkes intensity is a \
   stochastic process — its definition involves a sum over random event times \
   that cannot be expressed as a closed-form algebraic expression. \
   Represent λ(t) as a single symbol 'lambda_t' in your SymPy expression.

═══════════════════════════════════════════════════════
LEAN 4 SCOPE (WHAT NOT TO ATTEMPT)
═══════════════════════════════════════════════════════

You do not attempt Lean 4 proofs. That is Agent 151's job. You write \
Formula C as SYNTACTICALLY_CORRECT — SymPy-validated, behaviorally mapped, \
formally specified in LaTeX. Agent 151 will prove the properties when the \
Librarian eventually routes Formula C through the full pipeline.

═══════════════════════════════════════════════════════
SYMPY INTEGRAL REFERENCE
═══════════════════════════════════════════════════════

The SymPy executor has Integral in its whitelist. Use it as follows:

  Simple integral:
    Integral(lambda_t, (t, t_open, t_close))

  Normalized (CIHI):
    Integral(lambda_t, (t, t_open, t_close)) / VOL_d

  Time-average:
    Integral(lambda_t, (t, t_open, t_close)) / (t_close - t_open)

Rules:
  - 'Integral' is capital I
  - Integration variable must appear in (var, lower, upper) tuple
  - All symbols must be single-word Python identifiers
  - lambda_t is the Hawkes intensity treated as a symbol (do not expand it)
  - Multiplication: use '*' operator

═══════════════════════════════════════════════════════
MANDATORY WORKFLOW — FOUR PHASES
═══════════════════════════════════════════════════════

PHASE 1 — READ THE DIRECTIVE (mandatory first action)
  Call fetch_rejection_data with the rejection_id provided.
  Read bridging_concept in full. This is your specification.
  Note uuid_a and uuid_b — you must fetch both parent formulas.

PHASE 2 — UNDERSTAND THE VARIABLE SPACES
  Call fetch_formula_data for uuid_a (parent A).
  Call fetch_formula_data for uuid_b (parent B).
  Study their symbolic_expression_latex and behavioral_mappings.
  Identify: what is the key variable in A that needs aggregation?
  Identify: what is the target structure of B that Formula C must resemble?

PHASE 3 — CONSTRUCT AND VALIDATE FORMULA C
  Draft Formula C in LaTeX. It must be the exact operationalization
  of the bridging_concept, bridging parent A's domain to parent B's.
  Translate to SymPy. Call validate_sympy_expression.
  If it fails, correct syntax only — do not change the math.

PHASE 4 — COMMIT (terminal action)
  Call propose_formula_to_blackboard with the complete FormulaDNA fields.
  Include behavioral_mappings for every variable in Formula C.
  Your session ends here. The Librarian Router handles the rest.

═══════════════════════════════════════════════════════
EXIT CONDITION
═══════════════════════════════════════════════════════

Every run ends with exactly one:
  propose_formula_to_blackboard — Formula C committed to Blackboard

Ending without calling this is a bug. If you find yourself uncertain \
about Formula C's exact form, operationalize the bridging_concept literally. \
The bridging_concept is a specification, not a suggestion.
"""

    # ------------------------------------------------------------------
    # State: gate enforcement
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
            _PROPOSE_BRIDGING_FORMULA_TOOL,
        ]

    def build_initial_message(self, trigger_data: Dict[str, Any]) -> str:
        rejection_id = trigger_data.get("rejection_id", "UNKNOWN")
        uuid_a = trigger_data.get("uuid_a", "unknown")
        uuid_b = trigger_data.get("uuid_b", "unknown")
        bridging_concept_preview = trigger_data.get("bridging_concept_preview", "")

        return f"""\
EVOLUTIONARY GARDENING DIRECTIVE
════════════════════════════════════════════════════════
Rejection ID:  {rejection_id}
Parent A UUID: {uuid_a}
Parent B UUID: {uuid_b}
════════════════════════════════════════════════════════

Agent 105 evaluated the pair (A, B) and found no isomorphism at any tier.
Your directive: construct Formula C — the Missing Link.

Bridging concept preview:
  {bridging_concept_preview[:200]}{"..." if len(bridging_concept_preview) > 200 else ""}

════════════════════════════════════════════════════════

Fetch the full rejection record to read the complete bridging_concept.
Then fetch both parent formulas to understand their variable spaces.
Construct the integral aggregation operator that bridges their domains.

Begin Phase 1: fetch_rejection_data("{rejection_id}").\
"""

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
            # Handled by base class terminal action logic via extract_formula.
            return {"accepted": True}
        return {"error": f"Unknown tool: {tool_name}"}

    def extract_formula(
        self,
        tool_input: Dict[str, Any],
        validated_sympy_str: str,
    ) -> FormulaDNA:
        """Build Formula C from the proposal tool call."""
        # Gate enforcement
        if not self._rejection_fetched:
            raise ValueError(
                "Gate violation: propose_formula_to_blackboard requires "
                "fetch_rejection_data to have been called first."
            )
        if len(self._parent_uuids_fetched) < 2:
            raise ValueError(
                f"Gate violation: propose_formula_to_blackboard requires "
                f"fetch_formula_data for both parent UUIDs. "
                f"Only fetched: {self._parent_uuids_fetched}"
            )

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

        rejection_id_short = ""
        if self._rejection_data:
            rejection_id_short = self._rejection_data.get("rejection_id", "")[:8]

        tags = tool_input.get("tags", []) + [
            "agent_201",
            "temporal_bridge",
            "evolutionary",
            "integral_operator",
        ]
        if rejection_id_short:
            tags.append(f"resolves_{rejection_id_short}")

        return FormulaDNA(
            name=tool_input.get("name"),
            description=tool_input.get("description"),
            symbolic_expression_latex=tool_input["symbolic_expression_latex"],
            symbolic_expression_sympy=validated_sympy_str,
            behavioral_claim=tool_input["behavioral_claim"],
            behavioral_mappings=behavioral_mappings,
            axiomatic_origin=["agent_201"],
            mathematical_wing=[
                MathematicalWing.MEASURE_THEORY.value,
                MathematicalWing.ERGODIC_THEORY.value,
                MathematicalWing.EVOLUTIONARY.value,
            ],
            proof_status=ProofStatus.SYNTACTICALLY_CORRECT,
            tags=tags,
        )

    # ------------------------------------------------------------------
    # Tool handlers
    # ------------------------------------------------------------------

    async def _handle_fetch_rejection(self, tool_input: Dict[str, Any]) -> Dict[str, Any]:
        """Fetch the rejection record by rejection_id from GET /v1/rejections."""
        rejection_id = tool_input.get("rejection_id", "")
        url = f"{self._config.blackboard_api_url}/v1/rejections"

        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.get(url)
            resp.raise_for_status()
            rejections = resp.json()
        except httpx.RequestError as exc:
            return {"error": f"Blackboard API unreachable: {exc}", "found": False}

        for r in rejections:
            if r.get("rejection_id") == rejection_id:
                self._rejection_fetched = True
                self._rejection_data = r
                logger.info(
                    "[%s] Fetched rejection %s: %s ↔ %s",
                    self.AGENT_ID, rejection_id[:8],
                    r.get("uuid_a", "")[:8], r.get("uuid_b", "")[:8],
                )
                return {
                    "found": True,
                    "rejection_id": rejection_id,
                    "uuid_a": r.get("uuid_a"),
                    "uuid_b": r.get("uuid_b"),
                    "agent_version": r.get("agent_version"),
                    "bridging_concept": r.get("bridging_concept"),
                    "timestamp": r.get("timestamp"),
                    "message": (
                        "Read bridging_concept in full — it is your complete specification. "
                        "Now call fetch_formula_data for both uuid_a and uuid_b."
                    ),
                }

        return {
            "found": False,
            "error": (
                f"Rejection '{rejection_id}' not found in Blackboard. "
                f"Available rejection IDs: "
                f"{[r.get('rejection_id', '')[:8] for r in rejections[:5]]}"
            ),
        }

    async def _handle_fetch_formula(self, tool_input: Dict[str, Any]) -> Dict[str, Any]:
        """Fetch a parent formula by UUID."""
        uuid = tool_input.get("uuid", "")
        url = f"{self._config.blackboard_api_url}/v1/formulas/{uuid}"

        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.get(url)
            if resp.status_code == 404:
                return {"error": f"Formula {uuid} not found in Blackboard.", "found": False}
            resp.raise_for_status()
            data = resp.json()
        except httpx.RequestError as exc:
            return {"error": f"Blackboard API unreachable: {exc}", "found": False}

        self._parent_uuids_fetched.add(uuid)
        logger.info("[%s] Fetched formula %s: %s", self.AGENT_ID, uuid[:8], data.get("name"))

        return {
            "found": True,
            "uuid": uuid,
            "name": data.get("name"),
            "symbolic_expression_latex": data.get("symbolic_expression_latex"),
            "behavioral_claim": data.get("behavioral_claim"),
            "behavioral_mappings": data.get("behavioral_mappings", []),
            "mathematical_wing": data.get("mathematical_wing", []),
            "proof_status": data.get("proof_status"),
            "tags": data.get("tags", []),
        }

    def _handle_validate(self, tool_input: Dict[str, Any]) -> Dict[str, Any]:
        expression = tool_input.get("expression_string", "")
        result = validate_formula(expression, timeout=self._config.sympy_timeout_seconds)

        if result.get("valid"):
            logger.info("[%s] SymPy PASSED: %s", self.AGENT_ID, expression[:80])
            return {
                "valid": True,
                "sympy_str": result["sympy_str"],
                "latex_roundtrip": result.get("latex_roundtrip"),
                "free_symbols": result.get("free_symbols", []),
                "message": (
                    "Expression is algebraically valid. "
                    "Verify all free symbols appear in your behavioral_mappings. "
                    "You may now call propose_formula_to_blackboard."
                ),
            }
        else:
            logger.info("[%s] SymPy FAILED: %s", self.AGENT_ID, result.get("error"))
            return {
                "valid": False,
                "error": result.get("error"),
                "message": (
                    "Expression failed validation. Correct syntax only — do not change the math. "
                    "Remember: Integral (capital I), single-word variable names, "
                    "'*' for multiplication."
                ),
            }
