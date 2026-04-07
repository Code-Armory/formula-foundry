"""
Agent 060 — Macro Cross-Asset Specialist

Axiomatic domain: Macro Cross-Asset + Functional Analysis + Probability Theory
Behavioral focus: Bivariate self-exciting point processes, cross-asset contagion,
                  flight-to-quality dynamics, coordinated liquidation cascades.

This agent is the second eye of the Formula Foundry. Agent 089 watches a single
instrument (ES) for isolated panic. Agent 060 watches the cross-asset relationship
between ES (S&P 500 futures) and ZN (10-Year Treasury futures) for systemic events.

The crown jewel isomorphism target: Agent 060's output ↔ Agent 089's output
via Agent 105. The connection is a bivariate (vector) Hawkes process where the
scalar α of Agent 089 becomes a 2×2 excitation matrix A. The off-diagonal entries
of A (cross-excitation terms) are the novel IP — they quantify how aggressively
equity panic excites Treasury buying (or selling).

Authority: Layer 1 (Specialist) — writes SYNTACTICALLY_CORRECT formulas
           Agent 151 verifies subcriticality via the row-sum M-matrix criterion.

DOMAIN LOCK — NON-NEGOTIABLE CONSTRAINTS:
  MUST: Express joint intensity as λ(t) = [λ_ES(t), λ_ZN(t)]ᵀ (column vector)
  MUST: Express cross-excitation as a 2×2 matrix A where A_ij = excitation j→i
  MUST: State subcriticality as row-sum condition (row i: Σ_j A_ij < 1)
  MUST: Include behavioral mapping for all four entries of A
  MUST: Specify regime explicitly (FLIGHT_TO_QUALITY or COORDINATED_LIQUIDATION)
  NEVER: Reduce to a univariate formula — off-diagonal must be non-zero
  NEVER: Use game theory, entropy, or topology framing
  NEVER: Treat the two instruments as independent (this is the whole point)

Regime behavioral distinction:
  FLIGHT_TO_QUALITY:      A_ZN→ES < 0 in OFI space (Treasury buying dampens equity panic)
  COORDINATED_LIQUIDATION: A_ZN→ES > 0 (Treasury selling amplifies equity panic — no safe haven)

SymPy validation target: the intensity for one component, e.g.:
  mu_ES + a_ESES * exp(-b_ESES * (t - t_i)) + a_ZNES * exp(-b_ZNES * (t - t_j))
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

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

# ---------------------------------------------------------------------------
# Tool schemas
# ---------------------------------------------------------------------------

_VALIDATE_SYMPY_TOOL = {
    "name": "validate_sympy_expression",
    "description": (
        "Validate a mathematical expression string using SymPy. "
        "For Agent 060, validate ONE COMPONENT of the bivariate intensity vector. "
        "The full vector λ(t) = [λ_ES(t), λ_ZN(t)]ᵀ cannot be expressed as a single "
        "SymPy scalar. Instead, validate the scalar expression for λ_ES(t) or λ_ZN(t) alone.\n"
        "\n"
        "Recommended validation expression for λ_ES(t):\n"
        "  mu_ES + a_ESES * exp(-b_ESES * (t - t_i)) + a_ZNES * exp(-b_ZNES * (t - t_j))\n"
        "\n"
        "Variable naming rules:\n"
        "  Matrix entries: a_ESES, a_ZNES, a_ESZN, a_ZNZN (row_col notation)\n"
        "  Decay rates:    b_ESES, b_ZNES, b_ESZN, b_ZNZN\n"
        "  All single-word identifiers, no spaces, no subscript notation.\n"
        "  Use * for multiplication, ** for powers, exp() for exponential.\n"
        "\n"
        "If validation fails, correct syntax only — do not change the mathematics."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "expression_string": {
                "type": "string",
                "description": (
                    "Scalar SymPy expression for ONE intensity component. "
                    "E.g. 'mu_ES + a_ESES * exp(-b_ESES * (t - t_i)) "
                    "+ a_ZNES * exp(-b_ZNES * (t - t_j))'"
                ),
            },
            "reasoning": {
                "type": "string",
                "description": (
                    "Which component you are validating (ES or ZN), and how "
                    "the off-diagonal term captures the cross-asset mechanism."
                ),
            },
        },
        "required": ["expression_string", "reasoning"],
    },
}

_PROPOSE_FORMULA_TOOL = {
    "name": "propose_formula_to_blackboard",
    "description": (
        "TERMINAL ACTION. Submit the bivariate Vector Hawkes formula to the Blackboard.\n"
        "\n"
        "Prerequisites (enforced):\n"
        "  (1) validate_sympy_expression returned valid=True for at least one component.\n"
        "  (2) All four matrix entries (A_ES→ES, A_ZN→ES, A_ES→ZN, A_ZN→ZN) are in "
        "behavioral_mappings with distinct psychological interpretations.\n"
        "  (3) regime field is set to 'flight_to_quality' or 'coordinated_liquidation'.\n"
        "\n"
        "The symbolic_expression_latex must express the FULL VECTOR SYSTEM in LaTeX, e.g.:\n"
        "  \\vec{\\lambda}(t) = \\vec{\\mu} + \\mathcal{A} \\sum_{t_i < t} \\vec{\\phi}(t - t_i)\n"
        "\n"
        "This is a terminal action — your session ends here."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "name": {
                "type": "string",
                "description": "E.g. 'Bivariate Flight-to-Quality Hawkes Intensity v1'",
            },
            "description": {"type": "string"},
            "symbolic_expression_latex": {
                "type": "string",
                "description": (
                    "The FULL bivariate system in LaTeX. Must show both components "
                    "and the 2×2 excitation matrix. "
                    "E.g. r'\\vec{\\lambda}(t) = \\vec{\\mu} + \\mathcal{A} \\cdot \\vec{h}(t)'"
                ),
            },
            "regime": {
                "type": "string",
                "enum": ["flight_to_quality", "coordinated_liquidation"],
                "description": (
                    "The market regime this formula models. Determines the sign and "
                    "interpretation of the off-diagonal matrix entries."
                ),
            },
            "behavioral_claim": {
                "type": "string",
                "description": (
                    "Must reference: (1) the bivariate nature, (2) the specific regime, "
                    "(3) the off-diagonal contagion mechanism. "
                    "E.g. 'Models the flight-to-quality cascade as a bivariate Hawkes process "
                    "where equity panic (λ_ES) excites Treasury buying (λ_ZN) through the "
                    "cross-excitation coefficient A_ZN→ES, capturing the inverse correlation "
                    "between equity and bond order flows during risk-off events.'"
                ),
            },
            "behavioral_mappings": {
                "type": "array",
                "description": (
                    "MUST include entries for: mu_ES, mu_ZN, ALL FOUR matrix entries "
                    "(a_ESES, a_ZNES, a_ESZN, a_ZNZN), and the decay rates. "
                    "The off-diagonal entries are the novel IP — their behavioral "
                    "interpretations must be specific and empirically grounded."
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
                "minItems": 6,
            },
            "subcriticality_condition": {
                "type": "string",
                "description": (
                    "State the row-sum subcriticality conditions explicitly as inequalities. "
                    "These are what Agent 151 will prove in Lean 4. "
                    "E.g. 'Row 1: a_ESES + a_ZNES < 1 (ES subcriticality). "
                    "Row 2: a_ESZN + a_ZNZN < 1 (ZN subcriticality). "
                    "Together imply spectral radius ρ(A) < 1 by M-matrix criterion.'"
                ),
            },
            "tags": {
                "type": "array",
                "items": {"type": "string"},
                "description": "E.g. ['vector_hawkes', 'cross_asset', 'flight_to_quality', 'agent_060']",
            },
        },
        "required": [
            "name", "description", "symbolic_expression_latex",
            "regime", "behavioral_claim", "behavioral_mappings",
            "subcriticality_condition",
        ],
    },
}


# ---------------------------------------------------------------------------
# Agent 060
# ---------------------------------------------------------------------------


class MacroCrossAssetSpecialist(BaseAgent):
    """
    Agent 060 — Macro Cross-Asset Specialist.
    Triggered by ES↔ZN OFI correlation regime shifts.
    Produces bivariate Vector Hawkes formulas with 2×2 excitation matrices.
    """

    AGENT_ID = "agent_060"
    AGENT_LAYER = AgentLayer.LAYER_1
    MATHEMATICAL_WING = [
        MathematicalWing.MACRO_CROSS_ASSET,
        MathematicalWing.FUNCTIONAL_ANALYSIS,
        MathematicalWing.PROBABILITY_INFORMATION,
    ]

    SYSTEM_PROMPT = """\
You are Agent 060, the Macro Cross-Asset Specialist within the Formula Foundry.

═══════════════════════════════════════════════════════
YOUR IDENTITY
═══════════════════════════════════════════════════════

Agent 089 watches a single instrument for isolated panic. You watch the \
RELATIONSHIP between instruments for systemic events. Where Agent 089 sees \
one river flooding, you see the entire watershed.

You model the mathematical structure of cross-asset contagion: how panic in \
one market excites order flow in another, and how that cross-excitation either \
dampens or amplifies the original panic.

Your output is the Foundry's highest-value IP: a formally specified bivariate \
Hawkes process with a 2×2 cross-excitation matrix whose off-diagonal entries \
model cross-asset contagion that no single-instrument model can capture.

═══════════════════════════════════════════════════════
THE MATHEMATICAL STRUCTURE YOU PRODUCE
═══════════════════════════════════════════════════════

The bivariate Hawkes intensity vector:

  λ(t) = [λ_ES(t), λ_ZN(t)]ᵀ

  λ_ES(t) = μ_ES + Σ_{t_i<t} [A_ES→ES · φ_ES(t-t_i^ES) + A_ZN→ES · φ_ZN(t-t_j^ZN)]
  λ_ZN(t) = μ_ZN + Σ_{t_i<t} [A_ES→ZN · φ_ES(t-t_i^ES) + A_ZN→ZN · φ_ZN(t-t_j^ZN)]

Where:
  μ_ES, μ_ZN: baseline intensities for each instrument
  A: the 2×2 excitation matrix
    A_ES→ES: self-excitation of equity panic (equity sell → more equity sells)
    A_ZN→ES: cross-excitation from Treasury to Equity (ZN order flow → ES response)
    A_ES→ZN: cross-excitation from Equity to Treasury (ES panic → ZN response)
    A_ZN→ZN: self-excitation of Treasury panic (Treasury sell → more Treasury sells)
  φ_i(t): exponential decay kernels, typically e^{-β_i·t}

SUBCRITICALITY (what Agent 151 will prove):
  Row-sum condition guarantees ρ(A) < 1 (spectral radius < 1 = mean-reverting):
    Row 1: A_ES→ES + A_ZN→ES < 1
    Row 2: A_ES→ZN + A_ZN→ZN < 1
  By the M-matrix determinant criterion: det(I - A) = (1-A_ESES)(1-A_ZNZN) - A_ZNES·A_ESZN > 0

═══════════════════════════════════════════════════════
THE TWO REGIMES
═══════════════════════════════════════════════════════

FLIGHT_TO_QUALITY (Regime A):
  Empirical signature: ρ(ES_OFI_z, ZN_OFI_z) < -0.65
  Equity is aggressively sold, Treasuries are aggressively bought simultaneously.
  Off-diagonal interpretation:
    A_ZN→ES < 0 in behavioral terms: Treasury buying DAMPENS equity panic
    A_ES→ZN > 0: equity panic EXCITES Treasury buying
  Behavioral claim template: "Models the flight-to-quality cascade where equity \
  panic (λ_ES) excites Treasury buying (λ_ZN) through A_ES→ZN, while Treasury \
  buying partially dampens subsequent equity panic through A_ZN→ES."

COORDINATED_LIQUIDATION (Regime B):
  Empirical signature: ρ(ES_OFI_z, ZN_OFI_z) > +0.60
  Both equities AND Treasuries are aggressively sold — no safe haven.
  Off-diagonal interpretation:
    A_ZN→ES > 0: Treasury selling AMPLIFIES equity panic (margin calls, forced selling)
    A_ES→ZN > 0: equity panic AMPLIFIES Treasury selling (risk parity unwind)
  Behavioral claim template: "Models the coordinated liquidation cascade where \
  both equity and Treasury panic are mutually reinforcing through positive \
  off-diagonal entries in A, reflecting forced deleveraging and risk-parity \
  unwinding that eliminates the traditional flight-to-quality safe haven."

═══════════════════════════════════════════════════════
DOMAIN LOCK — HARD CONSTRAINTS
═══════════════════════════════════════════════════════

1. You ALWAYS express intensity as a column vector λ(t) = [λ_ES(t), λ_ZN(t)]ᵀ.
   A scalar formula from this agent is architecturally invalid.

2. You ALWAYS include the 2×2 excitation matrix A explicitly. All four entries
   must appear in the formula AND in behavioral_mappings with distinct psychological
   interpretations. The off-diagonal entries are the intellectual property.

3. You ALWAYS state the row-sum subcriticality condition explicitly in the
   subcriticality_condition field. These are the algebraic statements Agent 151
   will prove in Lean 4 using the M-matrix criterion.

4. You ALWAYS identify the regime (flight_to_quality or coordinated_liquidation)
   and explain how it determines the SIGN of the off-diagonal entries.

5. You NEVER use: game theory, entropy, topology framing. These are other agents'
   domains. You may not treat the two instruments as independent.

6. You NEVER collapse to a univariate formula. If your LaTeX only shows λ_ES(t)
   or only λ_ZN(t), your formula is incomplete. Both components are required.

═══════════════════════════════════════════════════════
SYMPY VALIDATION STRATEGY
═══════════════════════════════════════════════════════

The full vector λ(t) cannot be validated as a single SymPy scalar. Instead:

  Validate the λ_ES(t) component:
    mu_ES + a_ESES * exp(-b_ESES * (t - t_i)) + a_ZNES * exp(-b_ZNES * (t - t_j))

  Variable naming: use a_ESES for A_ES→ES, a_ZNES for A_ZN→ES, etc.
  All subscripts become part of the variable name: a_ZNES, b_ZNES.
  No spaces, no superscripts, no → symbols in SymPy strings.

  If this validates, the λ_ZN(t) component is structurally identical —
  you do not need to validate both.

═══════════════════════════════════════════════════════
MANDATORY WORKFLOW
═══════════════════════════════════════════════════════

STEP 1: Read the trigger data. Identify the regime from the correlation value.
        Draft the bivariate LaTeX formula with explicit matrix entries.

STEP 2: Translate the λ_ES(t) component to SymPy and call validate_sympy_expression.
        If it fails, correct syntax only. Retry up to 4 times.

STEP 3: Once valid, call propose_formula_to_blackboard with:
        - The FULL bivariate system in symbolic_expression_latex
        - regime set to the correct value
        - behavioral_mappings for ALL four matrix entries
        - subcriticality_condition stated explicitly

STEP 4: Your session ends. Agent 151 will prove the subcriticality conditions.
        Agent 105 will eventually find the isomorphism to Agent 089's formula.
        The off-diagonal entries are the bridge.
"""

    # ------------------------------------------------------------------
    # State
    # ------------------------------------------------------------------

    def __init__(self, config: AgentConfig) -> None:
        super().__init__(config)
        self._sympy_validated: bool = False

    # ------------------------------------------------------------------
    # BaseAgent interface
    # ------------------------------------------------------------------

    def tools(self) -> List[Dict[str, Any]]:
        return [_VALIDATE_SYMPY_TOOL, _PROPOSE_FORMULA_TOOL]

    def build_initial_message(self, trigger_data: Dict[str, Any]) -> str:
        regime          = trigger_data.get("regime", "unknown")
        correlation     = trigger_data.get("correlation", 0.0)
        instrument_a    = trigger_data.get("instrument_a", "ES.c.0")
        instrument_b    = trigger_data.get("instrument_b", "ZN.c.0")
        timestamp       = trigger_data.get("timestamp", "unknown")
        n_windows       = trigger_data.get("n_windows", 0)

        # Extract per-instrument z-scores using the instrument name
        es_key = f"ofi_zscore_{instrument_a.split('.')[0].lower()}"
        zn_key = f"ofi_zscore_{instrument_b.split('.')[0].lower()}"
        es_z   = trigger_data.get(es_key, trigger_data.get("ofi_z_a", 0.0))
        zn_z   = trigger_data.get(zn_key, trigger_data.get("ofi_z_b", 0.0))

        regime_description = {
            "flight_to_quality": (
                "FLIGHT-TO-QUALITY: Capital is simultaneously LEAVING equities and "
                "ENTERING Treasuries. This is the classic risk-off cascade. "
                "The off-diagonal A_ES→ZN should be POSITIVE (equity panic excites "
                "Treasury buying). A_ZN→ES may be NEGATIVE (Treasury buying provides "
                "a partial dampening signal on subsequent equity selling)."
            ),
            "coordinated_liquidation": (
                "COORDINATED LIQUIDATION: Capital is simultaneously LEAVING BOTH "
                "equities AND Treasuries. No safe haven. This is a deleveraging event. "
                "Both off-diagonal entries should be POSITIVE (mutual amplification). "
                "Possible causes: risk-parity unwind, margin calls, forced selling "
                "across all asset classes simultaneously."
            ),
        }.get(regime, f"Unknown regime: {regime}")

        return f"""\
CROSS-ASSET TRIGGER RECEIVED
════════════════════════════════════════════════════════
Timestamp:        {timestamp}
Instruments:      {instrument_a} (A) ↔ {instrument_b} (B)
Regime:           {regime.upper()}
════════════════════════════════════════════════════════
Rolling OFI Correlation (ρ): {correlation:.4f}
{instrument_a.split('.')[0]} OFI Z-Score:       {es_z:.3f}σ
{instrument_b.split('.')[0]} OFI Z-Score:       {zn_z:.3f}σ
Windows collected:            {n_windows} (×30s = {n_windows * 30}s of history)
════════════════════════════════════════════════════════

REGIME INTERPRETATION:
{regime_description}

════════════════════════════════════════════════════════

Your task: Construct the bivariate Hawkes intensity vector that formally \
models this cross-asset regime as a self-exciting point process with \
cross-excitation.

The NOVEL IP is in the off-diagonal entries of the excitation matrix A:
  A_ES→ZN: how equity panic excites Treasury order flow
  A_ZN→ES: how Treasury order flow affects subsequent equity order flow

These two entries have NEVER been formally specified and verified in the \
literature. This formula is original research.

Begin your analysis. Identify the regime, draft the LaTeX, validate SymPy, propose.\
"""

    async def handle_tool_call(
        self,
        tool_name: str,
        tool_input: Dict[str, Any],
    ) -> Dict[str, Any]:
        if tool_name == "validate_sympy_expression":
            return self._handle_validate(tool_input)
        if tool_name == "propose_formula_to_blackboard":
            # Handled by base class terminal action via extract_formula
            return {"accepted": True}
        return {"error": f"Unknown tool: {tool_name}"}

    def _handle_validate(self, tool_input: Dict[str, Any]) -> Dict[str, Any]:
        expression = tool_input.get("expression_string", "")
        logger.info("[%s] Validating: %s", self.AGENT_ID, expression[:100])
        result = validate_formula(expression, timeout=self._config.sympy_timeout_seconds)

        if result.get("valid"):
            self._sympy_validated = True
            symbols = result.get("free_symbols", [])
            logger.info("[%s] SymPy PASSED. Symbols: %s", self.AGENT_ID, symbols)

            # Check for off-diagonal matrix entries in the validated expression
            has_cross_term = any(
                s for s in symbols
                if ("ZNES" in s or "ESZN" in s or "znes" in s or "eszn" in s)
            )
            cross_term_note = (
                "✓ Cross-excitation term detected in expression. "
                if has_cross_term else
                "⚠ No cross-excitation term detected. Ensure A_ZN→ES or A_ES→ZN "
                "appears in the validated expression — these are the novel IP."
            )

            return {
                "valid": True,
                "sympy_str": result["sympy_str"],
                "free_symbols": symbols,
                "message": (
                    f"Component validated. {cross_term_note} "
                    "You may now call propose_formula_to_blackboard with the "
                    "full bivariate system in symbolic_expression_latex."
                ),
            }
        else:
            logger.info("[%s] SymPy FAILED: %s", self.AGENT_ID, result.get("error"))
            return {
                "valid": False,
                "error": result.get("error"),
                "message": (
                    "Validation failed. Correct syntax only — do not change the math. "
                    "Common fixes: use exp() not e^x, * for multiplication, "
                    "single-word variable names (a_ZNES not a_{ZN→ES})."
                ),
            }

    def extract_formula(
        self,
        tool_input: Dict[str, Any],
        validated_sympy_str: str,
    ) -> FormulaDNA:
        """Build FormulaDNA from Agent 060's proposal."""
        regime = tool_input.get("regime", "unknown")

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

        subcriticality = tool_input.get("subcriticality_condition", "")

        # Build description that captures the subcriticality condition
        description = tool_input.get("description", "")
        if subcriticality and "subcriticality" not in description.lower():
            description = f"{description}\n\nSubcriticality: {subcriticality}"

        base_tags = [
            "agent_060",
            "vector_hawkes",
            "cross_asset",
            "bivariate",
            regime,
            "macro_cross_asset",
        ]
        user_tags = [t for t in tool_input.get("tags", []) if t not in base_tags]

        return FormulaDNA(
            name=tool_input.get("name"),
            description=description,
            symbolic_expression_latex=tool_input["symbolic_expression_latex"],
            symbolic_expression_sympy=validated_sympy_str,
            behavioral_claim=tool_input["behavioral_claim"],
            behavioral_mappings=behavioral_mappings,
            axiomatic_origin=[self.AGENT_ID],
            mathematical_wing=[
                MathematicalWing.MACRO_CROSS_ASSET.value,
                MathematicalWing.FUNCTIONAL_ANALYSIS.value,
                MathematicalWing.PROBABILITY_INFORMATION.value,
            ],
            proof_status=ProofStatus.SYNTACTICALLY_CORRECT,
            tags=base_tags + user_tags,
        )
