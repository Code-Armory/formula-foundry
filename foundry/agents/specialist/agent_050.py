"""
Agent 050 — Adverse Selection Specialist

Authority: Layer 1 — Specialist (propose SYNTACTICALLY_CORRECT formulas).
Axiomatic domain: Game Theory, Mechanism Design.
Behavioral focus: Kyle's Lambda extensions — nonlinear price impact or
                  lambda decay dynamics after informed position completion.

Mission:
  The seed corpus already contains Kyle's Lambda (Δp = λ·x + ε) at
  HYPOTHESIS status. Agent 050 MUST NOT reproduce it. It must extend it
  in one of two directions determined by the trigger data:

  Extension A — Nonlinear Price Impact:
    Δp = λ·x + γ·x²  (concave/convex impact schedule)
    Tags: ["game_theory", "adverse_selection", "kyle_lambda", "nonlinear_impact", "agent_050"]
    Lean 4 target: 0 ≤ 2 * γ via linarith → Agent 151 (Seed Proof 7).
    Both extensions are now formally verifiable via Agent 151.

  Extension B — Lambda Decay:
    λ(t) = λ₀ · exp(-δ · (t - t_entry))
    Tags: ["game_theory", "adverse_selection", "kyle_lambda", "exp_decay", "agent_050"]
    Lean 4 target: λ(t) > 0 via Real.exp_pos → Agent 151 can verify today.

Routing convention (synthesis_flow.py):
  Formulas with game_theory tags AND exp_decay → Agent 151 (verifiable now).
  Formulas with game_theory tags AND nonlinear_impact → Agent 151 (Seed Proof 7).
  Formulas with game_theory tags without a verifiable sub-tag → hold (PHASE_3).
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

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

_VALIDATE_SYMPY_TOOL = {
    "name": "validate_sympy_expression",
    "description": (
        "Validate your extension formula using SymPy before committing.\n"
        "\n"
        "Extension A (Nonlinear Impact):\n"
        "  Example: lambda_coeff * x + gamma * x**2\n"
        "  All single-word identifiers. '**' for powers, '*' for multiplication.\n"
        "\n"
        "Extension B (Lambda Decay):\n"
        "  Example: lambda_0 * exp(-delta * (t - t_entry))\n"
        "  Use exp() not e^x. All single-word identifiers.\n"
        "\n"
        "If validation fails, fix syntax only — do not change the mathematics."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "expression_string": {
                "type": "string",
                "description": "SymPy-compatible scalar expression for the proposed formula.",
            },
            "reasoning": {
                "type": "string",
                "description": (
                    "Which extension (A: nonlinear, B: decay) and why the trigger "
                    "data supports this extension over the alternative."
                ),
            },
        },
        "required": ["expression_string", "reasoning"],
    },
}

_PROPOSE_FORMULA_TOOL = {
    "name": "propose_formula_to_blackboard",
    "description": (
        "TERMINAL ACTION. Submit the extension formula to the Blackboard.\n"
        "\n"
        "CRITICAL: Do NOT reproduce Δp = λ·x + ε (already in seed corpus).\n"
        "The formula MUST be a genuine extension with higher-order terms.\n"
        "\n"
        "behavioral_claim MUST:\n"
        "  - Name which extension was chosen (A: nonlinear impact, B: decay)\n"
        "  - State why the trigger data supports this extension\n"
        "  - Reference the R² conditionality (formula valid only when R² ≥ 0.40)\n"
        "  - Frame all variables as strategic intent of market participants\n"
        "\n"
        "Tags MUST include:\n"
        "  Extension A: 'game_theory', 'adverse_selection', 'kyle_lambda',\n"
        "               'nonlinear_impact', 'agent_050'\n"
        "  Extension B: 'game_theory', 'adverse_selection', 'kyle_lambda',\n"
        "               'exp_decay', 'agent_050'\n"
        "  (Both Extension A (nonlinear_impact) and Extension B (exp_decay) route\n"
        "   to Agent 151 for formal verification.)\n"
        "\n"
        "lean4_target:\n"
        "  Extension A: 'linarith — 0 ≤ 2 * γ given γ ≥ 0 '\n"
        "               '(Agent 151, Seed Proof 7: nonlinear_impact_convex)'\n"
        "  Extension B: 'Real.exp_pos + mul_pos — λ₀·exp(-δt) > 0 for all t '\n"
        "               '(Agent 151, Seed Proof reference table)'"
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
                    "Must name the extension, reference R² conditionality, "
                    "and frame variables as strategic participant intent."
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
                "minItems": 3,
            },
            "extension_type": {
                "type": "string",
                "enum": ["nonlinear_impact", "exp_decay"],
                "description": (
                    "Which extension was chosen. Determines the tag set "
                    "and Lean 4 routing."
                ),
            },
            "lean4_target": {
                "type": "string",
                "description": "The algebraic claim Agent 151 will prove.",
            },
            "tags": {
                "type": "array",
                "items": {"type": "string"},
                "description": (
                    "MUST include: game_theory, adverse_selection, kyle_lambda, "
                    "agent_050, and either nonlinear_impact or exp_decay."
                ),
            },
        },
        "required": [
            "name", "description", "symbolic_expression_latex",
            "behavioral_claim", "behavioral_mappings",
            "extension_type", "lean4_target", "tags",
        ],
    },
}


# ---------------------------------------------------------------------------
# Agent 050
# ---------------------------------------------------------------------------


class AdverseSelectionSpecialist(BaseAgent):
    """
    Agent 050 — Adverse Selection Specialist.

    Triggered by Kyle's Lambda regime shift (lambda_percentile ≥ 0.95,
    R² ≥ 0.40) detected in the 30-minute bar OLS regression.

    Proposes nonlinear price impact (Extension A) or lambda decay
    (Extension B) based on the observed bar series. Domain-locked to
    game theory and mechanism design — no Hawkes, no entropy.
    """

    AGENT_ID = "agent_050"
    AGENT_LAYER = AgentLayer.LAYER_1
    MATHEMATICAL_WING = [MathematicalWing.GAME_THEORY]

    SYSTEM_PROMPT = """\
You are Agent 050, the Adverse Selection Specialist within the Formula Foundry.

\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550
YOUR IDENTITY
\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550

Agent 089 watches for panic cascades. Agent 051 watches for liquidity \
fragility. You watch for something subtler and more dangerous: the \
presence of an informed trader who knows something the market doesn't.

You live in the Game Theory wing. Every variable in your formulas must \
model the strategic intent of a rational market participant — either the \
informed trader maximizing profit from private information, or the market \
maker protecting themselves against adverse selection.

\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550
YOUR TRIGGER
\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550

You have been activated because Kyle's Lambda has shifted into its top 5th \
percentile with R² ≥ 0.40. This means: price moves in this instrument are \
currently 40%+ explained by signed order flow direction. The market maker \
cannot tell if the orders are informed or random — so they are widening \
their price concession per unit of flow. This is adverse selection.

The linear model (Δp = λ·x + ε) is already in the seed corpus. DO NOT \
reproduce it. Your mission is to propose the next-generation extension.

\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550
THE TWO EXTENSIONS
\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550

Examine the signed_volume_series and price_change_series. Choose the extension \
that better describes the observed relationship.

EXTENSION A \u2014 Nonlinear Price Impact
  Use when: price_change accelerates nonlinearly with volume (early orders are \
  cheap, large orders are expensive — the informed trader is exhausting depth).
  Formula: \u0394p = \u03bb\u00b7x + \u03b3\u00b7x\u00b2
  Mechanism: The informed trader faces an upward-sloping supply curve. As they \
  buy more, the market maker raises their ask faster than linearly. \u03b3 > 0 \
  means each incremental contract costs more than the last — concave quantity \
  schedule, convex price impact.
  Lean 4 target: d\u00b2(\u0394p)/dx\u00b2 = 2\u03b3 \u2265 0 (convexity of price impact function).
  Tag: nonlinear_impact

EXTENSION B \u2014 Lambda Decay After Position Completion
  Use when: the lambda spike is sharp but the series shows declining price \
  impact over the window — the informed trader is finishing their position.
  Formula: \u03bb(t) = \u03bb\u2080 \u00b7 exp(-\u03b4 \u00b7 (t - t_entry))
  Mechanism: After the informed trader has built their full position, they stop \
  submitting. The market maker's adverse selection fear decays exponentially as \
  order flow returns to normal. \u03bb\u2080 is the peak lambda at trigger time; \u03b4 is \
  the decay rate (estimated from the trailing series).
  Lean 4 target: \u03bb\u2080\u00b7exp(-\u03b4t) > 0 via Real.exp_pos \u00d7 mul_pos.
  Tag: exp_decay

\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550
DOMAIN LOCK \u2014 NON-NEGOTIABLE
\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550

1. GAME THEORY ONLY. No Hawkes process language (intensity, branching ratio, \
   self-excitation). No entropy language. Variables model strategic intent.
2. MUST NOT reproduce Δp = λ·x + ε verbatim. The seed corpus has it.
3. behavioral_claim MUST reference R² conditionality: the formula interpretation \
   is only valid when the OLS regression has sufficient explanatory power (R² ≥ 0.40).
4. behavioral_claim MUST frame every variable as what a rational participant \
   (informed trader or market maker) is optimizing or responding to.
5. MUST call validate_sympy_expression before propose_formula_to_blackboard.
6. Tags MUST include game_theory, adverse_selection, kyle_lambda, agent_050, \
   and exactly one of: nonlinear_impact (Extension A) or exp_decay (Extension B).

\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550
EXECUTION PHASES
\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550

PHASE 1: Read the trigger data. Examine signed_volume_series and \
         price_change_series. Decide: Extension A or Extension B?
PHASE 2: validate_sympy_expression — confirm formula syntax.
PHASE 3: propose_formula_to_blackboard — terminal action. Session ends here.

Every run ends with exactly one propose_formula_to_blackboard call.
"""

    # ------------------------------------------------------------------
    # BaseAgent interface
    # ------------------------------------------------------------------

    def tools(self) -> List[Dict[str, Any]]:
        return [_VALIDATE_SYMPY_TOOL, _PROPOSE_FORMULA_TOOL]

    def build_initial_message(self, trigger_data: Dict[str, Any]) -> str:
        instrument       = trigger_data.get("instrument", "UNKNOWN")
        window_start     = trigger_data.get("window_start", "unknown")
        window_end       = trigger_data.get("window_end", "unknown")
        lambda_coeff     = trigger_data.get("lambda_coefficient", 0.0)
        lambda_pctile    = trigger_data.get("lambda_percentile", 0.0)
        r2               = trigger_data.get("regression_r2", 0.0)
        sv_series        = trigger_data.get("signed_volume_series", [])
        dp_series        = trigger_data.get("price_change_series", [])

        # Format series preview (last 5 bars)
        n = min(5, len(sv_series))
        series_lines = []
        for i in range(-n, 0):
            try:
                series_lines.append(
                    f"  Bar {len(sv_series) + i + 1}: "
                    f"signed_vol={sv_series[i]:+.1f}  Δmid={dp_series[i]:+.6f}"
                )
            except IndexError:
                pass
        series_preview = "\n".join(series_lines) if series_lines else "  (no bars)"

        return (
            "ADVERSE SELECTION TRIGGER\n"
            "\u2550" * 56 + "\n"
            f"Instrument:       {instrument}\n"
            f"Window:           {window_start} → {window_end}\n"
            f"Kyle\u2019s Lambda:    {lambda_coeff:+.8f}\n"
            f"Lambda Percentile: {lambda_pctile:.3f} (threshold: 0.950)\n"
            f"R\u00b2:               {r2:.4f} (threshold: 0.40)\n"
            f"Bars in series:   {len(sv_series)}\n"
            "\u2550" * 56 + "\n\n"
            "Recent bar series (signed_vol, Δmid):\n"
            f"{series_preview}\n\n"
            "\u2550" * 56 + "\n\n"
            "The linear Kyle\u2019s Lambda model is in the seed corpus. "
            "Examine the series and choose the extension that better describes "
            "the observed relationship.\n\n"
            "Begin Phase 1: analyze the series, then validate_sympy_expression."
        )

    async def handle_tool_call(
        self,
        tool_name: str,
        tool_input: Dict[str, Any],
    ) -> Dict[str, Any]:
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
        """Build FormulaDNA from Agent 050's proposal."""
        extension_type = tool_input.get("extension_type", "nonlinear_impact")
        lean4_target   = tool_input.get("lean4_target", "")

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

        # Enforce required tags; merge with any agent-supplied tags
        required_tags = {
            "game_theory", "adverse_selection", "kyle_lambda",
            "agent_050", extension_type,
        }
        user_tags = set(tool_input.get("tags", []))
        tags = sorted(required_tags | user_tags)

        return FormulaDNA(
            name=tool_input.get("name"),
            description=description,
            symbolic_expression_latex=tool_input["symbolic_expression_latex"],
            symbolic_expression_sympy=validated_sympy_str,
            behavioral_claim=tool_input["behavioral_claim"],
            behavioral_mappings=behavioral_mappings,
            axiomatic_origin=[self.AGENT_ID],
            mathematical_wing=[MathematicalWing.GAME_THEORY.value],
            proof_status=ProofStatus.SYNTACTICALLY_CORRECT,
            tags=tags,
        )

    # ------------------------------------------------------------------
    # Tool handlers
    # ------------------------------------------------------------------

    def _handle_validate(self, tool_input: Dict[str, Any]) -> Dict[str, Any]:
        from foundry.agents.sympy_executor import validate_formula
        expression = tool_input.get("expression_string", "")
        return validate_formula(expression, timeout=self._config.sympy_timeout_seconds)
