"""
Agent 051 — Order Book Entropy Specialist

Axiomatic domain: Probability Theory / Information Theory
Behavioral focus: Shannon entropy of bid-side volume distribution as a
                  leading indicator of liquidity fragility and cascade risk.

ROLE IN THE FOUNDRY:
  Agent 089 watches the intensity of aggressive selling (Hawkes process).
  Agent 051 watches the STRUCTURE of available liquidity (Shannon entropy).
  Both describe market fragility — but from orthogonal mathematical vantage points.

  When entropy collapses AND aggressive selling intensifies simultaneously,
  the two formulas describe the SAME psychological event from different angles.
  Agent 105 will find a Tier 3 behavioral isomorphism between them. The
  synthesis is the Foundry's second crown jewel.

TRIGGER:
  Bid-side Shannon entropy H(bid) below 5th historical percentile
  AND total bid volume below 30th historical percentile.
  (Dual condition prevents false positives from quote redistribution.)

MATHEMATICAL FRAMEWORK:
  H(bid) = -Σᵢ p(level_i) · log₂(p(level_i))
  p(level_i) = volume_at_level_i / total_bid_volume

  Leading indicator property: entropy collapses BEFORE aggressive orders arrive.
  Market makers withdraw from deep levels first (entropy drops), then the
  best bid is hit (Hawkes intensity spikes). The information-theoretic signal
  leads the point-process signal by several seconds.

DOMAIN LOCK — NON-NEGOTIABLE:
  MUST: Express formula in terms of information-theoretic quantities
        (entropy H, KL-divergence, mutual information, probability distributions)
  MUST: Reference the bid-side volume distribution explicitly
  MUST: State the leading-indicator behavioral claim
  NEVER: Use Hawkes processes (Agent 089's domain)
  NEVER: Use game theory or strategic intent (Agent 050's domain)
  NEVER: Produce a formula without a clearly defined probability distribution

SYMPY VALIDATION TARGET:
  The 2-level concentration approximation:
    p_best = volume at best bid / total bid volume
    H₂ = -(p_best * log(p_best) + (1-p_best) * log(1-p_best))
    Fragility index: F = 1 - H₂ / log(2)   (normalized to [0, 1])

  Full N-level entropy requires a symbolic sum and cannot be validated as a
  single SymPy scalar. Validate the 2-level approximation — it is the
  economically meaningful component (best bid vs. everything else).

LEAN 4 TARGET:
  Entropy term non-negativity: -p·log(p) ≥ 0 for 0 ≤ p ≤ 1.
  This proves the foundational property that Shannon entropy is well-defined
  (non-negative), which is the mathematical prerequisite for using H(bid)
  as a fragility measure.

  Seed proof structure:
    theorem entropy_term_nonneg (p : ℝ) (hp0 : 0 ≤ p) (hp1 : p ≤ 1) :
        0 ≤ -(p * Real.log p) := by
      rcases hp0.eq_or_gt with rfl | hp
      · simp
      have hlog : Real.log p ≤ 0 := Real.log_nonpos hp.le hp1
      linarith [mul_nonpos_of_nonneg_of_nonpos hp.le hlog]
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
        "Validate a mathematical expression string using SymPy.\n"
        "\n"
        "For Agent 051, validate the 2-LEVEL CONCENTRATION APPROXIMATION:\n"
        "  p_best  = volume at best bid / total bid volume\n"
        "  H_2     = -(p_best * log(p_best) + (1 - p_best) * log(1 - p_best))\n"
        "  Fragility index:\n"
        "    F = 1 - H_2 / log(2)\n"
        "\n"
        "The full N-level entropy H = -Σ p_i · log(p_i) cannot be expressed\n"
        "as a single SymPy scalar (requires a sum over symbolic N).\n"
        "The 2-level approximation is the economically meaningful projection:\n"
        "it separates best-bid liquidity from all other liquidity.\n"
        "\n"
        "Variable naming rules:\n"
        "  p_best — fraction of total bid volume at the best bid price level\n"
        "  H_2    — binary entropy of the best-bid concentration\n"
        "  F      — fragility index in [0, 1] (0=maximum entropy, 1=fully concentrated)\n"
        "  All single-word identifiers. Use log() for natural log or log(x, 2) for log₂.\n"
        "\n"
        "Example valid expression for F:\n"
        "  1 - (-(p_best * log(p_best) + (1 - p_best) * log(1 - p_best))) / log(2)\n"
        "\n"
        "If validation fails: correct syntax only. Do not change the mathematics."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "expression_string": {
                "type": "string",
                "description": (
                    "SymPy expression for the entropy fragility measure. "
                    "E.g. '1 - (-(p_best * log(p_best) + (1 - p_best) * log(1 - p_best))) / log(2)'"
                ),
            },
            "reasoning": {
                "type": "string",
                "description": (
                    "What this expression represents and why it captures "
                    "the behavioral claim about order book fragility."
                ),
            },
        },
        "required": ["expression_string", "reasoning"],
    },
}

_PROPOSE_FORMULA_TOOL = {
    "name": "propose_formula_to_blackboard",
    "description": (
        "TERMINAL ACTION. Submit the entropy fragility formula to the Blackboard.\n"
        "\n"
        "Prerequisites (enforced):\n"
        "  (1) validate_sympy_expression returned valid=True.\n"
        "  (2) behavioral_claim explicitly references Shannon entropy and the\n"
        "      leading-indicator property (entropy collapse BEFORE cascade).\n"
        "  (3) behavioral_mappings include entries for p_best, H_2, F, and\n"
        "      at least one temporal variable (the timestamp of collapse).\n"
        "\n"
        "The behavioral_claim MUST state:\n"
        "  - What probability distribution is being measured (bid volumes)\n"
        "  - The leading-indicator relationship to price cascades\n"
        "  - The threshold below which the formula predicts fragility\n"
        "\n"
        "This is a terminal action — your session ends here."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "name": {
                "type": "string",
                "description": "E.g. 'Bid-Side Entropy Fragility Index v1'",
            },
            "description": {"type": "string"},
            "symbolic_expression_latex": {
                "type": "string",
                "description": (
                    "Full formula in LaTeX. Must show entropy H(bid) and the "
                    "fragility index F. "
                    r"E.g. r'F = 1 - \frac{H_2(p_{\text{best}})}{log 2}' "
                    r"where H_2(p) = -p \log p - (1-p) \log(1-p)"
                ),
            },
            "behavioral_claim": {
                "type": "string",
                "description": (
                    "Must reference: (1) Shannon entropy of bid-side distribution, "
                    "(2) the leading-indicator property — entropy collapses BEFORE "
                    "aggressive order arrival, (3) the fragility threshold, "
                    "(4) the market microstructure mechanism (market maker withdrawal). "
                    "Min 80 characters."
                ),
                "minLength": 80,
            },
            "behavioral_mappings": {
                "type": "array",
                "description": (
                    "MUST include: p_best (best-bid concentration), H_2 (binary entropy), "
                    "F (fragility index), and the trigger context variables "
                    "(entropy_percentile, volume_percentile). "
                    "The leading-indicator relationship must be captured in at least "
                    "one mapping's psychological_state field."
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
                "minItems": 4,
            },
            "lean4_target": {
                "type": "string",
                "description": (
                    "State the algebraic claim Agent 151 should prove. "
                    "Recommended: entropy term non-negativity: "
                    "'For 0 ≤ p ≤ 1: -(p·log p) ≥ 0'. "
                    "This proves H(bid) is well-defined as a fragility measure."
                ),
            },
            "tags": {
                "type": "array",
                "items": {"type": "string"},
                "description": (
                    "E.g. ['entropy', 'order_book', 'information_theory', "
                    "'fragility', 'leading_indicator', 'agent_051']"
                ),
            },
        },
        "required": [
            "name", "description", "symbolic_expression_latex",
            "behavioral_claim", "behavioral_mappings", "lean4_target",
        ],
    },
}


# ---------------------------------------------------------------------------
# Agent 051
# ---------------------------------------------------------------------------


class OrderBookEntropySpecialist(BaseAgent):
    """
    Agent 051 — Order Book Entropy Specialist.
    Triggered by bid-side Shannon entropy collapse below 5th historical percentile
    AND total bid volume below 30th percentile (dual-condition guard).
    Produces entropy fragility formulas anchored to MBP-10 book structure.
    """

    AGENT_ID = "agent_051"
    AGENT_LAYER = AgentLayer.LAYER_1
    MATHEMATICAL_WING = [MathematicalWing.PROBABILITY_INFORMATION]

    SYSTEM_PROMPT = """\
You are Agent 051, the Order Book Entropy Specialist within the Formula Foundry.

═══════════════════════════════════════════════════════
YOUR IDENTITY
═══════════════════════════════════════════════════════

Agent 089 measures HOW FAST liquidity is being removed (Hawkes intensity).
Agent 060 measures CORRELATION of removal across instruments (cross-asset).
You measure HOW MUCH STRUCTURE remains in the liquidity that hasn't yet been
removed — the Shannon entropy of the bid-side volume distribution.

These are three orthogonal views of the same physical event: market fragility.
Your formula, combined with Agent 089's, will produce the Foundry's second
crown jewel synthesis — a Tier 3 behavioral isomorphism connecting information
theory and point process theory through the concept of pre-crisis structural
collapse.

═══════════════════════════════════════════════════════
THE PHYSICAL INTUITION
═══════════════════════════════════════════════════════

A healthy limit order book looks like this across 10 bid levels:
  Level 1 (best bid): 500 contracts
  Level 2:            350 contracts
  Level 3:            280 contracts
  ...
  Level 10:           180 contracts
  Shannon entropy H ≈ 3.1 bits (close to maximum log₂(10) ≈ 3.32 bits)

Minutes before a limit-down event, the book looks like this:
  Level 1 (best bid): 180 contracts   ← everyone else has left
  Level 2:            20 contracts
  Level 3:            5 contracts
  Levels 4-10:        0 contracts
  Shannon entropy H ≈ 0.6 bits  ← collapsed

The market makers have withdrawn their deep quotes. The book LOOKS like it
has support, but one institutional sell order will cascade through the thin
levels. The entropy collapse PRECEDES the aggressive order flow by seconds.

This is the leading-indicator property that makes Agent 051's formula
more valuable than a lagging price-impact model.

═══════════════════════════════════════════════════════
THE MATHEMATICAL STRUCTURE YOU PRODUCE
═══════════════════════════════════════════════════════

The full Shannon entropy of the bid side:
  H(bid) = -Σᵢ₌₁ᴺ p(level_i) · log₂(p(level_i))
  p(level_i) = volume_at_level_i / total_bid_volume
  N ≤ 10 (top 10 levels from MBP-10 schema)

The 2-level concentration approximation (SymPy-validatable):
  p_best = volume_at_level_1 / total_bid_volume
  H₂ = -(p_best · log(p_best) + (1 - p_best) · log(1 - p_best))
  F = 1 - H₂ / log(2)   ← Fragility index ∈ [0, 1]

  F = 0: best bid has equal volume to all other levels (maximum diversity)
  F = 1: all volume concentrated at best bid (or book completely empty)

The behavioral claim:
  "F > 0.8 (entropy below 20% of binary maximum) signals pre-crisis
   concentration where market maker depth has collapsed. The subsequent
   probability of a 3-sigma OFI event (Agent 089's domain) within 10 seconds
   is historically elevated."

═══════════════════════════════════════════════════════
LEAN 4 TARGET
═══════════════════════════════════════════════════════

The algebraic property Agent 151 will prove:

  theorem entropy_term_nonneg (p : ℝ) (hp0 : 0 ≤ p) (hp1 : p ≤ 1) :
      0 ≤ -(p * Real.log p)

This proves that each term -p·log(p) in the entropy sum is non-negative,
which guarantees H(bid) ≥ 0 and makes it mathematically valid as a
measure of book fragility.

Key Mathlib4 lemmas Agent 151 will use:
  Real.log_nonpos: 0 ≤ x → x ≤ 1 → Real.log x ≤ 0
  mul_nonpos_of_nonneg_of_nonpos: 0 ≤ p → log p ≤ 0 → p * log p ≤ 0

This is the foundational verification step. The synthesis with Agent 089
(proving the behavioral connection between H collapse and Hawkes criticality)
is the isomorphism target — that proof is harder and belongs to Agent 153
(the future probability theory auditor) rather than Agent 151.

═══════════════════════════════════════════════════════
DOMAIN LOCK — HARD CONSTRAINTS
═══════════════════════════════════════════════════════

1. ALWAYS express the formula in terms of probability distributions.
   Variables must be proportions (p_i ∈ [0,1] summing to 1).

2. ALWAYS include the leading-indicator behavioral claim.
   "Entropy collapses BEFORE the cascade arrives" is the core IP.
   A formula that merely measures entropy without stating this property
   is incomplete and has no synthesis value.

3. NEVER use Hawkes processes. The intensity λ(t) belongs to Agent 089.
   You may reference "the subsequent Hawkes event" in the behavioral_claim
   but not in the formula itself.

4. NEVER use game theory or strategic intent (Agent 050's domain).
   Your formula describes STRUCTURAL information content of the book —
   not the rational actions of strategic agents.

5. NEVER produce a formula where the probability distribution is undefined.
   Guard: if total_bid_volume = 0, F = 1 by convention (maximum fragility).
   State this edge case in the behavioral_mappings.

═══════════════════════════════════════════════════════
MANDATORY WORKFLOW
═══════════════════════════════════════════════════════

STEP 1: Read the trigger data. Note:
  - bid_entropy_bits: the current entropy value
  - entropy_percentile: how far below historical this is (e.g., 2.3 = 2.3rd pctile)
  - best_bid_fraction: p_best (what fraction is concentrated at best bid)
  - n_nonzero_levels: how many levels have ANY liquidity
  - bid_levels: the actual level-by-level snapshot

STEP 2: Draft the formula. Always produce:
  - The conceptual N-level formula in LaTeX: H(bid) = -Σ p_i log p_i
  - The 2-level SymPy-validatable approximation: F = 1 - H₂/log(2)
  - The fragility threshold: "F > 0.8 signals pre-crisis concentration"

STEP 3: Validate the 2-level approximation with validate_sympy_expression.
  Use `p_best` as the variable. If validation fails, fix syntax only.

STEP 4: Call propose_formula_to_blackboard with the complete formula.
  Include lean4_target pointing to the entropy term non-negativity claim.

STEP 5: Your session ends. Agent 151 proves the Lean 4 target.
        Agent 105 will find the isomorphism with Agent 089's Hawkes formula.
"""

    # ------------------------------------------------------------------
    # State
    # ------------------------------------------------------------------

    def __init__(self, config: AgentConfig) -> None:
        super().__init__(config)
        self._sympy_validated: bool = False
        self._validated_expression: str = ""

    # ------------------------------------------------------------------
    # BaseAgent interface
    # ------------------------------------------------------------------

    def tools(self) -> List[Dict[str, Any]]:
        return [_VALIDATE_SYMPY_TOOL, _PROPOSE_FORMULA_TOOL]

    def build_initial_message(self, trigger_data: Dict[str, Any]) -> str:
        instrument       = trigger_data.get("instrument", "UNKNOWN")
        timestamp        = trigger_data.get("timestamp", "unknown")
        entropy_bits     = trigger_data.get("bid_entropy_bits", 0.0)
        entropy_pctile   = trigger_data.get("entropy_percentile", 0.0)
        total_vol        = trigger_data.get("total_bid_volume", 0)
        vol_pctile       = trigger_data.get("volume_percentile", 0.0)
        best_frac        = trigger_data.get("best_bid_fraction", 0.0)
        n_levels         = trigger_data.get("n_nonzero_levels", 0)
        bid_levels       = trigger_data.get("bid_levels", [])

        # Format the book snapshot
        max_display = 10
        book_lines = []
        for i, lvl in enumerate(bid_levels[:max_display]):
            size = lvl.get("size", 0) if isinstance(lvl, dict) else lvl.size
            price = lvl.get("price", 0.0) if isinstance(lvl, dict) else lvl.price
            vol_frac = size / total_vol if total_vol > 0 else 0.0
            bar = "█" * int(vol_frac * 20)
            book_lines.append(
                f"  Level {i+1:2d}: ${price:>10.2f}  {size:>8d} contracts  {bar}"
            )
        book_display = "\n".join(book_lines) if book_lines else "  (no bid levels)"

        max_entropy = 3.321  # log2(10)
        entropy_fraction = entropy_bits / max_entropy if max_entropy > 0 else 0.0
        fragility_estimate = 1.0 - entropy_fraction

        return f"""\
ENTROPY COLLAPSE TRIGGER
════════════════════════════════════════════════════════
Instrument:  {instrument}
Timestamp:   {timestamp}
════════════════════════════════════════════════════════
Bid Entropy (H):      {entropy_bits:.4f} bits
  Historical pctile:  {entropy_pctile:.1f}th (below {100-entropy_pctile:.0f}% of history)
  As fraction of max: {entropy_fraction:.1%} of log₂(10) = {max_entropy:.3f} bits

Total Bid Volume:     {total_vol:,} contracts
  Historical pctile:  {vol_pctile:.1f}th

Best Bid Fraction:    {best_frac:.3f}  ({best_frac*100:.1f}% of all bid volume at level 1)
Non-zero Levels:      {n_levels}/10
Estimated Fragility:  F ≈ {fragility_estimate:.3f}
════════════════════════════════════════════════════════
BID BOOK SNAPSHOT (level → volume distribution):

{book_display}
════════════════════════════════════════════════════════

PHYSICAL INTERPRETATION:
The bid entropy is at the {entropy_pctile:.1f}th historical percentile.
{best_frac*100:.1f}% of remaining bid volume is concentrated at the best bid.
Only {n_levels} of 10 possible levels have any volume.

This is pre-crisis book structure. Market makers have withdrawn from
deep levels. The entropy collapse precedes aggressive order flow.

Your task: Formally express the information-theoretic fragility of this
order book state as a Shannon entropy formula, with an explicit
behavioral claim about the leading-indicator property.

Begin with validate_sympy_expression, then propose_formula_to_blackboard.\
"""

    async def handle_tool_call(
        self,
        tool_name: str,
        tool_input: Dict[str, Any],
    ) -> Dict[str, Any]:
        if tool_name == "validate_sympy_expression":
            return self._handle_validate(tool_input)
        if tool_name == "propose_formula_to_blackboard":
            return {"accepted": True}   # handled by base via extract_formula
        return {"error": f"Unknown tool: {tool_name}"}

    def _handle_validate(self, tool_input: Dict[str, Any]) -> Dict[str, Any]:
        expression = tool_input.get("expression_string", "")
        logger.info("[%s] Validating: %s", self.AGENT_ID, expression[:120])
        result = validate_formula(expression, timeout=self._config.sympy_timeout_seconds)

        if result.get("valid"):
            self._sympy_validated = True
            self._validated_expression = result.get("sympy_str", expression)
            symbols = result.get("free_symbols", [])
            logger.info("[%s] SymPy PASSED. Free symbols: %s", self.AGENT_ID, symbols)

            has_log   = any("log" in s.lower() for s in symbols) or "log" in expression
            has_p     = any("p_best" in s or "p" == s for s in symbols)

            feedback = []
            if not has_p:
                feedback.append(
                    "⚠ No probability variable detected. "
                    "Ensure p_best appears in the expression."
                )
            if not has_log:
                feedback.append(
                    "⚠ No log() detected. Entropy requires a logarithm."
                )
            if not feedback:
                feedback.append(
                    "✓ Expression contains probability variable and logarithm. "
                    "Structure looks correct for an entropy formula."
                )

            return {
                "valid": True,
                "sympy_str": result["sympy_str"],
                "free_symbols": symbols,
                "message": (
                    " ".join(feedback) + " "
                    "You may now call propose_formula_to_blackboard."
                ),
            }
        else:
            logger.info("[%s] SymPy FAILED: %s", self.AGENT_ID, result.get("error"))
            return {
                "valid": False,
                "error": result.get("error"),
                "message": (
                    "Validation failed. Correct syntax only — do not change the math. "
                    "Common fixes: use log(x) not ln(x), "
                    "use * for multiplication, "
                    "ensure p_best is a single identifier without subscripts."
                ),
            }

    def extract_formula(
        self,
        tool_input: Dict[str, Any],
        validated_sympy_str: str,
    ) -> FormulaDNA:
        """Build FormulaDNA from Agent 051's proposal."""
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

        lean4_note = tool_input.get("lean4_target", "")
        description = tool_input.get("description", "")
        if lean4_note and "lean4" not in description.lower():
            description = f"{description}\n\nLean 4 target: {lean4_note}"

        base_tags = [
            "agent_051",
            "entropy",
            "order_book",
            "information_theory",
            "fragility",
            "leading_indicator",
            "mbp10",
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
            mathematical_wing=[MathematicalWing.PROBABILITY_INFORMATION.value],
            proof_status=ProofStatus.SYNTACTICALLY_CORRECT,
            tags=base_tags + user_tags,
        )
