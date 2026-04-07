"""
Agent 089 — Hawkes Process Specialist

Axiomatic domain: Probability Theory + Functional Analysis
Behavioral focus: Self-exciting point processes, panic cascade dynamics.

Authority: Layer 1 (Specialist) — can PROPOSE (write SYNTACTICALLY_CORRECT via SymPy gate)
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List

from foundry.agents.base import AgentConfig, BaseAgent
from foundry.agents.sympy_executor import validate_formula
from foundry.core.schema.formula import (
    AgentLayer, BehavioralMapping, FormulaDNA, MathematicalWing, ProofStatus,
)

logger = logging.getLogger(__name__)

_VALIDATE_SYMPY_TOOL = {
    "name": "validate_sympy_expression",
    "description": (
        "Validates a mathematical expression string using SymPy. "
        "Use this to confirm your proposed LaTeX formula translates into a "
        "syntactically valid algebraic object before submitting to the Blackboard. "
        "Variables must be single words without spaces (e.g., 'lambda_t', 'mu', 'alpha'). "
        "Use 'exp(x)' not 'e^x'. Use '*' for multiplication. Use '**' for powers. "
        "If validation fails, read the error carefully and correct your syntax."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "expression_string": {
                "type": "string",
                "description": "The formula in SymPy-compatible string format.",
            },
            "reasoning": {
                "type": "string",
                "description": "Brief explanation of what this expression represents.",
            },
        },
        "required": ["expression_string", "reasoning"],
    },
}

_PROPOSE_FORMULA_TOOL = {
    "name": "propose_formula_to_blackboard",
    "description": (
        "Submit your validated formula to the Blackboard as a formal HYPOTHESIS. "
        "ONLY call this after validate_sympy_expression returns valid=True. "
        "This is a terminal action — it ends your reasoning session."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "description": {"type": "string"},
            "symbolic_expression_latex": {"type": "string"},
            "behavioral_claim": {"type": "string"},
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
                    "required": ["variable", "latex_symbol", "psychological_state", "measurement_proxy"],
                },
                "minItems": 1,
            },
            "tags": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["name", "description", "symbolic_expression_latex",
                     "behavioral_claim", "behavioral_mappings"],
    },
}


class HawkesSpecialist(BaseAgent):
    """Agent 089 — Hawkes Process Specialist. First responder to panic trigger."""

    AGENT_ID = "agent_089"
    AGENT_LAYER = AgentLayer.LAYER_1
    MATHEMATICAL_WING = [MathematicalWing.PROBABILITY_INFORMATION, MathematicalWing.FUNCTIONAL_ANALYSIS]

    SYSTEM_PROMPT = """\
You are Agent 089, the Hawkes Process Specialist within the Formula Foundry — \
a high-frequency behavioral finance think tank whose purpose is to generate \
formally verifiable formulas that model the mathematical structure of human \
emotion and cognition as expressed in market microstructure data.

═══════════════════════════════════════════════════════
AXIOMATIC DOMAIN: PROBABILITY THEORY + FUNCTIONAL ANALYSIS
═══════════════════════════════════════════════════════

You perceive the market exclusively through the mathematics of self-exciting \
point processes. Every market event you observe is a realization of an \
inhomogeneous Poisson process whose intensity is itself driven by past events.

Your vocabulary:
- Conditional intensity λ(t | H_t): the instantaneous rate of events given history H_t
- Baseline intensity μ: the background rate absent excitation
- Excitation kernel φ(t): how each past event elevates future intensity
- Branching ratio n = ∫φ(t)dt: whether the process is subcritical (n<1) or supercritical (n≥1)
- Decay function: typically exponential e^(-βΔt)

═══════════════════════════════════════════════════════
DOMAIN LOCK — HARD CONSTRAINTS
═══════════════════════════════════════════════════════

1. You ONLY use mathematics from: stochastic point processes, intensity modeling,
   branching processes, functional analysis, survival analysis.

2. You DO NOT use: game theory, topological constructs, algebraic geometry,
   information entropy (Agent 085's domain), or regime detection (Agent 050's domain).

3. Every variable you introduce MUST have a direct behavioral interpretation.

═══════════════════════════════════════════════════════
THE CoMT PIPELINE — YOUR MANDATORY WORKFLOW
═══════════════════════════════════════════════════════

  Step 1: Reason about the trigger data through a Hawkes lens.
          Write your candidate in LaTeX first.

  Step 2: Translate your LaTeX into SymPy string format and call
          validate_sympy_expression. If it fails, correct and retry.
          You have up to 6 validation attempts.

  Step 3: Once validate_sympy_expression returns valid=True, call
          propose_formula_to_blackboard with the full FormulaDNA fields.
          This is a terminal action.

CRITICAL: The behavioral_mappings field is not optional bureaucracy.
It is the intellectual property. Every variable must map to a psychological state.
"""

    def tools(self) -> List[Dict[str, Any]]:
        return [_VALIDATE_SYMPY_TOOL, _PROPOSE_FORMULA_TOOL]

    def build_initial_message(self, trigger_data: Dict[str, Any]) -> str:
        ofi_zscore = trigger_data.get("ofi_zscore", "N/A")
        spread_pctile = trigger_data.get("spread_percentile", "N/A")
        instrument = trigger_data.get("instrument", "unknown")
        timestamp = trigger_data.get("timestamp", "unknown")
        event_count = trigger_data.get("mbo_event_count", "N/A")
        ofi_acceleration = trigger_data.get("ofi_acceleration", "N/A")
        baseline_intensity = trigger_data.get("baseline_sell_rate_hz", "N/A")
        current_intensity = trigger_data.get("current_sell_rate_hz", "N/A")

        return f"""\
PANIC FINGERPRINT TRIGGER RECEIVED
════════════════════════════════════════════════════════
Instrument:           {instrument}
Timestamp:            {timestamp}
────────────────────────────────────────────────────────
OFI Z-Score:          {ofi_zscore:.3f}σ  [TRIGGER: >3.0σ]
Spread Percentile:    {spread_pctile:.1f}th  [TRIGGER: >95th]
OFI Acceleration:     {ofi_acceleration}
────────────────────────────────────────────────────────
Baseline sell rate:   {baseline_intensity} events/sec  (30-day μ)
Current sell rate:    {current_intensity} events/sec
MBO Events in window: {event_count}
════════════════════════════════════════════════════════

Propose a Hawkes process intensity function λ(t) that formally models \
the ACCELERATION and SELF-SUSTAINING nature of this panic cascade.

Begin your analysis. Remember your CoMT pipeline: reason → validate → propose.\
"""

    async def handle_tool_call(self, tool_name: str, tool_input: Dict[str, Any]) -> Dict[str, Any]:
        if tool_name == "validate_sympy_expression":
            return self._handle_validate(tool_input)
        if tool_name == "propose_formula_to_blackboard":
            return {"accepted": True}
        return {"error": f"Unknown tool: {tool_name}"}

    def _handle_validate(self, tool_input: Dict[str, Any]) -> Dict[str, Any]:
        expression = tool_input.get("expression_string", "")
        logger.info("[%s] Validating expression: %s", self.AGENT_ID, expression[:80])
        result = validate_formula(expression, timeout=self._config.sympy_timeout_seconds)

        if result.get("valid"):
            logger.info("[%s] SymPy PASSED. Free symbols: %s",
                        self.AGENT_ID, result.get("free_symbols"))
            return {
                "valid": True,
                "sympy_str": result["sympy_str"],
                "latex_roundtrip": result.get("latex_roundtrip"),
                "free_symbols": result.get("free_symbols", []),
                "message": (
                    "Expression is algebraically valid. "
                    "Ensure every symbol has a behavioral mapping. "
                    "You may now call propose_formula_to_blackboard."
                ),
            }
        else:
            logger.info("[%s] SymPy FAILED: %s", self.AGENT_ID, result.get("error"))
            return {
                "valid": False,
                "error": result.get("error"),
                "message": (
                    "Expression failed algebraic validation. "
                    "Correct the syntax and retry. Common issues: "
                    "use exp() not e^x, use ** not ^, use * for multiplication."
                ),
            }

    def extract_formula(self, tool_input: Dict[str, Any], validated_sympy_str: str) -> FormulaDNA:
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
        return FormulaDNA(
            name=tool_input.get("name"),
            description=tool_input.get("description"),
            symbolic_expression_latex=tool_input["symbolic_expression_latex"],
            symbolic_expression_sympy=validated_sympy_str,
            behavioral_claim=tool_input["behavioral_claim"],
            behavioral_mappings=behavioral_mappings,
            axiomatic_origin=[self.AGENT_ID],
            mathematical_wing=[w.value for w in self.MATHEMATICAL_WING],
            proof_status=ProofStatus.SYNTACTICALLY_CORRECT,
            tags=tool_input.get("tags", []) + ["agent_089", "hawkes", "panic_liquidity"],
        )
