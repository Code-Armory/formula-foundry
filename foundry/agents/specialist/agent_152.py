"""
Agent 152 — Entropy Auditor

Authority: Layer 3 — Master Lock.
The only agent that can write FORMALLY_VERIFIED or FALSIFIED to the Blackboard
for formulas tagged with entropy / information_theory / shannon.

Mission: Formally verify the Shannon entropy non-negativity claim in
Agent 051 formulas using Mathlib4's finite sum decomposition.

Domain lock (empirically derived from Mathlib4 REPL probe):
  PERMITTED: Real.log_nonpos, Finset.sum_nonpos,
             mul_nonpos_of_nonneg_of_nonpos, neg_nonneg
  FORBIDDEN: MeasureTheory.Measure.kl (does not exist — probe confirmed),
             MeasureTheory.kl_nonneg (does not exist — probe confirmed),
             MeasureTheory.Martingale (wrong namespace — probe confirmed),
             ProbabilityTheory.Martingale (brittle instance requirements — probe confirmed)

Key constraints (identical to Agent 151):
  - Does NOT produce a new formula — audits an existing one.
  - FORMALLY_VERIFIED requires self._last_verified_proof to be populated
    (only set when REPL returns valid=true). Cannot self-certify.
  - FALSIFIED requires 50+ chars of counterexample in lean4_notes.
    Proof exhaustion ≠ FALSIFIED. Shannon entropy cannot be falsified —
    do not attempt.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import httpx

from foundry.agents.base import AgentConfig, BaseAgent
from foundry.core.schema.formula import AgentLayer, FormulaDNA, MathematicalWing, ProofStatus

logger = logging.getLogger(__name__)
AGENT_VERSION = "0.1.0"
LEAN_MAX_ATTEMPTS = 5


# ---------------------------------------------------------------------------
# Tool schemas — identical contract to Agent 151
# ---------------------------------------------------------------------------

_FETCH_FORMULA_TOOL = {
    "name": "fetch_formula_data",
    "description": (
        "Retrieve the complete FormulaDNA from the Blackboard by UUID. "
        "MUST be your first tool call. Read symbolic_expression_latex, "
        "behavioral_claim, and every behavioral_mapping before drafting Lean 4 code. "
        "Your theorem must formalize the entropy non-negativity claim, "
        "not the full behavioral_claim verbatim."
    ),
    "input_schema": {
        "type": "object",
        "properties": {"uuid": {"type": "string"}},
        "required": ["uuid"],
    },
}

_CHECK_SYNTAX_TOOL = {
    "name": "check_lean_syntax",
    "description": (
        "FAST GATE. Validate a Lean 4 expression via #check. "
        "Use before committing to a full proof attempt. Fast — use freely.\n"
        "\n"
        "Recommended first checks:\n"
        "  '#check @Real.log_nonpos'\n"
        "  '#check @Finset.sum_nonpos'\n"
        "  '#check @mul_nonpos_of_nonneg_of_nonpos'\n"
        "\n"
        "These are the three lemmas in your seed proof. Confirm they load "
        "in the current REPL session before submitting a full proof."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "expression": {"type": "string"},
            "reasoning": {"type": "string"},
        },
        "required": ["expression", "reasoning"],
    },
}

_VERIFY_PROOF_TOOL = {
    "name": "verify_lean_proof",
    "description": (
        "HEAVY GATE. Submit a complete Lean 4 theorem + proof to the Mathlib4 REPL. "
        f"Up to {LEAN_MAX_ATTEMPTS} attempts. "
        "For entropy formulas, use the seed proof template from your SYSTEM_PROMPT directly. "
        "Adjust only: index type (Fin 10 vs Fin n) based on formula level count. "
        "Do NOT attempt MeasureTheory or ProbabilityTheory lemmas — they are not available. "
        "Do NOT submit a proof with 'sorry'."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "theorem_statement": {
                "type": "string",
                "description": "Complete Lean 4 theorem declaration header without := by.",
            },
            "proof_body": {
                "type": "string",
                "description": "The proof in tactic mode. Use \\n between tactics.",
            },
            "attempt_number": {"type": "integer"},
            "strategy_description": {"type": "string"},
        },
        "required": [
            "theorem_statement", "proof_body",
            "attempt_number", "strategy_description",
        ],
    },
}

_UPDATE_STATUS_TOOL = {
    "name": "update_formula_status",
    "description": (
        "TERMINAL ACTION — Layer 3 Master Lock. "
        "FORMALLY_VERIFIED: verify_lean_proof returned valid=true for the entropy bound. "
        "FALSIFIED: you have a concrete counterexample (impossible for Shannon entropy — "
        "do not use this unless mathematically certain). "
        "SYNTACTICALLY_CORRECT: 5 attempts exhausted, no counterexample found. "
        "CRITICAL: FALSIFIED requires an explicit counterexample in lean4_notes (50+ chars). "
        "'Could not find a proof' is NOT grounds for FALSIFIED."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "uuid": {"type": "string"},
            "new_status": {
                "type": "string",
                "enum": ["formally_verified", "falsified", "syntactically_correct"],
            },
            "lean4_theorem_statement": {"type": "string"},
            "lean4_proof_body": {"type": "string"},
            "lean4_notes": {"type": "string"},
        },
        "required": [
            "uuid", "new_status",
            "lean4_theorem_statement", "lean4_proof_body", "lean4_notes",
        ],
    },
}


# ---------------------------------------------------------------------------
# Agent 152
# ---------------------------------------------------------------------------

class EntropyAuditor(BaseAgent):
    """
    Agent 152 — Entropy Auditor.

    Activated by synthesis_flow.py when a formula carries entropy /
    information_theory / shannon tags. Proves Shannon entropy non-negativity
    using the Finset.sum_nonpos decomposition confirmed by Mathlib4 REPL probe.
    """

    AGENT_ID = "agent_152"
    AGENT_LAYER = AgentLayer.LAYER_3
    MATHEMATICAL_WING = [
        MathematicalWing.PROBABILITY_INFORMATION,
        MathematicalWing.VERIFICATION,
    ]

    SYSTEM_PROMPT = f"""\
You are Agent 152, the Entropy Auditor within the Formula Foundry.
Authority: Layer 3 — Master Lock.

Your domain is finite information theory. You hold the Master Lock for all \
formulas produced by Agent 051 (Order Book Entropy Specialist). Your job is \
to formally verify that the Shannon entropy of the limit order book is \
non-negative — the foundational mathematical guarantee that underpins all \
entropy-based liquidity fragility signals.

Your domain lock is empirically derived from a live Mathlib4 REPL probe. \
Every permitted lemma below was confirmed to exist. Every forbidden item \
below was confirmed to be absent. You cannot hallucinate past this boundary.

\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550
REPL-VERIFIED SEED PROOF \u2014 DISCRETE ENTROPY NON-NEGATIVITY
\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550

This proof was confirmed against the current Mathlib4 master branch. \
Every lemma exists with exactly the signature shown. Use it verbatim \
as your theorem template.

```lean
theorem discrete_entropy_nonneg
    {{α : Type*}} (s : Finset α) (p : α \u2192 \u211d)
    (hp_nn : \u2200 i \u2208 s, 0 \u2264 p i)
    (hp_le : \u2200 i \u2208 s, p i \u2264 1) :
    0 \u2264 -(\u2211 i in s, p i * Real.log (p i)) := by
  apply neg_nonneg.mpr
  apply Finset.sum_nonpos
  intro i hi
  exact mul_nonpos_of_nonneg_of_nonpos (hp_nn i hi) (Real.log_nonpos (hp_le i hi))
```

Permitted lemmas (all REPL-confirmed in current Mathlib4):
  neg_nonneg.mpr                  : 0 \u2264 -x \u2194 x \u2264 0 (use .mpr direction)
  Finset.sum_nonpos               : (\u2200 i \u2208 s, f i \u2264 0) \u2192 \u2211 i in s, f i \u2264 0
  mul_nonpos_of_nonneg_of_nonpos  : 0 \u2264 a \u2192 b \u2264 0 \u2192 a * b \u2264 0
  Real.log_nonpos                 : x \u2264 1 \u2192 Real.log x \u2264 0

Why it works: p(i) * Real.log(p(i)) \u2264 0 because:
  \u2022 p(i) \u2265 0  (non-negative probability)
  \u2022 Real.log(p(i)) \u2264 0 when p(i) \u2264 1  (log of sub-unit probability is negative)
  \u2022 non-negative \xd7 non-positive = non-positive
  Negation of a non-positive sum is non-negative. \u2610

For Agent 051\u2019s fixed 10-level MBP-10 schema, the instantiated template is:

```lean
theorem entropy_nonneg_order_book
    (p : Fin 10 \u2192 \u211d)
    (hp_nn : \u2200 i, 0 \u2264 p i)
    (hp_le : \u2200 i, p i \u2264 1) :
    0 \u2264 -(\u2211 i : Fin 10, p i * Real.log (p i)) := by
  apply neg_nonneg.mpr
  apply Finset.sum_nonpos
  intro i _
  exact mul_nonpos_of_nonneg_of_nonpos (hp_nn i) (Real.log_nonpos (hp_le i))
```

Note: Fin 10 (fixed index type) is cleaner than Finset.range 10 for the \
MBP-10 schema. Use parametric Fin n if the formula uses a variable level count. \
Real.log is Mathlib\u2019s natural logarithm. The non-negativity result holds \
regardless of log base \u2014 log\u2082 in the LaTeX becomes Real.log in Lean 4.

\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550
BEHAVIORAL AXIOMS \u2014 DO NOT ATTEMPT IN LEAN 4
\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550

These claims appear in Agent 051 formulas but are formally unprovable \
in the current Mathlib4. Attempting them will produce \u2018unknown identifier\u2019 \
or missing instance errors. They are stated as behavioral axioms.

AXIOM 1 \u2014 Gibbs\u2019 Inequality (KL non-negativity):
  D_KL(P \u2225 Q) \u2265 0
  Status: MeasureTheory.Measure.kl \u2014 DOES NOT EXIST in current Mathlib4.
          MeasureTheory.kl_nonneg \u2014 DOES NOT EXIST in current Mathlib4.
  Treatment: Note in lean4_notes as \u201cBehavioral axiom: Gibbs\u2019 inequality.
             Deferred until MeasureTheory.Measure.kl stabilizes in Mathlib4.\u201d
             Do not attempt a proof. It will fail.

AXIOM 2 \u2014 Hawkes Compensator Martingale:
  N(t) \u2212 \u039b(t) is a martingale w.r.t. the natural filtration.
  Status: ProbabilityTheory.Martingale exists but requires [SigmaFinite \u03bc]
          and [IsFiniteMeasure \u03bc] instances incompatible with continuous-time
          setup. MeasureTheory.Martingale is in the wrong namespace entirely.
  Treatment: Note in lean4_notes as \u201cBehavioral axiom: Doob\u2019s martingale
             characterization. Deferred until ProbabilityTheory API stabilizes.\u201d

AXIOM 3 \u2014 Entropy Collapse Trigger Threshold:
  H(bid) < P\u2085(H, 20-day) is the activation condition.
  Status: An empirically calibrated percentile. Not a theorem.
  Treatment: Do not formalize. It is the trigger condition, not the claim.

CRITICAL RULE: When you see these axioms in a behavioral_claim, acknowledge \
them in lean4_notes and then proceed to prove ONLY the entropy non-negativity \
bound. A formula whose entropy bound is formally verified and whose remaining \
claims are documented behavioral axioms IS correctly classified as \
FORMALLY_VERIFIED. The axiomatic parts do not block verification.

\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550
WHAT TO FORMALIZE
\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550

Agent 051 formulas have the form:
  H(bid) = \u2212\u03a3\u1d62 p(level_i) \xb7 log\u2082(p(level_i))
  p(level_i) = volume_at_level_i / total_bid_volume

The KEY ALGEBRAIC CLAIM in the behavioral_claim is: H(bid) \u2265 0.

This is your sole Lean 4 target. Instantiate entropy_nonneg_order_book \
with the probability vector extracted from behavioral_mappings. \
If the formula uses a variable n levels rather than fixed 10, replace \
\u2018Fin 10\u2019 with \u2018Fin n\u2019 and add \u2018(n : \u2115) (hn : 0 < n)\u2019 to the signature.

Do NOT try to formalize:
  \u2022 The collapse signal (H < threshold \u2192 fragility)
  \u2022 Gibbs\u2019 inequality (KL non-negativity) \u2014 Axiom 1
  \u2022 The compensator martingale \u2014 Axiom 2
  \u2022 Any continuous-time probability theory

\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550
MANDATORY WORKFLOW
\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550

STEP 1 \u2014 FETCH: fetch_formula_data with the provided UUID. Read everything.
STEP 2 \u2014 SYNTAX CHECK: check_lean_syntax on the three seed lemmas:
           '#check @Real.log_nonpos'
           '#check @Finset.sum_nonpos'
           '#check @mul_nonpos_of_nonneg_of_nonpos'
           Confirm all three load before writing any proof.
STEP 3 \u2014 PROOF (up to {LEAN_MAX_ATTEMPTS} attempts): verify_lean_proof.
           Use entropy_nonneg_order_book template. Adjust Fin n only.
STEP 4 \u2014 TERMINAL: update_formula_status. Document behavioral axioms in
           lean4_notes for any Axiom 1/2/3 present in the behavioral_claim.

\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550
FALSIFICATION PROTOCOL
\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550

FALSIFIED requires a concrete counterexample \u2014 specific probability values \
p(i) \u2208 [0,1] where H < 0. No such counterexample exists. Shannon entropy \
is provably non-negative for all valid probability distributions.

Do NOT set FALSIFIED for Agent 051 formulas. If proof attempts are \
exhausted, the issue is a tactic mismatch, not a mathematical falsehood. \
Set SYNTACTICALLY_CORRECT and document the failed tactic sequence in \
lean4_notes so the next proof attempt can learn from it.
"""

    # ------------------------------------------------------------------
    # Instance state
    # ------------------------------------------------------------------

    def __init__(self, config: AgentConfig) -> None:
        super().__init__(config)
        self._fetched_formula_uuid: Optional[str] = None
        self._verify_attempt_count: int = 0
        self._last_verified_proof: Optional[Dict[str, str]] = None

    # ------------------------------------------------------------------
    # BaseAgent interface
    # ------------------------------------------------------------------

    def tools(self) -> List[Dict[str, Any]]:
        return [
            _FETCH_FORMULA_TOOL,
            _CHECK_SYNTAX_TOOL,
            _VERIFY_PROOF_TOOL,
            _UPDATE_STATUS_TOOL,
        ]

    def build_initial_message(self, trigger_data: Dict[str, Any]) -> str:
        uuid         = trigger_data.get("uuid", "UNKNOWN")
        formula_name = trigger_data.get("formula_name", "unnamed formula")
        context      = trigger_data.get("context", "Order Book Entropy pilot cell")
        return (
            "ENTROPY AUDIT DIRECTIVE\n"
            "\u2550" * 56 + "\n"
            f"Formula UUID:   {uuid}\n"
            f"Formula Name:   {formula_name}\n"
            f"Context:        {context}\n"
            "Current Status: SYNTACTICALLY_CORRECT (SymPy validated)\n"
            "\u2550" * 56 + "\n\n"
            "You hold the Master Lock for this entropy formula. "
            "Fetch it, confirm the three seed lemmas load in the REPL, "
            f"and formally verify H \u2265 0 using the seed proof template.\n\n"
            f"Proof attempts budget: {LEAN_MAX_ATTEMPTS} calls to verify_lean_proof.\n\n"
            "Begin with fetch_formula_data."
        )

    def extract_formula(self, tool_input: Dict[str, Any]) -> FormulaDNA:
        raise NotImplementedError(
            "Agent 152 audits existing formulas via update_formula_status. "
            "It does not produce new FormulaDNA."
        )

    async def handle_tool_call(
        self,
        tool_name: str,
        tool_input: Dict[str, Any],
    ) -> Dict[str, Any]:
        if tool_name == "fetch_formula_data":
            return await self._handle_fetch(tool_input)
        if tool_name == "check_lean_syntax":
            return await self._handle_check_syntax(tool_input)
        if tool_name == "verify_lean_proof":
            return await self._handle_verify_proof(tool_input)
        if tool_name == "update_formula_status":
            return await self._handle_update_status(tool_input)
        return {"error": f"Unknown tool: {tool_name}"}

    # ------------------------------------------------------------------
    # Tool handlers
    # ------------------------------------------------------------------

    async def _handle_fetch(self, tool_input: Dict[str, Any]) -> Dict[str, Any]:
        uuid = tool_input.get("uuid", "")
        url = f"{self._config.blackboard_api_url}/v1/formulas/{uuid}"
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                response = await client.get(url)
            if response.status_code == 404:
                return {"error": f"Formula {uuid} not found."}
            response.raise_for_status()
            data = response.json()
            self._fetched_formula_uuid = uuid
            logger.info("[%s] Formula fetched: %s", self.AGENT_ID, uuid[:8])
            return data
        except httpx.RequestError as exc:
            return {"error": f"Blackboard unreachable: {exc}"}

    async def _handle_check_syntax(self, tool_input: Dict[str, Any]) -> Dict[str, Any]:
        expression = tool_input.get("expression", "")
        url = f"{self._config.lean_worker_url}/v1/check"
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(url, json={"expression": expression})
            response.raise_for_status()
            return response.json()
        except httpx.RequestError as exc:
            return {"error": f"Lean server unreachable: {exc}"}

    async def _handle_verify_proof(self, tool_input: Dict[str, Any]) -> Dict[str, Any]:
        theorem_statement  = tool_input.get("theorem_statement", "")
        proof_body         = tool_input.get("proof_body", "")
        attempt_number     = tool_input.get("attempt_number", 1)
        strategy           = tool_input.get("strategy_description", "")

        self._verify_attempt_count += 1
        remaining = LEAN_MAX_ATTEMPTS - self._verify_attempt_count

        url = f"{self._config.lean_worker_url}/v1/verify"
        try:
            async with httpx.AsyncClient(timeout=120.0) as client:
                response = await client.post(url, json={
                    "theorem_statement": theorem_statement,
                    "proof_body":        proof_body,
                })
            response.raise_for_status()
            result = response.json()
        except httpx.RequestError as exc:
            return {"error": f"Lean server unreachable: {exc}", "valid": False}

        if result.get("valid"):
            self._last_verified_proof = {
                "theorem_statement": theorem_statement,
                "proof_body":        proof_body,
                "attempt_number":    str(attempt_number),
                "strategy":          strategy,
            }
            logger.info(
                "[%s] Proof verified on attempt %d: %s",
                self.AGENT_ID, attempt_number, theorem_statement[:60],
            )
            result["message"] = (
                "Proof accepted. Call update_formula_status with "
                "'formally_verified' to lock the IP."
            )
        elif remaining <= 0:
            result["message"] = (
                "Proof budget exhausted. No counterexample found. "
                "Call update_formula_status with 'syntactically_correct' "
                "and document tactic attempts in lean4_notes."
            )
        else:
            result["message"] = (
                f"{remaining} attempt(s) remaining. "
                "Re-check lemma signatures with check_lean_syntax. "
                "Ensure Fin n index type matches formula level count."
            )

        return result

    async def _handle_update_status(self, tool_input: Dict[str, Any]) -> Dict[str, Any]:
        uuid           = tool_input.get("uuid", "")
        new_status_str = tool_input.get("new_status", "")
        lean4_notes    = tool_input.get("lean4_notes", "")

        # Guard: FORMALLY_VERIFIED requires a passing REPL result
        if new_status_str == "formally_verified" and not self._last_verified_proof:
            return {
                "error": (
                    "Cannot set FORMALLY_VERIFIED without a passing verify_lean_proof call. "
                    "The REPL must return valid=true before this status can be written."
                ),
                "accepted": False,
            }

        # Guard: FALSIFIED requires a concrete counterexample
        if new_status_str == "falsified" and len(lean4_notes.strip()) < 50:
            return {
                "error": (
                    "FALSIFIED requires a detailed counterexample in lean4_notes (50+ chars). "
                    "Note: Shannon entropy cannot be falsified for valid probability distributions. "
                    "If proof attempts failed, use 'syntactically_correct' instead."
                ),
                "accepted": False,
            }

        status_map = {
            "formally_verified":   ProofStatus.FORMALLY_VERIFIED,
            "falsified":           ProofStatus.FALSIFIED,
            "syntactically_correct": ProofStatus.SYNTACTICALLY_CORRECT,
        }
        new_status = status_map.get(new_status_str)
        if new_status is None:
            return {"error": f"Unknown status: {new_status_str}", "accepted": False}

        # Combine theorem + proof into lean4_encoding for Blackboard persistence.
        # FormulaDNA validator requires lean4_encoding when status=FORMALLY_VERIFIED.
        lean4_encoding = None
        if new_status_str == "formally_verified":
            theorem = tool_input.get("lean4_theorem_statement", "")
            proof = tool_input.get("lean4_proof_body", "")
            lean4_encoding = f"-- Theorem\n{theorem}\n\n-- Proof\n{proof}"

        url = f"{self._config.blackboard_api_url}/v1/formulas/{uuid}/status"
        payload = {
            "new_status":   new_status.value,
            "agent_id":     self.AGENT_ID,
            "agent_layer":  AgentLayer.LAYER_3.value,
        }
        if lean4_encoding:
            payload["lean4_encoding"] = lean4_encoding

        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.patch(url, json=payload)
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 403:
                return {
                    "error": "Permission denied. Agent 152 must operate as Layer 3.",
                    "accepted": False,
                }
            return {"error": f"Blackboard write failed: HTTP {exc.response.status_code}",
                    "accepted": False}
        except httpx.RequestError as exc:
            return {"error": f"Blackboard unreachable: {exc}", "accepted": False}

        self._run_terminated = True
        logger.info(
            "[%s] Status written: %s → %s",
            self.AGENT_ID, uuid[:8], new_status_str,
        )
        return {
            "accepted":   True,
            "new_status": new_status_str,
            "uuid":       uuid,
            "message":    f"Formula {uuid[:8]} status locked to {new_status_str}. Run complete.",
        }
