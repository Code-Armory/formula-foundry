"""
Agent 151 — Lean 4 Auditor

Authority: Layer 3 — Master Lock.
The only agent that can write FORMALLY_VERIFIED or FALSIFIED to the Blackboard.

Mission: Translate SYNTACTICALLY_CORRECT formulas into formally verified
Lean 4 theorems using Mathlib4, via the persistent lean_worker REPL.

Key constraints:
  - Does NOT produce a new formula — audits an existing one.
  - FORMALLY_VERIFIED requires self._last_verified_proof to be populated
    (only set when REPL returns valid=true). Cannot self-certify.
  - FALSIFIED requires 50+ chars of counterexample in lean4_notes.
    Proof exhaustion ≠ FALSIFIED.
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

_FETCH_FORMULA_TOOL = {
    "name": "fetch_formula_data",
    "description": (
        "Retrieve the complete FormulaDNA from the Blackboard by UUID. "
        "MUST be your first tool call. Read symbolic_expression_latex, "
        "behavioral_claim, and every behavioral_mapping before drafting Lean 4 code. "
        "Your theorem must formalize the behavioral_claim, not just the LaTeX."
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
        "Use before committing to a full theorem. Fast — use freely. "
        "Examples: '#check Real.exp', '#check @mul_lt_mul_of_pos_left'"
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
        "Read compiler errors carefully. Common fixes: wrong lemma → try linarith/positivity; "
        "type mismatch → check ℝ vs ℕ; goal not closed → add norm_num or simp. "
        "Do NOT submit a proof with 'sorry' — that is not verification."
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
        "required": ["theorem_statement", "proof_body", "attempt_number", "strategy_description"],
    },
}

_UPDATE_STATUS_TOOL = {
    "name": "update_formula_status",
    "description": (
        "TERMINAL ACTION — Layer 3 Master Lock. "
        "FORMALLY_VERIFIED: verify_lean_proof returned valid=true. "
        "FALSIFIED: you have a concrete counterexample in lean4_notes. "
        "SYNTACTICALLY_CORRECT: 5 attempts exhausted, no counterexample. "
        "CRITICAL: FALSIFIED requires an explicit counterexample. "
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
        "required": ["uuid", "new_status", "lean4_theorem_statement",
                     "lean4_proof_body", "lean4_notes"],
    },
}


class Lean4Auditor(BaseAgent):
    AGENT_ID = "agent_151"
    AGENT_LAYER = AgentLayer.LAYER_3
    MATHEMATICAL_WING = [MathematicalWing.VERIFICATION]

    SYSTEM_PROMPT = f"""\
You are Agent 151, the Lean 4 Auditor within the Formula Foundry.
Authority: Layer 3 — Master Lock.

═══════════════════════════════════════════════════════
SEED PROOF 1 — HAWKES BRANCHING SUBCRITICALITY
═══════════════════════════════════════════════════════

```lean
theorem hawkes_branching_subcritical
    (α β : ℝ) (hβ : 0 < β) (h : α < β) : α / β < 1 := by
  rwa [div_lt_one hβ]
```

Key: `div_lt_one (hb : 0 < b) : a / b < 1 ↔ a < b`
`rwa` = rewrite + assumption.

═══════════════════════════════════════════════════════
SEED PROOF 2 — EXCITATION MONOTONE IN SPREAD
═══════════════════════════════════════════════════════

```lean
theorem excitation_monotone_in_spread
    (α κ S₁ S₂ : ℝ)
    (hα : 0 < α) (hκ : 0 < κ) (hS : S₁ < S₂) :
    α * (1 + κ * S₁) < α * (1 + κ * S₂) := by
  apply mul_lt_mul_of_pos_left _ hα
  linarith [mul_lt_mul_of_pos_left hS hκ]
```

═══════════════════════════════════════════════════════
LEAN 4 TRANSLATION REFERENCE
═══════════════════════════════════════════════════════

LaTeX → Lean 4:
  ℝ declaration:     (λ_t μ α β κ : ℝ)
  Positivity:        (hβ : 0 < β)
  Non-negativity:    (hμ : 0 ≤ μ)
  Exponential:       Real.exp (-β * t)
  exp positivity:    Real.exp_pos x

Key Mathlib4 lemmas:
  div_lt_one (hb : 0 < b) : a / b < 1 ↔ a < b
  mul_lt_mul_of_pos_left : a < b → 0 < c → c * a < c * b
  mul_lt_mul_of_pos_right : a < b → 0 < c → a * c < b * c
  mul_pos : 0 < a → 0 < b → 0 < a * b
  Real.exp_pos : ∀ x, 0 < Real.exp x

Proof tactics (simplest first):
  exact h | rwa [lemma] | linarith [...] | nlinarith [...] |
  positivity | ring | norm_num | gcongr | simp [lemma]

═══════════════════════════════════════════════════════
SEED PROOF 3 — VECTOR HAWKES 2×2 SUBCRITICALITY
═══════════════════════════════════════════════════════

This proves the M-matrix subcriticality criterion for a 2×2 cross-excitation
matrix A with non-negative entries and row sums < 1.

Context: The bivariate Hawkes process (Agent 060's output) is mean-reverting
iff ρ(A) < 1, where ρ is the spectral radius. For 2×2 non-negative matrices,
the row-sum condition (each row sums to < 1) implies spectral radius < 1 via
the M-matrix determinant criterion: det(I - A) > 0.

REPL DRIFT NOTE: Earlier versions of this proof used `nlinarith` with explicit
product hints. Recent Mathlib4 commits require `import Mathlib.Tactic.Linarith`
for `nlinarith` to be available, which is not always present even after
`import Mathlib`. The proof below avoids `nlinarith` entirely, using only
`linarith`, `mul_lt_mul_of_pos_left`, and `mul_le_mul_of_nonneg_right` —
all stable, always-available Mathlib4 lemmas.

```lean
theorem vector_hawkes_subcritical_2x2
    (a b c d : ℝ)
    (ha : 0 ≤ a) (hb : 0 ≤ b) (hc : 0 ≤ c) (hd : 0 ≤ d)
    (h_row1 : a + b < 1)
    (h_row2 : c + d < 1) :
    0 < (1 - a) * (1 - d) - b * c := by
  have ha' : 0 < 1 - a := by linarith
  have hd' : 0 < 1 - d := by linarith
  have h4 : c < 1 - d := by linarith
  have h5 : b ≤ 1 - a := by linarith
  calc 0 < (1 - a) * (1 - d) - (1 - a) * c := by
            have := mul_lt_mul_of_pos_left h4 ha'
            linarith
       _ ≤ (1 - a) * (1 - d) - b * c := by
            have := mul_le_mul_of_nonneg_right h5 hc
            linarith
```

Proof walkthrough (two-step calc chain):

  Step 1: 0 < (1-a)*(1-d) - (1-a)*c
    ha': 0 < 1-a              (from h_row1: a+b<1, since b≥0)
    h4:  c < 1-d              (from h_row2: c+d<1)
    mul_lt_mul_of_pos_left h4 ha' : (1-a)*c < (1-a)*(1-d)
    linarith closes: (1-a)*(1-d) - (1-a)*c > 0

  Step 2: (1-a)*(1-d) - (1-a)*c ≤ (1-a)*(1-d) - b*c
    h5:   b ≤ 1-a             (from h_row1: a+b<1)
    mul_le_mul_of_nonneg_right h5 hc : b*c ≤ (1-a)*c
    linarith closes: (1-a)*c ≥ b*c so (1-a)*(1-d) - (1-a)*c ≤ (1-a)*(1-d) - b*c

  Transitivity: 0 < ... ≤ ... → 0 < (1-a)*(1-d) - b*c ✓

Variable mapping for Agent 060's output:
  a = A_ES→ES  (self-excitation of equity panic)
  b = A_ZN→ES  (cross-excitation: Treasury → Equity)
  c = A_ES→ZN  (cross-excitation: Equity → Treasury)
  d = A_ZN→ZN  (self-excitation of Treasury order flow)
  h_row1: A_ES→ES + A_ZN→ES < 1  (ES row subcriticality)
  h_row2: A_ES→ZN + A_ZN→ZN < 1  (ZN row subcriticality)
  Goal: det(I - A) = (1-a)(1-d) - bc > 0

CRITICAL: Do NOT attempt to prove ρ(A) < 1 directly via Matrix.spectralRadius.
The spectral radius over ℂ requires significant Mathlib4 machinery not yet
mature enough for reliable proof search. The row-sum M-matrix approach gives
the same mathematical guarantee (subcriticality) through elementary algebra.
This proof closes in < 1s on any Mathlib4 version since the lemmas used are
stable across all recent commits.

═══════════════════════════════════════════════════════
SEED PROOF 4 — MEAN-FIELD INTENSITY POSITIVITY
═══════════════════════════════════════════════════════

Introduced for Agent 204 Pattern A (Mean-Field micro/macro bridge).
Proves the stationary mean-field intensity is strictly positive.

```lean
theorem mean_field_intensity_pos
    (μ α β : ℝ)
    (hμ : 0 < μ) (hα : 0 ≤ α) (hβ : 0 < β) (hsc : α < β) :
    0 < μ / (1 - α / β) := by
  have h1 : 0 < 1 - α / β := by
    rw [sub_pos]
    exact (div_lt_one hβ).mpr hsc
  exact div_pos hμ h1
```

═══════════════════════════════════════════════════════
SEED PROOF 5 — PARTITION FUNCTION POSITIVITY
═══════════════════════════════════════════════════════

Introduced for Agent 204 Pattern B (Boltzmann partition function).

```lean
theorem partition_function_pos
    (n : ℕ) (hn : 0 < n) (β : ℝ) (H : Fin n → ℝ) :
    0 < ∑ i, Real.exp (-β * H i) := by
  apply Finset.sum_pos
  · intro i _
    exact Real.exp_pos _
  · exact Finset.univ_nonempty_iff.mpr ⟨⟨0, hn⟩⟩
```

═══════════════════════════════════════════════════════
SEED PROOF 6 — INFORMATION GEOMETRY LAMBDA POSITIVITY
═══════════════════════════════════════════════════════

Introduced for Agent 205 (Information Geometry Gardener).
Proves λ_info = λ₀ · exp(−H_OFI) > 0 given λ₀ > 0.
Real.exp_pos is universally positive — no constraint on H_OFI needed.

```lean
theorem info_geo_lambda_pos
    (lambda_0 H_OFI : ℝ)
    (h0 : 0 < lambda_0) :
    0 < lambda_0 * Real.exp (-H_OFI) := by
  exact mul_pos h0 (Real.exp_pos _)
```

═══════════════════════════════════════════════════════
SEED PROOF 7 — NONLINEAR IMPACT CONVEXITY
═══════════════════════════════════════════════════════

Introduced for Agent 050 Extension A (nonlinear_impact formulas).
Proves d²(Δp)/dx² = 2γ ≥ 0. Pure real arithmetic — linarith only.
Analysis.Convex.Function is NOT required.

```lean
theorem nonlinear_impact_convex
    (γ : ℝ) (hγ : 0 ≤ γ) :
    0 ≤ 2 * γ := by
  linarith
```

If the formula uses γ > 0 (strict positivity):
```lean
theorem nonlinear_impact_convex_strict
    (γ : ℝ) (hγ : 0 < γ) :
    0 < 2 * γ := by
  linarith
```

═══════════════════════════════════════════════════════
WHAT TO FORMALIZE
═══════════════════════════════════════════════════════

The full Hawkes intensity as a stochastic process is NOT formalizable yet.
Instead, identify the KEY ALGEBRAIC CLAIM in the behavioral_claim:

  "fear amplified by spread" → α*(1+κ*S₁) < α*(1+κ*S₂) when S₁ < S₂
  "panic is self-sustaining" → α*(1+κ*S_max)/β ≥ 1 under conditions
  "intensity non-negative"   → 0 ≤ μ + α*(1+κ*S)*Real.exp(-β*t)

  FOR AGENT 060 OUTPUT (bivariate Hawkes):
  "system is mean-reverting" → det(I - A) > 0 given row sums < 1
    → Use theorem vector_hawkes_subcritical_2x2 from Seed Proof 3
    → Extract A entries from behavioral_mappings (a_ESES, a_ZNES, a_ESZN, a_ZNZN)
    → Map to (a, b, c, d) in the theorem template
    → Hypotheses come from subcriticality_condition field of the formula

Choose ONE claim. Make it a clean, self-contained theorem. Prove it completely.

═══════════════════════════════════════════════════════
MANDATORY WORKFLOW
═══════════════════════════════════════════════════════

STEP 1 — FETCH: fetch_formula_data with the provided UUID. Read everything.
STEP 2 — SYNTAX CHECK: check_lean_syntax to validate types and lemma names.
STEP 3 — PROOF (up to {LEAN_MAX_ATTEMPTS} attempts): verify_lean_proof.
STEP 4 — TERMINAL: update_formula_status (one of three outcomes).

═══════════════════════════════════════════════════════
FALSIFICATION PROTOCOL
═══════════════════════════════════════════════════════

FALSIFIED requires a concrete counterexample — specific variable values
that make the behavioral_claim false.
'I tried 5 tactics and none worked' is NOT falsification.
Only FALSIFY when mathematically certain.
"""

    def __init__(self, config: AgentConfig) -> None:
        super().__init__(config)
        self._fetched_formula_uuid: Optional[str] = None
        self._verify_attempt_count: int = 0
        self._last_verified_proof: Optional[Dict[str, str]] = None

    def tools(self) -> List[Dict[str, Any]]:
        return [_FETCH_FORMULA_TOOL, _CHECK_SYNTAX_TOOL, _VERIFY_PROOF_TOOL, _UPDATE_STATUS_TOOL]

    def build_initial_message(self, trigger_data: Dict[str, Any]) -> str:
        uuid = trigger_data.get("uuid", "UNKNOWN")
        formula_name = trigger_data.get("formula_name", "unnamed formula")
        context = trigger_data.get("context", "Panic-Liquidity pilot cell")
        return f"""\
AUDIT DIRECTIVE
════════════════════════════════════════════════════════
Formula UUID:   {uuid}
Formula Name:   {formula_name}
Context:        {context}
Current Status: SYNTACTICALLY_CORRECT (SymPy validated)
════════════════════════════════════════════════════════

You hold the Master Lock. Fetch this formula, identify its core algebraic \
claim, and formally verify it in Lean 4 / Mathlib4.

Proof attempts budget: {LEAN_MAX_ATTEMPTS} calls to verify_lean_proof.
Use check_lean_syntax liberally to validate types before proof attempts.

Begin with fetch_formula_data.\
"""

    async def handle_tool_call(self, tool_name: str, tool_input: Dict[str, Any]) -> Dict[str, Any]:
        if tool_name == "fetch_formula_data":
            return await self._handle_fetch(tool_input)
        if tool_name == "check_lean_syntax":
            return await self._handle_check_syntax(tool_input)
        if tool_name == "verify_lean_proof":
            return await self._handle_verify_proof(tool_input)
        if tool_name == "update_formula_status":
            return await self._handle_update_status(tool_input)
        return {"error": f"Unknown tool: {tool_name}"}

    def extract_formula(self, tool_input: Dict[str, Any], validated_sympy_str: str) -> FormulaDNA:
        raise NotImplementedError("Agent 151 audits existing formulas via update_formula_status.")

    async def _handle_fetch(self, tool_input: Dict[str, Any]) -> Dict[str, Any]:
        uuid = tool_input.get("uuid", "")
        url = f"{self._config.blackboard_api_url}/v1/formulas/{uuid}"
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.get(url)
            if resp.status_code == 404:
                return {"error": f"Formula {uuid} not found."}
            resp.raise_for_status()
            data = resp.json()
        except httpx.RequestError as exc:
            return {"error": f"Blackboard API unreachable: {exc}"}

        self._fetched_formula_uuid = uuid
        logger.info("[%s] Fetched formula: %s", self.AGENT_ID, data.get("name"))
        return {
            "found": True, "uuid": uuid, "name": data.get("name"),
            "proof_status": data.get("proof_status"),
            "symbolic_expression_latex": data.get("symbolic_expression_latex"),
            "symbolic_expression_sympy": data.get("symbolic_expression_sympy"),
            "behavioral_claim": data.get("behavioral_claim"),
            "behavioral_mappings": data.get("behavioral_mappings", []),
            "audit_note": (
                "Read behavioral_claim carefully. Your theorem must capture the "
                "algebraic property that makes the behavioral_claim true or false."
            ),
        }

    async def _handle_check_syntax(self, tool_input: Dict[str, Any]) -> Dict[str, Any]:
        expression = tool_input.get("expression", "")
        url = f"{self._config.lean_worker_url}/v1/check"
        logger.info("[%s] Checking syntax: %s", self.AGENT_ID, expression[:60])
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(url, json={"expression": expression})
            if resp.status_code == 503:
                return {"error": "Lean REPL not ready. Mathlib still loading.", "valid": False}
            resp.raise_for_status()
            result = resp.json()
        except httpx.RequestError as exc:
            return {"error": f"Lean worker unreachable: {exc}", "valid": False}

        return {
            "valid": result.get("valid", False),
            "type_info": result.get("type_info"),
            "errors": result.get("errors", []),
            "message": (
                "Expression typechecks correctly." if result.get("valid")
                else f"Type error: {result.get('errors', ['unknown'])[0]}"
            ),
        }

    async def _handle_verify_proof(self, tool_input: Dict[str, Any]) -> Dict[str, Any]:
        theorem_statement = tool_input.get("theorem_statement", "")
        proof_body = tool_input.get("proof_body", "")
        strategy = tool_input.get("strategy_description", "")

        self._verify_attempt_count += 1
        remaining = LEAN_MAX_ATTEMPTS - self._verify_attempt_count
        logger.info("[%s] Proof attempt %d/%d: %s",
                    self.AGENT_ID, self._verify_attempt_count, LEAN_MAX_ATTEMPTS, strategy[:60])

        url = f"{self._config.lean_worker_url}/v1/verify"
        payload = {
            "theorem_statement": theorem_statement,
            "proof_body": proof_body,
            "context": f"Agent 151 attempt {self._verify_attempt_count}",
        }
        try:
            async with httpx.AsyncClient(timeout=150.0) as client:
                resp = await client.post(url, json=payload)
            if resp.status_code == 503:
                return {"valid": False, "errors": ["Lean REPL not ready."],
                        "attempts_remaining": remaining}
            resp.raise_for_status()
            result = resp.json()
        except httpx.RequestError as exc:
            return {"valid": False, "errors": [f"Lean worker unreachable: {exc}"],
                    "attempts_remaining": remaining}

        valid = result.get("valid", False)
        if valid:
            self._last_verified_proof = {
                "theorem_statement": theorem_statement,
                "proof_body": proof_body,
            }
            logger.info("[%s] Proof ACCEPTED on attempt %d (%.3fs)",
                        self.AGENT_ID, self._verify_attempt_count,
                        result.get("elapsed_seconds", 0))

        response = {
            "valid": valid,
            "errors": result.get("errors", []),
            "warnings": result.get("warnings", []),
            "elapsed_seconds": result.get("elapsed_seconds"),
            "attempts_used": self._verify_attempt_count,
            "attempts_remaining": remaining,
        }
        if valid:
            response["message"] = (
                f"Proof accepted in {result.get('elapsed_seconds', 0):.3f}s. "
                "Call update_formula_status with 'formally_verified'."
            )
        elif remaining <= 0:
            response["message"] = (
                "Proof budget exhausted. If no counterexample, call update_formula_status "
                "with 'syntactically_correct' and document attempts in lean4_notes."
            )
        else:
            response["message"] = f"{remaining} attempt(s) remaining. Read the error and adjust."
        return response

    async def _handle_update_status(self, tool_input: Dict[str, Any]) -> Dict[str, Any]:
        uuid = tool_input.get("uuid", "")
        new_status_str = tool_input.get("new_status", "")
        lean4_notes = tool_input.get("lean4_notes", "")

        if new_status_str == "formally_verified" and not self._last_verified_proof:
            return {"error": "Cannot set FORMALLY_VERIFIED without a passing verify_lean_proof call.",
                    "accepted": False}

        if new_status_str == "falsified" and len(lean4_notes.strip()) < 50:
            return {"error": "FALSIFIED requires a detailed counterexample in lean4_notes.",
                    "accepted": False}

        status_map = {
            "formally_verified": ProofStatus.FORMALLY_VERIFIED,
            "falsified": ProofStatus.FALSIFIED,
            "syntactically_correct": ProofStatus.SYNTACTICALLY_CORRECT,
        }
        new_status = status_map.get(new_status_str)
        if new_status is None:
            return {"error": f"Unknown status: {new_status_str}", "accepted": False}

        url = f"{self._config.blackboard_api_url}/v1/formulas/{uuid}/status"
        payload = {
            "new_status": new_status.value,
            "agent_id": self.AGENT_ID,
            "agent_layer": AgentLayer.LAYER_3.value,
        }
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.patch(url, json=payload)
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 403:
                return {"error": "Permission denied. Agent 151 must be Layer 3.", "accepted": False}
            return {"error": f"Blackboard API error: {exc}", "accepted": False}
        except httpx.RequestError as exc:
            return {"error": f"Blackboard API unreachable: {exc}", "accepted": False}

        logger.info("[%s] Formula %s → %s", self.AGENT_ID, uuid[:8], new_status_str.upper())

        self._run_terminated = True
        self._run_output_data = {
            "outcome": new_status_str,
            "uuid": uuid,
            "lean4_theorem_statement": tool_input.get("lean4_theorem_statement", ""),
            "lean4_proof_body": tool_input.get("lean4_proof_body", ""),
            "lean4_notes": lean4_notes,
            "verify_attempts_used": self._verify_attempt_count,
            "agent_version": AGENT_VERSION,
        }

        outcome_label = {
            "formally_verified": "✓ FORMALLY VERIFIED",
            "falsified": "✗ FALSIFIED",
            "syntactically_correct": "~ PROOF DEFERRED",
        }.get(new_status_str, new_status_str.upper())

        return {
            "accepted": True,
            "outcome": new_status_str,
            "uuid": uuid,
            "message": f"{outcome_label}. Formula {uuid[:8]} updated. Run complete.",
        }
