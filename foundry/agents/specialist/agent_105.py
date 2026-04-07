"""
Agent 105 — Isomorphism Synthesizer

Authority: Layer 2 — can MERGE, CROSS_LINK, produce unified FormulaDNA.
Mission: Given two FormulaDNA UUIDs, find the mathematical structure connecting
         them and synthesize a unified formula that subsumes both at a higher
         level of abstraction.

Three-tier classification:
  Tier 1 — Syntactic: bijective variable substitution makes equations identical
  Tier 2 — Structural: same mathematical objects in analogous roles
  Tier 3 — Behavioral: same psychological mechanism, different math form

Every run ends with exactly one of:
  propose_unified_formula  — PATH A (synthesis succeeded)
  reject_isomorphism       — PATH B (no connection found)
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import httpx

from foundry.agents.base import AgentConfig, BaseAgent
from foundry.agents.sympy_executor import validate_formula
from foundry.core.schema.formula import (
    AgentLayer, BehavioralMapping, FormulaDNA, MathematicalWing, ProofStatus,
)
from foundry.core.schema.isomorphism import (
    IsomorphismDeclaration, IsomorphismTier, RejectionFailureMode,
    RejectionRecord, TierAnalysis, TierResult,
)

logger = logging.getLogger(__name__)
AGENT_VERSION = "0.1.0"

_FETCH_FORMULA_TOOL = {
    "name": "fetch_formula_data",
    "description": (
        "Retrieve the complete FormulaDNA for a formula UUID from the Blackboard. "
        "MUST call for both UUIDs before performing any analysis."
    ),
    "input_schema": {
        "type": "object",
        "properties": {"uuid": {"type": "string"}},
        "required": ["uuid"],
    },
}

_DECLARE_ISOMORPHISM_TOOL = {
    "name": "declare_isomorphism",
    "description": (
        "Formally declare a mathematical connection between the two formulas. "
        "REQUIRED GATE: cannot call propose_unified_formula without this first. "
        "Creates the CROSS_LINKED edge in the graph."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "uuid_a": {"type": "string"},
            "uuid_b": {"type": "string"},
            "tier_level": {"type": "integer", "enum": [1, 2, 3]},
            "transformation_map": {"type": "string"},
            "tier_analysis": {
                "type": "object",
                "properties": {
                    "tier_1_result": {"type": "string", "enum": ["match", "no_match"]},
                    "tier_1_reasoning": {"type": "string"},
                    "tier_2_result": {"type": "string", "enum": ["match", "no_match"]},
                    "tier_2_reasoning": {"type": "string"},
                    "tier_3_result": {"type": "string", "enum": ["match", "no_match"]},
                    "tier_3_reasoning": {"type": "string"},
                },
                "required": ["tier_1_result", "tier_1_reasoning", "tier_2_result",
                             "tier_2_reasoning", "tier_3_result", "tier_3_reasoning"],
            },
        },
        "required": ["uuid_a", "uuid_b", "tier_level", "transformation_map", "tier_analysis"],
    },
}

_VALIDATE_SYMPY_TOOL = {
    "name": "validate_sympy_expression",
    "description": "Validate a SymPy expression. Must return valid=True before propose_unified_formula.",
    "input_schema": {
        "type": "object",
        "properties": {
            "expression_string": {"type": "string"},
            "reasoning": {"type": "string"},
        },
        "required": ["expression_string", "reasoning"],
    },
}

_PROPOSE_UNIFIED_FORMULA_TOOL = {
    "name": "propose_unified_formula",
    "description": (
        "TERMINAL ACTION — PATH A. Submit the unified formula. "
        "Prerequisites: fetch_formula_data for both UUIDs, declare_isomorphism accepted, "
        "validate_sympy_expression returned valid=True."
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
            "axiomatic_origin": {"type": "array", "items": {"type": "string"}, "minItems": 2},
            "mathematical_wing": {"type": "array", "items": {"type": "string"}, "minItems": 1},
            "tags": {"type": "array", "items": {"type": "string"}},
            "isomorphism_tier": {"type": "integer", "enum": [1, 2, 3]},
        },
        "required": ["name", "description", "symbolic_expression_latex", "behavioral_claim",
                     "behavioral_mappings", "axiomatic_origin", "mathematical_wing", "isomorphism_tier"],
    },
}

_REJECT_ISOMORPHISM_TOOL = {
    "name": "reject_isomorphism",
    "description": (
        "TERMINAL ACTION — PATH B. Record a formal rejection after exhaustive tier evaluation. "
        "Must have called fetch_formula_data for both UUIDs. All three tiers must be no_match."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "uuid_a": {"type": "string"},
            "uuid_b": {"type": "string"},
            "tier_analysis": {
                "type": "object",
                "properties": {
                    "tier_1_result": {"type": "string", "enum": ["no_match"]},
                    "tier_1_reasoning": {"type": "string"},
                    "tier_2_result": {"type": "string", "enum": ["no_match"]},
                    "tier_2_reasoning": {"type": "string"},
                    "tier_3_result": {"type": "string", "enum": ["no_match"]},
                    "tier_3_reasoning": {"type": "string"},
                },
                "required": ["tier_1_result", "tier_1_reasoning", "tier_2_result",
                             "tier_2_reasoning", "tier_3_result", "tier_3_reasoning"],
            },
            "conclusion": {"type": "string"},
            "suggested_bridging_formula": {
                "type": "string",
                "description": (
                    "The mathematical object that would allow these formulas to connect. "
                    "Be specific: name the construction, the operation, and what it produces."
                ),
            },
            "failure_mode": {
                "type": "string",
                "enum": [
                    "temporal_scale_mismatch",
                    "dimensionality_mismatch",
                    "stochastic_deterministic_mismatch",
                    "micro_macro_mismatch",
                    "information_geometry_mismatch",
                    "unclassified",
                ],
                "description": (
                    "WHY these formulas are incompatible — used for deterministic Gardener routing. "
                    "temporal_scale_mismatch: continuous rate vs discrete daily aggregate (→ Agent 201). "
                    "dimensionality_mismatch: univariate vs multivariate formula (→ Agent 202). "
                    "stochastic_deterministic_mismatch: probabilistic vs deterministic ratio (→ Agent 203). "
                    "micro_macro_mismatch: individual order vs aggregate market state (→ Agent 204). "
                    "information_geometry_mismatch: one formula is a game-theoretic strategic scalar "
                    "(e.g. Kyle's Lambda, adverse_selection tags) and the other is an information-theoretic "
                    "probability distribution or entropy measure (e.g. Shannon entropy, information_theory "
                    "tags) — requires Fisher Information bridge (→ Agent 205). "
                    "unclassified: none of the above — human review required."
                ),
            },
        },
        "required": [
            "uuid_a", "uuid_b", "tier_analysis", "conclusion",
            "suggested_bridging_formula", "failure_mode",
        ],
    },
}


class IsomorphismSynthesizer(BaseAgent):
    AGENT_ID = "agent_105"
    AGENT_LAYER = AgentLayer.LAYER_2
    MATHEMATICAL_WING = [MathematicalWing.SYNTHESIZER]

    SYSTEM_PROMPT = """\
You are Agent 105, the Isomorphism Synthesizer within the Formula Foundry.
Layer 2 authority. Your domain is the space BETWEEN domains.

Your output is a new formula: a generalization that subsumes both inputs and
operates at a higher level of abstraction than either parent.

═══════════════════════════════════════════════════════
THE THREE-TIER CLASSIFICATION SYSTEM
═══════════════════════════════════════════════════════

TIER 1 — SYNTACTIC: Does a bijective variable substitution φ: Var_A → Var_B
  exist such that f_A(φ(x)) = f_B(x)? List every substitution explicitly.

TIER 2 — STRUCTURAL: Do the same mathematical objects play analogous roles,
  producing the same qualitative dynamics? Name corresponding objects explicitly.

TIER 3 — BEHAVIORAL: Do the behavioral_mappings describe the same psychological
  mechanism? Quote specific fields from both behavioral_mappings.

═══════════════════════════════════════════════════════
MANDATORY WORKFLOW — FOUR PHASES
═══════════════════════════════════════════════════════

PHASE 1 — READ: fetch_formula_data for UUID_A, then UUID_B. Read full FormulaDNA.

PHASE 2 — ANALYZE: Work through Tier 1, Tier 2, Tier 3 in order.

PHASE 3 — GATE: If any tier matched: declare_isomorphism (creates CROSS_LINKED edge).
  If no tier matched: reject_isomorphism (PATH B, terminal).

PHASE 4 — SYNTHESIS (only if Phase 3 produced a declaration):
  Construct the unified formula. Validate with validate_sympy_expression.
  Call propose_unified_formula (PATH A, terminal).

Every run ends with exactly one terminal action.
If uncertain whether to reject or synthesize, reject with a precise bridging_concept.
A precise rejection is more valuable than a vague synthesis.

═══════════════════════════════════════════════════════
FAILURE MODE CLASSIFICATION — ROUTING KEYS
═══════════════════════════════════════════════════════

When rejecting, you MUST select the failure_mode that describes WHY the
formulas cannot connect. Agent 003 routes deterministically on this field —
no LLM interprets it downstream. Precision here is mandatory.

TEMPORAL_SCALE_MISMATCH (→ Agent 201)
  One formula operates on instantaneous rates (continuous time); the other
  on discrete aggregates (daily bars, session totals). The gap is temporal
  granularity, not domain.

DIMENSIONALITY_MISMATCH (→ Agent 202)
  One formula is univariate; the other is multivariate (vector, bivariate,
  or matrix). The gap is dimensional scope.

STOCHASTIC_DETERMINISTIC_MISMATCH (→ Agent 203)
  One formula is a stochastic process or probabilistic rate; the other is a
  deterministic scalar or OLS coefficient. The gap is the presence or absence
  of a probability space.

MICRO_MACRO_MISMATCH (→ Agent 204)
  One formula operates at the level of an individual order or trader
  (micro); the other at the aggregate market or index level (macro). The
  gap is scale of economic agency.

INFORMATION_GEOMETRY_MISMATCH (→ Agent 205)
  TRIGGER CONDITION — apply this classification when ALL of the following hold:
    1. Formula A carries tags: game_theory, adverse_selection, or kyle_lambda
       (it is a strategic pricing scalar — a regression coefficient or
       equilibrium lambda from a mechanism-design model).
    2. Formula B carries tags: entropy, information_theory, or shannon
       (it is an information-theoretic probability distribution or entropy
       measure over the order book or price process).
    3. No bijective variable substitution, no shared stochastic structure,
       and no common behavioral mechanism exists between them.
  The mathematical reason: mapping a strategic scalar to a macroscopic
  probability distribution requires the Fisher Information metric —
  an object from Information Geometry. Agent 205 constructs this bridge.
  DO NOT classify as unclassified when this trigger condition is satisfied.

UNCLASSIFIED (→ Human review queue)
  None of the above apply. Document the precise mathematical reason in
  suggested_bridging_formula. A human will extend the enum or reclassify.
"""

    def __init__(self, config: AgentConfig) -> None:
        super().__init__(config)
        self._declaration_made: bool = False
        self._fetched_uuids: set = set()

    def tools(self) -> List[Dict[str, Any]]:
        return [_FETCH_FORMULA_TOOL, _DECLARE_ISOMORPHISM_TOOL, _VALIDATE_SYMPY_TOOL,
                _PROPOSE_UNIFIED_FORMULA_TOOL, _REJECT_ISOMORPHISM_TOOL]

    def build_initial_message(self, trigger_data: Dict[str, Any]) -> str:
        uuid_a = trigger_data.get("uuid_a", "UNKNOWN")
        uuid_b = trigger_data.get("uuid_b", "UNKNOWN")
        context = trigger_data.get("synthesis_context", "Panic-Driven Liquidity Withdrawal pilot cell")
        return f"""\
SYNTHESIS DIRECTIVE
════════════════════════════════════════════════════════
Formula A UUID: {uuid_a}
Formula B UUID: {uuid_b}
Synthesis Context: {context}
════════════════════════════════════════════════════════

Fetch both formulas from the Blackboard. Analyze across all three tiers.
Declare if connected. Synthesize if declared. Reject if not.
One terminal action is required. Begin Phase 1.\
"""

    async def handle_tool_call(self, tool_name: str, tool_input: Dict[str, Any]) -> Dict[str, Any]:
        if tool_name == "fetch_formula_data":
            return await self._handle_fetch(tool_input)
        if tool_name == "declare_isomorphism":
            return await self._handle_declare(tool_input)
        if tool_name == "validate_sympy_expression":
            return self._handle_validate(tool_input)
        if tool_name == "reject_isomorphism":
            return await self._handle_reject(tool_input)
        if tool_name == "propose_unified_formula":
            # Handled by base class terminal action via extract_formula
            return {"accepted": True}
        return {"error": f"Unknown tool: {tool_name}"}

    def extract_formula(self, tool_input: Dict[str, Any], validated_sympy_str: str) -> FormulaDNA:
        if not self._declaration_made:
            raise ValueError(
                "Gate violation: propose_unified_formula requires declare_isomorphism first."
            )
        raw_mappings = tool_input.get("behavioral_mappings", [])
        behavioral_mappings = [
            BehavioralMapping(
                variable=m["variable"], latex_symbol=m["latex_symbol"],
                psychological_state=m["psychological_state"],
                measurement_proxy=m["measurement_proxy"], unit=m.get("unit"),
            )
            for m in raw_mappings
        ]
        axiomatic_origin = tool_input.get("axiomatic_origin", [])
        if len(axiomatic_origin) < 2:
            raise ValueError("axiomatic_origin must contain at least two parent UUIDs.")

        tier_label = {1: "syntactic", 2: "structural", 3: "behavioral"}.get(
            tool_input.get("isomorphism_tier", 0), "unknown"
        )
        return FormulaDNA(
            name=tool_input.get("name"),
            description=tool_input.get("description"),
            symbolic_expression_latex=tool_input["symbolic_expression_latex"],
            symbolic_expression_sympy=validated_sympy_str,
            behavioral_claim=tool_input["behavioral_claim"],
            behavioral_mappings=behavioral_mappings,
            axiomatic_origin=axiomatic_origin + [self.AGENT_ID],
            mathematical_wing=tool_input.get("mathematical_wing", [MathematicalWing.SYNTHESIZER.value]),
            proof_status=ProofStatus.SYNTACTICALLY_CORRECT,
            tags=tool_input.get("tags", []) + [
                "agent_105", "unified_formula", f"tier_{tier_label}_isomorphism",
            ],
        )

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
            self._fetched_uuids.add(uuid)
            logger.info("[%s] Fetched formula %s: %s", self.AGENT_ID, uuid[:8], data.get("name"))
            return {
                "found": True, "uuid": uuid, "name": data.get("name"),
                "symbolic_expression_latex": data.get("symbolic_expression_latex"),
                "behavioral_claim": data.get("behavioral_claim"),
                "behavioral_mappings": data.get("behavioral_mappings", []),
                "mathematical_wing": data.get("mathematical_wing", []),
                "proof_status": data.get("proof_status"),
                "axiomatic_origin": data.get("axiomatic_origin", []),
                "tags": data.get("tags", []),
            }
        except httpx.RequestError as exc:
            return {"error": f"Blackboard API unreachable: {exc}"}

    async def _handle_declare(self, tool_input: Dict[str, Any]) -> Dict[str, Any]:
        uuid_a = tool_input.get("uuid_a", "")
        uuid_b = tool_input.get("uuid_b", "")
        missing = {uuid_a, uuid_b} - self._fetched_uuids
        if missing:
            return {"error": f"Gate violation: must fetch both UUIDs first. Not fetched: {missing}",
                    "accepted": False}
        try:
            tier_input = tool_input.get("tier_analysis", {})
            tier_analysis = TierAnalysis(
                tier_1_result=TierResult(tier_input.get("tier_1_result", "no_match")),
                tier_1_reasoning=tier_input.get("tier_1_reasoning", ""),
                tier_2_result=TierResult(tier_input.get("tier_2_result", "no_match")),
                tier_2_reasoning=tier_input.get("tier_2_reasoning", ""),
                tier_3_result=TierResult(tier_input.get("tier_3_result", "no_match")),
                tier_3_reasoning=tier_input.get("tier_3_reasoning", ""),
            )
            tier_level = IsomorphismTier(tool_input.get("tier_level", 1))
            declaration = IsomorphismDeclaration(
                uuid_a=uuid_a, uuid_b=uuid_b, tier_level=tier_level,
                transformation_map=tool_input.get("transformation_map", ""),
                tier_analysis=tier_analysis, declared_by=self.AGENT_ID,
            )
        except Exception as exc:
            return {"error": f"Declaration schema failed: {exc}", "accepted": False}

        url = f"{self._config.blackboard_api_url}/v1/formulas/{uuid_a}/cross-link"
        payload = {
            "uuid_b": uuid_b, "agent_id": self.AGENT_ID,
            "isomorphism_description": tool_input.get("transformation_map", ""),
            "agent_layer": AgentLayer.LAYER_2.value,
            "tier_level": tier_level.value,
            "transformation_map": tool_input.get("transformation_map", ""),
        }
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                response = await client.post(url, json=payload)
            response.raise_for_status()
        except httpx.RequestError as exc:
            return {"error": f"Blackboard unreachable during cross-link: {exc}", "accepted": False}

        self._declaration_made = True
        logger.info("[%s] Isomorphism declared: %s ↔ %s (Tier %d)",
                    self.AGENT_ID, uuid_a[:8], uuid_b[:8], tier_level.value)
        return {
            "accepted": True,
            "declaration_id": declaration.declaration_id,
            "tier_level": tier_level.value,
            "cross_link_created": True,
            "message": (
                f"Tier {tier_level.value} isomorphism declared and CROSS_LINKED edge created. "
                "Validate your unified formula and call propose_unified_formula."
            ),
        }

    def _handle_validate(self, tool_input: Dict[str, Any]) -> Dict[str, Any]:
        expression = tool_input.get("expression_string", "")
        result = validate_formula(expression, timeout=self._config.sympy_timeout_seconds)
        if result.get("valid"):
            logger.info("[%s] SymPy PASSED: %s", self.AGENT_ID, expression[:60])
            return {
                "valid": True, "sympy_str": result["sympy_str"],
                "latex_roundtrip": result.get("latex_roundtrip"),
                "free_symbols": result.get("free_symbols", []),
                "message": "Expression is valid. Call propose_unified_formula.",
            }
        return {"valid": False, "error": result.get("error"),
                "message": "Expression failed. use exp() not e^x, ** not ^, * for multiplication."}

    async def _handle_reject(self, tool_input: Dict[str, Any]) -> Dict[str, Any]:
        uuid_a = tool_input.get("uuid_a", "")
        uuid_b = tool_input.get("uuid_b", "")
        missing = {uuid_a, uuid_b} - self._fetched_uuids
        if missing:
            return {"error": f"Gate violation: cannot reject without fetching both. Not fetched: {missing}",
                    "accepted": False}
        try:
            tier_input = tool_input.get("tier_analysis", {})
            tier_analysis = TierAnalysis(
                tier_1_result=TierResult(tier_input.get("tier_1_result", "no_match")),
                tier_1_reasoning=tier_input.get("tier_1_reasoning", ""),
                tier_2_result=TierResult(tier_input.get("tier_2_result", "no_match")),
                tier_2_reasoning=tier_input.get("tier_2_reasoning", ""),
                tier_3_result=TierResult(tier_input.get("tier_3_result", "no_match")),
                tier_3_reasoning=tier_input.get("tier_3_reasoning", ""),
            )
            failure_mode_raw = tool_input.get("failure_mode", "unclassified")
            try:
                failure_mode = RejectionFailureMode(failure_mode_raw)
            except ValueError:
                logger.warning(
                    "[%s] Unknown failure_mode '%s', defaulting to UNCLASSIFIED.",
                    self.AGENT_ID, failure_mode_raw,
                )
                failure_mode = RejectionFailureMode.UNCLASSIFIED

            record = RejectionRecord(
                uuid_a=uuid_a, uuid_b=uuid_b, tier_analysis=tier_analysis,
                conclusion=tool_input.get("conclusion", ""),
                suggested_bridging_formula=tool_input.get("suggested_bridging_formula", ""),
                failure_mode=failure_mode,
                rejected_by=self.AGENT_ID, agent_version=AGENT_VERSION,
            )
        except Exception as exc:
            return {"error": f"Rejection schema failed: {exc}. All tiers must be no_match.",
                    "accepted": False}

        url = f"{self._config.blackboard_api_url}/v1/rejections"
        payload = {
            "uuid_a": uuid_a, "uuid_b": uuid_b, "rejection_id": record.rejection_id,
            "agent_id": self.AGENT_ID, "agent_version": AGENT_VERSION,
            "tier_1_result": tier_analysis.tier_1_result.value,
            "tier_2_result": tier_analysis.tier_2_result.value,
            "tier_3_result": tier_analysis.tier_3_result.value,
            "conclusion": record.conclusion,
            "suggested_bridging_formula": record.suggested_bridging_formula,
            "failure_mode": failure_mode.value,
        }
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                response = await client.post(url, json=payload)
            response.raise_for_status()
        except httpx.RequestError as exc:
            logger.error("[%s] Blackboard unreachable for rejection write: %s", self.AGENT_ID, exc)

        self._run_terminated = True
        self._run_output_data = {
            "outcome": "rejected",
            "rejection_id": record.rejection_id,
            "uuid_a": uuid_a, "uuid_b": uuid_b,
            "failure_mode": failure_mode.value,
            "suggested_bridging_formula": record.suggested_bridging_formula,
        }
        logger.info("[%s] Rejection recorded: %s ↔ %s | Mode: %s | Bridge: %s",
                    self.AGENT_ID, uuid_a[:8], uuid_b[:8],
                    failure_mode.value,
                    record.suggested_bridging_formula[:80])
        return {
            "accepted": True,
            "rejection_id": record.rejection_id,
            "failure_mode": failure_mode.value,
            "message": (
                f"Rejection formally recorded (mode: {failure_mode.value}). "
                "Agent 003 will route to the correct Evolutionary Gardener. Run complete."
            ),
        }
