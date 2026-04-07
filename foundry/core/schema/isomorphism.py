"""
Isomorphism Schema — Data contracts for Agent 105's outputs.

Every isomorphism Agent 105 evaluates produces exactly one of:
  - IsomorphismDeclaration  (connection found → synthesis proceeds)
  - RejectionRecord         (no connection → leaves breadcrumbs for Evolutionary agents)

Both are permanent records in the Neo4j graph:
  CROSS_LINKED edge          ← IsomorphismDeclaration
  REJECTED_ISOMORPHISM edge  ← RejectionRecord

Design principle:
  A well-reasoned rejection is as valuable as a discovery.
  The `suggested_bridging_formula` in RejectionRecord is a research directive
  for the Evolutionary Gardeners (Agents 201-250).
  The `failure_mode` field is the deterministic routing key used by Agent 003
  (Evolution Trigger Monitor) to dispatch the correct Gardener without any
  LLM interpretation or keyword parsing.
"""

from __future__ import annotations

import uuid as uuid_lib
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, model_validator


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------


class TierResult(str, Enum):
    MATCH = "match"
    NO_MATCH = "no_match"


class IsomorphismTier(int, Enum):
    """
    The three-tier classification system for structural mathematical connections.

    Tier 1 — Syntactic: Equations are literally equivalent under variable substitution.
             Test: can I write a bijective map φ: Var_A → Var_B such that f_A(φ(x)) = f_B(x)?
             Verifiable mechanically by SymPy.

    Tier 2 — Structural: Same mathematical objects playing analogous roles.
             Same qualitative dynamics (self-excitation, decay, branching).
             Functional forms may differ. Requires transformation map + behavioral narrative.

    Tier 3 — Behavioral: The behavioral_mappings tell the same psychological story.
             Mechanisms are the same even when the math looks different.
             Hardest to verify, highest IP value when found.
    """
    SYNTACTIC = 1
    STRUCTURAL = 2
    BEHAVIORAL = 3


class RejectionFailureMode(str, Enum):
    """
    Deterministic routing key for Evolutionary Gardeners.

    Agent 105 MUST select one of these values when calling reject_isomorphism.
    Agent 003 (Evolution Trigger Monitor) reads this field to route the rejection
    to the correct Gardener without any LLM interpretation or keyword parsing.

    Routing table (see AGENT_MATRIX.md for full spec):
      temporal_scale_mismatch         → Agent 201 (Temporal Scale Bridger)
      dimensionality_mismatch         → Agent 202 (Dimensionality Projection Bridger)
      stochastic_deterministic_mismatch → Agent 203 (Stochastic/Deterministic Bridge)
      micro_macro_mismatch            → Agent 204 (Micro/Macro Scale Bridge)
      information_geometry_mismatch   → Agent 205 (Information Geometry Gardener)
      unclassified                    → Human review queue (no Gardener assigned)

    If the rejection genuinely doesn't fit any category, select UNCLASSIFIED
    and document the reason in suggested_bridging_formula. A human will review
    and either extend this enum or reclassify the rejection.
    """
    TEMPORAL_SCALE_MISMATCH              = "temporal_scale_mismatch"
    DIMENSIONALITY_MISMATCH              = "dimensionality_mismatch"
    STOCHASTIC_DETERMINISTIC_MISMATCH    = "stochastic_deterministic_mismatch"
    MICRO_MACRO_MISMATCH                 = "micro_macro_mismatch"
    INFORMATION_GEOMETRY_MISMATCH        = "information_geometry_mismatch"
    UNCLASSIFIED                         = "unclassified"


# ---------------------------------------------------------------------------
# Sub-models
# ---------------------------------------------------------------------------


class TierAnalysis(BaseModel):
    """
    Complete three-tier evaluation. Agent 105 must fill all three tiers
    before it is permitted to call either terminal action.
    """

    tier_1_result: TierResult = Field(..., description="Syntactic isomorphism result")
    tier_1_reasoning: str = Field(
        ...,
        min_length=20,
        description=(
            "Why tier 1 matches or doesn't. Must reference specific variables. "
            "E.g. 'No bijective substitution possible: formula A is a sum over "
            "discrete events, formula B is a continuous integral.'"
        ),
    )
    tier_2_result: TierResult = Field(..., description="Structural isomorphism result")
    tier_2_reasoning: str = Field(
        ...,
        min_length=20,
        description=(
            "Why tier 2 matches or doesn't. Must reference the mathematical objects. "
            "E.g. 'Both are self-exciting processes with exponential decay kernels "
            "and a critical branching ratio.'"
        ),
    )
    tier_3_result: TierResult = Field(..., description="Behavioral isomorphism result")
    tier_3_reasoning: str = Field(
        ...,
        min_length=20,
        description=(
            "Why tier 3 matches or doesn't. Must reference the behavioral_mappings "
            "of both formulas. E.g. 'Both model the feedback loop between an "
            "intensity-amplifying event and the structural fragility it creates.'"
        ),
    )

    @property
    def highest_tier_match(self) -> Optional[IsomorphismTier]:
        """Return the highest tier at which a match was found, or None."""
        if self.tier_3_result == TierResult.MATCH:
            return IsomorphismTier.BEHAVIORAL
        if self.tier_2_result == TierResult.MATCH:
            return IsomorphismTier.STRUCTURAL
        if self.tier_1_result == TierResult.MATCH:
            return IsomorphismTier.SYNTACTIC
        return None

    @property
    def any_match(self) -> bool:
        return self.highest_tier_match is not None


# ---------------------------------------------------------------------------
# Output contracts
# ---------------------------------------------------------------------------


class IsomorphismDeclaration(BaseModel):
    """
    Records a confirmed mathematical connection between two formulas.
    Persisted as a CROSS_LINKED edge in Neo4j.
    Triggers the synthesis phase: Agent 105 must produce a unified FormulaDNA.
    """

    declaration_id: str = Field(default_factory=lambda: str(uuid_lib.uuid4()))
    uuid_a: str = Field(..., description="UUID of the first source formula")
    uuid_b: str = Field(..., description="UUID of the second source formula")
    tier_level: IsomorphismTier = Field(
        ...,
        description="The highest tier at which the isomorphism holds",
    )
    transformation_map: str = Field(
        ...,
        min_length=30,
        description=(
            "The explicit variable correspondence or structural mapping. "
            "For Tier 1: list the bijective substitutions. "
            "For Tier 2: describe which mathematical objects correspond. "
            "For Tier 3: describe which behavioral mechanisms correspond."
        ),
    )
    tier_analysis: TierAnalysis
    declared_by: str = Field(..., description="Agent ID that made the declaration")
    declared_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @model_validator(mode="after")
    def validate_tier_consistency(self) -> "IsomorphismDeclaration":
        """The declared tier must match the tier_analysis results."""
        analysis_match = self.tier_analysis.highest_tier_match
        if analysis_match is None:
            raise ValueError(
                "Cannot declare an isomorphism when tier_analysis shows no matches at any tier. "
                "Use reject_isomorphism instead."
            )
        if analysis_match != self.tier_level:
            raise ValueError(
                f"Declared tier {self.tier_level} does not match "
                f"highest tier in analysis ({analysis_match}). "
                "tier_level must equal the highest tier that matched."
            )
        return self


class RejectionRecord(BaseModel):
    """
    Records a formal rejection after exhaustive three-tier evaluation.
    Persisted as a REJECTED_ISOMORPHISM edge in Neo4j.

    This is a scientific finding, not a failure.
    The `suggested_bridging_formula` is a research directive for
    Evolutionary Gardeners (Agents 201-250).
    """

    rejection_id: str = Field(default_factory=lambda: str(uuid_lib.uuid4()))
    uuid_a: str
    uuid_b: str
    tier_analysis: TierAnalysis = Field(
        ...,
        description="Complete tier-by-tier analysis. All three tiers required.",
    )
    conclusion: str = Field(
        ...,
        min_length=40,
        description="Why synthesis is not possible for this specific pair.",
    )
    suggested_bridging_formula: str = Field(
        ...,
        min_length=30,
        description=(
            "The mathematical object that, if it existed in the Blackboard, "
            "would allow these two formulas to connect. "
            "This becomes a concrete research directive for Evolutionary Gardeners. "
            "If no bridging concept is conceivable, state the mathematical reason why."
        ),
    )
    failure_mode: RejectionFailureMode = Field(
        default=RejectionFailureMode.UNCLASSIFIED,
        description=(
            "Deterministic routing key for Evolutionary Gardeners. "
            "Agent 003 reads this field to dispatch the correct Agent 20X "
            "without LLM interpretation. Select the mode that best describes "
            "WHY the two formulas are mathematically incompatible: "
            "TEMPORAL_SCALE_MISMATCH (continuous rate vs discrete aggregate), "
            "DIMENSIONALITY_MISMATCH (univariate vs multivariate), "
            "STOCHASTIC_DETERMINISTIC_MISMATCH (probabilistic vs deterministic), "
            "MICRO_MACRO_MISMATCH (individual order vs aggregate market), "
            "INFORMATION_GEOMETRY_MISMATCH (strategic scalar vs information-theoretic distribution), "
            "UNCLASSIFIED (none of the above — human review required)."
        ),
    )
    rejected_by: str = Field(..., description="Agent ID")
    agent_version: str = Field(..., description="Agent version string, e.g. '0.1.0'")
    rejected_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @model_validator(mode="after")
    def validate_no_matches(self) -> "RejectionRecord":
        """A rejection is only valid if tier_analysis shows no matches at any tier."""
        if self.tier_analysis.any_match:
            raise ValueError(
                "Cannot reject an isomorphism when tier_analysis shows a match. "
                "If any tier matches, use declare_isomorphism and proceed to synthesis."
            )
        return self
