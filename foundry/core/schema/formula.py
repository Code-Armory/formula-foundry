"""
Formula DNA — the canonical IP object of the Formula Foundry.

Every contribution to the Blackboard is an instance of FormulaDNA.
This schema enforces the layered authority model:
  Layer 1 (Specialists)  → can write HYPOTHESIS or SYNTACTICALLY_CORRECT (SymPy gate)
  Layer 2 (Synthesizers) → can write MERGED, CROSS_LINKED
  Layer 3 (Auditors)     → hold MASTER_LOCK, issue FORMALLY_VERIFIED
"""

from __future__ import annotations

import uuid as uuid_lib
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------


class ProofStatus(str, Enum):
    """Lifecycle state of a formula in the verification pipeline."""
    HYPOTHESIS = "hypothesis"
    SYNTACTICALLY_CORRECT = "syntactically_correct"
    FORMALLY_VERIFIED = "formally_verified"
    FALSIFIED = "falsified"
    ARCHIVED = "archived"


class MathematicalWing(str, Enum):
    """Which wing of the Think Tank birthed this formula."""
    ALGEBRAIC_NUMBER_THEORY = "algebraic_number_theory"
    ALGEBRAIC_GEOMETRY = "algebraic_geometry"
    GAME_THEORY = "game_theory"
    FUNCTIONAL_ANALYSIS = "functional_analysis"
    TOPOLOGY_GEOMETRY = "topology_geometry"
    PROBABILITY_INFORMATION = "probability_information"
    MEASURE_THEORY = "measure_theory"       # Added: integral operators, aggregation
    ERGODIC_THEORY = "ergodic_theory"       # Added: temporal scale bridging, time averages
    SYNTHESIZER = "synthesizer"
    VERIFICATION = "verification"
    EVOLUTIONARY = "evolutionary"


class AgentLayer(str, Enum):
    LAYER_1 = "layer1"   # Specialists — propose
    LAYER_2 = "layer2"   # Synthesizers — merge, cross-link; Evolutionary — bridge
    LAYER_3 = "layer3"   # Auditors — formally verify
    SYSTEM = "system"    # Seed corpus injection


# ---------------------------------------------------------------------------
# Sub-models
# ---------------------------------------------------------------------------


class BehavioralMapping(BaseModel):
    """
    Maps a single mathematical variable to a psychological/behavioral state.

    This is the semantic layer that separates a Formula Foundry formula
    from a generic quant model. Every variable must have a human
    interpretation grounded in market participant psychology.
    """
    variable: str = Field(
        ...,
        description="The variable name as it appears in the formula, e.g. 'lambda_0'",
    )
    latex_symbol: str = Field(
        ...,
        description="Rendered LaTeX symbol, e.g. r'\\lambda_0'",
    )
    psychological_state: str = Field(
        ...,
        description="Human-readable behavioral interpretation",
    )
    measurement_proxy: str = Field(
        ...,
        description="How this variable is measured from market data",
    )
    unit: Optional[str] = Field(None, description="Unit of measurement")
    empirical_range: Optional[Dict[str, float]] = Field(
        None,
        description="Observed min/max in historical data",
    )


class EmpiricalTrace(BaseModel):
    """Links a formula to the specific historical data window where it was observed."""
    trace_id: str = Field(default_factory=lambda: str(uuid_lib.uuid4()))
    data_source: str = Field(..., description="e.g. 'databento', 'polygon', 'tardis'")
    instrument: str = Field(..., description="e.g. 'ES', 'SPY', 'BTC-USD'")
    schema_type: str = Field(..., description="Databento schema, e.g. 'mbo', 'mbp-10'")
    time_range_start: datetime
    time_range_end: datetime
    trigger_conditions: Dict[str, Any] = Field(...)
    validation_r2: Optional[float] = Field(None, ge=0.0, le=1.0)
    validation_mse: Optional[float] = Field(None, ge=0.0)
    sample_count: int = Field(..., gt=0)
    notes: Optional[str] = None


class VerificationAttempt(BaseModel):
    """Records one complete pass through the CoMT pipeline."""
    attempt_id: str = Field(default_factory=lambda: str(uuid_lib.uuid4()))
    agent_id: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    iteration: int = Field(1, ge=1)
    sympy_valid: Optional[bool] = None
    sympy_expression: Optional[str] = None
    sympy_error: Optional[str] = None
    lean4_attempted: bool = False
    lean4_valid: Optional[bool] = None
    lean4_encoding: Optional[str] = None
    lean4_error: Optional[str] = None
    refinement_prompt: Optional[str] = None


# ---------------------------------------------------------------------------
# The Formula DNA
# ---------------------------------------------------------------------------


class FormulaDNA(BaseModel):
    """
    The canonical IP object. Every formula in the Foundry library is one of these.

    Lifecycle:
      HYPOTHESIS → SYNTACTICALLY_CORRECT → FORMALLY_VERIFIED
                                        ↘ FALSIFIED → ARCHIVED
    """

    # Identity
    uuid: str = Field(default_factory=lambda: str(uuid_lib.uuid4()))
    version: int = Field(1, ge=1)
    parent_uuid: Optional[str] = Field(None)

    # Human-readable identity
    name: Optional[str] = Field(None)
    description: Optional[str] = Field(None)

    # Mathematical expression — populated progressively through CoMT pipeline
    symbolic_expression_latex: str = Field(...)
    symbolic_expression_sympy: Optional[str] = Field(None)
    lean4_encoding: Optional[str] = Field(None)

    # Behavioral semantic layer
    behavioral_claim: str = Field(...)
    behavioral_mappings: List[BehavioralMapping] = Field(..., min_length=1)

    # Provenance
    axiomatic_origin: List[str] = Field(..., min_length=1)
    mathematical_wing: List[MathematicalWing] = Field(..., min_length=1)

    # Status and verification pipeline
    proof_status: ProofStatus = Field(ProofStatus.HYPOTHESIS)
    verification_attempts: List[VerificationAttempt] = Field(default_factory=list)

    # Quality signals
    resilience_score: Optional[float] = Field(None, ge=0.0, le=1.0)

    # Evidence
    empirical_traces: List[EmpiricalTrace] = Field(default_factory=list)

    # Cross-domain links (set by Synthesizer agents)
    isomorphic_to: List[str] = Field(default_factory=list)

    # Metadata
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    tags: List[str] = Field(default_factory=list)

    # ---------------------------------------------------------------------------
    # Validators
    # ---------------------------------------------------------------------------

    @model_validator(mode="after")
    def validate_pipeline_ordering(self) -> "FormulaDNA":
        """Enforce the CoMT pipeline dependency chain."""
        if self.lean4_encoding and not self.symbolic_expression_sympy:
            raise ValueError(
                "lean4_encoding requires symbolic_expression_sympy to be populated first. "
                "The SymPy validation gate must pass before Lean 4 encoding is stored."
            )
        if self.proof_status == ProofStatus.FORMALLY_VERIFIED and not self.lean4_encoding:
            raise ValueError(
                "A formula cannot be FORMALLY_VERIFIED without a lean4_encoding. "
                "The Lean 4 Auditor must provide the proof."
            )
        return self

    @field_validator("version")
    @classmethod
    def version_must_be_positive(cls, v: int) -> int:
        if v < 1:
            raise ValueError("version must be >= 1")
        return v

    # ---------------------------------------------------------------------------
    # Domain methods
    # ---------------------------------------------------------------------------

    def evolve(
        self,
        new_expression_latex: str,
        contributing_agent_id: str,
        behavioral_claim: Optional[str] = None,
    ) -> "FormulaDNA":
        """Produce a new version of this formula, preserving full lineage."""
        return FormulaDNA(
            parent_uuid=self.uuid,
            version=self.version + 1,
            symbolic_expression_latex=new_expression_latex,
            behavioral_claim=behavioral_claim or self.behavioral_claim,
            behavioral_mappings=[m.model_copy() for m in self.behavioral_mappings],
            axiomatic_origin=sorted(set(self.axiomatic_origin + [contributing_agent_id])),
            mathematical_wing=list(self.mathematical_wing),
            proof_status=ProofStatus.HYPOTHESIS,
            tags=list(self.tags),
            name=self.name,
            description=self.description,
        )

    def add_verification_attempt(self, attempt: VerificationAttempt) -> "FormulaDNA":
        return self.model_copy(
            update={
                "verification_attempts": self.verification_attempts + [attempt],
                "updated_at": datetime.now(timezone.utc),
            }
        )

    def latest_attempt(self) -> Optional[VerificationAttempt]:
        if not self.verification_attempts:
            return None
        return max(self.verification_attempts, key=lambda a: a.timestamp)

    class Config:
        use_enum_values = True
