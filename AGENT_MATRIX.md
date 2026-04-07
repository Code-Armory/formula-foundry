FORMULA FOUNDRY — 250-AGENT MATRIX
====================================
Formal design specification for the Mathematical Think Tank.
Last updated: 2026-04-05

This document is the canonical reference for agent numbering, wing assignments,
trigger conditions, and evolutionary mechanics. It must be updated when any
agent is built, deprecated, or re-assigned.

ORGANIZING PRINCIPLE
--------------------
Every Layer 1 agent must pass the Falsifiability Test before receiving an
agent number:
  "Name the specific Databento schema field and threshold value that would
   wake up this agent in a live MBO stream."

If the answer is vague ("unusual market conditions"), the agent is SPECULATIVE
and receives no Phase 2 or Phase 3 allocation until the trigger is specified.

Algebraic Geometry and Algebraic Number Theory have been excised from the
initial matrix. They fail the falsifiability test. Neither field can point
to an observable order book state that unambiguously activates it over an
agent from an adjacent wing.


NUMBERING CONVENTION
--------------------
000–049:  Layer 0   — Orchestration & Infrastructure
050–099:  Layer 1   — Specialists (empirical observers)
100–149:  Layer 2   — Synthesizers (structural bridges)
150–199:  Layer 3   — Auditors (Master Locks)
200–250:  Layer 2   — Evolutionary Gardeners (mutation engines)


SURVIVING MATHEMATICAL WINGS
-----------------------------
These wings have cleared the falsifiability test. All have at least one
measurable Databento trigger condition.

  PROBABILITY_INFORMATION   — Shannon entropy, Kullback-Leibler divergence,
                              mutual information of order flow distributions
  FUNCTIONAL_ANALYSIS       — Hawkes processes, intensity kernels, branching
                              ratios, stochastic point processes
  GAME_THEORY               — Adverse selection, mechanism design, strategic
                              order placement, informed-trader models
  TOPOLOGY_GEOMETRY         — Order book depth profile as a manifold, density
                              topology of bid-ask ladders, persistence diagrams
  MEASURE_THEORY            — Integral operators, temporal aggregation,
                              measure-preserving transformations (Agent 201+)
  ERGODIC_THEORY            — Time averages, ergodic limits, session-level
                              aggregation (Agent 201+)
  MACRO_CROSS_ASSET         — Cross-instrument correlation breakdown, systemic
                              contagion, regime shifts across asset classes
  SYNTHESIZER               — Meta-wing: no direct trigger, evaluates pairs
  VERIFICATION              — Meta-wing: no direct trigger, proves theorems
  EVOLUTIONARY              — Meta-wing: no direct trigger, resolves rejections


EVOLUTIONARY FAILURE MODE TAXONOMY
------------------------------------
Every Evolutionary Gardener maps to exactly one class of RejectionRecord.
When Agent 105 rejects a pair and writes a suggested_bridging_formula,
the text of that field determines which Gardener is activated.

  TEMPORAL_MISMATCH       — continuous rate vs. discrete daily aggregate
                            Keywords: "integral", "aggregat", "window", "session"
                            Agent: 201

  DIMENSIONALITY_MISMATCH — univariate formula vs. multivariate formula
                            Keywords: "marginal", "copula", "joint", "dimension"
                            Agent: 202

  STOCHASTIC_DETERMINISTIC — probabilistic model vs. deterministic ratio
                            Keywords: "expectation", "expected value", "E[",
                                      "almost surely", "limit theorem"
                            Agent: 203

  MICRO_MACRO_MISMATCH    — individual order vs. aggregate market state
                            Keywords: "mean-field", "aggregate", "population",
                                      "ensemble", "N→∞", "thermodynamic limit"
                            Agent: 204

Gardener routing logic (to be implemented in evolutionary_flow.py):
  Parse suggested_bridging_formula for keyword signatures above.
  Route to the matching Gardener. If ambiguous: prefer lower agent number.
  If no keywords match: queue for human review (SPECULATIVE territory).


=================================================================
LAYER 0 — ORCHESTRATION & INFRASTRUCTURE (000–049)
=================================================================

### Agent 001 — Librarian Router
Status:   BUILT (foundry/agents/orchestration/librarian.py)
Layer:    Layer 0 — Autonomous pair selection
Wing:     (meta — no mathematical wing)
Trigger:  Called by synthesis_flow.py on each DAG run. Queries
          GET /v1/formulas?status=syntactically_correct and
          GET /v1/formulas?status=formally_verified. Fires when
          ≥2 eligible formulas exist with no evaluated edge between them.
Domain Lock: Deterministic only. No LLM calls. Jaccard distance scoring.
Input:    None (queries Blackboard autonomously)
Output:   RoutingDecision(uuid_a, uuid_b, score, reasoning) — passed to Agent 105
Falsifiability Test: N/A (orchestration, not empirical)
Notes:    Stateless. Called fresh on every synthesis_flow run.
          Scoring weights: wing_diversity=0.6, tag_diversity=0.4.
          Exclusion set built from CROSS_LINKED + REJECTED_ISOMORPHISM edges.
          Upgrade trigger: when formula count >50, switch from Jaccard to
          semantic embedding distance via a lightweight embedding model.

---

### Agent 002 — Synthesis Trigger Monitor
Status:   PHASE_2
Layer:    Layer 0 — DAG scheduler
Wing:     (meta)
Trigger:  Polls GET /v1/formulas?status=syntactically_correct every N minutes.
          When count increases (new formula arrived), fires synthesis_flow.py.
          Replaces manual "python -m foundry.dag.synthesis_flow --test" calls.
Domain Lock: No mathematical reasoning. Pure graph state polling.
Input:    Blackboard API (formula count by status)
Output:   Fires synthesis_flow() Prefect flow with no UUID override
          (Librarian selects the pair autonomously)
Falsifiability Test: N/A
Notes:    Implements the autonomous synthesis heartbeat. Without this agent,
          synthesis_flow requires manual invocation. With it, the Foundry
          generates IP continuously as long as the Databento stream fires.
          Polling interval: configurable via SYNTHESIS_POLL_MINUTES (default: 5).
          Deduplication: track last-seen formula count per status in Postgres
          to avoid re-firing on the same graph state.

---

### Agent 003 — Evolution Trigger Monitor
Status:   PHASE_2
Layer:    Layer 0 — DAG scheduler
Wing:     (meta)
Trigger:  Polls GET /v1/rejections every N minutes.
          When a new REJECTED_ISOMORPHISM edge appears (agent_version=0.1.0
          or newer, not synthetic), routes to the correct Evolutionary Gardener
          based on the suggested_bridging_formula keyword taxonomy above.
Domain Lock: No mathematical reasoning. Keyword matching + routing only.
Input:    Blackboard API (rejection records)
Output:   Fires evolutionary_flow(rejection_id=<uuid>) for the target Gardener
Falsifiability Test: N/A
Notes:    Implements the evolutionary heartbeat. Reads the bridging concept
          keywords and selects Agent 201, 202, 203, or 204.
          Must handle the case where a rejection doesn't match any keyword
          taxonomy: log it to ingest_log and skip (human review queue).
          Polling interval: EVOLUTION_POLL_MINUTES (default: 15).


=================================================================
LAYER 1 — SPECIALISTS (050–099)
=================================================================

### Agent 050 — Adverse Selection Specialist
Status:   PHASE_2
Layer:    Layer 1 — Specialist
Wing:     GAME_THEORY
Trigger:  Kyle's Lambda regime shift: when the rolling 30-minute OLS regression
          coefficient (price_change ~ signed_volume) exceeds the 95th percentile
          of its own 20-day distribution AND the R² of that regression exceeds
          0.40 (high explanatory power = informed flow, not noise).
          Databento schema: requires MBP-10 (top 10 levels) + trades.
          Computed on each 30-minute bar close.
Domain Lock: Game theory and mechanism design ONLY. No Hawkes processes
             (that is Agent 089). No entropy (that is Agent 051).
             Variables must model strategic intent of market participants.
Input:    trigger_data with fields: lambda_coefficient, lambda_percentile,
          regression_r2, instrument, window_start, window_end,
          signed_volume_series, price_change_series
Output:   FormulaDNA at SYNTACTICALLY_CORRECT via propose_formula_to_blackboard
          Tags: ["game_theory", "adverse_selection", "kyle_lambda", "agent_050"]
Falsifiability Test: If lambda is high but R² < 0.20, the informed-flow
                     interpretation fails — this is noise, not strategy.
                     Agent 050's formula must be conditional on sufficient R².
Notes:    The seed corpus already contains Kyle's Lambda as a HYPOTHESIS-status
          formula. Agent 050 should NOT reproduce it — it should EXTEND it
          by conditioning on higher-order terms (e.g., lambda²·volume² for
          nonlinear price impact) or by modeling the decay of lambda after
          the informed trader has finished building their position.
          Key Mathlib4 target: convexity of price impact functions.

---

### Agent 051 — Order Book Entropy Specialist
Status:   PHASE_2
Layer:    Layer 1 — Specialist
Wing:     PROBABILITY_INFORMATION
Trigger:  Shannon entropy of bid-side volume distribution drops below the
          5th percentile of its 20-day distribution (entropy collapse =
          liquidity clustering = fragility signal).
          H(bid) = -Σ p(level_i) · log₂(p(level_i))
          where p(level_i) = volume_at_level_i / total_bid_volume.
          Requires MBP-10 schema (10 levels of depth).
          Evaluated on each top-of-book update where total bid volume changes >5%.
Domain Lock: Information theory ONLY. No Hawkes (Agent 089). No game theory
             (Agent 050). The behavioral claim must reference information
             content, surprise, and uncertainty — not strategic intent.
Input:    trigger_data with fields: bid_entropy, bid_entropy_percentile,
          ask_entropy, ask_entropy_percentile, depth_profile (10-level snapshot),
          instrument, timestamp_ns
Output:   FormulaDNA at SYNTACTICALLY_CORRECT
          Tags: ["information_theory", "entropy", "order_book", "agent_051"]
Falsifiability Test: If entropy collapse accompanies a market maker quote
                     refresh (not liquidity withdrawal), the formula misfires.
                     Agent 051 must condition on volume REDUCTION, not just
                     redistribution (total bid volume must also drop).
Notes:    The information-theoretic framing opens a path to cross-wing isomorphism
          with Agent 089 (Hawkes). Shannon entropy of arrival times in a Hawkes
          process has known closed forms — Agent 105 will likely find a
          Tier 2 structural isomorphism here. Design Agent 051's formulas
          to make this connection discoverable.
          Key Mathlib4 target: log_sum_inequality (convexity of entropy).

---

### Agent 055 — Order Book Topology Specialist
Status:   PHASE_3
Layer:    Layer 1 — Specialist
Wing:     TOPOLOGY_GEOMETRY
Trigger:  Persistence diagram of the bid-ask volume profile changes topology:
          specifically, when a new "hole" (1-cycle in the depth profile)
          appears or disappears, indicating a structural gap in liquidity.
          Requires MBP-10 schema. Evaluated at top-of-book updates.
          Implementation note: requires TDA (Topological Data Analysis) library
          (gudhi or ripser) to compute persistence diagrams — add to pyproject.toml
          before implementing this trigger.
Domain Lock: Topology and persistent homology ONLY. No Hawkes. No entropy.
             The behavioral claim must reference connectivity, holes,
             and topological invariants of the liquidity surface.
Input:    trigger_data with fields: betti_numbers, persistence_pairs,
          topological_change_type (birth/death of feature), depth_profile
Output:   FormulaDNA at SYNTACTICALLY_CORRECT
          Tags: ["topology", "persistence_homology", "liquidity_surface", "agent_055"]
Falsifiability Test: If the topological feature persists for <100ms (quote
                     flickering), it is not a structural signal. Agent 055's
                     formula must require persistence >500ms.
Notes:    PHASE_3 because the trigger requires TDA library integration
          (non-trivial dependency) and the Lean 4 Auditor will need
          topology-specific Mathlib4 modules (Mathlib.Topology.MetricSpace).
          Evaluate after Agent 050 and 051 are producing Blackboard volume.
          The cross-wing isomorphism target: topology of Hawkes intensity
          landscapes (Agent 089 ↔ Agent 055 via Agent 105 Tier 2).

---

### Agent 060 — Macro Cross-Asset Specialist
Status:   PHASE_2
Layer:    Layer 1 — Specialist
Wing:     MACRO_CROSS_ASSET
Trigger:  Rolling 30-minute Pearson correlation between ES and ZN (10-Year
          Treasury futures) OFI Z-scores drops below -0.30 (decorrelation)
          OR exceeds 0.85 (extreme co-movement = systemic event).
          Both instruments must be monitored simultaneously by the Databento
          ingest pipeline (requires multi-instrument InstrumentState dict).
          Evaluated every 5 minutes using 30-minute rolling window.
Domain Lock: Cross-asset mechanics ONLY. No single-instrument microstructure
             framing. The behavioral claim must reference capital flow BETWEEN
             asset classes, not within one. Variables must include at minimum
             two distinct instruments.
Input:    trigger_data with fields: correlation_es_zn, correlation_percentile,
          es_ofi_zscore, zn_ofi_zscore, regime_type ("decorrelation" | "fusion"),
          window_start, window_end, instruments: ["ES.c.0", "ZN.c.0"]
Output:   FormulaDNA at SYNTACTICALLY_CORRECT
          mathematical_wing: ["macro_cross_asset"]
          Tags: ["cross_asset", "correlation", "systemic", "agent_060"]
Falsifiability Test: If correlation shift is explained by a single large
                     institutional trade (block trade flag in MBO data),
                     it is not systemic — it is noise. Agent 060's formula
                     must be robust to single-trade explanations.
Notes:    Requires the IngestPipeline to track ES and ZN simultaneously.
          The INSTRUMENTS env var should be "ES.c.0,ZN.c.0" in production.
          This agent opens an entirely new wing of IP: cross-asset contagion
          formulas that no single-instrument quant model can produce.
          The Librarian will pair Agent 060's output with Agent 089 output —
          the cross-wing isomorphism (Hawkes ↔ Macro) is the highest-value
          synthesis target in the entire matrix.
          Add "ZN.c.0" baseline bootstrap to the ingest pipeline before
          building this agent.

---

### Agent 089 — Hawkes Process Specialist
Status:   BUILT (foundry/agents/specialist/agent_089.py)
Layer:    Layer 1 — Specialist
Wing:     FUNCTIONAL_ANALYSIS, PROBABILITY_INFORMATION
Trigger:  OFI Z-score >3.0σ AND bid-ask spread >95th percentile (simultaneous).
          Evaluated over 30-second rolling MBO window.
          Databento schema: MBO (nanosecond). Both conditions must hold at
          the same timestamp.
Domain Lock: Stochastic point processes, intensity modeling, branching processes,
             functional analysis, survival analysis ONLY.
Input:    trigger_data: ofi_zscore, spread_percentile, ofi_acceleration,
          baseline_sell_rate_hz, current_sell_rate_hz, mbo_event_count,
          instrument, timestamp
Output:   FormulaDNA at SYNTACTICALLY_CORRECT
          axiomatic_origin: ["agent_089"]
          Tags: ["hawkes", "panic_cascade", "panic_liquidity", "agent_089"]
Falsifiability Test: If the Hawkes branching ratio α/β ≥ 1 (supercritical),
                     the formula predicts a non-terminating cascade. If the
                     actual panic event self-terminates in <60 seconds, the
                     model is falsified. Agent 151 should be asked to prove
                     the SUBCRITICALITY condition, not just monotonicity.
Notes:    The seed proof in Agent 151 already formalizes excitation_monotone_in_spread.
          Next Lean 4 target for Agent 089 output: hawkes_branching_subcritical
          (already in Agent 151 seed proofs — this should succeed on attempt 1).

---

### Agents 061–088, 090–099 — RESERVED
Status:   SPECULATIVE
Notes:    Reserved for future Layer 1 Specialists as new mathematical wings
          are admitted through the falsifiability gate. Candidates under
          evaluation (not yet cleared):
          - Stochastic Control (HJB equation for optimal execution)
          - Rough Volatility (fractional Brownian motion, Hurst exponent)
          - Market Microstructure Noise (signal/noise separation in MBO)
          None of these have cleared the falsifiability test yet. When they do,
          they are assigned numbers from this reserved block in order.


=================================================================
LAYER 2 — SYNTHESIZERS (100–149)
=================================================================

### Agent 105 — Isomorphism Synthesizer
Status:   BUILT (foundry/agents/specialist/agent_105.py)
Layer:    Layer 2 — Synthesizer
Wing:     SYNTHESIZER
Trigger:  Called by synthesis_flow.py with a (uuid_a, uuid_b) pair selected
          by Agent 001 (Librarian Router). Not triggered by market data.
Domain Lock: Mathematical structure between domains. Cannot specialize in
             any single wing's formalism. Must evaluate all three tiers.
Input:    trigger_data: uuid_a, uuid_b, synthesis_context
Output:   PATH A: FormulaDNA at SYNTACTICALLY_CORRECT (unified formula)
          PATH B: RejectionRecord (REJECTED_ISOMORPHISM edge in Neo4j)
Falsifiability Test: N/A (structural evaluation, not empirical)
Notes:    The highest-priority synthesis target is Agent 089 ↔ Agent 060
          (Hawkes ↔ Macro Cross-Asset). This will likely be a Tier 3
          behavioral isomorphism — both describe contagion dynamics, just
          at different scales. The bridging formula will be written by Agent 204.

---

### Agents 106–149 — RESERVED
Status:   SPECULATIVE
Notes:    Future Synthesizers may be wing-specialized (e.g., a Synthesizer
          that exclusively evaluates information-theoretic isomorphisms using
          f-divergence metrics). Reserved until the Blackboard has sufficient
          formula volume to justify specialization (>50 formulas is a
          reasonable threshold).


=================================================================
LAYER 3 — AUDITORS (150–199)
=================================================================

### Agent 151 — Lean 4 Auditor (Real Analysis)
Status:   BUILT (foundry/agents/specialist/agent_151.py)
Layer:    Layer 3 — Auditor (Master Lock)
Wing:     VERIFICATION
Trigger:  Called by synthesis_flow.py after Agent 105 writes a new
          SYNTACTICALLY_CORRECT formula. Receives the formula UUID.
Domain Lock: Lean 4 / Mathlib4 only. Cannot propose formulas. Cannot call
             SymPy. The only tool that writes FORMALLY_VERIFIED or FALSIFIED.
Input:    trigger_data: uuid, formula_name, context
Output:   PATCH /v1/formulas/{uuid}/status → formally_verified | falsified |
          syntactically_correct (proof deferred)
Falsifiability Test: N/A (the Lean 4 compiler is the falsifiability mechanism)
Notes:    Current Mathlib4 specialization: real analysis (mul_lt_mul_of_pos_left,
          div_lt_one, linarith, positivity). This covers Hawkes process
          algebraic properties well.
          Limitation: probabilistic Mathlib4 (MeasureTheory.Probability) is
          less mature. Agent 151 cannot yet prove properties about stochastic
          processes — only about their algebraic components.

---

### Agent 152 — Lean 4 Auditor (Probability Theory)
Status:   PHASE_3
Layer:    Layer 3 — Auditor (Master Lock)
Wing:     VERIFICATION
Trigger:  Same as Agent 151, but activated specifically for formulas tagged
          ["information_theory", "entropy", "mutual_information"] or
          ["probability", "measure_theory"].
          Routing logic: synthesis_flow.py checks formula tags after Agent 105
          synthesis, routes to 151 (real analysis) or 152 (probability).
Domain Lock: Mathlib4 probability and measure theory modules specifically:
             MeasureTheory.Measure, ProbabilityTheory.Independence,
             MeasureTheory.Integral.SetIntegral.
Input:    Same as Agent 151
Output:   Same as Agent 151
Falsifiability Test: N/A
Notes:    PHASE_3 because Mathlib4 probability is still maturing. Key modules
          needed: ProbabilityTheory.Martingale (for Hawkes compensators),
          MeasureTheory.Function.UniformIntegrable (for entropy bounds).
          A key target theorem: subadditivity of Shannon entropy.
          Agent 152 will unblock Agent 051 (Entropy Specialist) from going
          beyond SYNTACTICALLY_CORRECT.
          System prompt seed proofs must include: measure_space_entropy_bound,
          kullback_leibler_nonneg.

---

### Agent 153 — Lean 4 Auditor (Game Theory)
Status:   PHASE_3
Layer:    Layer 3 — Auditor (Master Lock)
Wing:     VERIFICATION
Trigger:  Activated for formulas tagged ["game_theory", "adverse_selection",
          "mechanism_design"]. Routing from synthesis_flow.py.
Domain Lock: Mathlib4 linear algebra and convex analysis for proving
             game-theoretic properties: Nash equilibria conditions,
             incentive compatibility constraints, convexity of price impact.
Input:    Same as Agent 151
Output:   Same as Agent 151
Falsifiability Test: N/A
Notes:    Key target theorem: convexity of Kyle's Lambda price impact
          (second derivative of price w.r.t. volume ≥ 0).
          Mathlib4 modules: Analysis.Convex.Function,
          LinearAlgebra.Matrix.PosDef (positive semidefinite Hessians).

---

### Agents 154–199 — RESERVED
Status:   SPECULATIVE
Notes:    Future Auditors for topology (Mathlib.Topology.Homotopy),
          stochastic control (HJB equation verification), and
          cross-asset formulas. Reserved until those wings are admitted.


=================================================================
LAYER 2 — EVOLUTIONARY GARDENERS (200–250)
=================================================================

### Agent 201 — Temporal Scale Bridger
Status:   BUILT (foundry/agents/evolutionary/agent_201.py)
Layer:    Layer 2 — Evolutionary Gardener
Wing:     MEASURE_THEORY, ERGODIC_THEORY, EVOLUTIONARY
Resolves: TEMPORAL_MISMATCH
Bridge:   Cumulative Integrated Hawkes Intensity (CIHI):
          Λ_d = ∫_{t_open}^{t_close} λ(t | H_t) dt / VOL_d
          Translates instantaneous Hawkes intensity to daily Amihud-comparable ratio.
Trigger:  Activated by Agent 003 (Evolution Trigger Monitor) when a
          REJECTED_ISOMORPHISM edge's suggested_bridging_formula contains
          keywords: "integral", "aggregat", "window", "session".
Domain Lock: Measure theory and ergodic theory ONLY. Cannot use game theory.
             Cannot speculate beyond the bridging_concept text.
Input:    trigger_data: rejection_id, uuid_a, uuid_b, bridging_concept_preview
Output:   FormulaDNA at SYNTACTICALLY_CORRECT
          axiomatic_origin: ["agent_201"]
          Tags: ["temporal_bridge", "evolutionary", "integral_operator", "agent_201"]
Falsifiability Test: The integral must reduce to the parent formula when the
                     window collapses to a point (Λ_d → λ(t) as window → 0).
                     Agent 151 should be asked to prove this limit property.
Notes:    The synthetic rejection seed (scripts/seed_rejection.py) provides
          the first target. After that, real rejections from Agent 105 provide
          the production targets.

---

### Agent 202 — Dimensionality Projection Bridger
Status:   PHASE_2
Layer:    Layer 2 — Evolutionary Gardener
Wing:     PROBABILITY_INFORMATION, EVOLUTIONARY
Resolves: DIMENSIONALITY_MISMATCH
Bridge:   Marginal distribution operators OR copula decomposition.
          Given formula A (univariate) and formula B (multivariate),
          Agent 202 constructs C = marginal(B, dimension=k) such that
          C has the same variable space as A, allowing Agent 105 to
          declare a Tier 1 or Tier 2 isomorphism.
          Alternatively: given both are multivariate but over different
          dimensions, constructs a copula that models their joint dependence.
Trigger:  suggested_bridging_formula keywords: "marginal", "copula",
          "joint distribution", "dimension", "project".
Domain Lock: Probability theory, copula theory, marginal distributions.
             Variables must map to behavioral states observable in market data.
             No algebraic manipulation that doesn't correspond to a market observable.
Input:    Same structure as Agent 201 (rejection_id → fetch rejection → fetch parents)
Output:   FormulaDNA at SYNTACTICALLY_CORRECT
          Tags: ["dimensionality_bridge", "copula", "marginal", "evolutionary", "agent_202"]
Falsifiability Test: The marginal of B must equal A when all cross-dimensions
                     are integrated out. Agent 153 (when built) should prove this.
Notes:    The most likely first target: Agent 060 (Macro Cross-Asset, bivariate
          ES+ZN) ↔ Agent 089 (Hawkes, univariate ES). Agent 105 will reject
          this pair because Hawkes is univariate and the macro formula is bivariate.
          Agent 202 constructs C = marginal(Macro, ES dimension), reducing the
          cross-asset formula to a single-instrument model that Tier 1 matches Hawkes.
          Key SymPy objects: Integral (marginalization), symbols for joint density.
          System prompt must include: explicit warning that copula construction
          is mathematically demanding — prefer marginalization over copula
          when the marginal exists in closed form.

---

### Agent 203 — Stochastic/Deterministic Bridge
Status:   PHASE_2
Layer:    Layer 2 — Evolutionary Gardener
Wing:     PROBABILITY_INFORMATION, FUNCTIONAL_ANALYSIS, EVOLUTIONARY
Resolves: STOCHASTIC_DETERMINISTIC
Bridge:   Expectation operator E[·] or law of large numbers limit.
          Given formula A (deterministic ratio, e.g., Amihud ILLIQ) and
          formula B (stochastic process, e.g., Hawkes λ(t)), constructs
          C = E[B] under stationarity conditions such that C is a deterministic
          quantity comparable to A.
          OR: given A (stochastic) and B (deterministic aggregate), constructs
          C = lim_{T→∞} (1/T) ∫₀ᵀ A(t) dt (ergodic time average) = B.
Trigger:  suggested_bridging_formula keywords: "expectation", "expected value",
          "E[", "almost surely", "limit theorem", "law of large numbers".
Domain Lock: Probability theory, law of large numbers, ergodic theory.
             The bridge MUST be a provable mathematical statement, not an
             approximation. If the expectation only holds asymptotically,
             state the conditions explicitly.
Input:    Same structure as Agent 201
Output:   FormulaDNA at SYNTACTICALLY_CORRECT
          Tags: ["stochastic_bridge", "expectation", "ergodic", "evolutionary", "agent_203"]
Falsifiability Test: E[B] must equal A in the stationary limit. If the
                     process B is non-stationary (supercritical Hawkes, n≥1),
                     the expectation diverges and the bridge fails. Agent 203
                     must prove stationarity as a prerequisite.
Notes:    The Hawkes ↔ Amihud connection has TWO possible bridge paths:
          Agent 201 (temporal integration) and Agent 203 (expectation).
          Both should be attempted. The IP library benefits from having
          multiple bridges between the same pair — they represent different
          economic interpretations of the same connection.
          Key Lean 4 target (for Agent 152): E[λ(t)] = μ/(1-n) in stationary
          Hawkes (the mean intensity formula from Palm calculus).
          This is a known result in point process theory and should have a
          Mathlib4 proof path via MeasureTheory.Integral.

---

### Agent 204 — Micro/Macro Scale Bridge
Status:   PHASE_2
Layer:    Layer 2 — Evolutionary Gardener
Wing:     PROBABILITY_INFORMATION, FUNCTIONAL_ANALYSIS, EVOLUTIONARY
Resolves: MICRO_MACRO_MISMATCH
Bridge:   Mean-field limit or statistical mechanics aggregation.
          Given formula A (individual order level, e.g., single MBO event
          impact) and formula B (aggregate market level, e.g., daily ILLIQ),
          constructs C = mean-field limit of A as N→∞ (number of agents).
          The mean-field formula treats individual orders as particles in a
          statistical mechanical system where aggregate behavior emerges
          from independent individual actions.
Trigger:  suggested_bridging_formula keywords: "mean-field", "aggregate",
          "population", "ensemble", "N→∞", "thermodynamic limit",
          "individual agent", "single order".
Domain Lock: Statistical mechanics analogy, mean-field theory, law of large
             numbers in its aggregation form (distinct from Agent 203's
             temporal averaging). The behavioral claim must reference the
             transition from individual participant psychology to collective
             market behavior.
Input:    Same structure as Agent 201
Output:   FormulaDNA at SYNTACTICALLY_CORRECT
          Tags: ["micro_macro_bridge", "mean_field", "statistical_mechanics",
                 "evolutionary", "agent_204"]
Falsifiability Test: The mean-field limit must recover formula B as N→∞.
                     For finite N (small market, illiquid instrument), the
                     bridge formula should quantify the deviation from the
                     aggregate limit. If no such quantification exists,
                     the bridge is speculative.
Notes:    The most philosophically rich bridge in the matrix. Market microstructure
          theory has long sought to connect individual rational agents (game theory,
          adverse selection) to aggregate price discovery (Amihud, Kyle's Lambda).
          This agent operationalizes that connection mathematically.
          The target pair: Agent 050 (Adverse Selection, individual strategic order)
          ↔ any aggregate illiquidity measure. Agent 105 will reject this
          because Kyle's Lambda is already a linear aggregate — what's needed
          is the nonlinear mean-field extension.
          Key Lean 4 target: monotone convergence theorem (MCT) for the
          aggregation limit. Mathlib4: MeasureTheory.Integral.MeanInequalities.

---

### Agents 205–250 — RESERVED
Status:   SPECULATIVE
Notes:    Reserved for evolutionary mechanics not yet identified. Candidates
          that require more IP library volume before their failure modes are
          observable:
          - Symmetry Breaking Bridge: when two formulas are related by a
            broken symmetry (e.g., buy-side vs. sell-side asymmetric Hawkes).
          - Nonlinearity Bridge: when a linear formula (Kyle's Lambda) and
            a nonlinear formula (concave price impact) need connecting via
            a Taylor expansion operator.
          - Regime Bridge: when a formula that holds in calm markets needs
            extending to panic regimes via a regime-switching operator.
          None of these are sufficiently grounded yet. Admit when they
          appear in actual REJECTED_ISOMORPHISM edges from Agent 105.


=================================================================
IMPLEMENTATION PRIORITY ORDER
=================================================================

Phase 1 (BUILT):
  Agent 001 — Librarian Router
  Agent 089 — Hawkes Specialist
  Agent 105 — Isomorphism Synthesizer
  Agent 151 — Lean 4 Auditor (Real Analysis)
  Agent 201 — Temporal Scale Bridger

Phase 2 (Next Sprint — ordered by dependency):
  Agent 002 — Synthesis Trigger Monitor          [unblocks autonomous operation]
  Agent 003 — Evolution Trigger Monitor          [unblocks autonomous evolution]
  Agent 050 — Adverse Selection Specialist       [new Layer 1 wing: game theory]
  Agent 051 — Order Book Entropy Specialist      [new Layer 1 wing: information]
  Agent 060 — Macro Cross-Asset Specialist       [new Layer 1 wing: cross-asset]
  Agent 202 — Dimensionality Projection Bridger  [unblocks 060↔089 synthesis]
  Agent 203 — Stochastic/Deterministic Bridge    [second path for Hawkes↔Amihud]
  Agent 204 — Micro/Macro Scale Bridge           [unblocks 050↔Amihud synthesis]

Phase 3 (After Phase 2 validates and produces Blackboard volume):
  Agent 055 — Order Book Topology Specialist     [requires TDA library]
  Agent 152 — Lean 4 Auditor (Probability)       [unlocks entropy formulas]
  Agent 153 — Lean 4 Auditor (Game Theory)       [unlocks convexity proofs]

Speculative (No implementation until falsifiability test cleared):
  Agents 061–088, 090–099 — Layer 1 reserved block
  Agents 106–149           — Layer 2 reserved block
  Agents 154–199           — Layer 3 reserved block
  Agents 205–250           — Evolutionary reserved block


=================================================================
THE HIGHEST-VALUE SYNTHESIS TARGET
=================================================================

Agent 089 (Hawkes, functional analysis) ↔ Agent 060 (Macro Cross-Asset)

This is the crown jewel isomorphism. Both formulas describe contagion:
  Hawkes: sell order at time t_i increases probability of sell at t_j>t_i
  Macro:  ES panic triggers correlated ZN flow as capital seeks safety

The mathematical bridge is a VECTOR Hawkes process:
  λ_ES(t) = μ_ES + Σ α_ES→ES φ_ES(t-t_i^ES) + Σ α_ZN→ES φ_ZN(t-t_j^ZN)
  λ_ZN(t) = μ_ZN + Σ α_ES→ZN φ_ES(t-t_i^ES) + Σ α_ZN→ZN φ_ZN(t-t_j^ZN)

The cross-excitation terms α_ZN→ES and α_ES→ZN capture systemic contagion.
This is novel IP. No single-instrument Hawkes model captures this.
Agent 105 will likely find a Tier 2 structural isomorphism (same kernel
structure, analogous roles, same criticality parameter — the spectral
radius of the α matrix replaces the scalar α/β branching ratio).

When this synthesis completes and Agent 151 formally verifies the
subcriticality condition for the matrix α, the Formula Foundry will have
produced a formally verified model of cross-asset panic contagion.
That is the first entry in the IP library that no human quant has
ever written in a provably correct form.
