"""
Librarian Router — Layer 0 Orchestration

The only component in the system that operates above the DAG.
Makes curation decisions: which formula pairs to route to Agent 105.

Design: semantic embedding distance (primary) + structural Jaccard (fallback).
  Primary path:  OPENAI_API_KEY present → text-embedding-3-small via httpx.
  Fallback path: OPENAI_API_KEY absent or API failure → original Jaccard scoring.
  No new dependencies — httpx is already in the stack.

Scoring function (synthesis potential):
  Primary (embedding path):
    wing_diversity   (50%): Jaccard distance between mathematical_wing sets.
    semantic_diversity(35%): 1 − cosine_similarity(embed(behavioral_claim_a),
                                                    embed(behavioral_claim_b)).
      Embeddings batch all N eligible formulas in a single API call (N ≤ 2048).
      Pure-Python cosine similarity — no numpy required.
    tag_diversity    (15%): Jaccard distance between meaningful tag sets.

  Fallback (Jaccard path):
    wing_diversity (60%) + tag_diversity (40%) — identical to prior behaviour.

  In both paths:
    1.0 = completely different (maximum synthesis potential).
    0.0 = identical (same domain/claim — low synthesis value).

The semantic component prevents pairing-loop noise: two formulas sharing the
same five tags (e.g. two quadratic impact formulas both tagged game_theory /
adverse_selection / kyle_lambda / nonlinear_impact / agent_050) that would
score 0.0 on tag_diversity are now differentiated by their behavioral_claim
embeddings.

Exclusion rules (hard constraints applied before scoring):
  - Self-pairs (uuid == uuid)
  - Pairs with existing CROSS_LINKED edge (already synthesized)
  - Pairs with existing REJECTED_ISOMORPHISM edge (already evaluated)

Eligible formula statuses:
  - syntactically_correct: passed SymPy gate
  - formally_verified: passed Lean 4 gate (prime synthesis material)

Environment:
  OPENAI_API_KEY — required for embedding path. Graceful fallback if absent.
"""

from __future__ import annotations

import logging
import math
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from itertools import combinations
from typing import Any, Dict, FrozenSet, List, Optional, Set, Tuple

import httpx

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Scoring weights — primary (embedding) path
# ---------------------------------------------------------------------------
_WING_DIVERSITY_WEIGHT: float = 0.50
_SEMANTIC_DIVERSITY_WEIGHT: float = 0.35
_TAG_DIVERSITY_WEIGHT: float = 0.15

# Scoring weights — Jaccard fallback (OPENAI_API_KEY absent / API failure)
_WING_DIVERSITY_WEIGHT_FALLBACK: float = 0.6
_TAG_DIVERSITY_WEIGHT_FALLBACK: float = 0.4

# OpenAI embedding model — cheap, fast, 1536-dim
_EMBEDDING_MODEL: str = "text-embedding-3-small"
_OPENAI_EMBEDDINGS_URL: str = "https://api.openai.com/v1/embeddings"

_NOISE_TAGS: frozenset = frozenset({
    "seed_corpus", "agent_089", "agent_105", "agent_151", "agent_201",
    "unified_formula", "hawkes", "panic_liquidity", "temporal_bridge",
    "evolutionary", "integral_operator",
})

_ELIGIBLE_STATUSES = ("syntactically_correct", "formally_verified")


@dataclass
class RoutingDecision:
    uuid_a: str
    uuid_b: str
    formula_a_name: str
    formula_b_name: str
    score: float
    wing_diversity: float
    tag_diversity: float
    semantic_diversity: float      # 0.0 when Jaccard fallback is active
    reasoning: str
    candidates_evaluated: int
    pairs_excluded: int
    decided_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class LibrarianRouter:
    """
    Layer 0 — Orchestration Router.
    Stateless: each call to select_next_pair() reflects current graph state.
    """

    def __init__(self, blackboard_api_url: str) -> None:
        self._api_url = blackboard_api_url.rstrip("/")

    async def select_next_pair(self) -> Optional[RoutingDecision]:
        eligible = await self._fetch_eligible_formulas()
        if len(eligible) < 2:
            logger.info("[Librarian] Not enough eligible formulas (%d, need ≥2).", len(eligible))
            return None

        # Embed all eligible formulas in one API call before scoring.
        # Falls back to empty dict → Jaccard scoring if API unavailable.
        embeddings = await self._embed_formulas(eligible)

        exclusions = await self._build_exclusion_set(eligible)
        decision = self._select_best_pair(eligible, exclusions, embeddings)

        if decision is None:
            total = sum(1 for _ in combinations(eligible, 2))
            logger.info("[Librarian] All %d pairs already evaluated.", total)
            return None

        logger.info(
            "[Librarian] Selected: '%s' (%s) ↔ '%s' (%s) | score=%.3f | %s",
            decision.formula_a_name, decision.uuid_a[:8],
            decision.formula_b_name, decision.uuid_b[:8],
            decision.score,
            "embedding" if embeddings else "jaccard-fallback",
        )
        return decision

    # ------------------------------------------------------------------
    # Embedding (primary scoring path)
    # ------------------------------------------------------------------

    async def _embed_formulas(
        self, formulas: List[Dict[str, Any]]
    ) -> Dict[str, List[float]]:
        """
        Fetch text-embedding-3-small vectors for all eligible formulas.

        Input text per formula: behavioral_claim (richest semantic field),
        falling back to name, then uuid[:8] if absent.

        All N formulas are sent in a single batched API request (OpenAI
        accepts up to 2048 inputs). Returns {uuid: vector} on success,
        empty dict on failure — callers treat empty dict as Jaccard fallback.
        """
        api_key = os.environ.get("OPENAI_API_KEY", "")
        if not api_key:
            logger.info(
                "[Librarian] OPENAI_API_KEY not set. Jaccard fallback active."
            )
            return {}

        texts: List[Tuple[str, str]] = [
            (
                f["uuid"],
                f.get("behavioral_claim") or f.get("name") or f["uuid"][:8],
            )
            for f in formulas
        ]

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(
                    _OPENAI_EMBEDDINGS_URL,
                    headers={
                        "Authorization": f"Bearer {api_key}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": _EMBEDDING_MODEL,
                        "input": [text for _, text in texts],
                        "encoding_format": "float",
                    },
                )
                resp.raise_for_status()
                data = resp.json()
        except Exception as exc:
            logger.error(
                "[Librarian] Embedding API failed: %s. Jaccard fallback active.", exc
            )
            return {}

        embeddings: Dict[str, List[float]] = {}
        for (uuid, _), item in zip(texts, data["data"]):
            embeddings[uuid] = item["embedding"]

        logger.info(
            "[Librarian] Embedded %d formulas via %s.", len(embeddings), _EMBEDDING_MODEL
        )
        return embeddings

    @staticmethod
    def _cosine_similarity(vec_a: List[float], vec_b: List[float]) -> float:
        """Pure-Python cosine similarity. No numpy required."""
        dot = sum(a * b for a, b in zip(vec_a, vec_b))
        norm_a = math.sqrt(sum(a * a for a in vec_a))
        norm_b = math.sqrt(sum(b * b for b in vec_b))
        if norm_a == 0.0 or norm_b == 0.0:
            return 0.0
        return dot / (norm_a * norm_b)

    # ------------------------------------------------------------------
    # Blackboard queries (unchanged)
    # ------------------------------------------------------------------

    async def _fetch_eligible_formulas(self) -> List[Dict[str, Any]]:
        collected: List[Dict[str, Any]] = []
        async with httpx.AsyncClient(timeout=30.0) as client:
            for status in _ELIGIBLE_STATUSES:
                try:
                    resp = await client.get(
                        f"{self._api_url}/v1/formulas",
                        params={"status": status},
                    )
                    if resp.status_code == 400:
                        continue
                    resp.raise_for_status()
                    collected.extend(resp.json())
                except (httpx.HTTPStatusError, httpx.RequestError) as exc:
                    logger.error("[Librarian] Failed to fetch status=%s: %s", status, exc)

        seen: Set[str] = set()
        eligible: List[Dict[str, Any]] = []
        for formula in collected:
            if formula["uuid"] not in seen:
                seen.add(formula["uuid"])
                eligible.append(formula)

        logger.info(
            "[Librarian] Eligible formulas: %d (%s)",
            len(eligible),
            ", ".join(f.get("name", f["uuid"][:8]) for f in eligible),
        )
        return eligible

    async def _build_exclusion_set(
        self, formulas: List[Dict[str, Any]]
    ) -> Set[FrozenSet[str]]:
        excluded: Set[FrozenSet[str]] = set()
        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                resp = await client.get(f"{self._api_url}/v1/rejections")
                resp.raise_for_status()
                for r in resp.json():
                    excluded.add(frozenset({r["uuid_a"], r["uuid_b"]}))
            except httpx.RequestError as exc:
                logger.error("[Librarian] Failed to fetch rejections: %s", exc)

            for formula in formulas:
                uuid = formula["uuid"]
                try:
                    resp = await client.get(f"{self._api_url}/v1/formulas/{uuid}/cross-links")
                    resp.raise_for_status()
                    for other in resp.json():
                        excluded.add(frozenset({uuid, other["uuid"]}))
                except httpx.RequestError as exc:
                    logger.error("[Librarian] Failed cross-links for %s: %s", uuid, exc)

        return excluded

    # ------------------------------------------------------------------
    # Scoring
    # ------------------------------------------------------------------

    def _select_best_pair(
        self,
        formulas: List[Dict[str, Any]],
        exclusions: Set[FrozenSet[str]],
        embeddings: Dict[str, List[float]],
    ) -> Optional[RoutingDecision]:
        best_score = -1.0
        best: Optional[RoutingDecision] = None
        pairs_scored = 0
        pairs_excluded = 0

        for formula_a, formula_b in combinations(formulas, 2):
            pair_key = frozenset({formula_a["uuid"], formula_b["uuid"]})
            if pair_key in exclusions:
                pairs_excluded += 1
                continue

            wing_div, tag_div, sem_div, score = self._score_pair(
                formula_a, formula_b, embeddings
            )
            pairs_scored += 1

            if score > best_score:
                best_score = score
                mode = "embedding" if embeddings else "jaccard-fallback"
                best = RoutingDecision(
                    uuid_a=formula_a["uuid"],
                    uuid_b=formula_b["uuid"],
                    formula_a_name=formula_a.get("name") or formula_a["uuid"][:8],
                    formula_b_name=formula_b.get("name") or formula_b["uuid"][:8],
                    score=score,
                    wing_diversity=wing_div,
                    tag_diversity=tag_div,
                    semantic_diversity=sem_div,
                    reasoning=(
                        f"wing={wing_div:.3f} "
                        f"semantic={sem_div:.3f} "
                        f"tag={tag_div:.3f} "
                        f"combined={score:.3f} [{mode}]"
                    ),
                    candidates_evaluated=0,
                    pairs_excluded=0,
                )

        if best is not None:
            best.candidates_evaluated = pairs_scored
            best.pairs_excluded = pairs_excluded

        return best

    def _score_pair(
        self,
        formula_a: Dict[str, Any],
        formula_b: Dict[str, Any],
        embeddings: Dict[str, List[float]],
    ) -> Tuple[float, float, float, float]:
        """
        Returns (wing_diversity, tag_diversity, semantic_diversity, combined_score).

        Primary path (embeddings present):
          combined = 0.50·wing + 0.35·semantic + 0.15·tag

        Fallback path (embeddings empty):
          combined = 0.60·wing + 0.40·tag  (original Jaccard behaviour)
          semantic_diversity returned as 0.0
        """
        # Wing diversity — Jaccard on mathematical_wing sets
        wings_a = set(formula_a.get("mathematical_wing") or [])
        wings_b = set(formula_b.get("mathematical_wing") or [])
        wing_union = wings_a | wings_b
        wing_intersection = wings_a & wings_b
        wing_diversity = (
            1.0 - len(wing_intersection) / len(wing_union) if wing_union else 0.0
        )

        # Tag diversity — Jaccard on cleaned tag sets
        raw_tags_a = set(formula_a.get("tags") or [])
        raw_tags_b = set(formula_b.get("tags") or [])
        tags_a = raw_tags_a - _NOISE_TAGS
        tags_b = raw_tags_b - _NOISE_TAGS
        tag_union = tags_a | tags_b
        tag_intersection = tags_a & tags_b
        tag_diversity = (
            1.0 - len(tag_intersection) / len(tag_union) if tag_union else 0.5
        )

        # Semantic diversity — cosine distance on behavioral_claim embeddings
        vec_a = embeddings.get(formula_a["uuid"])
        vec_b = embeddings.get(formula_b["uuid"])

        if vec_a is not None and vec_b is not None:
            semantic_diversity = 1.0 - self._cosine_similarity(vec_a, vec_b)
            combined = (
                _WING_DIVERSITY_WEIGHT * wing_diversity
                + _SEMANTIC_DIVERSITY_WEIGHT * semantic_diversity
                + _TAG_DIVERSITY_WEIGHT * tag_diversity
            )
        else:
            # Jaccard fallback — preserve original behaviour exactly
            semantic_diversity = 0.0
            combined = (
                _WING_DIVERSITY_WEIGHT_FALLBACK * wing_diversity
                + _TAG_DIVERSITY_WEIGHT_FALLBACK * tag_diversity
            )

        return wing_diversity, tag_diversity, semantic_diversity, combined
