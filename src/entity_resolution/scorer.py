"""
Similarity Scorer — Entity Resolution Phase 3.

Computes a weighted similarity score between a probe document
and each candidate document. Returns a float in [0, 1].

Weight schema:
  HIGH   (sum = 0.55)
    - exact phone match        : 0.40
    - exact FIR + same role    : 0.15
  MEDIUM (sum = 0.30)
    - DOB match                : 0.10
    - relative name similarity : 0.10
    - address locality match   : 0.10
  LOW    (sum = 0.15)
    - fuzzy name similarity    : 0.10
    - age group match          : 0.05
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from src.normalization.name_normalizer import name_similarity
from src.normalization.phone_normalizer import phones_overlap
from src.core.logging import get_logger

logger = get_logger(__name__)

# Weight constants
W_PHONE_EXACT = 0.40
W_FIR_ROLE = 0.15
W_DOB = 0.10
W_RELATIVE_NAME = 0.10
W_ADDRESS_LOCALITY = 0.10
W_FUZZY_NAME = 0.10
W_AGE_GROUP = 0.05


@dataclass
class ScoredCandidate:
    candidate_id: str
    score: float
    breakdown: Dict[str, float] = field(default_factory=dict)
    source_doc: Optional[Dict[str, Any]] = None

    def __post_init__(self) -> None:
        self.score = round(min(max(self.score, 0.0), 1.0), 4)


class SimilarityScorer:
    """
    Scores similarity between a probe normalized_person doc
    and a list of candidates, returning ScoredCandidate objects sorted
    by score descending.
    """

    def score_all(
        self,
        probe: Dict[str, Any],
        candidates: List[Dict[str, Any]],
    ) -> List[ScoredCandidate]:
        """Score probe against all candidates. Returns sorted list."""
        results = []
        for candidate_hit in candidates:
            scored = self.score_pair(probe, candidate_hit["_source"])
            scored.candidate_id = candidate_hit["_id"]
            scored.source_doc = candidate_hit["_source"]
            results.append(scored)
        return sorted(results, key=lambda s: s.score, reverse=True)

    def score_pair(
        self,
        probe: Dict[str, Any],
        candidate: Dict[str, Any],
    ) -> ScoredCandidate:
        """Compute weighted similarity between probe and one candidate."""
        breakdown: Dict[str, float] = {}
        total = 0.0

        # ── 1. Phone exact match ──────────────────────────────────────────────
        probe_phones = probe.get("normalized_phones") or []
        cand_phones = candidate.get("normalized_phones") or []
        phone_score = W_PHONE_EXACT if phones_overlap(probe_phones, cand_phones) else 0.0
        breakdown["phone_exact"] = phone_score
        total += phone_score

        # ── 2. FIR + same role ────────────────────────────────────────────────
        probe_firs = set(probe.get("normalized_fir_numbers") or [])
        cand_firs = set(candidate.get("normalized_fir_numbers") or [])
        shared_firs = probe_firs & cand_firs
        same_role = probe.get("person_role") == candidate.get("person_role")
        fir_score = 0.0
        if shared_firs:
            fir_score = W_FIR_ROLE if same_role else W_FIR_ROLE * 0.5
        breakdown["fir_role"] = fir_score
        total += fir_score

        # ── 3. DOB match ──────────────────────────────────────────────────────
        probe_dob = probe.get("dob")
        cand_dob = candidate.get("dob")
        dob_score = 0.0
        if probe_dob and cand_dob:
            if probe_dob == cand_dob:
                dob_score = W_DOB
            elif probe_dob[:4] == cand_dob[:4]:  # year match only
                dob_score = W_DOB * 0.5
        breakdown["dob"] = dob_score
        total += dob_score

        # ── 4. Relative name similarity ───────────────────────────────────────
        probe_rels = probe.get("normalized_relative_names") or []
        cand_rels = candidate.get("normalized_relative_names") or []
        rel_score = self._best_list_similarity(probe_rels, cand_rels) * W_RELATIVE_NAME
        breakdown["relative_name"] = rel_score
        total += rel_score

        # ── 5. Address locality match ─────────────────────────────────────────
        probe_loc = probe.get("address_locality") or ""
        cand_loc = candidate.get("address_locality") or ""
        loc_score = 0.0
        if probe_loc and cand_loc and probe_loc == cand_loc:
            loc_score = W_ADDRESS_LOCALITY
        elif probe_loc and cand_loc:
            # Partial locality match
            loc_score = name_similarity(probe_loc, cand_loc) * W_ADDRESS_LOCALITY
        breakdown["address_locality"] = loc_score
        total += loc_score

        # ── 6. Fuzzy name similarity ──────────────────────────────────────────
        probe_name = probe.get("normalized_name") or ""
        cand_name = candidate.get("normalized_name") or ""
        fuzzy_name = name_similarity(probe_name, cand_name) * W_FUZZY_NAME
        breakdown["fuzzy_name"] = round(fuzzy_name, 4)
        total += fuzzy_name

        # ── 7. Age group match ────────────────────────────────────────────────
        probe_ag = probe.get("age_group") or ""
        cand_ag = candidate.get("age_group") or ""
        ag_score = W_AGE_GROUP if probe_ag and probe_ag == cand_ag else 0.0
        breakdown["age_group"] = ag_score
        total += ag_score

        return ScoredCandidate(
            candidate_id="",  # filled by caller
            score=total,
            breakdown=breakdown,
        )

    @staticmethod
    def _best_list_similarity(list_a: List[str], list_b: List[str]) -> float:
        """
        Best-pair similarity: max(sim(a, b)) for all a in list_a, b in list_b.
        """
        if not list_a or not list_b:
            return 0.0
        best = 0.0
        for a in list_a:
            for b in list_b:
                sim = name_similarity(a, b)
                if sim > best:
                    best = sim
                    if best >= 1.0:
                        return 1.0
        return best
