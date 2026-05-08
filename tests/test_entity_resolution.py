"""
Entity Resolution Tests — scoring and merge decision logic.
"""
from __future__ import annotations

import pytest

from src.entity_resolution.scorer import SimilarityScorer, ScoredCandidate
from src.normalization.blocking import generate_blocking_keys


class TestSimilarityScorer:
    def setup_method(self):
        self.scorer = SimilarityScorer()

    def _make_doc(self, **kwargs) -> dict:
        base = {
            "normalized_name": "ganesh kumar",
            "phonetic_name": "KNXK",
            "normalized_phones": ["919876543210"],
            "normalized_fir_numbers": ["KLM/456/2023"],
            "normalized_relative_names": ["rajan kumar"],
            "address_locality": "kadappakada",
            "district": "kollam",
            "dob": "1991-06-15",
            "age_group": "30to34",
            "person_role": "accused",
        }
        base.update(kwargs)
        return base

    def test_same_phone_high_score(self):
        probe = self._make_doc()
        candidate_hit = {
            "_id": "cand-001",
            "_score": 5.0,
            "_source": self._make_doc(),
        }
        results = self.scorer.score_all(probe, [candidate_hit])
        assert results[0].score >= 0.75  # should auto-merge threshold

    def test_different_phone_no_phone_bonus(self):
        probe = self._make_doc(normalized_phones=["919876543210"])
        candidate_hit = {
            "_id": "cand-002",
            "_score": 1.0,
            "_source": self._make_doc(normalized_phones=["917654321098"]),
        }
        results = self.scorer.score_all(probe, [candidate_hit])
        # No phone match bonus; score should be lower
        assert results[0].breakdown["phone_exact"] == 0.0

    def test_shared_fir_same_role_bonus(self):
        probe = self._make_doc(
            normalized_phones=[],  # no phone
            normalized_fir_numbers=["KLM/456/2023"],
        )
        candidate_hit = {
            "_id": "cand-003",
            "_score": 2.0,
            "_source": self._make_doc(
                normalized_phones=[],
                normalized_fir_numbers=["KLM/456/2023"],
            ),
        }
        results = self.scorer.score_all(probe, [candidate_hit])
        assert results[0].breakdown["fir_role"] == 0.15

    def test_empty_candidates(self):
        probe = self._make_doc()
        results = self.scorer.score_all(probe, [])
        assert results == []

    def test_score_clamped_to_one(self):
        probe = self._make_doc()
        candidate_hit = {
            "_id": "cand-004",
            "_score": 100.0,
            "_source": self._make_doc(),
        }
        results = self.scorer.score_all(probe, [candidate_hit])
        assert results[0].score <= 1.0

    def test_score_zero_on_no_overlap(self):
        probe = self._make_doc(
            normalized_phones=[],
            normalized_fir_numbers=[],
            normalized_relative_names=[],
            address_locality="",
            dob=None,
            age_group="40to44",
        )
        candidate_hit = {
            "_id": "cand-005",
            "_score": 0.1,
            "_source": self._make_doc(
                normalized_name="krishna pillai",
                normalized_phones=[],
                normalized_fir_numbers=[],
                normalized_relative_names=[],
                address_locality="",
                dob="1975-01-01",
                age_group="50to54",
            ),
        }
        results = self.scorer.score_all(probe, [candidate_hit])
        # Name similarity will be low but non-zero; score should be < review threshold
        assert results[0].score < 0.55

    def test_breakdown_sums_to_score(self):
        probe = self._make_doc()
        candidate_hit = {
            "_id": "cand-006",
            "_score": 3.0,
            "_source": self._make_doc(normalized_phones=["919111111111"]),
        }
        results = self.scorer.score_all(probe, [candidate_hit])
        sc = results[0]
        total_from_breakdown = round(sum(sc.breakdown.values()), 4)
        assert abs(total_from_breakdown - sc.score) < 0.01


class TestBlockingKeyRecall:
    """Verify blocking keys ensure recall (similar records share at least one key)."""

    def _make_keys(self, **kwargs) -> set:
        defaults = dict(
            normalized_name="ganesh kumar",
            phonetic_name="KNXK",
            normalized_phones=["919876543210"],
            district="kollam",
            police_station="kollam_town",
            address_locality="kadappakada",
            age=32,
        )
        defaults.update(kwargs)
        return set(generate_blocking_keys(**defaults))

    def test_same_phone_shares_key(self):
        keys_a = self._make_keys(normalized_phones=["919876543210"])
        keys_b = self._make_keys(normalized_phones=["919876543210"], normalized_name="ganeshan")
        assert keys_a & keys_b  # must share at least one key

    def test_same_district_and_name_prefix_shares_key(self):
        keys_a = self._make_keys(normalized_phones=[])
        keys_b = self._make_keys(normalized_name="ganesh nair", normalized_phones=[])
        assert keys_a & keys_b  # both have dist_kollam_name_gane

    def test_different_district_no_shared_phone_miss(self):
        keys_a = self._make_keys(district="kollam", normalized_phones=[])
        keys_b = self._make_keys(district="thrissur", normalized_name="different", normalized_phones=[])
        # May or may not share keys depending on phonetic; just verify no crash
        assert isinstance(keys_a & keys_b, set)
