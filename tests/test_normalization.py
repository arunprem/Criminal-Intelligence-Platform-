"""
Normalization Pipeline Tests.
"""
from __future__ import annotations

import pytest

from src.normalization.name_normalizer import (
    normalize_name,
    normalize_aliases,
    name_similarity,
    get_name_prefix,
)
from src.normalization.phone_normalizer import (
    normalize_phone,
    normalize_phones,
    phones_overlap,
)
from src.normalization.address_normalizer import (
    normalize_address,
    normalize_district,
    extract_locality,
)
from src.normalization.fir_normalizer import normalize_fir, normalize_firs
from src.normalization.blocking import generate_blocking_keys, compute_age_group
from src.normalization.pipeline import NormalizationPipeline


# ── Name Normalization Tests ───────────────────────────────────────────────────

class TestNameNormalizer:
    def test_basic_english_name(self):
        norm, phonetic, trans = normalize_name("Ganesh Kumar")
        assert norm == "ganesh kumar"
        assert phonetic  # should have phonetic code

    def test_name_with_title(self):
        norm, _, _ = normalize_name("Mr. Rajan Pillai")
        assert "mr" not in norm
        assert "rajan" in norm

    def test_whitespace_cleanup(self):
        norm, _, _ = normalize_name("  Suresh   Nair  ")
        assert norm == "suresh nair"

    def test_malayalam_name(self):
        # ഗണേഷ് = Ganesh in Malayalam
        norm, phonetic, trans = normalize_name("ഗണേഷ്")
        assert norm  # should produce something
        assert len(norm) > 0

    def test_empty_name(self):
        norm, phonetic, trans = normalize_name("")
        assert norm == ""
        assert phonetic == ""

    def test_none_name(self):
        norm, phonetic, trans = normalize_name(None)
        assert norm == ""

    def test_name_prefix(self):
        prefix = get_name_prefix("ganesh kumar", length=4)
        assert prefix == "gane"

    def test_name_similarity_high(self):
        score = name_similarity("ganesh", "ganesh")
        assert score >= 0.9

    def test_name_similarity_low(self):
        score = name_similarity("ganesh", "krishnan")
        assert score < 0.5

    def test_alias_normalization(self):
        aliases = normalize_aliases(["Gani", "Mr. Ganesh", "  Ganeshan  "])
        assert all(isinstance(a, str) for a in aliases)
        assert all(a == a.lower() for a in aliases)


# ── Phone Normalization Tests ──────────────────────────────────────────────────

class TestPhoneNormalizer:
    def test_standard_10_digit(self):
        assert normalize_phone("9876543210") == "919876543210"

    def test_with_plus91(self):
        assert normalize_phone("+919876543210") == "919876543210"

    def test_with_0(self):
        assert normalize_phone("09876543210") == "919876543210"

    def test_with_spaces(self):
        assert normalize_phone("98765 43210") == "919876543210"

    def test_with_dashes(self):
        assert normalize_phone("98765-43210") == "919876543210"

    def test_invalid_phone(self):
        assert normalize_phone("12345") is None

    def test_landline_invalid(self):
        # Landline starting with 0 not a valid mobile
        assert normalize_phone("0471-2345678") is None

    def test_normalize_list(self):
        phones = normalize_phones(["9876543210", "+919876543210", "invalid"])
        assert len(phones) == 1  # deduplicated
        assert "919876543210" in phones

    def test_phones_overlap(self):
        assert phones_overlap(["919876543210"], ["919876543210", "917654321098"])
        assert not phones_overlap(["919876543210"], ["917654321098"])


# ── Address Normalization Tests ────────────────────────────────────────────────

class TestAddressNormalizer:
    def test_district_abbreviation(self):
        assert normalize_district("TVM") == "thiruvananthapuram"
        assert normalize_district("kochi") == "ernakulam"
        assert normalize_district("calicut") == "kozhikode"

    def test_address_normalization(self):
        addr, locality, district = normalize_address(
            "House No 12, MG Road, Kollam",
            raw_district="Kollam"
        )
        assert district == "kollam"
        assert addr  # should have some content

    def test_empty_address(self):
        addr, locality, district = normalize_address(None, "Kollam")
        assert addr == ""
        assert district == "kollam"


# ── FIR Normalization Tests ────────────────────────────────────────────────────

class TestFirNormalizer:
    def test_num_year_format(self):
        result = normalize_fir("456/2023", police_station_code="KLM01")
        assert result is not None
        assert "456" in result
        assert "2023" in result

    def test_year_num_format(self):
        result = normalize_fir("2023/456", police_station_code="KLM01")
        assert result is not None

    def test_cr_format(self):
        result = normalize_fir("CR123/2023", police_station_code="EKM01")
        assert result is not None
        assert "CR" in result

    def test_empty_fir(self):
        assert normalize_fir(None) is None
        assert normalize_fir("") is None

    def test_normalize_list(self):
        firs = normalize_firs(["456/2023", "456/2023", None, "789/2022"], "KLM")
        assert len(firs) <= 2  # deduplicated, None removed


# ── Blocking Key Tests ─────────────────────────────────────────────────────────

class TestBlockingKeys:
    def test_age_group(self):
        assert compute_age_group(32) == "30to34"
        assert compute_age_group(0) == "unknown"
        assert compute_age_group(None) == "unknown"

    def test_generates_multiple_keys(self):
        keys = generate_blocking_keys(
            normalized_name="ganesh kumar",
            phonetic_name="KNXK",
            normalized_phones=["919876543210"],
            district="kollam",
            police_station="qlmps01",
            address_locality="kadappakada",
            age=32,
        )
        assert len(keys) >= 3
        assert all(isinstance(k, str) for k in keys)
        # Phone prefix key should be present
        assert any("ph_" in k for k in keys)
        # District+name key should be present
        assert any("dist_" in k for k in keys)

    def test_empty_inputs(self):
        keys = generate_blocking_keys(
            normalized_name="",
            phonetic_name="",
            normalized_phones=[],
            district="",
            police_station="",
            address_locality="",
        )
        assert isinstance(keys, list)  # may be empty but shouldn't crash


# ── Full Pipeline Integration Tests ───────────────────────────────────────────

class TestNormalizationPipeline:
    def setup_method(self):
        self.pipeline = NormalizationPipeline()

    def _sample_accused_doc(self) -> dict:
        return {
            "personInformation": {
                "name": "Ganesh Kumar",
                "age": 32,
                "dob": "15-06-1991",
                "gender": "M",
                "fatherName": "Rajan Kumar",
            },
            "mobileNumber": "9876543210",
            "addresses": [
                {
                    "houseNo": "12",
                    "street": "MG Road",
                    "locality": "Kadappakada",
                    "district": "Kollam",
                    "state": "Kerala",
                    "pincode": "691001",
                }
            ],
            "caseInfodetails": {
                "firNo": "456/2023",
                "policeStation": "Kollam Town",
                "district": "Kollam",
            },
            "aliases": ["Gani", "Ganeshan"],
        }

    def test_full_pipeline(self):
        doc = self._sample_accused_doc()
        result = self.pipeline.process(doc, "accused", "test-001")

        assert result["normalized_name"] == "ganesh kumar"
        assert "919876543210" in result["normalized_phones"]
        assert result["district"] == "kollam"
        assert result["age"] == 32
        assert result["person_role"] == "accused"
        assert len(result["blocking_keys"]) >= 3
        assert result["source_index"] == "accused"
        assert result["source_id"] == "test-001"
        assert result["processing_status"] == "normalized"

    def test_stable_id_generation(self):
        doc = self._sample_accused_doc()
        r1 = self.pipeline.process(doc, "accused", "test-001")
        r2 = self.pipeline.process(doc, "accused", "test-001")
        assert r1["_id"] == r2["_id"]  # deterministic

    def test_missing_fields_handled(self):
        """Pipeline should not raise on minimal document."""
        minimal = {"personInformation": {"name": "Unknown"}}
        result = self.pipeline.process(minimal, "witness", "min-001")
        assert result["normalized_name"] == "unknown"
        assert result["normalized_phones"] == []
