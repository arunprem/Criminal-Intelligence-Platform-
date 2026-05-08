"""
FIR number normalization.

FIR format varies across districts and police stations.
This module canonicalizes FIR numbers to a consistent format:
  {PS_CODE}/{YEAR}/{NUMBER}
Example: KLM/PS001/2023/456
"""
from __future__ import annotations

import re
from typing import List, Optional

_WHITESPACE = re.compile(r"\s+")
_NON_ALPHA_NUM = re.compile(r"[^a-zA-Z0-9/\-]")

# Common FIR format patterns
# Format 1: 123/2023 → number/year
_PATTERN_NUM_YEAR = re.compile(r"^(\d+)[/\-](\d{4})$")
# Format 2: 2023/123 → year/number
_PATTERN_YEAR_NUM = re.compile(r"^(\d{4})[/\-](\d+)$")
# Format 3: CR123/2023 (Crime Register prefix)
_PATTERN_CR = re.compile(r"^CR[.\-]?(\d+)[/\-](\d{4})$", re.IGNORECASE)
# Format 4: PS/CR/123/2023 (includes PS code)
_PATTERN_FULL = re.compile(
    r"^([A-Za-z\d]+)[/\-]([A-Za-z\d]+)[/\-](\d+)[/\-](\d{4})$"
)


def normalize_fir(
    raw_fir: Optional[str],
    police_station_code: Optional[str] = None,
) -> Optional[str]:
    """
    Normalize a single FIR number to canonical form.

    Returns None if the input cannot be parsed as a valid FIR.
    """
    if not raw_fir:
        return None

    text = _WHITESPACE.sub("", str(raw_fir).strip().upper())
    text = text.replace(" ", "")

    # Pattern: CR123/2023
    m = _PATTERN_CR.match(text)
    if m:
        num, year = m.group(1), m.group(2)
        ps = police_station_code or "UNK"
        return f"{ps}/CR/{num}/{year}"

    # Pattern: full PS/TYPE/NUM/YEAR
    m = _PATTERN_FULL.match(text)
    if m:
        return text  # already canonical

    # Pattern: NUM/YEAR
    m = _PATTERN_NUM_YEAR.match(text)
    if m:
        num, year = m.group(1), m.group(2)
        ps = police_station_code or "UNK"
        return f"{ps}/{num}/{year}"

    # Pattern: YEAR/NUM
    m = _PATTERN_YEAR_NUM.match(text)
    if m:
        year, num = m.group(1), m.group(2)
        ps = police_station_code or "UNK"
        return f"{ps}/{num}/{year}"

    # Fallback: just clean and return
    cleaned = _NON_ALPHA_NUM.sub("", text)
    return cleaned if cleaned else None


def normalize_firs(
    raw_firs: List[Optional[str]],
    police_station_code: Optional[str] = None,
) -> List[str]:
    """Normalize a list of FIR numbers, returning unique valid entries."""
    result = set()
    for raw in raw_firs:
        normalized = normalize_fir(raw, police_station_code)
        if normalized:
            result.add(normalized)
    return sorted(result)


def extract_fir_year(normalized_fir: Optional[str]) -> Optional[int]:
    """Extract year from a normalized FIR number."""
    if not normalized_fir:
        return None
    parts = normalized_fir.split("/")
    for part in reversed(parts):
        if part.isdigit() and 2000 <= int(part) <= 2100:
            return int(part)
    return None
