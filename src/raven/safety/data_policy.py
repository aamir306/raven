"""
Data policy enforcement — ensures no actual row data values leak to the OpenAI API.

RAVEN rule: schema names, column names, descriptions → OK to send.
Actual row data values → NEVER sent to API.

This module provides checks to validate prompts before they are sent to OpenAI.
"""

from __future__ import annotations

import re


# Patterns that look like actual data values (not metadata)
_DATA_PATTERNS = [
    re.compile(r"\b\d{10,}\b"),  # long numeric strings (phone numbers, IDs)
    re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"),  # emails
    re.compile(r"\b\d{3}[-.]?\d{3}[-.]?\d{4}\b"),  # phone numbers
    re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b"),  # IP addresses
    re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b", re.I),  # UUIDs
]

# Known safe patterns that look like data but are schema metadata
_SAFE_PATTERNS = [
    re.compile(r"vector\(\d+\)"),  # pgvector type
    re.compile(r"INTERVAL\s+'[\d]+'\s+\w+", re.I),  # Trino interval syntax
    re.compile(r"TIMESTAMP\s+'[\d\-: ]+'", re.I),  # Trino timestamp literal
    re.compile(r"'[\d\-]+'", re.I),  # date string literals in SQL examples
]


def check_prompt(prompt: str, known_columns: set[str] | None = None) -> list[str]:
    """Scan a prompt for potential data value leaks.

    Parameters
    ----------
    prompt:
        The text about to be sent to the OpenAI API.
    known_columns:
        Optional set of known column/table names.  Matches against these are excluded.

    Returns
    -------
    list[str]
        List of warning messages describing potential data leaks.
        Empty list = safe to send.
    """
    warnings: list[str] = []
    known = known_columns or set()

    for pattern in _DATA_PATTERNS:
        for match in pattern.finditer(prompt):
            value = match.group(0)
            # Exclude if it matches a known column/table name
            if value.lower() in {k.lower() for k in known}:
                continue
            # Exclude if inside a safe pattern context
            start, end = match.span()
            context = prompt[max(0, start - 20) : end + 20]
            if any(sp.search(context) for sp in _SAFE_PATTERNS):
                continue
            warnings.append(f"Potential data leak at position {start}: '{value[:50]}…'")

    return warnings


def strip_data_values(text: str) -> str:
    """Replace suspected data values with [REDACTED].

    Useful as a safety net before sending to the API.
    """
    result = text
    for pattern in _DATA_PATTERNS:
        result = pattern.sub("[REDACTED]", result)
    return result


def is_safe_for_api(prompt: str) -> bool:
    """Quick check — returns True if no data patterns detected."""
    return len(check_prompt(prompt)) == 0
