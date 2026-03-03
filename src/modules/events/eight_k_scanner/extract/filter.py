"""Rules-based 8-K item filter.

Classifies filings by item number into LONG and SHORT categories.
"""
from typing import Literal

Strategy = Literal["long", "short", "both"]

LONG_ITEMS: set[str] = {
    "1.01", "2.01", "2.02", "2.03", "3.02",
    "5.01", "5.02", "5.06", "7.01", "8.01", "9.01",
}

SHORT_ITEMS: set[str] = {
    "1.02", "1.03", "1.05", "2.02", "2.04", "2.05", "2.06",
    "3.01", "3.03", "4.01", "4.02", "5.01", "5.02", "5.06", "9.01",
}


def filter_filing(items_detected: list[str], strategy: Strategy = "long") -> tuple[bool, set[str]]:
    """Check if a filing's detected items pass the filter.

    Returns (passes, matched_items).
    """
    if strategy == "long":
        allowed = LONG_ITEMS
    elif strategy == "short":
        allowed = SHORT_ITEMS
    else:
        allowed = LONG_ITEMS | SHORT_ITEMS

    matched = set(items_detected) & allowed
    return bool(matched), matched
