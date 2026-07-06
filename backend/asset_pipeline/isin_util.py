"""ISIN validation + extraction for uploaded files.

A "valid ISIN" here means it passes BOTH the structural pattern (2 letters + 9
alphanumerics + 1 check digit) AND the ISIN check-digit (Luhn over the
letter-expanded digit string) — so pattern-shaped typos are dropped.
"""
from __future__ import annotations

import re

_ISIN_SHAPE = re.compile(r"[A-Z]{2}[A-Z0-9]{9}[0-9]")
_ISIN_EXACT = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")


def _check_digit_ok(isin: str) -> bool:
    """ISIN check-digit test: expand letters (A=10 … Z=35) to a digit string,
    then Luhn mod-10 (double every second digit from the right)."""
    digits = "".join(c if c.isdigit() else str(ord(c) - 55) for c in isin)
    total = 0
    for i, ch in enumerate(reversed(digits)):
        n = int(ch)
        if i % 2 == 1:
            n *= 2
            if n > 9:
                n -= 9
        total += n
    return total % 10 == 0


def is_valid_isin(s: str | None) -> bool:
    if not s:
        return False
    s = s.strip().upper()
    return bool(_ISIN_EXACT.match(s)) and _check_digit_ok(s)


def extract_isins(text: object) -> list[str]:
    """Every valid ISIN found in a cell/value, deduped, order-preserving."""
    out: dict[str, None] = {}
    for tok in _ISIN_SHAPE.findall(str(text).upper()):
        if _check_digit_ok(tok):
            out.setdefault(tok, None)
    return list(out.keys())
