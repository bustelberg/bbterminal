"""Deterministic corporate-action replacements for a locked portfolio."""
from __future__ import annotations


def next_same_sector_reserve(
    holding: dict, reserves: list[dict], held_company_ids: set[int],
    unavailable_company_ids: set[int],
) -> dict | None:
    """Return the first usable frozen reserve in ``holding``'s sector.

    Never re-score and never cross sectors: both would turn a corporate action
    into an unannounced discretionary rebalance.  ``None`` means keep this
    sleeve in cash until the ordinary rebalance.
    """
    sector = holding.get("sector")
    if not sector:
        return None
    blocked = held_company_ids | unavailable_company_ids
    choices = sorted(
        (r for r in reserves
         if r.get("sector") == sector and int(r.get("company_id") or 0) not in blocked),
        key=lambda r: (int(r.get("reserve_rank") or 2**31), int(r.get("company_id") or 0)),
    )
    return choices[0] if choices else None
