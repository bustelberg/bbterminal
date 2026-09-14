"""The reviewed AIRS-code/certificate map shared by names and look-through."""
from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path


def _key(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", "", (value or "").lower())


@lru_cache(maxsize=1)
def strategies() -> tuple[dict, ...]:
    path = Path(__file__).resolve().parent.parent / "config" / "airs_strategy_map.json"
    return tuple(json.loads(path.read_text(encoding="utf-8"))["strategies"])


def nickname_for(airs_name: str | None) -> str | None:
    wanted = _key(airs_name)
    for strategy in strategies():
        if wanted in {_key(name) for name in strategy["airs_names"]}:
            return strategy["nickname"]
    return None


def target_for_alias(fonds: str | None, portfolios: list[dict], composition: dict[int, list[dict]],
                     owner_id: int, isin: str | None) -> int | None:
    """Configured certificate target, guarded by the same anti-cycle rules as guesses."""
    wanted = _key(fonds)
    target_name = next((s["look_through_target"] for s in strategies()
                        if wanted in {_key(a) for a in s["holding_aliases"]}), None)
    if not target_name:
        return None
    target = next((p for p in portfolios if _key(p.get("name")) == _key(target_name)), None)
    if not target:
        return None
    pid = target["id"]
    holders = {p["id"] for p in portfolios if isin and any(
        (row.get("isin") or "") == isin for row in composition.get(p["id"], []))}
    return pid if pid != owner_id and pid not in holders and len(composition.get(pid, [])) > 1 else None
