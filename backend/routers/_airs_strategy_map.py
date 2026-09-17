"""The reviewed AIRS-code/certificate map shared by names and look-through."""
from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path


def _key(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", "", (value or "").lower())


@lru_cache(maxsize=4)
def _load(path_text: str, modified_ns: int) -> tuple[dict, ...]:
    """Parse one on-disk revision; the mtime is deliberately part of the cache key."""
    path = Path(path_text)
    return tuple(json.loads(path.read_text(encoding="utf-8"))["strategies"])


def strategies() -> tuple[dict, ...]:
    """Current checked-in map, reloaded after an operator edits the JSON.

    This used to be cached forever per backend process.  The whole point of a reviewed JSON file
    is that an operator can correct a name/link without a deploy; stale in-process data made a
    saved correction invisible until a restart.  ``st_mtime_ns`` keeps unchanged requests cheap
    while making the very next request see a saved edit.
    """
    path = Path(__file__).resolve().parent.parent / "config" / "airs_strategy_map.json"
    return _load(str(path), path.stat().st_mtime_ns)


def nickname_for(airs_name: str | None) -> str | None:
    wanted = _key(airs_name)
    for strategy in strategies():
        if wanted in {_key(name) for name in strategy["airs_names"]}:
            return strategy["nickname"]
    return None


def nickname_for_holding(holding_name: str | None) -> str | None:
    """The reader-facing strategy name for one mapped certificate holding.

    AIRS calls a certificate ``EuropaTopSelectie Index`` while the underlying model uses the
    compact ``EuropaTopSelect …`` code. Both name the same strategy, but only the former exists
    on a book that cannot be looked through, so it needs the same nickname resolution.
    """
    wanted = _key(holding_name)
    for strategy in strategies():
        if wanted in {_key(name) for name in (*strategy["airs_names"], *strategy["holding_aliases"])}:
            return strategy["nickname"]
    return None


def dynamic_account_for_holding(holding_name: str | None) -> str | None:
    """The valued Dynamic account behind a mapped certificate holding, when configured."""
    wanted = _key(holding_name)
    for strategy in strategies():
        aliases = {_key(name) for name in (*strategy["holding_aliases"], strategy["nickname"])}
        if wanted not in aliases:
            continue
        return next((name for name in strategy["airs_names"]
                     if _key(name).endswith(("dyn", "dy"))), None)
    return None


def is_strategy_holding(name: str | None) -> bool:
    """Whether a holding name is one of our mapped AIRS strategies/certificates."""
    wanted = _key(name)
    # A folded certificate is deliberately renamed to the reviewed nickname before the shared
    # bucket classifier runs. Treat that name as the same wrapper too, or EuropaTopSelectie moves
    # from Stock ETFs to individual Stocks solely because its label became more readable.
    return any(wanted in {_key(n) for n in (s["nickname"], *s["airs_names"], *s["holding_aliases"])}
               for s in strategies())


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
