"""Reviewed allowlist for the TopSelecties part of the management dashboard."""
from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path


def _key(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", "", (value or "").casefold())


@lru_cache(maxsize=4)
def _load(path_text: str, modified_ns: int) -> dict[str, dict]:
    path = Path(path_text)
    rows = json.loads(path.read_text(encoding="utf-8")).get("topselecties") or []
    return {_key(row.get("dynamic_portefeuille")): row for row in rows
            if row.get("dynamic_portefeuille") and row.get("display_name")}


def topselectie_for_account(account_name: str | None) -> dict | None:
    """The reviewed TopSelecties-tab entry for an AIRS Dynamic account, if it is listed."""
    path = Path(__file__).resolve().parent.parent / "config" / "management_topselecties.json"
    return _load(str(path), path.stat().st_mtime_ns).get(_key(account_name))
