"""Configured AIRS portfolio exclusions and the test-only TopSelecties collection."""
from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Iterable, TypeVar


_T = TypeVar("_T")


def _key(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", "", (value or "").casefold())


@lru_cache(maxsize=4)
def _load(path_text: str, modified_ns: int) -> frozenset[str]:
    path = Path(path_text)
    rows = json.loads(path.read_text(encoding="utf-8")).get("excluded_portfolios") or []
    return frozenset(_key(str(row)) for row in rows if _key(str(row)))


def is_excluded_portfolio(name: str | None) -> bool:
    """Whether ``name`` is an intentionally retired AIRS portfolio or display alias."""
    path = Path(__file__).resolve().parent / "config" / "airs_portfolio_exclusions.json"
    return _key(name) in _load(str(path), path.stat().st_mtime_ns)


def include_portfolios(rows: Iterable[_T], name_for) -> list[_T]:
    """Keep only configured, active rows without duplicating matching rules at each boundary."""
    return [row for row in rows if not is_excluded_portfolio(name_for(row))]


@lru_cache(maxsize=4)
def _test_topselecties(path_text: str, modified_ns: int) -> dict[str, dict]:
    path = Path(path_text)
    rows = json.loads(path.read_text(encoding="utf-8")).get("test_topselecties") or []
    return {_key(row.get("dynamic_portefeuille")): row for row in rows
            if _key(row.get("dynamic_portefeuille")) and row.get("display_name")}


def topselectie_test_entry_for_account(account_name: str | None) -> dict | None:
    """The configured test-only TopSelectie for this AIRS Dynamic account, if any."""
    path = Path(__file__).resolve().parent / "config" / "airs_portfolio_exclusions.json"
    return _test_topselecties(str(path), path.stat().st_mtime_ns).get(_key(account_name))
