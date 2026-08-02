"""Per-exchange, data-driven price freshness — "is the universe fresh enough
to rebalance?".

A first-Monday rebalance needs every company's deciding-bar close (the prior
trading day — Friday, or earlier if that market had a holiday). Across a global
universe each exchange keeps its OWN calendar, and GuruFocus publishes some
names a session late. Rather than hard-code every venue's holiday calendar, we
let the DATA be the calendar: a company is fresh when it's caught up to the
freshest close among its EXCHANGE PEERS (who share its trading calendar), so a
market holiday needs no special-casing — the peers simply have no bar that day
either. A whole-exchange stall (GuruFocus hasn't published ANY of an exchange's
latest session) is caught by a lenient global-latest sanity bound.

This module is split into a PURE classifier (`classify_universe_freshness`,
unit-tested with synthetic dates) and a thin DB wrapper (`universe_freshness`,
in `ingest/phases/prices.py`) that gathers its inputs. The rebalance op uses the
report's `to_fetch` to pick which names to FETCH before it computes, then
re-probes and WARNS about whatever is still behind — it never blocks the
rebalance on freshness.

⚠ THE REPORT IS RELATIVE, SO IT NEEDS AN ABSOLUTE PARTNER. Every judgement here
is "behind your peers" / "behind the global latest" — a universe uniformly a week
old is unanimously `fresh`, because nobody is behind anybody. The caller
therefore also compares `global_latest` against the date it actually needs (the
rebalance's deciding bar) and treats the whole active set as behind when the
universe as a whole has not reached it.
"""
from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date

from ingest.staleness import trading_days_between

_log = logging.getLogger(__name__)

# A company ≥ this many trading days behind its EXCHANGE PEERS is a laggard
# (the common GuruFocus case: most of an exchange has Friday, a few don't yet).
_PEER_TOLERANCE_DAYS = 1
# An entire exchange this many trading days behind the GLOBAL freshest close is
# treated as stalled (GuruFocus hasn't published any of its latest session) —
# every name on it becomes a laggard to fetch. Lenient enough that a normal
# single-day market holiday (exchange 1 behind the venues that traded) is NOT
# flagged; only a multi-session gap is.
_EXCHANGE_STALL_TOLERANCE_DAYS = 3


def _parse(d: str | None) -> date | None:
    if not d:
        return None
    try:
        return date.fromisoformat(str(d)[:10])
    except ValueError:
        return None


@dataclass(frozen=True)
class FreshnessReport:
    """Per-exchange freshness classification of a universe.

    `fresh` / `lagging` / `missing` partition the ACTIVE (non-excluded)
    universe: fresh = caught up to its market; lagging = behind its exchange
    peers or on a stalled exchange (re-fetchable); missing = no close data at
    all. `excluded` are delisted/out-of-scope/illiquid/unsubscribed names that
    lag by design and don't count toward readiness.
    """
    global_latest: date | None
    exchange_latest: dict[str, date]
    fresh: list[int] = field(default_factory=list)
    lagging: list[int] = field(default_factory=list)
    missing: list[int] = field(default_factory=list)
    excluded: list[int] = field(default_factory=list)

    @property
    def active_total(self) -> int:
        return len(self.fresh) + len(self.lagging) + len(self.missing)

    @property
    def to_fetch(self) -> list[int]:
        """Active names worth a (re-)fetch: missing data or lagging their market."""
        return self.missing + self.lagging

    @property
    def fresh_fraction(self) -> float:
        """Share of the active universe caught up to its market (1.0 when the
        active universe is empty — nothing to wait on)."""
        total = self.active_total
        return 1.0 if total == 0 else len(self.fresh) / total


def classify_universe_freshness(
    latest_by_cid: dict[int, str | None],
    exchange_by_cid: dict[int, str | None],
    *,
    excluded_ids: set[int] | frozenset[int] = frozenset(),
    peer_tolerance_days: int = _PEER_TOLERANCE_DAYS,
    exchange_stall_tolerance_days: int = _EXCHANGE_STALL_TOLERANCE_DAYS,
) -> FreshnessReport:
    """Classify each company's price freshness against its EXCHANGE peers.

    `latest_by_cid`: company_id → latest close date (ISO str; None/"" = no data).
    `exchange_by_cid`: company_id → exchange code (the peer-group key).
    `excluded_ids`: names that lag by design (delisted/out-of-scope/illiquid/
    unsubscribed) — reported in `excluded`, never counted as active.

    A company is:
      * `missing` — no close date at all,
      * `lagging` — ≥`peer_tolerance_days` trading days behind its exchange's
        freshest close, OR on an exchange whose freshest close is
        ≥`exchange_stall_tolerance_days` behind the GLOBAL freshest (a whole-
        exchange stall),
      * `fresh`  — otherwise (caught up to its market).

    Pure — no DB. `trading_days_between` counts Mon–Fri (holiday-agnostic), but
    the per-exchange anchor already absorbs holidays: peers define the sessions.
    """
    # Consider only companies we have BOTH a listing exchange for (the peer key)
    # and that aren't excluded. An unknown exchange can't be peer-anchored.
    cids = [
        c for c in exchange_by_cid
        if c not in excluded_ids and exchange_by_cid.get(c)
    ]
    dated: dict[int, date] = {}
    by_exchange: dict[str, list[int]] = defaultdict(list)
    for c in cids:
        exch = str(exchange_by_cid[c])
        by_exchange[exch].append(c)
        d = _parse(latest_by_cid.get(c))
        if d is not None:
            dated[c] = d

    exchange_latest: dict[str, date] = {}
    for exch, members in by_exchange.items():
        ds = [dated[c] for c in members if c in dated]
        if ds:
            exchange_latest[exch] = max(ds)
    global_latest = max(exchange_latest.values()) if exchange_latest else None

    fresh: list[int] = []
    lagging: list[int] = []
    missing: list[int] = []
    for c in cids:
        exch = str(exchange_by_cid[c])
        cdate = dated.get(c)
        if cdate is None:
            missing.append(c)
            continue
        exch_latest = exchange_latest.get(exch)
        # Whole-exchange stall: the exchange itself trails the global pack.
        exch_gap = (
            trading_days_between(exch_latest, global_latest)
            if exch_latest is not None and global_latest is not None else 0
        )
        peer_gap = (
            trading_days_between(cdate, exch_latest)
            if exch_latest is not None else 0
        )
        if peer_gap >= peer_tolerance_days or exch_gap >= exchange_stall_tolerance_days:
            lagging.append(c)
        else:
            fresh.append(c)

    return FreshnessReport(
        global_latest=global_latest,
        exchange_latest=exchange_latest,
        fresh=fresh,
        lagging=lagging,
        missing=missing,
        excluded=sorted(set(excluded_ids) & set(exchange_by_cid)),
    )
