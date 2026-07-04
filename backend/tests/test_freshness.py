"""Per-exchange price-freshness classifier (`ingest.freshness`).

Pins the "is the universe fresh enough to rebalance?" logic: peers on an
exchange define its trading calendar (so a single-day market holiday isn't
mistaken for a lag), a name behind its peers is a re-fetchable laggard, a whole
exchange trailing the global pack is a stall, and excluded names don't count.
"""
from __future__ import annotations

from ingest.freshness import classify_universe_freshness

# A Mon–Fri week of ISO trading dates to anchor scenarios on.
FRI_PREV = "2026-06-26"
MON = "2026-06-29"
TUE = "2026-06-30"
WED = "2026-07-01"
THU = "2026-07-02"


def test_all_caught_up_is_fresh():
    latest = {1: THU, 2: THU, 3: THU}
    exch = {1: "NYSE", 2: "NYSE", 3: "XTKS"}
    r = classify_universe_freshness(latest, exch)
    assert set(r.fresh) == {1, 2, 3}
    assert r.lagging == [] and r.missing == []
    assert r.global_latest.isoformat() == THU
    assert r.fresh_fraction == 1.0


def test_name_behind_its_exchange_peers_is_lagging():
    # NYSE peers have Thursday; #2 is still on Wednesday → 1 day behind peers.
    latest = {1: THU, 2: WED, 3: THU}
    exch = {1: "NYSE", 2: "NYSE", 3: "NYSE"}
    r = classify_universe_freshness(latest, exch)
    assert r.lagging == [2]
    assert set(r.fresh) == {1, 3}
    assert r.to_fetch == [2]


def test_single_day_holiday_exchange_is_not_flagged():
    # XTKS closed Thursday (holiday) → its peers top out at Wednesday, one day
    # behind NYSE's global-latest Thursday. That's a holiday, not a lag: exch_gap
    # (1) < the 3-day stall tolerance and every XTKS name is caught up to its own
    # peers → all fresh, nothing to fetch.
    latest = {1: THU, 2: THU, 3: WED, 4: WED}
    exch = {1: "NYSE", 2: "NYSE", 3: "XTKS", 4: "XTKS"}
    r = classify_universe_freshness(latest, exch)
    assert set(r.fresh) == {1, 2, 3, 4}
    assert r.lagging == []


def test_whole_exchange_stall_flags_all_its_names():
    # XTKS is 4 trading days behind the global pack (Fri-prev vs Thu) — beyond
    # the 3-day tolerance → a stall, so every XTKS name is a laggard to fetch,
    # even though they agree with each other.
    latest = {1: THU, 2: THU, 3: FRI_PREV, 4: FRI_PREV}
    exch = {1: "NYSE", 2: "NYSE", 3: "XTKS", 4: "XTKS"}
    r = classify_universe_freshness(latest, exch)
    assert set(r.lagging) == {3, 4}
    assert set(r.fresh) == {1, 2}


def test_missing_data_is_its_own_bucket_and_fetched():
    latest = {1: THU, 2: None, 3: ""}
    exch = {1: "NYSE", 2: "NYSE", 3: "NYSE"}
    r = classify_universe_freshness(latest, exch)
    assert r.fresh == [1]
    assert set(r.missing) == {2, 3}
    assert set(r.to_fetch) == {2, 3}


def test_excluded_names_do_not_count():
    latest = {1: THU, 2: FRI_PREV, 3: THU}
    exch = {1: "NYSE", 2: "NYSE", 3: "NYSE"}
    # #2 lags badly but is delisted/out-of-scope → excluded, not a laggard.
    r = classify_universe_freshness(latest, exch, excluded_ids={2})
    assert r.excluded == [2]
    assert set(r.fresh) == {1, 3}
    assert r.lagging == [] and r.missing == []
    assert r.active_total == 2


def test_unknown_exchange_is_skipped():
    latest = {1: THU, 2: THU}
    exch = {1: "NYSE", 2: None}  # #2 has no exchange → can't be peer-anchored
    r = classify_universe_freshness(latest, exch)
    assert r.fresh == [1]
    assert 2 not in (r.fresh + r.lagging + r.missing)


def test_to_fetch_and_active_total_back_the_warning():
    # The rebalance op warns using report.to_fetch / active_total.
    latest = {1: THU, 2: THU, 3: THU, 4: WED, 5: None}
    exch = {1: "NYSE", 2: "NYSE", 3: "NYSE", 4: "NYSE", 5: "NYSE"}
    r = classify_universe_freshness(latest, exch)
    assert r.active_total == 5
    assert set(r.to_fetch) == {4, 5}  # 1 lagging + 1 missing


def test_empty_universe_has_nothing_to_fetch():
    r = classify_universe_freshness({}, {})
    assert r.to_fetch == [] and r.active_total == 0
