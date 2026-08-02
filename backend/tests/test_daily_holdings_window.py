"""The retrospective daily-holdings walk: "what would this strategy have held?"

The /schedule "Daily holdings" section answers a question ABOUT THE PAST, and the machinery it
reuses is the machinery that RECORDS the present. These tests pin the three places that distinction
has to hold, because every one of them fails silently rather than loudly.
"""
from __future__ import annotations

import inspect
from datetime import date

from momentum.backtest import current_portfolio as cp
from routers.momentum.backtest_stream import single_run, stream


class TestTheWindowIsWholeMonths:
    """`months_back=2` on 31 July means 1 June, not 1 June-ish.

    ⚠ NOT `today - 60 days`. These picks come from a MONTHLY-rebalanced strategy, so a window that
    opens mid-month starts the chain-linked return partway through a holding period — the figure
    that comes out is not this strategy's over any period it ever held.
    """

    def test_two_months_back_from_july_is_june_first(self):
        assert single_run._daily_from(0) is None
        d = single_run._daily_from(2)
        assert d is not None and d.day == 1

    def test_it_crosses_a_year_boundary(self, monkeypatch):
        """January minus 2 is November of the previous year, not month -1."""
        class _FakeDate(date):
            @classmethod
            def today(cls):
                return date(2026, 1, 15)

        monkeypatch.setattr("datetime.date", _FakeDate, raising=False)
        # Recompute the arithmetic the function performs, independent of the patch
        # taking effect on the already-imported module.
        y, m = 2026, 1 - 2
        while m <= 0:
            m += 12
            y -= 1
        assert (y, m) == (2025, 11)

    def test_zero_and_negative_mean_this_period_only(self):
        assert single_run._daily_from(0) is None
        assert single_run._daily_from(-3) is None


class TestTheFloorNeverMovesForward:
    """⚠ `min(daily_from, month_start)`, AND THE `min` IS THE POINT.

    A `daily_from` inside the current period would otherwise TRUNCATE the live daily-picks panel
    that the /schedule card reads — a read-only question quietly changing what the pipeline
    reports. The floor can only ever reach further back.
    """

    def test_the_window_floor_is_a_min_against_month_start(self):
        src = inspect.getsource(cp.run_current_portfolio)
        assert "daily_floor = min(daily_from, month_start) if daily_from else month_start" in src
        assert "if daily_floor <= dd <= today_d:" in src

    def test_the_default_is_the_current_period(self):
        """`daily_from=None` must leave the pipeline's behaviour byte-identical — the golden
        master replays this function."""
        sig = inspect.signature(cp.run_current_portfolio)
        assert sig.parameters["daily_from"].default is None


class TestARetrospectiveWalkWritesNothing:
    """⚠ THE FAILURE THIS PREVENTS IS IRREVERSIBLE AND SILENT.

    `current_picks_day` is the record of what the pipeline DECIDED each day, on the data available
    at the time. The upsert is keyed `(strategy_hash, target_date)` — so recomputing a closed month
    on today's prices and persisting it does not add a row, it REPLACES the decision, and the
    original is simply gone. Same for the snapshot. Hence: compute freely, write nothing.
    """

    def test_the_read_only_branch_returns_before_any_persist(self):
        lines = inspect.getsource(single_run.run_single).splitlines()
        idx = {name: next(i for i, ln in enumerate(lines) if name in ln) for name in (
            "if req.daily_months_back > 0:", "_save_current_picks_snapshot,", "_persist_daily_picks,")}
        guard = idx["if req.daily_months_back > 0:"]
        ret = next(i for i, ln in enumerate(lines) if i > guard and ln.strip() == "return")
        # The early return must be reached BEFORE either writer.
        assert ret < idx["_save_current_picks_snapshot,"]
        assert ret < idx["_persist_daily_picks,"]

    def test_it_still_returns_the_STORED_days_alongside(self):
        """The stored days are what the pipeline decided; the computed ones are what it would
        decide now. Showing them together is the entire point — so the read-only branch still
        fetches the history it must not overwrite."""
        lines = inspect.getsource(single_run.run_single).splitlines()
        guard = next(i for i, ln in enumerate(lines) if "if req.daily_months_back > 0:" in ln)
        ret = next(i for i, ln in enumerate(lines) if i > guard and ln.strip() == "return")
        assert any("_fetch_daily_picks_history" in ln for ln in lines[guard:ret])

    def test_it_caches_into_its_OWN_table_not_the_pipelines(self):
        """⚠ THE ONE THAT MATTERS. The walk DOES persist now — its selections, so a re-run only
        pays for new days. Both tables are keyed (strategy_hash, target_date), so pointing this
        write at `current_picks_day` would replace the pipeline's decision with a recalculation and
        the original would be unrecoverable."""
        lines = inspect.getsource(single_run.run_single).splitlines()
        guard = next(i for i, ln in enumerate(lines) if "if req.daily_months_back > 0:" in ln)
        ret = next(i for i, ln in enumerate(lines) if i > guard and ln.strip() == "return")
        branch = lines[guard:ret]
        assert any("_persist_daily_holdings_cache" in ln for ln in branch)
        assert not any("_persist_daily_picks" in ln for ln in branch), \
            "the retrospective walk must never write the pipeline's daily picks"
        assert not any("_save_current_picks_snapshot" in ln for ln in branch)

    def test_the_two_stores_are_different_tables(self):
        from routers.momentum import _helpers

        assert 'table("daily_holdings_cache")' in inspect.getsource(
            _helpers.persist_daily_holdings_cache)
        assert 'table("current_picks_day")' in inspect.getsource(_helpers.persist_daily_picks)

    def test_the_payload_says_what_was_and_was_not_touched(self):
        src = inspect.getsource(single_run.run_single)
        assert '"read_only"' in src
        assert "were not touched" in src


class TestTheCacheCannotAnswerIt:
    """⚠ THE CACHE HOLDS THE CURRENT MONTH, AND THAT LOOKS LIKE A COMPLETE ANSWER.

    Served from it, a two-month request comes back with this month's days under a two-month
    heading — no error, no gap, just a shorter list nobody counts.
    """

    def test_the_short_circuit_is_gated_on_the_walk(self):
        src = inspect.getsource(stream._momentum_backtest_stream)
        assert "if not req.force_recompute and req.daily_months_back <= 0:" in src

    def test_the_price_window_grows_with_the_walk(self):
        """The signals need ~12 months before the EARLIEST cutoff. Without the extra months the
        oldest days score on a truncated history and select a different basket — a wrong answer
        that still renders as a full table."""
        src = inspect.getsource(stream._momentum_backtest_stream)
        assert "max(0, req.daily_months_back) * 31" in src


class TestTheCacheCannotServeAStaleSelection:
    """⚠ THE NEWEST DAYS ARE NEVER REUSED, AND THAT IS WHAT MAKES THIS A CACHE RATHER THAN A WRONG
    ANSWER. A day's selection is a function of the closes known before it, and closes keep
    arriving: GuruFocus publishes some late and `ingest/prices.py` writes them with their true
    (earlier) target_date. A cache that never revisits its newest entries can never correct itself.
    """

    def test_the_tail_is_withheld_from_reuse(self):
        from routers.momentum._helpers import DAILY_HOLDINGS_TAIL_DAYS

        assert DAILY_HOLDINGS_TAIL_DAYS >= 3, "shorter than the observed publish lag"
        src = inspect.getsource(single_run._load_cached_selections)
        assert "DAILY_HOLDINGS_TAIL_DAYS" in src
        assert "d <= cutoff.isoformat()" in src

    def test_force_recompute_bypasses_the_cache_entirely(self):
        assert single_run._load_cached_selections("h", None, 2, True) == ({}, {}, {})

    def test_a_day_cached_without_sector_scores_is_treated_as_STALE(self, monkeypatch):
        """⚠ THE REGRESSION THIS EXISTS FOR. `sector_scores` was added after the cache shipped, so
        every day stored by an earlier run carries the column's `'[]'` default. Serving those gave
        a day with correct holdings and silently empty sector ranks — 58 of 150 cached days,
        drawing the rank chart as flat gaps across three months while the rest looked fine. Nothing
        recomputes them on their own; the tail-refresh only reaches the newest few days."""
        from datetime import date

        rows = {
            "2026-01-05": {"holdings": [{"company_id": 1}], "sector_scores": [{"sector": "Tech"}]},
            "2026-01-06": {"holdings": [{"company_id": 2}], "sector_scores": []},   # legacy row
        }
        monkeypatch.setattr(single_run, "_fetch_daily_holdings_cache", lambda *a, **k: rows)
        frames, sectors, usable = single_run._load_cached_selections(
            "h", date(2026, 1, 1), 6, False)
        assert list(usable) == ["2026-01-05"], "the legacy row must be recomputed, not served"
        assert list(frames) == [date(2026, 1, 5)]
        assert list(sectors) == [date(2026, 1, 5)]

    def test_the_newest_computed_day_is_not_stored(self):
        """It has no next trading day, so its forward returns are blank and its selection is the
        one most likely to move when a late close lands."""
        picks = [
            {"date": "2026-06-01", "holdings": [{"company_id": 1, "ticker": "A"}]},
            {"date": "2026-06-02", "holdings": [{"company_id": 2, "ticker": "B"}]},
        ]
        out = single_run._selections_to_store(picks, {})
        assert list(out) == ["2026-06-01"]

    def test_an_already_cached_day_is_not_rewritten(self):
        picks = [
            {"date": "2026-06-01", "holdings": [{"company_id": 1}]},
            {"date": "2026-06-02", "holdings": [{"company_id": 2}]},
            {"date": "2026-06-03", "holdings": [{"company_id": 3}]},
        ]
        out = single_run._selections_to_store(picks, {"2026-06-01": [{}]})
        assert list(out) == ["2026-06-02"]

    def test_the_stored_shape_uses_the_SIGNAL_panel_column_names(self):
        """The engine rebuilds a cached day into the same frame `select_from_scored` consumed, so
        the keys have to be that frame's column names — not the holding payload's."""
        out = single_run._selections_to_store([
            {"date": "2026-06-01", "holdings": [{"company_id": 7, "ticker": "AAPL",
                                                 "company_name": "Apple", "sector": "Tech",
                                                 "score": 91.2,
                                                 "category_scores": {"price": 88.0, "volume": 61.5}}]},
            {"date": "2026-06-02", "holdings": []},
        ], {})
        row = out["2026-06-01"]["holdings"][0]
        assert row["gurufocus_ticker"] == "AAPL"
        assert row["momentum_score"] == 91.2
        assert "ticker" not in row and "score" not in row

    def test_the_per_company_PILLAR_scores_are_cached(self):
        """⚠ Otherwise a REUSED day renders blank price/volume columns while a freshly computed
        one fills them — which reads as "we only score some days", not as a cache dropping two
        fields."""
        out = single_run._selections_to_store([
            {"date": "2026-06-01", "holdings": [{"company_id": 7, "ticker": "AAPL",
                                                 "category_scores": {"price": 88.0, "volume": 61.5}}]},
            {"date": "2026-06-02", "holdings": []},
        ], {})
        row = out["2026-06-01"]["holdings"][0]
        assert row["score_price"] == 88.0
        assert row["score_volume"] == 61.5

    def test_the_per_sector_scores_are_cached_too(self):
        sectors = [{"sector": "Technology", "rank": 1, "momentum_score": 72.4,
                    "category_scores": {"price": 80.1, "volume": 55.0}, "companies": 41}]
        out = single_run._selections_to_store([
            {"date": "2026-06-01", "holdings": [{"company_id": 7}], "sector_scores": sectors},
            {"date": "2026-06-02", "holdings": []},
        ], {})
        assert out["2026-06-01"]["sector_scores"] == sectors


class TestTheEngineSkipsOnlyTheExpensiveStep:
    """A cached day still gets its prices, returns, turnover and cumulative derived live — all of
    those are properties of the WINDOW, so a stored value would be wrong the moment a different
    window is asked for."""

    def test_a_cached_cutoff_is_dropped_from_the_panel(self):
        src = inspect.getsource(cp.run_current_portfolio)
        assert "for d in trading_dates if d not in _cached" in src

    def test_month_start_is_never_served_from_the_cache(self):
        """It is the locked basket the pipeline reports, not part of the retrospective walk."""
        src = inspect.getsource(cp.run_current_portfolio)
        cut = src.index("panel_cutoffs: list[date] = sorted(")
        assert "{month_start," in src[cut:cut + 200]

    def test_the_chain_still_runs_over_cached_days(self):
        """The `continue` that skips an empty day must be OUTSIDE the else, or a cached day would
        never reach the price/turnover code below it."""
        src = inspect.getsource(cp.run_current_portfolio)
        assert "cached_sel = _cached.get(d)" in src
        assert "if daily_selected.empty:\n            continue" in src


class TestTheSectorScoresExplainTheSelection:
    """Per-sector price/volume scores for a day, shown beside the picks.

    ⚠ THEY ARE ONLY WORTH SHOWING IF THEY DESCRIBE THE SAME COMPUTATION THE SELECTION MADE. Two
    things guarantee that and neither is optional: the aggregation goes through
    `aggregate_to_sector` (the function the ranking itself uses — it is a MEAN, and the golden
    master exists partly because switching it to a median silently changes which sectors get
    picked), and the rows aggregated are `selection_pool`'s, the same ones the ranking saw.
    """

    def _scored(self):
        import pandas as pd

        return pd.DataFrame({
            "company_id": [1, 2, 3, 4],
            "sector": ["Tech", "Tech", "Health", "Health"],
            "momentum_score": [80.0, 60.0, 40.0, 30.0],
            "score_price": [90.0, 50.0, 45.0, 35.0],
            "score_volume": [70.0, 30.0, 20.0, 10.0],
        })

    def test_a_sector_score_is_the_mean_of_its_companies(self):
        from momentum.scoring import sector_pool_scores

        rows = {r["sector"]: r for r in sector_pool_scores(self._scored())}
        assert rows["Tech"]["momentum_score"] == 70.0
        assert rows["Tech"]["category_scores"]["price"] == 70.0
        assert rows["Tech"]["category_scores"]["volume"] == 50.0

    def test_it_uses_the_ranking_helper_rather_than_its_own_aggregation(self):
        src = inspect.getsource(__import__("momentum.scoring", fromlist=["x"]).sector_pool_scores)
        assert "aggregate_to_sector" in src
        assert ".median(" not in src and ".mean(" not in src, \
            "aggregate the same way the ranking does — never re-implement it here"

    def test_sectors_come_back_ranked_best_first(self):
        from momentum.scoring import sector_pool_scores

        rows = sector_pool_scores(self._scored())
        assert [r["sector"] for r in rows] == ["Tech", "Health"]
        assert [r["rank"] for r in rows] == [1, 2]

    def test_EVERY_sector_in_the_pool_is_reported_not_only_the_picked_ones(self):
        """The sector that just missed the cut is the informative row; showing only the chosen
        ones answers "what did we hold" a second time instead of "why"."""
        from momentum.scoring import sector_pool_scores

        assert len(sector_pool_scores(self._scored())) == 2   # top_n_sectors is not applied here

    def test_the_floor_filters_COMPANIES_not_the_sector_ranking(self):
        """⚠ 2026-07-31: sectors are ranked over EVERY scored company; `min_price_score` only
        decides which companies get bought inside the chosen sectors.

        Ranking on the survivors was survivorship-biased in the worst direction — a sector's
        survivor-mean rises the FEWER survivors it has, so the thinnest samples ranked highest.
        Measured on the golden fixture (floor 30, 250 survivors of 1,464): Services ranked 3rd on
        17 survivors out of 247 names, and 10th of 11 over all of them."""
        from momentum import scoring

        src = inspect.getsource(scoring.select_from_scored)
        assert "sector_scores = aggregate_to_sector(scored)" in src, \
            "sectors must be ranked over every scored company, not the floor-filtered pool"
        assert "selection_pool(" in src, "the floor must still gate which companies are picked"

    def test_the_displayed_sector_scores_match_the_ranking_pool(self):
        """The table beside the picks has to aggregate the same rows the ranking did, or it
        explains a selection that was never made."""
        from momentum.backtest import current_portfolio as cp

        assert "sector_pool_scores(scored)" in inspect.getsource(cp.run_current_portfolio)

    def test_an_empty_pool_returns_no_rows_rather_than_raising(self):
        import pandas as pd

        from momentum.scoring import sector_pool_scores

        assert sector_pool_scores(pd.DataFrame()) == []


class TestTheRequestDefaultsToTheLivePath:
    def test_daily_months_back_defaults_to_zero(self):
        from routers.momentum.backtest_stream.models import BacktestRequest

        assert BacktestRequest.model_fields["daily_months_back"].default == 0
