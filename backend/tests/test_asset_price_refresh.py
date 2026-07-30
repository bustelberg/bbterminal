"""`asset_price` must not age silently.

`metric_data` (GuruFocus) has had a daily refresh for ever. `asset_price` (Yahoo) never did — it
was written when a row was added, and then it aged. Nothing tells you: a stale series still
returns prices, still charts, still computes a return. It is just an old one.

    Measured 2026-07-14, over the AIRS model portfolios' holdings:
        223 held instruments priced
          1 current
        197 STALE                                  <- 88%

And it is not cosmetic. A model whose window opens AFTER its holdings' last close has no price
INSIDE the window, so no return over it can exist and the row renders blank: Meta Platforms —
correctly mapped to META, 3,556 bars — showed nothing in BUS_2.0_NEU_FX (window 2026-07-09,
Meta's last close 2026-07-02). The mapping was never the problem.
"""
from __future__ import annotations

import inspect

from asset_pipeline import price_refresh


class TestStalenessIsRelativeToTheData:
    """⚠ NEVER to the calendar."""

    def test_the_anchor_is_the_freshest_close_we_hold(self):
        src = inspect.getsource(price_refresh.find_stale)
        assert "global_latest_close()" in src
        assert "_days_between(latest_all, last)" in src

    def test_the_calendar_would_flag_everything_every_weekend(self):
        """Anchored on today, a Saturday makes every instrument on earth 'stale'; a bank holiday
        makes it worse; and a total Yahoo outage would order a refresh of all 16k rows — the one
        moment refreshing is guaranteed to achieve nothing. The global anchor self-corrects: if
        nothing anywhere has published, nothing is stale."""
        src = inspect.getsource(price_refresh)
        assert "date.today()" not in src
        assert "NEVER AGAINST TODAY" in src

    def test_a_default_that_clears_a_weekend(self):
        assert price_refresh.DEFAULT_STALE_DAYS >= 3


class TestItFetchesTheGapNotTheHistory:
    def test_extend_first_full_refetch_only_as_a_fallback(self):
        """`store_series` re-downloads every bar an instrument ever had (KO: 16,239, back to
        1962) to add eight days. Over ~200 rows that is minutes vs seconds."""
        src = inspect.getsource(price_refresh.refresh_stale)
        assert "store.extend_series(aid, sym, was)" in src
        assert "store.store_series(aid, sym, None)" in src
        # ...and the full path is reached ONLY when extend cannot recompute the stats exactly.
        assert "is None" in src.split("extend_series", 1)[1].split("\n", 1)[0]

    def test_extend_recomputes_the_grid_stats_from_the_DATABASE(self):
        """The trap `extend_series` exists to avoid: `store_series` derives `price_from`/`bars`
        FROM THE ROWS IT FETCHED. Hand it a two-week window and the grid learns that KO has 8
        bars beginning in 2026. Those stats are not a cache — they ARE what `asset_grid` reads."""
        from asset_pipeline import store

        src = inspect.getsource(store.extend_series)
        assert "FROM asset_price WHERE analysis_id" in src     # stats from the DB, not the slice
        assert "return None" in src                            # ...or refuse and let the caller
                                                               #    do the full, correct fetch
    def test_it_does_not_trim_the_leading_bars_of_a_WINDOW(self):
        """`trim_leading_no_volume` trims the settlement-only head of a FULL series. In a window
        the 'head' is just wherever the window starts, and trimming it drops real bars."""
        from asset_pipeline import store

        # The CALL, not the word — `extend_series` explains in a comment why it must not trim.
        assert "trim_leading_no_volume(rows)" not in inspect.getsource(store.extend_series)
        assert "trim_leading_no_volume(rows)" in inspect.getsource(store.store_series)


class TestTheSchedulerJob:
    def test_it_stands_down_while_the_resolver_is_using_yahoo(self):
        """⚠ The ingest queue is THE single Yahoo consumer by design. Yahoo answers an overloaded
        caller with an EMPTY result rather than a 429, and an empty candidate set is how a
        resolution silently lands on a thin foreign listing (NVDA-on-Stuttgart, Alphabet-on-
        Vienna). Our own traffic is only chart fetches for symbols we already hold, so it cannot
        mis-resolve anything ITSELF — but it can push Yahoo into that regime while the resolver
        is mid-search and corrupt ITS work. A day-late price is a nuisance; a wrong listing is a
        wrong price series for ever."""
        import scheduler

        # The guard lives in the SHARED body, so neither the tick nor the startup catch-up can
        # slip past it.
        src = inspect.getsource(scheduler._run_asset_price_refresh)
        assert "if _q.is_worker_active():" in src
        assert "return" in src.split("if _q.is_worker_active():", 1)[1][:500]

    def test_it_yields_to_the_WORKER_not_to_the_BACKLOG(self):
        """The bug the first version shipped with. `pending > 0` is a count of what is LEFT, and
        it stays high precisely when nobody is working: this queue holds 9,945 pending ISINs last
        touched 2026-07-07 — a week earlier — so a `pending` gate skipped EVERY tick, for ever.
        (`status()["working"]` is that same lie: it is just `pending > 0`.)"""
        import scheduler
        from asset_pipeline import queue

        src = inspect.getsource(scheduler._fire_asset_price_refresh)
        assert '.get("pending")' not in src, "a backlog is not a worker"

        # The real heartbeat: when a row was last MOVED out of pending.
        heartbeat = inspect.getsource(queue.is_worker_active)
        assert "last_activity()" in heartbeat
        assert "within_minutes" in heartbeat
        assert 'in_("status", ["done", "failed"])' in inspect.getsource(queue.last_activity)

    def test_it_is_registered_daily_and_cannot_overlap_itself(self):
        import scheduler

        src = inspect.getsource(scheduler)
        assert 'id="asset_price_refresh"' in src
        assert "_fire_asset_price_refresh," in src
        # ~220 gap fetches at ~1.5s each; a slow run must never overlap the next day's tick.
        block = src.split('_fire_asset_price_refresh,\n', 1)[1].split("misfire_grace_time", 1)[0]
        assert "max_instances=1" in block
        assert 'hour=6' in block           # after the 05:00 sequence, not racing it

    def test_the_scheduler_and_the_script_share_ONE_implementation(self):
        """A cron that drifts from the script you debug with is a cron nobody trusts."""
        import scheduler

        assert "price_refresh.refresh_stale(" in inspect.getsource(
            scheduler._run_asset_price_refresh)


class TestTheStartupCatchUp:
    """A daily tick keeps prices current going FORWARD. It cannot repair the PAST.

    A backend that was down over a weekend, a fresh deploy, a machine that has not run in a week
    — all come up with stale held prices and, without this, would serve blank rows on /portfolios
    until 06:00 the next morning. That is the exact state the whole problem was found in.
    """

    def test_startup_fires_the_same_refresh(self):
        import scheduler

        src = inspect.getsource(scheduler._maybe_kickstart_asset_prices)
        assert "_run_asset_price_refresh" in src
        assert "startup catch-up" in src

    def test_it_is_wired_into_scheduler_start(self):
        import scheduler

        src = inspect.getsource(scheduler)
        assert "_maybe_kickstart_asset_prices()" in src
        # ...and a probe failure must never take down scheduler startup. (rsplit: the FIRST
        # occurrence of the name is its own `def`; the CALL is the last one.)
        after = src.rsplit("_maybe_kickstart_asset_prices()", 1)[1][:200]
        assert "except Exception" in after

    def test_it_DETECTS_before_it_fetches(self):
        """`uvicorn --reload` restarts constantly. A catch-up that fetched first would cost ~220
        Yahoo calls per keystroke-triggered reload just to discover there was nothing to do."""
        import scheduler

        src = inspect.getsource(scheduler._run_asset_price_refresh)
        find_at = src.index("find_stale(held_only=True)")
        fetch_at = src.index("refresh_stale(held_only=True)")
        assert find_at < fetch_at
        assert "if not stale:" in src
        assert "return" in src.split("if not stale:", 1)[1][:400]

    def test_the_startup_path_ALSO_yields_to_the_resolver(self):
        """The queue guard lives in the shared body, so it cannot be bypassed by the startup
        path — a restart mid-resolve is exactly when a second Yahoo consumer is most damaging."""
        import scheduler

        assert "if _q.is_worker_active():" in inspect.getsource(
            scheduler._run_asset_price_refresh)


class TestTheAnchorQueryMustNotSCAN:
    """⚠ THE ANCHOR ITSELF TOOK THE WHOLE JOB DOWN.

        postgrest.exceptions.APIError: {'message': 'canceling statement due to statement
        timeout', 'code': '57014'}    <- global_latest_close(), in production

    `SELECT target_date FROM asset_price ORDER BY target_date DESC LIMIT 1` reads like one
    indexed row and is not. `asset_price`'s ONLY index is the primary key
    `(analysis_id, target_date)` — nothing LEADS with `target_date`, so Postgres has no ordered
    path to the newest date and falls back to a full scan + top-N sort over 14M+ rows. And this
    is the FIRST thing `find_stale` asks for, so the timeout takes the entire refresh with it —
    on the daily tick AND on every startup catch-up.

    `latest_close_by_analysis` escapes the same trap only because its grouped aggregate goes over
    COPY, where the statement timeout is disabled. Per-analysis lookups are safe for a different
    reason: `analysis_id` is the PK's LEADING column, so they are genuine index seeks.
    """

    def test_it_reads_the_denormalized_max_and_never_touches_asset_price(self, monkeypatch):
        """`asset_analysis.price_to` is the same fact over a few thousand rows instead of
        fourteen million — denormalized by migration 20260703010000 for exactly this class of
        timeout, and maintained by `store_series` AND `extend_series`, the only two writers of
        `asset_price` there are."""
        from tests._fake_supabase import FakeSupabase

        class _Tripwire(FakeSupabase):
            def table(self, name: str):
                assert name != "asset_price", (
                    "global_latest_close() went back to asset_price — a 14M-row scan through "
                    "PostgREST, i.e. the 57014 this exists to avoid"
                )
                return super().table(name)

        fake = _Tripwire({"asset_analysis": [
            {"analysis_id": 1, "price_to": "2026-07-11"},
            {"analysis_id": 2, "price_to": "2026-07-23"},   # the freshest close we hold
            {"analysis_id": 3, "price_to": None},           # resolved but never priced
        ]})
        monkeypatch.setattr(price_refresh, "supabase", fake)
        # A NULL must be filtered, not sorted: descending, "no price at all" would otherwise
        # outrank every real date and the anchor would come back None — which reads as
        # "the table is empty", i.e. nothing is stale, i.e. a silent no-op.
        assert price_refresh.global_latest_close() == "2026-07-23"

    def test_the_exact_aggregate_survives_only_behind_COPY(self):
        """A database whose denormalized column was never populated still needs an answer — but
        `max(target_date)` over 14M rows may only run where `statement_timeout = 0`, which is
        `_run_copy` and nowhere else."""
        src = inspect.getsource(price_refresh.global_latest_close)
        assert "_run_copy(" in src
        assert "max(target_date)" in src.split("_run_copy(", 1)[1]
        # ...and the scan is NOT the primary path: this runs on every startup catch-up, and a
        # 14M-row seq scan per `uvicorn --reload` restart is not the near-free probe that
        # `test_it_DETECTS_before_it_fetches` promises.
        assert src.index("asset_analysis") < src.index("_run_copy(")


class TestACapMustSayItCapped:
    def test_a_limited_run_reports_what_it_skipped(self):
        """"6 refreshed" over a silent 197 reads like a clean bill of health."""
        src = inspect.getsource(price_refresh.refresh_stale)
        assert '"skipped": skipped' in src
        assert "total_stale - limit" in src
