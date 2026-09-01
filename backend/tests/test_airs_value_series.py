"""THE BOOK'S VALUE THROUGH TIME, AND THE RETURN THAT VALUE EARNED.

⚠⚠ THE ONE THING NEITHER SERIES MAY DO IS PRESENT A FUNDING AS PERFORMANCE. AzTopSelectie goes from
0 to EUR 1,000,000 on 2026-06-30 because it was paid in that day. The VALUE series answers that by
carrying the flows so the step can be marked; the RETURN series answers it by being AIRS's own
flow-aware `cumulatief_rendement` — measured, it stays at 0.00% straight through that funding. The
tests below pin both, plus the absences that would quietly break a line: an unvalued holding treated
as a zero, a page boundary losing a date, a pinned zero thinned away.

Unit-only: `_fake_supabase` stands in for the database.
"""
from __future__ import annotations

import pytest


@pytest.fixture
def store(monkeypatch):
    """`(module, seed)` — seed(table, rows) fills the fake, and may cap/shuffle it like PostgREST."""
    from tests._fake_supabase import FakeSupabase  # noqa: PLC0415

    from routers import _airs_value_series as m  # noqa: PLC0415

    tables: dict[str, list[dict]] = {}
    state: dict[str, object] = {}

    def seed(name: str, rows: list[dict], *, max_rows=None, unstable_ties=False) -> None:
        tables[name] = rows
        if max_rows is not None:
            state["max_rows"] = max_rows
        if unstable_ties:
            state["unstable_ties"] = True
        monkeypatch.setattr(m, "supabase", FakeSupabase(
            tables=tables, max_rows=state.get("max_rows"),
            unstable_ties=bool(state.get("unstable_ties"))))

    monkeypatch.setattr(m, "supabase", FakeSupabase(tables=tables))
    return m, seed


def _hold(pf: str, date: str, value, hid: int) -> dict:
    return {"id": hid, "portefeuille": pf, "as_of_date": date, "current_value_eur": value}


class TestTheValueIsTheSumOfWhatWeStored:
    def test_one_point_per_snapshot_date(self, store):
        m, seed = store
        seed("airs_holding", [_hold("B", "2026-08-01", 100.0, 1),
                                   _hold("B", "2026-08-01", 50.0, 2),
                                   _hold("B", "2026-08-02", 160.0, 3)])
        out = m.value_series("B")
        assert [(p["date"], p["value_eur"], p["holdings"]) for p in out["points"]] == [
            ("2026-08-01", 150.0, 2), ("2026-08-02", 160.0, 1)]
        assert (out["first_date"], out["last_date"]) == ("2026-08-01", "2026-08-02")

    def test_an_unvalued_holding_is_skipped_not_counted_as_zero(self, store):
        """⚠ A NULL IS NOT A ZERO. Counted as one it would pull the whole book down by whatever
        that position is worth, on one date, and read as a fall the book never had."""
        m, seed = store
        seed("airs_holding", [_hold("B", "2026-08-01", 100.0, 1),
                                   _hold("B", "2026-08-01", None, 2)])
        [p] = m.value_series("B")["points"]
        assert (p["value_eur"], p["holdings"]) == (100.0, 1)

    def test_another_book_is_not_in_it(self, store):
        m, seed = store
        seed("airs_holding", [_hold("B", "2026-08-01", 100.0, 1),
                                   _hold("OTHER", "2026-08-01", 999.0, 2)])
        assert m.value_series("B")["points"][0]["value_eur"] == 100.0

    def test_it_pages_and_orders_on_a_unique_key(self, store):
        """⚠⚠ POSTGREST TRUNCATES SILENTLY at 1,000 rows on cloud and 10,000 locally, so an
        unpaged read gives a different answer per environment — and a series short by a page is a
        chart with a cliff in it. The fake caps at the cloud limit and shuffles ties, which is what
        an ORDER BY on `as_of_date` alone would lose rows to."""
        m, seed = store
        rows = [_hold("B", f"2026-08-{1 + (i // 400):02d}", 1.0, i) for i in range(1200)]
        seed("airs_holding", rows, max_rows=1000, unstable_ties=True)
        out = m.value_series("B")
        assert sum(p["holdings"] for p in out["points"]) == 1200
        assert sum(p["value_eur"] for p in out["points"]) == 1200.0


class TestTheFlowsTravelWithIt:
    def test_a_funding_is_reported_so_it_cannot_be_drawn_as_performance(self, store):
        m, seed = store
        seed("airs_holding", [_hold("B", "2026-06-30", 1_000_000.0, 1)])
        seed("airs_performance", [
            {"id": 1, "portefeuille": "B", "periode": "2026-06-30",
             "stortingen": 1_000_000.0, "onttrekkingen": 0.0}])
        assert m.value_series("B")["flows"] == [
            {"date": "2026-06-30", "deposits_eur": 1_000_000.0, "withdrawals_eur": 0.0}]

    def test_a_month_with_no_money_moving_is_not_a_flow(self, store):
        """⚠ EVERY period carries a `stortingen` column, nearly always 0. Emitting those would put
        a marker on every date on the axis, which is the same as marking none of them."""
        m, seed = store
        seed("airs_holding", [_hold("B", "2026-07-31", 10.0, 1)])
        seed("airs_performance", [
            {"id": 1, "portefeuille": "B", "periode": "2026-07-31",
             "stortingen": 0.0, "onttrekkingen": 0.0}])
        assert m.value_series("B")["flows"] == []


class TestNothingToDraw:
    def test_a_book_with_no_snapshots_answers_emptily_rather_than_raising(self, store):
        m, _seed = store
        out = m.value_series("B")
        assert out["points"] == [] and out["first_date"] is None


class TestBeforeOurFirstSnapshotItIsAirsOwnClose:
    """⚠⚠ THE QUESTION THIS ANSWERS IS "WHY DOES THIS START IN AUGUST?". Our snapshots begin
    2026-06-23 at the earliest and, on AITopSelectie and BUS_Offensief, 2026-07-30 — while
    `airs_performance` has held month-ends since 2026-01-31 (AITopSelectie: 1,044,066 in January).
    Refusing six months of history we already store, to keep the series pure, answers a question
    nobody asked; drawing the two in the same ink would be the other mistake."""

    def test_it_prepends_the_months_we_have_no_snapshots_for(self, store):
        m, seed = store
        seed("airs_holding", [_hold("B", "2026-07-30", 1_274_804.0, 1)])
        seed("airs_performance", [
            {"id": 1, "portefeuille": "B", "periode": "2026-01-31", "eindvermogen": 1_044_066.0},
            {"id": 2, "portefeuille": "B", "periode": "2026-06-30", "eindvermogen": 1_551_994.0},
            {"id": 3, "portefeuille": "B", "periode": "2026-07-30", "eindvermogen": 1_274_804.0}])
        out = m.value_series("B")
        assert [(p["date"], p["source"]) for p in out["points"]] == [
            ("2026-01-31", "airs"), ("2026-06-30", "airs"), ("2026-07-30", "holdings")]
        assert out["own_from"] == "2026-07-30"

    def test_a_date_we_have_ourselves_is_never_taken_from_airs(self, store):
        """⚠ OURS WINS ON A SHARED DATE — it has the positions behind it, and a duplicate would put
        two points on one x. The AIRS row for 2026-07-30 above is dropped for exactly this reason."""
        m, seed = store
        seed("airs_holding", [_hold("B", "2026-07-30", 999.0, 1)])
        seed("airs_performance", [
            {"id": 1, "portefeuille": "B", "periode": "2026-07-30", "eindvermogen": 111.0}])
        [p] = m.value_series("B")["points"]
        assert (p["value_eur"], p["source"], p["holdings"]) == (999.0, "holdings", 1)

    def test_leading_zeros_are_dropped_because_the_book_did_not_exist_yet(self, store):
        """⚠ AIRS reports 0.00 for every month before a book is funded. A line running along zero
        for five months and then jumping draws an absence as a measurement — and the jump reads as
        performance, which is the one thing this chart must never say."""
        m, seed = store
        seed("airs_holding", [_hold("B", "2026-06-30", 1_000_000.0, 1)])
        seed("airs_performance", [
            {"id": 1, "portefeuille": "B", "periode": "2026-01-31", "eindvermogen": 0.0},
            {"id": 2, "portefeuille": "B", "periode": "2026-05-31", "eindvermogen": 0.0}])
        assert m.value_series("B")["points"] == [
            {"date": "2026-06-30", "value_eur": 1_000_000.0, "holdings": 1, "source": "holdings"}]

    def test_an_interior_zero_is_kept(self, store):
        """⚠ ONLY THE LEADING ONES. A book emptied mid-year really was worth nothing that month,
        and dropping that point would draw a straight line over the event."""
        m, seed = store
        seed("airs_holding", [_hold("B", "2026-08-01", 50.0, 1)])
        seed("airs_performance", [
            {"id": 1, "portefeuille": "B", "periode": "2026-05-31", "eindvermogen": 100.0},
            {"id": 2, "portefeuille": "B", "periode": "2026-06-30", "eindvermogen": 0.0},
            {"id": 3, "portefeuille": "B", "periode": "2026-07-31", "eindvermogen": 40.0}])
        assert [p["value_eur"] for p in m.value_series("B")["points"]] == [100.0, 0.0, 40.0, 50.0]

    def test_an_airs_point_carries_no_holding_count(self, store):
        """⚠ NULL, NOT 0. AIRS reports a total with no positions behind it; a count of 0 there
        would read as a scrape that found nothing, which is a different and alarming fact."""
        m, seed = store
        seed("airs_holding", [_hold("B", "2026-08-01", 50.0, 1)])
        seed("airs_performance", [
            {"id": 1, "portefeuille": "B", "periode": "2026-05-31", "eindvermogen": 100.0}])
        assert m.value_series("B")["points"][0]["holdings"] is None


def _perf(pf: str, periode: str, **cols) -> dict:
    """One `airs_performance` row. ⚠ NO `id` COLUMN IN THE REAL TABLE — it is keyed
    `(portefeuille, periode)`, which is why `_page` is asked for no tiebreak here."""
    return {"portefeuille": pf, "periode": periode, **cols}


class TestTheReturnIsAirsOwnAndStartsTheYearAtZero:
    """⚠⚠ READ, NEVER DERIVED FROM THE VALUES BESIDE IT. `cumulatief_rendement` is flow-aware and
    a ratio of two of our own snapshots is not — and it is the same column the Analyse modal's
    `Return` chip reads through `_airs_accounts._year_perf`, so a second derivation here would put
    two disagreeing YTD figures in one row of one screen."""

    def test_the_curve_is_pinned_at_zero_on_the_first_of_the_first_month(self, store):
        """⚠ THE PERIOD'S OPENING, NOT THE PERIOD. January's `cumulatief_rendement` is the return
        earned OVER January, so the 0% it grew from belongs on the 1st — anchored on the 31st it
        would draw January's move as a vertical step out of nothing."""
        m, seed = store
        seed("airs_performance", [
            _perf("B", "2026-01-31", beginvermogen=1e6, eindvermogen=9.7e5,
                  cumulatief_rendement=-3.0),
            _perf("B", "2026-02-28", beginvermogen=9.7e5, eindvermogen=1.01e6,
                  cumulatief_rendement=1.0)])
        out = m.value_series("B")
        assert out["return_from"] == "2026-01-01"
        assert [(p["date"], p["cum_pct"]) for p in out["returns"]] == [
            ("2026-01-01", 0.0), ("2026-01-31", -3.0), ("2026-02-28", 1.0)]
        assert out["return_pct"] == 1.0

    def test_every_period_is_a_point_because_each_is_year_to_date(self, store):
        """⚠⚠ THIS IS EXACTLY WHAT `_year_perf` MUST NOT DO. It takes the freshest row per MONTH,
        because its money columns are per-period and June's seven rows would be counted seven
        times. Nothing is summed here: `cumulatief_rendement` is YTD as of its own `periode`, so
        the intra-month rows are real observations and dropping them would throw away the daily
        half of the line."""
        m, seed = store
        seed("airs_performance", [
            _perf("B", "2026-06-30", beginvermogen=1e6, eindvermogen=1e6,
                  cumulatief_rendement=0.0),
            _perf("B", "2026-07-05", beginvermogen=1e6, eindvermogen=1.02e6,
                  cumulatief_rendement=2.0),
            _perf("B", "2026-07-09", beginvermogen=1e6, eindvermogen=1.04e6,
                  cumulatief_rendement=4.0),
            _perf("B", "2026-07-31", beginvermogen=1e6, eindvermogen=1.03e6,
                  cumulatief_rendement=3.0)])
        assert [p["date"] for p in m.value_series("B")["returns"]] == [
            "2026-06-01", "2026-06-30", "2026-07-05", "2026-07-09", "2026-07-31"]

    def test_a_funding_does_not_move_the_return_line(self, store):
        """⚠⚠ THE MEASURED CASE, AND THE REASON THIS SERIES IS READ RATHER THAN COMPUTED.
        AzTopSelectie is funded EUR 1,000,000 on 2026-06-30: the VALUE series steps from nothing to
        seven figures, and AIRS's return stays at 0.00% because no money was earned. A curve built
        from two of our own values cannot tell those apart."""
        m, seed = store
        seed("airs_holding", [_hold("B", "2026-06-30", 1_000_000.0, 1)])
        seed("airs_performance", [
            _perf("B", "2026-05-31", beginvermogen=0.0, eindvermogen=0.0,
                  cumulatief_rendement=0.0),
            _perf("B", "2026-06-30", beginvermogen=0.0, eindvermogen=1e6, stortingen=1e6,
                  cumulatief_rendement=0.0),
            _perf("B", "2026-07-09", beginvermogen=1e6, eindvermogen=1_000_818.0,
                  cumulatief_rendement=0.0818)])
        out = m.value_series("B")
        # ⚠ THE MONTH THE BOOK DID NOT EXIST IS NOT THE START OF THE YEAR — see below.
        assert out["return_from"] == "2026-06-01"
        assert [p["cum_pct"] for p in out["returns"]] == [0.0, 0.0, 0.0818]
        # The value series still has the step, and still carries the flow that explains it.
        assert out["flows"] == [
            {"date": "2026-06-30", "deposits_eur": 1_000_000.0, "withdrawals_eur": 0.0}]

    def test_the_months_before_a_book_was_funded_are_not_its_year(self, store):
        """⚠ AIRS writes a row for every month of the year the moment a book exists, so an account
        funded in June carries five months of 0.00 before it. Pinned at 0% across them the line
        says the book was flat when it was absent — and the curve's origin moves to a January it
        was never invested in."""
        m, seed = store
        seed("airs_performance", [
            _perf("B", "2026-01-31", beginvermogen=0.0, eindvermogen=0.0,
                  cumulatief_rendement=0.0),
            _perf("B", "2026-04-30", beginvermogen=0.0, eindvermogen=0.0,
                  cumulatief_rendement=0.0),
            _perf("B", "2026-05-31", beginvermogen=0.0, eindvermogen=5e5,
                  cumulatief_rendement=0.0),
            _perf("B", "2026-06-30", beginvermogen=5e5, eindvermogen=5.1e5,
                  cumulatief_rendement=2.0)])
        out = m.value_series("B")
        assert out["return_from"] == "2026-05-01"
        assert [p["date"] for p in out["returns"]] == [
            "2026-05-01", "2026-05-31", "2026-06-30"]

    def test_only_the_newest_year_because_the_figure_restarts_each_january(self, store):
        """⚠ `cumulatief_rendement` resets on 1 January. A series spanning two years would chart it
        across the point where it resets — a fall to zero that never happened. The year is the
        newest one PRESENT, never today's: a table not refreshed since New Year would then answer
        nothing at all."""
        m, seed = store
        seed("airs_performance", [
            _perf("B", "2025-11-30", beginvermogen=9e5, eindvermogen=9.9e5,
                  cumulatief_rendement=40.0),
            _perf("B", "2025-12-31", beginvermogen=9.9e5, eindvermogen=1e6,
                  cumulatief_rendement=42.0),
            _perf("B", "2026-01-31", beginvermogen=1e6, eindvermogen=1.01e6,
                  cumulatief_rendement=1.0)])
        out = m.value_series("B")
        assert out["return_from"] == "2026-01-01"
        assert [p["cum_pct"] for p in out["returns"]] == [0.0, 1.0]

    def test_the_value_rides_along_where_we_hold_a_snapshot_and_is_null_where_we_do_not(self, store):
        """⚠ ONE HOVER ANSWERS BOTH QUESTIONS — how well, and how much — without a second line on a
        104px plot. NULL, never 0, on the dates AIRS published a return for while we were not yet
        scraping: `€0` beside a real percentage reads as a book that emptied."""
        m, seed = store
        seed("airs_holding", [_hold("B", "2026-07-09", 1_040_000.0, 1),
                              _hold("B", "2026-07-09", 10_000.0, 2)])
        seed("airs_performance", [
            _perf("B", "2026-06-30", beginvermogen=1e6, eindvermogen=1e6,
                  cumulatief_rendement=0.0),
            _perf("B", "2026-07-09", beginvermogen=1e6, eindvermogen=1.05e6,
                  cumulatief_rendement=5.0)])
        got = [(p["date"], p["value_eur"], p["holdings"]) for p in m.value_series("B")["returns"]]
        assert got == [("2026-06-01", None, None), ("2026-06-30", None, None),
                       ("2026-07-09", 1_050_000.0, 2)]

    def test_a_period_with_no_published_return_is_a_gap_not_a_zero(self, store):
        """⚠ A row stored before AIRS published its return is a hole in the curve. Drawn as 0% it
        is a round trip to flat that never happened."""
        m, seed = store
        seed("airs_performance", [
            _perf("B", "2026-01-31", beginvermogen=1e6, eindvermogen=1.05e6,
                  cumulatief_rendement=5.0),
            _perf("B", "2026-02-28", beginvermogen=1.05e6, eindvermogen=1.06e6),
            _perf("B", "2026-03-31", beginvermogen=1.06e6, eindvermogen=1.07e6,
                  cumulatief_rendement=7.0)])
        assert [(p["date"], p["cum_pct"]) for p in m.value_series("B")["returns"]] == [
            ("2026-01-01", 0.0), ("2026-01-31", 5.0), ("2026-03-31", 7.0)]

    def test_a_lone_anchor_is_not_a_series(self, store):
        """⚠ ONE PINNED ZERO DRAWN AS A LINE STATES A SHAPE THE DATA DOES NOT HAVE. The caller's
        "no return published yet" sentence is the honest answer, so the anchor is withheld rather
        than shipped alone."""
        m, seed = store
        seed("airs_performance", [_perf("B", "2026-01-31", beginvermogen=1e6, eindvermogen=1e6)])
        out = m.value_series("B")
        assert out["returns"] == [] and out["return_from"] is None and out["return_pct"] is None

    def test_a_book_with_no_performance_rows_answers_emptily(self, store):
        m, seed = store
        seed("airs_holding", [_hold("B", "2026-08-01", 100.0, 1)])
        out = m.value_series("B")
        assert out["returns"] == [] and out["return_pct"] is None
        assert out["points"]          # the value series is unaffected

    def test_another_book_is_not_in_it(self, store):
        m, seed = store
        seed("airs_performance", [
            _perf("B", "2026-01-31", beginvermogen=1e6, eindvermogen=1e6,
                  cumulatief_rendement=1.0),
            _perf("OTHER", "2026-02-28", beginvermogen=1e6, eindvermogen=1e6,
                  cumulatief_rendement=99.0)])
        assert m.value_series("B")["return_pct"] == 1.0

    def test_it_pages_the_performance_read(self, store):
        """⚠⚠ THE NEWEST ROWS ARE THE ONES A TRUNCATED READ LOSES. `periode` ascends, so a cap
        that binds cuts the END of the curve — which is the figure the header reports. This is the
        failure that once served June's YTD in production while July sat unread; here it would show
        as a chart that simply stops in the middle of the year with nothing to say so.

        ⚠ ORDERED ON `periode` ALONE AND THAT IS SOUND: it is half the primary key and the read is
        filtered to one book, so it cannot tie. There is no `id` column to break one with."""
        m, seed = store
        # ⚠ DAYS 02..29, never the 1st: a period landing on the anchor's own date is dropped as a
        # duplicate x, and an off-by-one there would make this test pass for the wrong reason.
        rows = [_perf("B", f"2026-{1 + i // 28:02d}-{2 + i % 28:02d}",
                      beginvermogen=1e6, eindvermogen=1e6, cumulatief_rendement=float(i))
                for i in range(1100)]
        seed("airs_performance", rows, max_rows=1000)
        out = m.value_series("B")
        assert len(out["returns"]) == 1101        # 1,100 published points + the pinned zero
        assert out["return_pct"] == 1099.0
