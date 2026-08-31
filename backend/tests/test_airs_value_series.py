"""THE BOOK'S VALUE THROUGH TIME, SUMMED FROM OUR OWN SNAPSHOTS.

⚠⚠ THE ONE THING THIS MUST NOT DO IS PRESENT A FUNDING AS PERFORMANCE. AzTopSelectie goes from 0 to
EUR 1,000,000 on 2026-06-30 because it was paid in that day; a value series without its flows draws
that as a 100% gain in one session. So the flows travel with the points, and the tests below pin the
absences that would quietly break the line — an unvalued holding treated as a zero, a page boundary
losing a date.

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
