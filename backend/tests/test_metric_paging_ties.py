"""`_page_metrics` must page on a UNIQUE sort key, or it silently drops rows.

⚠⚠ THE MEASURED BUG, 2026-08-17. It paged with `.order("target_date")` and nothing else. A company
files ~110 metric codes on the SAME `target_date`, so with `_PAGE = 1000` every page boundary falls
inside a tie group — and Postgres makes no promise about the order of tied rows across separate
LIMIT/OFFSET queries. Some rows come back twice, others never.

The damage was invisible in every way that matters:

  * no error, no empty panel — just a blended line missing a point;
  * a DIFFERENT arbitrary row per company. On Bustelberg Offensief's FCF/share, ASML, Alphabet and
    Amazon each lost their **2018** figure and Berkshire its **2019**;
  * and it only showed up as a DISAGREEMENT with the Tables tab, which reads through the same
    `_page_metrics` but with `exact=True` — ONE metric code, ~28 rows, so it never reaches a page
    boundary at all. That is why one screen was right and the other was not.

It moved the book's 10-year FCF/share CAGR by 0.14pp. Small here; unbounded in principle, since
which row is lost is arbitrary.

⚠ THE PRIMARY KEY IS `(company_id, metric_code, source_code, target_date)`. `company_id` is pinned
by the filter, so ordering by the other three is total. Same failure and same fix as the FX pager
(`tests/test_fx_paging.py`) — this is the second time, which is why the fake now models it.
"""
from __future__ import annotations

import routers.earnings as E
from tests._fake_supabase import FakeSupabase


def _rows(codes: int, years: int) -> list[dict]:
    """One company, `codes` metric codes x `years` fiscal years — every code sharing each date."""
    return [
        {"company_id": 7, "metric_code": f"annuals__Section__Line {c:03d}",
         "source_code": "gurufocus", "target_date": f"{2000 + y}-12-31",
         "numeric_value": float(c * 1000 + y)}
        for y in range(years) for c in range(codes)
    ]


def _page(monkeypatch, rows: list[dict], *, unstable: bool, page: int = 100) -> list[dict]:
    fake = FakeSupabase({"metric_data": rows}, unstable_ties=unstable)
    monkeypatch.setattr(E, "supabase", fake)
    monkeypatch.setattr(E, "_PAGE", page)
    monkeypatch.setattr(E, "_BLEND_START", "1900-01-01")
    return E._page_metrics(7, r"annuals__%")


def _ids(rows: list[dict]) -> list[tuple]:
    return [(r["metric_code"], r["target_date"]) for r in rows]


class TestEveryRowComesBackExactlyOnce:
    """110 codes x 12 years = 1,320 rows over a 100-row page: 13 boundaries, every one of them
    inside a 110-row tie group."""

    def test_no_row_is_lost(self, monkeypatch):
        rows = _rows(codes=110, years=12)
        got = _page(monkeypatch, rows, unstable=True)
        missing = set(_ids(rows)) - set(_ids(got))
        assert not missing, (
            f"{len(missing)} row(s) never came back — e.g. {sorted(missing)[:3]}. A page boundary "
            f"landed inside a tie the ORDER BY could not separate.")

    def test_no_row_comes_back_twice(self, monkeypatch):
        rows = _rows(codes=110, years=12)
        got = _ids(_page(monkeypatch, rows, unstable=True))
        assert len(got) == len(set(got)), "a row was served on two different pages"

    def test_the_count_is_exact(self, monkeypatch):
        rows = _rows(codes=110, years=12)
        assert len(_page(monkeypatch, rows, unstable=True)) == len(rows)

    def test_it_still_works_when_ties_happen_to_be_stable(self, monkeypatch):
        """⚠ THE REASON THIS WENT UNNOTICED. With a stable server-side order the broken pager is
        indistinguishable from the fixed one — which is exactly what a local Postgres, a small
        table or a warm cache will hand you."""
        rows = _rows(codes=110, years=12)
        assert len(_page(monkeypatch, rows, unstable=False)) == len(rows)


class TestTheFakeActuallyReproducesIt:
    """⚠ A REGRESSION TEST THAT CANNOT FAIL PROVES NOTHING. If `unstable_ties` were a no-op the
    four tests above would pass against the ORIGINAL bug. This one pins that the harness bites:
    paging the same rows on a NON-unique key must lose some."""

    def test_a_non_unique_order_key_does_lose_rows(self, monkeypatch):
        rows = _rows(codes=110, years=12)
        fake = FakeSupabase({"metric_data": rows}, unstable_ties=True)

        out: list[dict] = []
        start = 0
        while True:
            page = (fake.table("metric_data").select("*")
                    .eq("company_id", 7)
                    .order("target_date")                 # ← the bug: date alone, ties unbroken
                    .range(start, start + 99).execute().data or [])
            out += page
            if len(page) < 100:
                break
            start += 100
        assert len(set(_ids(out))) < len(rows), (
            "the fake did not reorder tied rows, so these tests would pass against the bug")


class TestASingleCodeWasNeverAffected:
    """Why the Tables tab was right while the Long Equity card was wrong: `exact=True` reads ONE
    code — ~28 rows for a company — so it never reaches a page boundary, tie or no tie."""

    def test_exact_read_is_complete(self, monkeypatch):
        rows = _rows(codes=110, years=12)
        fake = FakeSupabase({"metric_data": rows}, unstable_ties=True)
        monkeypatch.setattr(E, "supabase", fake)
        monkeypatch.setattr(E, "_PAGE", 100)
        monkeypatch.setattr(E, "_BLEND_START", "1900-01-01")
        got = E._page_metrics(7, "annuals__Section__Line 042", exact=True)
        assert len(got) == 12
