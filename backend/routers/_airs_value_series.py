"""THE BOOK'S VALUE THROUGH TIME, AND THE RETURN THAT VALUE EARNED.

⚠⚠ THE VALUE IS SUMMED FROM `airs_holding`, NOT READ FROM AIRS'S RENDEMENTEN. Every snapshot we
have ever stored carries one row per holding with its EUR value, so the book's value on that date is
their sum — a series this system owns rather than a figure fetched from a sheet. Measured on
AzTopSelectie_DYN (24 snapshots): it reproduces AIRS's own `eindvermogen` to the euro on 21 of
them, we hold two dates AIRS's sheet has no row for at all, and one date differs by EUR 8,888 where
the two disagree about which valuation the snapshot belongs to.

⚠⚠ AND THE RETURN IS AIRS'S OWN `cumulatief_rendement`, READ AND NEVER RECOMPUTED. That is the
whole reason a return can be drawn here at all: it is FLOW-AWARE. Our value series cannot become a
return by itself — AzTopSelectie goes from 0 to EUR 1,000,000 on 2026-06-30 because it was FUNDED
that day, and no ratio of two values can tell that apart from a gain. AIRS publishes the flows
(`stortingen`/`onttrekkingen`) and it publishes the answer; deriving a second answer from a value
series and a monthly flow column would part from theirs on every day a holding traded, and the two
would then sit one click apart on the same screen. So: read it.

⚠⚠ ONE `airs_performance` ROW IS ONE POINT ON THAT CURVE, WHICH IS NOT WHAT `_year_perf` DOES
WITH THEM. There the freshest row per MONTH is taken, because the money columns are per-period and
summing every row would count June seven times. Here nothing is summed: `cumulatief_rendement` is
year-to-date AS OF that row's `periode`, so every distinct `periode` is a real observation and all
of them are kept. Measured on BUS_FTS_DEF_DYN — month-ends until we began scraping, then a row per
scrape date, which is the same two-resolution shape the value series has:

    2026-01-31  -3.38%   2026-05-31 -10.08%   2026-06-23 -15.69%   2026-07-14  -7.42%
    2026-02-28  -7.01%   2026-06-05 -12.82%   2026-06-30 -14.59%   2026-07-20  -6.65%

⚠ THE ZERO IS THE START OF THE YEAR, NOT THE FIRST ROW. `cumulatief_rendement` restarts every
January, so the curve's origin is the opening of the first period we hold — the first day of that
month — pinned at exactly 0.0%. Without it the line starts at January's -3.38% and there is no
baseline on the chart to read the rest against.

⚠ ONE YEAR ONLY, for the same reason: a series spanning two years would chart a figure across the
point where it resets.

⚠ LEADING ZEROS ARE DROPPED, ON BOTH SERIES. AIRS reports `eindvermogen` 0.00 for every month
before a book was funded, and a line that runs along zero for five months then jumps is drawing an
absence as a measurement. An INTERIOR zero is kept: a book emptied mid-year really was worth
nothing.

⚠ THE POINTS ARE IRREGULAR. A snapshot exists only for a day the scrape ran AND AIRS had valued the
book, so gaps are the norm — a chart must plot against real dates rather than treating the points as
evenly spaced.
"""
from __future__ import annotations

from collections import defaultdict

from deps import supabase


def _page(table: str, select: str, *, eq: dict[str, str], order: str,
          tiebreak: str | None = "id") -> list[dict]:
    """Every matching row, paged.

    ⚠ PAGED, AND ORDERED ON A UNIQUE-ENOUGH KEY. PostgREST truncates silently at 1,000 rows on
    cloud and 10,000 locally, so an unpaged read of a fleet-sized table gives a different answer per
    environment — and a value series short by a page is a chart with a cliff in it.

    ⚠ `tiebreak=None` FOR A TABLE WHOSE `order` IS ALREADY UNIQUE UNDER THE FILTER.
    `airs_holding` needs `id` (a book has many rows per date); `airs_performance` is keyed
    `(portefeuille, periode)` and is read one book at a time, so `periode` alone cannot tie — and
    it has no `id` column to fall back on.
    """
    out: list[dict] = []
    off = 0
    while True:
        q = supabase.table(table).select(select)
        for k, v in eq.items():
            q = q.eq(k, v)
        q = q.order(order)
        if tiebreak:
            q = q.order(tiebreak)
        page = q.range(off, off + 999).execute().data or []
        out += page
        if len(page) < 1000:
            return out
        off += len(page)


def _month_start(period: str) -> str:
    """`2026-06-30` → `2026-06-01` — where the curve's zero goes.

    ⚠ THE PERIOD'S OPENING, NOT THE PERIOD ITSELF. `cumulatief_rendement` on a row dated
    `2026-01-31` is the return earned OVER January, so the 0% it grew from belongs on 1 January.
    Anchoring it on the 31st instead would draw January's move as a vertical step out of nothing.
    """
    return f"{period[:7]}-01"


def _return_series(perf: list[dict], by_date: dict[str, float],
                   counts: dict[str, int]) -> tuple[list[dict], str | None]:
    """`(points, anchor)` — AIRS's own year-to-date return on every date it has published one.

    ⚠⚠ READ, NEVER RECOMPUTED — see the module note. `cumulatief_rendement` is flow-aware and
    our value series is not; a return derived here would disagree with the Scorecard tile beside the
    chart, which reads the same column through `_airs_accounts._year_perf`.

    ⚠ EVERY DISTINCT `periode`, because each one is year-to-date as of its own date. This is
    exactly what `_year_perf` must NOT do — it sums per-period money columns, where June's seven
    rows are seven looks at one month.

    ⚠ THE VALUE RIDES ALONG WHERE WE HAVE IT, so one hover answers both "how much" and "how well"
    without a second line on a 104px plot. NULL where AIRS published a return for a date we hold no
    snapshot for, which is every month before we started scraping.
    """
    if not perf:
        return [], None
    # ⚠ ONE YEAR. `cumulatief_rendement` restarts each January, so a series spanning two of them
    # would chart a figure across the point where it resets. The newest year PRESENT, never
    # `date.today().year`: a table not refreshed since New Year would then answer nothing at all.
    year = max(str(r["periode"])[:4] for r in perf)
    rows = [r for r in perf if str(r["periode"]).startswith(year)]

    # ⚠ LEADING ZEROS ARE NOT THE START OF THE CURVE. AIRS reports a whole year of rows the
    # moment a book exists, so an account funded in June carries five months of 0.00 before it —
    # and a line pinned at 0% across them says the book was flat when it was absent.
    def _funded(r: dict) -> bool:
        return bool(r.get("beginvermogen") or r.get("eindvermogen"))

    first = next((i for i, r in enumerate(rows) if _funded(r)), len(rows))
    rows = rows[first:]
    if not rows:
        return [], None

    anchor = _month_start(str(rows[0]["periode"])[:10])
    out = [{"date": anchor, "cum_pct": 0.0, "value_eur": None, "holdings": None}]
    for r in rows:
        cum = r.get("cumulatief_rendement")
        if cum is None:
            # ⚠ SKIPPED, NOT ZEROED. A row stored before AIRS published its return is a gap in
            # the curve; drawn as 0% it is a round trip to flat that never happened.
            continue
        d = str(r["periode"])[:10]
        if d <= anchor:
            # ⚠ NOTHING ON OR BEFORE THE ZERO. A period dated the 1st would put a second point
            # on the anchor's x, and the two would disagree.
            continue
        out.append({"date": d, "cum_pct": round(float(cum), 4),
                    "value_eur": round(by_date[d], 2) if d in by_date else None,
                    "holdings": counts.get(d)})
    # ⚠ THE ANCHOR ALONE IS NOT A SERIES. One pinned zero drawn as a line states a shape the
    # data does not have, and the caller's "no return yet" sentence is the honest answer.
    return (out, anchor) if len(out) > 1 else ([], None)


def value_series(portefeuille: str) -> dict:
    """`{points, flows, …}` — the book's value on every date we hold a snapshot for."""
    rows = _page("airs_holding", "as_of_date,current_value_eur",
                 eq={"portefeuille": portefeuille}, order="as_of_date")

    by_date: dict[str, float] = defaultdict(float)
    counts: dict[str, int] = defaultdict(int)
    for r in rows:
        v = r.get("current_value_eur")
        if v is None:
            # ⚠ AN UNVALUED LINE IS NOT A ZERO. It would pull the whole book's value down by
            # whatever that holding is worth, on one date, and read as a fall.
            continue
        by_date[str(r["as_of_date"])] += float(v)
        counts[str(r["as_of_date"])] += 1

    # ⚠ ONE READ FOR THREE JOBS — the flows, the pre-snapshot history and the return curve are
    # all the same rows.
    # ⚠⚠ PAGED. It was not, and a book scraped daily writes a row per scrape date: ascending
    # order puts the NEWEST rows last, so a truncated read loses exactly the end of the curve —
    # the failure that once served June's YTD in production while July sat unread (see
    # `_airs_accounts._paged`). `periode` is unique under a `portefeuille` filter (it is half the
    # primary key), so it needs no tiebreak — and there is no `id` column to give it one.
    perf = _page("airs_performance",
                 "periode,beginvermogen,eindvermogen,stortingen,onttrekkingen,"
                 "cumulatief_rendement",
                 eq={"portefeuille": portefeuille}, order="periode", tiebreak=None)

    # ⚠ THE FLOWS COME FROM `airs_performance`, WHICH IS THE ONLY PLACE THAT HAS THEM — the
    # Vermogensoverzicht lists positions, not money in and out. They are DEPOSITS AND WITHDRAWALS,
    # never a result: the chart marks them so a funding cannot be read as performance.
    flows: list[dict] = []
    for r in perf:
        into = float(r.get("stortingen") or 0)
        out_ = float(r.get("onttrekkingen") or 0)
        if into or out_:
            flows.append({"date": str(r["periode"])[:10], "deposits_eur": round(into, 2),
                          "withdrawals_eur": round(out_, 2)})

    ours = [{"date": d, "value_eur": round(by_date[d], 2), "holdings": counts[d],
             "source": "holdings"} for d in sorted(by_date)]

    # ⚠ STRICTLY BEFORE OUR FIRST SNAPSHOT. Where both sides have a date, ours is the one with the
    # positions behind it — and they agree, so the only thing a duplicate would add is two points
    # on one x.
    cutoff = ours[0]["date"] if ours else None
    earlier: list[dict] = []
    for r in perf:
        d = str(r["periode"])[:10]
        if cutoff is not None and d >= cutoff:
            break
        v = r.get("eindvermogen")
        if v is None:
            continue
        earlier.append({"date": d, "value_eur": round(float(v), 2), "holdings": None,
                        "source": "airs"})
    # ⚠ LEADING ZEROS ONLY — see the module note. `next` finds the first point with any value;
    # everything before it is a book that did not exist yet.
    first_real = next((i for i, p in enumerate(earlier) if p["value_eur"]), len(earlier))
    points = earlier[first_real:] + ours

    returns, anchor = _return_series(perf, by_date, counts)
    return {
        "portefeuille": portefeuille,
        "points": points,
        "flows": flows,
        "returns": returns,
        # ⚠ THE ORIGIN IS REPORTED, not inferred from `returns[0]` by the caller — it is the one
        # date on that series nobody measured, and a chart made to work out which of its own points
        # is the pinned zero will eventually get it wrong.
        "return_from": anchor,
        # ⚠ THE HEADLINE IS THE LAST POINT OF THE FULL SERIES, so a display resolution cannot
        # move a reported figure. The same rule the value header already follows.
        "return_pct": returns[-1]["cum_pct"] if returns else None,
        # ⚠ SO THE CHART CAN SAY WHAT IT IS LOOKING AT WITHOUT COUNTING. `first`/`last` are the span
        # we actually hold, which is the answer to "why does this start in June".
        "first_date": points[0]["date"] if points else None,
        "last_date": points[-1]["date"] if points else None,
        # ⚠ WHERE THE SERIES CHANGES HANDS, so the chart can draw the two halves differently and
        # say which is which. None when every point is ours.
        "own_from": ours[0]["date"] if ours else None,
    }
