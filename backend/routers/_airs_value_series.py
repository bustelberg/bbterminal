"""THE BOOK'S VALUE THROUGH TIME, BUILT FROM OUR OWN SNAPSHOTS.

⚠⚠ IT IS SUMMED FROM `airs_holding`, NOT READ FROM AIRS'S RENDEMENTEN. Every snapshot we have ever
stored carries one row per holding with its EUR value, so the book's value on that date is their
sum — a series this system owns rather than a figure fetched from a sheet. Measured on
AzTopSelectie_DYN (24 snapshots): it reproduces AIRS's own `eindvermogen` to the euro on 21 of
them, we hold two dates AIRS's sheet has no row for at all, and one date differs by EUR 8,888 where
the two disagree about which valuation the snapshot belongs to (see `as_of_date` below).

⚠⚠ VALUE IS NOT RETURN, AND ON THIS DATA THAT IS NOT A QUIBBLE. AzTopSelectie goes from 0 to
EUR 1,000,000 on 2026-06-30 because it was FUNDED that day. A value line without its flows says the
book doubled its money and then some; the flows ride along so the step can be drawn as what it is.
Nothing here computes a return — `airs_performance.cumulatief_rendement` is that number, and
`_airs_accounts` is where it is assembled.

⚠⚠ AND BEFORE OUR FIRST SNAPSHOT IT FALLS BACK TO `airs_performance.eindvermogen`, TAGGED AS SUCH.
Our snapshots begin 2026-06-23 at the earliest and, on two books, 2026-07-30 — which is what a
reader sees as "why does this start in August?" while AIRS's own sheet has held month-ends since
2026-01-31 (AITopSelectie: 1,044,066 in January, 1,551,994 in June). Refusing six months of history
we already store, to keep the series pure, answers a question nobody asked. So every point carries
a `source`: `holdings` where we summed it ourselves, `airs` where it is AIRS's own close, and the
chart draws the two differently rather than pretending they are one measurement.

⚠ THE HANDOVER IS OUR FIRST SNAPSHOT, not the last AIRS row — where both exist ours is the one with
the holdings behind it, and the two agree anyway (21 of 24 dates to the euro on AzTopSelectie).

⚠ LEADING ZEROS ARE DROPPED. AIRS reports `eindvermogen` 0.00 for every month before a book was
funded, and a line that runs along zero for five months then jumps is drawing an absence as a
measurement. An INTERIOR zero is kept: a book emptied mid-year really was worth nothing.

⚠ THE POINTS ARE IRREGULAR. A snapshot exists only for a day the scrape ran AND AIRS had valued the
book, so gaps are the norm — a chart must plot against real dates rather than treating the points as
evenly spaced.
"""
from __future__ import annotations

from collections import defaultdict

from deps import supabase


def _page(table: str, select: str, *, eq: dict[str, str], order: str) -> list[dict]:
    """Every matching row, paged.

    ⚠ PAGED, AND ORDERED ON A UNIQUE-ENOUGH KEY. PostgREST truncates silently at 1,000 rows on
    cloud and 10,000 locally, so an unpaged read of a fleet-sized table gives a different answer per
    environment — and a value series short by a page is a chart with a cliff in it.
    """
    out: list[dict] = []
    off = 0
    while True:
        q = supabase.table(table).select(select)
        for k, v in eq.items():
            q = q.eq(k, v)
        page = q.order(order).order("id").range(off, off + 999).execute().data or []
        out += page
        if len(page) < 1000:
            return out
        off += len(page)


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

    # ⚠ ONE READ FOR BOTH JOBS — the flows and the pre-snapshot history are the same rows.
    perf = (supabase.table("airs_performance")
            .select("periode,eindvermogen,stortingen,onttrekkingen")
            .eq("portefeuille", portefeuille).order("periode").execute().data or [])

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
    return {
        "portefeuille": portefeuille,
        "points": points,
        "flows": flows,
        # ⚠ SO THE CHART CAN SAY WHAT IT IS LOOKING AT WITHOUT COUNTING. `first`/`last` are the span
        # we actually hold, which is the answer to "why does this start in June".
        "first_date": points[0]["date"] if points else None,
        "last_date": points[-1]["date"] if points else None,
        # ⚠ WHERE THE SERIES CHANGES HANDS, so the chart can draw the two halves differently and
        # say which is which. None when every point is ours.
        "own_from": ours[0]["date"] if ours else None,
    }
