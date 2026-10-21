"""Actual AIRS portfolio CAGR, kept separate from today's reconstructed risk sleeve."""
from __future__ import annotations

from datetime import date
from math import isfinite


def chain_airs_history(rows: list[dict], start: str, end: str) -> dict:
    """Chain AIRS's YTD indices across year resets; missing years never become flat returns."""
    usable = {}
    for row in rows:
        day = str(row.get("periode") or "")[:10]
        value = row.get("cumulatief_rendement")
        if day and value is not None and isfinite(float(value)) and float(value) > -100:
            usable[day] = row
    # AIRS historical reports have monthly marks. Use the first mark at/after the requested
    # anniversary and expose its date; both benchmark and portfolio will use this same endpoint.
    starts = sorted(d for d in usable if start <= d <= end)
    if not starts or (date.fromisoformat(starts[0]) - date.fromisoformat(start)).days > 35:
        return {"available": False, "reason": "AIRS has no portfolio return near the five-year opening date."}
    opening = starts[0]
    ends = sorted(d for d in usable if opening < d <= end)
    if not ends or (date.fromisoformat(end) - date.fromisoformat(ends[-1])).days > 35:
        return {"available": False, "reason": "AIRS has no recent portfolio performance endpoint."}
    closing = ends[-1]
    growth = 1.0
    for year in range(int(opening[:4]), int(closing[:4]) + 1):
        final = closing if year == int(closing[:4]) else f"{year}-12-31"
        row = usable.get(final)
        if not row:
            return {"available": False, "reason": f"AIRS performance is missing the {year} year-end return."}
        # A valid YTD figure includes the intervening months, so interior scrape gaps need not
        # cause missing returns. Positive capital protects against unfunded pre-inception zeros.
        if not (row.get("beginvermogen") or row.get("eindvermogen")):
            return {"available": False, "reason": f"The AIRS portfolio was not funded in {year}."}
        growth *= 1 + float(row["cumulatief_rendement"]) / 100
        if year == int(opening[:4]):
            growth /= 1 + float(usable[opening]["cumulatief_rendement"]) / 100
    elapsed = (date.fromisoformat(closing) - date.fromisoformat(opening)).days / 365.25
    return {"available": True, "source": "AIRS actual portfolio performance",
            "start_date": opening, "end_date": closing,
            "total_return_pct": (growth - 1) * 100,
            "portfolio_cagr_pct": (growth ** (1 / elapsed) - 1) * 100,
            "years_elapsed": elapsed}


def portfolio_historical_comparison(portefeuille: str, built: dict, years: int = 5) -> dict:
    from routers._airs_value_series import _page
    from routers._tracking_error import _benchmark_history

    series = built["series"]
    benchmark = _benchmark_history(built, years)
    start, end = benchmark.get("start_date"), benchmark.get("end_date")
    if not start or not end or not benchmark.get("complete"):
        return {"available": False, "reason": "Five-year benchmark history is incomplete."}
    rows = _page("airs_performance", "periode,beginvermogen,eindvermogen,cumulatief_rendement",
                 eq={"portefeuille": portefeuille}, order="periode", tiebreak=None)
    # Daily refresh normally imports only this year. Fetch absent historical years independently,
    # so a five-year request cannot silently accept the current-year cache as complete.
    present = {str(row["periode"])[:4] for row in rows}
    for year in range(int(start[:4]), int(end[:4]) + 1):
        if str(year) in present and (year == int(end[:4]) or
                                    any(str(r["periode"])[:10] == f"{year}-12-31" for r in rows)):
            continue
        from airs_scanner import download_portfolio_sync
        from routers.airs import _parse_att_excel, _save_performance_to_db
        fresh = _parse_att_excel(download_portfolio_sync(
            portefeuille, f"{year}-01-01", min(f"{year}-12-31", end)))
        _save_performance_to_db(portefeuille, fresh)
        rows.extend(fresh)
    result = chain_airs_history(rows, start, end)
    if not result["available"]:
        return result
    # Choose common observed benchmark dates within the AIRS interval; require matching endpoints
    # to avoid subtracting CAGRs spanning different periods.
    clipped = {isin: [(d, p) for d, p in prices
                       if result["start_date"] <= d <= result["end_date"]]
               for isin, prices in series.items()}
    paired = _benchmark_history({**built, "series": clipped}, years)
    if (paired.get("start_date") != result["start_date"] or
            paired.get("end_date") != result["end_date"]):
        # Month ends can fall on weekends. Sample the last actual close at/before each AIRS date.
        start_day, end_day = result["start_date"], result["end_date"]
        sampled = {}
        for isin in set(built["benchmark_component_isins"].values()):
            prices = series.get(isin) or []
            before = [(d, p) for d, p in prices if d <= start_day]
            after = [(d, p) for d, p in prices if d <= end_day]
            if not before or not after:
                return {"available": False, "reason": "Benchmark endpoint prices are unavailable."}
            sampled[isin] = [(start_day, before[-1][1])]
            sampled[isin] += [(d, p) for d, p in prices if start_day < d < end_day]
            sampled[isin].append((end_day, after[-1][1]))
        paired = _benchmark_history({**built, "series": sampled}, years)
    result["benchmark_cagr_pct"] = paired["cagr_pct"]
    result["active_return_pp"] = result["portfolio_cagr_pct"] - paired["cagr_pct"]
    result["benchmark_components"] = paired["components"]
    return result
