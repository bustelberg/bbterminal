"""AIRS-only monthly conversation report. Never rebuild performance from prices.

Historical downloads stay in their own bundle: they must not replace the live
holdings, transaction cache or the account's current performance observations.
"""
from __future__ import annotations

import calendar
import logging
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from fastapi import HTTPException

from deps import supabase

log = logging.getLogger(__name__)


def month_window(month: str, today: date | None = None) -> tuple[str, str, str]:
    """Calendar opening boundary and last completed market date, capped to month."""
    from airs_vermogen import latest_completed_market_date

    try:
        if len(month) != 7 or month[4] != "-":
            raise ValueError("invalid month")
        start = date.fromisoformat(month + "-01")
        if start == date.min:
            raise ValueError("no opening valuation boundary")
    except ValueError as exc:
        raise HTTPException(422, "Month must use YYYY-MM") from exc
    current = today or datetime.now(ZoneInfo("Europe/Amsterdam")).date()
    latest = date.fromisoformat(latest_completed_market_date(current))
    end = min(date(start.year, start.month, calendar.monthrange(start.year, start.month)[1]), latest)
    if end < start:
        raise HTTPException(422, "This month has no completed valuation date yet")
    return start.isoformat(), end.isoformat(), (start - timedelta(days=1)).isoformat()


def _account(portefeuille: str) -> None:
    from airs_portfolio_exclusions import is_excluded_portfolio

    if is_excluded_portfolio(portefeuille):
        raise HTTPException(404, "Portfolio not available")
    # Validate against the known AIRS account set before using a caller's name
    # in a live report request. An absent report is then a real missing-data state.
    rows = (supabase.table("airs_performance").select("portefeuille")
            .eq("portefeuille", portefeuille).limit(1).execute().data or [])
    if not rows:
        rows = (supabase.table("airs_account_roster").select("portefeuille")
                .eq("portefeuille", portefeuille).limit(1).execute().data or [])
    if not rows:
        raise HTTPException(404, "This AIRS account is not known. Refresh the account list first.")


def _monthly_rows(portefeuille: str, year: int) -> list[dict]:
    rows = (supabase.table("airs_performance").select("*")
            .eq("portefeuille", portefeuille).gte("periode", f"{year}-01-01")
            .lte("periode", f"{year}-12-31").order("periode").execute().data or [])
    latest: dict[str, dict] = {}
    for row in rows:
        key = str(row.get("periode") or "")[:7]
        if len(key) == 7:
            latest[key] = row
    return [{"month": month, **row} for month, row in sorted(latest.items())]


def _acwi_months(months: list[dict], ticker: str = "ACWI", currency: str = "USD") -> dict:
    """Read stored ACWI ETF closes in EUR; match each AIRS observation's cutoff."""
    if not months:
        return {}
    from routers._benchmark_index import _fx_to_eur, _rate
    import math

    try:
        benchmarks = (supabase.table("benchmark").select("benchmark_id")
                      .eq("ticker", ticker).limit(1).execute().data or [])
        if not benchmarks:
            return {}
        anchors = {r["month"]: (date.fromisoformat(r["month"] + "-01") - timedelta(days=1)).isoformat() for r in months}
        cutoffs = {r["month"]: str(r.get("periode") or "")[:10] for r in months}
        cutoffs = {m: d for m, d in cutoffs.items() if d.startswith(m + "-")}
        if not cutoffs:
            return {}
        start = (date.fromisoformat(min(anchors.values())) - timedelta(days=7)).isoformat()
        end = max(cutoffs.values())
        rows = (supabase.table("benchmark_price").select("target_date,price")
                .eq("benchmark_id", benchmarks[0]["benchmark_id"]).gte("target_date", start)
                .lte("target_date", end).order("target_date").limit(1000).execute().data or [])
        marks = [(str(r["target_date"])[:10], float(r["price"])) for r in rows
                 if r.get("price") is not None and math.isfinite(float(r["price"])) and float(r["price"]) > 0]
        fx = _fx_to_eur({currency}, start, end) if currency != "EUR" else {}

        def mark(target):
            candidates = [p for p in marks if p[0] <= target]
            if not candidates:
                return None
            day, price = candidates[-1]
            rate = _rate(fx, currency, day) if currency != "EUR" else 1
            if not rate or (date.fromisoformat(target) - date.fromisoformat(day)).days > 7:
                return None
            return day, price / rate

        result = {}
        for month, cutoff in cutoffs.items():
            first, last = mark(anchors[month]), mark(cutoff)
            if first and last and first[0] < last[0]:
                result[month] = {"return_pct": (last[1] / first[1] - 1) * 100,
                                 "start_price_eur": first[1], "end_price_eur": last[1],
                                 "start_date": first[0], "end_date": last[0], "source": f"{ticker} ETF · EUR"}
        return result
    except Exception:
        log.exception("[monthly-report] stored ACWI returns unavailable")
        return {}


def _weighted_months(portefeuille: str, months: list[dict]) -> dict:
    from routers._airs_portfolio_variant import portfolio_variant
    from routers._airs_portfolio_analysis import _weighted_benchmark_profiles

    policy = _weighted_benchmark_profiles().get(portfolio_variant(portefeuille) or "")
    if not policy:
        return {}
    sleeves = [(weight, ticker, _acwi_months(months, ticker, currency))
               for bucket, ticker, currency in (("Stocks", "ACWI", "USD"), ("Bonds", "AGGH.AS", "EUR"))
               if (weight := policy[bucket]) > 0]
    result = {}
    for row in months:
        key = row["month"]
        if all(key in series for _, _, series in sleeves):
            result[key] = {"return_pct": sum(weight * series[key]["return_pct"] / 100 for weight, _, series in sleeves),
                           "start_date": min(series[key]["start_date"] for _, _, series in sleeves),
                           "end_date": min(series[key]["end_date"] for _, _, series in sleeves),
                           "components": [{**series[key], "ticker": ticker, "weight_pct": weight}
                                          for weight, ticker, series in sleeves],
                           "source": f'{policy["Stocks"]:g}% ACWI / {policy["Bonds"]:g}% AGGH.AS · EUR'}
    return result


def read_report(portefeuille: str, month: str) -> dict:
    start, end, opening = month_window(month)
    _account(portefeuille)
    months = _monthly_rows(portefeuille, int(month[:4]))
    standard_months = {row['month']: row for row in months}
    warning = None
    try:
        # The year strip reads only small performance observations, not twelve
        # complete holdings/journal/transaction bundles on every month click.
        cached = (supabase.table("airs_monthly_report").select("month,performance")
                  .eq("portefeuille", portefeuille).gte("month", f"{month[:4]}-01-01")
                  .lte("month", f"{month[:4]}-12-31").order("month")
                  .execute().data or [])
        selected = (supabase.table("airs_monthly_report").select("report,fetched_at,datum_tot")
                    .eq("portefeuille", portefeuille).eq("month", start).limit(1)
                    .execute().data or [])
    except Exception:
        cached = []
        selected = []
        log.exception("[monthly-report] stored report read failed for %s", portefeuille)
        warning = "Monthly report details are unavailable. Ask an administrator to check report storage."
    by_month = {row["month"]: row for row in months}
    for stored in cached:
        key = str(stored["month"])[:7]
        if stored.get("performance"):
            existing = by_month.get(key)
            data = stored["performance"]
            # Custom-start ATT reports reset opening capital; do not let their
            # returns replace months from the canonical year-start AIRS report.
            if not existing:
                by_month[key] = {"month": key, **data}
    months = [by_month[key] for key in sorted(by_month)]
    bundle = selected[0] if selected else None
    # Enrich older cached bundles from their saved AIRS sources, without a new scan.
    if bundle and "positions" in bundle.get("report", {}):
        from routers._airs_monthly_positions import position_performance
        snapshot = bundle["report"]
        snapshot["positions"] = position_performance(snapshot.get("sections", {}), start, str(bundle["datum_tot"])[:10])
        from routers._airs_monthly_reconciliation import reconcile_positions
        canonical = standard_months.get(month)
        if canonical and str(canonical.get('periode', ''))[:10] == str(bundle['datum_tot'])[:10]:
            snapshot['sections']['performance'] = {'status': 'ok', 'data': canonical}
        elif snapshot.get('performance_basis') == 'year_start':
            section = snapshot.get('sections', {}).get('performance', {})
            canonical = section.get('data') if section.get('status') == 'ok' else None
            if canonical:
                by_month[month] = {'month': month, **canonical}
                months = [by_month[key] for key in sorted(by_month)]
        else:
            canonical = None
        snapshot['reconciliation'] = reconcile_positions(snapshot.get('sections', {}), canonical, snapshot['positions'])
    has_errors = bool(bundle and ("positions" not in bundle.get("report", {}) or any(s.get("status") == "error" for s in
                      bundle.get("report", {}).get("sections", {}).values())))
    return {
        "portefeuille": portefeuille, "month": month, "start": start,
        "end": end, "opening_date": opening, "months": months, "weighted_benchmark": _weighted_months(portefeuille, months),
        "snapshot": bundle.get("report") if bundle else None,
        "fetched_at": bundle.get("fetched_at") if bundle else None,
        "needs_refresh": not bundle or str(bundle["datum_tot"])[:10] != end or has_errors,
        "warning": warning,
    }


def collect_report(portefeuille: str, month: str, ctx) -> str:
    from airs_scanner import (
        AirsNoData, download_portfolio_sync,
        download_transacties_sync, download_vermogensoverzicht_sync,
    )
    from airs_vermogen import _LOCK, _acquire_session
    from airs_transacties import parse_transacties, trades, realised_results, TT, TT_BUY, TT_SELL, COL_FONDS, COL_DATE
    from portfolio import parse_airs_excel
    from routers.airs import _parse_att_excel

    start, end, opening = month_window(month)
    _account(portefeuille)
    # Refuse before downloading if persistence is not configured. These are
    # historical records, not an in-memory replacement for a missing migration.
    supabase.table("airs_monthly_report").select("month").limit(1).execute()
    ctx.check()
    ctx.progress(0, 4, f"{portefeuille} · {month}: waiting for the AIRS session")
    if not _acquire_session(30):
        raise RuntimeError("AIRS is busy with another scan. Try loading this month again shortly.")
    sections: dict[str, dict] = {}
    try:
        def performance():
            # Same year-start request as the homepage. A custom month-start
            # request changes AIRS's opening valuation and omits the first day.
            rows = _parse_att_excel(download_portfolio_sync(portefeuille, f'{start[:4]}-01-01', end))
            candidates = [r for r in rows if start <= str(r.get("periode") or "")[:10] <= end]
            if not candidates:
                raise ValueError("AIRS returned no performance observation within the requested month")
            return max(candidates, key=lambda r: str(r["periode"]))

        def holdings(day):
            # VOLK's per-holding result columns say 'lopend jaar'. Return
            # dated valuations only, never relabel those figures monthly.
            requested = date.fromisoformat(day)
            for offset in range(7):
                ctx.check()
                valued = (requested - timedelta(days=offset)).isoformat()
                try:
                    raw = download_vermogensoverzicht_sync(portefeuille, valued, valued)
                except (AirsNoData, RuntimeError) as exc:
                    if isinstance(exc, AirsNoData) or "Response too small" in str(exc):
                        continue
                    raise
                rows = parse_airs_excel(raw)
                return {"as_of": valued, "requested_date": day, "rows": [
                    {"name": h.holding_name, "isin": h.isin, "currency": h.currency,
                     "quantity": getattr(h, "quantity_exact", None) if getattr(h, "quantity_exact", None) is not None else h.quantity,
                     "value_eur": h.current_value_eur,
                     "weight_pct": h.airs_weight} for h in rows]}
            raise AirsNoData(f"No AIRS valuation available on or in the six days before {day}")

        def transactions():
            sheet = parse_transacties(download_transacties_sync(portefeuille, start, end))
            # Only present trades as monthly activity when AIRS supplied a
            # recognizable schema and a date within this exact month.
            activity = [{"name": t.fonds, "kind": t.kind, "date": t.datum,
                         "amount_eur": t.eur, "quantity": t.quantity}
                        for t in trades(sheet) if t.datum and start <= t.datum <= end]
            uncertain = [r for r in sheet.rows if (not r.get(COL_DATE) or start <= str(r[COL_DATE]) <= end)
                         and (r.get(TT) not in (TT_BUY, TT_SELL) or not r.get(COL_DATE))]
            return {"columns": sheet.columns, "rows": sheet.rows, "kinds": sheet.kinds,
                    "activity": activity, "readable": not realised_results(sheet).unreadable,
                    "unclassified_names": sorted({str(r[COL_FONDS]) for r in uncertain if r.get(COL_FONDS)}),
                    "unclassified_unassigned": any(not r.get(COL_FONDS) for r in uncertain)}

        tasks = [("performance", "Rendement", performance),
                 ("opening", "Opening holdings", lambda: holdings(opening)),
                 ("closing", "Closing holdings", lambda: holdings(end)),
                 ("transactions", "Transacties", transactions)]
        for n, (key, label, fn) in enumerate(tasks):
            ctx.check()
            ctx.progress(n, 4, f"{portefeuille} · {month}: reading {label}")
            try:
                sections[key] = {"status": "ok", "data": fn()}
            except AirsNoData as exc:
                sections[key] = {"status": "no_data", "message": str(exc), "data": None}
            except Exception as exc:
                sections[key] = {"status": "error", "message": f"{type(exc).__name__}: {exc}", "data": None}
    finally:
        _LOCK.release()
    ctx.check()
    successes = sum(s["status"] == "ok" for s in sections.values())
    if not successes:
        raise RuntimeError("No AIRS reports could be read. " + "; ".join(
            s.get("message", "") for s in sections.values()))
    fetched = datetime.now(timezone.utc).isoformat()
    payload = {"start": start, "end": end, "opening_date": opening, "performance_basis": "year_start",
               "sections": sections, "source": "AIRS", "fetched_at": fetched}
    from routers._airs_monthly_positions import position_performance
    payload["positions"] = position_performance(sections, start, end)
    from routers._airs_monthly_reconciliation import reconcile_positions
    payload['reconciliation'] = reconcile_positions(sections, sections.get('performance', {}).get('data'), payload['positions'])
    supabase.table("airs_monthly_report").upsert({
        "portefeuille": portefeuille, "month": start, "datum_tot": end,
        "fetched_at": fetched, "report": payload,
        "performance": sections["performance"].get("data"),
    }, on_conflict="portefeuille,month").execute()
    ctx.progress(4, 4, f"{portefeuille} · {month}: saved {successes}/4 reports")
    failures = sum(s["status"] == "error" for s in sections.values())
    if failures:
        from jobs import UserFacingJobError
        raise UserFacingJobError(f"Saved a partial report for {month}: {failures} AIRS report(s) failed. Open Report for details.")
    return f"{portefeuille} · {month}: AIRS monthly report saved"
