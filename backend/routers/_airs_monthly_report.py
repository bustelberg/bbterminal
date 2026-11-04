"""AIRS-only monthly conversation report. Never rebuild performance from prices.

Historical downloads stay in their own bundle: they must not replace the live
holdings, transaction cache or the account's current performance observations.
"""
from __future__ import annotations

import calendar
import logging
import threading
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from fastapi import HTTPException

from deps import supabase

log = logging.getLogger(__name__)
_PREPARATION_LOCKS = tuple(threading.Lock() for _ in range(32))


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
    while end.weekday() >= 5:
        end -= timedelta(days=1)
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
        anchors = {r["month"]: r.get("anchor") or (date.fromisoformat(r["month"] + "-01") - timedelta(days=1)).isoformat() for r in months}
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
        log.exception("[monthly-report] stored %s returns unavailable", ticker)
        return {}


def benchmark_policy(portefeuille: str) -> dict[str, float] | None:
    from routers._airs_portfolio_variant import portfolio_variant
    from routers._airs_portfolio_analysis import _weighted_benchmark_profiles
    from routers._airs_strategy_map import nickname_for

    if nickname_for(portefeuille) == "ObligatieTopSelectie":
        return {"Stocks": 0, "Bonds": 100}
    return _weighted_benchmark_profiles().get(portfolio_variant(portefeuille) or "")


def _weighted_months(portefeuille: str, months: list[dict]) -> dict:
    policy = benchmark_policy(portefeuille)
    if not policy:
        return {}
    sleeves = [(weight, ticker, _acwi_months(months, ticker, currency))
               for bucket, ticker, currency in (("Stocks", "ACWI", "USD"), ("Bonds", "AGGH.AS", "EUR"))
               if (weight := policy[bucket]) > 0]
    source = " / ".join(f"{weight:g}% {ticker}" for weight, ticker, _ in sleeves) + " · EUR"
    result = {}
    for row in months:
        key = row["month"]
        if all(key in series for _, _, series in sleeves):
            result[key] = {"return_pct": sum(weight * series[key]["return_pct"] / 100 for weight, _, series in sleeves),
                           "start_date": min(series[key]["start_date"] for _, _, series in sleeves),
                           "end_date": min(series[key]["end_date"] for _, _, series in sleeves),
                           "components": [{**series[key], "ticker": ticker, "weight_pct": weight}
                                          for weight, ticker, series in sleeves],
                           "source": source}
    return result


def _enrich_snapshot(bundle, start, month, standard_months):
    override = None
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
            override = canonical
        else:
            canonical = None
        snapshot['reconciliation'] = reconcile_positions(snapshot.get('sections', {}), canonical, snapshot['positions'])
    has_errors = bool(bundle and ("positions" not in bundle.get("report", {}) or any(s.get("status") == "error" for s in
                      bundle.get("report", {}).get("sections", {}).values())))
    # Earlier bundles discarded the AIRS component label. Offer a dated re-read when
    # those source rows cannot safely be matched; a new calculation cannot recreate it.
    if bundle:
        has_errors = has_errors or any(row.get('reason') == 'ambiguous_position'
                                      for row in bundle.get('report', {}).get('positions', []))
        sections = bundle.get("report", {}).get("sections", {})
        has_errors = has_errors or any((sections.get(key) or {}).get("status") == "no_data"
                                      for key in ("opening", "closing"))
        transactions = (sections.get("transactions") or {}).get("data")
        has_errors = has_errors or (transactions is not None and not transactions.get("readable"))
    return override, has_errors


def _available_years(portefeuille: str, year: int) -> list[int]:
    years = {year, datetime.now(ZoneInfo("Europe/Amsterdam")).year}
    for table, column in (("airs_performance", "periode"), ("airs_monthly_report", "month"), ("airs_year_archive", "year")):
        offset = 0
        while True:
            page = (supabase.table(table).select(column).eq("portefeuille", portefeuille)
                    .order(column).range(offset, offset + 999).execute().data or [])
            years.update(int(str(row[column])[:4]) for row in page if row.get(column))
            if len(page) < 1000:
                break
            offset += 1000
    return sorted(years, reverse=True)


def read_report(portefeuille: str, month: str, *, include_details: bool = True,
                bundled: bool = False) -> dict:
    try:
        start, end, opening = month_window(month)
    except HTTPException as exc:
        current = datetime.now(ZoneInfo("Europe/Amsterdam")).date()
        if exc.status_code != 422 or month != current.strftime("%Y-%m"):
            raise
        from airs_vermogen import latest_completed_market_date
        if latest_completed_market_date(current) >= month + "-01":
            raise
        # January 1 has no valuation in the new reporting year yet. History
        # must remain selectable without presenting last year's YTD as this year's.
        start = month + "-01"
        opening = (date.fromisoformat(start) - timedelta(days=1)).isoformat()
        end = opening
    if not bundled:
        _account(portefeuille)
    months = _monthly_rows(portefeuille, int(month[:4]))
    standard_months = {row['month']: row for row in months}
    annual_months = dict(standard_months)
    warning = "This month has no completed valuation date yet." if end < start else None
    try:
        # Keep legacy month reads small. The tab bundle reads each year's
        # stored source reports together and projects them to display data.
        columns = "month,performance,performance_basis"
        if bundled:
            columns += ",report,datum_tot"
        cached = (supabase.table("airs_monthly_report").select(columns)
                  .eq("portefeuille", portefeuille).gte("month", f"{month[:4]}-01-01")
                  .lte("month", f"{month[:4]}-12-31").order("month")
                  .execute().data or [])
        selected = (supabase.table("airs_monthly_report").select("report,fetched_at,datum_tot")
                    .eq("portefeuille", portefeuille).eq("month", start).limit(1)
                    .execute().data or []) if include_details else []
    except Exception:
        cached = []
        selected = []
        log.exception("[monthly-report] stored report read failed for %s", portefeuille)
        warning = "Monthly report details are unavailable. Ask an administrator to check report storage."
    by_month = {row["month"]: row for row in months}
    for stored in cached:
        key = str(stored["month"])[:7]
        data = stored.get("performance")
        basis = stored.get("performance_basis")
        if bundled and not data:
            snapshot = stored.get("report") or {}
            section = snapshot.get("sections", {}).get("performance", {})
            if snapshot.get("performance_basis") == "year_start" and section.get("status") == "ok":
                data, basis = section.get("data"), "year_start"
        if data:
            existing = by_month.get(key)
            annual_previous = annual_months.get(key)
            if basis == "year_start" and (not annual_previous
                    or str(data.get("periode", "")) > str(annual_previous.get("periode", ""))):
                annual_months[key] = {"month": key, **data}
            # Custom-start ATT reports reset opening capital; do not let their
            # returns replace months from the canonical year-start AIRS report.
            if not existing or (basis == "year_start"
                                and str(data.get("periode", "")) > str(existing.get("periode", ""))):
                by_month[key] = {"month": key, **data}
    months = [by_month[key] for key in sorted(by_month)]
    bundle = selected[0] if selected else None
    override, has_errors = _enrich_snapshot(bundle, start, month, standard_months)
    if override:
        by_month[month] = {"month": month, **override}
        annual_months[month] = by_month[month]
        months = [by_month[key] for key in sorted(by_month)]
    from routers._airs_value_series import _return_series
    points, anchor = _return_series([annual_months[key] for key in sorted(annual_months)], {}, {})
    closed = (supabase.table("airs_year_archive").select("payload").eq("portefeuille", portefeuille)
              .eq("year", int(month[:4])).eq("kind", "year_end").limit(1).execute().data or [])
    finalised = bool(closed and closed[0]["payload"].get("complete"))
    benchmark = {}
    summary = {"return_pct": None, "benchmark_pct": None, "start": None, "end": None}
    if points and anchor:
        last = points[-1]
        # Use the custodian's cumulative figure, not compounded rounded months.
        annual = [{"month": last["date"][:7], "periode": last["date"],
                   "anchor": (date.fromisoformat(anchor) - timedelta(days=1)).isoformat()}]
        benchmark = _weighted_months(portefeuille, annual).get(last["date"][:7], {})
        summary = {"return_pct": last["cum_pct"], "benchmark_pct": benchmark.get("return_pct"),
                   "start": anchor, "end": last["date"]}
    weighted_benchmark = _weighted_months(portefeuille, months)
    benchmark_expected = bool(benchmark_policy(portefeuille))
    if bundled:
        if benchmark and summary["start"][:7] == summary["end"][:7]:
            weighted_benchmark[summary["end"][:7]] = benchmark
        today = datetime.now(ZoneInfo("Europe/Amsterdam")).strftime("%Y-%m")
        limit = int(today[5:]) if month[:4] == today[:4] else 12
        keys = [f"{month[:4]}-{number:02d}" for number in range(1, limit + 1)]
        stored_by_month = {str(row["month"])[:7]: row for row in cached if row.get("report")}
        details = {}
        for key in keys:
            stored = stored_by_month.get(key)
            snapshot = None
            refresh_reason = "missing_report"
            if stored:
                _, errors = _enrich_snapshot(stored, key + "-01", key, standard_months)
                source = stored["report"]
                snapshot = {
                    "positions": source.get("positions"),
                    "reconciliation": source.get("reconciliation"),
                    "opening_weight_date": ((source.get("sections", {}).get("opening") or {}).get("data") or {}).get("as_of"),
                    "closing_date": ((source.get("sections", {}).get("closing") or {}).get("data") or {}).get("as_of"),
                }
                cutoff = end if key == month else month_window(key)[1]
                if any(row.get("reason") == "ambiguous_position" for row in snapshot["positions"] or []):
                    refresh_reason = "ambiguous_position"
                elif errors:
                    refresh_reason = "incomplete_report"
                elif str(stored["datum_tot"])[:10] < cutoff or (
                    snapshot["closing_date"] and snapshot["closing_date"] < cutoff
                ):
                    refresh_reason = "outdated_report"
                else:
                    refresh_reason = None
            details[key] = {
                "snapshot": snapshot, "needs_refresh": refresh_reason is not None, "refresh_reason": refresh_reason,
                "benchmark_needs_refresh": benchmark_expected and key in by_month and key not in weighted_benchmark,
            }
        portfolio, reference = summary["return_pct"], summary["benchmark_pct"]
        summary["excess_pp"] = (round(portfolio, 2) - round(reference, 2)
                                if portfolio is not None and reference is not None else None)
        return {
            "months": keys, "rows": {key: {"periode": row.get("periode"), "rendement": row.get("rendement")}
                                     for key, row in by_month.items()},
            "details": details, "weighted_benchmark": weighted_benchmark,
            "summary": {**summary, "complete": finalised}, "warning": warning,
        }
    benchmark_missing = benchmark_expected and any(row['month'] == month for row in months) and month not in weighted_benchmark
    return {
        "available_years": _available_years(portefeuille, int(month[:4])),
        "year_summary": {**summary, "complete": finalised},
        "portefeuille": portefeuille, "month": month, "start": start,
        "end": end, "opening_date": opening, "months": months, "weighted_benchmark": weighted_benchmark,
        "benchmark_needs_refresh": benchmark_missing,
        "snapshot": bundle.get("report") if bundle else None,
        "fetched_at": bundle.get("fetched_at") if bundle else None,
        "needs_refresh": not bundle or str(bundle["datum_tot"])[:10] != end or has_errors,
        "warning": warning,
    }


def read_performance(portefeuille: str) -> dict:
    _account(portefeuille)
    today = datetime.now(ZoneInfo("Europe/Amsterdam")).strftime("%Y-%m")
    years = [year for year in _available_years(portefeuille, int(today[:4])) if year <= int(today[:4])]
    return {
        "portefeuille": portefeuille,
        "current_month": today,
        "available_years": years,
        "years": {
            str(year): read_report(portefeuille, today if str(year) == today[:4] else f"{year}-12",
                                   include_details=False, bundled=True)
            for year in years
        },
    }


class _PerformancePreparation:
    """The Performance request has no job stream; AIRS still uses the same collector."""

    def check(self) -> None:
        pass

    def progress(self, done: int, total: int, message: str) -> None:
        pass


def _prepare_performance(portefeuille: str) -> dict:
    """Return positions in the initial bundle, repairing missing reports before responding."""
    from dashboard_daily import prepare_month

    with _PREPARATION_LOCKS[hash(portefeuille) % len(_PREPARATION_LOCKS)]:
        bundle = read_performance(portefeuille)
        months = {
            month: year["details"][month]["refresh_reason"]
            for year in bundle["years"].values() if not year["warning"]
            for month in reversed(year["months"])
            if year["rows"].get(month, {}).get("rendement") is not None
            and year["details"][month]["refresh_reason"] in (
                "missing_report", "incomplete_report", "ambiguous_position", "outdated_report",
            )
        }
        if not months:
            return bundle

        for month, reason in months.items():
            try:
                # Reusing the same saved valuation cannot restore a lost AIRS component label.
                if reason == "ambiguous_position" or prepare_month(portefeuille, month=month, complete_only=True)["errors"]:
                    collect_report(portefeuille, month, _PerformancePreparation())
            except Exception:
                log.exception("[monthly-report] could not prepare %s %s", portefeuille, month)

        bundle = read_performance(portefeuille)
        for month in months:
            details = bundle["years"][month[:4]]["details"][month]
            if details["refresh_reason"] in ("missing_report", "incomplete_report"):
                details["refresh_reason"] = "preparation_failed"
        return bundle


def prepare_performance(portefeuille: str) -> dict:
    from management_portfolios import MANAGEMENT_GROUPS, key
    from routers._airs_monthly_lookthrough import add_lookthrough

    bundle = _prepare_performance(portefeuille)
    if MANAGEMENT_GROUPS.get(key(portefeuille)) == "toppenberg":
        add_lookthrough(bundle)
    return bundle


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
    ctx.progress(0, 4, 'Waiting for AIRS…')
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
                     "valuation_component": getattr(h, 'valuation_component', None),
                     "price_local": getattr(h, 'current_price_local', None),
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

        tasks = [("performance", "Loading returns…", performance),
                 ("opening", "Loading portfolio…", lambda: holdings(opening)),
                 ("closing", "Loading portfolio…", lambda: holdings(end)),
                 ("transactions", "Checking changes…", transactions)]
        for n, (key, label, fn) in enumerate(tasks):
            ctx.check()
            ctx.progress(n, 4, label)
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
        "performance": sections["performance"].get("data"), "performance_basis": "year_start",
    }, on_conflict="portefeuille,month").execute()
    ctx.progress(4, 4, 'Finishing update…')
    failures = sum(s["status"] == "error" for s in sections.values())
    if failures:
        from jobs import UserFacingJobError
        raise UserFacingJobError(f"Saved a partial report for {month}: {failures} AIRS report(s) failed. Open Report for details.")
    return 'Performance updated.'
