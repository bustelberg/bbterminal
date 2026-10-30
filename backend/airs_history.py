"""Year-scoped custodian sources, separate from the replaceable live YTD caches."""
from datetime import datetime, timezone, date


def archive_source(db, account, start, end, kind, payload):
    if start[:4] != end[:4]:
        raise ValueError("An AIRS source archive must cover a single calendar year")
    year = int(start[:4])
    old = (db.table("airs_year_archive").select("*").eq("portefeuille", account)
           .eq("year", year).eq("kind", kind).limit(1).execute().data or [])
    if old and (old[0]["datum_tot"] > end or old[0]["datum_van"] < start
                or (kind == "year_end" and old[0].get("payload", {}).get("complete"))):
        return
    db.table("airs_year_archive").upsert({
        "portefeuille": account, "year": year, "kind": kind,
        "datum_van": start, "datum_tot": end, "payload": payload,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }, on_conflict="portefeuille,year,kind").execute()


def finalise_previous_year(ctx=None, today=None):
    """Retry unfinished December bundles during the existing post-AIRS workflow.

    Historical exports never pass through writers of the live YTD caches. A
    complete receipt is skipped on subsequent days; partial sources survive retries.
    """
    from zoneinfo import ZoneInfo
    from deps import supabase

    current = today or datetime.now(ZoneInfo("Europe/Amsterdam")).date()
    years = {current.year - 1}
    offset = 0
    while True:
        page = (supabase.table("airs_year_archive").select("portefeuille,year,payload")
                .eq("kind", "year_end").order("portefeuille").order("year")
                .range(offset, offset + 999).execute().data or [])
        years.update(row["year"] for row in page if row["year"] < current.year
                     and not row.get("payload", {}).get("complete"))
        if len(page) < 1000:
            break
        offset += 1000
    results = []
    for year in sorted(years):
        results.extend(_finalise_year(ctx, year))
    return results


def _finalise_year(ctx, year):
    from deps import supabase
    from routers._airs_monthly_report import collect_report
    from airs_vermogen import _acquire_session, _LOCK, latest_completed_market_date
    from airs_scanner import download_mutaties_sync, download_transacties_sync, AirsNoData
    from airs_mutaties import parse_mutaties
    from airs_transacties import parse_transacties, realised_results
    from dataclasses import asdict
    from airs_portfolio_exclusions import is_excluded_portfolio

    start, end = f"{year}-01-01", f"{year}-12-31"
    accounts = set()
    offset = 0
    while True:
        page = (supabase.table("airs_performance").select("portefeuille,periode")
                .gte("periode", start).lte("periode", end).order("portefeuille").order("periode")
                .range(offset, offset + 999).execute().data or [])
        accounts.update(row["portefeuille"] for row in page)
        if len(page) < 1000:
            break
        offset += 1000
    accounts = sorted(account for account in accounts if not is_excluded_portfolio(account))
    class Context:
        def check(self):
            if ctx is not None:
                ctx.check()
        def progress(self, *args):
            if ctx is not None:
                ctx.progress(*args)
    context = Context()
    results = []
    for account in accounts:
        context.check()
        sources = (supabase.table("airs_year_archive").select("*")
                   .eq("portefeuille", account).eq("year", year).execute().data or [])
        by_kind = {r["kind"]: r for r in sources}
        if by_kind.get("year_end", {}).get("payload", {}).get("complete"):
            continue
        errors = []
        try:
            saved = (supabase.table("airs_monthly_report").select("*")
                     .eq("portefeuille", account).eq("month", f"{year}-12-01").limit(1).execute().data or [])
            # A year-end export is requested after the year ends, even when an
            # earlier December snapshot was saved successfully.
            expected = latest_completed_market_date(date(year + 1, 1, 1))
            def complete(rows):
                if not rows or str(rows[0]["datum_tot"])[:10] != end:
                    return False
                sections = rows[0]["report"].get("sections", {})
                perf = sections.get("performance", {}).get("data") or {}
                closing = sections.get("closing", {}).get("data") or {}
                return (all(sections.get(k, {}).get("status") == "ok" for k in
                            ("performance", "opening", "closing", "transactions"))
                        and str(perf.get("periode", ""))[:10] >= expected
                        and perf.get("cumulatief_rendement") is not None
                        and (sections["transactions"].get("data") or {}).get("readable") is True
                        and str(closing.get("as_of", ""))[:10] >= expected)
            if not complete(saved):
                collect_report(account, f"{year}-12", context)
                saved = (supabase.table("airs_monthly_report").select("*")
                         .eq("portefeuille", account).eq("month", f"{year}-12-01").limit(1).execute().data or [])
            if not complete(saved):
                errors.append("December's final AIRS valuation or report sections are incomplete")
            for kind in ("transactions", "journal"):
                old = by_kind.get(kind)
                if (old and old["datum_van"] == start and old["datum_tot"] == end
                        and str(old.get("fetched_at", ""))[:10] >= f"{year + 1}-01-01"
                        and not old.get("payload", {}).get("coverage_unknown")):
                    continue
                if not _acquire_session(30):
                    raise RuntimeError("AIRS is busy; year-end archive will retry")
                try:
                    if kind == "transactions":
                        try:
                            sheet = parse_transacties(download_transacties_sync(account, start, end))
                            if sheet.rows and realised_results(sheet).unreadable:
                                raise ValueError("Year-end transaction export is unreadable")
                            payload = {"columns": sheet.columns, "kinds": sheet.kinds, "rows": sheet.rows}
                        except AirsNoData:
                            payload = {"columns": [], "kinds": {}, "rows": []}
                    else:
                        payload = {"rows": [dict(asdict(row), boekdatum=row.boekdatum.isoformat() if row.boekdatum else None)
                                            for row in parse_mutaties(download_mutaties_sync(account, start, end))]}
                    archive_source(supabase, account, start, end, kind, payload)
                finally:
                    _LOCK.release()
            archive_source(supabase, account, start, end, "year_end", {
                "complete": not errors, "errors": errors,
                "performance": saved[0].get("performance") if saved else None,
            })
        except Exception as exc:
            errors.append(str(exc))
            archive_source(supabase, account, start, end, "year_end", {"complete": False, "errors": errors})
        results.append({"portefeuille": account, "year": year, "errors": errors})
    return results
