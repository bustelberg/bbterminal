"""Daily dashboard stages and a read-only, per-account freshness receipt.

No function in this module opens AIRS. Monthly reports are assembled from dated
custodian snapshots and the transaction export saved by the one daily scan.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from contextlib import contextmanager
from contextvars import ContextVar
import os
from zoneinfo import ZoneInfo

from deps import supabase

_lease_held = ContextVar("dashboard_daily_lease", default=False)


@contextmanager
def daily_lease(job_id: str, triggered_by: str):
    """Serialize dashboard writers across replicas; nested preparation shares the lease."""
    from common.pg import _db_url
    import psycopg
    if _lease_held.get():
        yield True
        return
    url = _db_url()
    if not url:
        if os.environ.get("RAILWAY_PUBLIC_DOMAIN"):
            raise RuntimeError("Daily dashboard jobs require DATABASE_URL or SUPABASE_DB_URL for the cross-replica lock")
        yield True
        return
    with psycopg.connect(url, connect_timeout=5, autocommit=True, options="-c statement_timeout=5000") as conn:
        locked = conn.execute("SELECT pg_try_advisory_lock(hashtext('bbterminal.dashboard_daily'))").fetchone()[0]
        if not locked:
            raise RuntimeError("Another dashboard daily refresh holds the cross-replica lock")
        token = _lease_held.set(True)
        try:
            # At most one automatic AIRS attempt per Amsterdam day, including failed attempts.
            # An explicit manual retry is still available from the jobs page.
            run = []
            if job_id == "airs_vermogen_refresh" and triggered_by == "auto":
                start = datetime.now(ZoneInfo("Europe/Amsterdam")).replace(hour=0, minute=0, second=0, microsecond=0)
                run = (supabase.table("scheduled_job_run").select("id").eq("job_id", job_id)
                       .eq("triggered_by", "auto").gte("started_at", start.astimezone(timezone.utc).isoformat())
                       .in_("status", ["running", "ok", "error", "cancelled"]).limit(1).execute().data or [])
            yield not bool(run)
        finally:
            _lease_held.reset(token)
            conn.execute("SELECT pg_advisory_unlock(hashtext('bbterminal.dashboard_daily'))")


class IncompleteRefresh(RuntimeError):
    """A failed stage whose structured receipt must survive in scheduled_job_run."""

    def __init__(self, message: str, summary: dict):
        super().__init__(message)
        self.summary = summary


def rows(table: str, columns: str = "*", **equal) -> list[dict]:
    result, offset = [], 0
    while True:
        query = supabase.table(table).select(columns)
        for key, value in equal.items():
            query = query.eq(key, value)
        batch = query.range(offset, offset + 999).execute().data or []
        result.extend(batch)
        if len(batch) < 1000:
            return result
        offset += len(batch)


def held_company_ids() -> list[tuple[int, str]]:
    """Map the union of actual and model-held ISINs to GuruFocus companies."""
    accounts = rows("airs_account_roster", "portefeuille,last_seen_at")
    newest = max((str(row.get("last_seen_at") or "") for row in accounts), default="")
    isins = {r["isin"] for r in rows("airs_model_portfolio_position", "isin") if r.get("isin")}
    for account in accounts:
        if str(account.get("last_seen_at") or "") != newest:
            continue
        name = account["portefeuille"]
        latest = (supabase.table("airs_holding").select("as_of_date").eq("portefeuille", name)
                  .order("as_of_date", desc=True).limit(1).execute().data or [])
        if latest:
            isins.update(r["isin"] for r in rows("airs_holding", "isin", portefeuille=name,
                                                as_of_date=latest[0]["as_of_date"]) if r.get("isin"))
    isins = sorted(isins)
    ids: set[int] = set()
    for offset in range(0, len(isins), 100):
        batch = (supabase.table("asset_grid").select("company_id")
                 .in_("isin", isins[offset:offset + 100]).execute().data or [])
        ids.update(int(row["company_id"]) for row in batch if row.get("company_id") is not None)
        # The company table also knows holdings whose asset-grid bridge has not been filled.
        batch = (supabase.table("company").select("company_id")
                 .in_("isin", isins[offset:offset + 100]).execute().data or [])
        ids.update(int(row["company_id"]) for row in batch)
    result = []
    ordered = sorted(ids)
    for offset in range(0, len(ordered), 100):
        batch = (supabase.table("company")
                 .select("company_id,gurufocus_exchange:gurufocus_exchange(exchange_code)")
                 .in_("company_id", ordered[offset:offset + 100]).execute().data or [])
        result.extend((row["company_id"], (row.get("gurufocus_exchange") or {}).get("exchange_code") or "")
                      for row in batch)
    return result


def prepare_month(account: str, today=None) -> dict:
    """Rebuild this month's saved report without downloading any AIRS report."""
    from airs_transacties import ParsedSheet, trades, realised_results, TT, TT_BUY, TT_SELL, COL_FONDS, COL_DATE
    from routers._airs_monthly_report import month_window, _monthly_rows
    from routers._airs_monthly_positions import position_performance
    from routers._airs_monthly_reconciliation import reconcile_positions
    from airs_vermogen import latest_completed_market_date

    today = today or datetime.now(ZoneInfo("Europe/Amsterdam")).date()
    # On the first weekend/New Year, the newest completed month can still be the previous one.
    month = latest_completed_market_date(today)[:7]
    start, end, opening = month_window(month, today)
    performance = next((r for r in _monthly_rows(account, int(month[:4])) if r["month"] == month), None)
    sections: dict = {}
    sections["performance"] = ({"status": "ok", "data": performance}
                               if performance and str(performance.get("periode", ""))[:10] == end
                               else {"status": "error", "data": None, "message": f"No AIRS return for {end}"})
    for key, boundary in (("opening", opening), ("closing", end)):
        oldest = (datetime.fromisoformat(boundary) - timedelta(days=6)).date().isoformat()
        stored = (supabase.table("airs_holding").select("*").eq("portefeuille", account)
                  .gte("as_of_date", oldest).lte("as_of_date", boundary)
                  .order("as_of_date", desc=True).range(0, 999).execute().data or [])
        if stored:
            mark = str(stored[0]["as_of_date"])[:10]
            if key == "closing" and mark != end:
                sections[key] = {"status": "error", "data": None, "message": f"Latest valuation is {mark}, expected {end}"}
            else:
                sections[key] = {"status": "ok", "data": {
                    "as_of": mark, "requested_date": boundary,
                    "rows": [{"name": r["holding_name"], "isin": r.get("isin"), "currency": r.get("currency"),
                              "quantity": r.get("quantity"), "value_eur": r.get("current_value_eur"),
                              "weight_pct": r.get("airs_weight")} for r in stored if str(r["as_of_date"])[:10] == mark],
                }}
        else:
            # An existing historical export can supply an opening before we began daily snapshots.
            saved = (supabase.table("airs_monthly_report").select("report")
                     .eq("portefeuille", account).eq("month", start).limit(1).execute().data or [])
            previous = ((saved[0].get("report") or {}).get("sections") or {}).get(key) if saved else None
            sections[key] = previous if previous and key == "opening" else {
                "status": "error", "data": None, "message": f"No stored AIRS valuation for {boundary}"}
    cached = rows("airs_transactie_snapshot", portefeuille=account)
    snapshot = cached[0] if cached else None
    if (snapshot and str(snapshot.get("datum_van", ""))[:10] <= start
            and str(snapshot.get("datum_tot", ""))[:10] >= end):
        sheet = ParsedSheet(columns=snapshot.get("columns") or [], kinds=snapshot.get("kinds") or {}, rows=snapshot.get("rows") or [])
        activity = [{"name": t.fonds, "kind": t.kind, "date": t.datum, "amount_eur": t.eur, "quantity": t.quantity}
                    for t in trades(sheet) if t.datum and start <= t.datum <= end]
        uncertain = [r for r in sheet.rows if (not r.get(COL_DATE) or start <= str(r[COL_DATE]) <= end)
                     and (r.get(TT) not in (TT_BUY, TT_SELL) or not r.get(COL_DATE))]
        sections["transactions"] = {"status": "ok", "data": {
            "columns": sheet.columns, "kinds": sheet.kinds, "rows": sheet.rows, "activity": activity,
            # scan_one persists an explicitly empty export for AIRS's no-data answer.
            "readable": not sheet.rows or not realised_results(sheet).unreadable,
            "unclassified_names": sorted({str(r[COL_FONDS]) for r in uncertain if r.get(COL_FONDS)}),
            "unclassified_unassigned": any(not r.get(COL_FONDS) for r in uncertain),
        }}
    else:
        sections["transactions"] = {"status": "error", "data": None, "message": "Stored transactions do not cover this month"}
    positions = position_performance(sections, start, end)
    fetched = datetime.now(timezone.utc).isoformat()
    payload = {"start": start, "end": end, "opening_date": opening, "performance_basis": "year_start",
               "sections": sections, "source": "AIRS", "fetched_at": fetched, "positions": positions,
               "reconciliation": reconcile_positions(sections, performance, positions)}
    supabase.table("airs_monthly_report").upsert({
        "portefeuille": account, "month": start, "datum_tot": end, "fetched_at": fetched,
        "report": payload, "performance": performance,
    }, on_conflict="portefeuille,month").execute()
    return {"account": account, "end": end,
            "errors": {key: value.get("message") for key, value in sections.items() if value["status"] == "error"}}


def freshness(today=None) -> dict:
    """Read actual source dates and completeness; never repair or fetch a vendor."""
    from airs_vermogen import REPORTS, latest_completed_market_date
    from airs_portfolio_exclusions import is_excluded_portfolio
    from routers._airs_account_links import list_account_links
    from routers import _prepared_analyse as prepared

    today = today or datetime.now(ZoneInfo("Europe/Amsterdam")).date()
    day = today.isoformat()
    expected = latest_completed_market_date(today)
    roster = rows("airs_account_roster", "portefeuille,last_seen_at,reports_at,reports_ok")
    discovered = max((str(r.get("last_seen_at") or "") for r in roster), default="")
    roster = [r for r in roster if str(r.get("last_seen_at") or "") == discovered
              and not is_excluded_portfolio(r["portefeuille"])]
    links = {r["portefeuille"]: r for r in list_account_links()["accounts"]}
    models = {r["id"]: r for r in rows("airs_model_portfolio", "id,positions_scanned_at,positions_error")}
    checks = {r["portefeuille"]: r for r in rows("airs_price_check", "portefeuille,checked_at")}
    execution = {r["isin"]: r for r in rows("asset_execution", "isin,status,analysis_id,yahoo_symbol,currency")}
    assets = {r["analysis_id"]: r for r in rows("asset_analysis", "analysis_id,price_to")}
    fx_dates: dict[str, str] = {}
    from asset_pipeline.fx import SUBUNIT
    currencies = {SUBUNIT.get(r.get("currency"), (r.get("currency"), 1))[0] for r in execution.values()}
    for code in sorted(currencies - {None, "", "EUR"}):
        rate = (supabase.table("fx_rate").select("rate_date").eq("currency_code", code)
                .order("rate_date", desc=True).limit(1).execute().data or [])
        if rate:
            fx_dates[code] = str(rate[0]["rate_date"])[:10]
    sources, global_issues = {}, []
    for job in ("asset_price_refresh", "benchmark_price_slice", "benchmark_fundamentals_fill", "fx_sync"):
        latest = (supabase.table("scheduled_job_run").select("status,started_at,detail,summary")
                  .eq("job_id", job).order("started_at", desc=True).limit(1).execute().data or [])
        source = latest[0] if latest else {}
        sources[job] = source
        if source.get("status") != "ok" or str(source.get("started_at") or "")[:10] < day:
            global_issues.append(f"{job}: no successful source refresh today")
    monthly = {r["portefeuille"]: r for r in rows("airs_monthly_report", "portefeuille,datum_tot,report", month=f"{expected[:7]}-01")}
    accounts = []
    for row in roster:
        name = row["portefeuille"]
        issues, unsupported, price_dates = [], [], {}
        missing = sorted(set(REPORTS) - set(row.get("reports_ok") or []))
        if missing:
            issues.append(f"missing reports: {', '.join(missing)}")
        if str(row.get("reports_at") or "")[:10] < day:
            issues.append("AIRS reports were not checked today")
        latest = (supabase.table("airs_holding").select("as_of_date").eq("portefeuille", name)
                  .order("as_of_date", desc=True).limit(1).execute().data or [])
        as_of = str(latest[0]["as_of_date"])[:10] if latest else None
        held = rows("airs_holding", "isin,as_of_date,holding_name,current_value_eur", portefeuille=name,
                    as_of_date=as_of) if as_of else []
        if not as_of or as_of < expected:
            issues.append(f"holdings dated {as_of}; expected {expected}")
        latest_perf = (supabase.table("airs_performance").select("periode").eq("portefeuille", name)
                       .not_.is_("cumulatief_rendement", "null").order("periode", desc=True).limit(1).execute().data or [])
        perf = str(latest_perf[0]["periode"])[:10] if latest_perf else None
        if not perf or perf < expected:
            issues.append(f"return dated {perf}; expected {expected}")
        for holding in (r for r in held if str(r["as_of_date"])[:10] == as_of):
            isin = holding.get("isin")
            if not isin:
                continue
            mapped = execution.get(isin) or {}
            if not mapped.get("yahoo_symbol") or mapped.get("status") != "ok":
                unsupported.append(isin)
                continue
            close = str((assets.get(mapped.get("analysis_id")) or {}).get("price_to") or "")[:10]
            price_dates[isin] = close or None
            if close < expected:
                issues.append(f"{isin}: price dated {close or 'absent'}")
            currency = mapped.get("currency")
            major = SUBUNIT.get(currency, (currency, 1))[0]
            if major and major != "EUR" and fx_dates.get(major, "") < expected:
                issues.append(f"{major}: FX dated {fx_dates.get(major) or 'absent'}")
        pid = (links.get(name) or {}).get("model_portfolio_id")
        if pid is not None:
            model = models.get(pid) or {}
            if model.get("positions_error") or str(model.get("positions_scanned_at") or "")[:10] < day:
                issues.append("model composition not successfully scanned today")
            for look in (False, True):
                if prepared.read("holdings", prepared.holdings_key(pid, "ACWI", look))[1] is None:
                    issues.append(f"Analyse snapshot missing or outdated (look-through={look})")
        if str((checks.get(name) or {}).get("checked_at") or "")[:10] < day:
            issues.append("price checks not saved today")
        report = monthly.get(name) or {}
        if str(report.get("datum_tot") or "")[:10] < expected:
            issues.append("current monthly report is outdated or missing")
        elif any(s.get("status") == "error" for s in ((report.get("report") or {}).get("sections") or {}).values()):
            issues.append("current monthly report has missing source sections")
        month_payload = report.get("report") or {}
        unavailable = sum(p.get("return_pct") is None and not p.get("is_cash") for p in month_payload.get("positions") or [])
        if unavailable:
            issues.append(f"{unavailable} monthly position returns unavailable")
        accounts.append({"portefeuille": name, "holdings_as_of": as_of, "return_as_of": perf,
                         "price_dates": price_dates, "unmapped_isins": sorted(set(unsupported)),
                         "monthly_reconciliation": (month_payload.get("reconciliation") or {}).get("status"),
                         "status": "incomplete" if issues or unsupported else "ok", "issues": issues})
    incomplete = sum(r["status"] != "ok" for r in accounts)
    return {"day": day, "expected_market_date": expected, "accounts": accounts,
            "checked": len(accounts), "incomplete": incomplete, "empty_roster": not bool(accounts),
            "source_jobs": sources, "global_issues": global_issues, "fx_dates": fx_dates}
