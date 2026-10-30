from datetime import date
from types import SimpleNamespace

import pytest
import deps
import airs_history as history
from tests._fake_supabase import FakeSupabase


def test_year_sources_survive_january_and_narrower_reads_do_not_regress():
    db = FakeSupabase()
    history.archive_source(db, "BOOK", "2026-01-01", "2026-12-31", "transactions", {"rows": [1]})
    history.archive_source(db, "BOOK", "2027-01-01", "2027-01-04", "transactions", {"rows": [2]})
    history.archive_source(db, "BOOK", "2026-10-01", "2026-12-31", "transactions", {"rows": []})
    history.archive_source(db, "BOOK", "2026-01-01", "2026-11-30", "transactions", {"rows": []})
    rows = db.table("airs_year_archive").select("*").order("year").execute().data
    assert [r["payload"]["rows"] for r in rows] == [[1], [2]]
    with pytest.raises(ValueError):
        history.archive_source(db, "BOOK", "2026-12-01", "2027-01-01", "journal", {})


def test_archive_failure_prevents_transaction_cache_deletion(monkeypatch):
    from routers import _airs_transacties as transactions
    db = FakeSupabase({"airs_transactie_snapshot": [{"portefeuille": "BOOK", "rows": ["old"]}]})
    monkeypatch.setattr(transactions, "supabase", db)
    def fail(*args):
        raise RuntimeError("archive storage unavailable")
    monkeypatch.setattr(history, "archive_source", fail)
    assert transactions._store("BOOK", "2027-01-01", "2027-01-04", SimpleNamespace(columns=[], kinds={}, rows=[]))
    assert db.table("airs_transactie_snapshot").select("*").execute().data[0]["rows"] == ["old"]


def december(perf_date="2026-12-31"):
    performance = {"periode": perf_date, "cumulatief_rendement": 7.59,
                   "beginvermogen": 100, "eindvermogen": 108}
    return {"portefeuille": "BOOK", "month": "2026-12-01", "datum_tot": "2026-12-31",
            "performance": performance, "report": {"sections": {
                "performance": {"status": "ok", "data": performance},
                "opening": {"status": "ok", "data": {}},
                "closing": {"status": "ok", "data": {"as_of": perf_date}},
                "transactions": {"status": "ok", "data": {"readable": True}},
            }}}


def test_year_end_retries_late_valuation_then_skips_complete_archive(monkeypatch):
    from routers import _airs_monthly_report as monthly
    db = FakeSupabase({"airs_performance": [{"portefeuille": "BOOK", "periode": "2026-12-29"}],
                       "airs_monthly_report": [december("2026-12-29")]})
    monkeypatch.setattr(deps, "supabase", db)
    for kind in ("transactions", "journal"):
        history.archive_source(db, "BOOK", "2026-01-01", "2026-12-31", kind, {"rows": []})
    db.table("airs_year_archive").update({"fetched_at": "2027-01-04T11:00:00Z"}).execute()
    calls = []
    monkeypatch.setattr(monthly, "collect_report", lambda *args: calls.append(args[:2]))
    first = history.finalise_previous_year(today=date(2027, 1, 4))
    assert first[0]["errors"]
    receipt = db.table("airs_year_archive").select("*").eq("kind", "year_end").execute().data[0]
    assert receipt["payload"]["complete"] is False
    db.table("airs_monthly_report").upsert(december(), on_conflict="portefeuille,month").execute()
    assert history.finalise_previous_year(today=date(2027, 1, 5))[0]["errors"] == []
    assert history.finalise_previous_year(today=date(2027, 1, 6)) == []
    assert calls == [("BOOK", "2026-12")]


def test_annual_summary_uses_exact_cumulative_and_full_period_benchmark(monkeypatch):
    from routers import _airs_monthly_report as monthly
    db = FakeSupabase({"airs_performance": [
        {"portefeuille": "BOOK", "periode": "2025-06-30", "rendement": 10,
         "cumulatief_rendement": 10, "beginvermogen": 100, "eindvermogen": 110},
        {"portefeuille": "BOOK", "periode": "2025-12-31", "rendement": -10,
         "cumulatief_rendement": 7.59, "beginvermogen": 110, "eindvermogen": 108},
        {"portefeuille": "BOOK", "periode": "2026-10-29", "cumulatief_rendement": 99},
    ]})
    monkeypatch.setattr(monthly, "supabase", db)
    calls = []
    def benchmark(account, rows):
        calls.append(rows)
        return {r["month"]: {"return_pct": 5.12} for r in rows}
    monkeypatch.setattr(monthly, "_weighted_months", benchmark)
    result = monthly.read_report("BOOK", "2025-12", include_details=False)
    assert result["available_years"] == [2026, 2025]
    assert result["year_summary"] == {"return_pct": 7.59, "benchmark_pct": 5.12,
        "start": "2025-06-01", "end": "2025-12-31", "complete": False}
    assert calls[0] == [{"month": "2025-12", "periode": "2025-12-31", "anchor": "2025-05-31"}]


def test_january_weekend_still_allows_year_selection_without_a_new_valuation(monkeypatch):
    from datetime import datetime
    from routers import _airs_monthly_report as monthly
    class January(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2028, 1, 2, 12, tzinfo=tz)
    monkeypatch.setattr(monthly, "datetime", January)
    db = FakeSupabase({"airs_performance": [{"portefeuille": "BOOK", "periode": "2027-12-31",
        "cumulatief_rendement": 7.59, "beginvermogen": 100, "eindvermogen": 108}]})
    monkeypatch.setattr(monthly, "supabase", db)
    result = monthly.read_report("BOOK", "2028-01", include_details=False)
    assert result["available_years"] == [2028, 2027]
    assert result["months"] == []
    assert result["year_summary"]["return_pct"] is None
    assert "no completed valuation" in result["warning"]
    assert db.writes == []


def test_final_exports_use_archives_without_replacing_live_new_year_sources(monkeypatch):
    import airs_scanner
    import airs_vermogen
    import airs_transacties
    import airs_mutaties
    db = FakeSupabase({"airs_performance": [{"portefeuille": "BOOK", "periode": "2026-12-31"}],
        "airs_monthly_report": [december()],
        "airs_transactie_snapshot": [{"portefeuille": "BOOK", "datum_van": "2027-01-01", "rows": ["live"]}],
        "airs_mutatie": [{"portefeuille": "BOOK", "boekdatum": "2027-01-04", "amount_eur": 5}]})
    monkeypatch.setattr(deps, "supabase", db)
    locks = []
    monkeypatch.setattr(airs_vermogen, "_acquire_session", lambda timeout: locks.append("acquire") or True)
    monkeypatch.setattr(airs_vermogen, "_LOCK", SimpleNamespace(release=lambda: locks.append("release")))
    calls = []
    def download(account, start, end):
        calls.append((account, start, end))
        return b"export"
    monkeypatch.setattr(airs_scanner, "download_transacties_sync", download)
    monkeypatch.setattr(airs_scanner, "download_mutaties_sync", download)
    monkeypatch.setattr(airs_transacties, "parse_transacties", lambda raw: airs_transacties.ParsedSheet())
    monkeypatch.setattr(airs_mutaties, "parse_mutaties", lambda raw: [])
    result = history.finalise_previous_year(today=date(2027, 1, 4))
    assert result[0]["errors"] == []
    assert calls == [("BOOK", "2026-01-01", "2026-12-31")] * 2
    assert locks == ["acquire", "release", "acquire", "release"]
    assert db.tables["airs_transactie_snapshot"][0]["rows"] == ["live"]
    assert db.tables["airs_mutatie"][0]["boekdatum"] == "2027-01-04"


def test_older_pending_year_is_not_forgotten_at_next_rollover(monkeypatch):
    db = FakeSupabase({"airs_year_archive": [{"portefeuille": "BOOK", "year": 2025,
        "kind": "year_end", "payload": {"complete": False}}]})
    monkeypatch.setattr(deps, "supabase", db)
    years = []
    monkeypatch.setattr(history, "_finalise_year", lambda ctx, year: years.append(year) or [])
    history.finalise_previous_year(today=date(2028, 1, 4))
    assert years == [2025, 2027]


def test_annual_benchmark_reads_year_opening_price_not_december_opening(monkeypatch):
    from routers import _airs_monthly_report as monthly, _benchmark_index
    db = FakeSupabase({"benchmark": [{"ticker": "ACWI", "benchmark_id": 1}], "benchmark_price": [
        {"benchmark_id": 1, "target_date": "2025-12-31", "price": 100},
        {"benchmark_id": 1, "target_date": "2026-11-30", "price": 110},
        {"benchmark_id": 1, "target_date": "2026-12-31", "price": 120},
    ]})
    monkeypatch.setattr(monthly, "supabase", db)
    monkeypatch.setattr(_benchmark_index, "_fx_to_eur", lambda *args: {})
    monkeypatch.setattr(_benchmark_index, "_rate", lambda *args: 1)
    annual = monthly._acwi_months([{"month": "2026-12", "periode": "2026-12-31", "anchor": "2025-12-31"}])
    december_return = monthly._acwi_months([{"month": "2026-12", "periode": "2026-12-31"}])
    assert annual["2026-12"]["return_pct"] == pytest.approx(20)
    assert december_return["2026-12"]["return_pct"] == pytest.approx(100 * (120 / 110 - 1))


def test_january_preparation_reads_december_transactions_from_archive(monkeypatch):
    import dashboard_daily as daily
    from routers import _airs_monthly_report as monthly
    db = FakeSupabase({"airs_holding": [
        {"portefeuille": "BOOK", "as_of_date": day, "holding_name": "Stock", "isin": "TEST",
         "quantity": 1, "current_value_eur": 100} for day in ("2026-11-30", "2026-12-31")],
        "airs_transactie_snapshot": [{"portefeuille": "BOOK", "datum_van": "2027-01-01", "datum_tot": "2027-01-01", "rows": ["new year"]}]})
    history.archive_source(db, "BOOK", "2026-01-01", "2026-12-31", "transactions", {"columns": [], "kinds": {}, "rows": []})
    monkeypatch.setattr(daily, "supabase", db)
    monkeypatch.setattr(monthly, "_monthly_rows", lambda *args: [{"month": "2026-12", "periode": "2026-12-31",
        "rendement": 0, "beginvermogen": 100, "eindvermogen": 100, "stortingen": 0, "onttrekkingen": 0}])
    assert daily.prepare_month("BOOK", date(2027, 1, 1))["errors"] == {}
    saved = db.tables["airs_monthly_report"][0]
    assert saved["report"]["sections"]["transactions"]["data"]["rows"] == []


def test_incomplete_year_end_fails_post_job_and_still_runs_audit(monkeypatch):
    import scheduler
    import dashboard_daily
    from routers import _airs_accounts, _airs_price_check, _prepared_analyse
    monkeypatch.setattr(_airs_accounts, "list_accounts", lambda: [])
    monkeypatch.setattr(_airs_price_check, "refresh_price_checks", lambda *args, **kwargs: {})
    monkeypatch.setattr(_prepared_analyse, "run_controlled", lambda **kwargs: {})
    monkeypatch.setattr(history, "finalise_previous_year", lambda ctx: [{"portefeuille": "BOOK", "year": 2026, "errors": ["late valuation"]}])
    audits = []
    monkeypatch.setattr(scheduler, "_run_body", lambda *args: audits.append(args[0]))
    with pytest.raises(dashboard_daily.IncompleteRefresh) as caught:
        scheduler._body_dashboard_post_airs()
    assert caught.value.summary["errors"]["year_end"][0]["errors"] == ["late valuation"]
    assert audits == ["dashboard_freshness"]


def test_january_daily_preparation_cannot_replace_a_finalised_december(monkeypatch):
    import dashboard_daily as daily
    db = FakeSupabase({"airs_monthly_report": [december()]})
    history.archive_source(db, "BOOK", "2026-01-01", "2026-12-31", "year_end", {"complete": True})
    before = list(db.writes)
    monkeypatch.setattr(daily, "supabase", db)
    result = daily.prepare_month("BOOK", date(2027, 1, 1))
    assert result["archived"] is True
    assert db.writes == before
    assert db.tables["airs_monthly_report"][0]["performance"]["cumulatief_rendement"] == 7.59
