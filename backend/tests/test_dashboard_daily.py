"""Daily dependencies, failure receipts and stored-only preparation boundaries."""
from datetime import date
from types import SimpleNamespace
from contextlib import contextmanager

import pytest

import dashboard_daily as D
import scheduler as S
from tests._fake_supabase import FakeSupabase


def test_market_chain_completes_independent_sources_and_reports_failure(monkeypatch):
    from asset_pipeline import queue
    monkeypatch.setattr(queue, "is_worker_active", lambda: False)
    calls = []
    def run(stage, ctx=None):
        calls.append(stage)
        if stage == "asset_price_refresh":
            raise RuntimeError("Yahoo failed")
    monkeypatch.setattr(S, "_run_body", run)
    with pytest.raises(D.IncompleteRefresh) as caught:
        S._body_dashboard_market()
    assert calls == ["fx_sync", "asset_price_refresh", "benchmark_price_slice", "benchmark_fundamentals_fill"]
    assert caught.value.summary["errors"] == {"asset_price_refresh": "Yahoo failed"}


def test_incomplete_job_keeps_structured_receipt(monkeypatch):
    record = SimpleNamespace(summary=None)
    @contextmanager
    def recording(*args, **kwargs):
        yield record
    monkeypatch.setattr(S, "record_run", recording)
    def fail(ctx):
        raise D.IncompleteRefresh("partial", {"accounts": [{"portefeuille": "BOOK", "issues": ["old close"]}]})
    monkeypatch.setitem(S.JOB_BODIES, "unit-receipt", fail)
    with pytest.raises(D.IncompleteRefresh):
        S._run_body("unit-receipt")
    assert record.summary["accounts"][0]["issues"] == ["old close"]


def test_stored_model_pricing_never_reads_airs(monkeypatch):
    from routers import _airs_portfolio_refresh as refresh
    monkeypatch.setattr(refresh, "supabase", FakeSupabase({
        "airs_model_portfolio": [{"id": 1, "positions_datum": "2026-01-01", "positions_scanned_at": date.today().isoformat()}],
        "airs_model_portfolio_position": [{"portfolio_id": 1, "isin": "TEST", "fonds": "Stock", "percentage": 100}],
    }))
    monkeypatch.setattr(refresh, "_composition", lambda *_: pytest.fail("second AIRS composition fetch"))
    monkeypatch.setattr(refresh, "_instruments", lambda *_: {})
    monkeypatch.setattr(refresh, "_fx", lambda *_: {"failed": 0, "backfilled": 0, "extended": 0})
    monkeypatch.setattr(refresh, "_prices", lambda *_: {"failed": 0, "fetched": 0, "already_current": 1})
    monkeypatch.setattr(refresh, "_recompute", lambda *_: {"portfolio": {"ytd_pct": 1}})
    assert refresh.refresh_portfolio(1, lambda *_a, **_k: None, stored_composition=True)["ytd_pct"] == 1


def test_busy_airs_is_a_failed_attempt_without_a_model_scan(monkeypatch):
    import airs_vermogen
    import airs_scanner
    monkeypatch.setattr(airs_vermogen, "run_airs_vermogen_refresh_sync", lambda **_: {"status": "busy"})
    monkeypatch.setattr(airs_scanner, "fetch_model_portfolios_sync", lambda *_: pytest.fail("busy AIRS must not be re-entered"))
    with pytest.raises(D.IncompleteRefresh, match="could not acquire"):
        S._body_airs_vermogen()


def test_model_pricing_deduplicates_shared_models_and_surfaces_failures(monkeypatch):
    from routers import _airs_account_links as links, _airs_portfolio_refresh as refresh
    monkeypatch.setattr(links, "list_account_links", lambda: {"accounts": [
        {"portefeuille": "A", "model_portfolio_id": 1}, {"portefeuille": "B", "model_portfolio_id": 1},
        {"portefeuille": "C", "model_portfolio_id": 2}]})
    calls = []
    def price(pid, emit, *, stored_composition):
        assert stored_composition
        calls.append(pid)
        if pid == 2:
            raise RuntimeError("bad price")
        return {}
    monkeypatch.setattr(refresh, "refresh_portfolio", price)
    with pytest.raises(D.IncompleteRefresh) as caught:
        S._body_airs_model_prices()
    assert calls == [1, 2]
    assert caught.value.summary["failed_names"] == ["C"]


def test_freshness_reads_actual_dates_and_reports_unmapped_coverage(monkeypatch):
    from routers import _airs_account_links as links, _prepared_analyse as prepared
    db = FakeSupabase({
        "airs_account_roster": [{"portefeuille": "BOOK", "last_seen_at": "2026-10-30T10:00:00Z",
                                "reports_at": "2026-10-30T10:00:00Z", "reports_ok": list(__import__('airs_vermogen').REPORTS)}],
        "airs_holding": [{"portefeuille": "BOOK", "as_of_date": "2026-10-28", "isin": "UNMAPPED"}],
        "airs_performance": [{"portefeuille": "BOOK", "periode": "2026-10-28", "cumulatief_rendement": 1}],
    })
    monkeypatch.setattr(D, "supabase", db)
    monkeypatch.setattr(links, "list_account_links", lambda: {"accounts": []})
    monkeypatch.setattr(prepared, "read", lambda *_: (1, None))
    result = D.freshness(date(2026, 10, 30))
    assert result["incomplete"] == 1
    assert result["accounts"][0]["unmapped_isins"] == ["UNMAPPED"]
    assert any("expected 2026-10-29" in issue for issue in result["accounts"][0]["issues"])
    assert db.writes == []


def test_missing_month_opening_is_saved_as_incomplete_without_fetching_airs(monkeypatch):
    from routers import _airs_monthly_report as monthly
    db = FakeSupabase({"airs_holding": [{"portefeuille": "BOOK", "as_of_date": "2026-10-29",
        "holding_name": "Stock", "isin": "TEST", "quantity": 1, "current_value_eur": 100}]})
    monkeypatch.setattr(D, "supabase", db)
    monkeypatch.setattr(monthly, "_monthly_rows", lambda *_: [{"month": "2026-10", "periode": "2026-10-29", "rendement": 1}])
    result = D.prepare_month("BOOK", date(2026, 10, 30))
    assert "opening" in result["errors"]
    assert "transactions" in result["errors"]
    assert db.tables["airs_monthly_report"][0]["report"]["sections"]["closing"]["status"] == "ok"


def test_watchdog_does_not_start_additional_airs_passes(monkeypatch):
    monkeypatch.setattr(S, "job_health", lambda: {"history_error": None, "rows": [
        {"id": "airs_vermogen_refresh", "status": "missed"}, {"id": "dashboard_post_airs", "status": "overdue"}]})
    monkeypatch.setattr(S, "start_job_now", lambda *_a, **_k: pytest.fail("extra AIRS pass"))
    assert S._body_job_watchdog()[1]["restarted"] == 0


def test_airs_lease_deduplicates_an_attempt_across_replicas_but_allows_manual_retry(monkeypatch):
    import common.pg as pg
    import psycopg
    monkeypatch.setattr(pg, "_db_url", lambda: "unit-test")
    commands = []
    class Connection:
        def execute(self, command):
            commands.append(command)
            return SimpleNamespace(fetchone=lambda: (True,))
    @contextmanager
    def connect(*args, **kwargs):
        yield Connection()
    monkeypatch.setattr(psycopg, "connect", connect)
    monkeypatch.setattr(D, "supabase", FakeSupabase({"scheduled_job_run": [{
        "id": 1, "job_id": "airs_vermogen_refresh", "triggered_by": "auto", "status": "error",
        "started_at": D.datetime.now(D.timezone.utc).isoformat(),
    }]}))
    with D.daily_lease("airs_vermogen_refresh", "auto") as run:
        assert not run
    with D.daily_lease("airs_vermogen_refresh", "manual") as run:
        assert run
        with D.daily_lease("dashboard_post_airs", "auto") as nested:
            assert nested
    assert len([command for command in commands if "pg_try_advisory_lock" in command]) == 2
    assert len([command for command in commands if "pg_advisory_unlock" in command]) == 2


def test_month_preparation_uses_saved_boundaries_and_explicitly_empty_transactions(monkeypatch):
    from routers import _airs_monthly_report as monthly
    db = FakeSupabase({
        "airs_holding": [
            {"portefeuille": "BOOK", "as_of_date": day, "holding_name": "Stock", "isin": "TEST",
             "quantity": 1, "current_value_eur": value}
            for day, value in [("2026-09-30", 100), ("2026-10-29", 110)]],
        "airs_transactie_snapshot": [{"portefeuille": "BOOK", "datum_van": "2026-01-01", "datum_tot": "2026-10-30",
                                      "columns": [], "kinds": {}, "rows": []}],
    })
    monkeypatch.setattr(D, "supabase", db)
    monkeypatch.setattr(monthly, "_monthly_rows", lambda *_: [{"month": "2026-10", "periode": "2026-10-29", "rendement": 10,
        "beginvermogen": 100, "eindvermogen": 110, "beleggingsresultaat": 10, "stortingen": 0, "onttrekkingen": 0,
        "opbrengsten": 0, "kosten": 0, "mutatie_opgelopen_rente": 0}])
    assert D.prepare_month("BOOK", date(2026, 10, 30))["errors"] == {}
    saved = db.tables["airs_monthly_report"][0]["report"]
    assert saved["positions"][0]["return_pct"] == pytest.approx(10)
    assert saved["reconciliation"]["status"] == "reconciled"
