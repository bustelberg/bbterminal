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
        {"portefeuille": "BUS_Defensief_Dyn", "model_portfolio_id": 1}, {"portefeuille": "BUS_Neutraal_Dyn", "model_portfolio_id": 1},
        {"portefeuille": "BUS_Offensief_Dyn", "model_portfolio_id": 2}]})
    calls = []
    progress = []
    ctx = SimpleNamespace(cancelled=False, progress=lambda *args: progress.append(args[-1]))
    def price(pid, emit, *, stored_composition):
        assert stored_composition
        calls.append(pid)
        if pid == 2:
            raise RuntimeError("bad price")
        return {}
    monkeypatch.setattr(refresh, "refresh_portfolio", price)
    with pytest.raises(D.IncompleteRefresh) as caught:
        S._body_airs_model_prices(ctx)
    assert calls == [2, 1]  # Offensief before Neutraal, matching the dashboard refresh order.
    assert caught.value.summary["failed_names"] == ["BUS_Offensief_Dyn"]
    assert any("error: bad price" in message for message in progress)


def test_manual_model_refresh_reads_composition_when_previous_scan_is_old(monkeypatch):
    import jobs
    from routers import airs, _airs_account_links as links, _airs_portfolio_refresh as refresh

    monkeypatch.setattr(links, "list_account_links", lambda: {"accounts": [
        {"portefeuille": "BUS_Offensief_Dyn", "model_portfolio_id": 1}]})
    monkeypatch.setattr(refresh, "supabase", FakeSupabase({
        "airs_model_portfolio": [{"id": 1, "portfolio_type": "fixed (0)",
                                   "positions_scanned_at": "2000-01-01"}],
    }))
    reads = []
    def composition(pid, emit, wait):
        reads.append(pid)
        emit("phase", phase="composition", message="1/5 Composition — reading AirSPMS…")
        emit("progress", message="stored: model; composition dated 2000-01-01")
        return {"datum": "2026-01-01", "previous_datum": "2026-01-01",
                "rows": [{"isin": "TEST"}]}
    monkeypatch.setattr(refresh, "_composition", composition)
    monkeypatch.setattr(refresh, "_instruments", lambda *_: {})
    monkeypatch.setattr(refresh, "_fx", lambda *_: {"failed": 0, "backfilled": 0, "extended": 0})
    monkeypatch.setattr(refresh, "_prices", lambda *_: {"failed": 0, "fetched": 1, "already_current": 0})
    monkeypatch.setattr(refresh, "_recompute", lambda *_: {"portfolio": {"ytd_pct": 1}})
    messages, progress = [], []
    def start(kind, label, work):
        ctx = SimpleNamespace(cancelled=False, progress=lambda *args: progress.append(args[-1]))
        messages.append(work(ctx))
        return SimpleNamespace(id="manual-refresh"), False
    monkeypatch.setattr(jobs, "start", start)

    # This route only registers synchronous work and never awaits I/O.
    with pytest.raises(StopIteration) as completed:
        airs.airs_model_portfolios_refresh_job().send(None)
    result = completed.value.value

    assert result["job_id"] == "manual-refresh"
    assert reads == [1]
    assert messages == ["1/1 model portfolio(s) repriced"]
    assert "Reading this portfolio from AIRS…" in progress
    assert not any("composition dated" in message for message in progress)


def test_manual_airs_refresh_finishes_without_stock_price_work(monkeypatch):
    import jobs
    import airs_vermogen
    from routers import airs
    progress = []
    def refresh(**kwargs):
        assert kwargs['include_price_checks'] is False
        kwargs['on_step'](1, 22, '1/22: Bustelberg portfolios · Bustelberg Offensief')
        kwargs['on_step'](1, 22, 'technical report receipt')
        kwargs['on_account_done'](1, 22, 'BUS_Offensief_Dyn')
        return {'status': 'ok', 'complete_accounts': 1, 'portfolios_found': 1}
    monkeypatch.setattr(airs_vermogen, 'run_airs_vermogen_refresh_sync', refresh)
    def start(kind, label, work):
        work(SimpleNamespace(cancelled=False, progress=lambda *args, **kw: progress.append((args[-1], kw))))
        return SimpleNamespace(id='airs-only'), False
    monkeypatch.setattr(jobs, 'start', start)
    with pytest.raises(StopIteration):
        airs.airs_vermogen_refresh_job().send(None)
    assert len(progress) == 2
    assert progress[0][0].endswith('Reading from AIRS…')
    assert progress[1][1] == {'account': 'BUS_Offensief_Dyn', 'account_done': True}


def test_single_row_refresh_only_requests_airs_data(monkeypatch):
    import jobs
    from routers import airs, _airs_full_refresh as full
    calls = []
    def refresh(**kwargs):
        calls.append(kwargs)
        assert kwargs['halves'] == ('book',)
        assert kwargs['include_price_checks'] is False
        return {}
    monkeypatch.setattr(full, 'refresh_portfolio_fully', refresh)
    monkeypatch.setattr(full, 'job_verdict', lambda *_: 'Updated from AIRS')
    def start(kind, label, work):
        assert work(SimpleNamespace(cancelled=False, progress=lambda *_: None)) == 'Updated from AIRS'
        return SimpleNamespace(id='airs-row'), False
    monkeypatch.setattr(jobs, 'start', start)
    with pytest.raises(StopIteration):
        airs.airs_portfolio_refresh_job('BUS_Offensief_Dyn').send(None)
    assert len(calls) == 1


@pytest.mark.parametrize("kind", ["normaal", "meervoudig"])
def test_dynamic_link_does_not_require_a_fixed_composition(monkeypatch, kind):
    from routers import _airs_portfolio_refresh as refresh
    monkeypatch.setattr(refresh, "supabase", FakeSupabase({
        "airs_model_portfolio": [{"id": 1, "portfolio_type": kind}],
    }))
    monkeypatch.setattr(refresh, "_instruments", lambda *_: pytest.fail("no model to price"))
    result = refresh.refresh_portfolio(1, lambda *_a, **_k: None, stored_composition=True)
    assert result["status"] == "skipped"


@pytest.mark.parametrize("extra,expected", [
    ({"positions_scanned_at": date.today().isoformat()}, None),
    ({"positions_scanned_at": "2000-01-01"}, "not successfully scanned today"),
    ({"positions_scanned_at": date.today().isoformat(), "positions_error": "download failed"}, "download failed"),
    ({}, "no usable stored composition"),
])
def test_empty_fixed_composition_is_only_not_applicable_after_successful_scan(monkeypatch, extra, expected):
    from routers import _airs_portfolio_refresh as refresh
    monkeypatch.setattr(refresh, "supabase", FakeSupabase({
        "airs_model_portfolio": [{"id": 1, "portfolio_type": "fixed (model)", **extra}],
        "airs_model_portfolio_position": [],
    }))
    if expected:
        with pytest.raises(RuntimeError, match=expected):
            refresh.refresh_portfolio(1, lambda *_a, **_k: None, stored_composition=True)
    else:
        assert refresh.refresh_portfolio(1, lambda *_a, **_k: None, stored_composition=True)["status"] == "skipped"


def test_pricing_receipt_distinguishes_priced_and_not_applicable(monkeypatch):
    from routers import _airs_account_links as links, _airs_portfolio_refresh as refresh
    monkeypatch.setattr(links, "list_account_links", lambda: {"accounts": [
        {"portefeuille": "BUS_Defensief_Dyn", "model_portfolio_id": 1},
        {"portefeuille": "BUS_Neutraal_Dyn", "model_portfolio_id": 2}]})
    monkeypatch.setattr(refresh, "refresh_portfolio", lambda pid, *a, **kw:
                        {"status": "skipped", "note": "no Fixed composition"} if pid == 1 else {})
    _, receipt = S._body_airs_model_prices()
    assert (receipt["priced"], receipt["skipped"], receipt["failed"]) == (1, 1, 0)
    by_id = {r['model_portfolio_id']: r for r in receipt['results']}
    assert by_id[1]["model_status"] == "skipped"


def test_post_airs_keeps_nested_audit_receipt_and_logs_root_cause(monkeypatch, caplog):
    import airs_history
    from routers import _airs_accounts, _airs_price_check, _prepared_analyse
    monkeypatch.setattr(_airs_accounts, "list_accounts", lambda: [])
    monkeypatch.setattr(airs_history, "finalise_previous_year", lambda ctx, **kw: [])
    monkeypatch.setattr(_airs_price_check, "refresh_price_checks", lambda *a, **kw: {})
    monkeypatch.setattr(_prepared_analyse, "run_controlled", lambda **kw: {})
    receipt = {"accounts": [{"portefeuille": "BOOK", "issues": ["missing close"]}]}
    def audit(*a):
        raise D.IncompleteRefresh("incomplete audit", receipt)
    monkeypatch.setattr(S, "_run_body", audit)
    with pytest.raises(D.IncompleteRefresh) as caught:
        S._body_dashboard_post_airs()
    assert caught.value.summary["errors"]["audit"]["receipt"] == receipt
    assert "missing close" in caplog.text


def test_post_airs_downloads_month_when_daily_snapshots_are_incomplete(monkeypatch):
    import airs_history
    from routers import _airs_accounts, _airs_monthly_report, _airs_price_check, _monthly_benchmark, _prepared_analyse

    account = "BUS_Neutraal_Dyn"
    monkeypatch.setattr(_airs_accounts, "list_accounts", lambda: [{"portefeuille": account, "holdings": 1}])
    monkeypatch.setattr(_monthly_benchmark, "ensure_daily", lambda *_: {"errors": {}})
    monkeypatch.setattr(airs_history, "finalise_previous_year", lambda *_a, **_kw: [])
    monkeypatch.setattr(_airs_price_check, "refresh_price_checks", lambda *_a, **_kw: {})
    monkeypatch.setattr(_prepared_analyse, "run_controlled", lambda **_kw: {})
    monkeypatch.setattr(S, "_run_body", lambda *_: None)
    monkeypatch.setattr(D, "prepare_month", lambda name: {
        "account": name, "end": "2026-11-02", "errors": {"opening": "No stored valuation"},
    })
    downloads = []
    def collect(name, month, ctx):
        ctx.check()
        downloads.append((name, month))
        return "Performance updated."
    monkeypatch.setattr(_airs_monthly_report, "collect_report", collect)
    _, receipt = S._body_dashboard_post_airs()
    assert downloads == [(account, "2026-11")]
    assert receipt["monthly"][0]["errors"] == {}


@pytest.mark.parametrize("kind,needs_scan", [("normaal", False), ("meervoudig", False), ("fixed (model)", True), (None, True)])
def test_freshness_requires_model_scan_only_for_fixed_links(monkeypatch, kind, needs_scan):
    from routers import _airs_account_links as links, _prepared_analyse as prepared
    monkeypatch.setattr(D, "supabase", FakeSupabase({
        "airs_account_roster": [{"portefeuille": "BOOK", "last_seen_at": "2026-10-30T10:00:00Z"}],
        "airs_model_portfolio": [{"id": 1, "portfolio_type": kind}],
    }))
    monkeypatch.setattr(links, "list_account_links", lambda: {"accounts": [
        {"portefeuille": "BOOK", "model_portfolio_id": 1}]})
    monkeypatch.setattr(prepared, "read", lambda *_: (1, {}))
    result = D.freshness(date(2026, 10, 30))
    issues = result["accounts"][0]["issues"]
    assert ("model composition not successfully scanned today" in issues) == needs_scan


def test_daily_parent_retains_both_failed_child_receipts(monkeypatch):
    monkeypatch.setattr('management_portfolios.dashboard_model_rows', lambda rows: rows)
    import airs_vermogen
    import airs_scanner
    from routers import _airs_portfolio_store as store
    monkeypatch.setattr(airs_vermogen, "run_airs_vermogen_refresh_sync", lambda **kw: {"status": "ok", "complete_accounts": 1})
    monkeypatch.setattr(airs_vermogen, "_acquire_session", lambda *_: True)
    monkeypatch.setattr(airs_vermogen, "_LOCK", SimpleNamespace(release=lambda: None))
    monkeypatch.setattr(airs_scanner, "fetch_model_portfolios_sync", lambda *_: [{"id": 1}])
    monkeypatch.setattr(airs_scanner, "count_model_portfolio_holdings_sync", lambda *a, **kw: None)
    monkeypatch.setattr(store, "save_portfolios", lambda *_: None)
    pricing = {"results": [{"portefeuille": "BOOK", "error": "missing FX"}]}
    post = {"errors": {"BOOK": {"opening": "missing valuation"}}}
    def price(*a):
        raise D.IncompleteRefresh("pricing incomplete", pricing)
    def prepare(*a):
        raise D.IncompleteRefresh("post incomplete", post)
    monkeypatch.setattr(S, "_body_airs_model_prices", price)
    monkeypatch.setattr(S, "_run_body", prepare)
    with pytest.raises(D.IncompleteRefresh) as caught:
        S._body_airs_vermogen()
    assert caught.value.summary["pricing"] == pricing
    assert caught.value.summary["post_airs"] == post
    assert len(caught.value.summary["errors"]) == 2


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


@pytest.mark.parametrize('legacy_opening,correction', [(False, 0), (True, 0), (True, 1)])
def test_daily_month_report_keeps_accrued_interest_out_of_security_price_return(monkeypatch, legacy_opening, correction):
    from routers import _airs_monthly_report as monthly
    holdings = []
    for day, principal, interest in [('2026-09-30', 1000, 50), ('2026-10-29', 1100, 60)]:
        common = dict(portefeuille='BOOK', as_of_date=day, holding_name='Rabo',
                      isin='XS1002121454', currency='EUR')
        holdings.extend([{**common, 'quantity': 1000, 'current_value_eur': principal},
                         {**common, 'quantity': 1, 'current_value_eur': interest,
                          'valuation_component': 'accrued_interest'}])
    db = FakeSupabase({'airs_holding': holdings, 'airs_transactie_snapshot': [{
        'portefeuille': 'BOOK', 'datum_van': '2026-01-01', 'datum_tot': '2026-10-30',
        'columns': [], 'kinds': {}, 'rows': []}]})
    if legacy_opening:
        opening = [{'name': r['holding_name'], 'isin': r['isin'], 'currency': r['currency'],
                    'quantity': r['quantity'], 'value_eur': r['current_value_eur'],
                    'valuation_component': r.get('valuation_component')} for r in holdings[:2]]
        db.tables['airs_monthly_report'] = [dict(portefeuille='BOOK', month='2026-10-01', report={
            'sections': {'opening': {'status': 'ok', 'data': {'as_of': '2026-09-30', 'rows': opening}}}})]
        for r in db.tables['airs_holding'][:2]:
            r.pop('valuation_component', None)
            r['current_price_local'] = 101
        db.tables['airs_holding'][0]['current_value_eur'] += correction
    monkeypatch.setattr(D, 'supabase', db)
    monkeypatch.setattr(monthly, '_monthly_rows', lambda *_: [dict(month='2026-10', periode='2026-10-29', rendement=10)])
    D.prepare_month('BOOK', date(2026, 10, 30))
    saved = db.tables['airs_monthly_report'][0]['report']
    if correction:
        assert saved['positions'][0]['return_pct'] is None
    else:
        assert saved['positions'][0]['return_pct'] == pytest.approx(10)
        assert saved['positions'][0]['accrued_interest_closing_eur'] == 60
