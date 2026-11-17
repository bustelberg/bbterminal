"""Valuation refresh narrows database work without narrowing fresh source payloads."""
from importlib import import_module
from types import SimpleNamespace

import pytest

from ingest.earnings.financials import _parse_financials
from services import portfolio_valuation as V
from services.valuation_workbook import FORECAST_CODES


def test_financial_narrowing_preserves_eps_but_omits_unrelated_statement_metrics():
    data = {'financials': {'quarterly': {'Fiscal Year': ['2026-03', '2026-06'],
        'Per Share Data': {'EPS without NRI': [1, 2], 'Revenue per Share': [10, 20]},
        'Income Statement': {'Revenue': [100, 200]}}}}
    all_rows = _parse_financials(data, 1)
    narrow = _parse_financials(data, 1, metric_codes=set(V.EPS_CODES))
    assert len(narrow) == 2
    assert narrow == [row for row in all_rows if row['metric_code'] in V.EPS_CODES]
    assert len(all_rows) > len(narrow)


@pytest.mark.parametrize('narrow', [True, False])
def test_estimate_refresh_keeps_full_raw_payload_but_only_loads_requested_metrics(monkeypatch, narrow):
    estimates = import_module('ingest.earnings.analyst_estimates')
    history = import_module('ingest.earnings.estimate_history')
    data = {'annual': {'date': ['2027-12', '2028-12'], 'eps_nri_estimate': [10, 12],
                      'per_share_eps_estimate': [11, 13], 'revenue_estimate': [100, 120]},
            'quarterly': {'date': ['2027-03'], 'eps_nri_estimate': [2]}}
    uploaded, written, history_calls, requests = [], [], [], []
    monkeypatch.setattr(estimates, 'refuse_unsubscribed', lambda *_: None)
    monkeypatch.setattr(estimates, '_ensure_bucket', lambda *_: None)
    monkeypatch.setattr(estimates, '_build_api_url', lambda *_: 'fixture')
    monkeypatch.setattr(estimates, '_api_request', lambda url: requests.append(url) or SimpleNamespace(
        data=data, is_forbidden=False, status_code=200, log='Fresh fixture'))
    monkeypatch.setattr(estimates, 'track_api_call', lambda *_args, **_kwargs: None)
    monkeypatch.setattr(estimates, '_upload_to_storage', lambda _db, _path, payload: uploaded.append(payload))
    monkeypatch.setattr(estimates, '_fetch_from_storage', lambda *_: pytest.fail('Force must fetch the provider'))
    monkeypatch.setattr(estimates, '_stamp_fetched', lambda *_: None)
    monkeypatch.setattr(estimates, '_upsert_metric_rows', lambda _db, rows: written.extend(rows) or (len(rows), 0))
    monkeypatch.setattr(history, 'fetch_estimate_history', lambda *_args, **_kwargs: history_calls.append(True)
        or SimpleNamespace(api_calls=1, rows_loaded=0, rows_unchanged=0, metrics_found=0, error=None))
    options = {'metric_codes': set(FORECAST_CODES), 'include_history': False} if narrow else {}
    result = estimates.fetch_analyst_estimates(object(), 1, 'ABC', 'NAS', force_refresh=True, **options)
    all_rows = estimates._parse_analyst_estimates(data, 1)
    assert written == ([row for row in all_rows if row['metric_code'] in FORECAST_CODES] if narrow else all_rows)
    assert uploaded == [data]  # Full fresh facts stay available for other screens.
    assert requests == ['fixture']
    assert len(history_calls) == (0 if narrow else 1)
    assert result.cache_status == 'api_fresh'


@pytest.mark.parametrize('status', ['ingested', 'no_data'])
def test_new_mapping_fetches_scoped_financials_once_before_parallel_market_feeds(monkeypatch, status):
    from routers import _fundamental_ingest as ingest
    from asset_pipeline import company_prices
    from ingest import earnings
    from tests._fake_supabase import FakeSupabase
    database = FakeSupabase({'company': []})
    monkeypatch.setattr(V, 'supabase', database)
    def resolve(isin, name, *, force, metric_codes):
        assert isin == 'TW0002330008'
        assert force is True and metric_codes == set(V.EPS_CODES)
        database.tables['company'] = [{'company_id': 3223, 'isin': isin,
            'gurufocus_ticker': '2330', 'gurufocus_exchange': {'exchange_code': 'TPE'}}]
        return {'status': status}
    monkeypatch.setattr(ingest, 'ingest_fundamentals_for_isin', resolve)
    monkeypatch.setattr(earnings, 'fetch_financials', lambda *_args, **_kwargs: pytest.fail('Resolution already fetched financials'))
    monkeypatch.setattr(company_prices, 'refresh_company_prices', lambda *_args, **_kwargs: SimpleNamespace(error=None))
    monkeypatch.setattr(earnings, 'fetch_analyst_estimates', lambda *_args, **_kwargs: SimpleNamespace(
        error=None, is_forbidden=False, rows_loaded=4, rows_unchanged=0, metrics_found=2))
    V.refresh_row_sources('US8740391003', 'TSMC')



def test_empty_quarterly_eps_does_not_block_current_quotes_and_forecasts(monkeypatch):
    from asset_pipeline import company_prices
    from ingest import earnings
    from tests._fake_supabase import FakeSupabase
    monkeypatch.setattr(V, 'supabase', FakeSupabase({'company': [{'company_id': 1, 'isin': 'EXACT',
        'gurufocus_ticker': 'ABC', 'gurufocus_exchange': {'exchange_code': 'NAS'}}]}))
    monkeypatch.setattr(company_prices, 'refresh_company_prices', lambda *_args, **_kwargs: SimpleNamespace(error=None))
    monkeypatch.setattr(earnings, 'fetch_analyst_estimates', lambda *_args, **_kwargs: SimpleNamespace(
        error=None, is_forbidden=False, rows_loaded=4, rows_unchanged=0, metrics_found=2))
    monkeypatch.setattr(earnings, 'fetch_financials', lambda *_args, **_kwargs: SimpleNamespace(
        error=None, is_forbidden=False, rows_loaded=0, rows_unchanged=0, metrics_found=0))
    V.refresh_row_sources('EXACT', 'Company')


@pytest.mark.parametrize('status, code, detail', [
    ('not_found', 422, 'could not resolve this ISIN'),
    ('unsubscribed', 422, 'outside the current subscription'),
    ('error', 502, 'request failed'),
])
def test_resolution_failure_keeps_the_actual_failure_category(monkeypatch, status, code, detail):
    from routers import _fundamental_ingest
    from tests._fake_supabase import FakeSupabase
    from fastapi import HTTPException
    monkeypatch.setattr(V, 'supabase', FakeSupabase({'company': []}))
    monkeypatch.setattr(_fundamental_ingest, 'ingest_fundamentals_for_isin', lambda *_args, **_kwargs: {'status': status})
    with pytest.raises(HTTPException) as exc:
        V.refresh_row_sources('EXACT', 'Company')
    assert exc.value.status_code == code
    assert detail in exc.value.detail


def test_empty_financial_response_reason_survives_the_mapping_ingest_boundary(monkeypatch):
    from routers import _fundamental_ingest
    from tests._fake_supabase import FakeSupabase
    from fastapi import HTTPException
    monkeypatch.setattr(V, 'supabase', FakeSupabase({'company': []}))
    reason = 'GuruFocus returned an empty financial statements response (HTTP 200): no usable reporting periods. Retried once with the same result. Previous saved data was kept.'
    monkeypatch.setattr(_fundamental_ingest, 'ingest_fundamentals_for_isin', lambda *_args, **_kwargs: {
        'status': 'error', 'failure_reason': reason})
    with pytest.raises(HTTPException) as exc:
        V.refresh_row_sources('NL0012866412', 'BE Semiconductor')
    assert exc.value.status_code == 502
    assert reason in exc.value.detail
    assert '(error)' not in exc.value.detail
