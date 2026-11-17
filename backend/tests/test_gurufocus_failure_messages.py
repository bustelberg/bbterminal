from types import SimpleNamespace
from importlib import import_module

import pytest
from ingest.earnings._common import failure_message


@pytest.mark.parametrize('status, cache, forbidden, phrase', [
    (200, 'api_empty', False, 'empty financial statements response (HTTP 200)'),
    (429, 'api_error', False, 'rate limit'),
    (401, 'api_error', False, 'API credentials'),
    (403, 'forbidden', True, 'denied access'),
    (404, 'api_error', False, 'ticker and exchange'),
    (503, 'api_error', False, 'server error'),
    (None, 'api_error', False, 'connection or timeout'),
])
def test_provider_failures_have_specific_safe_messages(status, cache, forbidden, phrase):
    result = SimpleNamespace(http_status=status, cache_status=cache, is_forbidden=forbidden,
        api_calls=2 if cache == 'api_empty' else 1, error='https://api/SECRET_API_KEY/financials')
    message = failure_message(result)
    assert phrase in message
    assert 'SECRET_API_KEY' not in message
    assert 'Previous saved data was kept' in message
    if cache == 'api_empty':
        assert 'Retried once' in message


def test_empty_forecast_template_does_not_replace_cache_or_mark_fresh(monkeypatch):
    estimates = import_module('ingest.earnings.analyst_estimates')
    monkeypatch.setattr(estimates, 'refuse_unsubscribed', lambda *_: None)
    monkeypatch.setattr(estimates, '_ensure_bucket', lambda *_: None)
    monkeypatch.setattr(estimates, '_api_request', lambda *_: SimpleNamespace(
        data={'annual': {'date': [], 'eps_nri_estimate': []}}, log='OK', status_code=200, is_forbidden=False))
    monkeypatch.setattr(estimates, 'track_api_call', lambda *_args, **_kwargs: None)
    monkeypatch.setattr(estimates, '_upload_to_storage', lambda *_: pytest.fail('Must retain previous cache'))
    monkeypatch.setattr(estimates, '_stamp_fetched', lambda *_: pytest.fail('Must not stamp empty response as fresh'))
    result = estimates.fetch_analyst_estimates(None, 1, 'BESI', 'XAMS', force_refresh=True, include_history=False)
    assert result.cache_status == 'api_empty'
    assert 'empty annual EPS forecasts response' in failure_message(result, 'annual EPS forecasts')
