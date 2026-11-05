from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock

import pytest
import requests

import fx_rates as fx


class Clock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


def response(status, retry_after=None):
    result = requests.Response()
    result.status_code = status
    result.url = 'https://data-api.ecb.europa.eu/test'
    if retry_after:
        result.headers['Retry-After'] = retry_after
    return result


def test_concurrent_currencies_share_three_outage_attempts_not_three_each(monkeypatch):
    clock = Clock()
    gate = fx._EcbRetryGate(clock=clock, sleep=clock.sleep)
    get = Mock(return_value=response(504))
    monkeypatch.setattr(fx.requests, 'get', get)

    def fetch(code):
        with pytest.raises(requests.RequestException):
            gate.get(f'https://data-api.ecb.europa.eu/{code}', timeout=90, max_attempts=3)

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(fetch, ['CAD', 'ILS', 'INR', 'MYR', 'PHP', 'PLN', 'RON', 'THB']))
    assert get.call_count == 3
    assert clock.sleeps == [2, 4]
    assert gate._failures == 3
    assert gate._resume_at == 14


def test_failure_count_and_backoff_are_shared_even_across_separate_calls(monkeypatch):
    clock = Clock()
    gate = fx._EcbRetryGate(clock=clock, sleep=clock.sleep)
    get = Mock(return_value=response(504))
    monkeypatch.setattr(fx.requests, 'get', get)
    for code in ['CAD', 'ILS', 'INR']:
        with pytest.raises(requests.HTTPError):
            gate.get(code, timeout=90, max_attempts=1)
    with pytest.raises(fx.ECBTemporarilyUnavailable, match='shared cooldown'):
        gate.get('MYR', timeout=90, max_attempts=3)
    assert get.call_count == 3
    assert clock.sleeps == [2, 4]


def test_successful_recovery_resets_shared_failure_count(monkeypatch):
    clock = Clock()
    gate = fx._EcbRetryGate(clock=clock, sleep=clock.sleep)
    get = Mock(side_effect=[response(504), response(504), response(504), response(200), response(504), response(200)])
    monkeypatch.setattr(fx.requests, 'get', get)
    with pytest.raises(requests.HTTPError):
        gate.get('CAD', timeout=90, max_attempts=3)
    clock.now = gate._resume_at
    assert gate.get('ILS', timeout=90, max_attempts=3).status_code == 200
    assert gate._failures == 0
    assert gate.get('INR', timeout=90, max_attempts=3).status_code == 200
    assert clock.sleeps == [2, 4, 2]


def test_continuing_outage_uses_one_probe_and_caps_exponential_delay(monkeypatch):
    clock = Clock()
    gate = fx._EcbRetryGate(clock=clock, sleep=clock.sleep)
    get = Mock(return_value=response(504))
    monkeypatch.setattr(fx.requests, 'get', get)
    with pytest.raises(requests.HTTPError):
        gate.get('CAD', timeout=90, max_attempts=3)
    for expected_delay in (16, 32, 60, 60):
        clock.now = gate._resume_at
        before = get.call_count
        with pytest.raises(requests.HTTPError):
            gate.get('ILS', timeout=90, max_attempts=3)
        assert get.call_count == before + 1
        assert gate._resume_at - clock.now == expected_delay


@pytest.mark.parametrize('status', [400, 404])
def test_client_errors_are_not_retried_or_counted_as_service_outages(monkeypatch, status):
    clock = Clock()
    gate = fx._EcbRetryGate(clock=clock, sleep=clock.sleep)
    get = Mock(return_value=response(status))
    monkeypatch.setattr(fx.requests, 'get', get)
    with pytest.raises(requests.HTTPError):
        gate.get('CAD', timeout=90, max_attempts=3)
    get.assert_called_once()
    assert gate._failures == 0
    assert clock.sleeps == []


def test_server_retry_after_is_respected(monkeypatch):
    clock = Clock()
    gate = fx._EcbRetryGate(clock=clock, sleep=clock.sleep)
    monkeypatch.setattr(fx.requests, 'get', Mock(side_effect=[response(503, '12'), response(200)]))
    assert gate.get('CAD', timeout=90, max_attempts=3).status_code == 200
    assert clock.sleeps == [12]


@pytest.mark.parametrize('failure', [requests.Timeout('slow'), requests.ConnectionError('connection reset')])
def test_network_failures_use_the_same_gate(monkeypatch, failure):
    clock = Clock()
    gate = fx._EcbRetryGate(clock=clock, sleep=clock.sleep)
    monkeypatch.setattr(fx.requests, 'get', Mock(side_effect=[failure, response(200)]))
    assert gate.get('CAD', timeout=90, max_attempts=3).status_code == 200
    assert clock.sleeps == [2]
