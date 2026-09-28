"""Provider, workflow and outcome are dimensions, while GuruFocus caps stay regional."""
from __future__ import annotations

from ingest import api_usage


class _Result:
    def __init__(self, data=None):
        self.data = data


class _Rpc:
    def __init__(self, owner, name, payload):
        self.owner, self.name, self.payload = owner, name, payload

    def execute(self):
        self.owner.calls.append((self.name, self.payload))
        return _Result()


class _TrackingSupabase:
    def __init__(self):
        self.calls = []

    def rpc(self, name, payload):
        return _Rpc(self, name, payload)


class _UsageQuery:
    def __init__(self, rows):
        self.rows = rows

    def select(self, _columns):
        return self

    def eq(self, _column, _value):
        return self

    def execute(self):
        return _Result(self.rows)


class _UsageSupabase:
    def __init__(self, rows):
        self.rows = rows

    def table(self, name):
        assert name == "api_usage"
        return _UsageQuery(self.rows)


def test_the_atomic_increment_carries_every_dimension(monkeypatch):
    monkeypatch.setattr(api_usage, "_current_month_est", lambda: "2026-09")
    sb = _TrackingSupabase()

    with api_usage.api_usage_job("benchmark_refresh"):
        api_usage.track_api_call(
            sb, source="yahoo", region="global", outcome="success", count=3)

    assert sb.calls == [("increment_api_usage_attributed", {
        "p_month": "2026-09", "p_region": "global", "p_source": "yahoo",
        "p_job": "benchmark_refresh", "p_outcome": "success", "p_count": 3,
    })]


def test_yahoo_is_reported_but_does_not_consume_a_gurufocus_regional_cap(monkeypatch):
    monkeypatch.setattr(api_usage, "_current_month_est", lambda: "2026-09")
    usage = api_usage.get_usage(_UsageSupabase([
        {"region": "usa", "source": "gurufocus", "job": "price_gap",
         "outcome": "success", "request_count": 15711},
        {"region": "global", "source": "yahoo", "job": "benchmark_refresh",
         "outcome": "success", "request_count": 24000},
        {"region": "usa", "source": "gurufocus", "job": "price_gap",
         "outcome": "not_found", "request_count": 7},
    ]))

    assert usage["usa"] == 15718
    assert usage["europe"] == usage["asia"] == 0
    assert usage["total"] == 39718
    assert usage["by_source"] == {"gurufocus": 15718, "yahoo": 24000}
    assert usage["breakdown"][0]["job"] == "benchmark_refresh"


def test_outcomes_are_stable_low_cardinality_buckets():
    assert api_usage.classify_outcome(200, has_data=True) == "success"
    assert api_usage.classify_outcome(200, has_data=False) == "empty"
    assert api_usage.classify_outcome(404, has_data=False) == "not_found"
    assert api_usage.classify_outcome(429, has_data=False) == "rate_limited"
    assert api_usage.classify_outcome(None, has_data=False) == "transport_error"


def test_one_yahoo_http_attempt_reaches_the_shared_meter(monkeypatch):
    from asset_pipeline import yahoo

    class _Response:
        status_code = 200
        text = '{"chart": {"result": []}}'

    class _Requests:
        @staticmethod
        def get(_url, **_kwargs):
            return _Response()

    seen = []
    monkeypatch.setattr(yahoo, "_HAS_CURL", True)
    monkeypatch.setattr(yahoo, "_creq", _Requests())
    monkeypatch.setattr(yahoo, "_track_request",
                        lambda url, status, body="": seen.append((url, status, body)))

    url = "https://query1.finance.yahoo.com/v8/finance/chart/ASML.AS"
    assert yahoo._raw_get(url) == (200, _Response.text)
    assert seen == [(url, 200, _Response.text)]
