"""A feed is stamped only when the vendor actually ANSWERED.

⚠⚠ THIS BECAME LOAD-BEARING THE MOMENT THE STAMP STARTED SUPPRESSING CALLS. `company.<feed>_fetched_at`
now silences a feed for `SMART_RETRY_EMPTY_AFTER_DAYS` (30) when nothing came back — which is the whole
saving. The other edge of that: a stamp written after a FAILED call records "we asked, there is
nothing" as a fact, and the feed goes quiet for a month over a transient vendor problem. The gap is
silent and looks exactly like real coverage.

⚠ AND THE THROTTLE SIGNATURE HERE IS AN EMPTY BODY, NOT A 429 — `_api_request_cf` turns one into
`data=None`. Zero rows from a throttled call and zero rows from a company GuruFocus genuinely has no
forward P/E for are indistinguishable by CONTENT, so they can only be told apart by whether the
REQUEST succeeded. This matters more the faster we ask: the whole reason to lower
`GURUFOCUS_MIN_INTERVAL_SECONDS` is throughput, and the failure that buys is precisely an empty body.

`financials` and `analyst_estimates` return early on that condition and never reach their stamp.
`indicators` loops over its keys and `continue`s, so it fell through and stamped anyway.
"""
from __future__ import annotations

import ingest.earnings.indicators as ind


class _Api:
    def __init__(self, data, forbidden=False, log="ok"):
        self.data = data
        self.is_forbidden = forbidden
        self.log = log
        self.status_code = 200


def _rig(monkeypatch, api_result):
    """Run `fetch_indicators` against fakes, returning what it stamped."""
    stamped: list[str] = []
    monkeypatch.setattr(ind, "_ensure_bucket", lambda _sb: None)
    monkeypatch.setattr(ind, "_fetch_from_storage", lambda _sb, _p: None)
    monkeypatch.setattr(ind, "_upload_to_storage", lambda _sb, _p, _d: None)
    monkeypatch.setattr(ind, "track_api_call", lambda _sb, _e: None)
    monkeypatch.setattr(ind, "_api_request", lambda _url: api_result)
    monkeypatch.setattr(ind, "_upsert_metric_rows", lambda _sb, rows: (len(rows), 0))
    monkeypatch.setattr(ind, "_stamp_fetched",
                        lambda _sb, _cid, source, _log: stamped.append(source))
    # ⚠ IT PARSES THE PAYLOAD, RATHER THAN JUST CHECKING IT IS TRUTHY. `{"indicator": []}` — a
    # well-formed response carrying no series, which is the case this file exists to separate from a
    # failed request — is itself truthy, so a `not data` fake handed back a row for it and the test
    # asserting "nothing was loaded" failed against correct code.
    monkeypatch.setattr(ind, "_parse_single_indicator",
                        lambda data, key, cid: ([{"company_id": cid, "metric_code": key,
                                                  "source_code": "gurufocus",
                                                  "target_date": "2026-06-30",
                                                  "numeric_value": 1.0,
                                                  "is_prediction": False}]
                                                if (data or {}).get("indicator") else []))
    res = ind.fetch_indicators(object(), 7, "AAPL", "NASDAQ", force_refresh=True)
    return res, stamped


class TestAnEmptyANSWERIsStamped:
    """The saving depends on this: a company GuruFocus has no forward P/E for must be recorded, or
    it is re-asked on every press for ever — 1,267 of ACWI's 1,551 indicator calls."""

    def test_a_well_formed_response_with_no_series_still_stamps(self, monkeypatch):
        # `data` present (the request succeeded) but the parser finds nothing in it.
        res, stamped = _rig(monkeypatch, _Api({"indicator": []}))
        assert stamped == ["indicators"]
        assert res.rows_loaded == 0


class TestAFAILEDCallIsNotStamped:
    """⚠ THE ONE THAT MATTERS. A transient empty body must not buy 30 days of silence."""

    def test_an_empty_body_does_not_stamp(self, monkeypatch):
        _res, stamped = _rig(monkeypatch, _Api(None, log="GuruFocus returned empty body"))
        assert stamped == [], (
            "an empty body was recorded as 'asked, nothing there' — the feed now goes quiet for "
            "SMART_RETRY_EMPTY_AFTER_DAYS over a vendor hiccup")

    def test_a_non_json_body_does_not_stamp(self, monkeypatch):
        _res, stamped = _rig(monkeypatch, _Api(None, log="non-JSON content"))
        assert stamped == []

    def test_an_unsubscribed_exchange_does_not_stamp(self, monkeypatch):
        """`eligible()` should refuse these before a call is spent; if one slips through, the answer
        is about the SUBSCRIPTION and does not belong in a per-company freshness stamp."""
        res, stamped = _rig(monkeypatch, _Api(None, forbidden=True))
        assert stamped == []
        assert res.is_forbidden


class TestTheOtherTwoFeedsAlreadyHadThisProperty:
    """Pinned so a refactor that unifies the three cannot quietly drop it — the early return IS the
    guard there, and it does not look like one."""

    def test_financials_returns_before_its_stamp_on_a_dead_call(self, monkeypatch):
        import ingest.earnings.financials as fin

        stamped: list[str] = []
        monkeypatch.setattr(fin, "_ensure_bucket", lambda _sb: None)
        monkeypatch.setattr(fin, "_fetch_from_storage", lambda _sb, _p: None)
        monkeypatch.setattr(fin, "track_api_call", lambda _sb, _e: None)
        monkeypatch.setattr(fin, "_api_request", lambda _url: _Api(None, log="empty body"))
        monkeypatch.setattr(fin, "_stamp_fetched",
                            lambda _sb, _cid, source, _log: stamped.append(source))
        res = fin.fetch_financials(object(), 7, "AAPL", "NASDAQ", force_refresh=True)
        assert stamped == [] and res.error

    def test_estimates_returns_before_its_stamp_on_a_dead_call(self, monkeypatch):
        import ingest.earnings.analyst_estimates as est

        stamped: list[str] = []
        monkeypatch.setattr(est, "_ensure_bucket", lambda _sb: None)
        monkeypatch.setattr(est, "_fetch_from_storage", lambda _sb, _p: None)
        monkeypatch.setattr(est, "track_api_call", lambda _sb, _e: None)
        monkeypatch.setattr(est, "_api_request", lambda _url: _Api(None, log="empty body"))
        monkeypatch.setattr(est, "_stamp_fetched",
                            lambda _sb, _cid, source, _log: stamped.append(source))
        res = est.fetch_analyst_estimates(object(), 7, "AAPL", "NASDAQ", force_refresh=True)
        assert stamped == [] and res.error
