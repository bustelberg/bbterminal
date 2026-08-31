"""A 200 carrying NO PERIODS is an outage, not an answer.

⚠⚠ MEASURED 2026-08-31, LIVE. GuruFocus's `financials` endpoint returned the full 15.7 KB template
for EVERY symbol — every section and every key present, every array empty — for AAPL and ASML as
much as for the two ACWI constituents under investigation. `summary`, `keyratios` and `price` were
healthy at the same moment and the monthly quota was barely half spent, so nothing upstream said
anything was wrong.

⚠ THE PARSER WAS ALREADY SAFE: no `Fiscal Year` means no periods means no rows, so this never wrote
zeros into `metric_data`. The damage was one step earlier — the empty payload replacing a good
cached raw JSON, and `financials_fetched_at` stamped as though the company had been refreshed. A
company that then looks current, reads as having no financials, and has lost the copy that proved
otherwise.

Unit-only: the Supabase client is a fake and the API call is monkeypatched.
"""
from __future__ import annotations

import pytest

#: The shape the vendor actually returned — keys all present, every array empty.
EMPTY = {"financials": {
    "financial_template_parameters": {"ind_template": "N"},
    "annuals": {"Fiscal Year": [], "Per Share Data": {"EPS without NRI": []},
                "Income Statement": {"Revenue": []}},
    "quarterly": {"Fiscal Quarter": [], "Income Statement": {"Revenue": []}},
}}

GOOD = {"financials": {"annuals": {
    "Fiscal Year": ["2024-12", "2025-12"],
    "Income Statement": {"Revenue": [100.0, 110.0]},
}}}


@pytest.fixture
def fin(monkeypatch):
    from ingest.earnings import financials as f

    monkeypatch.setattr(f, "_ensure_bucket", lambda *_a, **_k: None)
    monkeypatch.setattr(f, "_fetch_from_storage", lambda *_a, **_k: None)
    monkeypatch.setattr(f, "track_api_call", lambda *_a, **_k: None)
    monkeypatch.setattr(f, "_upsert_metric_rows", lambda *_a, **_k: (0, 0))
    return f


def _run(f, monkeypatch, payload):
    """`fetch_financials` with the API answering `payload`. Returns (result, what it wrote)."""
    class _Api:
        data = payload
        log = "OK"
        is_forbidden = False

    wrote: dict = {"uploaded": False, "stamped": False}
    monkeypatch.setattr(f, "_api_request", lambda *_a, **_k: _Api())
    monkeypatch.setattr(f, "_upload_to_storage",
                        lambda *_a, **_k: wrote.__setitem__("uploaded", True))
    monkeypatch.setattr(f, "_stamp_fetched", lambda *_a, **_k: wrote.__setitem__("stamped", True))
    res = f.fetch_financials(None, company_id=1, ticker="AAPL", exchange="NAS")
    return res, wrote


class TestAnEmptyTemplateIsRefused:
    def test_it_does_not_overwrite_the_cache(self, fin, monkeypatch):
        res, wrote = _run(fin, monkeypatch, EMPTY)
        assert wrote["uploaded"] is False
        assert res.cache_status == "api_empty"

    def test_it_does_not_stamp_the_company_as_freshly_fetched(self, fin, monkeypatch):
        """⚠ THE HALF THAT OUTLIVES THE OUTAGE. A stamp says "we asked, this is what there is" —
        so a company whose data the vendor lost for an afternoon would look deliberately empty."""
        _res, wrote = _run(fin, monkeypatch, EMPTY)
        assert wrote["stamped"] is False

    def test_it_says_why(self, fin, monkeypatch):
        res, _wrote = _run(fin, monkeypatch, EMPTY)
        assert "no periods" in (res.error or "")

    def test_a_payload_WITH_periods_is_cached_and_stamped_as_before(self, fin, monkeypatch):
        # ⚠ THE CONTROL. The guard must key on "carries a period", not on size or key count —
        # the empty template passes both of those comfortably (15.7 KB, 263 leaf keys).
        res, wrote = _run(fin, monkeypatch, GOOD)
        assert (wrote["uploaded"], wrote["stamped"]) == (True, True)
        assert res.cache_status == "api_fresh"


class TestTheFillStopsInsteadOfWalkingTheWholeIndex:
    """⚠⚠ THE GUARD ABOVE PROTECTS THE CACHE; IT DOES NOT PROTECT THE BUDGET. A quarterly benchmark
    fill would still visit all 2,526 constituents to collect 2,526 identical refusals — a call each,
    roughly half a region's monthly quota, spent to learn one fact that the tenth company already
    proved."""

    def test_the_marker_the_breaker_matches_is_the_one_the_fetcher_emits(self):
        """⚠ THE SEAM, ASSERTED FROM BOTH ENDS. The breaker matches on a substring of the error
        because threading a status code through `ingest_company` would teach three layers about a
        vendor state they have no other reason to know. That is only safe while the two strings
        cannot drift apart, which is what this pins."""
        from routers._fundamental_fill import VENDOR_EMPTY_LIMIT, VENDOR_EMPTY_MARKER

        from ingest.earnings import financials as f

        class _Api:
            data = EMPTY
            log = "OK"
            is_forbidden = False

        import pytest as _pytest
        mp = _pytest.MonkeyPatch()
        try:
            mp.setattr(f, "_ensure_bucket", lambda *_a, **_k: None)
            mp.setattr(f, "_fetch_from_storage", lambda *_a, **_k: None)
            mp.setattr(f, "track_api_call", lambda *_a, **_k: None)
            mp.setattr(f, "_upsert_metric_rows", lambda *_a, **_k: (0, 0))
            mp.setattr(f, "_api_request", lambda *_a, **_k: _Api())
            mp.setattr(f, "_upload_to_storage", lambda *_a, **_k: None)
            mp.setattr(f, "_stamp_fetched", lambda *_a, **_k: None)
            res = f.fetch_financials(None, company_id=1, ticker="AAPL", exchange="NAS")
        finally:
            mp.undo()

        assert VENDOR_EMPTY_MARKER in (res.error or "")
        # ⚠ AND THE LIMIT IS A HANDFUL, NOT A HUNDRED: a few genuinely empty companies are ordinary,
        # ten in a row across regions are not.
        assert 3 <= VENDOR_EMPTY_LIMIT <= 25
