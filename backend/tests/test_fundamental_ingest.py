"""Ingesting the fundamentals a coverage row is missing.

The orchestration (resolve → create company → fetch) is DB/API-bound; what is pure — and what a
wrong answer here would present to a reader as a plausible fact — is (1) which gaps a fetch can
close and (2) how a completed fetch result maps to a status. Both are pinned below.
"""
from __future__ import annotations

from types import SimpleNamespace

from routers._fundamental_ingest import (
    INGESTABLE_REASONS,
    classify_fetch_outcome,
    is_ingestable,
    reusable_same_listing,
)


class TestOnlyOurOwnGapsAreIngestable:
    """⚠ A FETCH FIXES A GAP ON OUR SIDE, NOT A PURCHASE DECISION. `unsubscribed` is the data we
    cannot buy; `fund`/`not_equity`/`cash` are categories the question does not apply to. Offering
    an ingest button on any of them promises something a fetch can never deliver."""

    def test_the_two_fixable_reasons(self):
        assert is_ingestable("no_company") is True
        assert is_ingestable("no_metrics") is True

    def test_everything_else_is_refused(self):
        for r in ("unsubscribed", "fund", "not_equity", "cash", "covered", "", None):
            assert is_ingestable(r) is False

    def test_the_set_matches_the_two_documented_reasons(self):
        assert INGESTABLE_REASONS == {"no_company", "no_metrics"}


class TestTheOutcomeIsWhatTheFetchRETURNED:
    """⚠ NEVER GUESSED FROM THE INPUT. A `no_company` ISIN can resolve to an unsubscribed listing
    or one GuruFocus has no financials for — the status is whatever the fetch actually did."""

    def test_rows_loaded_is_ingested(self):
        status, detail = classify_fetch_outcome(1200, 85, False, None)
        assert status == "ingested"
        assert "1200 rows" in detail and "85 metrics" in detail

    def test_metrics_without_a_row_count_still_counts_as_ingested(self):
        # An idempotent re-fetch can report metrics parsed with 0 NEW rows upserted.
        assert classify_fetch_outcome(0, 85, False, None)[0] == "ingested"

    def test_a_forbidden_fetch_is_unsubscribed_not_an_error(self):
        """⚠ A 403 is an ANSWER ("you can't have it"), not a fault — a well-formed request the
        subscription refused. Reading it as an error sends the reader chasing a bug."""
        status, detail = classify_fetch_outcome(0, 0, True, "403 unsubscribed region for X")
        assert status == "unsubscribed"

    def test_an_empty_load_with_no_error_is_no_data_not_an_error(self):
        """⚠ GuruFocus simply has nothing for this listing. `no_data` is a gap, not a fault; only
        a real transport/parse error is an `error`."""
        assert classify_fetch_outcome(0, 0, False, None)[0] == "no_data"

    def test_a_real_error_with_no_rows_is_an_error(self):
        status, detail = classify_fetch_outcome(0, 0, False, "connection reset")
        assert status == "error"
        assert detail == "connection reset"

    def test_forbidden_wins_over_a_stray_row_count(self):
        # A 403 is definitive regardless of anything else in the result.
        assert classify_fetch_outcome(5, 5, True, None)[0] == "unsubscribed"


class TestOnlyTheSameListingIsReused:
    """⚠ THE "✓ INGESTED BUT NO GF EXCHANGE" BUG. A cross-exchange NAME match (Constellation on
    TSX) is not the OTC line we resolved — reusing it corrupts that row and leaves the holding
    reading `no_company` because the ISIN stamp lands on the wrong (or a differently-keyed) row."""

    def _m(self, cid, exch):
        return SimpleNamespace(company_id=cid, exchange_code=exch)

    def test_a_match_on_the_resolved_exchange_is_reused(self):
        matches = [self._m(1, "TSX"), self._m(2, "OTCPK")]
        assert reusable_same_listing(matches, "OTCPK").company_id == 2

    def test_a_cross_exchange_name_match_is_NOT_reused(self):
        # Only TSX rows exist; we resolved the OTC line — nothing here is the same listing.
        matches = [self._m(1, "TSX"), self._m(3, "TSXV")]
        assert reusable_same_listing(matches, "OTCPK") is None

    def test_no_matches_at_all(self):
        assert reusable_same_listing([], "OTCPK") is None

    def test_exchange_comparison_is_case_and_space_insensitive(self):
        assert reusable_same_listing([self._m(9, " otcpk ")], "OTCPK").company_id == 9


class TestResolvingThePrimaryListing:
    """The ISIN → primary SUBSCRIBED listing step behind both ingest branches. Shopify sits on
    TSX; GuruFocus resolves NASDAQ:SHOP, which is what the ingest repoints/creates against."""

    def _patch(self, monkeypatch, listing: dict):
        import routers._asset_dividends as ad
        monkeypatch.setattr(ad, "_resolve_listing", lambda _canon, force=False: listing)

    def test_a_subscribed_primary_is_returned(self, monkeypatch):
        from routers._fundamental_ingest import _resolve_primary
        self._patch(monkeypatch, {"status": "ok", "gurufocus_ticker": "SHOP",
                                  "exchange_code": "NASDAQ", "is_home": True})
        ticker, exch, refusal = _resolve_primary("CA82509L1076", "CA82509L1076", force=False)
        assert (ticker, exch, refusal) == ("SHOP", "NASDAQ", None)

    def test_each_refusal_status_maps_to_its_own_answer(self, monkeypatch):
        from routers._fundamental_ingest import _resolve_primary
        cases = {
            "not_applicable": "not_equity",
            "not_found": "not_found",
            "unsubscribed": "unsubscribed",
            "error": "error",
        }
        for gf_status, want in cases.items():
            self._patch(monkeypatch, {"status": gf_status, "gurufocus_ticker": None,
                                      "exchange_code": None})
            ticker, exch, refusal = _resolve_primary("X", "X", force=False, company_id=7)
            assert ticker is None and exch is None
            assert refusal["status"] == want
            assert refusal["company_id"] == 7   # threaded through so the row keeps its identity

    def test_a_transient_error_is_retriable_not_not_found(self, monkeypatch):
        """⚠ A 500 must NOT read as `not_found`. `_resolve_listing` already returns an uncached
        `error` on a bad spell; the ingest surfaces it as a retriable `error`, not a dead end."""
        from routers._fundamental_ingest import _resolve_primary
        self._patch(monkeypatch, {"status": "error", "gurufocus_ticker": None, "exchange_code": None})
        _t, _e, refusal = _resolve_primary("X", "X", force=False)
        assert refusal["status"] == "error"
