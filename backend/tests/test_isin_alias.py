"""Manual ISIN aliases: one ISIN served by another's instrument.

The measured case: TSMC's NYSE ADR (`US8740391003`) is served by the Taiwan ordinary
(`TW0002330008`) — two identifiers, one business.
"""
from __future__ import annotations

from asset_pipeline import isin_alias
from tests._fake_supabase import FakeSupabase

ADR, ORD = "US8740391003", "TW0002330008"


def _tables(alias=True, adr_gf=True, ord_gf=False):
    return {
        "asset_isin_alias": ([{"isin": ADR, "canonical_isin": ORD, "note": "TSMC"}]
                             if alias else []),
        "asset_execution": [
            {"execution_id": 19, "isin": ADR, "analysis_id": 19, "yahoo_symbol": "TSM",
             "name": "TSMC", "exchange": "NYSE", "currency": "USD", "med_adv_eur": 4.96e9,
             "first_date": "1997-10-09", "years": 28.8, "status": "ok", "asset_class": "equity",
             "listing_country": "United States", "is_leveraged": False,
             # ⚠ The identity fields must SURVIVE — they describe the security, and these two
             # genuinely are different securities.
             "openfigi_figi": "BBG000BD8ZK0", "openfigi_type": "Depositary Receipt",
             "openfigi_name": "TAIWAN SEMICONDUCTOR-SP ADR"},
            {"execution_id": 2889, "isin": ORD, "analysis_id": 9040, "yahoo_symbol": "2330.TW",
             "name": "TSMC", "exchange": "Taiwan", "currency": "TWD", "med_adv_eur": 2.08e9,
             "first_date": "2000-01-04", "years": 26.5, "status": "ok", "asset_class": "equity",
             "listing_country": "Taiwan", "is_leveraged": False,
             "openfigi_figi": "BBG000BN2JD8", "openfigi_type": "Common Stock",
             "openfigi_name": "TAIWAN SEMICONDUCTOR MANUFAC"},
        ],
        "gurufocus_listing": ([{"isin": ADR, "exchange_code": "NYSE", "gurufocus_ticker": "TSM",
                                "is_home": True, "status": "ok"}] if adr_gf else [])
                             + ([{"isin": ORD, "exchange_code": "TPE", "gurufocus_ticker": "2330",
                                  "is_home": True, "status": "ok"}] if ord_gf else []),
    }


def _run(monkeypatch, **kw):
    fake = FakeSupabase(_tables(**kw))
    monkeypatch.setattr(isin_alias, "supabase", fake)
    changed = isin_alias.apply_aliases()
    rows = {r["isin"]: r for r in fake.tables["asset_execution"]}
    gf = {r["isin"]: r for r in fake.tables["gurufocus_listing"]}
    return changed, rows, gf


class TestTheAliasTakesTheCanonicalsInstrument:
    def test_the_same_analysis_id_not_a_copied_symbol(self, monkeypatch):
        """⚠ THE SAME SERIES, NOT A DUPLICATE OF IT. Copying the symbol alone would leave two rows
        that agree today and diverge at the next price refresh."""
        _, rows, _ = _run(monkeypatch)
        assert rows[ADR]["analysis_id"] == rows[ORD]["analysis_id"] == 9040

    def test_symbol_currency_and_venue_all_follow(self, monkeypatch):
        _, rows, _ = _run(monkeypatch)
        for k in ("yahoo_symbol", "currency", "exchange", "listing_country", "med_adv_eur"):
            assert rows[ADR][k] == rows[ORD][k], k
        assert rows[ADR]["yahoo_symbol"] == "2330.TW"

    def test_the_openfigi_identity_is_NOT_overwritten(self, monkeypatch):
        """⚠ It describes the SECURITY, and these are different securities. Overwriting it erases
        the only record that this row is an ADR — exactly what a reader needs to interpret a price
        shared with the ordinary."""
        _, rows, _ = _run(monkeypatch)
        assert rows[ADR]["openfigi_type"] == "Depositary Receipt"
        assert rows[ORD]["openfigi_type"] == "Common Stock"
        assert rows[ADR]["openfigi_figi"] != rows[ORD]["openfigi_figi"]

    def test_the_canonical_row_is_never_touched(self, monkeypatch):
        """One-directional on purpose: an override that edited both ends would make "which one is
        authoritative" unanswerable."""
        _, rows, _ = _run(monkeypatch)
        assert rows[ORD]["yahoo_symbol"] == "2330.TW" and rows[ORD]["execution_id"] == 2889


class TestItIsIdempotentAndSurvivesAReResolve:
    def test_a_second_run_changes_nothing(self, monkeypatch):
        fake = FakeSupabase(_tables())
        monkeypatch.setattr(isin_alias, "supabase", fake)
        assert isin_alias.apply_aliases() == 1
        assert isin_alias.apply_aliases() == 0

    def test_it_puts_the_row_back_after_something_re_resolves_it(self, monkeypatch):
        """⚠ THE WHOLE POINT. `fast_resolve`, the repointers and the queue worker all write
        `asset_execution` per ISIN; an override they can silently undo is not an override."""
        fake = FakeSupabase(_tables())
        monkeypatch.setattr(isin_alias, "supabase", fake)
        isin_alias.apply_aliases()
        # A resolver hands the ADR a listing of its own again.
        for r in fake.tables["asset_execution"]:
            if r["isin"] == ADR:
                r.update({"analysis_id": 19, "yahoo_symbol": "TSMN.MX", "currency": "MXN"})
        assert isin_alias.apply_aliases() == 1
        adr = next(r for r in fake.tables["asset_execution"] if r["isin"] == ADR)
        assert adr["yahoo_symbol"] == "2330.TW" and adr["analysis_id"] == 9040


class TestTheGuruFocusListingFollowsTheSameRule:
    def test_a_stale_listing_on_the_alias_is_removed_when_the_canonical_has_none(self, monkeypatch):
        """⚠ DELETED, NOT LEFT BEHIND — a kept row is the exact drift this exists to prevent."""
        _, _, gf = _run(monkeypatch, adr_gf=True, ord_gf=False)
        assert ADR not in gf

    def test_the_canonicals_listing_is_copied_onto_the_alias(self, monkeypatch):
        _, _, gf = _run(monkeypatch, adr_gf=False, ord_gf=True)
        assert gf[ADR]["gurufocus_ticker"] == "2330" and gf[ADR]["exchange_code"] == "TPE"


class TestItRefusesWhatItCannotDo:
    def test_no_aliases_is_a_no_op(self, monkeypatch):
        changed, _, _ = _run(monkeypatch, alias=False)
        assert changed == 0

    def test_a_canonical_with_no_execution_row_is_skipped_not_guessed(self, monkeypatch):
        t = _tables()
        t["asset_execution"] = [r for r in t["asset_execution"] if r["isin"] != ORD]
        fake = FakeSupabase(t)
        monkeypatch.setattr(isin_alias, "supabase", fake)
        assert isin_alias.apply_aliases() == 0
        adr = next(r for r in fake.tables["asset_execution"] if r["isin"] == ADR)
        assert adr["yahoo_symbol"] == "TSM"       # left exactly as it was
