"""`ingest.company_overrides.apply_company_overrides` — the manual override
dedup layer. Tested in dry_run against the in-memory FakeSupabase so we assert
WHICH overrides fire (alias when both ISINs resolve to distinct companies;
exclude when the target exists and isn't already out-of-scope) without the
FK-rewire machinery.
"""
from __future__ import annotations

from ingest.company_overrides import apply_company_overrides
from tests._fake_supabase import FakeSupabase


def _co(cid, name, isin, ticker="X", exch="NYSE"):
    return {
        "company_id": cid, "company_name": name, "isin": isin,
        "gurufocus_ticker": ticker, "out_of_scope_at": None,
        "market_cap_eur": None, "market_cap_native": None, "market_cap_currency": None,
        "market_cap_fx_rate": None, "market_cap_date": None,
        "gurufocus_exchange": {"exchange_code": exch},
    }


def test_alias_fires_only_when_both_exist():
    fake = FakeSupabase(tables={
        "company": [
            _co(1, "New Oriental HK", "KYG6470A1168"),       # secondary (loser)
            _co(2, "New Oriental ADR", "US6475812060"),      # canonical (winner)
        ],
        "company_override": [
            {"kind": "alias", "isin": "KYG6470A1168", "canonical_isin": "US6475812060",
             "ticker": None, "exchange": None, "note": None},
            # canonical missing → must NOT fire
            {"kind": "alias", "isin": "GB0007980591", "canonical_isin": "US0556221044",
             "ticker": None, "exchange": None, "note": None},
        ],
    })
    rep = apply_company_overrides(fake, dry_run=True)
    assert rep.aliases_merged == 1   # only New Oriental; BP canonical absent
    assert rep.excluded_marked == 0


def test_exclude_by_ticker_exchange():
    fake = FakeSupabase(tables={
        "company": [
            _co(10, "GE Vernova T&D India", None, ticker="GVTD", exch="NSE"),
            _co(11, "Already out", None, ticker="ZZZ", exch="NSE"),
        ],
        "company_override": [
            {"kind": "exclude", "isin": None, "ticker": "GVTD", "exchange": "NSE",
             "canonical_isin": None, "note": "unwanted"},
            # matches nothing (no such ticker) → no-op
            {"kind": "exclude", "isin": None, "ticker": "NOPE", "exchange": "NSE",
             "canonical_isin": None, "note": None},
        ],
    })
    # Mark cid=11 already out-of-scope to confirm idempotency isn't what's tested
    rep = apply_company_overrides(fake, dry_run=True)
    assert rep.excluded_marked == 1  # only GVTD/NSE
    assert rep.aliases_merged == 0
