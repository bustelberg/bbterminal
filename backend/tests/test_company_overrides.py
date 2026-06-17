"""`ingest.company_overrides.apply_company_overrides` — the manual override
dedup layer. Tested in dry_run against the in-memory FakeSupabase so we assert
WHICH overrides fire (alias when both ISINs resolve to distinct companies;
exclude when the target exists and isn't already out-of-scope) without the
FK-rewire machinery.
"""
from __future__ import annotations

from ingest.company_overrides import apply_company_overrides, set_company_isin
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


def test_set_isin_forces_value_and_is_idempotent():
    fake = FakeSupabase(tables={
        "company": [
            # Zillow Class C row carrying the WRONG (Class A) ISIN.
            _co(4253, "Zillow Group Inc", "US98954M1018", ticker="Z", exch="NASDAQ"),
            # Already-correct row → must NOT be rewritten.
            _co(99, "Other Co", "US0000000001", ticker="OTH", exch="NASDAQ"),
        ],
        "company_override": [
            {"kind": "set_isin", "isin": None, "ticker": "Z", "exchange": "NASDAQ",
             "canonical_isin": "US98954M2008", "note": "Class C"},
            {"kind": "set_isin", "isin": None, "ticker": "OTH", "exchange": "NASDAQ",
             "canonical_isin": "US0000000001", "note": "already correct"},
        ],
    })
    rep = apply_company_overrides(fake)  # not dry_run — assert the actual write
    assert rep.isin_set == 1  # only Zillow; "Other Co" already matches → no-op
    by_id = {c["company_id"]: c for c in fake.tables["company"]}
    assert by_id[4253]["isin"] == "US98954M2008"
    assert by_id[99]["isin"] == "US0000000001"

    # Re-running is a no-op now that the value is correct (idempotent).
    rep2 = apply_company_overrides(fake)
    assert rep2.isin_set == 0


def test_set_company_isin_inserts_when_absent():
    fake = FakeSupabase(tables={"company_override": []})
    set_company_isin(fake, ticker="Z", exchange="NASDAQ", isin="US98954M2008")
    rows = [r for r in fake.tables["company_override"] if r["kind"] == "set_isin"]
    assert len(rows) == 1
    assert rows[0]["canonical_isin"] == "US98954M2008"


def test_set_company_isin_updates_existing_in_place():
    # Pre-seed a row WITH an id (real Postgres assigns it) so the upsert takes
    # the update branch instead of inserting a duplicate.
    fake = FakeSupabase(tables={"company_override": [
        {"id": 1, "kind": "set_isin", "isin": None, "ticker": "Z", "exchange": "NASDAQ",
         "canonical_isin": "US98954M1018", "note": "stale"},
    ]})
    set_company_isin(fake, ticker="Z", exchange="NASDAQ", isin="US98954M2008")
    rows = [r for r in fake.tables["company_override"] if r["kind"] == "set_isin"]
    assert len(rows) == 1  # updated in place, not duplicated
    assert rows[0]["canonical_isin"] == "US98954M2008"
