"""Unit tests for `LeonteqTemplate._match_company` — the multi-tier
matcher that resolves a scraped Leonteq row to a `company_id`. These
tests run the matcher in isolation against fixture indexes (no DB, no
network) and pin down the tier-order semantics that the Kikkoman /
Chang Hwa bug exposed.

Tiers, in order of reliability (most reliable first):
  1. (ticker, exchange) derived from the row's RIC suffix.
  2. (ticker, exchange) derived from the row's country.
  3. ISIN match against a prior_isin_map.
  4. Bare ticker fallback — logs a WARNING when ambiguous.
"""
from __future__ import annotations

import logging
import pytest

from index_universe.templates.leonteq import LeonteqTemplate


# ── Fixture helpers ─────────────────────────────────────────────────

def _co(cid: int, ticker: str, exchange: str, name: str = "") -> dict:
    """Shape mimics what `_load_company_index` extracts from the
    company table (gurufocus_exchange is a nested dict, like
    PostgREST's embedded-resource output)."""
    return {
        "company_id": cid,
        "company_name": name or f"Company{cid}",
        "gurufocus_ticker": ticker.upper(),
        "gurufocus_exchange": {"exchange_code": exchange},
    }


def _build_indexes(rows: list[dict]) -> tuple[dict, dict, dict]:
    """Mimics what `_load_company_index` produces: by_(ticker, exchange),
    by_bare_ticker (list of candidates per ticker), by_id."""
    by_te: dict[tuple[str, str], dict] = {}
    by_bare: dict[str, list[dict]] = {}
    by_id: dict[int, dict] = {}
    for r in rows:
        tkr = (r.get("gurufocus_ticker") or "").strip().upper()
        exch = ((r.get("gurufocus_exchange") or {}).get("exchange_code") or "").strip().upper()
        if tkr and exch:
            by_te[(tkr, exch)] = r
        if tkr:
            by_bare.setdefault(tkr, []).append(r)
        by_id[int(r["company_id"])] = r
    return by_te, by_bare, by_id


# ── Tier 1: RIC-derived (ticker, exchange) ──────────────────────────

def test_ric_suffix_resolves_kikkoman_not_chang_hwa() -> None:
    """The canonical regression case: both Kikkoman (TSE:2801) and
    Chang Hwa (TPE:2801) exist; Leonteq scrape carries RIC '2801.T'
    (Tokyo suffix) → must resolve to Kikkoman, not the alphabetically
    or insertion-order-last collider."""
    by_te, by_bare, by_id = _build_indexes([
        _co(4348, "2801", "TSE", "KIKKOMAN CORP"),
        _co(4389, "2801", "TPE", "CHANG HWA COMMERCIAL BANK LTD"),
    ])
    tpl = LeonteqTemplate()
    cid = tpl._match_company(
        {"ticker": "2801", "ric": "2801.T", "isin": "JP3240400006", "country": "Japan"},
        by_te, by_bare, by_id, prior_isin_map={},
    )
    assert cid == 4348, "RIC '.T' must steer to Tokyo / Kikkoman"


def test_ric_suffix_taipei_resolves_chang_hwa() -> None:
    """The mirror case — '.TW' suffix means Taipei, must resolve to
    Chang Hwa even though Kikkoman has the same ticker."""
    by_te, by_bare, by_id = _build_indexes([
        _co(4348, "2801", "TSE", "KIKKOMAN CORP"),
        _co(4389, "2801", "TPE", "CHANG HWA COMMERCIAL BANK LTD"),
    ])
    tpl = LeonteqTemplate()
    cid = tpl._match_company(
        {"ticker": "2801", "ric": "2801.TW", "country": "Taiwan"},
        by_te, by_bare, by_id, prior_isin_map={},
    )
    assert cid == 4389


def test_unknown_ric_suffix_falls_through_to_country() -> None:
    """RIC parser only knows the suffixes in `_RIC_SUFFIX_TO_EXCHANGE`;
    an unknown suffix shouldn't error — it should just fail tier 1 and
    fall through to tier 2 (country)."""
    by_te, by_bare, by_id = _build_indexes([
        _co(100, "TEST", "TSE"),
    ])
    tpl = LeonteqTemplate()
    cid = tpl._match_company(
        {"ticker": "TEST", "ric": "TEST.UNKNOWN", "country": "Japan"},
        by_te, by_bare, by_id, prior_isin_map={},
    )
    assert cid == 100  # country-based fallback wins


# ── Tier 2: country-derived (ticker, exchange) ──────────────────────

def test_country_fallback_when_no_ric() -> None:
    """When the row has no RIC, tier 1 returns None; tier 2 reads
    `country` and tries each candidate exchange in order."""
    by_te, by_bare, by_id = _build_indexes([
        _co(10, "AAPL", "NASDAQ"),
    ])
    tpl = LeonteqTemplate()
    cid = tpl._match_company(
        {"ticker": "AAPL", "country": "United States"},
        by_te, by_bare, by_id, prior_isin_map={},
    )
    assert cid == 10


def test_country_multi_exchange_tries_each() -> None:
    """India maps to [NSE, BSE]; if our company is on BSE, the second
    candidate should match. (Order matters: NSE checked first, so if a
    company existed on both, NSE wins — matches the dict order.)"""
    by_te, by_bare, by_id = _build_indexes([
        _co(20, "RELIANCE", "BSE"),
    ])
    tpl = LeonteqTemplate()
    cid = tpl._match_company(
        {"ticker": "RELIANCE", "country": "India"},
        by_te, by_bare, by_id, prior_isin_map={},
    )
    assert cid == 20


# ── Tier 3: ISIN via prior_isin_map ─────────────────────────────────

def test_isin_resolves_when_ticker_and_country_fail() -> None:
    """RIC missing, country not mapped → tier 1+2 miss. ISIN known in
    prior_isin_map points us to the correct company even though the
    Leonteq ticker doesn't match our gurufocus_ticker."""
    by_te, by_bare, by_id = _build_indexes([
        _co(30, "GFI", "NYSE"),
    ])
    tpl = LeonteqTemplate()
    cid = tpl._match_company(
        {"ticker": "DIFFERENT_TICKER", "isin": "US38059T1060", "country": "Unmapped"},
        by_te, by_bare, by_id, prior_isin_map={"US38059T1060": 30},
    )
    assert cid == 30


def test_isin_ignored_when_pointing_at_nonexistent_company() -> None:
    """Stale prior_isin_map entries (pointing at deleted company_ids)
    shouldn't break the matcher — tier 3 silently misses and we fall
    through to tier 4."""
    by_te, by_bare, by_id = _build_indexes([])
    tpl = LeonteqTemplate()
    cid = tpl._match_company(
        {"ticker": "X", "isin": "XX0000000001"},
        by_te, by_bare, by_id, prior_isin_map={"XX0000000001": 99999},
    )
    assert cid is None


# ── Tier 4: bare ticker fallback + ambiguity warning ────────────────

def test_bare_ticker_unique_match_returns_cid() -> None:
    """No RIC, no country, no ISIN — just a unique bare ticker. Should
    match without warning."""
    by_te, by_bare, by_id = _build_indexes([
        _co(40, "UNIQUE", "NYSE"),
    ])
    tpl = LeonteqTemplate()
    cid = tpl._match_company(
        {"ticker": "UNIQUE"},
        by_te, by_bare, by_id, prior_isin_map={},
    )
    assert cid == 40


def test_bare_ticker_ambiguous_warns_and_picks_first(caplog) -> None:
    """When the bare-ticker tier finds 2+ candidates AND the scraped row has
    no usable name to disambiguate, it must log a WARNING and return the
    first candidate — the safety net for cases the higher tiers can't split."""
    by_te, by_bare, by_id = _build_indexes([
        _co(50, "AMBIG", "TSE", "First"),
        _co(51, "AMBIG", "LSE", "Second"),
    ])
    tpl = LeonteqTemplate()
    with caplog.at_level(logging.WARNING, logger="index_universe.templates.leonteq"):
        cid = tpl._match_company(
            {"ticker": "AMBIG"},  # no RIC/country/ISIN/name
            by_te, by_bare, by_id, prior_isin_map={},
        )
    assert cid == 50
    assert any("AMBIGUOUS bare-ticker" in r.message for r in caplog.records)


def test_bare_ticker_picks_name_overlapping_candidate() -> None:
    """With a scraped name, a bare-ticker collision resolves to the candidate
    whose NAME overlaps — not just the first listed."""
    by_te, by_bare, by_id = _build_indexes([
        _co(60, "ALV", "XTER", "ALLIANZ"),
        _co(61, "ALV", "NYSE", "Autoliv Inc"),
    ])
    tpl = LeonteqTemplate()
    cid = tpl._match_company(
        {"ticker": "ALV", "name": "Autoliv Inc"},  # no RIC/country/ISIN
        by_te, by_bare, by_id, prior_isin_map={},
    )
    assert cid == 61  # Autoliv, not the first-listed Allianz


def test_bare_ticker_wrong_issuer_rejected(caplog) -> None:
    """The Autoliv→Allianz class of bug: a lone bare-ticker candidate whose
    name doesn't overlap the scraped name is rejected (→ None) so the OpenFIGI
    ISIN resolver can find the right company, instead of silently mismapping."""
    by_te, by_bare, by_id = _build_indexes([
        _co(70, "ALV", "XTER", "ALLIANZ"),
    ])
    tpl = LeonteqTemplate()
    with caplog.at_level(logging.WARNING, logger="index_universe.templates.leonteq"):
        cid = tpl._match_company(
            {"ticker": "ALV", "name": "Autoliv Inc", "isin": "US0528001094"},
            by_te, by_bare, by_id, prior_isin_map={},
        )
    assert cid is None
    assert any("rejected" in r.message for r in caplog.records)


def test_returns_none_when_nothing_matches() -> None:
    by_te, by_bare, by_id = _build_indexes([
        _co(60, "X", "NYSE"),
    ])
    tpl = LeonteqTemplate()
    cid = tpl._match_company(
        {"ticker": "Y", "country": "Unknown"},
        by_te, by_bare, by_id, prior_isin_map={},
    )
    assert cid is None


def test_empty_ticker_returns_none() -> None:
    by_te, by_bare, by_id = _build_indexes([_co(70, "X", "NYSE")])
    tpl = LeonteqTemplate()
    assert tpl._match_company({"ticker": ""}, by_te, by_bare, by_id, prior_isin_map={}) is None
    assert tpl._match_company({"ticker": None}, by_te, by_bare, by_id, prior_isin_map={}) is None


# ── HKSE zero-pad (legacy GuruFocus convention) ─────────────────────

def test_hkse_zero_pad_when_ric_says_hong_kong() -> None:
    """GuruFocus stores HKSE tickers zero-padded to 5 digits (e.g.
    HKSE:02628). Leonteq's RIC is '2628.HK' (bare). Tier 1's
    `(ticker, exchange)` lookup should retry with the zero-padded form
    so we hit the existing canonical row."""
    by_te, by_bare, by_id = _build_indexes([
        _co(80, "02628", "HKSE"),
    ])
    tpl = LeonteqTemplate()
    cid = tpl._match_company(
        {"ticker": "2628", "ric": "2628.HK"},
        by_te, by_bare, by_id, prior_isin_map={},
    )
    assert cid == 80


# ── Name-token overlap (used by the post-match audit) ───────────────

def test_name_token_overlap_detects_mismatch() -> None:
    """The audit pass relies on `_name_token_overlap` to flag matches
    where the scraped name and the matched company name share no
    non-trivial token. Should be False for the Kikkoman/Chang Hwa case,
    True for normal matches with shared tokens."""
    assert LeonteqTemplate._name_token_overlap("Kikkoman Corp", "CHANG HWA COMMERCIAL BANK LTD") is False
    assert LeonteqTemplate._name_token_overlap("Kikkoman Corp", "KIKKOMAN CORP") is True
    assert LeonteqTemplate._name_token_overlap("Apple Inc", "APPLE INC.") is True
    # Edge: short / numeric tokens get filtered (length >= 3)
    assert LeonteqTemplate._name_token_overlap("A B C", "X Y Z") is True  # nothing left, can't tell


@pytest.mark.parametrize("ric,expected_exch", [
    ("2801.T", "TSE"),
    ("2801.TW", "TPE"),
    ("VOD.L", "LSE"),
    ("BMW.DE", "XTER"),
    ("XYZ.UNKNOWN", None),
    ("", None),
    (None, None),
])
def test_exchange_from_ric(ric, expected_exch) -> None:
    """Snapshot a handful of suffix mappings to lock in the table."""
    from index_universe.templates.leonteq import _exchange_from_ric
    assert _exchange_from_ric(ric) == expected_exch
