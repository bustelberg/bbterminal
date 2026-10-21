"""`asset_pipeline/symbol_overrides.json` — the checked-in wrong-listing fixes.

 THIS FILE IS THE ONLY THING THAT CATCHES A TYPO IN IT. `load_file_overrides()` deliberately
swallows a parse error (one broken bracket must not un-pin every other ISIN, and the DB overrides
still apply) — so a malformed file costs nothing but a log line at 05:00, and the symptom appears
weeks later as a constituent quietly back on a Stuttgart listing doing EUR 3k/day. CI is where that
has to be caught, which is what these assertions are.

 NO DB, NO NETWORK. The loader reads a file; only `load_symbol_overrides()` (the merge with the
table) touches Supabase, and that is not what is tested here.
"""
from __future__ import annotations

import json
from pathlib import Path

from asset_pipeline.isin_util import is_valid_isin

_FILE = Path(__file__).resolve().parents[1] / "asset_pipeline" / "symbol_overrides.json"


def _entries() -> list[dict]:
    return json.loads(_FILE.read_text(encoding="utf-8"))["overrides"]


class TestTheOverridesFileIsWellFormed:
    def test_it_parses(self):
        """The whole point: `load_file_overrides` cannot tell you this, because it is written to
        keep going when it is false."""
        assert isinstance(_entries(), list)

    def test_every_isin_is_a_real_isin(self):
        """ CHECK DIGIT, NOT JUST SHAPE. A mistyped ISIN does not fail loudly anywhere downstream —
        `_needs_repoint` simply finds no execution row, logs `has no execution row to repoint` and
        moves on, so the override reads as applied and the listing never changes."""
        for e in _entries():
            assert is_valid_isin(e["isin"]), f"{e.get('name')}: {e['isin']} is not a valid ISIN"

    def test_every_entry_names_a_symbol_and_says_why(self):
        """ THE `note` IS NOT DECORATION. An override is a human overruling the ranker; the next
        person to read this file has to be able to tell a measured wrong-listing fix from a guess,
        or the safe move becomes "leave it alone for ever"."""
        for e in _entries():
            assert e["symbol"].strip(), f"{e['isin']} has no symbol"
            assert len(e.get("note", "")) > 40, f"{e['isin']} has no explanation"
            assert e.get("name", "").strip(), f"{e['isin']} has no company name"

    def test_no_isin_is_pinned_twice(self):
        """Two entries for one ISIN means the later silently wins, and which is later is whatever the
        file happens to say — a coin flip between two venues, decided by line order."""
        isins = [e["isin"].strip().upper() for e in _entries()]
        assert len(isins) == len(set(isins)), "duplicate ISIN in symbol_overrides.json"

    def test_the_loader_returns_exactly_those_pairs(self):
        """The file and the loader must not drift — e.g. a renamed key would leave the loader
        returning {} while the file looks fully populated."""
        from asset_pipeline.symbol_override import load_file_overrides
        assert load_file_overrides() == {e["isin"].strip().upper(): e["symbol"].strip()
                                        for e in _entries()}

    def test_the_three_hong_kong_repoints_are_present(self):
        """The measured cohort from 2026-08-17: three ACWI constituents priced off German lines at
        EUR 2.6k-6.6k/day, which also put two Hong Kong companies in the Europe bucket of the region
        axis. Pinned by name so a future clean-up cannot quietly drop them."""
        got = {e["isin"]: e["symbol"] for e in _entries()}
        assert got["KYG5264Y1089"] == "3888.HK"    # Kingsoft, was 3K1.SG
        assert got["KYG5496K1242"] == "2331.HK"    # Li Ning, was LNLB.SG
        assert got["BMG677491539"] == "0316.HK"    # Orient Overseas, was ORI1.MU

    def test_tsmc_ordinary_is_not_priced_from_the_us_adr(self):
        """ACWI holds 2330.TW; `TSM` is a different security with a different ISIN."""
        got = {e["isin"]: e["symbol"] for e in _entries()}
        assert got["TW0002330008"] == "2330.TW"

    def test_ferrovial_uses_the_listing_named_by_the_acwi_workbook(self):
        """The file's `FER` on Bolsa de Madrid is `FER.MC`, not the 0P2N.IL line the automatic
        resolver selected. An exact-symbol membership join can only work when both sides use the
        workbook's listing."""
        got = {e["isin"]: e["symbol"] for e in _entries()}
        assert got["NL0015001FS8"] == "FER.MC"

    def test_dino_polska_uses_its_warsaw_ordinary_listing(self):
        """0TCP.IL is not the native PLN listing and creates a false price-check gap."""
        got = {e["isin"]: e["symbol"] for e in _entries()}
        assert got["PLDINPL00011"] == "DNP.WA"

    def test_hoya_uses_its_tokyo_ordinary_listing(self):
        """The TopSelectie look-through holds the Japanese ordinary, not an ADR."""
        got = {e["isin"]: e["symbol"] for e in _entries()}
        assert got["JP3837800006"] == "7741.T"

    def test_native_asml_and_tsmc_adr_price_listings_are_pinned(self):
        got = {e["isin"]: e for e in _entries()}
        assert got["NL0010273215"]["symbol"] == "ASML.AS"
        assert got["US8740391003"]["symbol"] == "TSM"
        assert got["US8740391003"]["alias_price_override"] is True

    def test_alibaba_uses_the_hong_kong_ordinary_listing(self):
        got = {e["isin"]: e["symbol"] for e in _entries()}
        assert got["KYG017191142"] == "9988.HK"

    def test_baidu_uses_the_hong_kong_class_a_listing(self):
        got = {e["isin"]: e["symbol"] for e in _entries()}
        assert got["KYG070341048"] == "9888.HK"

    def test_shell_uses_the_amsterdam_eur_listing(self):
        got = {e["isin"]: e["symbol"] for e in _entries()}
        assert got["GB00BP6MXD84"] == "SHELL.AS"

    def test_linde_uses_the_xetra_eur_listing_for_both_issued_isins(self):
        """The native EUR ordinary must not be compared with the USD NYSE line."""
        got = {e["isin"]: e["symbol"] for e in _entries()}
        assert got["IE00BZ12WP82"] == "LIN.DE"
        assert got["IE000S9YS762"] == "LIN.DE"

    def test_relx_uses_the_amsterdam_eur_listing(self):
        """REL.L is a London pence quote, not AIRS's held EUR Amsterdam line."""
        got = {e["isin"]: e["symbol"] for e in _entries()}
        assert got["GB00B2B0DG97"] == "REN.AS"

    def test_argenx_uses_the_brussels_eur_listing(self):
        got = {e["isin"]: e["symbol"] for e in _entries()}
        assert got["NL0010832176"] == "ARGX.BR"

    def test_pro_medicus_uses_the_asx_aud_listing(self):
        got = {e["isin"]: e["symbol"] for e in _entries()}
        assert got["AU000000PME8"] == "PME.AX"

    def test_roko_uses_the_stockholm_sek_listing(self):
        got = {e["isin"]: e["symbol"] for e in _entries()}
        assert got["SE0023950795"] == "ROKO-B.ST"

    def test_risk_profile_bond_tracker_is_pinned_to_aggh(self):
        """A fresh environment must bootstrap the fixed policy benchmark without fuzzy search."""
        got = {e["isin"]: e["symbol"] for e in _entries()}
        assert got["IE00BDBRDM35"] == "AGGH.AS"

    def test_letko_fund_uses_its_reviewed_yahoo_series(self):
        got = {e["isin"]: e["symbol"] for e in _entries()}
        assert got["IE000MEQP5U8"] == "0P0001TPVP.F"

    def test_aqr_apex_fund_uses_its_reviewed_yahoo_series(self):
        got = {e["isin"]: e["symbol"] for e in _entries()}
        assert got["LU1662495628"] == "0P0001BD8Q.F"
