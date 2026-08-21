"""Grouping a portfolio's holdings by AIRS's own Beleggingscategorie.

Every case is real, from AirSPMS on 2026-07-17.
"""
from __future__ import annotations

from routers._airs_holding_isin import _is_etf, _segments


def H(name, cls, value, start=None, etf=False):
    # `_segments` groups by the CALCULATED CLASS (`bucket`, incl. any manual override), so the
    # holding carries it; `asset_class` is kept for the fields that still read AIRS's own label.
    return {"holding_name": name, "asset_class": cls, "bucket": cls, "current_value_eur": value,
            "start_value_eur": start, "is_etf": etf}


class TestTheWrapperIsNotAnAssetClass:
    """⚠ AIRS classifies what a holding INVESTS IN. An equity ETF is Equity; a bond ETF is
    Bonds. Measured: 10 of the 11 bond ISINs are ETFs, and on BUS_Defensief_FX an "ETFs" bucket
    would move 43.20 of the 48.65% bond sleeve out of Bonds — a defensive book reading as though
    it held almost none."""

    def test_a_bond_etf_stays_in_bonds_and_is_counted_there(self):
        segs = {s["asset_class"]: s for s in _segments([
            H("iShares Global Corp Bond ETF", "Bonds", 178797, 183505, etf=True),
            H("Rabobank Certificaten", "Bonds", 30354, 30378),
            H("ASML Holding", "Equity", 41834, 24878),
        ])}
        assert set(segs) == {"Bonds", "Equity"}, "an ETF is not a bucket"
        assert segs["Bonds"]["holdings"] == 2
        assert segs["Bonds"]["etf_value_eur"] == 178797   # counted, beside the class
        assert segs["Bonds"]["value_eur"] == 209151       # NOT reduced by the ETF's weight

    def test_netflix_is_not_an_etf(self):
        """⚠ `name ILIKE '%ETF%'` matches n-ETF-lix. Measured: of the model's ISINs that test
        flags exactly one EQUITY, and it is Netflix."""
        assert _is_etf({"name": "Netflix, Inc.", "leonteq_product_type": "EQUITY"}) is False

    def test_an_untyped_etf_is_still_an_etf(self):
        """`leonteq_product_type` types 19 of the model's ISINs and leaves 40 blank, 11 of them
        plainly ETFs."""
        assert _is_etf({"name": "Vanguard FTSE Japan UCITS ETF USD Accumulation",
                        "leonteq_product_type": None}) is True
        # ⚠ 'UCITS' alone is not enough — this one carries no UCITS in its name.
        assert _is_etf({"name": "iShares J.P. Morgan EM Corporate Bond ETF",
                        "leonteq_product_type": None}) is True

    def test_the_declared_type_wins_over_the_name(self):
        assert _is_etf({"name": "Amundi Index Solutions", "leonteq_product_type": "ETF"}) is True

    def test_it_reads_the_BOOK_s_name_too_not_only_the_grid_s(self):
        """⚠⚠ THE GRID CARRIES THE VENDOR'S ABBREVIATION AND IT DROPS THE WORD. Reported 2026-08-21:
        two funds sat in `Individual stocks` on /management-dashboard because this only ever read
        `grid_row["name"]`, and the readable name is the one the BOOK uses."""
        assert _is_etf({"name": "INVESCO MARKETS II PLC IVZ MSCI", "sector": "etf"},
                       "Invesco World Equal Weight ETF Acc") is True
        assert _is_etf({"name": "LETKO BROS GBL EMR MKT-CLEUR"},
                       "Letko Bross Global EM Equity Fund") is True

    def test_our_own_etf_sector_outranks_any_name_test(self):
        """⚠ A row the asset-pipeline has already filed under the literal sector `etf` must not be
        re-decided as a company because the vendor's name says nothing. This signal alone is what
        catches the Invesco line — its grid name mentions neither ETF nor UCITS."""
        assert _is_etf({"name": "INVESCO MARKETS II PLC IVZ MSCI", "sector": "etf"}) is True

    def test_a_fund_need_not_be_an_ETF(self):
        """⚠ THE FLAG MEANS 'WRAPPER', and a SICAV or a mutual fund has no earnings of its own
        either — which is the only property its consumers (the owner-earnings gate, the
        Individual-stocks / Stock-ETFs division) actually depend on."""
        for n in ("Letko Bross Global EM Equity Fund", "Mint Tower Arbitrage Fund I - EUR",
                  "Fresh Fixed Income Fund", "High Income Quality fund",
                  "Some Luxembourg SICAV", "An Irish ICAV"):
            assert _is_etf(None, n) is True, n

    def test_the_word_boundary_is_load_bearing(self):
        """⚠ `Fundsmith` AND `Fundamental` ARE COMPANIES. Widening to a fund word is only safe
        because the pattern is word-bounded; without it this rule would reclassify operating
        companies as wrappers and quietly drop them out of the fundamentals blend."""
        assert _is_etf({"name": "Fundsmith Equity"}, "Fundsmith Equity") is False
        assert _is_etf({"name": "Fundamental Global Inc"}, "Fundamental Global Inc") is False
        assert _is_etf(None, "ASML Holding") is False


class TestReturnAndWeightDoNotCoverTheSameHoldings:
    """⚠ A holding with no opening value has an UNDEFINED return and REAL exposure."""

    def test_cash_counts_in_the_weight_and_not_in_the_return(self):
        # MoTopSelectie's cash is EUR 600,750 of a EUR 973k book. Priced into the segment return
        # it would read as 600,750 of gain.
        segs = {s["asset_class"]: s for s in _segments([
            H("Effectenrekening", "Cash", 600750, 0),
            H("ASML Holding", "Equity", 41834, 24878),
        ])}
        assert segs["Cash"]["value_eur"] == 600750      # the exposure is real
        assert segs["Cash"]["return_pct"] is None       # the return is not invented
        assert segs["Cash"]["gain_eur"] is None
        assert segs["Cash"]["priced_value_eur"] == 0    # and it says the return spans nothing

    def test_a_short_with_no_opening_value_is_excluded_from_the_return_only(self):
        """TOPS_BEOFF_BEH_DYN holds Nestle India at -3,504 shares and -EUR 44,680, with no
        opening value. It is exposure; it is not a return."""
        segs = {s["asset_class"]: s for s in _segments([
            H("Nestle India Ltd", "Equity", -44680, 0),
            H("ASML Holding", "Equity", 41834, 24878),
        ])}
        e = segs["Equity"]
        assert e["holdings"] == 2
        assert e["value_eur"] == -2846                       # the short reduces the exposure
        assert e["priced_value_eur"] == 41834                 # but not the priced basket
        assert e["return_pct"] == 68.16                       # ASML's return, uncontaminated

    def test_a_fully_priced_segment_reports_the_whole_of_itself(self):
        segs = {s["asset_class"]: s for s in _segments([H("ASML", "Equity", 41834, 24878)])}
        assert segs["Equity"]["value_eur"] == segs["Equity"]["priced_value_eur"]


class TestTheSegmentReturnIsTheStartWeightedValueChange:
    def test_it_is_sum_now_over_sum_start(self):
        # The sleeve return is Σnow / Σstart − 1 — the basket's actual value change, i.e. each
        # holding's return weighted by its OPENING value. NOT weighted by current value, which lets a
        # winner (now a bigger share) dominate: that would read 66.67% here (= (200·100%+100·0%)/300),
        # inflating the true 50%.
        segs = {s["asset_class"]: s for s in _segments([
            H("A", "Equity", 200, 100),     # +100%
            H("B", "Equity", 100, 100),     #    0%
        ])}
        assert segs["Equity"]["return_pct"] == 50.0     # 300/200 − 1
        assert segs["Equity"]["gain_eur"] == 100.0


class TestOrderAndCoverage:
    def test_cash_and_unclassified_sort_last(self):
        # The producible buckets are these five — Real estate is not one of its own (VAS folds into
        # Alternatives in `classify_bucket`) — and Cash + Unclassified sort last.
        order = [s["asset_class"] for s in _segments([
            H("c", "Cash", 1, 0), H("u", "Unclassified", 1, 1),
            H("b", "Bonds", 1, 1), H("e", "Equity", 1, 1),
            H("a", "Alternatives", 1, 1),
        ])]
        assert order == ["Equity", "Bonds", "Alternatives", "Cash", "Unclassified"]

    def test_weights_are_shares_of_the_book(self):
        segs = {s["asset_class"]: s for s in _segments([
            H("a", "Equity", 750, 700), H("b", "Bonds", 250, 240),
        ])}
        assert segs["Equity"]["weight_pct"] == 75.0
        assert segs["Bonds"]["weight_pct"] == 25.0
