"""WHICH AIRS book a per-holding Return comes from — and why the price series is the last resort.

A holding's Return in the Analyse modal is an AIRS POSITION RESULT wherever any AIRS book has one.
Before 2026-08-04 a non-empty `via_names` sent the row to our yfinance series outright, and that
was wrong twice over on the measured book (BUS_Offensief_Dyn -> model 1935):

  * MasterCard is EUR 50,489 held OUTRIGHT against EUR 1,991 (3.8%) through the certificate — and
    was priced off a listing purely because SOME of it arrives wrapped, while the book's own
    Vermogensoverzicht values it exactly.
  * 20 of the 23 look-through legs are reachable ONLY through the certificate, and the AIRS account
    BEHIND that certificate values every one of them. The price series answered a different
    question and diverged wildly: Shopify -25.54% against AIRS's +18.24%, Fair Isaac -32.04%
    against +15.33%, BE Semiconductor +47.66% against -32.54%.

 AN AIRS FIGURE IS A POSITION RESULT, NOT A PRICE RETURN. Beginwaarde is the year-open value OR
the PURCHASE value for a position opened during the year, so ONE instrument can legitimately read
differently in two books — MasterCard is +2.14% in BUS_Offensief_Dyn and +17.62% in
StarTopSelectie's. That is why every figure names the book it came from instead of leaving it
inferable, and why two books' answers are never averaged.
"""
from __future__ import annotations

import pytest

from routers import _airs_portfolio_analysis as pa


class TestTheOneDefinition:
    """One AIRS total-return definition: price + FX + share-aligned net dividend."""

    def test_a_position_without_airs_result_has_no_airs_return(self):
        assert pa._airs_position_return({"start_value_eur": 0, "current_value_eur": 110}) is None
        assert pa._airs_position_return(None) is None

    def test_loreal_reaches_the_pdf_total_after_its_partial_sale(self):
        row = {"start_value_eur": 18_330, "current_value_eur": 19_025,
               "fund_result_eur": 695, "fx_result_eur": 0, "airs_result_pct": 3.79}
        aligned = (698.40 - 185.08) * 50 / 97
        assert pa._airs_position_return(row, aligned) == pytest.approx(5.2351224121)

    def test_idexx_uses_the_exact_value_change_within_component_rounding_error(self):
        row = {"start_value_eur": 33_394.67, "current_value_eur": 26_533.31,
               "fund_result_eur": -7_853.0, "fx_result_eur": 992.10,
               "airs_result_pct": -20.54}
        assert pa._airs_position_return(row) == pytest.approx(-20.5462728034)

    def test_a_material_gap_is_not_mistaken_for_component_rounding(self):
        # A trade-related gap belongs outside the price+FX return. Only the tiny display-rounding
        # residue may be reconciled from current minus opening.
        row = {"start_value_eur": 25_928.0, "current_value_eur": 26_472.0,
               "fund_result_eur": 684.0, "fx_result_eur": 0.0,
               "airs_result_pct": 2.64}
        assert pa._airs_position_return(row) == pytest.approx(684 / 25_928 * 100)

    def test_raw_price_fx_is_the_fallback_when_dividend_alignment_is_unavailable(self):
        assert pa._airs_position_return({"airs_result_pct": 3.79}, None) == 3.79

    def test_each_payment_uses_the_quantity_held_on_its_own_date(self):
        from airs_transacties import Trade

        holdings = [{"holding_name": "L` Oreal", "quantity": 50}]
        trades = [Trade(fonds="L` Oreal", kind="sell", datum="2026-05-22", quantity=47)]
        mutations = [
            {"fonds": "L` Oreal", "boekdatum": "2026-05-04", "grootboek": "Dividend",
             "amount_eur": 698.40},
            {"fonds": "L` Oreal", "boekdatum": "2026-05-04",
             "grootboek": "Dividendbelasting", "amount_eur": -185.08},
        ]
        got = pa._aligned_dividend_income(holdings, trades, mutations)
        assert got["L` Oreal"] == pytest.approx(264.5979381443)

    def test_payments_on_both_sides_of_a_sale_get_different_share_counts(self):
        from airs_transacties import Trade

        holdings = [{"holding_name": "Example", "quantity": 80}]
        trades = [Trade(fonds="Example", kind="sell", datum="2026-04-01", quantity=20)]
        mutations = [
            {"fonds": "Example", "boekdatum": "2026-03-01", "grootboek": "Dividend",
             "amount_eur": 100},
            {"fonds": "Example", "boekdatum": "2026-05-01", "grootboek": "Dividend",
             "amount_eur": 80},
        ]
        # First payment: 100 × 80/100. Second payment: all 80 shares still remain.
        assert pa._aligned_dividend_income(holdings, trades, mutations)["Example"] == 160

    def test_a_later_purchase_never_invents_dividends_for_the_new_shares(self):
        from airs_transacties import Trade

        holdings = [{"holding_name": "Hermes Internationale", "quantity": 22}]
        trades = [Trade(fonds="Hermes Internationale", kind="buy",
                        datum="2026-05-13", quantity=11)]
        mutations = [
            {"fonds": "Hermes Internationale", "boekdatum": "2026-02-18",
             "grootboek": "Dividend", "amount_eur": 55.0},
            {"fonds": "Hermes Internationale", "boekdatum": "2026-02-18",
             "grootboek": "Dividendbelasting", "amount_eur": -14.58},
            {"fonds": "Hermes Internationale", "boekdatum": "2026-04-23",
             "grootboek": "Dividend", "amount_eur": 143.0},
            {"fonds": "Hermes Internationale", "boekdatum": "2026-04-23",
             "grootboek": "Dividendbelasting", "amount_eur": -37.90},
        ]
        aligned = pa._aligned_dividend_income(holdings, trades, mutations)
        assert aligned["Hermes Internationale"] == pytest.approx(145.52)
        row = {"start_value_eur": 40_711.0, "current_value_eur": 29_689.0,
               "fund_result_eur": -11_022.0, "fx_result_eur": 0.0,
               "airs_result_pct": -27.07}
        assert pa._airs_position_return(row, aligned["Hermes Internationale"]) == pytest.approx(
            -26.7163174572)

    def test_a_proven_split_does_not_discard_kla_dividends(self):
        from airs_transacties import Trade

        holdings = [{"holding_name": "KLA Corp.", "quantity": 310}]
        trades = [Trade(fonds="KLA Corp.", kind="buy",
                        datum="2026-02-03", quantity=14)]
        mutations = [
            {"fonds": "KLA Corp.", "boekdatum": "2026-03-03", "grootboek": "Dividend",
             "amount_eur": 50.74961237},
            {"fonds": "KLA Corp.", "boekdatum": "2026-03-03",
             "grootboek": "Dividendbelasting", "amount_eur": -7.608133739},
            {"fonds": "KLA Corp.", "boekdatum": "2026-06-02", "grootboek": "Dividend",
             "amount_eur": 61.206970214},
            {"fonds": "KLA Corp.", "boekdatum": "2026-06-02",
             "grootboek": "Dividendbelasting", "amount_eur": -9.1767533182},
            {"fonds": "KLA Corp.", "boekdatum": "2026-09-01", "grootboek": "Dividend",
             "amount_eur": 61.518550501},
            {"fonds": "KLA Corp.", "boekdatum": "2026-09-01",
             "grootboek": "Dividendbelasting", "amount_eur": -9.2234685113},
        ]
        aligned = pa._aligned_dividend_income(
            holdings, trades, mutations, {"KLA Corp."},
            {"KLA Corp.": (10.0, "2026-06-12")})
        assert aligned["KLA Corp."] == pytest.approx(147.4667770175)

    def test_a_pre_split_sale_is_converted_to_todays_share_basis(self):
        from airs_transacties import Trade

        holdings = [{"holding_name": "Example", "quantity": 500}]
        trades = [Trade(fonds="Example", kind="sell", datum="2026-03-01", quantity=50)]
        mutations = [{"fonds": "Example", "boekdatum": "2026-02-01",
                      "grootboek": "Dividend", "amount_eur": 100}]
        aligned = pa._aligned_dividend_income(
            holdings, trades, mutations, {"Example"},
            {"Example": (10.0, "2026-06-01")})
        # 500 remaining shares out of 1,000 held on the payment date.
        assert aligned["Example"] == pytest.approx(50.0)

    def test_the_kla_deposit_is_proven_as_a_split(self):
        from types import SimpleNamespace

        holdings = [{"holding_name": "KLA Corp.", "quantity": 310,
                     "start_value_eur": 34_146.96}]
        sheet = SimpleNamespace(rows=[
            {"Tt": "A", "Datum": "2026-02-03", "Fonds": "KLA Corp.",
             "Aantal": 14.0, "Waarde  EUR": 16_567.077772728},
            {"Tt": "D", "Datum": "2026-06-12", "Fonds": "KLA Corp.",
             "Aantal": 279.0, "Waarde  EUR": 0.0},
        ])
        unknown, splits = pa._detected_book_splits(holdings, sheet)
        assert unknown == {"KLA Corp."}
        assert splits["KLA Corp."][0] == pytest.approx(10.0)
        assert splits["KLA Corp."][1] == "2026-06-12"


class TestTheLadder:
    """Four rungs, and the price series is the fourth."""

    def _wire(self, monkeypatch, *, pre, post, wrapped=None, marks=None, income=None,
              aligned_income=None):
        """`pre` = the rows as AIRS stores them, certificates intact. `direct_marks` and
        `wrapped_ids` are read from THESE, before the expansion, which is the whole fix: afterwards
        an instrument held both directly and inside a certificate is one merged row whose
        start/current are the sum of the two.

        `post` = what the expansion yields (merged by ISIN, `via_names` stamped)."""
        import routers._airs_account_links as links
        import routers._airs_holding_isin as hisin

        from routers import _airs_accounts as accounts

        monkeypatch.setattr(links, "list_account_links", lambda: {
            "accounts": [{"portefeuille": "X_DYN", "model_portfolio_id": 7}]})
        #  `**_kw` BECAUSE `resolve_account_isins` GREW A `freshen` KEYWORD (default True) and this
        # caller passes False. A positional-only stub raises TypeError the moment production starts
        # naming the argument — which is what took these nine tests, and five more in
        # `test_airs_portfolio_analysis`, red. `TestWrappedBookMarks` below pins the VALUE, so
        # widening here does not lose the invariant.
        monkeypatch.setattr(hisin, "resolve_account_isins", lambda p, **_kw: {"rows": pre})
        monkeypatch.setattr(pa, "_expand_book_rows", lambda rows, *_args: post)
        # The wrapped books are stubbed here; they have their own tests below and their own DB hops.
        monkeypatch.setattr(pa, "_wrapped_book_marks", lambda ids: dict(wrapped or {}))
        monkeypatch.setattr(pa, "_book_aligned_dividend_income",
                            lambda pf, rows: dict(aligned_income or {}))
        monkeypatch.setattr(accounts, "_direct_result", lambda pf, names: (dict(income or {}), {}))
        #  The last database hop in an otherwise pure function. Unstubbed it reads `airs_holding`
        # for the book's snapshot date — which passes on a developer machine (dotenv supplies
        # credentials, and the test reads PRODUCTION) and raises `KeyError: 'SUPABASE_URL'` in CI.
        # That asymmetry is the one `tests/conftest.py` exists to convert into a hard failure.
        monkeypatch.setattr(pa, "_book_snapshot_date", lambda pf: "2026-08-04")
        monkeypatch.setattr("routers._airs_lookthrough._datum_of", lambda pid: None)
        monkeypatch.setattr("routers._airs_portfolio_perf.compute_holding_marks",
                            lambda isins, anchor, **kw: dict(marks or {}))
        monkeypatch.setattr(pa, "_grid", lambda isins: {})
        monkeypatch.setattr(pa, "_country_by_code", lambda: {})
        # The book view now consults the paired model's categories only as a
        # fallback for an empty grid.  These cases supply their own classes,
        # so keep this otherwise-pure ladder test off the reference database.
        monkeypatch.setattr(pa, "ref_positions_for", lambda _pid: [])

    @staticmethod
    def _row(out, isin):
        return next(h for h in out["holdings_detail"] if h["isin"] == isin)

    @staticmethod
    def _direct(value, start):
        return [{"label": None, "model_id": None, "value_eur": value, "start_value_eur": start}]

    @staticmethod
    def _via(value, start, model=99, label="Star"):
        return [{"label": label, "model_id": model, "value_eur": value, "start_value_eur": start}]

    def test_a_purely_direct_row_is_this_books_own_figure(self, monkeypatch):
        self._wire(
            monkeypatch,
            pre=[{"isin": "US1", "holding_name": "Fortinet", "start_value_eur": 100.0,
                  "current_value_eur": 111.74, "airs_result_pct": 11.74,
                  "asset_class": "Equity"}],
            post=[{"isin": "US1", "holding_name": "Fortinet", "start_value_eur": 100.0,
                   "current_value_eur": 111.74, "airs_result_pct": 11.74,
                   "asset_class": "Equity", "bucket": "Equity", "via_names": [],
                   "sources": [{**self._direct(111.74, 100.0)[0], "return_pct": 11.74}]}],
        )
        h = self._row(pa._book_port_items(7, {}), "US1")
        assert h["own_return_pct"] == pytest.approx(11.74)
        assert h["own_return_source"] == "airs"
        assert h["own_return_book"] == "X_DYN"

    def test_result_keeps_full_income_while_airs_return_uses_aligned_income(self, monkeypatch):
        from airs_mutaties import DirectResult

        self._wire(
            monkeypatch,
            pre=[{"isin": "US1", "holding_name": "Procter & Gamble",
                  "start_value_eur": 17_563.10, "current_value_eur": 18_599.74,
                  "airs_result_pct": 5.90, "asset_class": "Equity"}],
            post=[{"isin": "US1", "holding_name": "Procter & Gamble",
                   "start_value_eur": 17_563.10, "current_value_eur": 18_599.74,
                   "airs_result_pct": 5.90, "asset_class": "Equity", "bucket": "Equity",
                   "via_names": [],
                   "sources": [{**self._direct(18_599.74, 17_563.10)[0],
                                "return_pct": 7.10}]}],
            income={"Procter & Gamble": DirectResult(
                fonds="Procter & Gamble", gross_eur=662.166647, tax_eur=-99.317743)},
            aligned_income={"Procter & Gamble": 210.75},
        )
        h = self._row(pa._book_port_items(7, {}), "US1")
        assert h["own_return_pct"] == pytest.approx(7.10)
        assert h["income_eur"] == pytest.approx(562.848904)

    def test_a_split_holding_is_BOTH_legs_weighted_by_opening_value(self, monkeypatch):
        """The MasterCard case, in round numbers: 490 held outright at +2.04% and 106 through the
        certificate at +17.62%. Neither leg alone is the position's return, and before this the
        whole row went to yfinance because SOME of it arrives wrapped."""
        self._wire(
            monkeypatch,
            pre=[
                # held outright — the clean valuation, and it exists ONLY before the expansion
                {"isin": "US1", "holding_name": "MasterCard", "start_value_eur": 490.0,
                 "current_value_eur": 500.0, "airs_result_pct": 2.04,
                 "asset_class": "Equity"},
                {"isin": "CH1", "holding_name": "Cert", "start_value_eur": 106.0,
                 "current_value_eur": 100.0, "asset_class": "Equity", "linked_portfolio_id": 99},
            ],
            #  the merged row: direct + the certificate's proportional slice, whose half carries
            # the CERTIFICATE's -5.7%. Reading the instrument's return off THIS is the trap.
            post=[{"isin": "US1", "holding_name": "MasterCard", "start_value_eur": 596.0,
                   "current_value_eur": 600.0, "asset_class": "Equity", "bucket": "Equity",
                   "via_names": ["Star"],
                   "sources": [{**self._direct(500.0, 490.0)[0], "return_pct": 2.04},
                               *self._via(100.0, 106.0)]}],
            wrapped={99: {"US1": {"return_pct": 17.62, "as_of": "2026-07-30",
                                  "portefeuille": "Star_DYN", "income_eur": None}}},
            marks={"US1": {"return_pct": 2.71, "end_date": "2026-07-31"}},
        )
        h = self._row(pa._book_port_items(7, {}), "US1")
        direct_ret = (500.0 / 490.0 - 1.0) * 100.0
        expected = 100 * ((490.0 * (1 + direct_ret / 100) + 106.0 * 1.1762) / 596.0 - 1)
        assert h["own_return_pct"] == pytest.approx(expected)
        assert h["own_return_pct"] != pytest.approx(direct_ret)      # not the direct leg alone
        assert h["own_return_pct"] != pytest.approx(17.62)           # nor the wrapped one
        assert h["own_return_source"] == "airs"
        #  No single book owns a blend. Naming one would credit the whole figure to a book that
        # produced 82% of it; the per-leg attribution is on the routes.
        assert h["own_return_book"] is None
        legs = {s["label"]: s for s in h["sources"]}
        assert legs[None]["book"] == "X_DYN"
        assert legs["Star"]["book"] == "Star_DYN"
        assert sum(s["blend_weight_pct"] for s in h["sources"]) == pytest.approx(100.0)

    def test_a_leg_reachable_only_through_the_certificate_takes_the_wrapped_books_figure(
            self, monkeypatch):
        self._wire(
            monkeypatch,
            pre=[{"isin": "CH1", "holding_name": "Cert", "start_value_eur": 106.0,
                  "current_value_eur": 100.0, "asset_class": "Equity",
                  "linked_portfolio_id": 99}],
            post=[{"isin": "US2", "holding_name": "Shopify", "start_value_eur": 106.0,
                   "current_value_eur": 100.0, "asset_class": "Equity", "bucket": "Equity",
                   "via_names": ["Star"], "sources": self._via(100.0, 106.0)}],
            wrapped={99: {"US2": {"return_pct": 18.24, "as_of": "2026-07-30",
                                  "portefeuille": "Star_DYN", "income_eur": None}}},
            marks={"US2": {"return_pct": -25.54, "end_date": "2026-07-31"}},
        )
        h = self._row(pa._book_port_items(7, {}), "US2")
        assert h["own_return_pct"] == pytest.approx(18.24)
        assert h["own_return_source"] == "airs"
        assert h["own_return_book"] == "Star_DYN"
        #  The wrapped book's own snapshot, which trails this one (measured 5 days apart).
        # Stamping it with the parent's would age-check a number against a scan it never came from.
        assert h["own_return_as_of"] == "2026-07-30"

    def test_two_certificates_each_ask_their_OWN_book(self, monkeypatch):
        #  The marks are keyed by model, not flattened to one ISIN map: two strategies can both
        # hold NVIDIA, each with its own purchase date and its own result.
        self._wire(
            monkeypatch,
            pre=[{"isin": "CH1", "holding_name": "CertA", "start_value_eur": 100.0,
                  "current_value_eur": 100.0, "asset_class": "Equity", "linked_portfolio_id": 1},
                 {"isin": "CH2", "holding_name": "CertB", "start_value_eur": 100.0,
                  "current_value_eur": 100.0, "asset_class": "Equity", "linked_portfolio_id": 2}],
            post=[{"isin": "US1", "holding_name": "NVIDIA", "start_value_eur": 200.0,
                   "current_value_eur": 200.0, "asset_class": "Equity", "bucket": "Equity",
                   "via_names": ["A", "B"],
                   "sources": [*self._via(100.0, 100.0, model=1, label="A"),
                               *self._via(100.0, 100.0, model=2, label="B")]}],
            wrapped={1: {"US1": {"return_pct": 10.0, "as_of": "2026-07-30",
                                 "portefeuille": "A_DYN", "income_eur": None}},
                     2: {"US1": {"return_pct": 30.0, "as_of": "2026-07-29",
                                 "portefeuille": "B_DYN", "income_eur": None}}},
        )
        h = self._row(pa._book_port_items(7, {}), "US1")
        assert h["own_return_pct"] == pytest.approx(20.0)      # equal opening values -> the mean
        legs = {s["label"]: s["return_pct"] for s in h["sources"]}
        assert legs == {"A": pytest.approx(10.0), "B": pytest.approx(30.0)}
        #  A blend is only as fresh as its stalest leg.
        assert h["own_return_as_of"] == "2026-07-29"

    def test_the_price_series_survives_where_no_airs_book_values_the_row(self, monkeypatch):
        # A certificate wrapping a model nobody holds an account for. The leg is real, no book
        # values it, and the price series is the only honest answer left.
        self._wire(
            monkeypatch,
            pre=[{"isin": "CH1", "holding_name": "Cert", "start_value_eur": 106.0,
                  "current_value_eur": 100.0, "asset_class": "Equity", "linked_portfolio_id": 99}],
            post=[{"isin": "US3", "holding_name": "Orphan", "start_value_eur": 106.0,
                   "current_value_eur": 100.0, "asset_class": "Equity", "bucket": "Equity",
                   "via_names": ["Star"], "sources": self._via(100.0, 106.0)}],
            wrapped={},
            marks={"US3": {"return_pct": -5.0, "end_date": "2026-07-31"}},
        )
        h = self._row(pa._book_port_items(7, {}), "US3")
        assert h["own_return_pct"] == pytest.approx(-5.0)
        assert h["own_return_source"] == "yfinance"
        assert h["own_return_book"] is None

    def test_a_wrapped_figure_is_not_reported_as_this_books_dividend(self, monkeypatch):
        # `own_income_eur` is money THIS book received. A figure computed in another book has none
        # to declare here, and borrowing the other book's states something false about this account.
        self._wire(
            monkeypatch,
            pre=[{"isin": "CH1", "holding_name": "Cert", "start_value_eur": 106.0,
                  "current_value_eur": 100.0, "asset_class": "Equity", "linked_portfolio_id": 99}],
            post=[{"isin": "US2", "holding_name": "Shopify", "start_value_eur": 106.0,
                   "current_value_eur": 100.0, "asset_class": "Equity", "bucket": "Equity",
                   "via_names": ["Star"], "sources": self._via(100.0, 106.0)}],
            wrapped={99: {"US2": {"return_pct": 18.24, "as_of": "2026-07-30",
                                  "portefeuille": "Star_DYN", "income_eur": 12.0}}},
        )
        assert self._row(pa._book_port_items(7, {}), "US2")["own_income_eur"] is None


class TestWrappedBookMarks:
    """The AIRS account behind a certificate — loaded only when something is actually wrapped."""

    def test_an_unwrapped_book_costs_nothing(self):
        #  No argument, no query. Most books hold no certificate, and this runs on every open of
        # the modal; an unconditional account load would put two round-trips on all of them.
        assert pa._wrapped_book_marks(set()) == {}

    def test_a_wrapped_model_with_no_paired_account_yields_nothing_rather_than_raising(
            self, monkeypatch):
        # A certificate can wrap a model nobody holds an account for. Its legs fall back to the
        # price series; they do not take the modal down.
        import routers._airs_account_links as links

        monkeypatch.setattr(links, "list_account_links", lambda: {"accounts": []})
        assert pa._wrapped_book_marks({99}) == {}

    def test_each_model_keeps_its_own_map(self, monkeypatch):
        #  Keyed by model. Flattened to one ISIN map, whichever book was read first would answer
        # for a leg that came through the other — two positions, two purchase dates, one figure.
        import routers._airs_account_links as links
        import routers._airs_holding_isin as hisin

        from routers import _airs_accounts as accounts

        monkeypatch.setattr(links, "list_account_links", lambda: {"accounts": [
            {"portefeuille": "A_DYN", "model_portfolio_id": 1},
            {"portefeuille": "B_DYN", "model_portfolio_id": 2}]})
        seen: list[dict] = []

        def _resolve(pf, **kw):
            seen.append({"portefeuille": pf, **kw})
            return {"as_of": "2026-07-30",
                    "rows": [{"isin": "US1", "holding_name": "NVIDIA", "start_value_eur": 100.0,
                              "current_value_eur": 110.0 if pf == "A_DYN" else 150.0,
                              "airs_result_pct": 10.0 if pf == "A_DYN" else 50.0}]}

        monkeypatch.setattr(hisin, "resolve_account_isins", _resolve)
        monkeypatch.setattr(pa, "_book_aligned_dividend_income", lambda pf, rows: {})
        monkeypatch.setattr(accounts, "account_holdings", lambda pf: {"rows": []})
        out = pa._wrapped_book_marks({1, 2})
        #  `freshen=False` IS LOAD-BEARING HERE IN A WAY IT IS NOT ELSEWHERE. This path reads ONE
        # book PER WRAPPED MODEL, so the default (True) would fire a live AIRS scrape per
        # certificate — a chart with three wrapped models becomes three scrapes on every open.
        assert all(c["freshen"] is False for c in seen), seen
        assert out[1]["US1"]["return_pct"] == pytest.approx(10.0)
        assert out[2]["US1"]["return_pct"] == pytest.approx(50.0)
        assert out[1]["US1"]["portefeuille"] == "A_DYN"
        assert out[2]["US1"]["portefeuille"] == "B_DYN"

    def test_a_position_the_wrapped_book_cannot_value_is_absent_not_zero(self, monkeypatch):
        # The certificate's own cash line has no opening value. It must not arrive as 0.00%.
        import routers._airs_account_links as links
        import routers._airs_holding_isin as hisin

        from routers import _airs_accounts as accounts

        monkeypatch.setattr(links, "list_account_links", lambda: {
            "accounts": [{"portefeuille": "A_DYN", "model_portfolio_id": 1}]})
        monkeypatch.setattr(hisin, "resolve_account_isins", lambda pf, **_kw: {
            "as_of": "2026-07-30",
            "rows": [{"isin": "CASH1", "holding_name": "Effectenrekening",
                      "start_value_eur": 0.0, "current_value_eur": 31072.23},
                     {"isin": "US1", "holding_name": "NVIDIA", "start_value_eur": 100.0,
                      "current_value_eur": 110.0, "airs_result_pct": 10.0}]})
        monkeypatch.setattr(pa, "_book_aligned_dividend_income", lambda pf, rows: {})
        monkeypatch.setattr(accounts, "account_holdings", lambda pf: {"rows": []})
        out = pa._wrapped_book_marks({1})
        assert "CASH1" not in out[1]
        assert out[1]["US1"]["return_pct"] == pytest.approx(10.0)
        assert out[1]["US1"]["income_eur"] == pytest.approx(0.0)

    def test_the_wrapped_books_include_their_aligned_dividend(
            self, monkeypatch):
        # The child book supplies its own aligned dividend, on the same quantity as its values.
        import routers._airs_account_links as links
        import routers._airs_holding_isin as hisin

        from routers import _airs_accounts as accounts

        monkeypatch.setattr(links, "list_account_links", lambda: {
            "accounts": [{"portefeuille": "A_DYN", "model_portfolio_id": 1}]})
        monkeypatch.setattr(hisin, "resolve_account_isins", lambda pf, **_kw: {
            "as_of": "2026-07-30",
            "rows": [{"isin": "US1", "holding_name": "Shell", "start_value_eur": 100.0,
                      "current_value_eur": 110.0, "airs_result_pct": 10.0}]})
        monkeypatch.setattr(pa, "_book_aligned_dividend_income",
                            lambda pf, rows: {"Shell": 8.5})
        out = pa._wrapped_book_marks({1})
        assert out[1]["US1"]["return_pct"] == pytest.approx(18.5)
        assert out[1]["US1"]["income_eur"] == pytest.approx(8.5)
