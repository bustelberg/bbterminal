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

⚠ AN AIRS FIGURE IS A POSITION RESULT, NOT A PRICE RETURN. Beginwaarde is the year-open value OR
the PURCHASE value for a position opened during the year, so ONE instrument can legitimately read
differently in two books — MasterCard is +2.14% in BUS_Offensief_Dyn and +17.62% in
StarTopSelectie's. That is why every figure names the book it came from instead of leaving it
inferable, and why two books' answers are never averaged.
"""
from __future__ import annotations

import pytest

from routers import _airs_portfolio_analysis as pa


class TestTheOneDefinition:
    """`_airs_position_return` is the single formula behind every AIRS-sourced figure on the
    screen — this book's rows, a directly-held leg, and a leg valued by a wrapped book."""

    def test_a_position_with_no_opening_value_has_no_return(self):
        # Undefined, never 0 — "not held when the window opened" is not "did not move".
        assert pa._airs_position_return({"start_value_eur": 0, "current_value_eur": 100}) is None
        assert pa._airs_position_return({"start_value_eur": None, "current_value_eur": 100}) is None
        assert pa._airs_position_return({"start_value_eur": 100, "current_value_eur": None}) is None
        assert pa._airs_position_return(None) is None

    def test_the_withholding_is_ADDED_because_airs_books_it_negative(self):
        # gross 10 + tax -1.5 = 8.5 net on a 100 -> 110 position: (110 + 8.5)/100 - 1 = 18.5%.
        # Writing the intuitive `- tax` overstates every foreign holding by twice the withholding.
        assert pa._airs_position_return(
            {"start_value_eur": 100, "current_value_eur": 110}, 10 - 1.5) == pytest.approx(18.5)

    def test_a_plain_value_change_when_nothing_was_paid_out(self):
        assert pa._airs_position_return(
            {"start_value_eur": 490.0, "current_value_eur": 500.0}) == pytest.approx(
                100 * (500.0 / 490.0 - 1))


class TestTheLadder:
    """Four rungs, and the price series is the fourth."""

    def _wire(self, monkeypatch, *, pre, post, wrapped=None, marks=None):
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
        monkeypatch.setattr(hisin, "resolve_account_isins", lambda p: {"rows": pre})
        monkeypatch.setattr(pa, "_expand_book_rows", lambda rows: post)
        # The wrapped books are stubbed here; they have their own tests below and their own DB hops.
        monkeypatch.setattr(pa, "_wrapped_book_marks", lambda ids: dict(wrapped or {}))
        monkeypatch.setattr(accounts, "_direct_result", lambda pf, names: ({}, {}))
        monkeypatch.setattr("routers._airs_lookthrough._datum_of", lambda pid: None)
        monkeypatch.setattr("routers._airs_portfolio_perf.compute_holding_marks",
                            lambda isins, anchor, **kw: dict(marks or {}))
        monkeypatch.setattr(pa, "_grid", lambda isins: {})
        monkeypatch.setattr(pa, "_country_by_code", lambda: {})

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
                  "current_value_eur": 111.74, "asset_class": "Equity"}],
            post=[{"isin": "US1", "holding_name": "Fortinet", "start_value_eur": 100.0,
                   "current_value_eur": 111.74, "asset_class": "Equity", "bucket": "Equity",
                   "via_names": [], "sources": self._direct(111.74, 100.0)}],
        )
        h = self._row(pa._book_port_items(7, {}), "US1")
        assert h["own_return_pct"] == pytest.approx(11.74)
        assert h["own_return_source"] == "airs"
        assert h["own_return_book"] == "X_DYN"

    def test_a_split_holding_is_BOTH_legs_weighted_by_opening_value(self, monkeypatch):
        """The MasterCard case, in round numbers: 490 held outright at +2.04% and 106 through the
        certificate at +17.62%. Neither leg alone is the position's return, and before this the
        whole row went to yfinance because SOME of it arrives wrapped."""
        self._wire(
            monkeypatch,
            pre=[
                # held outright — the clean valuation, and it exists ONLY before the expansion
                {"isin": "US1", "holding_name": "MasterCard", "start_value_eur": 490.0,
                 "current_value_eur": 500.0, "asset_class": "Equity"},
                {"isin": "CH1", "holding_name": "Cert", "start_value_eur": 106.0,
                 "current_value_eur": 100.0, "asset_class": "Equity", "linked_portfolio_id": 99},
            ],
            # ⚠ the merged row: direct + the certificate's proportional slice, whose half carries
            # the CERTIFICATE's -5.7%. Reading the instrument's return off THIS is the trap.
            post=[{"isin": "US1", "holding_name": "MasterCard", "start_value_eur": 596.0,
                   "current_value_eur": 600.0, "asset_class": "Equity", "bucket": "Equity",
                   "via_names": ["Star"],
                   "sources": [*self._direct(500.0, 490.0), *self._via(100.0, 106.0)]}],
            wrapped={99: {"US1": {"return_pct": 17.62, "as_of": "2026-07-30",
                                  "portefeuille": "Star_DYN", "income_eur": None}}},
            marks={"US1": {"return_pct": 2.71, "end_date": "2026-07-31"}},
        )
        h = self._row(pa._book_port_items(7, {}), "US1")
        direct_ret = 100 * (500.0 / 490.0 - 1)
        expected = 100 * ((490.0 * (1 + direct_ret / 100) + 106.0 * 1.1762) / 596.0 - 1)
        assert h["own_return_pct"] == pytest.approx(expected)
        assert h["own_return_pct"] != pytest.approx(direct_ret)      # not the direct leg alone
        assert h["own_return_pct"] != pytest.approx(17.62)           # nor the wrapped one
        assert h["own_return_source"] == "airs"
        # ⚠ NO SINGLE BOOK OWNS A BLEND. Naming one would credit the whole figure to a book that
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
        # ⚠ THE WRAPPED BOOK'S OWN SNAPSHOT, which trails this one (measured 5 days apart).
        # Stamping it with the parent's would age-check a number against a scan it never came from.
        assert h["own_return_as_of"] == "2026-07-30"

    def test_two_certificates_each_ask_their_OWN_book(self, monkeypatch):
        # ⚠ The marks are keyed by model, not flattened to one ISIN map: two strategies can both
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
        # ⚠ A BLEND IS ONLY AS FRESH AS ITS STALEST LEG.
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
        # ⚠ NO ARGUMENT, NO QUERY. Most books hold no certificate, and this runs on every open of
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
        # ⚠ Keyed by model. Flattened to one ISIN map, whichever book was read first would answer
        # for a leg that came through the other — two positions, two purchase dates, one figure.
        import routers._airs_account_links as links
        import routers._airs_holding_isin as hisin

        from routers import _airs_accounts as accounts

        monkeypatch.setattr(links, "list_account_links", lambda: {"accounts": [
            {"portefeuille": "A_DYN", "model_portfolio_id": 1},
            {"portefeuille": "B_DYN", "model_portfolio_id": 2}]})
        monkeypatch.setattr(hisin, "resolve_account_isins", lambda pf: {
            "as_of": "2026-07-30",
            "rows": [{"isin": "US1", "holding_name": "NVIDIA", "start_value_eur": 100.0,
                      "current_value_eur": 110.0 if pf == "A_DYN" else 150.0}]})
        monkeypatch.setattr(accounts, "account_holdings", lambda pf: {"rows": []})
        out = pa._wrapped_book_marks({1, 2})
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
        monkeypatch.setattr(hisin, "resolve_account_isins", lambda pf: {
            "as_of": "2026-07-30",
            "rows": [{"isin": "CASH1", "holding_name": "Effectenrekening",
                      "start_value_eur": 0.0, "current_value_eur": 31072.23},
                     {"isin": "US1", "holding_name": "NVIDIA", "start_value_eur": 100.0,
                      "current_value_eur": 110.0}]})
        monkeypatch.setattr(accounts, "account_holdings", lambda pf: {"rows": []})
        out = pa._wrapped_book_marks({1})
        assert "CASH1" not in out[1]
        assert out[1]["US1"]["return_pct"] == pytest.approx(10.0)

    def test_the_wrapped_books_own_journal_income_is_inside_its_figure(self, monkeypatch):
        # A leg that paid a dividend must not read lower than the identical instrument held
        # directly — the parent's rows carry their income, so these have to as well.
        import routers._airs_account_links as links
        import routers._airs_holding_isin as hisin

        from routers import _airs_accounts as accounts

        monkeypatch.setattr(links, "list_account_links", lambda: {
            "accounts": [{"portefeuille": "A_DYN", "model_portfolio_id": 1}]})
        monkeypatch.setattr(hisin, "resolve_account_isins", lambda pf: {
            "as_of": "2026-07-30",
            "rows": [{"isin": "US1", "holding_name": "Shell", "start_value_eur": 100.0,
                      "current_value_eur": 110.0}]})
        monkeypatch.setattr(accounts, "account_holdings", lambda pf: {
            "rows": [{"holding_name": "Shell", "dividend_eur": 10.0,
                      "dividend_tax_eur": -1.5}]})       # ⚠ AIRS books the withholding negative
        out = pa._wrapped_book_marks({1})
        assert out[1]["US1"]["return_pct"] == pytest.approx(18.5)
        assert out[1]["US1"]["income_eur"] == pytest.approx(8.5)
