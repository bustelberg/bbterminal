"""Supplying a holding's ISIN by hand, when its model portfolio has no position for it.

⚠ NO AMOUNT OF MATCHING CAN FIND AN ISIN THAT IS NOT IN THE DATA. Measured 2026-07-23: AIRS's
Fixed portfolios hold `Invesco Wld EW ETF Acc` (IE000OEF25S1); our newest available snapshot
(positions_datum 2025-04-28) still holds `Ish DJS GSD 100` (DE000A0F5UH1). The book's Invesco
holding therefore had nothing to pair with, and a 1:1 assignment cannot say "none" — it took the
iShares position and published DE000A0F5UH1 as the answer, in all four BUS_* books.

The fixture below is that case with the real numbers (implied EUR 5.887/unit against MWEP.L's
EUR 5.8959 close), driven against the in-memory fake Supabase.
"""
from __future__ import annotations

import pytest

from routers import _airs_holding_isin as hi
from tests._fake_supabase import FakeSupabase

AS_OF = "2026-07-22"
INVESCO = "Invesco World Equal Weight ETF Acc"
RIGHT = "IE000OEF25S1"          # what AIRS's Fixed portfolio actually holds today
LEFTOVER = "DE000A0F5UH1"       # the stale snapshot's `Ish DJS GSD 100`


def _tables(*, override: dict | None = None, model_has_invesco: bool = False,
            book_isin: str | None = None) -> dict:
    pos = [
        {"portfolio_id": 1, "fonds": "ASML Holding", "isin": "NL0010273215",
         "percentage": 5.02, "categorie": "AAND", "sector": None},
        {"portfolio_id": 1, "fonds": "Ish DJS GSD 100", "isin": LEFTOVER,
         "percentage": 2, "categorie": "AAND", "sector": None},
    ]
    if model_has_invesco:
        pos.append({"portfolio_id": 1, "fonds": "Invesco Wld EW ETF Acc", "isin": RIGHT,
                    "percentage": 2.5, "categorie": "AAND", "sector": None})
    return {
        "airs_holding": [
            {"portefeuille": "BOOK", "as_of_date": AS_OF, "holding_name": "ASML Holding",
             "quantity": 10, "currency": "EUR", "weight": 0.05,
             "current_value_eur": 1000.0, "start_value_eur": 900.0, "ytd_return_eur": 100.0},
            {"portefeuille": "BOOK", "as_of_date": AS_OF, "holding_name": INVESCO,
             "isin": book_isin,     # AIRS's own `ISIN-code`; None on a pre-2026-07-23 snapshot
             "quantity": 100, "currency": "EUR", "weight": 0.025,
             "current_value_eur": 588.7, "start_value_eur": 500.0, "ytd_return_eur": 88.7},
        ],
        "airs_model_portfolio_position": pos,
        "airs_holding_isin_override": [override] if override else [],
        # RIGHT is a fund (grid asset_class 'etf'), which is what lets the Class resolve.
        "asset_grid": [
            {"isin": "NL0010273215", "name": "ASML Holding NV", "asset_class": "equity",
             "sector": "Technology", "country": "Netherlands", "msci_region": "Europe"},
            {"isin": RIGHT, "name": "Invesco MSCI World Equal Weight UCITS ETF Acc",
             "asset_class": "etf", "sector": "etf", "country": "United Kingdom",
             "msci_region": "Europe"},
            {"isin": LEFTOVER, "name": "iShares STOXX Global Select Dividend 100",
             "asset_class": "etf", "sector": "etf", "country": "Germany",
             "msci_region": "Europe"},
        ],
        "asset_bucket_override": [],
    }


@pytest.fixture
def wire(monkeypatch):
    """Everything except the pairing itself is stubbed: the point under test is which ISIN each
    holding ends up with, not how prices or FX are fetched."""
    def _go(**kw):
        fake = FakeSupabase(_tables(**kw))
        monkeypatch.setattr(hi, "supabase", fake)
        monkeypatch.setattr(hi, "_fx_to_eur", lambda *a, **k: {})     # everything is EUR here
        closes = {
            "NL0010273215": {"close": 100.0, "currency": "EUR", "date": AS_OF,
                             "name": "ASML Holding NV"},
            # The real MWEP.L close, pence already converted: 503.10 GBp / (100 x 0.85333).
            RIGHT: {"close": 5.8959, "currency": "EUR", "date": AS_OF,
                    "name": "INVESCO MARKETS II PLC IVZ MSCI"},
            LEFTOVER: {"close": 39.02, "currency": "EUR", "date": AS_OF,
                       "name": "iShares STOXX Global Select Dividend 100"},
        }
        monkeypatch.setattr(hi, "_last_closes",
                            lambda isins, as_of: {i: c for i, c in closes.items() if i in isins})
        import routers._airs_account_links as links
        monkeypatch.setattr(links, "list_account_links", lambda: {"accounts": [
            {"portefeuille": "BOOK", "model_portfolio_id": 1, "model_name": "MODEL_FX",
             "source": "manual", "reason": "test"}]})
        return hi.resolve_account_isins("BOOK")
    return _go


def _row(res, name):
    return next(r for r in res["rows"] if r["holding_name"] == name)


class TestWithoutAPinTheAssignmentPublishesTheLeftover:
    """The bug as measured. Kept as a test so the guard cannot be removed silently."""

    def test_the_holding_is_refused_rather_than_given_the_wrong_isin(self, wire):
        r = _row(wire(), INVESCO)
        assert r["isin"] is None, "an ISIN two signals reject must not be published"
        assert r["verdict"] == "unmatched"
        # It still NAMES what it declined — a blank here would read as "this line has no ISIN".
        assert r["rejected_isin"] == LEFTOVER
        assert r["rejected_fonds"] == "Ish DJS GSD 100"

    def test_the_position_the_book_does_not_hold_is_reported_as_such(self, wire):
        assert [u["isin"] for u in wire()["unmatched_model_positions"]] == [LEFTOVER]


class TestAPinDecidesIdentityAndNothingElse:
    PIN = {"holding_name": INVESCO, "isin": RIGHT, "note": "AIRS Fixed lists it; snapshot is old"}

    def test_the_pinned_isin_is_used(self, wire):
        r = _row(wire(override=self.PIN), INVESCO)
        assert r["isin"] == RIGHT
        assert r["isin_overridden"] is True
        assert r["isin_override_note"]

    def test_it_is_still_price_checked_not_simply_trusted(self, wire):
        """⚠ A human typing an ISIN is not evidence about what the book holds. Implied
        EUR 5.887/unit vs MWEP.L's EUR 5.8959 — ratio 0.9985, confirmed independently."""
        r = _row(wire(override=self.PIN), INVESCO)
        assert r["verdict"] == "ok"
        assert r["price_ratio"] == pytest.approx(0.9985, abs=5e-4)

    def test_a_wrong_pin_is_contradicted_rather_than_believed(self, wire):
        bad = {"holding_name": INVESCO, "isin": LEFTOVER, "note": None}
        r = _row(wire(override=bad), INVESCO)
        assert r["isin_overridden"] is True
        assert r["verdict"] == "price_mismatch", "a hand-typed ISIN gets no free pass"

    def test_a_pinned_holding_does_not_consume_a_model_position(self, wire):
        """⚠ THE WHOLE POINT. Left in the 1:1 assignment it would still take the leftover, and
        that position would then read as held — hiding the drift the pin exists to expose."""
        res = wire(override=self.PIN)
        assert [u["isin"] for u in res["unmatched_model_positions"]] == [LEFTOVER]
        assert _row(res, INVESCO)["model_fonds"] is None

    def test_the_class_resolves_once_the_instrument_is_known(self, wire):
        """Unpaired means no AIRS `categorie`, so the Class falls back to the grid — a fund with
        no bond tell is an equity ETF. Before the pin there was no instrument to ask at all."""
        assert _row(wire(), INVESCO)["bucket"] == hi.BUCKET_UNKNOWN
        assert _row(wire(override=self.PIN), INVESCO)["bucket"] == hi.BUCKET_EQUITY_ETF

    def test_a_score_is_not_invented_for_a_row_nothing_scored(self, wire):
        """A 0.0 here would render as 'matched at zero confidence', which is a different claim
        from 'not matched at all'."""
        r = _row(wire(override=self.PIN), INVESCO)
        assert r["name_score"] is None and r["weak_name"] is None

    def test_pinning_a_position_the_model_DOES_have_invents_no_drift(self, wire):
        """The pin claims the position by ISIN, so it is not also reported as 'not held here' —
        drift that is not there, invented by the fix for drift that is."""
        res = wire(override=self.PIN, model_has_invesco=True)
        assert [u["isin"] for u in res["unmatched_model_positions"]] == [LEFTOVER]

    def test_the_other_holdings_pair_exactly_as_before(self, wire):
        for res in (wire(), wire(override=self.PIN)):
            asml = _row(res, "ASML Holding")
            assert asml["isin"] == "NL0010273215" and asml["verdict"] == "ok"


class TestTheBooksOwnIsinEndsTheGuessing:
    """AIRS's Vermogensoverzicht gained an `ISIN-code` column on 2026-07-23. Where a holding
    carries one there is nothing to infer: no scoring, no 1:1 assignment, no leftover to place —
    so none of the failures the name route needs guards against can happen at all."""

    def test_the_books_own_isin_is_used_and_named_as_exact(self, wire):
        r = _row(wire(book_isin=RIGHT), INVESCO)
        assert r["isin"] == RIGHT
        assert r["isin_source"] == "book"
        assert r["isin_overridden"] is False

    def test_it_beats_the_stale_models_leftover_with_no_pin_needed(self, wire):
        """The whole incident, undone by the column: the same fixture that published DE000A0F5UH1
        now resolves exactly, and the position the book does not hold is reported as such."""
        res = wire(book_isin=RIGHT)
        assert _row(res, INVESCO)["isin"] == RIGHT
        assert _row(res, INVESCO)["verdict"] == "ok"
        assert [u["isin"] for u in res["unmatched_model_positions"]] == [LEFTOVER]

    def test_an_exact_isin_outranks_a_hand_pin(self, wire):
        """⚠ A pin exists only because the book had no ISIN. Once it does, AIRS's own value is
        the stronger source — a stale pin must not shadow it."""
        stale = {"holding_name": INVESCO, "isin": LEFTOVER, "note": "set before the column existed"}
        r = _row(wire(override=stale, book_isin=RIGHT), INVESCO)
        assert r["isin"] == RIGHT and r["isin_source"] == "book"

    def test_a_known_isin_is_never_discarded_on_a_score_that_was_never_computed(self, wire):
        """⚠ THE TRAP IN THIS REFACTOR. `pairing_refused` reads `name_score`, which is 0.0 when
        nothing was scored. Applied to an exactly-joined row it would throw away a KNOWN ISIN
        because a price disagreed — inverting the fix it was written to be."""
        # LEFTOVER's own price (39.02) is nothing like the implied 5.887, so the verdict is a
        # mismatch — and the ISIN must survive it, because the book itself stated it.
        r = _row(wire(book_isin=LEFTOVER), INVESCO)
        assert r["verdict"] == "price_mismatch"
        assert r["isin"] == LEFTOVER, "an exact ISIN is not a pairing and cannot be refused"
        assert r["isin_source"] == "book"

    def test_no_score_is_invented_for_a_row_nothing_scored(self, wire):
        r = _row(wire(book_isin=RIGHT), INVESCO)
        assert r["name_score"] is None and r["weak_name"] is None

    def test_the_model_position_is_still_joined_for_its_category(self, wire):
        """The ISIN settles identity; AIRS's `categorie` still comes from the model, exactly."""
        r = _row(wire(book_isin=RIGHT, model_has_invesco=True), INVESCO)
        assert r["categorie"] == "AAND"
        assert r["bucket"] == hi.BUCKET_EQUITY_ETF     # AAND + a fund wrapper
        assert r["model_fonds"] == "Invesco Wld EW ETF Acc"

    def test_a_holding_the_model_lacks_still_resolves_fully(self, wire):
        """No model position means no `categorie` — and that is now a detail, not a dead end:
        the instrument is known, so the Class falls back to the grid."""
        r = _row(wire(book_isin=RIGHT), INVESCO)
        assert r["categorie"] is None and r["model_fonds"] is None
        assert r["bucket"] == hi.BUCKET_EQUITY_ETF

    def test_rows_without_one_still_take_the_name_route(self, wire):
        """⚠ THE FALLBACK MUST SURVIVE. Every pre-2026-07-23 snapshot has no ISIN, and the cash
        line never will. ASML carries none here and must still resolve by name."""
        asml = _row(wire(book_isin=RIGHT), "ASML Holding")
        assert asml["isin"] == "NL0010273215"
        assert asml["isin_source"] == "model"
        assert asml["name_score"] == 100.0
