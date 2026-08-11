"""WHERE a holding came from, and HOW MUCH came each way.

`via_names` names the routes into a position but cannot size them, and unsized they mislead in the
one direction that matters. Measured on BUS_Offensief_Dyn -> model 1935: MasterCard is EUR 50,489
of the book's OWN shares against EUR 1,991 (3.8%) reached through the Star Selection Index
certificate — and a row chipped only "StarTopSelectie Offensief" reads as a position the book does
not hold itself. Three of that book's 52 rows have more than one route in.

⚠ THE PERCENTAGES ARE SHARES OF THE BOOK, NOT OF THE ROW, so they add up to the holding's
`weight_now_pct` — the column printed beside them. A share of the row ("3.8% of this position") is
a different question that ties to nothing else on screen; it belongs in a tooltip, not in a figure
sitting next to a weight it does not reconcile with.
"""
from __future__ import annotations

import pytest

from routers import _airs_portfolio_analysis as pa
from routers._airs_lookthrough import merge_by_isin


class TestWeighSources:
    def test_the_routes_sum_to_the_holdings_weight(self):
        # 50,489 direct + 1,991 via, over a 1,245,014 book -> 4.0553% + 0.1599% = 4.2152%.
        out = pa._weigh_sources(
            [{"label": None, "model_id": None, "value_eur": 50488.89, "start_value_eur": 49557.24},
             {"label": "StarTopSelectie Offensief", "model_id": 2094,
              "value_eur": 1990.55, "start_value_eur": 2118.97}], 1245014.26)
        assert sum(s["weight_now_pct"] for s in out) == pytest.approx(
            100 * (50488.89 + 1990.55) / 1245014.26)
        assert out[0]["label"] is None and out[0]["weight_now_pct"] == pytest.approx(4.0553, abs=1e-4)
        assert out[1]["weight_now_pct"] == pytest.approx(0.1599, abs=1e-4)

    def test_largest_route_first(self):
        out = pa._weigh_sources(
            [{"label": "Star", "model_id": 1, "value_eur": 10.0},
             {"label": None, "model_id": None, "value_eur": 90.0}], 100.0)
        assert [s["label"] for s in out] == [None, "Star"]

    def test_two_routes_through_the_same_strategy_are_one_chip(self):
        # A book can reach an instrument through two certificates wrapping the SAME strategy. Two
        # chips with one name and two different percentages is a puzzle, not a breakdown.
        out = pa._weigh_sources(
            [{"label": "Star", "model_id": 1, "value_eur": 30.0, "start_value_eur": 25.0},
             {"label": "Star", "model_id": 1, "value_eur": 20.0, "start_value_eur": 15.0}], 100.0)
        assert len(out) == 1
        assert out[0]["weight_now_pct"] == pytest.approx(50.0)
        assert out[0]["start_value_eur"] == pytest.approx(40.0)

    def test_two_DIFFERENT_strategies_stay_apart_even_under_one_name(self):
        # ⚠ Keyed by (label, model). Two models can share a display name, and merging them would
        # ask ONE book to answer for a leg that came through the other.
        out = pa._weigh_sources(
            [{"label": "Top", "model_id": 1, "value_eur": 30.0},
             {"label": "Top", "model_id": 2, "value_eur": 20.0}], 100.0)
        assert len(out) == 2

    def test_a_route_carrying_nothing_is_not_a_route(self):
        out = pa._weigh_sources(
            [{"label": None, "model_id": None, "value_eur": 100.0},
             {"label": "Star", "model_id": 1, "value_eur": 0.0}], 100.0)
        assert [s["label"] for s in out] == [None]

    def test_an_empty_book_does_not_divide_by_zero(self):
        assert pa._weigh_sources([{"label": None, "value_eur": 5.0}], 0.0) == []
        assert pa._weigh_sources(None, 100.0) == []


class TestBlendRoutes:
    """One position reached two ways is still one position, and either leg alone misrepresents it."""

    # The measured MasterCard row: 95.90% of the opening value held outright at +2.14% (this
    # book's own valuation) and 4.10% through the Star certificate at +17.62% (StarTopSelectie's).
    MC = [{"label": None, "start_value_eur": 49557.24, "return_pct": 2.1373,
           "book": "BUS_Offensief_Dyn"},
          {"label": "StarTopSelectie Offensief", "start_value_eur": 2118.97, "return_pct": 17.6190,
           "book": "StarTopSelectie OFF DYN"}]

    def test_the_blend_is_weighted_by_opening_value(self):
        ret, books = pa._blend_routes([dict(r) for r in self.MC])
        assert ret == pytest.approx(2.7722, abs=1e-3)
        assert books == ["BUS_Offensief_Dyn", "StarTopSelectie OFF DYN"]

    def test_neither_leg_alone_is_the_answer(self):
        ret, _ = pa._blend_routes([dict(r) for r in self.MC])
        assert ret != pytest.approx(2.1373, abs=1e-2)     # the direct leg
        assert ret != pytest.approx(17.6190, abs=1e-2)    # the wrapped leg

    def test_todays_value_is_NOT_what_weights_it(self):
        # A leg that rose carries a bigger share of the position now than it held while it was
        # rising. Weighting by current value would read differently — and higher.
        rows = [dict(r) for r in self.MC]
        ret, _ = pa._blend_routes(rows)
        now = [49557.24 * 1.021373, 2118.97 * 1.176190]
        by_now = sum(n * r["return_pct"] for n, r in zip(now, self.MC, strict=True)) / sum(now)
        assert by_now > ret

    def test_each_leg_is_stamped_with_the_share_it_spoke_for(self):
        rows = [dict(r) for r in self.MC]
        pa._blend_routes(rows)
        assert rows[0]["blend_weight_pct"] == pytest.approx(95.90, abs=1e-2)
        assert rows[1]["blend_weight_pct"] == pytest.approx(4.10, abs=1e-2)
        assert sum(r["blend_weight_pct"] for r in rows) == pytest.approx(100.0)

    def test_a_leg_with_no_return_leaves_BOTH_sides_of_the_average(self):
        # ⚠ Not counted as 0%. Dropping it from the numerator alone would dilute the answer toward
        # zero by exactly the weight of the leg we could not value — a plausible, wrong number.
        rows = [{"label": None, "start_value_eur": 100.0, "return_pct": 10.0, "book": "A"},
                {"label": "Star", "start_value_eur": 100.0, "return_pct": None, "book": None}]
        ret, books = pa._blend_routes(rows)
        assert ret == pytest.approx(10.0)
        assert books == ["A"]
        # ...and the leg that did not speak says so, rather than showing a 50% it never carried.
        assert rows[1]["blend_weight_pct"] is None
        assert rows[0]["blend_weight_pct"] == pytest.approx(100.0)

    def test_a_leg_with_no_opening_value_cannot_be_weighted(self):
        # Bought after the window opened: it has a real position but no share of the opening value.
        rows = [{"label": None, "start_value_eur": 100.0, "return_pct": 10.0, "book": "A"},
                {"label": "Star", "start_value_eur": 0.0, "return_pct": 50.0, "book": "B"}]
        ret, books = pa._blend_routes(rows)
        assert ret == pytest.approx(10.0)
        assert books == ["A"]

    def test_no_valuable_leg_at_all_returns_none_so_the_caller_falls_back(self):
        ret, books = pa._blend_routes(
            [{"label": None, "start_value_eur": 0.0, "return_pct": None, "book": None}])
        assert ret is None and books == []


class TestTheRoutesSurviveTheMerge:
    """`merge_by_isin` is what turns three rows for one instrument into one — and it has to carry
    the provenance across, or the split is lost exactly where it is needed."""

    def test_direct_and_wrapped_legs_are_concatenated_not_overwritten(self):
        merged = merge_by_isin(
            [{"isin": "US1", "current_value_eur": 50488.89, "via_names": [],
              "sources": [{"label": None, "model_id": None, "value_eur": 50488.89}]},
             {"isin": "US1", "current_value_eur": 1990.55, "via_names": ["Star"],
              "sources": [{"label": "Star", "model_id": 2094, "value_eur": 1990.55}]}],
            fields=("current_value_eur",))
        assert len(merged) == 1
        assert [s["label"] for s in merged[0]["sources"]] == [None, "Star"]
        # The value the routes describe is the value that was merged — same total, both ways.
        assert sum(s["value_eur"] for s in merged[0]["sources"]) == pytest.approx(
            merged[0]["current_value_eur"])

    def test_a_caller_that_never_set_sources_does_not_grow_an_empty_one(self):
        # `merge_by_isin` is shared with the MODEL side, which merges `percentage` and has no
        # sources at all. It must not start emitting a key that side never asked for.
        merged = merge_by_isin(
            [{"isin": "US1", "percentage": 3.0}, {"isin": "US1", "percentage": 1.0}],
            fields=("percentage",))
        assert merged[0]["percentage"] == pytest.approx(4.0)
        assert "sources" not in merged[0]


class TestTheExpansionStampsTheRoute:
    """Stamped where the split happens, because that is the only place that still knows."""

    def _wire(self, monkeypatch, child):
        monkeypatch.setattr("routers._airs_lookthrough._datum_of", lambda pid: None)
        monkeypatch.setattr("routers._airs_lookthrough._positions_of", lambda pid, d: child)
        monkeypatch.setattr(pa, "_reclassify_book_rows", lambda rows: rows)

    def test_a_directly_held_row_is_labelled_direct(self, monkeypatch):
        self._wire(monkeypatch, [])
        out = pa._expand_book_rows(
            [{"isin": "US1", "holding_name": "ASML", "current_value_eur": 100.0,
              "start_value_eur": 90.0}])
        assert out[0]["sources"] == [{"label": None, "model_id": None,
                                      "value_eur": 100.0, "start_value_eur": 90.0}]

    def test_a_certificate_stamps_each_leg_with_the_strategy_its_slice_and_its_model(
            self, monkeypatch):
        self._wire(monkeypatch, [{"isin": "US1", "fonds": "MasterCard", "percentage": 4.0},
                                 {"isin": "US2", "fonds": "Shopify", "percentage": 96.0}])
        out = pa._expand_book_rows(
            [{"isin": "CH1", "holding_name": "Cert", "current_value_eur": 49763.68,
              "start_value_eur": 52974.24, "linked_portfolio_id": 99,
              "linked_portfolio_name": "StarTopSelectie Offensief"}])
        mc = next(r for r in out if r["isin"] == "US1")
        assert mc["sources"] == [{"label": "StarTopSelectie Offensief", "model_id": 99,
                                  "value_eur": pytest.approx(49763.68 * 0.04),
                                  "start_value_eur": pytest.approx(52974.24 * 0.04)}]
        # ⚠ Value is conserved on BOTH ends: the routes across every leg still add to the
        # certificate. The opening value has to travel too, or the leg's return is computed
        # against a base that does not exist.
        assert sum(s["value_eur"] for r in out for s in r["sources"]) == pytest.approx(49763.68)
        assert sum(s["start_value_eur"] for r in out for s in r["sources"]) == pytest.approx(52974.24)

    def test_a_certificate_with_nothing_behind_it_is_a_DIRECT_route(self, monkeypatch):
        # It is left whole, so the book holds IT — labelling the row with the strategy it wraps
        # would claim a look-through that did not happen.
        self._wire(monkeypatch, [])
        out = pa._expand_book_rows(
            [{"isin": "CH1", "holding_name": "Cert", "current_value_eur": 100.0,
              "start_value_eur": 106.0, "linked_portfolio_id": 99,
              "linked_portfolio_name": "Star"}])
        assert out[0]["sources"] == [{"label": None, "model_id": None,
                                      "value_eur": 100.0, "start_value_eur": 106.0}]
        assert out[0]["via_names"] == []
