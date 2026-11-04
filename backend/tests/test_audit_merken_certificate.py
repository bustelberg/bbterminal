import pytest

from scripts.audit_merken_certificate import attribute_month


@pytest.fixture
def month():
    def section(data):
        return {"status": "ok", "data": data}

    def valuation(day, *rows):
        return section({"as_of": day, "rows": rows})

    def position(name, isin, quantity, value):
        return {"name": name, "isin": isin, "currency": "EUR", "quantity": quantity, "value_eur": value}

    parent = {
        "opening": valuation("2026-08-31", position("MerkenTopSelectie Index", "WRAPPER", 20, 200)),
        "closing": valuation("2026-09-30", position("MerkenTopSelectie Index", "WRAPPER", 20, 214)),
        "transactions": {"status": "no_data"},
    }
    child = {
        "opening": valuation("2026-08-31", position("Equity", "STOCK", 10, 600),
                             position("Liquiditeiten", None, 400, 400)),
        "closing": valuation("2026-09-30", position("Equity", "STOCK", 10, 660),
                             position("Liquiditeiten", None, 410, 410)),
        "transactions": {"status": "no_data"},
    }
    performance = {"periode": "2026-09-30", "beginvermogen": 1000, "eindvermogen": 1070,
                   "rendement": 7, "beleggingsresultaat": 70, "stortingen": 0, "onttrekkingen": 0,
                   "opbrengsten": 10, "mutatie_opgelopen_rente": 0, "kosten": 0}
    return parent, child, performance


def test_verified_certificate_uses_child_results_and_separate_income(month):
    result = attribute_month("2026-09", "2026-09-30", *month)
    assert result["status"] == "matched"
    assert result["bottom_up_eur"] == result["certificate_value_change_eur"] == 14
    assert {row["name"]: row["result_eur"] for row in result["attribution"]} == pytest.approx({
        "Equity": 12, "Liquiditeiten": 0, "opbrengsten": 2, "mutatie_opgelopen_rente": 0,
    })


def test_month_does_not_allocate_unexplained_difference_to_constituents(month):
    month[0]["closing"]["data"]["rows"][0]["value_eur"] = 217
    result = attribute_month("2026-09", "2026-09-30", *month)
    assert result["status"] == "certificate_result_differs"
    assert result["difference_eur"] == 3
    assert result["attribution"] == []


def test_constant_opening_and_closing_units_do_not_mask_intramonth_trades(month):
    month[0]["transactions"] = {"status": "ok", "data": {"readable": True, "activity": [
        {"name": "MerkenTopSelectie Index", "kind": "sell"},
        {"name": "MerkenTopSelectie Index", "kind": "buy"},
    ]}}
    assert attribute_month("2026-09", "2026-09-30", *month)["status"] == "certificate_units_changed_within_month"


@pytest.mark.parametrize("wrapper_result", [14, 17])
def test_monthly_bundle_shows_underlying_contributions_only_if_they_tie(month, wrapper_result):
    from routers._airs_monthly_lookthrough import _constituents

    _, child, performance = month
    child["performance"] = {"status": "ok", "data": performance}
    wrapper = {
        "name": "MerkenTopSelectie Index", "isin": "WRAPPER", "status": "held",
        "opening_weight_pct": 10, "contribution_pp": wrapper_result / 20,
        "contribution_calculation": {
            "trades": [], "opening_date": "2026-08-31", "closing_date": "2026-09-30",
            "opening_value_eur": 200, "portfolio_opening_capital_eur": 2000, "result_eur": wrapper_result,
        },
    }
    rows = _constituents(wrapper, {"opening_weight_date": "2026-08-31", "closing_date": "2026-09-30"},
                         {"sections": child}, "2026-09")
    assert sum(row.get("contribution_pp", 0) for row in rows) == pytest.approx(wrapper["contribution_pp"])
    if wrapper_result == 14:
        assert next(row for row in rows if row["isin"] == "STOCK")["contribution_pp"] == pytest.approx(.6)
        assert next(row for row in rows if row.get("attribution_kind") == "opbrengsten")["contribution_pp"] == pytest.approx(.1)
    else:
        assert next(row for row in rows if row.get("attribution_kind") == "certificate")["contribution_pp"] == .85
        assert "contribution_pp" not in next(row for row in rows if row["isin"] == "STOCK")
