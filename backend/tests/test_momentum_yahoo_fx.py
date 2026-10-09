"""Yahoo quote currencies include minor units that ECB does not publish."""
from __future__ import annotations

import pandas as pd

from momentum.data.fx import convert_prices_to_eur


def test_gbp_pence_uses_gbp_rate_and_divides_price_by_100():
    prices = pd.DataFrame({
        "company_id": [1],
        "target_date": pd.to_datetime(["2026-01-02"]),
        "price": [1_250.0],
    })
    rates = {"GBp": pd.Series([0.8], index=pd.to_datetime(["2026-01-02"]))}

    actual, stats = convert_prices_to_eur(prices, {1: "GBp"}, rates)

    assert actual.loc[0, "price"] == 15.625
    assert stats["missing_currencies"] == []
