def test_daily_closes_fill_only_missing_annual_price_years():
    from routers.earnings import _annual_price_rows_from_daily

    rows = _annual_price_rows_from_daily(
        {352: {
            "2018-12-30": 74.0,
            "2018-12-31": 75.0,
            "2019-12-30": 81.0,
            "2020-12-29": 90.0,
        }},
        {352: [{"target_date": "2020-09-30", "numeric_value": 88.0}]},
    )

    assert [(r["target_date"], r["numeric_value"]) for r in rows] == [
        ("2018-12-31", 75.0),
        ("2019-12-30", 81.0),
    ]
