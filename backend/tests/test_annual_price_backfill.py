from datetime import date


def test_daily_history_fills_missing_annual_price_years_without_replacing_reported_ones():
    from ingest.prices import _annual_price_rows

    rows = _annual_price_rows(
        352,
        [
            (date(2015, 12, 30), 74.0),
            (date(2015, 12, 31), 75.0),
            (date(2016, 12, 29), 80.0),
            (date(2016, 12, 30), 81.0),
            (date(2017, 12, 29), 90.0),
        ],
        {2017},  # GuruFocus reported FY2017 onward, but not FY2015/2016.
    )

    assert [(row["target_date"], row["numeric_value"]) for row in rows] == [
        ("2015-12-31", 75.0),
        ("2016-12-30", 81.0),
    ]
