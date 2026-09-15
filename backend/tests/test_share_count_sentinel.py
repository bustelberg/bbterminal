"""A zero diluted-share count is missing data, never a real level."""

from ingest.earnings.financials import _parse_financials
from routers.earnings import _latest_per_year, _normalise_share_count_rows


_CODE = "annuals__Income Statement__Shares Outstanding (Diluted Average)"


def test_financials_parser_turns_vendor_zero_share_count_into_missing() -> None:
    rows = _parse_financials({
        "financials": {"annuals": {
            "Fiscal Year": ["2020-12", "2021-12"],
            "Income Statement": {
                "Shares Outstanding (Diluted Average)": ["0", "299.1"],
            },
        }},
    }, company_id=1)

    values = {row["target_date"]: row["numeric_value"] for row in rows
              if row["metric_code"] == _CODE}
    assert values == {"2020-12-31": None, "2021-12-31": 299.1}


def test_existing_zero_share_count_is_ignored_before_the_next_refresh() -> None:
    rows = [
        {"metric_code": _CODE, "target_date": "2020-12-31", "numeric_value": 0},
        {"metric_code": _CODE, "target_date": "2021-12-31", "numeric_value": 299.1},
    ]

    assert _latest_per_year(rows) == {"2021": 299.1}


def test_thousand_fold_share_count_unit_break_is_normalised() -> None:
    rows = [
        {"metric_code": _CODE, "target_date": "2017-12-31", "numeric_value": 1.221038},
        {"metric_code": _CODE, "target_date": "2018-12-31", "numeric_value": 1.175538},
        {"metric_code": _CODE, "target_date": "2019-12-31", "numeric_value": 1123.536},
        {"metric_code": _CODE, "target_date": "2020-12-31", "numeric_value": 1105.0},
        {"metric_code": _CODE, "target_date": "2021-12-31", "numeric_value": 1080.0},
    ]

    assert _latest_per_year(rows) == {
        "2017": 1221.038,
        "2018": 1175.538,
        "2019": 1123.536,
        "2020": 1105.0,
        "2021": 1080.0,
    }


def test_parser_normalises_a_nonzero_thousand_fold_share_count_unit_break() -> None:
    rows = _parse_financials({
        "financials": {"annuals": {
            "Fiscal Year": ["2019-12", "2020-12", "2021-12", "2022-12"],
            "Income Statement": {
                "Shares Outstanding (Diluted Average)": ["209.462", "0.262523", "283.222", "297.919"],
            },
        }},
    }, company_id=1)

    values = {row["target_date"]: row["numeric_value"] for row in rows
              if row["metric_code"] == _CODE}
    assert values["2020-12-31"] == 262.523


def test_raw_benchmark_rows_are_normalised_before_blending() -> None:
    rows = [
        {"company_id": 1, "metric_code": _CODE, "target_date": "2019-12-31", "numeric_value": 209.462},
        {"company_id": 1, "metric_code": _CODE, "target_date": "2020-12-31", "numeric_value": 0.262523},
        {"company_id": 1, "metric_code": _CODE, "target_date": "2021-12-31", "numeric_value": 283.222},
        {"company_id": 1, "metric_code": _CODE, "target_date": "2022-12-31", "numeric_value": 297.919},
    ]

    _normalise_share_count_rows(rows)
    assert rows[1]["numeric_value"] == 262.523
