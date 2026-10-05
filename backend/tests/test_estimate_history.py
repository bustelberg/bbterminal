"""Historical earnings-surprise rows stored from GuruFocus estimate_history."""
from __future__ import annotations

from ingest.earnings.estimate_history import _parse_estimate_history


def test_quarterly_history_preserves_consensus_and_surprise_fields():
    rows = _parse_estimate_history({
        "quarterly": {
            "revenue_estimate": {
                "202608": {
                    "actual": "6760",
                    "surprisemean": "6699.413",
                    "difference": "60.587",
                    "surprise_pct": "0.9",
                },
            },
            "eps_nri_estimate": {
                "202608": {
                    "actual": 6.13,
                    "surprisemean": 6.087,
                    "difference": 0.043,
                    "surprise_pct": 0.71,
                },
            },
        },
    }, company_id=42)

    values = {row["metric_code"]: row for row in rows}
    assert values["quarterly_estimate_history__revenue_estimate__consensus"]["numeric_value"] == 6699.413
    assert values["quarterly_estimate_history__revenue_estimate__difference"]["numeric_value"] == 60.587
    assert values["quarterly_estimate_history__eps_nri_estimate__surprise_pct"]["numeric_value"] == 0.71
    assert {row["target_date"] for row in rows} == {"2026-08-31"}
    assert {row["company_id"] for row in rows} == {42}


def test_parser_ignores_unknown_metrics_and_invalid_periods():
    assert _parse_estimate_history({
        "quarterly": {
            "ebit_estimate": {"202608": {"actual": 1}},
            "revenue_estimate": {"not-a-date": {"actual": 1}},
        },
    }, company_id=42) == []
