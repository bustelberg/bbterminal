"""Retired AIRS portfolios must not consume scan work or leak back from cached tables."""
from __future__ import annotations

from airs_portfolio_exclusions import include_portfolios, is_excluded_portfolio


def test_the_json_excludes_both_airs_books_and_reader_facing_aliases():
    for name in (
        "DealmakersTopSel OFF DYN",
        "DealmakersTopSel OFF FX",
        "DealmakersTopSelectie",
        "TolpoortenSelect OFF DYN",
        "TolpoortenSelect OFF FX",
        "TolpoortenSelectie",
        "TolpoortenTopSelectie",
    ):
        assert is_excluded_portfolio(name)


def test_matching_ignores_case_and_punctuation_without_hiding_other_topselecties():
    assert is_excluded_portfolio(" tolpoorten-topselectie ")
    assert not is_excluded_portfolio("AITopSelectie OFF DYN")


def test_filtering_a_model_scan_keeps_only_models_we_still_use():
    rows = [{"name": "DealmakersTopSel OFF FX"}, {"name": "AITopSelectie OFF FX"}]
    assert include_portfolios(rows, lambda row: row["name"]) == [rows[1]]
