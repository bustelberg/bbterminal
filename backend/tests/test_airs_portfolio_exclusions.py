"""Test-only AIRS TopSelecties stay distinct from the normal product set."""
from __future__ import annotations

from airs_portfolio_exclusions import include_portfolios, is_excluded_portfolio, topselectie_test_entry_for_account


def test_the_json_classifies_both_airs_books_as_test_topselecties():
    assert topselectie_test_entry_for_account("DealmakersTopSel OFF DYN") == {
        "dynamic_portefeuille": "DealmakersTopSel OFF DYN", "display_name": "DealmakersTopSelectie",
    }
    assert topselectie_test_entry_for_account("TolpoortenSelect OFF DYN") == {
        "dynamic_portefeuille": "TolpoortenSelect OFF DYN", "display_name": "TolpoortenTopSelectie",
    }


def test_test_topselecties_are_not_excluded_from_refresh_or_storage():
    assert not is_excluded_portfolio("TolpoortenSelect OFF DYN")
    assert not is_excluded_portfolio("AITopSelectie OFF DYN")


def test_filtering_a_model_scan_keeps_test_models_available_for_their_own_tab():
    rows = [{"name": "DealmakersTopSel OFF FX"}, {"name": "AITopSelectie OFF FX"}]
    assert include_portfolios(rows, lambda row: row["name"]) == rows
