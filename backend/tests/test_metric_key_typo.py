"""An unknown metric key is REFUSED, never answered with revenue.

⚠⚠ THE MEASURED BUG, 2026-08-17. `_metric_codes` was
`_METRIC_CODES.get(metric, _METRIC_CODES["revenue"])`, so a key the registry does not carry came
back as REVENUE. The Tables tab asked `portfolio-revenue-matrix?metric=fcf_per_share` — the
registry key is `fcf_ps` — and its row labelled "FCF / share CAGR" was therefore the book's
revenue growth. Bustelberg Offensief read **+19.0%/yr** there against the Long Equity FCF/share
card's **+28.0%** on the same book, the same window and the same modal.

⚠ WHY IT SURVIVED, WHICH IS THE PART WORTH REMEMBERING. Nothing errored, no cell was empty, and
both figures were entirely plausible for that book. The only symptom was that two numbers
disagreed — and the tab's own footnote offered a CREDIBLE WRONG EXPLANATION for the gap
("point-to-point will not match the card's trend fit"). Measured on the real series, that
explanation accounts for 0.5pp: the card's fit is 27.46%/yr and point-to-point on the SAME line is
28.00%. It could never have accounted for nine.

A default that is itself a valid, common series is the worst possible default: it converts a typo
into a different question, answered confidently, under the label of the question that was asked.
"""
from __future__ import annotations

import pytest

from routers.earnings import _METRIC_CODES, _metric_codes


class TestTheRegistryIsTheOnlyAnswer:

    def test_a_known_key_resolves(self):
        assert _metric_codes("fcf_ps")[0] == "annuals__Per Share Data__Free Cash Flow per Share"
        assert _metric_codes("revenue")[0] == "annuals__Income Statement__Revenue"

    def test_the_typo_that_caused_it_is_refused(self):
        with pytest.raises(ValueError, match="fcf_per_share"):
            _metric_codes("fcf_per_share")

    def test_the_error_names_the_valid_keys(self):
        """A refusal that does not say what WAS expected sends the reader to grep for the dict."""
        with pytest.raises(ValueError) as e:
            _metric_codes("free_cash_flow_per_share")
        assert "fcf_ps" in str(e.value)

    def test_it_does_not_fall_back_to_revenue(self):
        """⚠ THE ASSERTION THAT IS THE WHOLE FILE. Any exception is better than this equality."""
        with pytest.raises(ValueError):
            got = _metric_codes("not_a_metric")
            assert got != _METRIC_CODES["revenue"], (
                "an unknown key was answered with revenue — a typo is now a different series "
                "under the caller's own label")


class TestEveryKeyTheFrontendSends:
    """⚠ THE KEYS ARE TYPED AS STRING LITERALS IN TSX AND NOTHING CHECKS THEM AT BUILD TIME.

    `LongEquityTab`'s `CARDS[].benchmarkMetric` and `TablesTab`'s two matrix URLs are plain
    strings; a rename on either side compiles, deploys and renders. Until the fallback was removed
    it also produced numbers. These are the keys those files send today — a failure here means one
    side moved without the other.
    """

    @pytest.mark.parametrize("key", [
        "revenue", "fcf_ps", "shares", "eps_nri",         # LongEquityTab CARDS
        "eps_nri_estimate",                                # the forecast leg
        "fcf", "sbc", "net_income", "gross_profit", "roic",  # the derived cards' inputs
    ])
    def test_it_is_in_the_registry(self, key):
        assert key in _METRIC_CODES
        assert _metric_codes(key)

    def test_the_tables_tab_asks_for_a_real_metric(self):
        """The exact two keys `TablesTab` puts in its query strings."""
        for key in ("fcf_ps", "eps_nri"):
            assert _metric_codes(key)
