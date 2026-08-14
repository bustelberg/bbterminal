"""The analysts' EPS consensus, as a metric the EPS card can draw a dotted continuation from.

⚠⚠ IT IS A FORECAST SHARING A CHART WITH MEASUREMENTS, which is the whole reason it needs pinning:
every safeguard here exists to stop it being read, blended or rolled as though someone had reported
it.
"""
from __future__ import annotations

from routers.earnings import (
    _FORECAST_BASE, _METRIC_CODES, _TTM_RULE, _codes_and_rule, _metric_codes,
)

EST = "eps_nri_estimate"


class TestItContinuesTheLineItForecasts:
    def test_it_is_anchored_on_this_cards_actual_series(self):
        """⚠⚠ THE ANCHOR IS WHAT STOPS A ~94% PHANTOM COLLAPSE. A level blend rebases each member to
        100 at its own first period; rebased independently, a forecast restarts at 100 beside an
        actual that has run to 1,800 and the chart shows an earnings collapse that exists only in
        the arithmetic. `_FORECAST_BASE` is what makes it a continuation."""
        assert _FORECAST_BASE[_metric_codes(EST)[0]] == _metric_codes("eps_nri")[0]

    def test_it_forecasts_the_WITHOUT_NRI_line_specifically(self):
        """⚠ GuruFocus publishes `annual_per_share_eps_estimate` beside it and the two agree to a
        cent on almost every company (Apple 8.76 vs 8.77) — so this cannot be checked by eye on one
        name. The card's actual is `EPS without NRI`; continuing it with an including-NRI consensus
        would put a one-off impairment on the wrong side of the join."""
        assert _metric_codes(EST) == ("annual_eps_nri_estimate",)


class TestItIsRefusedWhereAForecastHasNoMeaning:
    def test_the_QUARTERLY_basis_omits_it_entirely(self):
        """Analysts publish a figure per forward FISCAL YEAR. There is no trailing-twelve-month
        reading of a consensus, and rolling one would invent quarters nobody published — so
        `_codes_and_rule` refuses it and every quarterly reader drops it."""
        assert _codes_and_rule(EST, "quarterly") == (None, None)

    def test_it_has_no_TTM_rule_which_is_WHY_it_is_refused(self):
        """⚠ THE REFUSAL IS DATA, NOT A SPECIAL CASE. Adding a rule here would silently start rolling
        forecasts into trailing years on the quarterly toggle."""
        assert EST not in _TTM_RULE

    def test_the_annual_basis_reads_it_as_a_plain_level(self):
        codes, rule = _codes_and_rule(EST, "annual")
        assert codes == ["annual_eps_nri_estimate"]
        assert rule is None          # a level, not a roll-up


class TestItCannotCollideWithAReportedLine:
    def test_no_metric_code_belongs_to_two_metrics(self):
        """⚠ `rows_by_metric` SPLITS ONE BULK READ BACK OUT BY CODE, so a code appearing under two
        keys would land in both buckets — the forecast would be read as an actual by whichever card
        asked second. It would also mean two lines are the same line."""
        seen: dict[str, str] = {}
        for metric, codes in _METRIC_CODES.items():
            for code in codes:
                assert code not in seen, f"{code} is under both {seen.get(code)} and {metric}"
                seen[code] = metric

    def test_the_estimate_code_is_not_an_annuals_line(self):
        """The three feeds are named differently on purpose — a statement line is
        `annuals__Section__Line`, an estimate is `annual_…_estimate` (singular, no section, no double
        underscore). A reader filtering on `annuals__` must never pick this up."""
        assert not _metric_codes(EST)[0].startswith("annuals__")
