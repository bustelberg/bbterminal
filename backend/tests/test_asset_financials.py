"""Revenue parsing — the two traps in GuruFocus's financials blob.

Every fixture below mirrors the REAL response shape (probed 2026-07-13), including the
snake_case section key (`income_statement`, NOT "Income Statement") and the literal
"TTM" that terminates the fiscal axis.
"""
from __future__ import annotations

import pytest

from routers._asset_financials import (  # noqa: F401
    _FUND_PRODUCTS,
    _ITEMS,
    _has_line,
    _period_end,
    _series,
)


def _blob(fy: list[str], rev: list[str], *, cadence: str = "annuals",
          section: str = "income_statement") -> dict:
    axis = "Fiscal Year" if cadence == "annuals" else "Fiscal Quarter"
    return {"financials": {cadence: {axis: fy, section: {"Revenue": rev}}}}


class TestBothSchemaShapes:
    """GuruFocus RENAMED the financials sections, and our Storage cache holds both.

        live API today   annuals.income_statement.Revenue      (snake_case)
        cached blobs     annuals["Income Statement"].Revenue   (Title Case)

    A reader that knows only one shape returns an empty series for exactly the companies
    we already have cached — which is how this surfaced: Apple and CSX (cached, old
    shape) charted nothing while Mitsui (fresh, new shape) was fine. Silent, and it looks
    identical to "this company has no revenue".
    """

    @pytest.mark.parametrize("section", ["income_statement", "Income Statement"])
    def test_both_section_spellings_parse(self, section):
        pts = _series(_blob(["2024-12", "2025-12"], ["14540", "14092"], section=section), "annuals")
        assert [p.value for p in pts] == [14540.0, 14092.0]   # CSX, real figures


class TestPeriodEnd:
    def test_a_fiscal_month_becomes_that_month_s_last_day(self):
        assert _period_end("2025-12") == "2025-12-31"
        assert _period_end("2026-03") == "2026-03-31"      # Mitsui's March year-end
        assert _period_end("2024-02") == "2024-02-29"      # leap year, not 28

    def test_TTM_is_not_a_date(self):
        """THE trap. GuruFocus terminates every fiscal axis with the literal 'TTM' — a
        rolling window, not a period. `date.fromisoformat('TTM')` raises, and treating
        it as a period plots a phantom point on top of the latest year."""
        assert _period_end("TTM") is None

    @pytest.mark.parametrize("junk", ["", "2025", "2025-13-01", "garbage", None])
    def test_malformed_labels_are_dropped_not_guessed(self, junk):
        assert _period_end(junk) is None


class TestSeries:
    def test_parses_the_real_shape(self):
        # Apple's axis really does end ['2024-09', '2025-09', 'TTM'].
        pts = _series(_blob(["2024-09", "2025-09", "TTM"], ["391035", "416161", "440000"]), "annuals")
        assert [p.date for p in pts] == ["2024-09-30", "2025-09-30"]
        assert [p.value for p in pts] == [391035.0, 416161.0]

    def test_the_ttm_value_is_dropped_with_its_label(self):
        """Dropping the LABEL but keeping the VALUE would silently shift every revenue
        one period — the worst kind of off-by-one, since the chart still looks fine."""
        pts = _series(_blob(["2024-09", "TTM"], ["391035", "999999"]), "annuals")
        assert len(pts) == 1
        assert pts[0].value == 391035.0          # NOT 999999

    def test_empty_strings_are_holes_not_zeros(self):
        # GuruFocus writes "" for a period it lacks. Charting that as 0 would draw a
        # revenue collapse that never happened.
        pts = _series(_blob(["2023-12", "2024-12", "2025-12"], ["100", "", "120"]), "annuals")
        assert [p.date for p in pts] == ["2023-12-31", "2025-12-31"]
        assert all(p.value > 0 for p in pts)

    def test_points_come_back_in_date_order(self):
        pts = _series(_blob(["2025-12", "2023-12", "2024-12"], ["3", "1", "2"]), "annuals")
        assert [p.value for p in pts] == [1.0, 2.0, 3.0]

    def test_the_quarterly_block_labels_its_axis_FISCAL_YEAR(self):
        """Not 'Fiscal Quarter'. GuruFocus reuses the key, and assuming otherwise gives
        every company an empty quarterly series — a blank chart indistinguishable from
        'no data'. (It did, until the live blob was actually read.)"""
        blob = {"financials": {"quarterly": {
            "Fiscal Year": ["2025-09", "2025-12"],
            "income_statement": {"Revenue": ["10", "11"]},
        }}}
        pts = _series(blob, "quarterly")
        assert [p.date for p in pts] == ["2025-09-30", "2025-12-31"]

    def test_a_fiscal_quarter_axis_still_works_if_they_ever_use_it(self):
        pts = _series(_blob(["2025-09", "2025-12"], ["10", "11"], cadence="quarterly"), "quarterly")
        assert [p.date for p in pts] == ["2025-09-30", "2025-12-31"]

    def test_a_missing_section_is_empty_not_an_exception(self):
        assert _series({"financials": {"annuals": {}}}, "annuals") == []
        assert _series({}, "annuals") == []


class TestLineItemRegistry:
    """Adding a column is adding a registry entry — not a module, not an endpoint."""

    def test_the_columns_the_grid_exposes(self):
        assert set(_ITEMS) == {
            "revenue", "gross_profit", "ebit", "interest_expense", "pretax_income",
            "income_tax", "net_income", "depreciation_amort", "eps_diluted",
            "operating_cash_flow", "capex", "total_debt", "cash_and_equivalents",
            "shareholders_equity", "shares_outstanding", "forward_eps",
            "revenue_growth_est", "eps_lt_growth_est",
        }

    def test_net_income_is_the_SHAREHOLDERS_line_not_the_including_NCI_one(self):
        """GuruFocus carries several "Net Income" lines and they are NOT interchangeable.

        Mitsui Chemicals FY2025:
            pretax 68,608 + tax -21,698 = 46,910
              = "Net Income Including Noncontrolling Interests"   (the arithmetic)
            "Net Income"                = 34,378                  (the SHAREHOLDERS')
            the 12,532 gap is minority interest.

        We chart `Net Income` — what an equity holder actually owns, and what EPS is built
        from. So `pretax + tax` deliberately does NOT tie to this column for a company with
        minorities. JPMorgan's two figures are IDENTICAL (57,048, no minorities), so a
        check against JPM alone would bless either choice — the same shape as the
        EBIT/Operating-Income trap.
        """
        blob = {"financials": {"annuals": {
            "Fiscal Year": ["2025-03"],
            "income_statement": {
                "Pretax Income": ["68608"], "Tax Provision": ["-21698"],
                "Net Income Including Noncontrolling Interests": ["46910"],
                "Net Income": ["34378"],
            },
        }}}
        assert _ITEMS["net_income"]["field"] == "Net Income"
        charted = _series(blob, "annuals", _ITEMS["net_income"]["field"])[0].value
        assert charted == 34378.0                     # shareholders', NOT 46,910
        pretax = _series(blob, "annuals", "Pretax Income")[0].value
        tax = _series(blob, "annuals", "Tax Provision")[0].value
        assert pretax + tax != charted                # the minority-interest gap, on purpose

    def test_every_item_declares_a_field_or_a_computed_fields_list(self):
        for key, spec in _ITEMS.items():
            assert spec.get("field") or spec.get("fields"), f"{key} names no source line"

    def test_equity_is_the_SHAREHOLDERS_line_not_TOTAL_equity(self):
        """The net-income trap again, one statement over. GuruFocus carries both:

            Mitsui Chemicals:  Total Stockholders Equity   864,727   <- shareholders'
                               Minority Interest           124,057
                               Total Equity                988,784   <- incl. minorities

        We chart the shareholders' line — what an equity holder owns, and the denominator
        of book value per share and ROE. JPMorgan's two are IDENTICAL (362,438, zero
        minorities), so a check against JPM alone would bless either choice.
        """
        assert _ITEMS["shareholders_equity"]["field"] == "Total Stockholders Equity"
        assert _ITEMS["shareholders_equity"]["field"] != "Total Equity"
        blob = {"financials": {"annuals": {
            "Fiscal Year": ["2026-03"],
            "balance_sheet": {
                "Total Stockholders Equity": ["864727"],
                "Minority Interest": ["124057"],
                "Total Equity": ["988784"],
            },
        }}}
        charted = _series(blob, "annuals", _ITEMS["shareholders_equity"]["field"], "balance")[0].value
        assert charted == 864727.0                       # NOT 988,784
        minority = _series(blob, "annuals", "Minority Interest", "balance")[0].value
        total = _series(blob, "annuals", "Total Equity", "balance")[0].value
        assert charted + minority == pytest.approx(total)

    def test_each_item_names_gurufocus_s_own_field(self):
        assert _ITEMS["revenue"]["field"] == "Revenue"
        assert _ITEMS["gross_profit"]["field"] == "Gross Profit"
        assert _ITEMS["ebit"]["field"] == "EBIT"
        assert _ITEMS["interest_expense"]["field"] == "Interest Expense"
        assert _ITEMS["pretax_income"]["field"] == "Pretax Income"
        # GuruFocus has NO "Income Tax" key — the line is called "Tax Provision".
        assert _ITEMS["income_tax"]["field"] == "Tax Provision"
        assert _ITEMS["net_income"]["field"] == "Net Income"
        assert _ITEMS["depreciation_amort"]["field"] == (
            "Depreciation, Depletion and Amortization")

    def test_d_and_a_is_read_from_the_INCOME_statement_not_the_cashflow_twin(self):
        """The cashflow section carries an identical line, keyed "Cash Flow Depreciation,
        Depletion and Amortization" (Apple: 11,698 in both). D&A declares section=income,
        so the twin cannot be picked up by accident."""
        assert not _ITEMS["depreciation_amort"]["field"].startswith("Cash Flow")
        assert _ITEMS["depreciation_amort"].get("section", "income") == "income"

    def test_a_line_is_only_found_in_ITS_OWN_statement(self):
        """Sections became explicit when operating cash flow arrived. This is the reason:
        a blob holding BOTH statements must not let one leak into the other."""
        blob = {"financials": {"annuals": {
            "Fiscal Year": ["2025-09"],
            "income_statement": {"Depreciation, Depletion and Amortization": ["11698"]},
            "cashflow_statement": {"Cash Flow from Operations": ["111482"]},
        }}}
        # Operating CF is in the cashflow statement, and ONLY there.
        assert _has_line(blob, "Cash Flow from Operations", "cashflow") is True
        assert _has_line(blob, "Cash Flow from Operations", "income") is False
        # D&A is in the income statement, and ONLY there.
        assert _has_line(blob, "Depreciation, Depletion and Amortization", "income") is True
        assert _has_line(blob, "Depreciation, Depletion and Amortization", "cashflow") is False
        assert [p.value for p in _series(blob, "annuals", "Cash Flow from Operations", "cashflow")]             == [111482.0]

    def test_capex_ties_out_to_free_cash_flow(self):
        """The identity that PROVES the mapping: OCF + capex = free cash flow.
        Apple FY2025: 111,482 + (-12,715) = 98,767, and GuruFocus's own "Free Cash Flow"
        line says 98,767. If capex were wired to the wrong field this breaks."""
        blob = {"financials": {"annuals": {
            "Fiscal Year": ["2025-09"],
            "cashflow_statement": {
                "Cash Flow from Operations": ["111482"],
                "Capital Expenditure": ["-12715"],
                "Free Cash Flow": ["98767"],
            },
        }}}
        ocf = _series(blob, "annuals", "Cash Flow from Operations", "cashflow")[0].value
        capex = _series(blob, "annuals", _ITEMS["capex"]["field"], "cashflow")[0].value
        fcf = _series(blob, "annuals", "Free Cash Flow", "cashflow")[0].value
        assert capex < 0                       # an outflow, reported negative
        assert ocf + capex == pytest.approx(fcf)

    def test_capex_is_NOT_purchase_of_ppe(self):
        """They are different numbers, and one company will not tell you that. Mitsui
        Chemicals reports PP&E purchases of -128,242 against capex of -137,759 — capex
        also picks up intangibles. Apple's PP&E line is 0, so checking there would make
        the substitution look harmless."""
        assert _ITEMS["capex"]["field"] == "Capital Expenditure"
        assert _ITEMS["capex"]["field"] != "Purchase Of Property, Plant, Equipment"
        blob = {"financials": {"annuals": {
            "Fiscal Year": ["2026-03"],
            "cashflow_statement": {
                "Purchase Of Property, Plant, Equipment": ["-128242"],
                "Capital Expenditure": ["-137759"],
            },
        }}}
        assert _series(blob, "annuals", _ITEMS["capex"]["field"], "cashflow")[0].value == -137759.0

    def test_operating_cash_flow_sign_is_information_not_a_convention(self):
        """Apple +111,482, JPMorgan -147,782 — a bank's operating cash flow routinely goes
        negative as loans and trading assets grow. So it must NOT be treated like interest
        expense or tax (always-negative by convention): telling the user "this line is
        reported negative" would be false for half the companies."""
        blob = {"financials": {"annuals": {
            "Fiscal Year": ["2024-12", "2025-12"],
            "cashflow_statement": {"Cash Flow from Operations": ["-147782", "111482"]},
        }}}
        pts = _series(blob, "annuals", "Cash Flow from Operations", "cashflow")
        assert [p.value for p in pts] == [-147782.0, 111482.0]   # both signs, as reported

    def test_d_and_a_is_positive_unlike_interest_and_tax(self):
        # A magnitude, not an outflow: Apple 11,698 · JPMorgan 8,821 · Mitsui 104,744.
        blob = {"financials": {"annuals": {
            "Fiscal Year": ["2024-09", "2025-09"],
            "income_statement": {
                "Depreciation, Depletion and Amortization": ["11445", "11698"],
            },
        }}}
        pts = _series(blob, "annuals", _ITEMS["depreciation_amort"]["field"])
        assert [p.value for p in pts] == [11445.0, 11698.0]
        assert all(p.value > 0 for p in pts)

    def test_a_share_count_is_NOT_currency_and_must_not_be_fx_converted(self):
        """The unit that changes BEHAVIOUR, not just labels.

        Shares outstanding is a COUNT (Apple 15,004.697 million shares). "15,004.697
        million shares / 1.17 EUR-per-USD" is not a quantity — it is the same category
        error the ratio ban warns about. So `shares` is excluded from _CURRENCY_UNITS:
        the EUR conversion is SKIPPED (value_eur stays None) and the modal shows ONE
        chart, rather than a second panel implying a conversion that cannot exist.
        """
        from routers._asset_financials import _CURRENCY_UNITS
        assert _ITEMS["shares_outstanding"]["unit"] == "shares"
        assert "shares" not in _CURRENCY_UNITS
        assert "millions" in _CURRENCY_UNITS and "per_share" in _CURRENCY_UNITS

    def test_eps_times_shares_reconciles_to_net_income(self):
        """Both are on the DILUTED basis, which is what makes them tie — and it proves
        both fields are the right ones. Apple: 7.46 x 15,004.697 = 111,935 against net
        income 112,010 (the ~0.07% gap is diluted-vs-basic rounding)."""
        blob = {"financials": {"annuals": {
            "Fiscal Year": ["2025-09"],
            "income_statement": {
                "EPS (Diluted)": ["7.46"],
                "Shares Outstanding (Diluted Average)": ["15004.697"],
                "Net Income": ["112010"],
            },
        }}}
        eps = _series(blob, "annuals", _ITEMS["eps_diluted"]["field"])[0].value
        shares = _series(blob, "annuals", _ITEMS["shares_outstanding"]["field"])[0].value
        net = _series(blob, "annuals", _ITEMS["net_income"]["field"])[0].value
        assert eps * shares == pytest.approx(net, rel=0.01)

    def test_eps_declares_itself_per_share_and_everything_else_is_millions(self):
        """EPS broke the assumption every other line shares.

        `EPS (Diluted) = 7.46` is $7.46 A SHARE. Render it on the millions path and it
        reads "$7.46 million" — off by 1e6, and entirely plausible-looking. Note it does
        NOT contain the string "per Share", so the old field-name guard would have waved
        it straight through: the unit has to be DECLARED, not sniffed from the name.

        Both units are CURRENCY, so `value_eur` stays valid for both — only the scale and
        the axis label change.
        """
        assert _ITEMS["eps_diluted"]["unit"] == "per_share"
        assert "per Share" not in _ITEMS["eps_diluted"]["field"]     # the guard's blind spot

        # The invariant: every unit is one of the three KNOWN ones, and anything that is
        # not millions has SAID so. A new item that quietly means something else (a ratio,
        # a count, a rate) must not inherit the millions default by omission.
        known = {"millions", "per_share", "shares", "percent"}
        per_share = {k for k, v in _ITEMS.items() if v.get("unit") == "per_share"}
        for key, spec in _ITEMS.items():
            assert spec.get("unit", "millions") in known, f"{key} declares an unknown unit"
        assert per_share == {"eps_diluted", "forward_eps"}
        assert {k for k, v in _ITEMS.items() if v.get("unit") == "shares"} == {"shares_outstanding"}
        assert {k for k, v in _ITEMS.items() if v.get("unit") == "percent"} == {
            "revenue_growth_est", "eps_lt_growth_est"}

    def test_no_registry_item_is_a_ratio(self):
        """Every item here is assumed to be a CURRENCY AMOUNT IN MILLIONS: the axis is
        labelled "millions of USD" and `_convert_to_eur` DIVIDES it by an FX rate. Push a
        percentage through that and "Tax Rate % = 15.61" becomes "15.61 million USD =
        13.3 million EUR" — nonsense, rendered confidently. GuruFocus sits `Tax Rate %`
        right next to `Tax Provision` in the same section, so this is one typo away."""
        for key, spec in _ITEMS.items():
            for field in (spec.get("fields") or [spec["field"]]):
                assert "%" not in field, (
                    f"{key} is a RATIO. A `unit` fixes a scale mismatch (per-share vs "
                    f"millions), but a percentage is not currency at all: _convert_to_eur "
                    f"divides by an FX rate, so 'Tax Rate % = 15.61' would come back as "
                    f"'13.3% in EUR'. It needs the conversion SKIPPED, not relabelled.")

    def test_pretax_income_is_not_ebit(self):
        """EBIT is BEFORE interest; pretax is AFTER it. The gap is the interest bill —
        Mitsui Chemicals FY2026: EBIT 85,035, interest expense -16,427, pretax 68,608."""
        blob = {"financials": {"annuals": {
            "Fiscal Year": ["2026-03"],
            "income_statement": {
                "EBIT": ["85035"], "Interest Expense": ["-16427"], "Pretax Income": ["68608"],
            },
        }}}
        ebit = _series(blob, "annuals", "EBIT")[0].value
        interest = _series(blob, "annuals", "Interest Expense")[0].value
        pretax = _series(blob, "annuals", "Pretax Income")[0].value
        assert ebit != pretax
        assert pretax == pytest.approx(ebit + interest, rel=0.01)   # interest is negative

    def test_a_phrase_is_not_the_label_lowercased(self):
        """`label.lower()` turns EBIT into "ebit" ("a fund has no ebit"). An acronym is
        not a word, so prose gets its own field."""
        assert _ITEMS["ebit"]["phrase"] == "EBIT"
        assert _ITEMS["gross_profit"]["phrase"] == "gross profit"

    def test_ebit_is_gurufocus_s_own_line_NOT_operating_income(self):
        """They are different numbers, and one company will not tell you that.

        Mitsui Chemicals: EBIT 85,035 vs Operating Income 56,602 — EBIT picks up
        non-operating income. Apple's two are IDENTICAL (133,050 both), which is exactly
        how you'd convince yourself the substitution is safe. GuruFocus publishes EBIT as
        its own line; use it.
        """
        assert _ITEMS["ebit"]["field"] != "Operating Income"
        blob = {"financials": {"annuals": {
            "Fiscal Year": ["2026-03"],
            "income_statement": {"EBIT": ["85035"], "Operating Income": ["56602"]},
        }}}
        assert [p.value for p in _series(blob, "annuals", "EBIT")] == [85035.0]
        assert [p.value for p in _series(blob, "annuals", "Operating Income")] == [56602.0]

    def test_interest_expense_keeps_its_negative_sign(self):
        """GuruFocus reports it NEGATIVE — it is an outflow (Apple -3,933; JPMorgan
        -101,350; Mitsui -14,702). We chart it as reported. A silent sign-flip to make
        the line "look right" would make our number disagree with the source it cites."""
        blob = {"financials": {"annuals": {
            "Fiscal Year": ["2023-09", "2024-09", "2025-09"],
            "income_statement": {"Interest Expense": ["-3933", "0", "0"]},
        }}}
        pts = _series(blob, "annuals", "Interest Expense")
        assert [p.value for p in pts] == [-3933.0, 0.0, 0.0]

    def test_a_zero_is_a_value_not_a_hole(self):
        """Apple's last two years report interest expense as literally 0 — it nets it
        out. That is a number, and dropping it would leave a gap in the line where the
        company actually reported nil. Only "" is a hole."""
        blob = {"financials": {"annuals": {
            "Fiscal Year": ["2024-09", "2025-09"],
            "income_statement": {"Interest Expense": ["0", ""]},
        }}}
        pts = _series(blob, "annuals", "Interest Expense")
        assert [(p.date, p.value) for p in pts] == [("2024-09-30", 0.0)]   # 0 kept, "" dropped

    def test_the_tax_identity_holds(self):
        """The check that proves the RIGHT field is mapped: pretax + tax = net income.
        JPMorgan FY2025: 74,666 + (-15,767) = 58,899 = Net Income. If `income_tax` were
        wired to "Tax Rate %" (22.12) or to a positive-signed field, this breaks."""
        blob = {"financials": {"annuals": {
            "Fiscal Year": ["2025-12"],
            "income_statement": {
                "Pretax Income": ["74666"], "Tax Provision": ["-15767"],
                "Net Income": ["58899"], "Tax Rate %": ["21.12"],
            },
        }}}
        pretax = _series(blob, "annuals", _ITEMS["pretax_income"]["field"])[0].value
        tax = _series(blob, "annuals", _ITEMS["income_tax"]["field"])[0].value
        net = _series(blob, "annuals", "Net Income")[0].value
        assert tax < 0                                     # reported as an outflow
        assert pretax + tax == pytest.approx(net)

    @pytest.mark.parametrize(
        "item",
        ["revenue", "gross_profit", "ebit", "interest_expense", "pretax_income",
         "income_tax", "net_income", "depreciation_amort", "eps_diluted",
         "operating_cash_flow", "capex", "total_debt", "cash_and_equivalents",
         "shareholders_equity", "shares_outstanding"])
    def test_a_line_parses_by_field_name(self, item):
        spec = _ITEMS[item]
        if spec.get("fields"):
            pytest.skip("computed item — covered by TestTotalDebtIsComputed")
        field = spec["field"]
        blob = {"financials": {"annuals": {
            "Fiscal Year": ["2024-12", "2025-12", "TTM"],
            "income_statement": {field: ["100", "120", "130"]},
        }}}
        pts = _series(blob, "annuals", field)
        assert [p.value for p in pts] == [100.0, 120.0]


class TestABankHasNoGrossProfit:
    """The line is ABSENT, and that is an ANSWER — not a gap.

    JPMorgan's income statement (`ind_template: 'B'`) is:
        Interest Income · Interest Expense · Net Interest Income (for Banks) ·
        Non Interest Income · Revenue · Credit Losses Provision · ...
    There is no `Gross Profit` key, because a bank has no cost of goods sold. Badging
    that "NO DATA" would claim GuruFocus is missing a number that does not exist.
    """

    @staticmethod
    def _bank_blob() -> dict:
        return {"financials": {
            "financial_template_parameters": {"ind_template": "B"},
            "annuals": {
                "Fiscal Year": ["2024-12", "2025-12"],
                "income_statement": {
                    "Interest Income": ["193341", "195679"],        # JPM's real figures
                    "Interest Expense": ["-97898", "-98143"],
                    "Net Interest Income (for Banks)": ["95443", "97536"],
                    "Revenue": ["169439", "181847"],
                    "Pretax Income": ["72595", "74666"],
                },
            },
        }}

    def test_a_bank_HAS_revenue(self):
        assert _has_line(self._bank_blob(), "Revenue") is True

    def test_a_bank_DOES_have_pretax_income(self):
        """Pretax income survives every industry template — JPMorgan reports 75,081 while
        having NO gross profit, NO operating income and NO EBIT. So of the five columns,
        a bank charts Revenue, Interest expense and Pretax income, and N/As the other two.
        That mix is only expressible because the predicate is per-LINE."""
        assert _has_line(self._bank_blob(), "Pretax Income") is True
        assert [p.value for p in _series(self._bank_blob(), "annuals", "Pretax Income")] \
            == [72595.0, 74666.0]

    def test_a_bank_DOES_have_interest_expense(self):
        """The predicate is per-LINE, not per-template — which this proves. Interest
        expense is a bank's CORE cost, so unlike gross profit and EBIT it is present and
        charts normally. A template-level "banks are special" rule would have wrongly
        N/A'd it."""
        assert _has_line(self._bank_blob(), "Interest Expense") is True
        pts = _series(self._bank_blob(), "annuals", "Interest Expense")
        assert [p.value for p in pts] == [-97898.0, -98143.0]

    @pytest.mark.parametrize("field", ["Gross Profit", "Operating Income", "EBIT"])
    def test_a_bank_has_none_of_the_operating_lines(self, field):
        # JPMorgan really does have none of these — it reports Interest Income, Net
        # Interest Income and Pretax Income instead. So Gross profit AND EBIT both render
        # N/A for a bank, off the same predicate.
        assert _has_line(self._bank_blob(), field) is False

    def test_absent_is_distinguishable_from_present_but_empty(self):
        """The whole point. An absent line means 'this industry has no such concept'; a
        present-but-empty one means 'GuruFocus is missing the values'. They render
        differently (N/A vs an empty chart), so they must not collapse."""
        empty = {"financials": {"annuals": {
            "Fiscal Year": ["2025-12"], "income_statement": {"Gross Profit": [""]},
        }}}
        assert _has_line(empty, "Gross Profit") is True        # present, just no values
        assert _series(empty, "annuals", "Gross Profit") == []  # ...and no points

    def test_an_industrial_does_have_gross_profit(self):
        # Apple's real cached figures, to prove the predicate isn't just always-false.
        blob = {"financials": {"annuals": {
            "Fiscal Year": ["2024-09", "2025-09"],
            "Income Statement": {"Gross Profit": ["180683", "195201"]},
        }}}
        assert _has_line(blob, "Gross Profit") is True
        assert [p.value for p in _series(blob, "annuals", "Gross Profit")] == [180683.0, 195201.0]


class TestTotalDebtIsComputed:
    """GuruFocus publishes NO "Total Debt" line, so this one is SUMMED — the first item
    that is derived rather than read."""

    @staticmethod
    def _bs(st: list[str], lt: list[str], *, plain: bool = True) -> dict:
        sec: dict[str, list[str]] = {
            "Short-Term Debt & Capital Lease Obligation": st,
            "Long-Term Debt & Capital Lease Obligation": lt,
        }
        if plain:                      # Apple/Mitsui carry these too; JPMorgan does NOT
            sec["Short-Term Debt"] = st
            sec["Long-Term Debt"] = lt
        return {"financials": {"annuals": {
            "Fiscal Year": ["2024-09", "2025-09"], "balance_sheet": sec,
        }}}

    def test_it_sums_short_and_long_term(self):
        from routers._asset_financials import _summed_series
        pts = _summed_series(self._bs(["20879", "20329"], ["85750", "78328"]),
                             "annuals", _ITEMS["total_debt"]["fields"], "balance")
        assert [p.value for p in pts] == [106629.0, 98657.0]      # Apple: 20,329 + 78,328

    def test_it_uses_the_CAPITAL_LEASE_variants_because_a_bank_has_only_those(self):
        """JPMorgan's balance sheet has ONLY the "& Capital Lease Obligation" keys — its
        plain Short-/Long-Term Debt are absent. For Apple and Mitsui the two spellings are
        IDENTICAL, so the lease variants lose nothing and are the only choice that works
        for every template."""
        from routers._asset_financials import _summed_series
        bank = self._bs(["66012", "68048"], ["433970", "448764"], plain=False)
        assert _has_line(bank, "Short-Term Debt", "balance") is False
        pts = _summed_series(bank, "annuals", _ITEMS["total_debt"]["fields"], "balance")
        assert [p.value for p in pts] == [499982.0, 516812.0]     # JPM: 68,048 + 448,764

    def test_a_period_missing_a_component_is_DROPPED_not_summed_as_zero(self):
        """A partial total silently UNDERSTATES the debt, and an understated number that
        looks real is worse than a gap in the line. Same discipline as _trailing_12m."""
        from routers._asset_financials import _summed_series
        blob = self._bs(["20879", ""], ["85750", "78328"], plain=False)
        pts = _summed_series(blob, "annuals", _ITEMS["total_debt"]["fields"], "balance")
        assert [p.date for p in pts] == ["2024-09-30"]            # 2025 dropped, NOT 78,328
        assert [p.value for p in pts] == [106629.0]

    def test_it_says_out_loud_that_it_is_derived(self):
        # A computed number must disclose that it was computed.
        assert "Computed" in _ITEMS["total_debt"]["note"]


class TestCashIsCoalescedNotSummed:
    """Two NAMES for one quantity, not two components — so `combine: "first"`.

    GuruFocus renames the line per industry template:
        Apple / Mitsui   "Cash and Cash Equivalents"                   35,934
        JPMorgan         "Balance Statement Cash and cash equivalents" 343,338
    A bank has NEITHER ordinary key. Mapping only the first N/As every bank; SUMMING them
    (the total_debt mode) double-counts anyone carrying both. Hence `combine` is declared,
    never inferred from the presence of `fields`.
    """

    @staticmethod
    def _bs(sec: dict) -> dict:
        return {"financials": {"annuals": {"Fiscal Year": ["2025-09"], "balance_sheet": sec}}}

    def test_an_ordinary_company_uses_the_ordinary_key(self):
        from routers._asset_financials import _coalesced_series
        blob = self._bs({"Cash and Cash Equivalents": ["35934"]})
        pts = _coalesced_series(blob, "annuals", _ITEMS["cash_and_equivalents"]["fields"], "balance")
        assert [p.value for p in pts] == [35934.0]

    def test_a_bank_uses_its_own_key(self):
        from routers._asset_financials import _coalesced_series
        blob = self._bs({"Balance Statement Cash and cash equivalents": ["343338"]})
        assert _has_line(blob, "Cash and Cash Equivalents", "balance") is False
        pts = _coalesced_series(blob, "annuals", _ITEMS["cash_and_equivalents"]["fields"], "balance")
        assert [p.value for p in pts] == [343338.0]      # JPMorgan, not N/A

    def test_carrying_BOTH_names_takes_one_not_the_sum(self):
        """The bug `combine` exists to prevent: coalescing must never double-count."""
        from routers._asset_financials import _coalesced_series
        blob = self._bs({
            "Cash and Cash Equivalents": ["35934"],
            "Balance Statement Cash and cash equivalents": ["35934"],
        })
        pts = _coalesced_series(blob, "annuals", _ITEMS["cash_and_equivalents"]["fields"], "balance")
        assert [p.value for p in pts] == [35934.0]       # NOT 71,868

    def test_it_is_the_NARROW_cash_line_not_cash_plus_marketable_securities(self):
        """Apple: cash 35,934, but "Cash, Cash Equivalents, Marketable Securities" = 54,697.
        A different concept, 52% larger. "Cash and equivalents" means the narrow one."""
        fields = _ITEMS["cash_and_equivalents"]["fields"]
        assert not any("Marketable" in f for f in fields)

    def test_cash_is_coalesced_and_debt_is_summed(self):
        assert _ITEMS["cash_and_equivalents"]["combine"] == "first"
        assert _ITEMS["total_debt"]["combine"] == "sum"


class TestForwardEpsIsAForecastFromAnotherSource:
    """The first FORECAST, and the first line not from the `financials` blob.

    It comes from `stock/{sym}/analyst_estimate` — SINGULAR. The plural
    `analyst_estimates` is one of the endpoints that LOOKS real: GuruFocus 200s on it and
    hands back the router-fallback payload, exactly like `dividend` vs `dividends`.
    """

    @staticmethod
    def _est() -> dict:
        # Apple's real response shape — note the COMPACT dates and the SCALAR field.
        return {
            "annual": {
                "date": ["202609", "202709", "202809"],
                "per_share_eps_estimate": [8.76, 9.71, 10.84],
                "eps_nri_estimate": [8.77, 9.72, 10.84],
                "long_term_growth_rate_mean": 12.5,          # a bare float, NOT a list
            },
            "quarterly": {"date": ["202606"], "per_share_eps_estimate": [2.1]},
        }

    def test_it_declares_the_estimates_source(self):
        assert _ITEMS["forward_eps"]["source"] == "estimates"
        assert _ITEMS["forward_eps"]["field"] == "per_share_eps_estimate"

    def test_compact_YYYYMM_dates_parse(self):
        from routers._asset_financials import _period_end_compact
        assert _period_end_compact("202609") == "2026-09-30"
        assert _period_end_compact("202602") == "2026-02-28"
        # NOT the financials blob's format — these must not be confused.
        assert _period_end_compact("2026-09") is None
        assert _period_end_compact("TTM") is None

    def test_it_parses_the_forecast(self):
        from routers._asset_financials import _estimate_series
        pts = _estimate_series(self._est(), "annuals", "per_share_eps_estimate")
        assert [(p.date, p.value) for p in pts] == [
            ("2026-09-30", 8.76), ("2027-09-30", 9.71), ("2028-09-30", 10.84)]

    def test_a_SCALAR_field_does_not_blow_it_up(self):
        """`long_term_growth_rate_mean` is a bare float in the same dict. A parser that
        assumes every value is a list raises on it."""
        from routers._asset_financials import _estimate_series
        assert _estimate_series(self._est(), "annuals", "long_term_growth_rate_mean") == []

    def test_it_is_the_consensus_eps_not_the_NRI_variant(self):
        """Apple: 8.76 vs 8.77 — near-identical, which is exactly why eyeballing ONE
        company is not a way to choose between two fields."""
        assert _ITEMS["forward_eps"]["field"] != "eps_nri_estimate"

    def test_it_says_out_loud_that_it_is_an_estimate(self):
        note = _ITEMS["forward_eps"]["note"]
        assert "CONSENSUS" in note and "latest known rate" in note


class TestRevenueGrowthIsDerivedAndIsAPercent:
    """A RATE, and a derived one — the case the ratio ban has been pointing at.

    GuruFocus's own `future_revenue_estimate_growth` is a SCALAR (Apple 10.09, NVIDIA
    45.73), one long-term rate, not a series — there is nothing to plot. So the per-period
    growth is computed from `revenue_estimate`, anchored on the last REPORTED revenue.
    """

    EST = {"annual": {
        "date": ["202609", "202709", "202809"],
        "revenue_estimate": [477599.6, 520531.73, 557206.37],   # Apple, real
        "future_revenue_estimate_growth": 10.09,                 # a SCALAR, not a series
    }}
    FIN = {"financials": {"annuals": {
        "Fiscal Year": ["2024-09", "2025-09"],
        "income_statement": {"Revenue": ["391035", "416161"]},   # reported
    }}}

    def test_a_percent_is_not_currency_and_is_never_fx_converted(self):
        from routers._asset_financials import _CURRENCY_UNITS
        assert _ITEMS["revenue_growth_est"]["unit"] == "percent"
        assert "percent" not in _CURRENCY_UNITS

    def test_the_first_forecast_year_is_anchored_to_the_last_REPORTED_revenue(self):
        """The number most people came for: est FY2026 477,600 against ACTUAL FY2025
        416,161 = +14.8%. Without the anchor the first forecast year has no predecessor and
        would simply vanish."""
        from routers._asset_financials import _yoy_growth_series
        pts = _yoy_growth_series(self.EST, self.FIN, "annuals", "revenue_estimate", "Revenue")
        assert [p.date for p in pts] == ["2026-09-30", "2027-09-30", "2028-09-30"]
        assert pts[0].value == pytest.approx(14.76, abs=0.05)    # vs reported, not dropped
        assert pts[1].value == pytest.approx(8.99, abs=0.05)     # est-on-est
        assert pts[2].value == pytest.approx(7.04, abs=0.05)

    def test_without_the_reported_history_the_first_year_is_dropped_not_faked(self):
        from routers._asset_financials import _yoy_growth_series
        pts = _yoy_growth_series(self.EST, None, "annuals", "revenue_estimate", "Revenue")
        assert [p.date for p in pts] == ["2027-09-30", "2028-09-30"]   # no invented base

    def test_an_overlapping_base_is_ignored_rather_than_producing_nonsense(self):
        """If the "reported" period is not strictly BEFORE the first estimate, using it
        would compute a growth across an overlap — a meaningless number that still plots."""
        from routers._asset_financials import _yoy_growth_series
        overlapping = {"financials": {"annuals": {
            "Fiscal Year": ["2027-09"],
            "income_statement": {"Revenue": ["999999"]},
        }}}
        pts = _yoy_growth_series(self.EST, overlapping, "annuals", "revenue_estimate", "Revenue")
        assert [p.date for p in pts] == ["2027-09-30", "2028-09-30"]

    def test_it_says_it_is_derived_and_why_gurufocus_s_own_field_cannot_be_used(self):
        note = _ITEMS["revenue_growth_est"]["note"]
        assert "Derived" in note and "not a series" in note


class TestLongTermEpsGrowthIsAScalar:
    """The first item with NO SERIES AT ALL.

    The analysts' long-term EPS growth consensus is ONE number, and GuruFocus publishes it
    as one (Apple 13.01, NVIDIA 47.57, JPMorgan 8.66). Inventing a date so it could be
    plotted would dress a single point up as a trend, so it is returned as a scalar and
    shown as a number.
    """

    EST = {"annual": {
        "date": ["202609", "202709"],
        "per_share_eps_estimate": [8.76, 9.71],
        "long_term_growth_rate_mean": 13.01,                # THE consensus LTG
        "future_per_share_eps_estimate_growth": 13.03,      # the series-implied CAGR
        "future_eps_nri_estimate_growth": 13.01,            # the NRI variant
    }}

    def test_it_is_declared_scalar_and_percent(self):
        assert _ITEMS["eps_lt_growth_est"]["scalar"] is True
        assert _ITEMS["eps_lt_growth_est"]["unit"] == "percent"
        assert _ITEMS["eps_lt_growth_est"]["source"] == "estimates"

    def test_it_reads_the_consensus_LTG_not_its_two_lookalikes(self):
        """Apple's three are 13.01 / 13.03 / 13.01 — indistinguishable by eye. NVIDIA is
        where they part company (47.57 vs 45.72), which is the whole reason this is chosen
        by name and pinned, not eyeballed."""
        from routers._asset_financials import _estimate_scalar
        assert _ITEMS["eps_lt_growth_est"]["field"] == "long_term_growth_rate_mean"
        assert _estimate_scalar(self.EST, _ITEMS["eps_lt_growth_est"]["field"]) == 13.01

    def test_a_scalar_reader_refuses_to_consume_a_SERIES(self):
        """If the field it names ever becomes an array, taking [0] would silently report
        the first forecast year as though it were a long-run rate."""
        from routers._asset_financials import _estimate_scalar
        assert _estimate_scalar(self.EST, "per_share_eps_estimate") is None   # a list
        assert _estimate_scalar(self.EST, "nonexistent_field") is None

    def test_a_string_number_still_parses(self):
        # GuruFocus quotes some of these ("10.09"), so the reader coerces.
        from routers._asset_financials import _estimate_scalar
        assert _estimate_scalar({"annual": {"x": "8.66"}}, "x") == 8.66

    def test_it_says_it_is_a_RATE_not_earnings(self):
        note = _ITEMS["eps_lt_growth_est"]["note"]
        assert "not a series" in note and "growth" in note.lower()


class TestFundsAreACategoryError:
    """A fund HOLDS securities; it does not operate a business. GuruFocus agrees —
    `stock/QQQ/financials` returns null, not an empty blob. So revenue is not a small
    number for an ETF, it is a question that does not apply, and we never spend a call
    asking: ETF (2,507) + FUNDS (1,681) is 26% of the grid on top of the 33% that is
    bonds/futures."""

    @pytest.mark.parametrize("product", ["ETF", "FUNDS"])
    def test_funds_are_answered_locally(self, product):
        assert product in _FUND_PRODUCTS

    def test_an_equity_is_not(self):
        assert "EQUITY" not in _FUND_PRODUCTS
