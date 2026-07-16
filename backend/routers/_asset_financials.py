"""Income-statement lines for the /asset-pipeline grid — Revenue, Gross profit, EBIT.

ONE primitive, TWO charts: a timeseries of (fiscal period, value), in the listing's own
currency and in EUR. It rides the SAME ISIN -> GuruFocus bridge the dividend column built
(`_asset_dividends._resolve_listing`), so an ETF and an equity resolve identically and
nothing here re-implements that.

A COLUMN IS A REGISTRY ENTRY (`_ITEMS`), NOT A MODULE. One `financials.json` blob carries
every line, and it is Storage-cached and shared with the earnings pipeline — so the
second and third columns on a company cost ZERO extra API calls. Adding the next line
item is one dict entry plus a `<th>`.

FIVE THINGS GURUFOCUS DOES THAT WILL BITE YOU (all measured 2026-07-13):

0. A BANK HAS NO GROSS PROFIT, AND NO EBIT. `financial_template_parameters.ind_template`
   picks the INDUSTRY template, and it decides which lines exist at all. JPMorgan
   ('B' = bank) reports Interest Income / Net Interest Income / Pretax Income — its
   income statement has no `Gross Profit`, `Operating Income` or `EBIT` key, because a
   bank has no cost of goods sold. `_has_line` distinguishes ABSENT (the concept does not
   apply -> `applicable=False`, an ANSWER) from PRESENT-BUT-EMPTY (values missing -> a
   gap). Badging the former "NO DATA" would claim a missing number that does not exist.

1. FINANCIALS ARE FX-CONVERTED PER LISTING. This is the exact OPPOSITE of the dividend
   feed, which reports the declaration currency on every listing of an ISIN (Apple =
   0.27 USD on Nasdaq, Xetra, Zurich and Milan alike). Revenue is not:

       CSX          FY2024-12  14,540 USD        FY2025-12  14,092 USD
       XTER:CXR     FY2024-12  13,885.700 EUR    FY2025-12  12,034.568 EUR
                               (x0.955 = 2024 FX)           (x0.854 = 2025 FX)

   Same company, same fiscal year, different number — converted at each PERIOD's rate.
   So for revenue the choice of listing genuinely changes the value, and a non-home
   listing is not merely a shorter history (as it is for dividends) but a different
   currency basis. `summary.company_data` names both: `currency` (the listing's trading
   currency, which is what the financials come in) vs `currency_comp` (the company's
   reporting currency, which is not).

2. VALUES ARE IN MILLIONS. `14092` is $14.092bn. Never render it as 14,092 dollars.

3. THE AXIS CONTAINS "TTM". `Fiscal Year` ends `[..., '2025-12', 'TTM']` — a label, not
   a date. `date.fromisoformat('TTM')` raises, and treating it as a period would plot a
   phantom point on top of the latest year. It is dropped.

WHY MOST ROWS COST NOTHING
   Revenue is a property of an OPERATING BUSINESS. ~59% of the grid cannot have one and
   is answered without an API call: bonds/futures/FX aren't equities, and a FUND holds
   securities rather than operating a business (GuruFocus agrees — `stock/QQQ/financials`
   returns `null`, not an empty blob).
"""
from __future__ import annotations

import asyncio

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter(tags=["asset-pipeline"])

# GuruFocus's key for the top line, inside `financials.{annuals,quarterly}` — and there
# are TWO of them, because GuruFocus CHANGED THE SCHEMA and our Storage cache holds both:
#
#     live API today     annuals.income_statement.Revenue      (snake_case)
#     cached blobs       annuals["Income Statement"].Revenue   (Title Case)
#
# The `financials.json` cache is shared with the earnings pipeline and some of it was
# written before the change, so a reader that knows only one shape returns an empty
# series for exactly the companies we already have data for — which is how this surfaced:
# Apple and CSX (cached, old shape) charted nothing while Mitsui (fresh, new shape) was
# fine. Try both; the field name inside is `Revenue` either way.
#
# ⚠ The same schema change is a LATENT BUG in `ingest/earnings/financials.py`, which
# derives its `metric_code`s from these section names: a re-fetched company now writes
# `annuals__income_statement__Revenue` where the constants (and the /earnings dashboard)
# expect `annuals__Income Statement__Revenue`. Not this module's to fix — see TODO.md.
_SECTIONS = ("income_statement", "Income Statement")

# Which STATEMENT a line lives in. Until operating cash flow arrived every item was
# hardcoded to the income statement; now each entry names its section, and the two
# spellings per section are the schema-rename problem above (live snake_case vs the
# Title Case still sitting in our Storage cache).
#
# This is load-bearing, not tidiness: the cashflow statement carries its OWN
# "Cash Flow Depreciation, Depletion and Amortization", identical in value to the income
# statement's D&A. With sections explicit, a line can no longer be picked up from the
# wrong statement by accident.
# A SECOND SOURCE. Everything above comes from the `financials` blob (reported history);
# forward estimates come from `stock/{sym}/analyst_estimate` — a different endpoint, a
# different shape, and FUTURE dates.
#
# ⚠ SINGULAR. `stock/{sym}/analyst_estimates` (plural) is one of the endpoints that LOOKS
# real — GuruFocus 200s on it and returns the router-fallback payload — exactly like
# `dividend` vs `dividends`. `gurufocus_api.json` pins both verdicts.
#
# Shape: {"annual": {"date": ["202609", ...], "per_share_eps_estimate": [8.76, ...], ...},
#         "quarterly": {...}}. Dates are compact YYYYMM, NOT the "2025-09" the financials
# blob uses — and some entries in that dict are SCALARS (long_term_growth_rate_mean), not
# arrays, so a parser that assumes every value is a list will blow up.
_SOURCE_FINANCIALS = "financials"
_SOURCE_ESTIMATES = "estimates"
# Our cadence name -> the estimate blob's block name.
_ESTIMATE_BLOCKS = {"annuals": "annual", "quarterly": "quarterly"}

_SECTION_ALIASES: dict[str, tuple[str, ...]] = {
    "income": ("income_statement", "Income Statement"),
    "cashflow": ("cashflow_statement", "Cashflow Statement"),
    "balance": ("balance_sheet", "Balance Sheet"),
    # The RATIO sections, read by `_asset_fundamentals` (the four soundness charts) and by
    # nothing in `_ITEMS`. Naming a section here does NOT make it a column: every `_ITEMS` entry
    # is FX-converted, which is why that registry bans ratios ("13.3% in EUR"). `_series` is
    # unit-agnostic — it reads numbers off an axis — so the two concerns stay apart.
    #
    # ⚠ BOTH SPELLINGS, ALWAYS, AND THE OLD ONE IS NOT THE NEW ONE TITLE-CASED.
    # GuruFocus renamed these sections and Storage holds blobs from before and after. Every name
    # on the right was READ OFF A REAL CACHED BLOB — two of them are not what mechanical
    # title-casing predicts, and guessing produced a chart that was silently EMPTY rather than an
    # error (the section simply is not there, so `_series` finds no values and returns []):
    #
    #     common_size_ratios  ->  "Ratios"              NOT "Common Size Ratios"
    #     gurufocus_rankings  ->  "Gurufocus Rankings"  NOT "GuruFocus Rankings"  (lowercase f)
    #
    # If you add a section here, dump `financials.annuals`' keys from Storage. Do not infer it.
    "valuation": ("valuation_ratios", "Valuation Ratios"),
    "quality": ("valuation_and_quality", "Valuation and Quality"),
    "common_size": ("common_size_ratios", "Ratios"),
    "rankings": ("gurufocus_rankings", "Gurufocus Rankings"),
    "per_share": ("per_share_data_array", "Per Share Data"),
}
_DEFAULT_SECTION = "income"

# THE registry. Every income-statement line the grid exposes is one entry here — adding
# a column is adding a row to this dict plus a `<th>`, not a new module.
#
# `field` is GuruFocus's own key, which is spelled the same in BOTH schema shapes (only
# the SECTION was renamed).
#
# `phrase` is the label as it reads INSIDE a sentence, and it is not `label.lower()` —
# that turns EBIT into "ebit" ("a fund has no ebit"). An acronym is not a word.
#
# `unit` says what the NUMBER IS, and it exists because EPS broke the assumption every
# other line shares:
#     "millions"   a currency amount in MILLIONS   (14,092 = $14.1bn)
#     "per_share"  a currency amount PER SHARE     (7.46 = $7.46 a share)
# Both are currency, so the EUR conversion is valid for both — it is only the SCALE and
# the axis label that differ. Render `EPS (Diluted) = 7.46` on the millions path and it
# reads "$7.46 million a year", which is wrong by six orders of magnitude and looks
# entirely plausible. Default is "millions"; anything else must say so.
#     "shares"     a COUNT, in millions of shares — NOT currency at all
# The third unit is the one that changes behaviour rather than labels: a share count has
# no EUR version, and dividing 15,004.697 million shares by an FX rate produces a number
# that means nothing. So `shares` SKIPS the conversion (`value_eur` stays null) and the
# modal renders ONE chart instead of two. This is exactly the "conversion must be
# skipped, not relabelled" case the ratio ban warns about — it is now implemented, and a
# ratio column could reuse it if it also carried a `%` unit.
#     "percent"    a RATE, in percent — not currency either
_UNIT_MILLIONS = "millions"
_UNIT_PER_SHARE = "per_share"
_UNIT_SHARES = "shares"
_UNIT_PERCENT = "percent"
# The units that ARE currency, and therefore convert to EUR. Anything else must not.
# `shares` and `percent` are the two that are not: "15,004 million shares / 1.17" and
# "10.09% / 1.17" are both category errors, and the second is precisely what the ratio ban
# above warns about. They share one mechanism — SKIP the conversion, leave value_eur null,
# and render a single chart — rather than being relabelled and quietly divided.
_CURRENCY_UNITS = frozenset({_UNIT_MILLIONS, _UNIT_PER_SHARE})

_ITEMS: dict[str, dict[str, str]] = {
    "revenue": {"label": "Revenue", "field": "Revenue", "phrase": "revenue"},
    "gross_profit": {"label": "Gross profit", "field": "Gross Profit", "phrase": "gross profit"},
    # GuruFocus publishes EBIT as its OWN line — do not substitute "Operating Income".
    # They are not the same number: EBIT picks up non-operating income, and Mitsui
    # Chemicals reports EBIT 85,035 against Operating Income 56,602. Apple's two happen
    # to be identical (133,050 both), which is precisely how you talk yourself into the
    # wrong field.
    "ebit": {"label": "EBIT", "field": "EBIT", "phrase": "EBIT"},
    # REPORTED NEGATIVE — it is an outflow. Apple -3,933; JPMorgan -101,350; Mitsui
    # Chemicals -14,702. We chart it as GuruFocus reports it and do NOT flip the sign:
    # a silent sign-flip is exactly the kind of "helpful" transform that makes a number
    # disagree with the source it claims to come from. The UI says which way it points.
    #
    # Zeros are REAL: Apple's last two fiscal years report 0 (it nets interest out), and
    # that is a value, not a hole — `_series` keeps it, and only "" is dropped.
    #
    # Unlike Gross profit and EBIT, a BANK DOES have this line (interest expense is a
    # bank's core cost), which is why `_has_line` is asked per-LINE and not per-template.
    "interest_expense": {
        "label": "Interest expense", "field": "Interest Expense",
        "phrase": "interest expense",
    },
    # Present in EVERY industry template, including the bank one — JPMorgan reports
    # Pretax Income 75,081 while having no EBIT and no gross profit at all. It is also
    # NOT EBIT: EBIT is before interest, pretax is after it (Mitsui Chemicals FY2026:
    # EBIT 85,035 vs Pretax 68,608 — the gap is the interest bill).
    "pretax_income": {
        "label": "Pretax income", "field": "Pretax Income",
        "phrase": "pretax income",
    },
    # GuruFocus calls this "Tax Provision" — there is NO "Income Tax" key. Like interest
    # expense it is REPORTED NEGATIVE (Apple -20,719; JPMorgan -16,610; Mitsui -29,018),
    # and it is charted as reported. The identity holds: JPM pretax 74,666 + tax -15,767
    # = 58,899 = Net Income. Present in every template, banks included.
    "income_tax": {
        "label": "Income tax", "field": "Tax Provision",
        "phrase": "income tax",
    },
    # THE BOTTOM LINE ATTRIBUTABLE TO SHAREHOLDERS. GuruFocus carries several "Net
    # Income" lines and they are NOT interchangeable:
    #
    #     Mitsui Chemicals FY2025:  pretax 68,608 + tax -21,698 = 46,910
    #         Net Income Including Noncontrolling Interests = 46,910   <- the arithmetic
    #         Net Income                                    = 34,378   <- the SHAREHOLDERS'
    #         (the 12,532 gap is minority interest)
    #
    # We chart `Net Income` — what an equity holder actually owns, and what EPS is built
    # from. So `pretax + tax` will NOT tie to this column for any company with minority
    # interests, and that is CORRECT: do not "fix" it by swapping in the Including-NCI
    # field. JPMorgan's two figures are identical (57,048 both, no minorities), so a check
    # against JPM alone would bless either choice — same shape as the EBIT /
    # Operating Income trap above.
    "net_income": {
        "label": "Net income", "field": "Net Income",
        "phrase": "net income",
    },
    # POSITIVE, unlike interest expense and tax — GuruFocus reports D&A as a magnitude
    # (Apple 11,698; JPMorgan 8,821; Mitsui Chemicals 104,744), so no sign caveat. Present
    # in every template, banks included (JPM has it despite having no EBIT or EBITDA).
    #
    # There is a DUPLICATE of this line in the cashflow section, keyed "Cash Flow
    # Depreciation, Depletion and Amortization". Its values are IDENTICAL (Apple: 11,698
    # both), and we read the income-statement one — `_SECTIONS` only looks there, so the
    # cashflow twin can never be picked up by accident.
    "depreciation_amort": {
        "label": "D&A", "field": "Depreciation, Depletion and Amortization",
        "phrase": "depreciation & amortization",
    },
    # PER SHARE, not millions — the first item that isn't. Apple 7.46 USD/share; JPMorgan
    # 20.02; Mitsui Chemicals 91.62 JPY/share. It ties out: Apple's net income 112,010M ÷
    # 15,004.697M diluted shares = 7.46. Still a CURRENCY amount, so the EUR conversion is
    # correct; only the scale and the axis label change. Present in every template, banks
    # included. Diluted, not basic — the conservative count, and what "EPS" means unqualified.
    "eps_diluted": {
        "label": "EPS (diluted)", "field": "EPS (Diluted)",
        "phrase": "diluted EPS", "unit": _UNIT_PER_SHARE,
    },
    # THE FIRST LINE FROM A DIFFERENT STATEMENT — it lives in the CASHFLOW section, not
    # the income statement, hence `section`. Every item before this one was implicitly
    # income-statement.
    #
    # ITS SIGN IS NOT A CONVENTION, IT IS THE ANSWER. Apple +111,482; JPMorgan -147,782.
    # A bank's operating cash flow routinely goes NEGATIVE as loans and trading assets
    # grow — that is what a bank looks like, not a bug and not an outflow-convention like
    # interest expense. So it is NOT in the frontend's NEGATIVE_BY_CONVENTION set: we
    # must not tell the user "this line is reported negative" when the sign is real
    # information that flips company by company.
    "operating_cash_flow": {
        "label": "Operating CF", "field": "Cash Flow from Operations",
        "phrase": "operating cash flow", "section": "cashflow",
    },
    # Cashflow statement, and NEGATIVE by convention — an outflow (Apple -12,715; Mitsui
    # Chemicals -137,759). The mapping is confirmed by the identity: Apple's OCF 111,482 +
    # capex -12,715 = 98,767 = GuruFocus's own "Free Cash Flow".
    #
    # NOT "Purchase Of Property, Plant, Equipment", which is a DIFFERENT number: Mitsui
    # reports PP&E purchases of -128,242 against capex of -137,759 — capex also picks up
    # intangibles. GuruFocus publishes "Capital Expenditure" explicitly; use it.
    #
    # JPMorgan reports 0, PRESENT rather than absent: a bank's capex is negligible here.
    # That is a value, not an N/A, and `_has_line` says so.
    "capex": {
        "label": "Capex", "field": "Capital Expenditure",
        "phrase": "capital expenditure", "section": "cashflow",
    },
    # COMPUTED. GuruFocus has NO "Total Debt" line — it carries the components separately,
    # so this is the first item that SUMS fields rather than reading one (`fields`, not
    # `field`). Balance sheet.
    #
    # It uses the "& Capital Lease Obligation" variants deliberately, because they are the
    # only ones that exist for EVERY template: JPMorgan has ONLY those two keys — its plain
    # "Short-Term Debt" / "Long-Term Debt" are absent — while for Apple and Mitsui the
    # plain and the "& Capital Lease" figures are IDENTICAL (Apple ST: 20,329 both). So the
    # lease variants lose nothing and are the only choice that works for a bank.
    #
    #     Apple      20,329 + 78,328  =  98,657
    #     JPMorgan   68,048 + 448,764 = 516,812
    #     Mitsui    296,727 + 443,464 = 740,191
    #
    # A period is only summed when EVERY present component has a value there. A partial sum
    # would silently UNDERSTATE the debt — the same discipline `_trailing_12m` uses.
    "total_debt": {
        "label": "Total debt",
        "fields": [
            "Short-Term Debt & Capital Lease Obligation",
            "Long-Term Debt & Capital Lease Obligation",
        ],
        "phrase": "total debt", "section": "balance", "combine": "sum",
        "note": ("Computed: short-term + long-term debt (each incl. capital lease "
                 "obligations). GuruFocus publishes no 'Total Debt' line."),
    },
    # COALESCED, not summed — `combine: "first"`. These are two NAMES for one quantity,
    # because GuruFocus renames the line per industry template:
    #
    #     Apple / Mitsui   "Cash and Cash Equivalents"                    (35,934)
    #     JPMorgan         "Balance Statement Cash and cash equivalents"  (343,338)
    #
    # A bank has NEITHER of the ordinary keys, so mapping only the first would N/A every
    # bank. And summing them (the total_debt mode) would double-count anyone carrying both
    # — which is exactly why `combine` is explicit rather than implied by `fields`.
    #
    # ⚠ NOT "Cash, Cash Equivalents, Marketable Securities", which is a DIFFERENT and much
    # larger number (Apple: 54,697 against cash-only 35,934). "Cash and equivalents" means
    # the narrow line; the broad one is a separate concept and deserves its own column if
    # anyone wants it.
    "cash_and_equivalents": {
        "label": "Cash & equiv.",
        "fields": [
            "Cash and Cash Equivalents",
            "Balance Statement Cash and cash equivalents",   # the bank template's name
        ],
        "phrase": "cash and equivalents", "section": "balance", "combine": "first",
    },
    # THE SHAREHOLDERS' line — the exact same trap as `net_income`, one statement over.
    # GuruFocus carries BOTH, and they are not interchangeable:
    #
    #     Mitsui Chemicals:  Total Stockholders Equity   864,727   <- the SHAREHOLDERS'
    #                        Minority Interest           124,057
    #                        Total Equity                988,784   <- incl. minorities
    #
    # We chart `Total Stockholders Equity` — what an equity holder actually owns, and the
    # denominator of book value per share and ROE. JPMorgan's two figures are IDENTICAL
    # (362,438 both; zero minority interest), so a check against JPM alone would bless
    # either choice — exactly as it would have for net income.
    "shareholders_equity": {
        "label": "Equity", "field": "Total Stockholders Equity",
        "phrase": "shareholders' equity", "section": "balance",
    },
    # A COUNT, in MILLIONS OF SHARES — not currency (Apple 15,004.697 = 15.0 billion
    # shares; JPMorgan 2,781.5; Mitsui Chemicals 375.212). So `unit: shares`, which SKIPS
    # the EUR conversion: "15,004.697 million shares ÷ 1.17 EUR/USD" is not a quantity.
    # The modal renders ONE chart for it, not the usual native+EUR pair.
    #
    # DILUTED AVERAGE — the same basis as `eps_diluted`, which is what makes the two tie:
    # Apple 7.46 x 15,004.697 = 111,935 ≈ net income 112,010 (the gap is diluted-vs-basic
    # rounding). GuruFocus publishes no other share count in the financials blob.
    "shares_outstanding": {
        "label": "Shares out.", "field": "Shares Outstanding (Diluted Average)",
        "phrase": "shares outstanding", "unit": _UNIT_SHARES,
    },
    # THE FIRST FORECAST, and the first line from a SOURCE other than `financials`:
    # `stock/{sym}/analyst_estimate` (SINGULAR — the plural is the router fallback).
    #
    # These are analyst CONSENSUS estimates, not reported results. Apple: FY2026-09 8.76,
    # rising to 14.43 by FY2030. Every date is in the FUTURE, which has one consequence
    # worth stating: we hold no FX rate for a future day, so `load_fx_rates` forward-fills
    # and the EUR line converts at the LATEST KNOWN rate. That is the only honest choice —
    # nobody knows the 2030 EUR/USD — but it means the EUR panel carries today's FX
    # assumption, not a forecast one. The modal says so.
    #
    # `per_share_eps_estimate`, not `eps_nri_estimate` (which excludes non-recurring
    # items; Apple 8.77 vs 8.76 — near-identical here, which is exactly why picking by
    # eye off one company is unsafe).
    "forward_eps": {
        "label": "Forward EPS", "field": "per_share_eps_estimate",
        "phrase": "forward EPS", "unit": _UNIT_PER_SHARE,
        "source": _SOURCE_ESTIMATES,
        "note": ("Analyst CONSENSUS estimate, not a reported result. Future periods carry "
                 "no FX rate, so the EUR line converts at the latest known rate."),
    },
    # A RATE, in PERCENT — the unit the ratio ban has been waiting for. It is not currency,
    # so it reuses the `shares` mechanism: no EUR conversion, one chart.
    #
    # DERIVED, and it has to be: GuruFocus's own `future_revenue_estimate_growth` is a
    # SCALAR — one long-term rate (Apple 10.09, NVIDIA 45.73) — not a series, so there is
    # nothing to plot. The per-period growth is computed from `revenue_estimate`, with the
    # FIRST forecast year measured against the last REPORTED revenue (Apple: est FY2026
    # 477,600 against actual FY2025 416,161 = +14.8%). That anchor is why this item reads
    # BOTH blobs — the estimates for the numerator, the financials for the first base.
    #
    # Note our first-year figure will NOT equal GuruFocus's scalar: theirs is a long-run
    # average, ours is the actual year-on-year step the consensus implies.
    "revenue_growth_est": {
        "label": "Rev growth (est)", "field": "revenue_estimate",
        "phrase": "estimated revenue growth", "unit": _UNIT_PERCENT,
        "source": _SOURCE_ESTIMATES, "derive": "yoy_growth", "base_field": "Revenue",
        "note": ("Derived: year-over-year change in the consensus REVENUE estimates. The "
                 "first forecast year is measured against the last REPORTED revenue. "
                 "GuruFocus's own 'future_revenue_estimate_growth' is a single long-term "
                 "rate, not a series, so it cannot be charted."),
    },
    # A SCALAR — the first item with NO series at all. The analysts' long-term EPS growth
    # consensus is ONE number (Apple 13.01, NVIDIA 47.57, JPMorgan 8.66), and GuruFocus
    # publishes it as one. There is no date attached to it, and inventing one so it could
    # be plotted would dress a single point up as a trend. The UI shows the number.
    #
    # `long_term_growth_rate_mean` is the consensus LTG itself. Its two neighbours are NOT
    # the same thing and are close enough to be mistaken for it:
    #     future_per_share_eps_estimate_growth   13.03 / 45.72 / 8.23  (CAGR implied by the
    #                                                                   estimate SERIES)
    #     future_eps_nri_estimate_growth         13.01 / 47.57 / 8.66  (the NRI variant)
    # Apple's three are 13.01 / 13.03 / 13.01 — indistinguishable by eye, and NVIDIA's
    # spread (47.57 vs 45.72) is where they part company.
    "eps_lt_growth_est": {
        "label": "EPS LTG (est)", "field": "long_term_growth_rate_mean",
        "phrase": "long-term EPS growth estimate", "unit": _UNIT_PERCENT,
        "source": _SOURCE_ESTIMATES, "scalar": True,
        "note": ("Analyst CONSENSUS long-term EPS growth rate — a single figure, not a "
                 "series, so there is nothing to chart. It is a forecast of the growth "
                 "RATE, not of earnings."),
    },
    # ⚠ DO NOT add "Tax Rate %", "Gross Margin %", "Debt-to-Equity" or any other RATIO to
    # this registry.
    # A `unit` fixes a SCALE mismatch (per-share vs millions), but a percentage is not a
    # currency at all: `_convert_to_eur` divides every value by an FX rate, so
    # "Tax Rate % = 15.61" would come back as "13.3% in EUR", which is meaningless. A
    # ratio column needs the conversion SKIPPED, not merely relabelled — different change.
}

# `financial_template_parameters.ind_template` — the INDUSTRY template GuruFocus renders
# the income statement with, and it decides which lines EXIST at all.
#
# A bank has no gross profit. JPMorgan (`ind_template: 'B'`) reports Interest Income,
# Interest Expense and Net Interest Income; there is no "Gross Profit" key in its income
# statement, because the concept doesn't apply — a bank has no cost of goods sold. That
# is a fact about the industry, NOT a hole in GuruFocus's data, and the UI must not badge
# it "NO DATA" as though the number were merely missing.
_TEMPLATES = {"N": "normal", "B": "bank", "I": "insurance", "R": "REIT"}

# The fiscal axis. GuruFocus labels the QUARTERLY block's axis "Fiscal Year" too — not
# "Fiscal Quarter" — so keying off the cadence name silently yields an empty quarterly
# series for every company (which is exactly what it did until this was probed). Try the
# candidates in order, same as `ingest/earnings/financials.py` already does.
_PERIOD_AXES = ("Fiscal Year", "Fiscal Quarter", "Quarter", "Date", "date")

# A fund holds securities; it does not operate a business, so "revenue" is not a small
# number, it is a category error. GuruFocus returns null for these. Answered locally.
_FUND_PRODUCTS = frozenset({"ETF", "FUNDS"})
_FUND_CLASSES = frozenset({"etf", "crypto", "commodity"})


class FinancialPoint(BaseModel):
    date: str                      # fiscal period END (YYYY-MM -> month end)
    value: float                   # MILLIONS, in `currency`
    value_eur: float | None = None
    fx_rate: float | None = None   # units of `currency` per 1 EUR


class FinancialSeriesResponse(BaseModel):
    """One income-statement line, in MILLIONS of `currency` — the LISTING's trading
    currency, because that is what GuruFocus converts the financials into (see the
    module header)."""

    item: str                      # registry key, e.g. "gross_profit"
    label: str                     # human label, e.g. "Gross profit"
    # The label as it reads inside a sentence ("EBIT", not "ebit"). The UI needs this
    # for its N/A copy, and `label.lower()` would mangle the acronym.
    phrase: str = ""
    # "millions" (a currency amount in millions) or "per_share" (a currency amount per
    # share — EPS). BOTH are currency, so `value_eur` is valid either way; the unit only
    # decides the SCALE and the axis label. Rendering EPS on the millions path turns
    # $7.46 a share into "$7.46 million" — plausible-looking and off by 1e6.
    unit: str = "millions"
    symbol: str | None = None
    currency: str | None = None
    company_id: int | None = None
    is_home: bool = True
    annual: list[FinancialPoint]
    quarterly: list[FinancialPoint]
    fetched: bool = False
    fx_from: str | None = None
    # The company HAS financials, but this LINE does not exist in its industry template.
    # A bank has no gross profit — JPMorgan (ind_template 'B') reports Net Interest
    # Income and has no cost of goods sold, so there is no such line to report. That is
    # a fact about banking, not a missing number, and the UI says so instead of drawing
    # an empty chart or badging NO DATA.
    applicable: bool = True
    template: str | None = None    # "bank", "insurance", "REIT", "normal"
    # A SCALAR item has no series at all — `annual`/`quarterly` are empty and this carries
    # the single value. GuruFocus publishes the long-term EPS growth consensus as ONE
    # number (Apple 13.01), not a timeseries, and inventing a date for it so it could be
    # "charted" would be dressing a point up as a trend. The UI shows the number.
    scalar_value: float | None = None
    # Set when the series is COMPUTED rather than read straight from GuruFocus (total debt
    # = short-term + long-term; there is no "Total Debt" line). The UI shows it, because a
    # derived number must say it is derived.
    note: str | None = None


def _fetch_financials_raw(ticker: str, exchange: str, *, force: bool) -> tuple[dict | None, bool]:
    """The raw `financials` blob, Storage-cached. `(blob, hit_the_api)`.

    Shares the `{EXCHANGE}_{TICKER}/financials.json` path with the earnings pipeline, so
    a company the earnings dashboard already pulled costs ZERO extra calls here.
    """
    from urllib.parse import quote  # noqa: PLC0415

    from deps import supabase  # noqa: PLC0415
    from ingest.api_usage import track_api_call  # noqa: PLC0415
    from ingest.earnings._api_client import _api_request, _build_api_url  # noqa: PLC0415
    from ingest.earnings._common import (  # noqa: PLC0415
        _build_symbol,
        _ensure_bucket,
        _fetch_from_storage,
        _storage_path,
        _upload_to_storage,
    )

    _ensure_bucket(supabase)
    path = _storage_path(ticker, exchange, "financials")

    if not force:
        cached = _fetch_from_storage(supabase, path)
        if isinstance(cached, dict) and cached.get("financials"):
            return cached, False

    symbol = _build_symbol(ticker, exchange)
    api = _api_request(_build_api_url(f"stock/{quote(symbol, safe=':')}/financials"))
    track_api_call(supabase, exchange)
    if api.is_forbidden:
        raise HTTPException(403, f"403 unsubscribed region for {symbol}")
    if not isinstance(api.data, dict) or not api.data.get("financials"):
        # `null` — GuruFocus has no financials for this symbol. For a fund that is the
        # correct answer; for an equity it's a gap. Either way it is not a server fault.
        return None, True
    _upload_to_storage(supabase, path, api.data)
    return api.data, True


def _period_end(label: str) -> str | None:
    """`'2025-12'` -> `'2025-12-31'`. `'TTM'` -> None.

    The fiscal axis ends with the literal string "TTM", which is not a period: it is a
    rolling window that duplicates the latest year. Plotting it puts a phantom point on
    top of the final bar, and `date.fromisoformat("TTM")` simply raises.
    """
    import calendar  # noqa: PLC0415

    s = (label or "").strip()
    if len(s) != 7 or s[4] != "-":
        return None                       # "TTM", "", anything malformed
    try:
        y, m = int(s[:4]), int(s[5:])
        return f"{y:04d}-{m:02d}-{calendar.monthrange(y, m)[1]:02d}"
    except ValueError:
        return None


def _has_line(blob: dict, field: str, section: str = _DEFAULT_SECTION) -> bool:
    """Does this statement CONTAIN this line at all?

    Distinct from "the line is there but empty". A bank's statement simply has no
    `Gross Profit` key (JPMorgan, ind_template 'B'), and that must not read as a data
    gap — it's what a bank's income statement looks like."""
    for cadence in ("annuals", "quarterly"):
        block = ((blob.get("financials") or {}).get(cadence) or {})
        for name in _SECTION_ALIASES.get(section, _SECTIONS):
            sec = block.get(name)
            if isinstance(sec, dict) and field in sec:
                return True
    return False


def _series(
    blob: dict, cadence: str, field: str = "Revenue", section: str = _DEFAULT_SECTION,
) -> list[FinancialPoint]:
    """Pull one cadence of one line item out of the blob, dropping TTM and holes.

    Accepts BOTH schema shapes (see `_SECTIONS`) — a cached blob predating GuruFocus's
    rename would otherwise read as "no data".

    NOTE on ordering: the cached (old) blobs run NEWEST-FIRST and the live ones
    OLDEST-FIRST, both with `TTM` appended last. Labels and values share the ordering and
    the length, so `zip` pairs correctly either way and the sort below normalises. Do not
    "fix" the order by reversing one side."""
    block = ((blob.get("financials") or {}).get(cadence) or {})
    labels: list = []
    for axis in _PERIOD_AXES:
        if isinstance(block.get(axis), list) and block[axis]:
            labels = block[axis]
            break
    values: list = []
    for name in _SECTION_ALIASES.get(section, _SECTIONS):
        values = (block.get(name) or {}).get(field) or []
        if values:
            break

    out: list[FinancialPoint] = []
    for label, raw in zip(labels, values):
        day = _period_end(label)
        if not day:
            continue
        try:
            v = float(raw)
        except (TypeError, ValueError):
            continue                      # GuruFocus uses "" for a period it lacks
        out.append(FinancialPoint(date=day, value=v))
    out.sort(key=lambda p: p.date)
    return out


def _fetch_estimates_raw(ticker: str, exchange: str, *, force: bool) -> tuple[dict | None, bool]:
    """The raw `analyst_estimate` blob, Storage-cached. `(blob, hit_the_api)`.

    Shares the `{EXCHANGE}_{TICKER}/analyst_estimate.json` path with the earnings
    pipeline, so a company it already pulled costs ZERO extra calls here."""
    from urllib.parse import quote  # noqa: PLC0415

    from deps import supabase  # noqa: PLC0415
    from ingest.api_usage import track_api_call  # noqa: PLC0415
    from ingest.earnings._api_client import _api_request, _build_api_url  # noqa: PLC0415
    from ingest.earnings._common import (  # noqa: PLC0415
        _build_symbol,
        _ensure_bucket,
        _fetch_from_storage,
        _storage_path,
        _upload_to_storage,
    )

    _ensure_bucket(supabase)
    path = _storage_path(ticker, exchange, "analyst_estimate")

    if not force:
        cached = _fetch_from_storage(supabase, path)
        if isinstance(cached, dict) and cached.get("annual"):
            return cached, False

    symbol = _build_symbol(ticker, exchange)
    # SINGULAR — the plural is the router fallback. See `_SOURCE_ESTIMATES`.
    api = _api_request(_build_api_url(f"stock/{quote(symbol, safe=':')}/analyst_estimate"))
    track_api_call(supabase, exchange)
    if api.is_forbidden:
        raise HTTPException(403, f"403 unsubscribed region for {symbol}")
    if not isinstance(api.data, dict) or not api.data.get("annual"):
        return None, True
    _upload_to_storage(supabase, path, api.data)
    return api.data, True


def _period_end_compact(label: str) -> str | None:
    """`'202609'` -> `'2026-09-30'`. The estimate blob's date format, which is NOT the
    financials blob's `'2025-09'` — six digits, no separator."""
    import calendar  # noqa: PLC0415

    s = str(label or "").strip()
    if len(s) != 6 or not s.isdigit():
        return None
    y, m = int(s[:4]), int(s[4:])
    if not 1 <= m <= 12:
        return None
    return f"{y:04d}-{m:02d}-{calendar.monthrange(y, m)[1]:02d}"


def _estimate_scalar(blob: dict, field: str) -> float | None:
    """A single number out of the estimate blob — NOT a series.

    The long-term growth consensus is one figure (Apple 13.01, NVIDIA 47.57), and the same
    dict mixes it in with the array fields. Anything that IS a list is rejected here rather
    than silently taking `[0]`: a scalar reader must not quietly consume a series."""
    v = (blob.get("annual") or {}).get(field)
    if isinstance(v, list) or v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _estimate_series(blob: dict, cadence: str, field: str) -> list[FinancialPoint]:
    """One field out of the analyst-estimate blob.

    Only LIST values are usable: the same dict also holds scalars
    (`long_term_growth_rate_mean` is a bare float), so indexing blindly raises."""
    block = blob.get(_ESTIMATE_BLOCKS.get(cadence, cadence)) or {}
    dates = block.get("date")
    values = block.get(field)
    if not isinstance(dates, list) or not isinstance(values, list):
        return []

    out: list[FinancialPoint] = []
    for label, raw in zip(dates, values):
        day = _period_end_compact(label)
        if not day:
            continue
        try:
            out.append(FinancialPoint(date=day, value=float(raw)))
        except (TypeError, ValueError):
            continue
    out.sort(key=lambda p: p.date)
    return out


def _yoy_growth_series(
    est: dict, fin: dict | None, cadence: str, field: str, base_field: str,
) -> list[FinancialPoint]:
    """Year-over-year % change of a forward estimate series.

    WHY DERIVE IT AT ALL: GuruFocus's own `future_revenue_estimate_growth` is a SCALAR —
    one long-term rate (Apple 10.09, NVIDIA 45.73), not a series. There is nothing to plot.
    The per-period growth the estimates actually imply has to be computed from
    `revenue_estimate` itself.

    THE FIRST POINT IS THE INTERESTING ONE, and it needs a base from OUTSIDE the estimate
    blob: growth into the first forecast year is measured against the last REPORTED year
    (Apple: est FY2026 477,600 vs actual FY2025 416,161 = +14.8%). Without that anchor the
    first forecast year has no predecessor and would simply be dropped — losing the number
    most people came for. Hence `fin`, the financials blob, alongside `est`.

    The base is used only if it genuinely PRECEDES the first estimate; otherwise it is
    ignored rather than producing a nonsense negative "growth" across an overlap.
    """
    est_pts = _estimate_series(est, cadence, field)
    if not est_pts:
        return []

    prior: float | None = None
    if fin is not None:
        reported = _series(fin, cadence, base_field, _DEFAULT_SECTION)
        if reported and reported[-1].date < est_pts[0].date and reported[-1].value:
            prior = reported[-1].value

    out: list[FinancialPoint] = []
    for p in est_pts:
        if prior:                       # None or 0 -> no meaningful growth
            out.append(FinancialPoint(date=p.date, value=(p.value / prior - 1.0) * 100.0))
        prior = p.value
    return out


def _coalesced_series(
    blob: dict, cadence: str, fields: list[str], section: str,
) -> list[FinancialPoint]:
    """The FIRST of these lines this company actually has — alternative spellings, not
    components to add.

    GuruFocus renames a line per industry template: cash is `Cash and Cash Equivalents`
    for Apple and Mitsui, but a bank has no such key — JPMorgan calls it
    `Balance Statement Cash and cash equivalents`. These are the SAME quantity under two
    names, so they must be coalesced. Summing them (the `total_debt` mode) would
    double-count for any company that happened to carry both.
    """
    for field in fields:
        if _has_line(blob, field, section):
            return _series(blob, cadence, field, section)
    return []


def _summed_series(
    blob: dict, cadence: str, fields: list[str], section: str,
) -> list[FinancialPoint]:
    """Sum several lines into one series — e.g. total debt = short-term + long-term.

    Only the components that EXIST in this company's statement are summed (a bank has no
    plain "Short-Term Debt" key), and a period is emitted only when EVERY present component
    has a value at that date. A period where one component is a hole is DROPPED, not summed
    as if the missing part were zero: a partial total silently understates the debt, and
    understated debt that looks like a real number is worse than a gap in the line.
    """
    present = [f for f in fields if _has_line(blob, f, section)]
    if not present:
        return []

    by_date: dict[str, list[float]] = {}
    for field in present:
        for p in _series(blob, cadence, field, section):
            by_date.setdefault(p.date, []).append(p.value)

    return [
        FinancialPoint(date=day, value=sum(values))
        for day, values in sorted(by_date.items())
        if len(values) == len(present)          # every component reported that period
    ]


def _convert_to_eur(points: list[FinancialPoint], currency: str | None) -> str | None:
    """Fill value_eur/fx_rate in place, each period at ITS OWN period-end rate. Returns
    the FX coverage start; points before it keep `value_eur=None` and the chart draws a
    gap rather than a zero."""
    from datetime import date as _date  # noqa: PLC0415

    from deps import supabase  # noqa: PLC0415
    from momentum.data.fx import load_fx_rates  # noqa: PLC0415

    from ._asset_dividends import _backfill_fx_history, _fx_asof  # noqa: PLC0415

    ccy = (currency or "").upper()
    if not points:
        return None
    if not ccy or ccy == "EUR":
        for p in points:
            p.value_eur, p.fx_rate = p.value, 1.0
        return None

    lo, hi = points[0].date, points[-1].date
    start = _backfill_fx_history(ccy, lo)
    if not start:
        return None
    series = load_fx_rates(
        supabase, [ccy], _date.fromisoformat(lo), _date.fromisoformat(hi),
    ).get(ccy)
    for p in points:
        rate = None if (start and p.date < start) else _fx_asof(series, p.date)
        if rate is None:
            p.value_eur, p.fx_rate = None, None
        else:
            # fx_rate is units of `currency` per 1 EUR, so divide (same convention as
            # _schedule_snapshots._to_eur and the dividend column).
            p.value_eur, p.fx_rate = round(p.value / rate, 3), rate
    return start


def resolve_gf_listing(isin: str, *, phrase: str = "this") -> dict:
    """Which GuruFocus listing answers for this ISIN, via whichever of the two bridges reaches it.

    `{company_id, ticker, exchange, currency, is_home}` — `currency` being the listing's TRADING
    currency, which is what GuruFocus converts the financials into (see the module docstring).

    Extracted so `_asset_fundamentals` can reach the same listing by the same rule. A second copy
    of this would be a second answer to "which GuruFocus listing IS this ISIN", and the two would
    disagree the first time `pick_listing`'s scoring changed — the fundamentals modal would then
    chart one listing's numbers under another listing's name.

    `phrase` only shapes the fund refusal's wording ("a fund has no EBIT").
    """
    from deps import supabase  # noqa: PLC0415

    from ._asset_dividends import (  # noqa: PLC0415
        _UNRESOLVED_REASON,
        _asset_row,
        _exchange,
        _exchange_by_code,
        _resolve_listing,
    )

    asset = _asset_row(isin)
    product = (asset.get("leonteq_product_type") or "").upper()
    if product in _FUND_PRODUCTS:
        raise HTTPException(404, _FUND_REASON.format(phrase=phrase))

    co = (supabase.table("company")
          .select("company_id, gurufocus_ticker, exchange_id, has_financials")
          .eq("isin", isin).limit(1).execute().data or [])

    if co:
        company_id = co[0]["company_id"]
        ticker = co[0].get("gurufocus_ticker")
        exchange, currency = _exchange(co[0].get("exchange_id"))
        if not ticker:
            raise HTTPException(422, f"company {company_id} has no gurufocus_ticker")
        return {"company_id": company_id, "ticker": ticker, "exchange": exchange,
                "currency": currency, "is_home": True}

    row = _resolve_listing(isin)
    status = row.get("status") or "ok"
    if status != "ok" or not row.get("gurufocus_ticker"):
        reason = _UNRESOLVED_REASON.get(status, f"unresolved ISIN ({status})")
        symbol = f"{row.get('exchange_code')}:{row.get('gurufocus_ticker')}"
        raise HTTPException(404, reason.format(symbol=symbol) if "{symbol}" in reason else reason)
    exchange = row["exchange_code"]
    _, currency = _exchange_by_code(exchange)
    return {"company_id": None, "ticker": row["gurufocus_ticker"], "exchange": exchange,
            "currency": currency, "is_home": bool(row.get("is_home"))}


def _line_item_for_isin(isin: str, item: str, *, force: bool = False) -> FinancialSeriesResponse:
    """One income-statement line, via whichever bridge reaches this ISIN."""
    spec = _ITEMS.get(item)
    if not spec:
        raise HTTPException(404, f"unknown line item '{item}' (have: {', '.join(_ITEMS)})")

    _resolved = resolve_gf_listing(isin, phrase=spec["phrase"])
    company_id = _resolved["company_id"]
    ticker, exchange = _resolved["ticker"], _resolved["exchange"]
    currency, is_home = _resolved["currency"], _resolved["is_home"]

    source = spec.get("source", _SOURCE_FINANCIALS)
    if source == _SOURCE_ESTIMATES:
        # A different endpoint entirely — forward consensus, not reported history. It does
        # NOT touch `has_financials`: that flag is about the financials blob, and a company
        # can have one without the other.
        est, hit_api = _fetch_estimates_raw(ticker, exchange, force=force)
        base = dict(
            item=item, label=spec["label"], phrase=spec["phrase"],
            unit=spec.get("unit", _UNIT_MILLIONS), note=spec.get("note"),
            symbol=f"{exchange}:{ticker}", currency=currency,
            company_id=company_id, is_home=is_home, fetched=hit_api, template=None,
        )
        if est is None:
            raise HTTPException(404, _NO_ESTIMATES.format(symbol=f"{exchange}:{ticker}"))

        if spec.get("scalar"):
            # No series exists. Returning an empty one would render as "no data"; the value
            # is right there, it simply is not a timeseries.
            value = _estimate_scalar(est, spec["field"])
            if value is None:
                return FinancialSeriesResponse(
                    annual=[], quarterly=[], applicable=False, **base)
            return FinancialSeriesResponse(
                annual=[], quarterly=[], scalar_value=value, **base)

        if spec.get("derive") == "yoy_growth":
            # Needs the REPORTED history too, to anchor the first forecast year. Both blobs
            # are Storage-cached, so this is usually zero extra API calls.
            fin, fin_hit = _fetch_financials_raw(ticker, exchange, force=force)
            hit_api = hit_api or fin_hit
            # NOT `base` — that name is already the response-kwargs dict below.
            base_field = spec["base_field"]
            annual = _yoy_growth_series(est, fin, "annuals", spec["field"], base_field)
            quarterly = _yoy_growth_series(est, fin, "quarterly", spec["field"], base_field)
        else:
            annual = _estimate_series(est, "annuals", spec["field"])
            quarterly = _estimate_series(est, "quarterly", spec["field"])
        if not annual and not quarterly:
            # GuruFocus has the blob but no consensus for this line — nobody covers it.
            return FinancialSeriesResponse(annual=[], quarterly=[], applicable=False, **base)

        fx_from = None
        if spec.get("unit", _UNIT_MILLIONS) in _CURRENCY_UNITS:
            fx_from = _convert_to_eur(annual, currency)
            _convert_to_eur(quarterly, currency)
        return FinancialSeriesResponse(
            annual=annual, quarterly=quarterly, fx_from=fx_from, **base,
        )

    blob, hit_api = _fetch_financials_raw(ticker, exchange, force=force)
    if blob is None:
        _mark_financials(isin, company_id, has=False)
        raise HTTPException(404, _NO_FINANCIALS.format(symbol=f"{exchange}:{ticker}"))

    # We HAVE financials for this company — that flag is about the blob, not about any
    # one line inside it, so record it before asking whether this particular line exists.
    _mark_financials(isin, company_id, has=True)

    params = (blob.get("financials") or {}).get("financial_template_parameters") or {}
    template = _TEMPLATES.get(str(params.get("ind_template") or "").upper())

    section = spec.get("section", _DEFAULT_SECTION)
    # A COMPUTED item (`fields`) sums several lines; a plain one (`field`) reads one.
    fields: list[str] = spec.get("fields") or [spec["field"]]
    base = dict(
        item=item, label=spec["label"], phrase=spec["phrase"],
        unit=spec.get("unit", _UNIT_MILLIONS), note=spec.get("note"),
        symbol=f"{exchange}:{ticker}", currency=currency,
        company_id=company_id, is_home=is_home, fetched=hit_api, template=template,
    )
    # Applicable when ANY component exists: a bank has no plain "Short-Term Debt" key but
    # does have the capital-lease variant, and that is still a real total debt.
    if not any(_has_line(blob, f, section) for f in fields):
        # The line does not exist in this industry's statement — a bank has no gross
        # profit. Not an error, not a gap: an answer. 200, with `applicable=False`.
        return FinancialSeriesResponse(annual=[], quarterly=[], applicable=False, **base)

    # "sum" adds components (total debt = short-term + long-term); "first" coalesces
    # alternative SPELLINGS of one line (a bank names its cash line differently). Getting
    # these backwards either double-counts or N/As a whole template, so it is declared.
    combine = _summed_series if spec.get("combine", "sum") == "sum" else _coalesced_series
    annual = combine(blob, "annuals", fields, section)
    quarterly = combine(blob, "quarterly", fields, section)

    # Convert ONLY the units that are currency. A share count is not: dividing
    # "15,004.697 million shares" by an FX rate yields a number that means nothing, so
    # `value_eur` stays null and the UI shows a single chart. Both cadences share one
    # currency, so one FX pull serves both.
    unit = spec.get("unit", _UNIT_MILLIONS)
    fx_from = None
    if unit in _CURRENCY_UNITS:
        fx_from = _convert_to_eur(annual, currency)
        _convert_to_eur(quarterly, currency)

    return FinancialSeriesResponse(
        annual=annual, quarterly=quarterly, fx_from=fx_from, **base,
    )


def _mark_financials(isin: str, company_id: int | None, *, has: bool) -> None:
    """Persist the three-valued flag so the grid can badge NO DATA without a re-fetch."""
    from deps import supabase  # noqa: PLC0415

    table, key, val = (
        ("company", "company_id", company_id) if company_id is not None
        else ("gurufocus_listing", "isin", isin)
    )
    try:
        supabase.table(table).update({"has_financials": has}).eq(key, val).execute()
    except Exception:  # noqa: BLE001,S110 -- a cache flag must never fail the request
        pass


_FUND_REASON = (
    "A fund has no {phrase}: it HOLDS securities, it does not operate a business. "
    "GuruFocus agrees — it returns no financials for an ETF at all. This is a category "
    "error, not a data gap, so no API call is spent asking."
)
_NO_FINANCIALS = (
    "GuruFocus resolved this ISIN to {symbol} but holds no financial statements for that "
    "listing — typically a dead OTC line of an acquired or delisted company. A gap, not "
    "a statement that the company earns nothing."
)
_NO_ESTIMATES = (
    "GuruFocus holds no analyst estimates for {symbol}. Forward figures are a CONSENSUS of "
    "analysts covering the stock — an uncovered company simply has none, which is an "
    "answer about coverage, not a gap in its accounts."
)


@router.get("/api/asset-pipeline/financials/isin/{isin}/{item}",
            response_model=FinancialSeriesResponse)
async def financial_line_by_isin(isin: str, item: str, refresh: bool = False):
    """One income-statement line (MILLIONS, listing currency + EUR) for any grid row.

    `item` is a key of `_ITEMS` — `revenue`, `gross_profit`. Adding a column is adding an
    entry there; there is no per-item endpoint to write.

    One GuruFocus call, and only when the Storage cache is stale — shared with the
    earnings pipeline's `financials.json`, so a company already pulled there is FREE, and
    the second line item on the same company is free regardless (one blob, every line).

    200 with `applicable=false` when the company's industry template has no such line (a
    bank has no gross profit); 404 only when there is genuinely nothing to show."""
    return await asyncio.to_thread(_line_item_for_isin, isin, item, force=refresh)


class FundamentalPoint(BaseModel):
    date: str
    value: float


class FundamentalSeries(BaseModel):
    field: str
    label: str
    points: list[FundamentalPoint] = []
    # ⚠ `dropped` IS THE HONEST PART. GuruFocus writes "" / "N/A" / "-" for a period it has no
    # value for, and those are far commoner in the ratio sections than in the statements: on a
    # real blob Piotroski has 17 points where Revenue has 24, Interest Coverage 20, GF Value 11.
    # A loss year HAS no PE. Read a 17-point line as a 24-year history and you are reading only
    # the periods that worked.
    period_count: int = 0
    dropped: int = 0
    # How many of the DRAWN points are <= 0. A fair value <= 0 is an ANSWER (Peter Lynch needs
    # positive earnings growth; EPV <= 0 says the business earns nothing) — a quarter of the band
    # is <= 0 in-window on a real sample. It stays in the payload; a LOG axis cannot plot it, so
    # the chart breaks its line there rather than bridging a decade the method had no value for.
    non_positive: int = 0


class QualityMetric(BaseModel):
    """One of the four quality numbers, and its verdict.

    ⚠ FOUR STATES, AND ONLY ONE OF THEM IS "BAD".
        ok       measured, and it passes
        fail     measured, and it does not
        n_a      the LINE DOES NOT EXIST for this company. A bank has no ROIC and no gross margin
                 at all (JPMorgan, template 'B' — structurally absent, not empty), so two of the
                 four are inapplicable to one. That is an answer about the industry template.
        unknown  the line exists but there is too little history to say — a 10y median off three
                 points is not a median.
    Collapsing `n_a` or `unknown` into `fail` marks every bank a bad business and every young
    company a suspect one.
    """

    key: str
    label: str
    unit: str = ""
    value: float | None = None
    periods: int = 0
    status: str = "unknown"          # ok | fail | n_a | unknown
    note: str | None = None


class FundamentalsResponse(BaseModel):
    """The four soundness charts, off ONE cached blob plus one yfinance price read."""

    isin: str
    symbol: str                       # GuruFocus's EXCHANGE:TICKER
    company_id: int | None = None
    currency: str | None = None       # the GuruFocus listing's trading currency
    yahoo_symbol: str | None = None
    price_currency: str | None = None
    is_home: bool = True
    template: str | None = None
    cadence: str = "annuals"
    period_count: int = 0
    fetched: bool = False

    # Chart 1. `price_eur` is YFINANCE, DAILY — never GuruFocus. The band is GuruFocus's
    # per-share fair values converted to the SAME EUR, because GF denominates them in ITS
    # listing's currency and that listing need not be the one we price.
    price_eur: list[FundamentalPoint] = []
    fair_values_eur: list[FundamentalSeries] = []
    # Never drawn — GuruFocus's own month-end price in EUR. If it and `price_eur` diverge after
    # FX, the ISIN reached two different securities and the band belongs to the other one.
    price_crosscheck_eur: list[FundamentalPoint] = []

    # Charts 2-4: percentages and scores. No EUR leg exists — a ROIC of 18% is 18% everywhere.
    yields: list[FundamentalSeries] = []
    returns: list[FundamentalSeries] = []
    safety: list[FundamentalSeries] = []

    # The four-number quality verdict — does it create value, is the moat melting, is the profit
    # real, is there pricing power. Deliberately NOT a composite score: a single 0-100 hides the
    # disagreement between them, which is the part worth reading.
    quality: list[QualityMetric] = []

    has_roic: bool = True
    has_earnings_yield: bool = True


@router.get("/api/asset-pipeline/fundamentals/isin/{isin}",
            response_model=FundamentalsResponse)
async def fundamentals_by_isin(isin: str, cadence: str = "annuals"):
    """Everything the four soundness charts need, in ONE call off ONE cached blob.

    Price vs fair value · yield · ROIC vs WACC · safety. Free for any company that already has a
    financials column (the blob carries 262 line items; the charts are reads).

    ⚠ The price line is yfinance daily, in EUR, and never GuruFocus's — /portfolios prices
    everything from `asset_price`, and a second vendor on that page would compare two price
    universes. Both legs of chart 1 are therefore EUR; see `_asset_fundamentals`.
    """
    from routers._asset_fundamentals import compute_fundamentals_async  # noqa: PLC0415

    return await compute_fundamentals_async(isin, cadence)
