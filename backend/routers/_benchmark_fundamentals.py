"""What GuruFocus data we actually hold, per constituent — the RAW lines the Long Equity charts
are computed from, and the period span we have for each.

WHAT THIS ANSWERS
    Not "what is this company's margin" — the charts do that. This answers the question underneath
    it: *can* a chart be drawn for this company, and over what years. Every card on the Long Equity
    tab is a ratio of two or three of these lines, so a missing or short line is exactly why a
    chart is blank or starts late, and there is otherwise no way to see it without opening each
    company one at a time.

⚠ ONE COLUMN PAIR PER LINE, NOT A DERIVED FIGURE. A `from`/`to` says what exists; a value would
    say what it is, which is the charts' job. The pair is what makes the table a coverage report:
    after a backfill you can see the spans appear, and a line that reads 2021-2025 where its
    neighbours read 2015-2025 is the reason a nine-year chart has a four-year trend.

⚠ THE LINES ARE THE ONES THE CARDS CONSUME, taken from `earnings._METRIC_CODES` rather than listed
    again here. A second list would drift the day a card starts reading a new line, and the table
    would report full coverage for a chart that cannot be drawn.

⚠ ONE BULK READ PER LINE, NEVER PER COMPANY. Measured on SP500 (503 members), a per-company loop
    costs 44.5 s for ONE metric against 0.1 s for a bulk read — and this needs nineteen of them.
    See `earnings._metrics_by_company`.
"""
from __future__ import annotations

import logging

_log = logging.getLogger(__name__)

# The raw GuruFocus lines every Long Equity card is built from, in statement order so the table
# reads like a set of accounts rather than an alphabetical list. `label` is the column head.
#
# ⚠ THE KEYS ARE `_METRIC_CODES` KEYS. That module owns which GuruFocus spelling each one maps to
# (there are two per line, and a bank uses different keys again) — this is only the ORDER and the
# reader-facing names.
COLUMNS: tuple[dict, ...] = (
    {"key": "revenue", "label": "Revenue", "note": "Income statement — the top line."},
    {"key": "gross_profit", "label": "Gross profit",
     "note": "⚠ A BANK HAS NO GROSS PROFIT LINE AT ALL (GuruFocus template 'B'), so a blank here "
             "is an answer — the concept does not apply — not a gap."},
    {"key": "operating_income", "label": "Operating income", "note": "Income statement."},
    {"key": "net_income", "label": "Net income",
     "note": "The SHAREHOLDERS' line, not 'including noncontrolling interests'."},
    {"key": "interest_expense", "label": "Interest expense",
     "note": "Reported NEGATIVE (an outflow); the cards take its absolute value."},
    {"key": "ocf", "label": "Operating cash flow", "note": "Cash flow statement."},
    {"key": "capex", "label": "Capex", "note": "Reported negative; the cards take its magnitude."},
    {"key": "fcf", "label": "Free cash flow", "note": "Cash flow statement."},
    {"key": "sbc", "label": "Stock-based comp.",
     "note": "Subtracted from FCF wherever the tab's SBC correction is on."},
    {"key": "total_assets", "label": "Total assets", "note": "Balance sheet — a point in time."},
    {"key": "goodwill", "label": "Goodwill",
     "note": "Netted out of assets in the debt ratio. Absent = none reported, treated as 0."},
    {"key": "long_term_debt", "label": "Long-term debt", "note": "Balance sheet."},
    {"key": "noncurrent_liabilities", "label": "Non-current liabilities",
     "note": "With equity, the invested-capital base."},
    {"key": "total_equity", "label": "Total equity",
     "note": "The SHAREHOLDERS' variant, not 'Total Equity' incl. minorities."},
    {"key": "shares", "label": "Shares outstanding",
     "note": "Diluted AVERAGE — the same basis as EPS, which is what makes them tie."},
    {"key": "div_ps", "label": "Dividends / share", "note": "Per Share Data."},
    {"key": "price_ps", "label": "Month-end price",
     "note": "Per Share Data. The daily equivalent is `close_price`, which is the same series "
             "sampled — measured ratio 1.0000 at three consecutive year-ends."},
    {"key": "market_cap", "label": "Market cap", "note": "Valuation and Quality."},
    {"key": "roic", "label": "ROIC %",
     "note": "Published as a percent, and its QUARTERLY figure is already annualised."},
)

_KEYS: tuple[str, ...] = tuple(c["key"] for c in COLUMNS)


def normalise_cadence(value: str | None) -> str:
    """A request's cadence → the one it will actually be answered on.

    ⚠ AN UNRECOGNISED VALUE MUST RESOLVE TO A REAL BASIS, NOT TO NOTHING. `_metrics_by_company`
    falls back to the ANNUAL codes for anything that is not "quarterly", so passing the raw string
    through would echo a cadence the rows were not computed on — annual spans under a heading that
    says something else. The table's whole job is saying which periods we hold, and "2025" versus
    "2025-Q3" are different claims.
    """
    return "quarterly" if value == "quarterly" else "annual"


def constituent_fundamentals(company_ids: list[int], cadence: str = "annual") -> dict[int, dict]:
    """{company_id: {metric key: {"from": period, "to": period, "n": count}}}.

    ⚠ A LINE WITH NO ROWS IS ABSENT FROM THE DICT, not present with nulls. The table renders the
    difference as a dash, and "we hold nothing" is the finding — padding it with an empty object
    would make an unfetched company look identical to one whose fiscal year has not landed.

    ⚠ `cadence="quarterly"` REPORTS THE **TRAILING-TWELVE-MONTH** SPAN, NOT THE RAW QUARTERS, because
    that is what the tab draws — it reads through the same `_metrics_by_company`, so this table
    cannot claim a period the chart would not plot. Two consequences worth knowing before reading a
    span as a gap: a series needs FOUR quarters before it has any TTM point at all, so `from` sits
    three quarters after the first raw quarter and a company with three or fewer is absent
    entirely; and `n` counts TTM points, i.e. raw quarters minus three.

    ⚠ THE TWO CADENCES ARE ONE GuruFocus CALL, NOT TWO. `fetch_financials` writes the `annuals` and
    `quarterly` blocks of the same blob, so switching this toggle never means "fetch again" — it
    means "look at what the same fetch already brought". Measured 2026-08-04 on the live DB: of 264
    SP500 constituents with annual Free Cash Flow, 263 also have the quarterly line. That ONE row is
    exactly what this toggle exists to make visible; before it, the two cadences could only be
    compared by opening a chart.
    """
    from routers.earnings import _metrics_by_company  # noqa: PLC0415  (cycle at module import)

    if not company_ids:
        return {}
    out: dict[int, dict] = {}
    for key in _KEYS:
        for cid, series in _metrics_by_company(company_ids, key, cadence).items():
            if not series:
                continue
            periods = sorted(series)
            out.setdefault(cid, {})[key] = {
                "from": periods[0], "to": periods[-1], "n": len(periods),
            }
    _log.warning("[bench-fund] %s: %d companies x %d raw lines in %d bulk reads — %d companies "
                 "carry at least one", cadence, len(company_ids), len(_KEYS), len(_KEYS), len(out))
    return out
