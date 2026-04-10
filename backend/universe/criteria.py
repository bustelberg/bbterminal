"""
Evaluate the 7 LongEquity quality criteria for a company using GuruFocus
annual financials data.

Each criterion is scored 0 or 1. A company needs >= 1 point to be included
in the universe.

Criteria:
1. FCF/share growth > 15% (consistently)
2. Cash return on capital (ROIC) > 20%
3. FCF margin > 20%
4. Asset light (PPE < 40% total assets) + capital light (capex < 20% revenue)
5. SBC < 30% of operating cash flow
6. Shares outstanding change < 5% over 5 years
7. Interest expense < 20% of operating profit
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date


@dataclass
class CriteriaResult:
    """Result of evaluating criteria for one company at one point in time."""
    company_id: int
    eval_date: date  # fiscal year end used for evaluation
    scores: dict[str, int] = field(default_factory=dict)  # criterion name -> 0/1
    details: dict[str, str] = field(default_factory=dict)  # criterion name -> explanation
    total_score: int = 0
    passes: bool = False  # total_score >= 1


def _safe_div(a: float | None, b: float | None) -> float | None:
    if a is None or b is None or b == 0:
        return None
    return a / b


def _get_annual_values(annuals: dict, section: str, key: str) -> list[tuple[str, float]]:
    """Extract (fiscal_year, value) pairs from annuals data."""
    # Periods are at the top level of annuals, not inside each section
    periods = None
    for c in ("Fiscal Year", "Date", "date"):
        if c in annuals and isinstance(annuals[c], list):
            periods = annuals[c]
            break
    if not periods:
        return []

    block = annuals.get(section)
    if not isinstance(block, dict):
        return []

    # Navigate nested structure (e.g. "Stock Based Compensation" or "Property, Plant and Equipment")
    node = block
    for part in key.split(" > "):
        if isinstance(node, dict) and part in node:
            node = node[part]
        else:
            return []

    if not isinstance(node, list):
        return []

    results = []
    for period_str, val in zip(periods, node):
        if val is None or str(val).strip() in ("", "-", "None"):
            continue
        try:
            results.append((str(period_str).strip(), float(val)))
        except (ValueError, TypeError):
            continue
    return results


def _get_value_at_year(values: list[tuple[str, float]], year: str) -> float | None:
    """Get value for a specific fiscal year (YYYY-MM format)."""
    for fy, v in values:
        if fy.startswith(year) or fy == year:
            return v
    return None


def _get_latest_n_values(values: list[tuple[str, float]], n: int) -> list[float]:
    """Get the most recent n values (values are ordered most recent first from GF)."""
    return [v for _, v in values[:n]]


def evaluate_criteria(
    annuals: dict,
    company_id: int,
    as_of_year: str | None = None,
) -> CriteriaResult:
    """Evaluate all 7 criteria using annual financials data.

    Args:
        annuals: The 'annuals' block from GuruFocus financials response.
        company_id: Company ID for the result.
        as_of_year: If set, only use data up to this fiscal year (YYYY-MM).
                    If None, use all available data.

    Returns:
        CriteriaResult with per-criterion scores and overall pass/fail.
    """
    # Periods are at the top level of annuals
    periods = None
    for c in ("Fiscal Year", "Date", "date"):
        if c in annuals and isinstance(annuals[c], list):
            periods = annuals[c]
            break

    if not periods:
        return CriteriaResult(company_id=company_id, eval_date=date.today())

    # Filter to as_of_year if specified
    if as_of_year:
        periods_filtered = [p for p in periods if str(p).strip() <= as_of_year]
    else:
        periods_filtered = list(periods)

    if not periods_filtered:
        return CriteriaResult(company_id=company_id, eval_date=date.today())

    latest_fy = str(periods_filtered[0]).strip()
    try:
        eval_date = date(int(latest_fy[:4]), int(latest_fy[5:7]), 1)
    except (ValueError, IndexError):
        eval_date = date.today()

    result = CriteriaResult(company_id=company_id, eval_date=eval_date)

    # Helper: get values filtered by as_of_year
    def get_vals(section: str, key: str) -> list[tuple[str, float]]:
        all_vals = _get_annual_values(annuals, section, key)
        if as_of_year:
            return [(fy, v) for fy, v in all_vals if fy <= as_of_year]
        return all_vals

    # --- Criterion 1: FCF/share growth > 15% consistently ---
    fcf_ps = get_vals("Per Share Data", "Free Cash Flow per Share")
    if len(fcf_ps) >= 4:
        # Check 3-year CAGR
        recent = fcf_ps[0][1]
        three_yr_ago = fcf_ps[3][1] if len(fcf_ps) > 3 else None
        if three_yr_ago and three_yr_ago > 0 and recent > 0:
            cagr_3y = (recent / three_yr_ago) ** (1 / 3) - 1
            result.scores["fcf_growth"] = 1 if cagr_3y >= 0.15 else 0
            result.details["fcf_growth"] = f"3yr CAGR: {cagr_3y:.1%}"
        else:
            result.scores["fcf_growth"] = 0
            result.details["fcf_growth"] = f"negative/zero FCF (recent={recent:.2f})"
    else:
        result.scores["fcf_growth"] = 0
        result.details["fcf_growth"] = f"insufficient data ({len(fcf_ps)} years)"

    # --- Criterion 2: ROIC > 20% ---
    roic = get_vals("Ratios", "ROIC %")
    if roic:
        # Check latest 3 years median
        recent_vals = _get_latest_n_values(roic, 3)
        if recent_vals:
            median_roic = sorted(recent_vals)[len(recent_vals) // 2]
            result.scores["roic"] = 1 if median_roic >= 20 else 0
            result.details["roic"] = f"3yr median: {median_roic:.1f}%"
        else:
            result.scores["roic"] = 0
            result.details["roic"] = "no data"
    else:
        result.scores["roic"] = 0
        result.details["roic"] = "no ROIC data"

    # --- Criterion 3: FCF margin > 20% ---
    fcf_margin = get_vals("Ratios", "FCF Margin %")
    if fcf_margin:
        recent_vals = _get_latest_n_values(fcf_margin, 3)
        if recent_vals:
            median_margin = sorted(recent_vals)[len(recent_vals) // 2]
            result.scores["fcf_margin"] = 1 if median_margin >= 20 else 0
            result.details["fcf_margin"] = f"3yr median: {median_margin:.1f}%"
        else:
            result.scores["fcf_margin"] = 0
            result.details["fcf_margin"] = "no data"
    else:
        result.scores["fcf_margin"] = 0
        result.details["fcf_margin"] = "no FCF margin data"

    # --- Criterion 4: Asset light + Capital light ---
    ppe = get_vals("Balance Sheet", "Property, Plant and Equipment")
    total_assets = get_vals("Balance Sheet", "Total Assets")
    capex_to_rev = get_vals("Ratios", "Capex-to-Revenue")

    asset_light = False
    capital_light = False
    asset_detail = ""
    capital_detail = ""

    if ppe and total_assets:
        ppe_val = ppe[0][1]
        ta_val = total_assets[0][1]
        ppe_pct = _safe_div(ppe_val, ta_val)
        if ppe_pct is not None:
            asset_light = ppe_pct < 0.40
            asset_detail = f"PPE/Assets: {ppe_pct:.1%}"
        else:
            asset_detail = "can't compute PPE/Assets"
    else:
        asset_detail = "missing PPE or Total Assets"

    if capex_to_rev:
        ctr = capex_to_rev[0][1]
        capital_light = ctr < 0.20
        capital_detail = f"Capex/Rev: {ctr:.1%}"
    else:
        capital_detail = "missing Capex-to-Revenue"

    result.scores["asset_capital_light"] = 1 if (asset_light and capital_light) else 0
    result.details["asset_capital_light"] = f"{asset_detail}, {capital_detail}"

    # --- Criterion 5: SBC < 30% of OCF ---
    sbc = get_vals("Cashflow Statement", "Stock Based Compensation")
    ocf = get_vals("Cashflow Statement", "Cash Flow from Operations")
    if sbc and ocf:
        sbc_val = abs(sbc[0][1])  # SBC can be reported as positive or negative
        ocf_val = ocf[0][1]
        sbc_pct = _safe_div(sbc_val, ocf_val)
        if sbc_pct is not None and ocf_val > 0:
            result.scores["sbc"] = 1 if sbc_pct < 0.30 else 0
            result.details["sbc"] = f"SBC/OCF: {sbc_pct:.1%}"
        else:
            result.scores["sbc"] = 0
            result.details["sbc"] = f"negative OCF ({ocf_val:.0f})"
    else:
        result.scores["sbc"] = 0
        result.details["sbc"] = "missing SBC or OCF data"

    # --- Criterion 6: Shares outstanding change < 5% over 5 years ---
    shares = get_vals("Valuation and Quality", "Shares Outstanding (EOP)")
    if len(shares) >= 6:
        current = shares[0][1]
        five_yr_ago = shares[5][1]
        if five_yr_ago > 0 and current > 0:
            change_pct = (current - five_yr_ago) / five_yr_ago
            result.scores["dilution"] = 1 if change_pct < 0.05 else 0
            result.details["dilution"] = f"5yr change: {change_pct:+.1%}"
        else:
            result.scores["dilution"] = 0
            result.details["dilution"] = "zero shares data"
    else:
        result.scores["dilution"] = 0
        result.details["dilution"] = f"insufficient data ({len(shares)} years, need 6)"

    # --- Criterion 7: Interest expense < 20% of operating profit ---
    interest = get_vals("Income Statement", "Interest Expense")
    op_income = get_vals("Income Statement", "Operating Income")
    if interest and op_income:
        int_val = abs(interest[0][1])  # reported as negative
        oi_val = op_income[0][1]
        int_pct = _safe_div(int_val, oi_val)
        if int_pct is not None and oi_val > 0:
            result.scores["interest_burden"] = 1 if int_pct < 0.20 else 0
            result.details["interest_burden"] = f"Interest/OpIncome: {int_pct:.1%}"
        else:
            result.scores["interest_burden"] = 0
            result.details["interest_burden"] = f"negative operating income ({oi_val:.0f})"
    else:
        result.scores["interest_burden"] = 0
        result.details["interest_burden"] = "missing interest or operating income"

    result.total_score = sum(result.scores.values())
    result.passes = result.total_score >= 1
    return result


CRITERIA_NAMES = [
    ("fcf_growth", "FCF/Share Growth > 15%"),
    ("roic", "Cash Return on Capital > 20%"),
    ("fcf_margin", "FCF Margin > 20%"),
    ("asset_capital_light", "Asset & Capital Light"),
    ("sbc", "SBC < 30% of OCF"),
    ("dilution", "Share Dilution < 5% (5yr)"),
    ("interest_burden", "Interest < 20% of Op. Profit"),
]

CRITERIA_MIN_YEARS: dict[str, int] = {
    "fcf_growth": 4,
    "roic": 1,
    "fcf_margin": 1,
    "asset_capital_light": 1,
    "sbc": 1,
    "dilution": 6,
    "interest_burden": 1,
}

CRITERIA_DESCRIPTIONS: dict[str, str] = {
    "fcf_growth": (
        "Free Cash Flow per Share growth over 3 years (CAGR). "
        "Takes the most recent FCF/share and compares to 3 years prior. "
        "Pass if 3-year CAGR >= 15%. Needs at least 4 years of data. "
        "Source: GuruFocus > Per Share Data > Free Cash Flow per Share."
    ),
    "roic": (
        "Return on Invested Capital (ROIC). "
        "Takes the median of the most recent 3 years. "
        "Pass if median ROIC >= 20%. "
        "Source: GuruFocus > Ratios > ROIC %."
    ),
    "fcf_margin": (
        "Free Cash Flow Margin as a percentage of revenue. "
        "Takes the median of the most recent 3 years. "
        "Pass if median FCF Margin >= 20%. "
        "Source: GuruFocus > Ratios > FCF Margin %."
    ),
    "asset_capital_light": (
        "Both conditions must be met: "
        "(1) Asset light: PPE / Total Assets < 40%. "
        "(2) Capital light: Capex / Revenue < 20%. "
        "Uses the most recent fiscal year. "
        "Source: GuruFocus > Balance Sheet > Property, Plant and Equipment & Total Assets; "
        "Ratios > Capex-to-Revenue."
    ),
    "sbc": (
        "Stock-Based Compensation relative to Operating Cash Flow. "
        "Takes absolute SBC divided by OCF for the most recent year. "
        "Pass if SBC/OCF < 30% (and OCF is positive). "
        "Source: GuruFocus > Cashflow Statement > Stock Based Compensation & "
        "Cash Flow from Operations."
    ),
    "dilution": (
        "Change in shares outstanding over 5 years. "
        "Compares current shares to 5 years prior. "
        "Pass if increase < 5%. Needs at least 6 years of data. "
        "Source: GuruFocus > Valuation and Quality > Shares Outstanding (EOP)."
    ),
    "interest_burden": (
        "Interest Expense as a proportion of Operating Income. "
        "Takes absolute interest expense divided by operating income for the most recent year. "
        "Pass if Interest/Operating Income < 20% (and operating income is positive). "
        "Source: GuruFocus > Income Statement > Interest Expense & Operating Income."
    ),
}
