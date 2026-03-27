# src/quick_insight/web/helpers/earnings_dashboard_helpers/constants.py
from __future__ import annotations

ASPECT_W_OVER_H = 1.65
DEFAULT_TICKER = "MSFT"

METRIC: dict[str, str] = {
    # Existing charts
    "FCF_YIELD": "annuals__Valuation Ratios__FCF Yield %",
    "PRICE": "annuals__Per Share Data__Month End Stock Price",
    "EPS_WO_NRI": "annuals__Per Share Data__EPS without NRI",
    "DIV_PS": "annuals__Per Share Data__Dividends per Share",
    "EPS_EST": "annual_eps_nri_estimate",
    "DIV_EST": "annual_dividend_estimate",
    "FCF_PS": "annuals__Per Share Data__Free Cash Flow per Share",
    # Snapshot stats
    "INTEREST_COVERAGE": "indicator_q_interest_coverage",
    "DEBT_TO_EQUITY": "annuals__Balance Sheet__Debt-to-Equity",
    "CAPEX_TO_REV": "annuals__Ratios__Capex-to-Revenue",
    "CAPEX_TO_OCF": "annuals__Ratios__Capex-to-Operating-Cash-Flow",
    "ROE": "indicator_q_roe",
    "ROIC": "indicator_q_roic",
    "GROSS_MARGIN": "indicator_q_gross_margin",
    "NET_MARGIN": "indicator_q_net_margin",
    "REV_GROWTH_5Y": "revenue_growth_5yr",
    "REV_GROWTH_EST_3_5Y": "annual_future_revenue_estimate_growth",
    "EPS_LT_GROWTH_EST": "annual_long_term_growth_rate_mean",
    "FWD_PE": "indicator_q_forward_pe_ratio",
    "PEG": "indicator_q_peg_ratio",
    # For computed ratios / extras
    "FCF": "annuals__Cashflow Statement__Free Cash Flow",
    "NET_INCOME": "annuals__Income Statement__Net Income",
    "EPS_DILUTED": "annuals__Income Statement__EPS (Diluted)",
    "EPS_FY1_EST": "annual_per_share_eps_estimate",
}

SERIES: dict[str, str] = {
    "PRICE": "Price (Month End Stock Price)",
    "OE_ACT": "Owner Earnings (Actual: EPS ex NRI + Dividends)",
    "OE_EST": "Owner Earnings (Estimate: EPS ex NRI est + Dividend est)",
    "FCF_PS": "FCF/share",
}

# Optional: central place to encode units (helps prevent % vs points bugs)
# - "pct_ratio" means 0.12 -> "12%"
# - "pct_points" means 12.0 -> "12%"
METRIC_UNIT: dict[str, str] = {
    METRIC["ROE"]: "pct_points",
    METRIC["ROIC"]: "pct_points",
    METRIC["GROSS_MARGIN"]: "pct_points",
    METRIC["NET_MARGIN"]: "pct_points",
    METRIC["REV_GROWTH_EST_3_5Y"]: "pct_points",
    METRIC["EPS_LT_GROWTH_EST"]: "pct_points",
    # Many ratio feeds can be points too; adjust if your source confirms it:
    # METRIC["CAPEX_TO_REV"]: "pct_points",
    # METRIC["CAPEX_TO_OCF"]: "pct_points",
    METRIC["REV_GROWTH_5Y"]: "pct_ratio",  # if your feed returns 0.12 for 12%
}


def metrics_for_page() -> list[str]:
    """
    Centralized list so pages/helpers don’t diverge.
    """
    return [
        METRIC["FCF_YIELD"],
        METRIC["PRICE"],
        METRIC["EPS_WO_NRI"],
        METRIC["DIV_PS"],
        METRIC["EPS_EST"],
        METRIC["DIV_EST"],
        METRIC["FCF_PS"],
        METRIC["INTEREST_COVERAGE"],
        METRIC["DEBT_TO_EQUITY"],
        METRIC["CAPEX_TO_REV"],
        METRIC["CAPEX_TO_OCF"],
        METRIC["ROE"],
        METRIC["ROIC"],
        METRIC["GROSS_MARGIN"],
        METRIC["NET_MARGIN"],
        METRIC["REV_GROWTH_5Y"],
        METRIC["REV_GROWTH_EST_3_5Y"],
        METRIC["EPS_LT_GROWTH_EST"],
        METRIC["FWD_PE"],
        METRIC["PEG"],
        METRIC["FCF"],
        METRIC["NET_INCOME"],
        METRIC["EPS_DILUTED"],
        METRIC["EPS_FY1_EST"],
    ]
