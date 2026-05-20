/**
 * Shared types + metric-code constants for the /earnings page and every
 * sub-component under `components/earnings/`. The chart/stat blocks
 * (SnapshotStats, RelativeGrowthChart, ReverseDCF, etc.) all consume
 * the same `MetricRow[]` payload, so the types live here once.
 */

export type Company = {
  company_id: number;
  gurufocus_ticker: string;
  gurufocus_exchange: string;
  company_name: string | null;
  country: string | null;
};

export type MetricRow = {
  metric_code: string;
  target_date: string;
  numeric_value: number | null;
  is_prediction: boolean;
};

export type Cadence = {
  /** Human bucket: 'Daily' | 'Weekly' | 'Monthly' | 'Quarterly' | 'Semi-annual' | 'Annual' | 'Single point'. */
  label: string;
  /** Median gap between observations in days; null when only 1 point. */
  medianDays: number | null;
  /** Total observations for this code. */
  count: number;
  firstDate: string;
  lastDate: string;
};

// ---------------------------------------------------------------------------
// Metric codes
// ---------------------------------------------------------------------------
// Most ratios sit on `annuals__X` codes; lv() in SnapshotStats auto-prefers
// the `quarterly__X` twin when it has a fresher target_date. Forward P/E is
// the one exception — it's forward-looking (price ÷ next-FY EPS estimate)
// and isn't in the financials JSON, so it stays on the indicators code.
export const MC = {
  FCF_YIELD: 'annuals__Valuation Ratios__FCF Yield %',
  PRICE: 'annuals__Per Share Data__Month End Stock Price',
  EPS_WO_NRI: 'annuals__Per Share Data__EPS without NRI',
  DIV_PS: 'annuals__Per Share Data__Dividends per Share',
  EPS_EST: 'annual_eps_nri_estimate',
  DIV_EST: 'annual_dividend_estimate',
  FCF_PS: 'annuals__Per Share Data__Free Cash Flow per Share',
  INTEREST_COVERAGE: 'annuals__Valuation and Quality__Interest Coverage',
  DEBT_TO_EQUITY: 'annuals__Balance Sheet__Debt-to-Equity',
  CAPEX_TO_REV: 'annuals__Ratios__Capex-to-Revenue',
  CAPEX_TO_OCF: 'annuals__Ratios__Capex-to-Operating-Cash-Flow',
  ROE: 'annuals__Ratios__ROE %',
  ROIC: 'annuals__Ratios__ROIC %',
  GROSS_MARGIN: 'annuals__Ratios__Gross Margin %',
  NET_MARGIN: 'annuals__Ratios__Net Margin %',
  FWD_PE: 'indicator_q_forward_pe_ratio',
  PEG: 'annuals__Valuation Ratios__PEG Ratio',
  FCF: 'annuals__Cashflow Statement__Free Cash Flow',
  REVENUE: 'annuals__Income Statement__Revenue',
  NET_INCOME: 'annuals__Income Statement__Net Income',
  OPERATING_INCOME: 'annuals__Income Statement__Operating Income',
  INTEREST_EXPENSE: 'annuals__Income Statement__Interest Expense',
  EPS_DILUTED: 'annuals__Income Statement__EPS (Diluted)',
  EPS_FY1_EST: 'annual_per_share_eps_estimate',
  // Reverse DCF / WACC metrics
  WACC: 'annuals__Ratios__WACC %',
  BETA: 'annuals__Valuation and Quality__Beta',
  NET_CASH_PS: 'annuals__Valuation and Quality__Net Cash per Share',
  GF_INTRINSIC: 'annuals__Valuation and Quality__Intrinsic Value: Projected FCF',
  PIOTROSKI: 'annuals__Valuation and Quality__Piotroski F-Score',
  ALTMAN_Z: 'annuals__Valuation and Quality__Altman Z-Score',
  BUYBACK_RATIO: 'annuals__Valuation and Quality__Shares Buyback Ratio %',
  YOY_REV_GROWTH: 'annuals__Valuation and Quality__YoY Rev. per Sh. Growth',
  EBITDA_5Y_GROWTH: 'annuals__Valuation and Quality__5-Year EBITDA Growth Rate (Per Share)',
  YOY_EPS_GROWTH: 'annuals__Valuation and Quality__YoY EPS Growth',
  DIV_YIELD: 'annuals__Valuation Ratios__Dividend Yield %',
  TAX_RATE: 'annuals__Income Statement__Tax Rate %',
  // LongEquity metrics
  SP_5Y_CAGR: 'share_price_5yr_cagr',
  SP_5Y_RSQ: 'share_price_5yr_rsq',
  SP_10Y_CAGR: 'share_price_10yr_cagr',
  SP_10Y_RSQ: 'share_price_10yr_rsq',
  REV_GROWTH_5Y: 'revenue_growth_5yr',
  REV_GROWTH_RSQ: 'revenue_growth_rsq',
  FCF_GROWTH_5Y: 'fcf_growth_5yr',
  FCF_GROWTH_SD: 'fcf_growth_sd',
  FCF_GROWTH_RSQ: 'fcf_growth_rsq',
};
