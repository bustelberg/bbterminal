/**
 * Config for the snapshot-stat band charts on /earnings. Each entry drives one
 * <MetricBandChart> cell. Mirrors the matching SnapshotStats row exactly — same
 * metric code(s), same scoring rubric (band thresholds + polarity), same value
 * formatting — so the chart's latest point/colour agrees with the snapshot
 * table (in quarterly mode). Excludes the "Value Creation" and "Historical
 * Growth" sections (derived CAGR/R²/SD stats, not point-in-time values) and
 * Forward P/E (which already has its own period-average chart).
 */
import { MC, type ChartCadence, type MetricRow } from './types';
import { quarterlyPreferredSeries, quarterlyRatioSeries, interestCoverageSeries, pegDailySeries, fmtNum, fmtPct, fmtPctPoints } from './utils';
import type { BandSpec } from './MetricBandChart';

export type EarningsSource = 'prices' | 'indicators' | 'financials' | 'analyst_estimates';

export type SnapshotChartConfig = {
  key: string;
  title: string;        // card header
  source: EarningsSource;
  headerInfo: string;   // header InfoTip
  subtitle: string;     // grey subtitle line in the chart (cadence appended by the chart)
  chartInfo: string;    // subtitle InfoTip
  emptyText: string;
  /** Override the cadence word appended to the subtitle (e.g. 'daily' for PEG,
   * which is recomputed per trading day and ignores the Quarterly/Annual toggle). */
  cadenceLabel?: string;
  band: BandSpec;
  format: (v: number) => string;
  axisFormat?: (v: number) => string;
  buildSeries: (m: MetricRow[], cadence: ChartCadence) => { date: string; value: number }[];
};

const num2 = (v: number) => fmtNum(v, 2);
const axisPct = (v: number) => `${v.toFixed(0)}%`;       // percentage-points axis
const axisFrac = (v: number) => `${(v * 100).toFixed(0)}%`; // fraction → % axis

export const SNAPSHOT_BAND_CHARTS: SnapshotChartConfig[] = [
  // ── Balance Sheet ────────────────────────────────────────────────────────
  {
    key: 'interest_coverage',
    title: 'Interest Coverage',
    source: 'financials',
    headerInfo: "Interest Coverage — how many times over a company's operating earnings cover its interest bill; its cushion against debt costs. Computed each period as Operating Income (EBIT) ÷ Interest Expense (GuruFocus financials; quarterly mode uses the latest quarter, annual uses fiscal-year figures). Higher = safer — below ~1.5× the company barely services its debt. Bands: red below 3×, amber 3–7×, green 7×+.",
    subtitle: 'Operating income ÷ interest expense (×)',
    chartInfo: 'Interest Coverage = Operating Income ÷ Interest Expense. Quarterly mode matches the Snapshot Stats value; annual uses fiscal-year figures. Higher is safer. Bands: red below 3×, amber 3–7×, green 7×+ — same thresholds as the Snapshot Stats row.',
    emptyText: 'No interest coverage data. Refresh to load.',
    band: { kind: 'higher', poorBelow: 3, goodAtOrAbove: 7 },
    format: (v) => `${fmtNum(v, 2)}×`,
    buildSeries: (m, cadence) => interestCoverageSeries(m, cadence),
  },
  {
    key: 'debt_equity',
    title: 'Debt / Equity',
    source: 'financials',
    headerInfo: "Debt / Equity — financial leverage: how much of the company is funded by debt versus shareholders. Computed as Total Debt ÷ Total Shareholders' Equity per period (GuruFocus financials; quarterly twin preferred, annual fiscal-year fallback). Lower = less leverage and lower financial risk; a highly levered balance sheet amplifies both gains and losses. Bands: green ≤0.5, amber 0.5–2, red above 2.",
    subtitle: 'Total debt ÷ total equity',
    chartInfo: 'Debt-to-Equity = Total Debt ÷ Total Equity. Quarterly mode matches the Snapshot Stats value; annual uses fiscal-year figures. Lower = less leverage. Bands: green ≤0.5, amber 0.5–2, red above 2 — same thresholds as the Snapshot Stats row.',
    emptyText: 'No debt/equity data. Refresh to load.',
    band: { kind: 'lower', goodAtOrBelow: 0.5, poorAbove: 2 },
    format: num2,
    buildSeries: (m, cadence) => quarterlyPreferredSeries(m, MC.DEBT_TO_EQUITY, cadence),
  },

  // ── Capital Intensity (lower is better) ──────────────────────────────────
  {
    key: 'capex_rev',
    title: 'CAPEX / Revenue',
    source: 'financials',
    headerInfo: 'CAPEX / Revenue — how capital-intensive the business is: the share of every sales dollar that has to be reinvested in property, plant & equipment. Computed as Capital Expenditure ÷ Revenue per period (GuruFocus financials, %). Lower = more capital-light, so more of revenue is free to return to owners rather than maintain the asset base. Bands: green ≤5, amber 5–15, red above 15.',
    subtitle: 'Capex ÷ revenue',
    chartInfo: 'CAPEX as a share of revenue (matches Snapshot Stats in quarterly mode). Lower = more capital-light. Bands: green ≤5, amber 5–15, red above 15 — same thresholds as the Snapshot Stats row.',
    emptyText: 'No CAPEX/Revenue data. Refresh to load.',
    band: { kind: 'lower', goodAtOrBelow: 5, poorAbove: 15 },
    format: num2,
    buildSeries: (m, cadence) => quarterlyPreferredSeries(m, MC.CAPEX_TO_REV, cadence),
  },
  {
    key: 'capex_ocf',
    title: 'CAPEX / OCF',
    source: 'financials',
    headerInfo: 'CAPEX / OCF — what fraction of the cash the business actually generates gets consumed by capital spending. Computed as Capital Expenditure ÷ Operating Cash Flow per period (GuruFocus financials, %). Lower = more cash left over as free cash flow after keeping the business running; above 100% means capex exceeds operating cash (the business is funding investment from elsewhere). Bands: green ≤30, amber 30–60, red above 60.',
    subtitle: 'Capex ÷ operating cash flow',
    chartInfo: 'CAPEX as a share of operating cash flow (matches Snapshot Stats in quarterly mode). Lower = more cash left after reinvestment. Bands: green ≤30, amber 30–60, red above 60 — same thresholds as the Snapshot Stats row.',
    emptyText: 'No CAPEX/OCF data. Refresh to load.',
    band: { kind: 'lower', goodAtOrBelow: 30, poorAbove: 60 },
    format: num2,
    buildSeries: (m, cadence) => quarterlyPreferredSeries(m, MC.CAPEX_TO_OCF, cadence),
  },

  // ── Capital Allocation (higher is better, % points) ──────────────────────
  {
    key: 'roe',
    title: 'ROE',
    source: 'financials',
    headerInfo: 'Return on Equity (ROE) — profit generated per dollar of shareholders’ equity; a core read on how efficiently the company compounds owners’ capital. Computed as Net Income ÷ Shareholders’ Equity per period (GuruFocus financials, %). Higher is better, but watch context: very high ROE can be inflated by heavy debt (leverage shrinks the equity denominator) — cross-check with ROIC. Bands: green ≥15%, amber 8–15%, red below 8%.',
    subtitle: 'Return on equity (%)',
    chartInfo: 'Return on Equity (matches Snapshot Stats in quarterly mode). Bands: green ≥15%, amber 8–15%, red below 8% — same thresholds as the Snapshot Stats row.',
    emptyText: 'No ROE data. Refresh to load.',
    band: { kind: 'higher', poorBelow: 8, goodAtOrAbove: 15 },
    format: fmtPctPoints,
    axisFormat: axisPct,
    buildSeries: (m, cadence) => quarterlyPreferredSeries(m, MC.ROE, cadence),
  },
  {
    key: 'roic',
    title: 'ROIC',
    source: 'financials',
    headerInfo: 'Return on Invested Capital (ROIC) — profit generated per dollar of ALL capital invested (debt + equity); the cleanest, leverage-neutral gauge of capital efficiency. Computed as NOPAT (after-tax operating profit) ÷ Invested Capital per period (GuruFocus financials, %). The key test is ROIC vs the cost of capital (~8–10%): sustainably above it = the company creates value as it grows; below it = growth destroys value. Bands: green ≥15%, amber 8–15%, red below 8%.',
    subtitle: 'Return on invested capital (%)',
    chartInfo: 'Return on Invested Capital (matches Snapshot Stats in quarterly mode). Bands: green ≥15%, amber 8–15%, red below 8% — same thresholds as the Snapshot Stats row.',
    emptyText: 'No ROIC data. Refresh to load.',
    band: { kind: 'higher', poorBelow: 8, goodAtOrAbove: 15 },
    format: fmtPctPoints,
    axisFormat: axisPct,
    buildSeries: (m, cadence) => quarterlyPreferredSeries(m, MC.ROIC, cadence),
  },

  // ── Profitability ────────────────────────────────────────────────────────
  {
    key: 'gross_margin',
    title: 'Gross Margin',
    source: 'financials',
    headerInfo: 'Gross Margin — the share of revenue left after the direct cost of making the product or delivering the service (COGS); a proxy for pricing power and product economics. Computed as Gross Profit ÷ Revenue per period (GuruFocus financials, %). High and stable margins signal durable pricing power and a moat; falling margins flag competition or cost pressure. Bands: green ≥40%, amber 20–40%, red below 20%.',
    subtitle: 'Gross margin (%)',
    chartInfo: 'Gross margin (matches Snapshot Stats in quarterly mode). Bands: green ≥40%, amber 20–40%, red below 20% — same thresholds as the Snapshot Stats row.',
    emptyText: 'No Gross Margin data. Refresh to load.',
    band: { kind: 'higher', poorBelow: 20, goodAtOrAbove: 40 },
    format: fmtPctPoints,
    axisFormat: axisPct,
    buildSeries: (m, cadence) => quarterlyPreferredSeries(m, MC.GROSS_MARGIN, cadence),
  },
  {
    key: 'net_margin',
    title: 'Net Margin',
    source: 'financials',
    headerInfo: 'Net Margin — bottom-line profitability: the share of revenue that survives every cost, plus interest and tax, to become net income. Computed as Net Income ÷ Revenue per period (GuruFocus financials, %). Higher = more profit per sales dollar; compare to gross margin to see how much is eaten by operating costs, interest and tax. Bands: green ≥15%, amber 5–15%, red below 5%.',
    subtitle: 'Net margin (%)',
    chartInfo: 'Net margin (matches Snapshot Stats in quarterly mode). Bands: green ≥15%, amber 5–15%, red below 5% — same thresholds as the Snapshot Stats row.',
    emptyText: 'No Net Margin data. Refresh to load.',
    band: { kind: 'higher', poorBelow: 5, goodAtOrAbove: 15 },
    format: fmtPctPoints,
    axisFormat: axisPct,
    buildSeries: (m, cadence) => quarterlyPreferredSeries(m, MC.NET_MARGIN, cadence),
  },
  {
    key: 'fcf_ni',
    title: 'FCF / Net Income',
    source: 'financials',
    headerInfo: 'FCF / Net Income (cash conversion) — how much of reported accounting profit actually turns into spendable free cash. Computed as Free Cash Flow ÷ Net Income per period (GuruFocus financials). Above 100% = cash earnings exceed accounting earnings (high earnings quality); persistently below 100% warns that profits are propped up by accruals, not cash. Bands: green ≥120%, amber 80–120%, red below 80%.',
    subtitle: 'Free cash flow ÷ net income',
    chartInfo: 'Free Cash Flow ÷ Net Income (matches Snapshot Stats in quarterly mode). Above 100% = cash earnings exceed accounting earnings. Bands: green ≥120%, amber 80–120%, red below 80% — same thresholds as the Snapshot Stats row.',
    emptyText: 'No FCF / Net Income data. Refresh to load.',
    band: { kind: 'higher', poorBelow: 0.8, goodAtOrAbove: 1.2 },
    format: fmtPct,
    axisFormat: axisFrac,
    buildSeries: (m, cadence) => quarterlyRatioSeries(m, MC.FCF, MC.NET_INCOME, { cadence }),
  },

  // ── Outlook ────────────────────────────────────────────────────────────
  {
    key: 'eps_lt_growth',
    title: 'EPS LT Growth EST',
    source: 'analyst_estimates',
    headerInfo: "EPS LT Growth EST — the analyst consensus for the company's long-term (≈3–5 year) annual earnings-per-share growth rate. Sourced directly from GuruFocus analyst estimates (a forward expectation, not computed from reported financials). Higher = faster expected earnings growth, which underpins valuations like PEG — but consensus estimates are forward-looking and tend to run optimistic, so treat as a guide, not a fact. Bands: green ≥12%, amber 5–12%, red below 5%.",
    subtitle: 'Analyst LT EPS growth estimate (%)',
    chartInfo: 'Analyst consensus long-term EPS growth estimate over time (matches Snapshot Stats). Bands: green ≥12%, amber 5–12%, red below 5% — same thresholds as the Snapshot Stats row.',
    emptyText: 'No analyst EPS growth estimate. Refresh to load.',
    band: { kind: 'higher', poorBelow: 5, goodAtOrAbove: 12 },
    format: fmtPctPoints,
    axisFormat: axisPct,
    // Estimate code isn't an `annuals__` twin — quarterlyPreferredSeries falls
    // straight through to its annual-style series at either cadence.
    buildSeries: (m, cadence) => quarterlyPreferredSeries(m, MC.EPS_EST, cadence),
  },

  // ── Valuation (lower is better) ──────────────────────────────────────────
  {
    key: 'peg',
    title: 'PEG',
    source: 'financials',
    headerInfo: "PEG ratio — valuation relative to growth: the P/E multiple divided by the expected EPS growth rate, so a fast grower and a slow grower can be judged on a level field (a high P/E is fine if growth is high). We recompute it every trading day by scaling GuruFocus's periodic PEG by the live close (P/E moves with price daily; EPS and growth are held at the last reported period). Below 1 = cheap for its growth, above 2 = expensive. GuruFocus stores 0 when growth ≤ 0 (PEG undefined) — those points are dropped. Bands: green ≤1, amber 1–2, red above 2.",
    subtitle: 'PEG ratio (P/E ÷ growth)',
    chartInfo: "PEG recomputed per trading day: GuruFocus's periodic PEG scaled by the daily price (P/E uses the live price, so PEG moves with it daily; EPS + growth are held at the period's values). Bands: green ≤1, amber 1–2, red above 2 — same thresholds as the Snapshot Stats row. Ignores the Quarterly/Annual toggle. GuruFocus stores exactly 0 when EPS growth ≤ 0 (undefined); those points are dropped.",
    emptyText: 'No PEG data. Refresh to load.',
    band: { kind: 'lower', goodAtOrBelow: 1, poorAbove: 2 },
    format: num2,
    cadenceLabel: 'daily',
    // PEG ∝ daily price; pegDailySeries scales GF's periodic PEG by the close.
    buildSeries: (m) => pegDailySeries(m),
  },
];
