'use client';

import { useCallback, useMemo } from 'react';
import InfoTip from '../InfoTip';
import Spinner from '../Spinner';
import { MC, type Cadence, type MetricRow } from './types';
import {
  cadenceHoverText,
  computeCAGR,
  flowSeriesPreferQuarterlyTTM,
  fmtNum,
  fmtPct,
  fmtPctPoints,
  latestObservation,
  logLinearR2,
  observationCadence,
  trailingPositiveRun,
  stdDev,
  timeSeries,
  trailingYearsWindow,
  ttmYoYGrowthRates,
  yoyGrowthRates,
} from './utils';

/** Two-column snapshot table (Balance Sheet / Capital Intensity / Capital
 * Allocation / Value Creation on the left; Profitability / Historical
 * Growth / Outlook / Valuation on the right). Each row shows the latest
 * value, its anchor date, an info tooltip, and a cadence tooltip on the
 * value indicating the publication frequency of the underlying source.
 *
 * Each row also carries an optional `score` (good / neutral / poor)
 * that colors the value text. Thresholds are per-metric and live next
 * to the row definition so a tweak doesn't require chasing a separate
 * config file. Bands are generic — industry-specific cutoffs would
 * need a sector lookup we don't have here.
 *
 * Comparison mode: when `metricsB` is supplied, each row renders TWO
 * value+score cells side by side (A on the left, B on the right). The
 * row spec is defined once and materialized per-snapshot, so the two
 * sides share the same label, info, source, and scoring rubric —
 * differences are entirely driven by the underlying metric data.
 * Scoring is computed independently per side, so A and B can land in
 * different bands. */
type EarningsSource = 'prices' | 'indicators' | 'financials' | 'analyst_estimates';

type Score = 'good' | 'neutral' | 'poor';

const SCORE_TEXT_COLOR: Record<Score, string> = {
  good: 'text-pos-400',
  neutral: 'text-warn-300',
  poor: 'text-neg-400',
};

/** Higher-is-better scorer. `poorBelow` is exclusive lower bound for
 * the poor band; `goodAtOrAbove` is inclusive lower bound for the good
 * band; everything in between is neutral. Returns null on null /
 * non-finite values so the row stays uncolored. */
function higherBetter(
  v: number | null | undefined,
  t: { poorBelow: number; goodAtOrAbove: number },
): Score | null {
  if (v == null || !Number.isFinite(v)) return null;
  if (v < t.poorBelow) return 'poor';
  if (v >= t.goodAtOrAbove) return 'good';
  return 'neutral';
}

/** Lower-is-better scorer — mirror of `higherBetter`. `goodAtOrBelow`
 * is inclusive upper bound for the good band; `poorAbove` is exclusive
 * lower bound for the poor band. */
function lowerBetter(
  v: number | null | undefined,
  t: { goodAtOrBelow: number; poorAbove: number },
): Score | null {
  if (v == null || !Number.isFinite(v)) return null;
  if (v <= t.goodAtOrBelow) return 'good';
  if (v > t.poorAbove) return 'poor';
  return 'neutral';
}

// ─── Snapshot — bundled derivations from one company's metrics ─────

type ValueGrowth = {
  price5YCAGR: number | null;
  price5YR2: number | null;
  price10YCAGR: number | null;
  price10YR2: number | null;
  priceDate: string | null;
  rev5YCAGR: number | null;
  rev5YR2: number | null;
  revDate: string | null;
  fcf5YCAGR: number | null;
  fcf5YR2: number | null;
  fcfGrowthSD: number | null;
  fcfDate: string | null;
  fcfGrowthFromDate: string | null;
  fcfGrowthToDate: string | null;
  // Diagnostic snapshots of the windows used downstream, kept so the
  // "why is this empty?" tooltip can read off endpoints / counts
  // without recomputing the series. Endpoints are { date, value } so
  // the explanation can quote concrete numbers like "2019 FCF: -2.6M".
  price5Y: { date: string; value: number }[];
  price10Y: { date: string; value: number }[];
  rev5Y: { date: string; value: number }[];
  fcf5Y: { date: string; value: number }[];
  fcfTrailingPositiveSpanYears: number;
  fcfYoYRatesCount: number;
};

type Snapshot = {
  lv: (code: string) => { value: number | null; date: string } | null;
  cadenceFor: (code: string) => Cadence | null;
  fcfOverNi: number | null;
  interestCoverage: { value: number | null; date: string; computed: boolean } | null;
  valueGrowth: ValueGrowth;
  priceCadence: Cadence | null;
  revenueCadence: Cadence | null;
  fcfCadence: Cadence | null;
  interestCoverageCadence: Cadence | null;
};

/** All the per-company derivations the row builders consume, computed
 * once per metrics array. Extracted so the comparison company can reuse
 * the exact same logic without parallel useMemo / useCallback chains
 * in the main component. */
function useSnapshot(metrics: MetricRow[]): Snapshot {
  // For any `annuals__X` code, prefer the `quarterly__X` twin whenever it
  // exists — quarterly is the always-correct read for point-in-time / ratio
  // metrics (Debt-to-Equity, Interest Coverage, CAPEX/Revenue, ROE, …) since
  // GuruFocus's annual block is just a snapshot of the last fiscal year and
  // will lag for most of the year. Annual is only used when the quarterly
  // twin doesn't exist at all in the response (some metrics ship annual-only).
  //
  // `latestObservation` is null-aware: when the most recent period exists
  // but GF reported "N/A" for it, the dashboard shows the period date
  // with a "—" value rather than reaching back to a stale numeric from
  // years ago.
  const lv = useCallback(
    (code: string) => {
      if (!code.startsWith('annuals__')) return latestObservation(metrics, code);
      const quarterly = latestObservation(metrics, 'quarterly__' + code.slice('annuals__'.length));
      if (quarterly) return quarterly;
      return latestObservation(metrics, code);
    },
    [metrics],
  );

  const resolvedCode = useCallback(
    (code: string): string => {
      if (!code.startsWith('annuals__')) return code;
      const qCode = 'quarterly__' + code.slice('annuals__'.length);
      const quarterly = latestObservation(metrics, qCode);
      return quarterly ? qCode : code;
    },
    [metrics],
  );

  const cadenceFor = useCallback(
    (code: string): Cadence | null => observationCadence(metrics, resolvedCode(code)),
    [metrics, resolvedCode],
  );

  const fcfOverNi = useMemo(() => {
    const fcf = lv(MC.FCF);
    const ni = lv(MC.NET_INCOME);
    if (!fcf || !ni || fcf.value == null || ni.value == null || ni.value === 0) return null;
    return fcf.value / ni.value;
  }, [lv]);

  // GuruFocus's pre-computed `Valuation and Quality > Interest Coverage` is
  // sparse on cash-rich / low-debt names (e.g. AAPL — GF returns "N/A" for
  // recent quarters because the ratio is huge or undefined). For a real
  // numeric we recompute from raw Operating Income / Interest Expense so
  // the row matches Debt-to-Equity's freshness whenever both components
  // are populated.
  const interestCoverage = useMemo<Snapshot['interestCoverage']>(() => {
    const op = lv(MC.OPERATING_INCOME);
    const ie = lv(MC.INTEREST_EXPENSE);
    if (op?.value != null && ie?.value != null && Math.abs(ie.value) > 0) {
      const value = op.value / Math.abs(ie.value);
      const date = op.date < ie.date ? op.date : ie.date;
      return { value, date, computed: true };
    }
    const fallback = lv(MC.INTEREST_COVERAGE);
    return fallback ? { value: fallback.value, date: fallback.date, computed: false } : null;
  }, [lv]);

  // Revenue and FCF flow series prefer the quarterly twin's TTM (so the
  // latest data point lands at the most recent reported quarter end, not
  // the latest fiscal-year close). Falls back to annuals when fewer than
  // 5 quarterly points are available. The TTM construction means each
  // point covers a full year of activity, so the CAGR / R² / SD math
  // stays apples-to-apples with the annual version — just shifted to a
  // freshly-anchored sliding window.
  const valueGrowth = useMemo<ValueGrowth>(() => {
    const priceSeries = timeSeries(metrics, 'close_price');
    const revSeries = flowSeriesPreferQuarterlyTTM(metrics, MC.REVENUE);
    const fcfSeries = flowSeriesPreferQuarterlyTTM(metrics, MC.FCF);

    const price5Y = trailingYearsWindow(priceSeries, 5);
    const price10Y = trailingYearsWindow(priceSeries, 10);
    const rev5Y = trailingYearsWindow(revSeries, 5);
    const fcf5Y = trailingYearsWindow(fcfSeries, 5);

    const lastDate = (s: { date: string }[]) => (s.length ? s[s.length - 1].date : null);

    const fcfYoY = fcf5Y.length >= 8 ? ttmYoYGrowthRates(fcf5Y) : yoyGrowthRates(fcf5Y);

    // FCF CAGR / R² salvage: if the trailing 5Y starts in a negative-FCF
    // era (e.g. heavy CAPEX expansion — Dino Polska 2021-22), strict
    // CAGR / R² return null because of non-positive endpoints. Fall back
    // to the longest trailing contiguous positive run within the 5Y
    // window if it spans ≥ 2 years.
    const fcfTail = trailingPositiveRun(fcf5Y);
    const fcfSeriesForGrowth = fcfTail.spanYears >= 2 ? fcfTail.trimmed : fcf5Y;
    const fcfPartialWindow = fcfTail.spanYears >= 2 && fcfTail.trimmed.length < fcf5Y.length;

    return {
      price5YCAGR: computeCAGR(price5Y),
      price5YR2: logLinearR2(price5Y),
      price10YCAGR: computeCAGR(price10Y),
      price10YR2: logLinearR2(price10Y),
      priceDate: lastDate(priceSeries),
      rev5YCAGR: computeCAGR(rev5Y),
      rev5YR2: logLinearR2(rev5Y),
      revDate: lastDate(revSeries),
      fcf5YCAGR: computeCAGR(fcfSeriesForGrowth),
      fcf5YR2: logLinearR2(fcfSeriesForGrowth),
      fcfGrowthSD: stdDev(fcfYoY),
      fcfDate: lastDate(fcfSeries),
      fcfGrowthFromDate: fcfPartialWindow ? fcfSeriesForGrowth[0]?.date ?? null : null,
      fcfGrowthToDate: fcfPartialWindow ? fcfSeriesForGrowth[fcfSeriesForGrowth.length - 1]?.date ?? null : null,
      price5Y,
      price10Y,
      rev5Y,
      fcf5Y,
      fcfTrailingPositiveSpanYears: fcfTail.spanYears,
      fcfYoYRatesCount: fcfYoY.length,
    };
  }, [metrics]);

  // Cadence sources for the computed Value-Creation / Historical-Growth rows.
  // The displayed metric is derived (CAGR, R², SD), but the user wants to
  // know how often the underlying observations land — i.e. is this 5Y
  // growth built from 5 annual points or 20 quarterly ones?
  const priceCadence = useMemo(() => observationCadence(metrics, 'close_price'), [metrics]);
  const revenueCadence = cadenceFor(MC.REVENUE);
  const fcfCadence = cadenceFor(MC.FCF);

  // For the computed Interest Coverage row: report the slower of the two
  // anchor cadences (Operating Income vs Interest Expense). When neither
  // component is available we fall back to GuruFocus's pre-computed
  // series and report its cadence directly.
  const interestCoverageCadence = useMemo<Cadence | null>(() => {
    if (interestCoverage?.computed) {
      const opC = cadenceFor(MC.OPERATING_INCOME);
      const ieC = cadenceFor(MC.INTEREST_EXPENSE);
      if (!opC) return ieC ?? null;
      if (!ieC) return opC;
      return (opC.medianDays ?? 0) >= (ieC.medianDays ?? 0) ? opC : ieC;
    }
    return cadenceFor(MC.INTEREST_COVERAGE);
  }, [interestCoverage, cadenceFor]);

  return {
    lv, cadenceFor,
    fcfOverNi, interestCoverage, valueGrowth,
    priceCadence, revenueCadence, fcfCadence, interestCoverageCadence,
  };
}

// ─── Row specs — defined once, materialized per snapshot ───────────

type StatCell = {
  value: string;
  score: Score | null;
  date: string | null | undefined;
  cadence: Cadence | null | undefined;
};

type RowSpec = {
  label: string;
  source: EarningsSource;
  /** Info tooltip text. A function so rows whose info depends on the
   * underlying values (e.g. Interest Coverage's "computed vs fallback"
   * disclosure, FCF Growth's window-trim disclosure) can read from the
   * snapshot. Most rows just return a static string. The info shown is
   * sourced from snapshot A; comparison context (B) doesn't change the
   * tooltip — the metric definition is the same regardless of which
   * company you're looking at. */
  info: (snap: Snapshot) => string;
  /** Build the value+score+date+cadence cell from a snapshot. */
  cellFrom: (snap: Snapshot) => StatCell;
  /** Explain WHY this metric came out empty for the given snapshot.
   * Render layer calls this only when `cellFrom(snap).value === '—'`
   * and appends the result to the info tooltip. Return null when the
   * metric isn't blank or there's no specific reason to surface. */
  whyEmpty?: (snap: Snapshot) => string | null;
};

// ─── whyEmpty helpers ──────────────────────────────────────────────

/** Format a number with a "M" suffix when |v| ≥ 100 (matches the
 * magnitude of revenue / FCF numbers in millions). Use only in the
 * "why empty" prose to avoid pulling in fmt* which adds % suffixes. */
function fmtBare(v: number | null | undefined, digits = 2): string {
  if (v == null || !Number.isFinite(v)) return '—';
  return v.toLocaleString(undefined, { minimumFractionDigits: digits, maximumFractionDigits: digits });
}

/** Generic "metric was empty because lv() found nothing" reason —
 * used by every row whose value comes straight from a `s.lv(code)`
 * call. The code shown is the one from MC, not the resolved one, so
 * the user sees what the dashboard asked for. */
function whyLvEmpty(s: Snapshot, code: string, codeLabel: string): string | null {
  const r = s.lv(code);
  if (!r) return `GuruFocus didn't ship a ${codeLabel} observation for this ticker (annual or quarterly).`;
  if (r.value == null) return `Latest ${codeLabel} observation (${r.date}) is N/A in GuruFocus — the value isn't reported for that period.`;
  return null;
}

/** Common "trailing window starts with a non-positive value" prose —
 * shared by the FCF rows since that's the dominant blank-cause for
 * companies coming out of a loss-making era. */
function whyTrailingCAGREmpty(
  window: { date: string; value: number }[],
  metricLabel: string,
  windowYears: number,
): string | null {
  if (window.length === 0) return `No ${metricLabel} data in the trailing ${windowYears} years — need at least 2 observations spanning ${windowYears - 0.5}y+.`;
  if (window.length < 2) return `Only ${window.length} ${metricLabel} observation in the trailing ${windowYears} years — need at least 2.`;
  const start = window[0];
  const end = window[window.length - 1];
  const parts: string[] = [];
  if (start.value <= 0) parts.push(`window start ${start.date}: ${fmtBare(start.value)} is non-positive`);
  if (end.value <= 0) parts.push(`window end ${end.date}: ${fmtBare(end.value)} is non-positive`);
  if (parts.length === 0) return null;
  return `CAGR is undefined when an endpoint ≤ 0 — ${parts.join('; ')}. (end/start)^(1/years) doesn't reduce to a real number.`;
}

/** "R² needs all-positive values" prose — used for any log-linear R²
 * row (Price, Revenue, FCF). Lists how many points in the window were
 * non-positive when that's the cause. */
function whyLogR2Empty(
  window: { date: string; value: number }[],
  metricLabel: string,
  windowYears: number,
): string | null {
  if (window.length < 3) return `Need at least 3 ${metricLabel} observations in the trailing ${windowYears} years for a meaningful log-linear fit.`;
  const nonPositive = window.filter((p) => p.value <= 0);
  if (nonPositive.length > 0) {
    const examples = nonPositive.slice(0, 3).map((p) => `${p.date}: ${fmtBare(p.value)}`).join(', ');
    return `R² requires every point in the window to be > 0 (log of a non-positive number is undefined). ${nonPositive.length} of ${window.length} points fail this — ${examples}${nonPositive.length > 3 ? '…' : ''}.`;
  }
  return null;
}

type Section = { title: string; rows: RowSpec[] };

const LEFT_SECTIONS: Section[] = [
  {
    title: 'Balance Sheet',
    rows: [
      {
        label: 'Interest Coverage',
        source: 'financials',
        info: (s) => `EBIT / Interest Expense. Higher = more capacity to service debt. Below 3 is a warning sign. Scored green ≥7, yellow 3–7, red <3. ${s.interestCoverage?.computed ? 'Computed from quarterly Operating Income ÷ Interest Expense — uses GuruFocus\'s pre-computed series only when raw fields are missing.' : 'Sourced from GuruFocus\'s Valuation & Quality > Interest Coverage (raw Income Statement components weren\'t both populated).'}`,
        // <3 is the canonical "covenant warning" cutoff; ≥7 means
        // interest expense is a rounding error vs operating income.
        cellFrom: (s) => ({
          value: fmtNum(s.interestCoverage?.value ?? null),
          score: higherBetter(s.interestCoverage?.value, { poorBelow: 3, goodAtOrAbove: 7 }),
          date: s.interestCoverage?.date,
          cadence: s.interestCoverageCadence,
        }),
        whyEmpty: (s) => {
          if (s.interestCoverage == null) return 'Neither raw components (Operating Income, Interest Expense) nor GuruFocus\'s pre-computed Interest Coverage series shipped for this ticker.';
          if (s.interestCoverage.value == null) return `GuruFocus reported "N/A" for the latest period (${s.interestCoverage.date}) — typical for cash-rich names where interest expense is ~0 (ratio is huge/undefined).`;
          return null;
        },
      },
      {
        label: 'Debt / Equity',
        source: 'financials',
        info: () => 'Total Debt / Total Equity. Lower = less leverage. Above 2 warrants scrutiny. Scored green ≤0.5, yellow 0.5–2, red >2.',
        // <0.5 conservative / fortress balance sheet, >2 the cutoff the
        // info tooltip already calls out as "warrants scrutiny".
        cellFrom: (s) => {
          const r = s.lv(MC.DEBT_TO_EQUITY);
          return {
            value: fmtNum(r?.value ?? null),
            score: lowerBetter(r?.value, { goodAtOrBelow: 0.5, poorAbove: 2 }),
            date: r?.date,
            cadence: s.cadenceFor(MC.DEBT_TO_EQUITY),
          };
        },
        whyEmpty: (s) => whyLvEmpty(s, MC.DEBT_TO_EQUITY, 'Debt-to-Equity'),
      },
    ],
  },
  {
    title: 'Capital Intensity',
    rows: [
      {
        label: 'CAPEX / Revenue',
        source: 'financials',
        info: () => 'Capital expenditure as a share of revenue. Lower = more capital-light business model. Scored green ≤5%, yellow 5–15%, red >15%.',
        cellFrom: (s) => {
          const r = s.lv(MC.CAPEX_TO_REV);
          return {
            value: fmtNum(r?.value ?? null),
            score: lowerBetter(r?.value, { goodAtOrBelow: 5, poorAbove: 15 }),
            date: r?.date,
            cadence: s.cadenceFor(MC.CAPEX_TO_REV),
          };
        },
        whyEmpty: (s) => whyLvEmpty(s, MC.CAPEX_TO_REV, 'CAPEX/Revenue'),
      },
      {
        label: 'CAPEX / OCF',
        source: 'financials',
        info: () => 'Capital expenditure as a share of operating cash flow. Lower = more cash left after reinvestment. Scored green ≤30%, yellow 30–60%, red >60%.',
        cellFrom: (s) => {
          const r = s.lv(MC.CAPEX_TO_OCF);
          return {
            value: fmtNum(r?.value ?? null),
            score: lowerBetter(r?.value, { goodAtOrBelow: 30, poorAbove: 60 }),
            date: r?.date,
            cadence: s.cadenceFor(MC.CAPEX_TO_OCF),
          };
        },
        whyEmpty: (s) => whyLvEmpty(s, MC.CAPEX_TO_OCF, 'CAPEX/OCF'),
      },
    ],
  },
  {
    title: 'Capital Allocation',
    rows: [
      {
        label: 'ROE',
        source: 'financials',
        info: () => 'Return on Equity = Net Income / Shareholders\' Equity. Measures profit generated per dollar of equity. Scored green ≥15%, yellow 8–15%, red <8%.',
        cellFrom: (s) => {
          const r = s.lv(MC.ROE);
          return {
            value: fmtPctPoints(r?.value ?? null),
            score: higherBetter(r?.value, { poorBelow: 8, goodAtOrAbove: 15 }),
            date: r?.date,
            cadence: s.cadenceFor(MC.ROE),
          };
        },
        whyEmpty: (s) => whyLvEmpty(s, MC.ROE, 'ROE'),
      },
      {
        label: 'ROIC',
        source: 'financials',
        info: () => 'Return on Invested Capital = NOPAT / Invested Capital. Measures efficiency of all capital deployed. Scored green ≥15%, yellow 8–15%, red <8%.',
        cellFrom: (s) => {
          const r = s.lv(MC.ROIC);
          return {
            value: fmtPctPoints(r?.value ?? null),
            score: higherBetter(r?.value, { poorBelow: 8, goodAtOrAbove: 15 }),
            date: r?.date,
            cadence: s.cadenceFor(MC.ROIC),
          };
        },
        whyEmpty: (s) => whyLvEmpty(s, MC.ROIC, 'ROIC'),
      },
    ],
  },
  {
    title: 'Value Creation',
    rows: [
      {
        label: 'Price 5Y CAGR',
        source: 'prices',
        info: () => 'Compound Annual Growth Rate of share price over the last 5 years, from GuruFocus daily close prices. Scored green ≥12%, yellow 5–12%, red <5%.',
        cellFrom: (s) => ({
          value: fmtPct(s.valueGrowth.price5YCAGR),
          score: higherBetter(s.valueGrowth.price5YCAGR, { poorBelow: 0.05, goodAtOrAbove: 0.12 }),
          date: s.valueGrowth.priceDate,
          cadence: s.priceCadence,
        }),
        whyEmpty: (s) => whyTrailingCAGREmpty(s.valueGrowth.price5Y, 'close-price', 5),
      },
      {
        label: 'Price 5Y R²',
        source: 'prices',
        info: () => 'R-squared of log-linear regression of share price vs time over the last 5 years. Higher = more consistent growth. Scored green ≥0.8, yellow 0.5–0.8, red <0.5.',
        cellFrom: (s) => ({
          value: fmtNum(s.valueGrowth.price5YR2),
          score: higherBetter(s.valueGrowth.price5YR2, { poorBelow: 0.5, goodAtOrAbove: 0.8 }),
          date: s.valueGrowth.priceDate,
          cadence: s.priceCadence,
        }),
        whyEmpty: (s) => whyLogR2Empty(s.valueGrowth.price5Y, 'close-price', 5),
      },
      {
        label: 'Price 10Y CAGR',
        source: 'prices',
        info: () => 'Compound Annual Growth Rate of share price over the last 10 years, from GuruFocus daily close prices. Scored green ≥12%, yellow 5–12%, red <5%.',
        cellFrom: (s) => ({
          value: fmtPct(s.valueGrowth.price10YCAGR),
          score: higherBetter(s.valueGrowth.price10YCAGR, { poorBelow: 0.05, goodAtOrAbove: 0.12 }),
          date: s.valueGrowth.priceDate,
          cadence: s.priceCadence,
        }),
        whyEmpty: (s) => whyTrailingCAGREmpty(s.valueGrowth.price10Y, 'close-price', 10),
      },
      {
        label: 'Price 10Y R²',
        source: 'prices',
        info: () => 'R-squared of log-linear regression of share price vs time over the last 10 years. Higher = more consistent growth. Scored green ≥0.8, yellow 0.5–0.8, red <0.5.',
        cellFrom: (s) => ({
          value: fmtNum(s.valueGrowth.price10YR2),
          score: higherBetter(s.valueGrowth.price10YR2, { poorBelow: 0.5, goodAtOrAbove: 0.8 }),
          date: s.valueGrowth.priceDate,
          cadence: s.priceCadence,
        }),
        whyEmpty: (s) => whyLogR2Empty(s.valueGrowth.price10Y, 'close-price', 10),
      },
    ],
  },
];

const RIGHT_SECTIONS: Section[] = [
  {
    title: 'Profitability',
    rows: [
      {
        label: 'Gross Margin',
        source: 'financials',
        info: () => 'Gross Profit / Revenue. Indicates pricing power and cost of goods sold efficiency. Scored green ≥40%, yellow 20–40%, red <20%.',
        cellFrom: (s) => {
          const r = s.lv(MC.GROSS_MARGIN);
          return {
            value: fmtPctPoints(r?.value ?? null),
            score: higherBetter(r?.value, { poorBelow: 20, goodAtOrAbove: 40 }),
            date: r?.date,
            cadence: s.cadenceFor(MC.GROSS_MARGIN),
          };
        },
        whyEmpty: (s) => whyLvEmpty(s, MC.GROSS_MARGIN, 'Gross Margin'),
      },
      {
        label: 'Net Margin',
        source: 'financials',
        info: () => 'Net Income / Revenue. Bottom-line profitability after all expenses. Scored green ≥15%, yellow 5–15%, red <5%.',
        cellFrom: (s) => {
          const r = s.lv(MC.NET_MARGIN);
          return {
            value: fmtPctPoints(r?.value ?? null),
            score: higherBetter(r?.value, { poorBelow: 5, goodAtOrAbove: 15 }),
            date: r?.date,
            cadence: s.cadenceFor(MC.NET_MARGIN),
          };
        },
        whyEmpty: (s) => whyLvEmpty(s, MC.NET_MARGIN, 'Net Margin'),
      },
      {
        label: 'FCF / Net Income',
        source: 'financials',
        info: () => 'Free Cash Flow / Net Income. Above 1 means cash earnings exceed accounting earnings (high quality). Scored green ≥120%, yellow 80–120%, red <80%.',
        cellFrom: (s) => ({
          value: fmtPct(s.fcfOverNi),
          score: higherBetter(s.fcfOverNi, { poorBelow: 0.8, goodAtOrAbove: 1.2 }),
          date: s.lv(MC.FCF)?.date,
          cadence: s.cadenceFor(MC.FCF),
        }),
        whyEmpty: (s) => {
          const fcf = s.lv(MC.FCF);
          const ni = s.lv(MC.NET_INCOME);
          if (!fcf) return 'No FCF observation found in GuruFocus.';
          if (!ni) return 'No Net Income observation found in GuruFocus.';
          if (fcf.value == null) return `FCF for the latest period (${fcf.date}) is N/A.`;
          if (ni.value == null) return `Net Income for the latest period (${ni.date}) is N/A.`;
          if (ni.value === 0) return `Net Income for the latest period (${ni.date}) is exactly 0 — division by zero, ratio undefined.`;
          return null;
        },
      },
    ],
  },
  {
    title: 'Historical Growth',
    rows: [
      {
        label: 'Revenue 5Y Growth',
        source: 'financials',
        info: () => '5-year revenue CAGR. Anchored to the latest quarter\'s trailing-twelve-months revenue when quarterly data is available, otherwise to annual revenue. Scored green ≥10%, yellow 3–10%, red <3%.',
        cellFrom: (s) => ({
          value: fmtPct(s.valueGrowth.rev5YCAGR),
          score: higherBetter(s.valueGrowth.rev5YCAGR, { poorBelow: 0.03, goodAtOrAbove: 0.10 }),
          date: s.valueGrowth.revDate,
          cadence: s.revenueCadence,
        }),
        whyEmpty: (s) => whyTrailingCAGREmpty(s.valueGrowth.rev5Y, 'revenue', 5),
      },
      {
        label: 'Revenue R²',
        source: 'financials',
        info: () => 'R-squared of log-linear regression of TTM revenue vs time over the last 5 years. Higher = more consistent growth. Scored green ≥0.9, yellow 0.7–0.9, red <0.7.',
        cellFrom: (s) => ({
          value: fmtNum(s.valueGrowth.rev5YR2),
          score: higherBetter(s.valueGrowth.rev5YR2, { poorBelow: 0.7, goodAtOrAbove: 0.9 }),
          date: s.valueGrowth.revDate,
          cadence: s.revenueCadence,
        }),
        whyEmpty: (s) => whyLogR2Empty(s.valueGrowth.rev5Y, 'revenue', 5),
      },
      {
        label: 'FCF 5Y Growth',
        source: 'financials',
        info: (s) => `5-year FCF CAGR. Same TTM-quarterly construction as Revenue. ${s.valueGrowth.fcfGrowthFromDate ? `Trailing 5Y included non-positive FCF (heavy CAPEX or losses); CAGR is computed over the most recent contiguous positive run from ${s.valueGrowth.fcfGrowthFromDate} to ${s.valueGrowth.fcfGrowthToDate}.` : 'Null if the most recent contiguous positive-FCF run spans less than 2 years within the trailing 5Y window.'} Scored green ≥10%, yellow 3–10%, red <3%.`,
        cellFrom: (s) => ({
          value: fmtPct(s.valueGrowth.fcf5YCAGR),
          score: higherBetter(s.valueGrowth.fcf5YCAGR, { poorBelow: 0.03, goodAtOrAbove: 0.10 }),
          date: s.valueGrowth.fcfDate,
          cadence: s.fcfCadence,
        }),
        whyEmpty: (s) => {
          const base = whyTrailingCAGREmpty(s.valueGrowth.fcf5Y, 'FCF', 5);
          if (!base) return null;
          const tailNote = s.valueGrowth.fcfTrailingPositiveSpanYears > 0
            ? ` Salvage fallback: the most recent contiguous positive-FCF run spans ${s.valueGrowth.fcfTrailingPositiveSpanYears.toFixed(1)}y; need ≥2y to use it.`
            : ' Salvage fallback: no contiguous positive-FCF run found within the trailing 5Y window.';
          return base + tailNote;
        },
      },
      {
        label: 'FCF Growth R²',
        source: 'financials',
        info: (s) => `R-squared of log-linear regression of TTM FCF vs time over the last 5 years. ${s.valueGrowth.fcfGrowthFromDate ? `Trailing 5Y included non-positive FCF; R² is fit over the most recent contiguous positive run from ${s.valueGrowth.fcfGrowthFromDate} to ${s.valueGrowth.fcfGrowthToDate}.` : 'Null if the most recent contiguous positive-FCF run spans less than 2 years within the trailing 5Y window.'} Scored green ≥0.9, yellow 0.7–0.9, red <0.7.`,
        cellFrom: (s) => ({
          value: fmtNum(s.valueGrowth.fcf5YR2),
          score: higherBetter(s.valueGrowth.fcf5YR2, { poorBelow: 0.7, goodAtOrAbove: 0.9 }),
          date: s.valueGrowth.fcfDate,
          cadence: s.fcfCadence,
        }),
        whyEmpty: (s) => whyLogR2Empty(s.valueGrowth.fcf5Y, 'FCF', 5),
      },
      {
        label: 'FCF Growth SD',
        source: 'financials',
        info: () => 'Standard deviation of 4-quarter-lag TTM FCF growth rates over the last 5 years (or annual YoY rates when quarterly data is sparse). Lower = more predictable. Scored green ≤20%, yellow 20–50%, red >50%.',
        cellFrom: (s) => ({
          value: fmtPct(s.valueGrowth.fcfGrowthSD),
          score: lowerBetter(s.valueGrowth.fcfGrowthSD, { goodAtOrBelow: 0.2, poorAbove: 0.5 }),
          date: s.valueGrowth.fcfDate,
          cadence: s.fcfCadence,
        }),
        whyEmpty: (s) => {
          const n = s.valueGrowth.fcfYoYRatesCount;
          if (n < 2) return `Need at least 2 usable YoY growth rates (both-endpoints-positive) to compute SD. Found ${n} in the trailing 5Y window — too many FCF sign flips for valid rate calculations.`;
          return null;
        },
      },
    ],
  },
  {
    title: 'Outlook',
    rows: [
      {
        label: 'EPS LT Growth EST',
        source: 'analyst_estimates',
        info: () => 'Analyst consensus long-term EPS growth rate estimate (3-5 years forward). Scored green ≥12%, yellow 5–12%, red <5%.',
        cellFrom: (s) => {
          const r = s.lv(MC.EPS_EST);
          return {
            value: fmtPctPoints(r?.value ?? null),
            score: higherBetter(r?.value, { poorBelow: 5, goodAtOrAbove: 12 }),
            date: r?.date,
            cadence: s.cadenceFor(MC.EPS_EST),
          };
        },
        whyEmpty: (s) => {
          const r = s.lv(MC.EPS_EST);
          if (!r) return 'GuruFocus has no analyst long-term EPS growth estimate for this ticker — common for small caps and non-US names with sparse sell-side coverage.';
          if (r.value == null) return `Latest estimate (${r.date}) is N/A — no consensus reported for that period.`;
          return null;
        },
      },
    ],
  },
  {
    title: 'Valuation',
    rows: [
      {
        label: 'Forward P/E',
        source: 'indicators',
        info: () => 'Price / Forward EPS estimate. Lower = cheaper relative to expected earnings. Scored green ≤15, yellow 15–25, red >25.',
        cellFrom: (s) => {
          const r = s.lv(MC.FWD_PE);
          return {
            value: fmtNum(r?.value ?? null),
            score: lowerBetter(r?.value, { goodAtOrBelow: 15, poorAbove: 25 }),
            date: r?.date,
            cadence: s.cadenceFor(MC.FWD_PE),
          };
        },
        whyEmpty: (s) => whyLvEmpty(s, MC.FWD_PE, 'Forward P/E'),
      },
      {
        label: 'PEG',
        source: 'financials',
        info: () => 'P/E / EPS Growth Rate. Below 1 suggests undervalued relative to growth; above 2 may be expensive. Rendered as "—" when GuruFocus couldn\'t compute it (zero or negative EPS growth — the ratio is mathematically undefined; GF stores exactly 0 as its sentinel). Scored green ≤1, yellow 1–2, red >2.',
        cellFrom: (s) => {
          const r = s.lv(MC.PEG);
          // GuruFocus stores PEG = 0 when EPS growth is ≤ 0 (the ratio
          // is undefined). Treat exact-0 as null so the row doesn't
          // render a misleading "great PEG, scored green".
          const v = r?.value === 0 ? null : (r?.value ?? null);
          return {
            value: fmtNum(v),
            score: lowerBetter(v, { goodAtOrBelow: 1, poorAbove: 2 }),
            date: r?.date,
            cadence: s.cadenceFor(MC.PEG),
          };
        },
        whyEmpty: (s) => {
          const r = s.lv(MC.PEG);
          if (!r) return 'No PEG observation found in GuruFocus.';
          if (r.value === 0) return `GuruFocus stored PEG as exactly 0 for the latest period (${r.date}) — its sentinel for "couldn't compute" (typically because EPS growth was zero or negative, making P/E ÷ growth mathematically undefined).`;
          if (r.value == null) return `Latest PEG observation (${r.date}) is N/A in GuruFocus.`;
          return null;
        },
      },
    ],
  },
];

// ─── Component ─────────────────────────────────────────────────────

export default function SnapshotStats({
  metrics,
  metricsB,
  labelA,
  labelB,
  refreshingSources,
  refreshingSourcesB,
  loadingB,
}: {
  metrics: MetricRow[];
  /** When supplied, every row renders a second value cell to the right
   * of the primary cell. The two cells share label, info, and scoring
   * rubric — scoring is computed independently per side. */
  metricsB?: MetricRow[];
  /** Display labels shown in the comparison header (e.g. "AAPL" /
   * "MSFT"). Only used when `metricsB` is set. */
  labelA?: string;
  labelB?: string;
  /** Sources currently being refreshed for A. A row whose source is in
   * the set renders a spinner next to A's value cell. */
  refreshingSources?: Set<string>;
  /** Same as `refreshingSources` but for the comparison column. */
  refreshingSourcesB?: Set<string>;
  /** True during B's initial metrics fetch (before any SSE refresh
   * starts). Replaces every B value with a spinner so the user sees
   * "B is loading" without losing A's data. */
  loadingB?: boolean;
}) {
  const snapA = useSnapshot(metrics);
  // Always call the hook (rules of hooks) — passing an empty array when
  // there's no comparison company yields a snapshot whose every cell
  // renders as "—". The component then skips the B column anyway based
  // on `!!metricsB`, so the wasted work doesn't reach the DOM.
  const snapB = useSnapshot(metricsB ?? []);
  const hasB = !!metricsB;

  function renderColumn(sections: Section[]) {
    return (
      <div className="space-y-4">
        {sections.map((sec) => (
          <div key={sec.title}>
            <div className="text-fg-muted text-xs font-semibold uppercase tracking-wider mb-2 flex items-center justify-between gap-2">
              <span>{sec.title}</span>
              {hasB && (labelA || labelB) && (
                <span className="text-[10px] normal-case font-mono tracking-normal flex items-center gap-3">
                  {labelA && <span className="text-accent-400">{labelA}</span>}
                  {labelB && <span className="text-warn-400">{labelB}</span>}
                </span>
              )}
            </div>
            <div className="space-y-1">
              {sec.rows.map((row) => {
                const cellA = row.cellFrom(snapA);
                const cellB = hasB ? row.cellFrom(snapB) : null;
                // Compose the info tooltip dynamically: metric definition +
                // (when a cell is empty) the row's per-side `whyEmpty`
                // explanation. Empty detection uses `score === null` —
                // every scorer returns null when the underlying value
                // is null/non-finite, so it's a more robust signal than
                // a Unicode em-dash string match.
                const baseInfo = row.info(snapA);
                const empties: string[] = [];
                if (row.whyEmpty) {
                  if (cellA.score === null) {
                    const why = row.whyEmpty(snapA);
                    if (why) empties.push(hasB ? `${labelA ?? 'A'} is blank — ${why}` : `Blank because: ${why}`);
                  }
                  if (cellB && cellB.score === null && !loadingB) {
                    const why = row.whyEmpty(snapB);
                    if (why) empties.push(`${labelB ?? 'B'} is blank — ${why}`);
                  }
                }
                const infoText = empties.length > 0
                  ? `${baseInfo}\n\n${empties.join('\n\n')}`
                  : baseInfo;
                return (
                  <div key={row.label} className="flex justify-between items-center gap-3">
                    <span className="text-fg-muted text-sm flex items-center gap-1.5 min-w-0">
                      <span className="truncate">{row.label}</span>
                      <InfoTip text={infoText} />
                    </span>
                    <div className="flex items-stretch gap-4 shrink-0">
                      <ValueCell cell={cellA} source={row.source} refreshingSources={refreshingSources} />
                      {cellB && <ValueCell cell={cellB} source={row.source} refreshingSources={refreshingSourcesB} loading={loadingB} />}
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        ))}
      </div>
    );
  }

  return (
    <div className="grid grid-cols-2 gap-8">
      {renderColumn(LEFT_SECTIONS)}
      {renderColumn(RIGHT_SECTIONS)}
    </div>
  );
}

function ValueCell({
  cell,
  source,
  refreshingSources,
  loading,
}: {
  cell: StatCell;
  source: EarningsSource;
  refreshingSources?: Set<string>;
  /** When true, the cell shows ONLY a spinner — used for B's column
   * during its initial metrics fetch, before per-source refresh
   * tracking can take over. */
  loading?: boolean;
}) {
  // Score color: emerald / amber / rose for graded rows; fall back to
  // white when the metric has no scoring rubric or the value is N/A.
  const valueColor = cell.score ? SCORE_TEXT_COLOR[cell.score] : 'text-fg-strong';
  if (loading) {
    return (
      <span className="text-right">
        <span className="inline-flex items-center justify-end gap-1.5">
          <Spinner size={10} />
          <span className="text-fg-subtle font-mono text-sm">…</span>
        </span>
      </span>
    );
  }
  return (
    <span className="text-right">
      <span className="inline-flex items-center justify-end gap-1.5">
        {refreshingSources?.has(source) && <Spinner size={10} />}
        <span className={`${valueColor} font-mono text-sm`}>{cell.value}</span>
        {cell.cadence && <InfoTip text={cadenceHoverText(cell.cadence)} />}
      </span>
      {cell.date && (
        <span className="block text-fg-subtle text-[10px] font-mono">{cell.date}</span>
      )}
    </span>
  );
}
