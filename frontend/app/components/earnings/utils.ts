/**
 * Pure utility functions for the /earnings page. Three rough groups:
 *
 *   - **Metric extractors** that mine a `MetricRow[]` payload for the
 *     freshest / earliest / observed point on a given metric_code.
 *   - **Time-series builders** (annual → TTM → preferred-flow) that
 *     reshape the raw rows into a `{date, value}[]` series the charts
 *     and CAGR helpers consume.
 *   - **Generic statistical helpers** (`stdDev`, `computeCAGR`,
 *     `logLinearR2`, `trailingYearsWindow`, `yoyGrowthRates`) that
 *     could conceivably move to a shared `lib/financialMath.ts` later
 *     if another page needs them; today only earnings does.
 *   - **Number formatters** with EARNINGS semantics — `fmtPct(0.05)`
 *     returns `"5.00%"`, ie. the input is a FRACTION. This differs
 *     from `momentum/utils.ts:fmtPct` which expects the input to
 *     already be in percentage terms (`5 → "+5.00%"`). The naming
 *     collision is intentional: each page imports from its own utils
 *     so the semantics match.
 */

import type { Cadence, MetricRow } from './types';

// ─── Metric extractors ─────────────────────────────────────────────

export function latestValue(metrics: MetricRow[], code: string): { value: number; date: string } | null {
  let best: MetricRow | null = null;
  for (const m of metrics) {
    if (m.metric_code !== code || m.numeric_value == null) continue;
    if (!best || m.target_date > best.target_date) best = m;
  }
  return best ? { value: best.numeric_value!, date: best.target_date } : null;
}

/** Like `latestValue` but also returns rows whose `numeric_value` is null —
 * the parser emits null rows when GuruFocus reports "N/A" for a period that
 * exists in the response, so the dashboard can show the freshest period
 * label even when the value isn't meaningful (rather than walk back to a
 * stale numeric from years ago). */
export function latestObservation(metrics: MetricRow[], code: string): { value: number | null; date: string } | null {
  let best: MetricRow | null = null;
  for (const m of metrics) {
    if (m.metric_code !== code) continue;
    if (!best || m.target_date > best.target_date) best = m;
  }
  return best ? { value: best.numeric_value, date: best.target_date } : null;
}

/** Pick the earliest estimate after a reference date (for FY1 = next fiscal year). */
export function earliestFutureValue(metrics: MetricRow[], code: string, afterDate: string): { value: number; date: string } | null {
  let best: MetricRow | null = null;
  for (const m of metrics) {
    if (m.metric_code !== code || m.numeric_value == null) continue;
    if (m.target_date <= afterDate) continue;
    if (!best || m.target_date < best.target_date) best = m;
  }
  return best ? { value: best.numeric_value!, date: best.target_date } : null;
}

export function timeSeries(metrics: MetricRow[], code: string): { date: string; value: number }[] {
  return metrics
    .filter((m) => m.metric_code === code && m.numeric_value != null)
    .sort((a, b) => a.target_date.localeCompare(b.target_date))
    .map((m) => ({ date: m.target_date, value: m.numeric_value! }));
}

// ─── Cadence ───────────────────────────────────────────────────────

/** Empirical observation cadence for a metric_code: bucket the median gap
 * between consecutive periods where the source reported anything (numeric
 * or N/A — both produce metric_data rows). Useful for sanity-checking
 * "is this number stale or just slow-publishing?". */
export function observationCadence(metrics: MetricRow[], code: string): Cadence | null {
  const dates = metrics
    .filter((m) => m.metric_code === code)
    .map((m) => m.target_date)
    .sort();
  if (dates.length === 0) return null;
  if (dates.length === 1) {
    return { label: 'Single point', medianDays: null, count: 1, firstDate: dates[0], lastDate: dates[0] };
  }
  const gaps: number[] = [];
  for (let i = 1; i < dates.length; i++) {
    gaps.push((new Date(dates[i]).getTime() - new Date(dates[i - 1]).getTime()) / 86400000);
  }
  gaps.sort((a, b) => a - b);
  const median = gaps[Math.floor(gaps.length / 2)];
  let label: Cadence['label'];
  if (median <= 2) label = 'Daily';
  else if (median <= 14) label = 'Weekly';
  else if (median <= 45) label = 'Monthly';
  else if (median <= 120) label = 'Quarterly';
  else if (median <= 220) label = 'Semi-annual';
  else label = 'Annual';
  return {
    label,
    medianDays: Math.round(median),
    count: dates.length,
    firstDate: dates[0],
    lastDate: dates[dates.length - 1],
  };
}

export function cadenceHoverText(c: Cadence): string {
  if (c.medianDays == null) {
    return `${c.label}. Only 1 observation stored (${c.firstDate}).`;
  }
  return `${c.label} (~${c.medianDays} days between observations). ${c.count} observations from ${c.firstDate} to ${c.lastDate}.`;
}

// ─── Staleness ─────────────────────────────────────────────────────

/** Mirror of backend/ingest/staleness.py:is_cache_fresh — true when today's
 * date suggests at least one new observation should be available beyond
 * what's currently in `metrics` for this code. No history at all → stale. */
export function isMetricStale(metrics: MetricRow[], code: string, today: Date = new Date()): boolean {
  const dates = metrics.filter((m) => m.metric_code === code).map((m) => m.target_date).sort();
  if (dates.length === 0) return true;
  const last = new Date(dates[dates.length - 1] + 'T00:00:00Z');
  const todayMs = Date.UTC(today.getUTCFullYear(), today.getUTCMonth(), today.getUTCDate());
  const ageDays = (todayMs - last.getTime()) / 86400000;

  if (dates.length < 2) return ageDays > 7;

  const gaps: number[] = [];
  for (let i = 1; i < dates.length; i++) {
    const d = (new Date(dates[i] + 'T00:00:00Z').getTime() - new Date(dates[i - 1] + 'T00:00:00Z').getTime()) / 86400000;
    if (d > 0) gaps.push(d);
  }
  if (gaps.length === 0) return true;
  gaps.sort((a, b) => a - b);
  const median = gaps[Math.floor(gaps.length / 2)];

  if (median <= 2) {
    // Daily cadence: previous weekday strictly before today should be present.
    const mostRecent = new Date(todayMs);
    mostRecent.setUTCDate(mostRecent.getUTCDate() - 1);
    while (mostRecent.getUTCDay() === 0 || mostRecent.getUTCDay() === 6) {
      mostRecent.setUTCDate(mostRecent.getUTCDate() - 1);
    }
    return last < mostRecent;
  }
  const buffer = Math.max(1, Math.floor(median * 0.5));
  return ageDays > median + buffer;
}

/** Representative metric_code per earnings refresh source. If ALL representatives
 * for a source look stale, that source is due for a refresh. */
const SOURCE_REPRESENTATIVE_CODES: Record<string, string[]> = {
  prices: ['close_price'],
  indicators: ['indicator_q_forward_pe_ratio'],
  financials: [
    'quarterly__Income Statement__Revenue',
    'annuals__Income Statement__Revenue',
  ],
  analyst_estimates: ['annual_per_share_eps_estimate'],
};

/** Which earnings refresh sources look due for new data given today's date.
 * Returns a subset of {prices, indicators, financials, analyst_estimates}. */
export function expectedStaleSources(metrics: MetricRow[], today: Date = new Date()): string[] {
  const stale: string[] = [];
  for (const [source, codes] of Object.entries(SOURCE_REPRESENTATIVE_CODES)) {
    const anyFresh = codes.some((code) => !isMetricStale(metrics, code, today));
    if (!anyFresh) stale.push(source);
  }
  return stale;
}

// ─── Time-series builders ──────────────────────────────────────────

/** Collapse annual metrics to one point per calendar year (last date in each year). */
export function annualSeries(metrics: MetricRow[], code: string): { date: string; value: number }[] {
  const raw = timeSeries(metrics, code);
  const byYear: Record<string, { date: string; value: number }> = {};
  for (const p of raw) {
    const yr = p.date.slice(0, 4);
    if (!byYear[yr] || p.date > byYear[yr].date) byYear[yr] = p;
  }
  return Object.values(byYear).sort((a, b) => a.date.localeCompare(b.date));
}

/** Rolling trailing-twelve-months (TTM) sum over a quarterly flow series.
 * Returns one point per quarter starting from the 4th, where each value is
 * the sum of the previous 4 quarterly values. Use this to anchor 5Y CAGR
 * and trend stats to the latest reported quarter rather than the latest
 * fiscal-year close. */
export function ttmSeries(quarterly: { date: string; value: number }[]): { date: string; value: number }[] {
  if (quarterly.length < 4) return [];
  const out: { date: string; value: number }[] = [];
  for (let i = 3; i < quarterly.length; i++) {
    const sum = quarterly[i - 3].value + quarterly[i - 2].value + quarterly[i - 1].value + quarterly[i].value;
    out.push({ date: quarterly[i].date, value: sum });
  }
  return out;
}

/** True when the series's observations are spaced ~quarterly (median
 * gap ≤ 120 days). Used to gate the TTM construction below: many
 * European reporters (notably German names like 2G Energy) populate
 * the `quarterly__...` code with semi-annual entries (Jun-30, Dec-31)
 * — summing 4 of those is "trailing 24 months", not TTM. */
function isQuarterlyCadence(series: { date: string }[]): boolean {
  if (series.length < 4) return false;
  const gaps: number[] = [];
  for (let i = 1; i < series.length; i++) {
    const d = (new Date(series[i].date).getTime() - new Date(series[i - 1].date).getTime()) / 86400000;
    if (d > 0) gaps.push(d);
  }
  if (gaps.length === 0) return false;
  gaps.sort((a, b) => a - b);
  const median = gaps[Math.floor(gaps.length / 2)];
  return median <= 120;
}

/** Build the freshest reasonable flow series for a financials code: prefer
 * the quarterly twin's TTM (anchored to the latest reported quarter) when
 * we have enough quarterly history AND the underlying cadence is actually
 * quarterly. Falls back to the annual series for semi-annual reporters
 * and for companies / periods where GuruFocus only ships annual values. */
export function flowSeriesPreferQuarterlyTTM(metrics: MetricRow[], annualsCode: string): { date: string; value: number }[] {
  if (annualsCode.startsWith('annuals__')) {
    const qCode = 'quarterly__' + annualsCode.slice('annuals__'.length);
    const q = timeSeries(metrics, qCode);
    if (isQuarterlyCadence(q)) {
      const ttm = ttmSeries(q);
      if (ttm.length >= 5) return ttm;
    }
  }
  return annualSeries(metrics, annualsCode);
}

/** Year-over-year growth rates for a TTM series: each rate compares quarter
 * t's TTM to quarter (t-4)'s TTM, giving ~4× the data points of the annual
 * version. Used for "FCF Growth SD" to get a tighter volatility estimate
 * while keeping the same statistical meaning. */
export function ttmYoYGrowthRates(ttm: { date: string; value: number }[]): number[] {
  const rates: number[] = [];
  for (let i = 4; i < ttm.length; i++) {
    const prev = ttm[i - 4].value;
    const curr = ttm[i].value;
    if (prev > 0 && curr > 0) rates.push(curr / prev - 1);
  }
  return rates;
}

// ─── Generic statistical helpers ───────────────────────────────────

export function computeCAGR(series: { date: string; value: number }[], requirePositive = true): number | null {
  if (series.length < 2) return null;
  const start = series[0];
  const end = series[series.length - 1];
  if (requirePositive && (start.value <= 0 || end.value <= 0)) return null;
  const years = (new Date(end.date).getTime() - new Date(start.date).getTime()) / (365.25 * 86400000);
  if (years < 0.5) return null;
  return Math.pow(end.value / start.value, 1 / years) - 1;
}

/** R² of log-linear regression (log(value) vs time in years). Returns null if any value <= 0. */
export function logLinearR2(series: { date: string; value: number }[]): number | null {
  if (series.length < 3) return null;
  if (series.some((p) => p.value <= 0)) return null;
  const t0 = new Date(series[0].date).getTime();
  const xs = series.map((p) => (new Date(p.date).getTime() - t0) / (365.25 * 86400000));
  const ys = series.map((p) => Math.log(p.value));
  const n = xs.length;
  const meanX = xs.reduce((s, v) => s + v, 0) / n;
  const meanY = ys.reduce((s, v) => s + v, 0) / n;
  let sxx = 0;
  let syy = 0;
  let sxy = 0;
  for (let i = 0; i < n; i++) {
    const dx = xs[i] - meanX;
    const dy = ys[i] - meanY;
    sxx += dx * dx;
    syy += dy * dy;
    sxy += dx * dy;
  }
  if (sxx === 0 || syy === 0) return null;
  const r = sxy / Math.sqrt(sxx * syy);
  return r * r;
}

/** Walk backward from the last point and return the longest run of
 * strictly-positive contiguous values ending at that point, along with
 * its span in years. Use to salvage CAGR / R² on FCF for companies whose
 * 5Y window starts in a heavy-CAPEX / loss-making era — Dino Polska
 * 2021-22 is the canonical case (sparse positive points before the
 * 2023+ positive-FCF era; the contiguous tail picks the actual era).
 *
 * Why contiguous rather than "all positives": a lone positive point
 * inside a sea of negative quarters (e.g. Dino's 2022-Q3 = +60 in the
 * middle of a -200…-300 range) would inflate CAGR wildly when treated
 * as the series start. Restricting to the tail gives the most recent
 * coherent positive era. Returns `{ trimmed: [], spanYears: 0 }` when
 * the tail is fewer than 2 points. */
export function trailingPositiveRun(
  series: { date: string; value: number }[],
): { trimmed: { date: string; value: number }[]; spanYears: number } {
  if (series.length === 0) return { trimmed: [], spanYears: 0 };
  let start = series.length;
  for (let i = series.length - 1; i >= 0; i--) {
    if (series[i].value > 0) start = i; else break;
  }
  const trimmed = series.slice(start);
  if (trimmed.length < 2) return { trimmed: [], spanYears: 0 };
  const spanYears =
    (new Date(trimmed[trimmed.length - 1].date).getTime() -
      new Date(trimmed[0].date).getTime()) /
    (365.25 * 86400000);
  return { trimmed, spanYears };
}

/** Filter a daily/annual series to entries within the trailing N years
 * of its last point. Uses real calendar-year subtraction for the cutoff
 * — NOT `N * 365.25 days`, which misses by ~1 day when the window
 * straddles two leap years (e.g. 2019-12-31 to 2024-12-31 is 1827 days,
 * but 5 * 365.25 = 1826.25, so a year-end entry 5 calendar years before
 * the anchor was wrongly excluded). The boundary entry stays included
 * by the inclusive `>=` comparison. */
export function trailingYearsWindow(series: { date: string; value: number }[], years: number): { date: string; value: number }[] {
  if (series.length === 0) return [];
  const endStr = series[series.length - 1].date;
  const endMs = new Date(endStr).getTime();
  // Calendar-year subtraction. Date.UTC handles month-end edge cases
  // (e.g. Feb-29 - 5 years → Feb-28 of a non-leap year) the way the
  // intent of "exactly N calendar years ago" suggests.
  const endDate = new Date(endStr);
  const cutoffDate = new Date(Date.UTC(
    endDate.getUTCFullYear() - years,
    endDate.getUTCMonth(),
    endDate.getUTCDate(),
  ));
  const cutoffMs = cutoffDate.getTime();
  const filtered = series.filter((p) => new Date(p.date).getTime() >= cutoffMs);
  if (filtered.length < 2) return [];
  // Tolerance: the earliest point should not be more recent than (years
  // - 0.5) ago, i.e. we need enough span to actually represent N years.
  const span = (endMs - new Date(filtered[0].date).getTime()) / (365.25 * 86400000);
  if (span < years - 0.5) return [];
  return filtered;
}

export function yoyGrowthRates(series: { date: string; value: number }[]): number[] {
  const rates: number[] = [];
  for (let i = 1; i < series.length; i++) {
    const prev = series[i - 1].value;
    const curr = series[i].value;
    if (prev > 0 && curr > 0) rates.push(curr / prev - 1);
  }
  return rates;
}

export function stdDev(values: number[]): number | null {
  if (values.length < 2) return null;
  const mean = values.reduce((s, v) => s + v, 0) / values.length;
  const variance = values.reduce((s, v) => s + (v - mean) ** 2, 0) / (values.length - 1);
  return Math.sqrt(variance);
}

// ─── Recharts shared style ─────────────────────────────────────────

/** `style` value for recharts `<Tooltip>` — matches the dark-theme cards
 * on the /earnings page. Different shape from `momentum/utils.ts`'s
 * `tooltipStyle` (which is an object of style slots); each page has its
 * own to match its visual treatment. */
export const tooltipStyle = {
  backgroundColor: '#151821',
  border: '1px solid #374151',
  borderRadius: '8px',
};

// ─── Number formatters (earnings semantics — input is a FRACTION) ──

/** 0.05 → "5.00%". Use this when the source field is a ratio. */
export function fmtPct(v: number | null): string {
  if (v == null) return '—';
  return `${(v * 100).toFixed(2)}%`;
}

/** Plain number with fixed-precision and thousand separators. */
export function fmtNum(v: number | null, digits = 2): string {
  if (v == null) return '—';
  return v.toLocaleString(undefined, { minimumFractionDigits: digits, maximumFractionDigits: digits });
}

/** 5 → "5.00%". Use when the source field is already in percentage points. */
export function fmtPctPoints(v: number | null): string {
  if (v == null) return '—';
  return `${v.toFixed(2)}%`;
}
