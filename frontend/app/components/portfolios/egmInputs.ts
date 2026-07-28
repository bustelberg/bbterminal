/**
 * Pulling the EGM's four inputs (+ two reference hints) out of the company metrics payload the
 * modal already loads. Pure, so the metric-code traps below are unit-tested rather than discovered
 * as a wrong valuation.
 *
 * ⚠ NOTHING HERE RE-FETCHES. `/api/earnings/by-isin/{isin}/metrics` already returns every code —
 * daily closes, the forward-P/E indicator, the fiscal statement lines and the analyst estimates —
 * so a second request would only be a second chance to disagree with the other tabs.
 */

import { type MetricRow } from './quickValuation';

// ⚠ THREE NAMING SCHEMES IN ONE STREAM. A statement line is `annuals__Section__Line`, an analyst
// estimate is `annual_<field>` (SINGULAR, no section, no double underscore), and the forward P/E is
// `indicator_q_forward_pe_ratio` — an indicator, a third scheme again. Each is matched explicitly;
// a pattern that catches two of the three leaves its panel reading "n/a" beside ones that filled in.
const PRICE_CODE = 'close_price';
const FWD_PE_CODE = 'indicator_q_forward_pe_ratio';
const EPS_EST_CODE = 'annual_per_share_eps_estimate';
// Both section spellings, and the quarterly twin — a fresher point-in-time yield than the last
// fiscal year's, which can be nine months stale.
const DIV_YIELD_CODES = [
  'quarterly__Valuation Ratios__Dividend Yield %',
  'quarterly__valuation_ratios__Dividend Yield %',
  'annuals__Valuation Ratios__Dividend Yield %',
  'annuals__valuation_ratios__Dividend Yield %',
];
const EPS_CODES = [
  'annuals__Per Share Data__EPS without NRI',
  'annuals__per_share_data__EPS without NRI',
  'annuals__per_share_data_array__EPS without NRI',
];
const PRICE_PS_CODES = [
  'annuals__Per Share Data__Month End Stock Price',
  'annuals__per_share_data__Month End Stock Price',
  'annuals__per_share_data_array__Month End Stock Price',
];

// ── The reverse-DCF's inputs ────────────────────────────────────────────────────────────────
const SHARES_CODES = [
  'annuals__Income Statement__Shares Outstanding (Diluted Average)',
  'annuals__income_statement__Shares Outstanding (Diluted Average)',
];
const WACC_CODES = [
  'annuals__Ratios__WACC %',
  'annuals__ratios__WACC %',
];
const FCF_CODES = [
  'annuals__Cashflow Statement__Free Cash Flow',
  'annuals__cashflow_statement__Free Cash Flow',
];

export type EgmSource = {
  price: number | null;
  forwardPE: number | null;
  dividendYield: number | null;   // decimal
  epsNextFY: number | null;
  epsNextFYDate: string | null;   // which fiscal period the estimate is for
  analystGrowth5Y: number | null; // decimal — reference only, never fed to the math
  medianPE5Y: number | null;      // reference only
};

/** The most recent observation of any of these codes, whatever its date. */
function latest(metrics: MetricRow[], codes: string[]): { date: string; value: number } | null {
  const want = new Set(codes);
  let best: { date: string; value: number } | null = null;
  for (const m of metrics) {
    if (!want.has(m.metric_code) || m.numeric_value == null) continue;
    if (!best || m.target_date > best.date) best = { date: m.target_date, value: m.numeric_value };
  }
  return best;
}

/** {year: value} — the latest observation within each fiscal year. */
function byYear(metrics: MetricRow[], codes: string[]): Map<number, number> {
  const want = new Set(codes);
  const acc = new Map<number, { date: string; value: number }>();
  for (const m of metrics) {
    if (!want.has(m.metric_code) || m.numeric_value == null) continue;
    const y = parseInt(String(m.target_date).slice(0, 4), 10);
    if (!Number.isFinite(y)) continue;
    const cur = acc.get(y);
    if (!cur || m.target_date > cur.date) acc.set(y, { date: m.target_date, value: m.numeric_value });
  }
  return new Map([...acc].map(([y, v]) => [y, v.value]));
}

/**
 * The consensus EPS for the NEXT fiscal year — the earliest estimate dated after `today`.
 *
 * ⚠ NOT THE FIRST ROW IN THE SERIES. The estimate block is stored from whenever it was fetched, so
 * its early periods can already be in the past; taking `[0]` would value the company on a year it
 * has since reported.
 */
export function nextFyEps(metrics: MetricRow[], today: string): { date: string; value: number } | null {
  let best: { date: string; value: number } | null = null;
  for (const m of metrics) {
    if (m.metric_code !== EPS_EST_CODE || m.numeric_value == null) continue;
    if (m.target_date <= today) continue;
    if (!best || m.target_date < best.date) best = { date: m.target_date, value: m.numeric_value };
  }
  return best;
}

/**
 * The compound growth the analyst EPS estimates imply, as a decimal.
 *
 * ⚠ THIS IS NOT GURUFOCUS'S `long_term_growth_rate_mean`, AND THE TWO ARE NOT INTERCHANGEABLE.
 * That field is a SCALAR, and the estimates parser only ingests list-valued fields (correctly — a
 * single number has no target_date to sit on), so it never reaches `metric_data` and cannot be read
 * here. This is the CAGR of the estimate series instead, which GuruFocus publishes separately as
 * `future_per_share_eps_estimate_growth`: near-identical for Apple (13.03 vs 13.01) and materially
 * different for a high-growth name (NVIDIA 45.72 vs 47.57). It is labelled for what it is, and it
 * is reference-only — nothing computes from it.
 */
export type EstimatePoint = { date: string; eps: number };
export type CagrWorking = {
  points: EstimatePoint[]; years: number | null; cagr: number | null;
};

/** The estimate series the CAGR is taken from, alongside the CAGR itself — so the drill-down shows
 *  the arithmetic rather than restating the conclusion. */
export function estimateCagrWorking(metrics: MetricRow[], today: string): CagrWorking {
  const points: EstimatePoint[] = metrics
    .filter((m) => m.metric_code === EPS_EST_CODE && m.numeric_value != null && m.target_date > today)
    .sort((a, b) => a.target_date.localeCompare(b.target_date))
    .map((m) => ({ date: m.target_date, eps: m.numeric_value as number }));
  if (points.length < 2) return { points, years: null, cagr: null };
  const first = points[0];
  const last = points[points.length - 1];
  const years = parseInt(last.date.slice(0, 4), 10) - parseInt(first.date.slice(0, 4), 10);
  if (!(first.eps > 0) || !(last.eps > 0) || years <= 0) {
    return { points, years: years > 0 ? years : null, cagr: null };   // no CAGR out of a loss
  }
  return { points, years, cagr: Math.pow(last.eps / first.eps, 1 / years) - 1 };
}

export function estimateCagr(metrics: MetricRow[], today: string): number | null {
  return estimateCagrWorking(metrics, today).cagr;
}

/**
 * The median P/E of the last five fiscal years, computed from the year-end price and that year's
 * normalised EPS.
 *
 * ⚠ DERIVED, BECAUSE `annuals__Valuation Ratios__PE Ratio` IS NOT IN THE INGESTED SET. A
 * loss-making year has no meaningful P/E and is skipped rather than contributing a negative — a
 * negative multiple would drag the median down and read as "historically cheap".
 */
export type PeYearRow = {
  year: number; price: number | null; eps: number | null; pe: number | null; used: boolean;
};
export type MedianPeWorking = { rows: PeYearRow[]; median: number | null };

/** Every year in the window with its two legs and the resulting multiple, plus the median of the
 *  usable ones. `used: false` marks a year shown but excluded — a loss year, whose negative
 *  multiple would drag the median down and read as "historically cheap". */
export function medianPEWorking(metrics: MetricRow[], years = 5): MedianPeWorking {
  const price = byYear(metrics, PRICE_PS_CODES);
  const eps = byYear(metrics, EPS_CODES);
  const rows: PeYearRow[] = [];
  for (const y of [...price.keys()].sort((a, b) => a - b).slice(-years)) {
    const p = price.get(y) ?? null;
    const e = eps.get(y) ?? null;
    const usable = p != null && e != null && e > 0 && p > 0;
    rows.push({ year: y, price: p, eps: e, pe: usable ? (p as number) / (e as number) : null, used: usable });
  }
  const pes = rows.filter((r) => r.used).map((r) => r.pe as number).sort((a, b) => a - b);
  if (!pes.length) return { rows, median: null };
  const m = Math.floor(pes.length / 2);
  return { rows, median: pes.length % 2 ? pes[m] : (pes[m - 1] + pes[m]) / 2 };
}

export function medianPE(metrics: MetricRow[], years = 5): number | null {
  return medianPEWorking(metrics, years).median;
}

export type YieldObs = { code: string; date: string; pct: number; chosen: boolean };

/** Every `Dividend Yield %` observation on offer, newest first, with the one the panel took.
 *  ⚠ The pick is by DATE alone across all four code spellings — the quarterly rows usually win
 *  because they are fresher, not because quarterly is preferred. */
export function dividendYieldWorking(metrics: MetricRow[]): { rows: YieldObs[]; chosen: YieldObs | null } {
  const want = new Set(DIV_YIELD_CODES);
  const rows: YieldObs[] = metrics
    .filter((m) => want.has(m.metric_code) && m.numeric_value != null)
    .map((m) => ({ code: m.metric_code, date: m.target_date, pct: m.numeric_value as number, chosen: false }))
    .sort((a, b) => b.date.localeCompare(a.date));
  if (!rows.length) return { rows, chosen: null };
  rows[0].chosen = true;
  return { rows, chosen: rows[0] };
}

export type ReverseDcfSource = {
  price: number | null;
  sharesOutstanding: number | null;
  fcf: number | null;
  /** The company's own cost of capital, as a DECIMAL — the discount rate's default. */
  wacc: number | null;
};

/**
 * The reverse-DCF's inputs, from the same payload.
 *
 * Three figures, nothing derived: the share price, the share count and the latest reported free
 * cash flow. ⚠ THE FCF IS AS FILED — no growth-capex add-back, no stock-comp deduction, no forward
 * estimate. A plain DCF compounds the cash flow the company reported, and every adjustment on top
 * of that is an opinion the reader did not ask for.
 */
/** One input as it was actually found: what the vendor filed (`raw`), what the model uses (`used`,
 *  sign-normalised where the two differ), and where it came from. */
export type SourceObs = {
  raw: number | null; used: number | null; date: string | null; code: string | null;
};
export type ReverseDcfWorking = {
  price: SourceObs; shares: SourceObs; fcf: SourceObs; wacc: SourceObs;
};

const NONE: SourceObs = { raw: null, used: null, date: null, code: null };

/** The latest observation across a set of codes, keeping the code it came from. */
function latestObs(metrics: MetricRow[], codes: string[], magnitude = false): SourceObs {
  const want = new Set(codes);
  let best: MetricRow | null = null;
  for (const m of metrics) {
    if (!want.has(m.metric_code) || m.numeric_value == null) continue;
    if (!best || m.target_date > best.target_date) best = m;
  }
  if (!best) return NONE;
  const raw = best.numeric_value as number;
  return { raw, used: magnitude ? Math.abs(raw) : raw, date: best.target_date, code: best.metric_code };
}

/** A `… %` observation as the DECIMAL the models want, keeping the vendor's figure in `raw`. */
function asDecimal(o: SourceObs): SourceObs {
  return o.used == null ? o : { ...o, used: o.used / 100 };
}

/** Every input the reverse DCF reads, with its provenance — the drill-down's whole content, and
 *  the source `reverseDcfSource` reduces to scalars, so the two cannot disagree. */
export function reverseDcfWorking(metrics: MetricRow[]): ReverseDcfWorking {
  return {
    price: latestObs(metrics, [PRICE_CODE]),
    shares: latestObs(metrics, SHARES_CODES),
    fcf: latestObs(metrics, FCF_CODES),
    // ⚠ FILED AS A PERCENT, like every other `… %` line — 8.2 means 8.2%. Passed through unscaled
    // it would be an 820% discount rate, and every company on earth would read as worthless.
    wacc: asDecimal(latestObs(metrics, WACC_CODES)),
  };
}

export function reverseDcfSource(metrics: MetricRow[]): ReverseDcfSource {
  const w = reverseDcfWorking(metrics);
  return {
    price: w.price.used, sharesOutstanding: w.shares.used, fcf: w.fcf.used, wacc: w.wacc.used,
  };
}

/** Everything the panel needs, from the one payload. `today` is passed in rather than read from the
 *  clock so the extraction stays pure and testable. */
export function egmSource(metrics: MetricRow[], today: string): EgmSource {
  const eps = nextFyEps(metrics, today);
  const dy = latest(metrics, DIV_YIELD_CODES);
  return {
    price: latest(metrics, [PRICE_CODE])?.value ?? null,
    forwardPE: latest(metrics, [FWD_PE_CODE])?.value ?? null,
    // ⚠ THE FIELD IS NAMED `… %` AND HOLDS PERCENT UNITS — GuruFocus files 0.3 for 0.3%, exactly
    // as it does for `ROE %`. The model wants a decimal, and passing the percent through unscaled
    // would apply a 0.3% payer as a 30% one: on a ten-year compounder that is a ~3.4x fair value.
    dividendYield: dy ? dy.value / 100 : null,
    epsNextFY: eps?.value ?? null,
    epsNextFYDate: eps?.date ?? null,
    analystGrowth5Y: estimateCagr(metrics, today),
    medianPE5Y: medianPE(metrics),
  };
}
