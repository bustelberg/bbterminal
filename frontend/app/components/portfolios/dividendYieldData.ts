/** Shared types + helpers for the dividend-yield card and its drill-down. The ratio is derived on
 *  the client from two raw lines so the plotted number and the drill-down can't disagree. Mirrors
 *  {@link ./fcfSbcYieldData}.
 *
 *  ⚠ THE YIELD, NOT DIVIDENDS PER SHARE — the per-share amount has no portfolio-level meaning.
 *  There is no portfolio share; the amounts are in different currencies; and a level series that
 *  legitimately starts at 0.00 cannot be rebased to a growth index, which is what left the
 *  portfolio's dividend card permanently empty while every holding carried the line. `DPS / price`
 *  is currency-free, so the weight-weighted average IS the book's yield — portfolio yield =
 *  Σ value·yield ÷ Σ value, and these weights ARE value weights, so the arithmetic mean is the
 *  aggregate here rather than an approximation of it.
 */

import { coverageByYear as sharedCoverageByYear, weightedByYear } from './marginData';

export type DividendYieldRow = {
  isin: string; name: string; weight_pct: number; currency: string | null;
  ticker: string | null; exchange: string | null;
  status: 'ok' | 'unsubscribed' | 'no_data';
  div_ps: Record<string, number | null>;
  price_ps: Record<string, number | null>;
};
export type DividendYieldInputs = { years: string[]; rows: DividendYieldRow[] };

/**
 * One company's dividend yield for a year (as a %), or null when it can't be computed: Dividends
 * per Share ÷ the fiscal year-end share price.
 *
 * ⚠ AN ABSENT DIVIDEND LINE IS NOT A ZERO. GuruFocus files an explicit `0.00` for a company that
 * pays nothing — a real answer that belongs in the average and drags the book's yield down
 * honestly. A MISSING line means we never ingested one, and reading that as zero would let
 * un-ingested holdings quietly deflate the portfolio's yield with a number nobody reported. The
 * price is the denominator and must be present and positive.
 */
export function dividendYieldOf(dps: number | null | undefined, price: number | null | undefined) {
  if (dps == null || price == null) return null;
  if (!(price > 0)) return null;
  return dps / price * 100;
}

const YEARS = (r: DividendYieldRow) => Object.keys(r.price_ps);
const YIELD = (r: DividendYieldRow, y: string) => dividendYieldOf(r.div_ps[y], r.price_ps[y]);

/** The book's dividend yield per year — the weight-weighted average of each holding's yield,
 *  renormalised over the holdings that HAVE both lines that year (so a year's figure is never
 *  dragged toward zero by holdings that simply didn't report) and subject to the shared
 *  `MIN_YEAR_COVERAGE_PCT` floor. For a single company this is just that company's yield. */
export function dividendYieldByYear(rows: DividendYieldRow[]): Map<number, number> {
  return weightedByYear(rows, YEARS, YIELD);
}

/** The share of the charted holdings a year's yield is computed over. A yield averaged over 40% of
 *  the weight is not the book's yield — below the floor no point is drawn at all, and above it the
 *  card still states the share. */
export function coverageByYear(rows: DividendYieldRow[]): Map<number, number> {
  return sharedCoverageByYear(rows, YEARS, YIELD);
}
