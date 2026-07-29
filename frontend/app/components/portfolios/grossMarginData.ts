/** Shared types + helpers for the Gross-margin card and its drill-down. The ratio is derived on
 *  the client from two raw lines so the plotted number and the drill-down cannot disagree.
 *  Mirrors {@link ./marginData} and {@link ./cashReturnData}. */

import { weightedByYear } from './marginData';

export type GrossMarginRow = {
  isin: string; name: string; weight_pct: number; currency: string | null;
  ticker: string | null; exchange: string | null;
  status: 'ok' | 'unsubscribed' | 'no_data';
  gross_profit: Record<string, number | null>;
  revenue: Record<string, number | null>;
};
export type GrossMarginInputs = { years: string[]; rows: GrossMarginRow[] };

/**
 * One company's gross margin for a year (as a %), or null when it cannot be computed.
 *
 * ⚠ A MISSING GROSS PROFIT IS NOT ZERO, AND FOR A BANK IT NEVER WILL BE. GuruFocus's 'B' industry
 * template has no cost of goods sold, so the line is simply absent (JPMorgan) — the concept does
 * not apply. Returning 0 there would draw a company selling at cost, which is a claim; returning
 * null leaves a hole, which is the truth.
 *
 * ⚠ A NEGATIVE GROSS PROFIT IS KEPT. Selling below cost is real (a bad year for a manufacturer, a
 * miner under water) and it is exactly the observation a margin chart exists to surface. Only the
 * denominator is gated: revenue must be positive, or the ratio is meaningless.
 */
export function grossMarginOf(
  grossProfit: number | null | undefined,
  revenue: number | null | undefined,
) {
  if (grossProfit == null || revenue == null) return null;
  if (!(revenue > 0)) return null;
  return grossProfit / revenue * 100;
}

/** The book's gross margin per year — a WEIGHT-weighted average of each company's margin (each is
 *  a currency-free ratio, so averaging is currency-safe; summing mixed-currency amounts is not).
 *  For a single company this is just that company's margin. */
export function grossMarginByYear(rows: GrossMarginRow[]): Map<number, number> {
  return weightedByYear(rows, (r) => Object.keys(r.revenue),
    (r, y) => grossMarginOf(r.gross_profit[y], r.revenue[y]));
}
