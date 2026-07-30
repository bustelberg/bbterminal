/** Shared types + helpers for the Cash-conversion card and its drill-down. The ratio is derived on
 *  the client from two raw lines so the plotted number and the drill-down cannot disagree.
 *  Mirrors {@link ./grossMarginData}. */

import { weightedByYear } from './marginData';
import { correctedFcf } from './sbcCorrection';

export type CashConversionRow = {
  isin: string; name: string; weight_pct: number; currency: string | null;
  ticker: string | null; exchange: string | null;
  status: 'ok' | 'unsubscribed' | 'no_data';
  fcf: Record<string, number | null>;
  /** Carried for the tab-level SBC correction; see `sbcCorrection`. */
  sbc: Record<string, number | null>;
  net_income: Record<string, number | null>;
};
export type CashConversionInputs = { years: string[]; rows: CashConversionRow[] };

/**
 * One company's cash conversion for a year (as a %): free cash flow per unit of reported profit.
 * The question it answers is whether the earnings are real — profit you cannot bank is an opinion
 * about revenue recognition.
 *
 * ⚠ ABOVE 100% IS NORMAL AND GOOD. Depreciation running ahead of capex converts more cash than the
 * accounts book as profit (ASML 2025: 11,027.3 / 9,609.4 = 114.8%). It is not an error to clamp.
 *
 * ⚠ A NEGATIVE FCF IS KEPT — earnings with no cash behind them is precisely what this exists to
 * catch, and it belongs on the chart below zero.
 *
 * ⚠ BUT A NON-POSITIVE DENOMINATOR RETURNS NULL. A loss-making company with POSITIVE free cash
 * flow would otherwise print a negative conversion — reading as "burning cash" when the opposite
 * is happening — and two companies could show the same −80% for opposite reasons. The ratio simply
 * does not apply to a loss, so the year is a hole.
 */
export function cashConversionOf(
  fcf: number | null | undefined,
  netIncome: number | null | undefined,
  sbc: number | null | undefined = null,
  correct = false,
) {
  if (netIncome == null || !(netIncome > 0)) return null;
  const num = correctedFcf(fcf, sbc, correct);
  return num == null ? null : num / netIncome * 100;
}

/** The book's cash conversion per year — a WEIGHT-weighted average of each company's ratio (each is
 *  a currency-free ratio, so averaging is currency-safe; summing mixed-currency amounts is not).
 *  For a single company this is just that company's ratio. */
export function cashConversionByYear(
  rows: CashConversionRow[], correct = false,
): Map<number, number> {
  return weightedByYear(rows, (r) => Object.keys(r.net_income),
    (r, y) => cashConversionOf(r.fcf[y], r.net_income[y], r.sbc?.[y], correct));
}
