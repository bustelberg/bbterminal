/** Shared types + helpers for the LTD / (Total Assets − Goodwill) card and its drill-down. The
 *  ratio is derived on the client from three raw balance-sheet lines so the plotted number and the
 *  drill-down can't disagree. Mirrors {@link ./marginData}. */

import { weightedByYear } from './marginData';

export type DebtRatioRow = {
  isin: string; name: string; weight_pct: number; currency: string | null;
  ticker: string | null; exchange: string | null;
  status: 'ok' | 'unsubscribed' | 'no_data';
  long_term_debt: Record<string, number | null>;
  total_assets: Record<string, number | null>;
  goodwill: Record<string, number | null>;
};
export type DebtRatioInputs = { years: string[]; rows: DebtRatioRow[] };

/** One company's LTD / (Total Assets − Goodwill) for a year (as a %), or null when it can't be
 *  computed. Goodwill missing is treated as 0 (no acquisitions); Long-Term Debt and the
 *  goodwill-adjusted asset base must both be present and the base positive. */
export function debtRatioOf(
  ltd: number | null | undefined,
  totalAssets: number | null | undefined,
  goodwill: number | null | undefined,
) {
  if (ltd == null || totalAssets == null) return null;
  const base = totalAssets - (goodwill ?? 0);
  if (!(base > 0)) return null;
  return ltd / base * 100;
}

/** The book's ratio per year — a WEIGHT-weighted average of each company's ratio (each is a
 *  currency-free ratio, so averaging is currency-safe; summing mixed-currency amounts is not).
 *  For a single company this is just that company's ratio. */
export function debtRatioByYear(rows: DebtRatioRow[]): Map<number, number> {
  return weightedByYear(rows, (r) => Object.keys(r.total_assets),
    (r, y) => debtRatioOf(r.long_term_debt[y], r.total_assets[y], r.goodwill[y]));
}
