/** Shared types + helpers for the Cash-return-on-capital card and its drill-down. The ratio is
 *  derived on the client from three raw lines so the plotted number and the drill-down can't
 *  disagree. Mirrors {@link ./debtRatioData}. */

import { weightedByYear } from './marginData';

export type CashReturnRow = {
  isin: string; name: string; weight_pct: number; currency: string | null;
  ticker: string | null; exchange: string | null;
  status: 'ok' | 'unsubscribed' | 'no_data';
  fcf: Record<string, number | null>;
  noncurrent_liabilities: Record<string, number | null>;
  total_equity: Record<string, number | null>;   // Total Equity (incl. minority interest)
};
export type CashReturnInputs = { years: string[]; rows: CashReturnRow[] };

/** One company's cash return on capital for a year (as a %), or null when it can't be computed:
 *  FCF ÷ invested capital, where invested capital = non-current liabilities + total equity. Both
 *  legs of the capital base must be present (a missing non-current-liabilities line means the
 *  issuer doesn't split current/non-current — Berkshire, banks — so the base is undefined, NOT
 *  equity alone), and the base must be positive. */
export function cashReturnOf(
  fcf: number | null | undefined,
  noncurrentLiabilities: number | null | undefined,
  totalEquity: number | null | undefined,
) {
  if (fcf == null || noncurrentLiabilities == null || totalEquity == null) return null;
  const base = noncurrentLiabilities + totalEquity;
  if (!(base > 0)) return null;
  return fcf / base * 100;
}

/** The book's cash return on capital per year — a WEIGHT-weighted average of each company's ratio
 *  (each is a currency-free ratio, so averaging is currency-safe; summing mixed-currency amounts is
 *  not). For a single company this is just that company's ratio. */
export function cashReturnByYear(rows: CashReturnRow[]): Map<number, number> {
  return weightedByYear(rows, (r) => Object.keys(r.total_equity),
    (r, y) => cashReturnOf(r.fcf[y], r.noncurrent_liabilities[y], r.total_equity[y]));
}
