/** Shared types + helpers for the Capex-margin card and its drill-down. The ratio is derived on the
 *  client from two raw lines so the plotted number and the drill-down can't disagree. Mirrors
 *  {@link ./sbcOcfData}. */

import { weightedByYear } from './marginData';

export type CapexMarginRow = {
  isin: string; name: string; weight_pct: number; currency: string | null;
  ticker: string | null; exchange: string | null;
  status: 'ok' | 'unsubscribed' | 'no_data';
  capex: Record<string, number | null>;
  revenue: Record<string, number | null>;
};
export type CapexMarginInputs = { years: string[]; rows: CapexMarginRow[] };

/** One company's capex margin for a year (as a %), or null when it can't be computed: |Capex| ÷
 *  Revenue (capital intensity). Capex is reported negative, so its magnitude is used (a 0 is a real
 *  "capital-light"). Revenue must be present and positive. */
export function capexMarginOf(capex: number | null | undefined, revenue: number | null | undefined) {
  if (capex == null || revenue == null) return null;
  if (!(revenue > 0)) return null;
  return Math.abs(capex) / revenue * 100;
}

/** The book's capex margin per year — a WEIGHT-weighted average of each company's ratio (each is a
 *  currency-free ratio, so averaging is currency-safe; summing mixed-currency amounts is not). For
 *  a single company this is just that company's ratio. */
export function capexMarginByYear(rows: CapexMarginRow[]): Map<number, number> {
  return weightedByYear(rows, (r) => Object.keys(r.revenue),
    (r, y) => capexMarginOf(r.capex[y], r.revenue[y]));
}
