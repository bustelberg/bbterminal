/** Shared types + helpers for the interest-burden card and its drill-down. The ratio is derived on
 *  the client from two raw lines so the plotted number and the drill-down can't disagree. Mirrors
 *  {@link ./debtRatioData}. */

import { weightedByYear } from './marginData';

export type InterestBurdenRow = {
  isin: string; name: string; weight_pct: number; currency: string | null;
  ticker: string | null; exchange: string | null;
  status: 'ok' | 'unsubscribed' | 'no_data';
  interest_expense: Record<string, number | null>;
  operating_income: Record<string, number | null>;
};
export type InterestBurdenInputs = { years: string[]; rows: InterestBurdenRow[] };

/** One company's interest burden for a year (as a %), or null when it can't be computed: the share
 *  of operating profit spent on interest = |Interest expense| ÷ Operating income. Interest expense
 *  is reported negative, so its magnitude is used (a 0 is a real "nets to nothing"). Operating
 *  income must be present and positive — a loss makes "% of profit" meaningless. */
export function interestBurdenOf(
  interestExpense: number | null | undefined,
  operatingIncome: number | null | undefined,
) {
  if (interestExpense == null || operatingIncome == null) return null;
  if (!(operatingIncome > 0)) return null;
  return Math.abs(interestExpense) / operatingIncome * 100;
}

/** The book's interest burden per year — a WEIGHT-weighted average of each company's ratio (each is
 *  a currency-free ratio, so averaging is currency-safe; summing mixed-currency amounts is not).
 *  For a single company this is just that company's ratio. */
export function interestBurdenByYear(rows: InterestBurdenRow[]): Map<number, number> {
  return weightedByYear(rows, (r) => Object.keys(r.operating_income),
    (r, y) => interestBurdenOf(r.interest_expense[y], r.operating_income[y]));
}

/**
 * Interest COVERAGE from an interest BURDEN — `100 ÷ burden%`, or null when there is none to state.
 *
 * ⚠⚠ COVERAGE IS A VIEW OF THE BURDEN, NOT A SERIES OF ITS OWN, AND THAT IS THE WHOLE POINT. The
 * burden (interest as a share of operating profit) is the ADDITIVE quantity here — exactly as an
 * earnings yield is where a P/E is not — so every average, across holdings OR across years, has to
 * be taken on the burden and only then inverted. Averaging coverages instead is the same mistake
 * `_fundamental_blend` refuses when it combines a multiple harmonically.
 *
 * ⚠⚠ AND DOING IT IN ONE DIMENSION BUT NOT THE OTHER IS WHAT SHIPPED FIRST (2026-08-21). The
 * cross-section averaged burdens correctly and the WINDOW then averaged the resulting coverages,
 * which broke twice over on ASML:
 *
 *     10y   mean-of-coverages   84.2× over 9 of 10 years   ← 2016 dropped: interest was exactly 0
 *           1 ÷ mean-burden     84.8× over 10 of 10
 *      5y   mean-of-coverages   87.4×
 *           1 ÷ mean-burden     79.3×                     ← 8 turns, from one high-coverage year
 *
 * A debt-free year has a burden of ZERO — a real, excellent, perfectly averageable number — and
 * only becomes an unusable ∞ once you invert it too early. Inverting last keeps the year in, which
 * is why the `(9/10)` badge that prompted this is simply gone rather than explained.
 *
 * Null only when the average burden is itself zero: nothing in the window paid any interest at all,
 * so there is no coverage to state and a dash is the answer.
 */
export function coverageFromBurden(burdenPct: number | null | undefined): number | null {
  return burdenPct != null && burdenPct > 0 ? 100 / burdenPct : null;
}
