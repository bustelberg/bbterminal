/** Shared types + helpers for the interest-burden card and its drill-down. The ratio is derived on
 *  the client from two raw lines so the plotted number and the drill-down can't disagree. Mirrors
 *  {@link ./debtRatioData}. */

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
  const years = new Set<string>();
  for (const r of rows) for (const y of Object.keys(r.operating_income)) years.add(y);
  const out = new Map<number, number>();
  for (const y of years) {
    let num = 0;
    let den = 0;
    for (const r of rows) {
      const v = interestBurdenOf(r.interest_expense[y], r.operating_income[y]);
      if (v == null) continue;
      num += r.weight_pct * v;
      den += r.weight_pct;
    }
    if (den > 0) out.set(Number(y), num / den);
  }
  return out;
}
