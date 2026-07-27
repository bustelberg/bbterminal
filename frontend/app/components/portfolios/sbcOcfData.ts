/** Shared types + helpers for the SBC/OCF card and its drill-down. The ratio is derived on the
 *  client from two raw lines so the plotted number and the drill-down can't disagree. Mirrors
 *  {@link ./debtRatioData}. */

export type SbcOcfRow = {
  isin: string; name: string; weight_pct: number; currency: string | null;
  ticker: string | null; exchange: string | null;
  status: 'ok' | 'unsubscribed' | 'no_data';
  sbc: Record<string, number | null>;
  ocf: Record<string, number | null>;
};
export type SbcOcfInputs = { years: string[]; rows: SbcOcfRow[] };

/** One company's SBC/OCF for a year (as a %), or null when it can't be computed: Stock-Based
 *  Compensation ÷ Operating Cash Flow. SBC is an add-back (its magnitude is used; a 0 is real).
 *  Operating cash flow must be present and positive — a negative OCF (a bank) makes the share
 *  meaningless. */
export function sbcOcfOf(sbc: number | null | undefined, ocf: number | null | undefined) {
  if (sbc == null || ocf == null) return null;
  if (!(ocf > 0)) return null;
  return Math.abs(sbc) / ocf * 100;
}

/** The book's SBC/OCF per year — a WEIGHT-weighted average of each company's ratio (each is a
 *  currency-free ratio, so averaging is currency-safe; summing mixed-currency amounts is not). For
 *  a single company this is just that company's ratio. */
export function sbcOcfByYear(rows: SbcOcfRow[]): Map<number, number> {
  const years = new Set<string>();
  for (const r of rows) for (const y of Object.keys(r.ocf)) years.add(y);
  const out = new Map<number, number>();
  for (const y of years) {
    let num = 0;
    let den = 0;
    for (const r of rows) {
      const v = sbcOcfOf(r.sbc[y], r.ocf[y]);
      if (v == null) continue;
      num += r.weight_pct * v;
      den += r.weight_pct;
    }
    if (den > 0) out.set(Number(y), num / den);
  }
  return out;
}
