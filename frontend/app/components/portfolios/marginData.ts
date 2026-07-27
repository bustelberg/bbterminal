/** Shared types + helpers for the FCF-SBC margin card and its drill-down. The margin is derived on
 *  the client from three raw lines so the numbers and the drill-down can't disagree. */

export type MarginRow = {
  isin: string; name: string; weight_pct: number; currency: string | null;
  ticker: string | null; exchange: string | null;
  status: 'ok' | 'unsubscribed' | 'no_data';
  revenue: Record<string, number | null>;
  fcf: Record<string, number | null>;
  sbc: Record<string, number | null>;
};
export type MarginInputs = { years: string[]; rows: MarginRow[] };

/** Amounts are millions of the reporting currency — compact B/T/M. */
export const fmtRevM = (v: number | null | undefined) => {
  if (v == null) return '—';
  const a = Math.abs(v);
  if (a >= 1e6) return `${(v / 1e6).toFixed(2)}T`;
  if (a >= 1e3) return `${(v / 1e3).toFixed(1)}B`;
  return `${v.toFixed(0)}M`;
};

/** One company's FCF-SBC margin for a year, or null when it can't be computed. SBC missing is
 *  treated as 0 (many companies report none); revenue must be positive. */
export function marginOf(rev: number | null | undefined, fcf: number | null | undefined, sbc: number | null | undefined) {
  if (rev == null || rev <= 0 || fcf == null) return null;
  return (fcf - (sbc ?? 0)) / rev * 100;
}

/** The book's FCF-SBC margin per year — a WEIGHT-weighted average of each company's margin (each is
 *  a currency-free ratio, so averaging is currency-safe; summing mixed-currency euros/£/$ is not).
 *  For a single company this is just that company's margin. */
export function marginByYear(rows: MarginRow[]): Map<number, number> {
  const years = new Set<string>();
  for (const r of rows) for (const y of Object.keys(r.revenue)) years.add(y);
  const out = new Map<number, number>();
  for (const y of years) {
    let num = 0;
    let den = 0;
    for (const r of rows) {
      const m = marginOf(r.revenue[y], r.fcf[y], r.sbc[y]);
      if (m == null) continue;
      num += r.weight_pct * m;
      den += r.weight_pct;
    }
    if (den > 0) out.set(Number(y), num / den);
  }
  return out;
}

export const meanOf = (xs: number[]) => (xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : null);
