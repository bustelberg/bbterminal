/** Shared types + helpers for the FCF-SBC yield card and its drill-down. The ratio is derived on
 *  the client from three raw lines so the plotted number and the drill-down can't disagree. Mirrors
 *  {@link ./marginData}. */

export type FcfSbcYieldRow = {
  isin: string; name: string; weight_pct: number; currency: string | null;
  ticker: string | null; exchange: string | null;
  status: 'ok' | 'unsubscribed' | 'no_data';
  fcf: Record<string, number | null>;
  sbc: Record<string, number | null>;
  market_cap: Record<string, number | null>;
};
export type FcfSbcYieldInputs = { years: string[]; rows: FcfSbcYieldRow[] };

/** One company's FCF-SBC yield for a year (as a %), or null when it can't be computed: (FCF − SBC)
 *  ÷ Market Cap. SBC missing is treated as 0 (many report none); FCF may be negative (yield goes
 *  negative); market cap must be present and positive. */
export function fcfSbcYieldOf(
  fcf: number | null | undefined,
  sbc: number | null | undefined,
  marketCap: number | null | undefined,
) {
  if (fcf == null || marketCap == null || !(marketCap > 0)) return null;
  return (fcf - (sbc ?? 0)) / marketCap * 100;
}

/** The book's FCF-SBC yield per year — a WEIGHT-weighted average of each company's yield (each is a
 *  currency-free ratio, so averaging is currency-safe; summing mixed-currency amounts is not). For
 *  a single company this is just that company's yield. */
export function fcfSbcYieldByYear(rows: FcfSbcYieldRow[]): Map<number, number> {
  const years = new Set<string>();
  for (const r of rows) for (const y of Object.keys(r.market_cap)) years.add(y);
  const out = new Map<number, number>();
  for (const y of years) {
    let num = 0;
    let den = 0;
    for (const r of rows) {
      const v = fcfSbcYieldOf(r.fcf[y], r.sbc[y], r.market_cap[y]);
      if (v == null) continue;
      num += r.weight_pct * v;
      den += r.weight_pct;
    }
    if (den > 0) out.set(Number(y), num / den);
  }
  return out;
}
