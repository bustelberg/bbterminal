/** Shared types + helpers for the FCF-SBC yield card and its drill-down. The ratio is derived on
 *  the client from three raw lines so the plotted number and the drill-down can't disagree. Mirrors
 *  {@link ./marginData}. */

import { weightedByYear } from './marginData';
import { correctedFcf } from './sbcCorrection';

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
  correct = true,
) {
  if (marketCap == null || !(marketCap > 0)) return null;
  const num = correctedFcf(fcf, sbc, correct);
  return num == null ? null : num / marketCap * 100;
}

/** The book's FCF-SBC yield per year — a WEIGHT-weighted average of each company's yield (each is a
 *  currency-free ratio, so averaging is currency-safe; summing mixed-currency amounts is not). For
 *  a single company this is just that company's yield. */
export function fcfSbcYieldByYear(rows: FcfSbcYieldRow[], correct = true): Map<number, number> {
  return weightedByYear(rows, (r) => Object.keys(r.market_cap),
    (r, y) => fcfSbcYieldOf(r.fcf[y], r.sbc[y], r.market_cap[y], correct));
}
