import type { ActiveShareHolding } from './ActiveSharePanel';

export function isDirectEquityStock(holding: ActiveShareHolding): boolean {
  return !holding.is_fund
    && (!holding.bucket || holding.bucket === 'Equity')
    && !/\b(etf|etp|etn|ucits|fund|fonds|certificate|certificaten|index|perp|bond|obligat|coupon|treasur|gilt|bund|govie|sovereign|floating\s*rate|frn|high\s*yield|fixed\s*income)\b|\d[.,]?\d*\s*%/i.test(holding.name ?? '');
}

/** The warmup and visible Risk panel must submit exactly the same membership and weights. */
export function riskBundleBody(holdings: ActiveShareHolding[], policy?: Record<string, number> | null, account?: string | null): string {
  return JSON.stringify({
    holdings: holdings.map((holding) => ({ ...holding, weight_pct: holding.weight_now_pct ?? holding.weight_pct })).filter(isDirectEquityStock),
    benchmark_weights: policy ? { ...policy, EquityOnly: 1 } : undefined,
    returns_account: account,
  });
}
