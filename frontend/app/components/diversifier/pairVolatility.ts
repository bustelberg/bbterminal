/** Sample standard deviation of periodic returns, annualized to a fraction. */
export function annualizedMonthlyVolatility(returns: number[], periodsPerYear = 12): number | null {
  if (returns.length < 2 || returns.some((value) => !Number.isFinite(value))) return null;
  const mean = returns.reduce((sum, value) => sum + value, 0) / returns.length;
  const variance = returns.reduce((sum, value) => sum + (value - mean) ** 2, 0) / (returns.length - 1);
  return Math.sqrt(variance * periodsPerYear);
}

/** Match Diversifier's annualized_stats: (geometric annual return − RF) / vol. */
export function monthlySharpeRatio(returns: number[], riskFreeAnnual: number, periodsPerYear = 12): number | null {
  const volatility = annualizedMonthlyVolatility(returns, periodsPerYear);
  if (volatility == null || volatility < 1e-12 || !Number.isFinite(riskFreeAnnual)) return null;
  const annualReturn = compoundAnnualGrowth(returns, periodsPerYear);
  return annualReturn == null ? null : (annualReturn - riskFreeAnnual) / volatility;
}

/** Compound growth annualized by observation count, using the entire return chain. */
export function compoundAnnualGrowth(returns: number[], periodsPerYear = 252): number | null {
  if (returns.length < 2 || returns.some((value) => !Number.isFinite(value) || value < -1)) return null;
  if (!Number.isFinite(periodsPerYear) || periodsPerYear <= 0) return null;
  const growth = returns.reduce((factor, value) => factor * (1 + value), 1);
  const annualReturn = growth > 0 ? growth ** (periodsPerYear / returns.length) - 1 : -1;
  return Number.isFinite(annualReturn) ? annualReturn : null;
}

/** Largest peak-to-trough decline, including the initial invested value. */
export function monthlyMaxDrawdown(returns: number[]): number | null {
  if (returns.length === 0 || returns.some((value) => !Number.isFinite(value) || value < -1)) return null;
  let equity = 1;
  let peak = 1;
  let drawdown = 0;
  for (const value of returns) {
    equity *= 1 + value;
    peak = Math.max(peak, equity);
    drawdown = Math.min(drawdown, equity / peak - 1);
  }
  return drawdown;
}

export type DailyDrawdown = {
  depth: number;
  peakDate: string | null;
  troughDate: string;
  recoveryDate: string | null;
  remainingDrawdown: number | null;
};

/** Distinct episodes: one peak through recovery, or through the last observation. */
export function topDailyDrawdowns(points: { date: string; return: number }[], limit = 5): DailyDrawdown[] {
  if (points.length < 2 || points.some((point) => !Number.isFinite(point.return) || point.return < -1)) return [];
  let equity = 1;
  let peak = 1;
  let peakDate: string | null = null;
  let episode: DailyDrawdown | null = null;
  const episodes: DailyDrawdown[] = [];
  for (const point of points) {
    equity *= 1 + point.return;
    if (equity >= peak * (1 - 1e-12)) {
      if (episode) episodes.push({ ...episode, recoveryDate: point.date });
      episode = null;
      peak = Math.max(peak, equity);
      peakDate = point.date;
    } else {
      const depth = equity / peak - 1;
      if (!episode || depth < episode.depth) {
        episode = { depth, peakDate, troughDate: point.date, recoveryDate: null, remainingDrawdown: null };
      }
    }
  }
  if (episode) episodes.push({ ...episode, remainingDrawdown: equity / peak - 1 });
  return episodes.sort((a, b) => a.depth - b.depth).slice(0, Math.max(0, limit));
}
