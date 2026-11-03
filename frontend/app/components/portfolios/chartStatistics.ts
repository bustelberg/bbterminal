import type { TrendFit } from '../../../lib/trendFit';
import type { Cagr } from './lineCagr';

export type ChartStatistics = {
  fit: Omit<TrendFit, 'trend'>; cagr: Cagr; fromX: number | null; toX: number | null;
};

/** Project the saved regression for drawing; R² and CAGR are supplied by the same response. */
export function savedFit(stats: ChartStatistics, points: { year: number; value: number }[]): TrendFit {
  const { slope, intercept } = stats.fit;
  return { ...stats.fit, trend: slope == null || intercept == null ? []
    : points.filter(p => p.value > 0 && Number.isFinite(p.value))
      .map(p => ({ year: p.year, value: Math.exp(intercept + slope * p.year) })) };
}
