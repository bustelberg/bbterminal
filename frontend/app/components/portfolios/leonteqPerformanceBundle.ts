import type { PerformanceBundle } from './monthlyReport';

export type LeonteqPerformanceReturns = {
  portfolio_ytd_pct?: number | null; benchmark_ytd_pct?: number | null;
  ytd_excess_pct?: number | null; ytd_from?: string | null; portfolio_as_of?: string | null;
};

export type LeonteqPerformanceSource = {
  success?: boolean;
  monthly_benchmark?: PerformanceBundle['years'][string]['weighted_benchmark'];
  monthly_attribution?: Record<string, {
    opening_weight_date?: string;
    positions_through?: string | null;
    positions_return_pct?: number;
    positions?: NonNullable<PerformanceBundle['years'][string]['details'][string]['snapshot']>['positions'];
    reason?: string;
    estimated_contribution_gap_pp?: number | null;
  }>;
  performance?: { months?: { month: string; return_pct: number | null; as_of?: string }[]; yearly_totals?: Record<string, number> };
};

export function leonteqPerformanceBundle(name: string, source: LeonteqPerformanceSource,
  returns?: LeonteqPerformanceReturns): PerformanceBundle {
  const observations = (source.performance?.months ?? []).filter(row => row.return_pct != null);
  const currentMonth = returns?.portfolio_as_of?.slice(0, 7)
    ?? observations.map(row => row.month).sort().at(-1) ?? '';
  const years: PerformanceBundle['years'] = {};
  for (const row of observations) {
    const year = row.month.slice(0, 4);
    const report = years[year] ??= {
      months: [], rows: {}, weighted_benchmark: {}, details: {}, warning: null,
      summary: { return_pct: source.performance?.yearly_totals?.[year] ?? null,
        benchmark_pct: null, excess_pp: null, start: null, end: null },
    };
    report.months.push(row.month);
    const benchmark = source.monthly_benchmark?.[row.month];
    if (benchmark) report.weighted_benchmark[row.month] = benchmark;
    report.rows[row.month] = { rendement: row.return_pct,
      periode: row.as_of ?? (row.month === currentMonth ? returns?.portfolio_as_of ?? '' : '') };
    const attribution = source.monthly_attribution?.[row.month];
    const contributions = attribution?.positions
      ?.map(position => position.contribution_pp)
      .filter((value): value is number => value != null && Number.isFinite(value)) ?? [];
    const contributionSum = contributions.length ? contributions.reduce((sum, value) => sum + value, 0) : null;
    const estimated = attribution?.estimated_contribution_gap_pp != null;
    const positionsReturn = attribution?.positions_return_pct ?? row.return_pct;
    report.details[row.month] = { needs_refresh: false, benchmark_needs_refresh: false,
      position_error: attribution?.reason,
      positions_through: attribution?.positions_through,
      estimated_contribution_gap_pp: attribution?.estimated_contribution_gap_pp,
      snapshot: attribution?.positions ? { positions: attribution.positions,
        opening_weight_date: attribution.opening_weight_date ?? null,
        reconciliation: {
          status: contributionSum == null ? 'unavailable' : estimated ? 'not_reconciled' : 'reconciled',
          monthly_return_pct: positionsReturn,
          contribution_sum_pp: contributionSum,
          difference_pp: contributionSum == null || positionsReturn == null ? null : positionsReturn - contributionSum,
          issues: attribution.reason ? [attribution.reason] : [],
          adjustments: [],
        } } : null };
  }
  const currentYear = currentMonth.slice(0, 4);
  if (years[currentYear] && returns) {
    years[currentYear].summary = { return_pct: returns.portfolio_ytd_pct ?? null,
      benchmark_pct: returns.benchmark_ytd_pct ?? null, excess_pp: returns.ytd_excess_pct ?? null,
      start: returns.ytd_from ?? null, end: returns.portfolio_as_of ?? null };
  }
  for (const report of Object.values(years)) report.months.sort();
  return { portefeuille: name, current_month: currentMonth,
    available_years: Object.keys(years).map(Number).sort((a, b) => b - a), years };
}
