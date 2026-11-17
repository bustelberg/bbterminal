import type { PerformanceBundle } from './monthlyReport';

export type LeonteqPerformanceReturns = {
  portfolio_ytd_pct?: number | null; benchmark_ytd_pct?: number | null;
  ytd_excess_pct?: number | null; ytd_from?: string | null; portfolio_as_of?: string | null;
};

export type LeonteqPerformanceSource = {
  success?: boolean;
  performance?: { months?: { month: string; return_pct: number | null }[]; yearly_totals?: Record<string, number> };
};

/** Preserve the shared report UI without manufacturing monthly position results. */
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
    report.rows[row.month] = { rendement: row.return_pct,
      periode: row.month === currentMonth ? returns?.portfolio_as_of ?? '' : '' };
    report.details[row.month] = { needs_refresh: false, benchmark_needs_refresh: false, snapshot: null };
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
