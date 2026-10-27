import type { ActiveShareRow } from '../../../lib/types/api';

/** Project normalised issuer weights for the table without changing headline inputs. */
export function activeShareTableRows(rows: ActiveShareRow[], stockAllocation: number, benchmarkAllocation: number): ActiveShareRow[] {
  return rows.map(row => {
    const portfolio = (row.portfolio_pct ?? 0) * stockAllocation / 100;
    const benchmark = (row.benchmark_pct ?? 0) * benchmarkAllocation / 100;
    return { ...row, portfolio_pct: portfolio, benchmark_pct: benchmark, active_pct: portfolio - benchmark };
  }).sort((left, right) => Math.abs(right.active_pct ?? 0) - Math.abs(left.active_pct ?? 0));
}
