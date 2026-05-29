'use client';

import { Fragment, useMemo, useState } from 'react';
import type { Column } from '../../../../lib/tableExport';
import CellInfoTip from '../CellInfoTip';
import CollapsibleCard from '../CollapsibleCard';
import TableDownloadButton from '../../TableDownloadButton';
import { fmtPct } from '../utils';
import { parenPct } from '../feeStats';
import type { AlignedResult, CustomRangeReturn, YearlyBreakdown as YB } from './seriesMath';

type YearlyExportRow = { year: string; series: string; return_pct: number | null };

const MONTH_NAMES = [
  'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
  'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec',
];

/** Per-month chained return for every series within a single calendar
 * year. Baseline for each month is the previous month's end-cum
 * (or end-of-previous-year for January, or 0 if neither exists — same
 * rebase semantics as `computeYearlyBreakdown`).
 *
 * Returns:
 *   months: ['YYYY-MM', ...] sorted, union across series
 *   bySeries: series_id → month → return_pct (null when undefined)
 */
function monthlyReturnsForYear(
  aligned: AlignedResult,
  year: string,
): { months: string[]; bySeries: Record<string, Record<string, number | null>> } {
  const monthsSet = new Set<string>();
  const bySeries: Record<string, Record<string, number | null>> = {};

  for (const s of aligned.series) {
    let prevYearEndCum: number | null = null;
    const byMonth = new Map<string, number>();
    for (const p of s.points) {
      if (p.cumReturnPct == null) continue;
      const py = p.date.slice(0, 4);
      if (py < year) {
        prevYearEndCum = p.cumReturnPct;
      } else if (py === year) {
        byMonth.set(p.date.slice(0, 7), p.cumReturnPct);
      } else {
        break;
      }
    }

    const months = Array.from(byMonth.keys()).sort();
    const rowMap: Record<string, number | null> = {};
    let prevCum: number | null = prevYearEndCum;
    for (const m of months) {
      const lastCum = byMonth.get(m)!;
      const baseline = prevCum ?? 0;
      rowMap[m] = ((1 + lastCum / 100) / (1 + baseline / 100) - 1) * 100;
      prevCum = lastCum;
      monthsSet.add(m);
    }
    bySeries[s.id] = rowMap;
  }

  const months = Array.from(monthsSet).sort();
  for (const s of aligned.series) {
    for (const m of months) if (!(m in bySeries[s.id])) bySeries[s.id][m] = null;
  }
  return { months, bySeries };
}

/** Yearly performance grid + custom-range cumulative return widget.
 * One row per calendar year, one column per aligned series. Each cell
 * shows gross + an optional (net) parenthetical for holdings-bearing
 * series when fees are configured. */
export default function YearlyBreakdown({
  yearlyBreakdown,
  alignedSeries,
  customRangeReturn,
  customFromMonth,
  setCustomFromMonth,
  yearlyExportRows,
  yearlyExportColumns,
}: {
  yearlyBreakdown: YB;
  alignedSeries: AlignedResult;
  customRangeReturn: CustomRangeReturn | null;
  customFromMonth: string;
  setCustomFromMonth: (v: string) => void;
  yearlyExportRows: YearlyExportRow[];
  yearlyExportColumns: Column<YearlyExportRow>[];
}) {
  const [expandedYear, setExpandedYear] = useState<string | null>(null);
  const monthlyForExpanded = useMemo(
    () => (expandedYear ? monthlyReturnsForYear(alignedSeries, expandedYear) : null),
    [expandedYear, alignedSeries],
  );

  if (yearlyBreakdown.years.length === 0) return null;

  return (
    <CollapsibleCard
      title="Yearly Performance"
      rightSlot={
        <TableDownloadButton
          rows={yearlyExportRows}
          columns={yearlyExportColumns}
          filename="backtest_yearly_performance"
          title={`Download ${yearlyExportRows.length} years × ${alignedSeries.series.length} series as CSV / XLSX`}
        />
      }
    >
      <div className="overflow-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-gray-800/40 text-gray-500 text-xs">
              <th className="px-5 py-2.5 text-left font-medium">
                Year<CellInfoTip>Calendar year. Each cell shows the series&apos; cumulative return across that year (Jan 1 → Dec 31, or partial for the latest year).</CellInfoTip>
              </th>
              {alignedSeries.series.map((s) => (
                <th key={s.id} className="px-3 py-2.5 text-right font-medium">
                  <span className="inline-flex items-center gap-1.5">
                    <span className="inline-block w-1.5 h-1.5 rounded-full" style={{ background: s.color }} />
                    <span className="truncate max-w-[140px]">{s.label}</span>
                  </span>
                  <CellInfoTip>Annual return for this series. Strategy returns chain-link the monthly portfolio returns; benchmark returns chain-link daily closes.</CellInfoTip>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {yearlyBreakdown.years.map((y) => {
              const isExpanded = expandedYear === y;
              return (
                <Fragment key={y}>
                  <tr
                    className="border-b border-gray-800/20 hover:bg-white/[0.02] cursor-pointer"
                    onClick={() => setExpandedYear(isExpanded ? null : y)}
                  >
                    <td className="px-5 py-2 text-gray-200 font-mono">
                      <span className="inline-flex items-center gap-1.5">
                        <span className="text-gray-500 w-3 text-xs">{isExpanded ? '▾' : '▸'}</span>
                        {y}
                      </span>
                    </td>
                    {alignedSeries.series.map((s) => {
                      const v = yearlyBreakdown.bySeries[s.id]?.[y];
                      // Active strategy + saved comparisons both get the
                      // (net) parenthetical when fees are configured; benchmarks
                      // stay gross-only. Uses `netYearlyBySeries` (gross × per-year
                      // fee-factor drag) rather than NetStats.yearly so the
                      // parenthetical can never exceed displayed gross — the
                      // period-start-bucketed `yearly` could drift above gross for
                      // rebalances that don't align to Jan 1.
                      const netY = s.kind !== 'benchmark'
                        ? yearlyBreakdown.netYearlyBySeries?.[s.id]?.[y]
                        : undefined;
                      return (
                        <td
                          key={s.id}
                          className={`px-3 py-2 text-right font-mono ${v != null ? (v >= 0 ? 'text-emerald-400' : 'text-rose-400') : 'text-gray-600'}`}
                        >
                          {v != null ? fmtPct(v) : '—'}
                          {netY != null && <span className="text-gray-500">{parenPct(netY)}</span>}
                        </td>
                      );
                    })}
                  </tr>
                  {isExpanded && monthlyForExpanded?.months.map((m) => (
                    <tr key={`${y}-${m}`} className="border-b border-gray-800/10 bg-white/[0.015]">
                      <td className="pl-12 pr-5 py-1.5 text-gray-500 font-mono text-xs">
                        {MONTH_NAMES[parseInt(m.slice(5), 10) - 1]}
                      </td>
                      {alignedSeries.series.map((s) => {
                        const v = monthlyForExpanded.bySeries[s.id]?.[m];
                        return (
                          <td
                            key={s.id}
                            className={`px-3 py-1.5 text-right font-mono text-xs ${v != null ? (v >= 0 ? 'text-emerald-400' : 'text-rose-400') : 'text-gray-600'}`}
                          >
                            {v != null ? fmtPct(v) : '—'}
                          </td>
                        );
                      })}
                    </tr>
                  ))}
                </Fragment>
              );
            })}
          </tbody>
        </table>
      </div>
      <div className="px-5 py-3 border-t border-gray-800/40 flex items-center gap-4 flex-wrap">
        <label className="text-xs text-gray-400 font-medium">From month:</label>
        <input
          type="month"
          value={customFromMonth}
          min={alignedSeries.windowStart ?? undefined}
          max={alignedSeries.windowEnd ?? undefined}
          onChange={(e) => setCustomFromMonth(e.target.value)}
          className="bg-[#0f1117] border border-gray-700 rounded-lg px-2 py-1 text-xs text-gray-200 focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
        />
        {customFromMonth && (
          <button
            onClick={() => setCustomFromMonth('')}
            className="text-xs text-gray-500 hover:text-gray-300 transition-colors"
          >
            clear
          </button>
        )}
        {customRangeReturn ? (
          <div className="flex items-center gap-4 text-xs ml-auto flex-wrap">
            <span className="text-gray-500 font-mono">{customRangeReturn.fromDate} → {customRangeReturn.toDate}</span>
            {customRangeReturn.perSeries.map((s) => (
              <span key={s.id} className="text-gray-400 inline-flex items-center gap-1.5">
                <span className="inline-block w-1.5 h-1.5 rounded-full" style={{ background: s.color }} />
                {s.label}:{' '}
                {s.ret != null ? (
                  <span className={`font-mono ${s.ret >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                    {fmtPct(s.ret)}
                    {s.netRet != null && <span className="text-gray-500">{parenPct(s.netRet)}</span>}
                  </span>
                ) : (
                  <span className="font-mono text-gray-600">—</span>
                )}
              </span>
            ))}
          </div>
        ) : (
          <span className="text-xs text-gray-500">Cumulative return from picked month through end of aligned window.</span>
        )}
      </div>
    </CollapsibleCard>
  );
}
