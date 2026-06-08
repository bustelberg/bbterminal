'use client';

import { useMemo } from 'react';
import type { CSSProperties } from 'react';
import CollapsibleCard from './CollapsibleCard';
import type { BacktestResult } from '../../../lib/stores/momentum';

/**
 * Classic year × month returns heatmap. Calendar-month % returns are
 * resampled from the strategy's daily equity curve (last cumulative value in
 * each month, chained month-over-month — the same construction the backend
 * uses for win-rate), then laid out as a grid coloured green (up) / red
 * (down) with intensity scaling to magnitude. A trailing "Year" column
 * compounds each year's months.
 */
const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

function tint(v: number | undefined, scale: number): CSSProperties {
  const base: CSSProperties = { border: '1px solid var(--color-card)' };
  if (v == null) return base;
  const a = Math.min(1, Math.abs(v) / scale);
  const pct = Math.round(12 + a * 58); // 12% → 70% saturation
  const c = v >= 0 ? 'var(--color-pos-500)' : 'var(--color-neg-500)';
  return { ...base, background: `color-mix(in srgb, ${c} ${pct}%, transparent)`, color: 'var(--color-fg-strong)' };
}

const fmt = (v: number) => `${v >= 0 ? '+' : ''}${v.toFixed(1)}`;

export default function MonthlyReturnsHeatmap({
  result,
  defaultCollapsed = false,
}: {
  result: BacktestResult;
  defaultCollapsed?: boolean;
}) {
  const { years, byKey, yearTotals, maxAbs, maxAbsYear } = useMemo(() => {
    const recs = result.daily_records ?? [];
    // Last cumulative value seen in each calendar month, chronological.
    const order: string[] = [];
    const lastCum = new Map<string, number>();
    for (const r of recs) {
      const m = r.date.slice(0, 7);
      if (!lastCum.has(m)) order.push(m);
      lastCum.set(m, r.cumulative_return_pct);
    }
    const byKey = new Map<string, number>();
    let prevFactor = 1 + (recs.length ? recs[0].cumulative_return_pct : 0) / 100;
    for (const m of order) {
      const cur = 1 + (lastCum.get(m) as number) / 100;
      if (prevFactor > 0) byKey.set(m, (cur / prevFactor - 1) * 100);
      prevFactor = cur;
    }
    const yearFactor = new Map<string, number>();
    for (const [key, ret] of byKey) {
      const y = key.slice(0, 4);
      yearFactor.set(y, (yearFactor.get(y) ?? 1) * (1 + ret / 100));
    }
    const yearTotals = new Map<string, number>();
    for (const [y, f] of yearFactor) yearTotals.set(y, (f - 1) * 100);
    const years = [...yearFactor.keys()].sort();
    const maxAbs = Math.max(1, ...[...byKey.values()].map(Math.abs));
    const maxAbsYear = Math.max(1, ...[...yearTotals.values()].map(Math.abs));
    return { years, byKey, yearTotals, maxAbs, maxAbsYear };
  }, [result]);

  if (years.length === 0) {
    return (
      <CollapsibleCard title="Monthly returns" defaultCollapsed={defaultCollapsed} bodyClassName="px-5 py-4">
        <div className="text-xs text-fg-subtle">No daily equity curve available for this run.</div>
      </CollapsibleCard>
    );
  }

  return (
    <CollapsibleCard title="Monthly returns" defaultCollapsed={defaultCollapsed} bodyClassName="px-3 py-3">
      <div className="overflow-x-auto">
        <table className="w-full text-[11px]" style={{ borderCollapse: 'collapse' }}>
          <thead>
            <tr className="text-fg-faint">
              <th className="px-2 py-1 text-left font-medium" />
              {MONTHS.map((m) => <th key={m} className="px-1.5 py-1 text-center font-medium">{m}</th>)}
              <th className="px-2 py-1 text-right font-medium">Year</th>
            </tr>
          </thead>
          <tbody>
            {years.map((y) => (
              <tr key={y}>
                <td className="px-2 py-1 font-mono text-fg-soft" style={{ border: '1px solid var(--color-card)' }}>{y}</td>
                {MONTHS.map((_, mi) => {
                  const v = byKey.get(`${y}-${String(mi + 1).padStart(2, '0')}`);
                  return (
                    <td key={mi} className="px-1.5 py-1 text-center font-mono" style={tint(v, maxAbs)}>
                      {v == null ? '' : fmt(v)}
                    </td>
                  );
                })}
                <td className="px-2 py-1 text-right font-mono font-medium" style={tint(yearTotals.get(y), maxAbsYear)}>
                  {(() => { const yv = yearTotals.get(y); return yv == null ? '' : fmt(yv); })()}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className="text-[10px] text-fg-faint mt-2 px-2">
        Calendar-month % returns from the daily equity curve (resampled to month-end). Green = up, red = down; intensity scales with magnitude. The first month is measured from inception; &ldquo;Year&rdquo; compounds that year&apos;s months.
      </p>
    </CollapsibleCard>
  );
}
