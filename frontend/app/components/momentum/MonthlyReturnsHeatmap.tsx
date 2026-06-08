'use client';

import { useEffect, useMemo, useState } from 'react';
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
const monthLabel = (ym: string) => {
  const [y, m] = ym.split('-');
  return `${MONTHS[Number(m) - 1]} ${y}`;
};

export default function MonthlyReturnsHeatmap({
  result,
  defaultCollapsed = false,
}: {
  result: BacktestResult;
  defaultCollapsed?: boolean;
}) {
  const [selected, setSelected] = useState<string | null>(null); // "YYYY-MM" drill-down

  const { years, byKey, yearTotals, maxAbs, maxAbsYear, dailyByMonth, maxAbsDaily } = useMemo(() => {
    // Dedupe by date (keep the last cumulative value) — the daily curve repeats
    // period-boundary dates (prior period's exit == next period's entry), which
    // would otherwise produce duplicate day cells / spurious 0% boundary points.
    const seen = new Map<string, number>();
    const recs: { date: string; cumulative_return_pct: number }[] = [];
    for (const r of result.daily_records ?? []) {
      const d = r.date.slice(0, 10);
      const idx = seen.get(d);
      if (idx === undefined) { seen.set(d, recs.length); recs.push({ date: d, cumulative_return_pct: r.cumulative_return_pct }); }
      else { recs[idx].cumulative_return_pct = r.cumulative_return_pct; }
    }
    // Daily % returns from the cumulative curve, grouped by calendar month.
    const dailyByMonth = new Map<string, { date: string; ret: number }[]>();
    let maxAbsDaily = 0.5;
    for (let i = 1; i < recs.length; i++) {
      const f0 = 1 + recs[i - 1].cumulative_return_pct / 100;
      const f1 = 1 + recs[i].cumulative_return_pct / 100;
      if (f0 <= 0) continue;
      const ret = (f1 / f0 - 1) * 100;
      const date = recs[i].date.slice(0, 10);
      const m = date.slice(0, 7);
      if (!dailyByMonth.has(m)) dailyByMonth.set(m, []);
      dailyByMonth.get(m)!.push({ date, ret });
      maxAbsDaily = Math.max(maxAbsDaily, Math.abs(ret));
    }
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
    return { years, byKey, yearTotals, maxAbs, maxAbsYear, dailyByMonth, maxAbsDaily };
  }, [result]);

  // Close the drill-down when a new backtest loads or the month vanishes.
  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect
    if (selected != null && !dailyByMonth.has(selected)) setSelected(null);
  }, [dailyByMonth, selected]);

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
                  const key = `${y}-${String(mi + 1).padStart(2, '0')}`;
                  const v = byKey.get(key);
                  const isSel = selected === key;
                  return (
                    <td
                      key={mi}
                      onClick={v == null ? undefined : () => setSelected(isSel ? null : key)}
                      title={v == null ? undefined : `${monthLabel(key)}: ${fmt(v)}% — click for daily`}
                      className={`px-1.5 py-1 text-center font-mono ${v == null ? '' : 'cursor-pointer'}`}
                      style={{ ...tint(v, maxAbs), ...(isSel ? { boxShadow: 'inset 0 0 0 2px var(--color-accent-500)' } : {}) }}
                    >
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
      {/* Daily drill-down for the clicked month. */}
      {selected && (() => {
        const days = dailyByMonth.get(selected) ?? [];
        if (days.length === 0) return null;
        const rets = days.map((d) => d.ret);
        const total = (days.reduce((f, d) => f * (1 + d.ret / 100), 1) - 1) * 100;
        const up = rets.filter((r) => r > 0).length;
        const best = Math.max(...rets), worst = Math.min(...rets);
        return (
          <div className="mt-3 pt-3 border-t border-neutral-800/40 px-2">
            <div className="flex items-center justify-between mb-2">
              <span className="text-xs font-medium text-fg-soft">{monthLabel(selected)} · daily returns</span>
              <button type="button" onClick={() => setSelected(null)} className="text-[11px] text-fg-faint hover:text-fg-soft">✕ close</button>
            </div>
            <div className="flex flex-wrap gap-1">
              {days.map((d) => (
                <div
                  key={d.date}
                  title={`${d.date}: ${fmt(d.ret)}%`}
                  className="flex flex-col items-center justify-center rounded w-9 h-9 text-[9px] font-mono leading-tight"
                  style={tint(d.ret, maxAbsDaily)}
                >
                  <span className="text-fg-faint">{Number(d.date.slice(8, 10))}</span>
                  <span className="text-fg-strong">{fmt(d.ret)}</span>
                </div>
              ))}
            </div>
            <div className="text-[11px] text-fg-subtle mt-2 flex flex-wrap gap-x-4 gap-y-1">
              <span>month <span className={`font-mono ${total >= 0 ? 'text-pos-400' : 'text-neg-400'}`}>{fmt(total)}%</span></span>
              <span>up days <span className="font-mono text-fg-soft">{up}/{days.length}</span></span>
              <span>best <span className="font-mono text-pos-400">{fmt(best)}%</span></span>
              <span>worst <span className="font-mono text-neg-400">{fmt(worst)}%</span></span>
            </div>
          </div>
        );
      })()}
      <p className="text-[10px] text-fg-faint mt-2 px-2">
        Calendar-month % returns from the daily equity curve (resampled to month-end). Green = up, red = down; intensity scales with magnitude. The first month is measured from inception; &ldquo;Year&rdquo; compounds that year&apos;s months. <span className="text-fg-subtle">Click a month to drill into its daily returns.</span>
      </p>
    </CollapsibleCard>
  );
}
