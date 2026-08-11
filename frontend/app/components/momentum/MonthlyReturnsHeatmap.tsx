'use client';

import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { CSSProperties } from 'react';
import CollapsibleCard from './CollapsibleCard';
import CellInfoTip from './CellInfoTip';
import type { BacktestResult } from '../../../lib/stores/momentum';
import { useBenchmarks } from '../../../lib/hooks/apiData';
import { useClickOutside } from '../../../lib/hooks/useClickOutside';
import { loadFreshBenchmarkPrices, type BenchStatus } from './benchmarkFreshness';
import type { BenchmarkOption, BenchmarkPrice } from './types';

/** A month whose latest live data point mixes carried-forward (stale) prices
 * for some holdings — so its return can't be shown as a clean number. Supplied
 * by /schedule's `stale_prices`; the flagged month's cell renders a warning +
 * an info icon listing the lagging assets instead of a value. */
export type StalePriceWarning = {
  /** "YYYY-MM" — the calendar month of the incomplete cell. */
  month: string;
  /** The freshest close across the basket (the day the cell is marked to). */
  reference_date: string;
  missing: { company_id: number; label: string; ticker: string | null; last_close: string | null }[];
};

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

const fmt = (v: number) => `${v >= 0 ? '+' : ''}${v.toFixed(2)}`;
const monthLabel = (ym: string) => {
  const [y, m] = ym.split('-');
  return `${MONTHS[Number(m) - 1]} ${y}`;
};

/** Monthly + daily returns derived from a cumulative-return curve. */
type Returns = {
  years: string[];
  byKey: Map<string, number>;                       // "YYYY-MM" → month %
  yearTotals: Map<string, number>;                  // "YYYY" → year %
  dailyByMonth: Map<string, { date: string; ret: number }[]>;
  maxAbs: number; maxAbsYear: number; maxAbsDaily: number;
};

/** Resample a `(date, cumulative_return_pct)` curve into calendar-month %
 * returns + a per-month daily-return series + year totals. Shared by the
 * strategy and any benchmark overlay so their cells are computed identically. */
function computeReturns(records: readonly { date: string; cumulative_return_pct: number }[]): Returns {
  // Dedupe by date (keep the last cumulative value) — period-boundary dates
  // repeat, which would otherwise make duplicate day cells / spurious 0% points.
  const seen = new Map<string, number>();
  const recs: { date: string; cumulative_return_pct: number }[] = [];
  for (const r of records) {
    const d = r.date.slice(0, 10);
    const idx = seen.get(d);
    if (idx === undefined) { seen.set(d, recs.length); recs.push({ date: d, cumulative_return_pct: r.cumulative_return_pct }); }
    else { recs[idx].cumulative_return_pct = r.cumulative_return_pct; }
  }
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
  return { years, byKey, yearTotals, dailyByMonth, maxAbs, maxAbsYear, maxAbsDaily };
}

/** A benchmark's daily price series → the `(date, cumulative_return_pct)` curve
 * `computeReturns` expects, clipped to the strategy's `[start, end]` window and
 * rebased so its first in-window close is 0%. */
function benchmarkCurve(prices: readonly BenchmarkPrice[], start: string, end: string): { date: string; cumulative_return_pct: number }[] {
  const win = prices
    .map((p) => ({ date: String(p.target_date).slice(0, 10), price: Number(p.price) }))
    .filter((p) => p.date >= start && p.date <= end && Number.isFinite(p.price) && p.price > 0);
  if (win.length === 0) return [];
  const base = win[0].price;
  return win.map((p) => ({ date: p.date, cumulative_return_pct: (p.price / base - 1) * 100 }));
}

/** Strategy − benchmark, month-by-month / day-by-day, over the dates they share.
 * The grid keeps the strategy's year rows so the layout is unchanged. */
function excessReturns(a: Returns, b: Returns): Returns {
  const byKey = new Map<string, number>();
  for (const [m, v] of a.byKey) if (b.byKey.has(m)) byKey.set(m, v - b.byKey.get(m)!);
  const yearTotals = new Map<string, number>();
  for (const [y, v] of a.yearTotals) if (b.yearTotals.has(y)) yearTotals.set(y, v - b.yearTotals.get(y)!);
  const dailyByMonth = new Map<string, { date: string; ret: number }[]>();
  for (const [m, days] of a.dailyByMonth) {
    const bDay = new Map((b.dailyByMonth.get(m) ?? []).map((d) => [d.date, d.ret]));
    dailyByMonth.set(m, days.filter((d) => bDay.has(d.date)).map((d) => ({ date: d.date, ret: d.ret - bDay.get(d.date)! })));
  }
  const maxAbs = Math.max(1, ...[...byKey.values()].map(Math.abs));
  const maxAbsYear = Math.max(1, ...[...yearTotals.values()].map(Math.abs));
  const maxAbsDaily = Math.max(0.5, ...[...dailyByMonth.values()].flat().map((d) => Math.abs(d.ret)));
  return { years: a.years, byKey, yearTotals, dailyByMonth, maxAbs, maxAbsYear, maxAbsDaily };
}

export default function MonthlyReturnsHeatmap({
  result,
  defaultCollapsed = false,
  markerDate,
  liveThrough,
  staleWarning,
}: {
  result: BacktestResult;
  defaultCollapsed?: boolean;
  /** Go-live date (YYYY-MM-DD). Months before it render dimmed (backtested
   * context); the go-live month gets a red outline matching the equity
   * curve's go-live marker. */
  markerDate?: string;
  /** When the curve was extended with live data, the latest priced day
   * (YYYY-MM-DD) — surfaced in the caption so it's clear the grid is
   * current rather than ending where the backtest was saved. */
  liveThrough?: string;
  /** When the latest live cell mixes carried-forward prices, the affected
   * month + the lagging holdings. That cell shows a warning + info icon
   * instead of a (misleading, partial) number. */
  staleWarning?: StalePriceWarning | null;
}) {
  const [selected, setSelected] = useState<string | null>(null); // "YYYY-MM" drill-down

  // ── Benchmark comparison ──────────────────────────────────────────────
  const { data: _benchmarks } = useBenchmarks();
  const benchmarkOptions = useMemo(() => (_benchmarks ?? []) as BenchmarkOption[], [_benchmarks]);
  const [benchId, setBenchId] = useState<number | null>(null);
  const [benchLabel, setBenchLabel] = useState<string>('');
  const [benchPrices, setBenchPrices] = useState<BenchmarkPrice[] | null>(null);
  const [benchStatus, setBenchStatus] = useState<BenchStatus>(null);
  const [view, setView] = useState<'strategy' | 'benchmark' | 'excess'>('strategy');
  const [loadingBench, setLoadingBench] = useState(false);
  const [pickerOpen, setPickerOpen] = useState(false);
  const pickerRef = useRef<HTMLDivElement>(null);
  useClickOutside(pickerRef, () => setPickerOpen(false));

  // Select a benchmark: fetch its prices (auto-refreshing from GuruFocus if
  // stale vs the strategy's latest date), then default the grid to the "Excess"
  // view. Narrated via `benchStatus`.
  const chooseBenchmark = useCallback(async (id: number) => {
    const opt = benchmarkOptions.find((b) => b.benchmark_id === id);
    const label = opt ? opt.ticker : `Benchmark ${id}`;
    setPickerOpen(false);
    setBenchId(id);
    setBenchLabel(label);
    setLoadingBench(true);
    const dr = result.daily_records ?? [];
    const ref = dr.length ? String(dr[dr.length - 1].date).slice(0, 10) : null;
    try {
      const prices = await loadFreshBenchmarkPrices(id, label, ref, setBenchStatus);
      setBenchPrices(prices);
      setView('excess');
      window.setTimeout(() => setBenchStatus((cur) => (cur && cur.tone === 'ok' ? null : cur)), 6000);
    } catch {
      setBenchStatus({ msg: `Couldn't load ${label}.`, tone: 'warn' });
    } finally {
      setLoadingBench(false);
    }
  }, [benchmarkOptions, result.daily_records]);

  const clearBenchmark = useCallback(() => {
    setBenchId(null); setBenchPrices(null); setBenchStatus(null); setView('strategy');
  }, []);

  // Go-live anchors: the month it falls in gets a marker outline; earlier
  // months are dimmed as pre-go-live backtest context.
  const goLiveMonth = markerDate ? markerDate.slice(0, 7) : null;
  const goLiveYear = markerDate ? markerDate.slice(0, 4) : null;

  // Tooltip body for the incomplete-data cell — lists each lagging holding and
  // the close date it's stuck on. Rendered via CellInfoTip (portaled to <body>
  // so it isn't clipped/mispositioned by the card's containment).
  const staleTip = staleWarning ? (
    <div className="space-y-1">
      <div className="font-medium text-warn-300">Incomplete data — {monthLabel(staleWarning.month)}</div>
      <div className="text-fg-muted">
        No close for <span className="font-mono">{staleWarning.reference_date}</span>{' '}yet; carried forward at an older price, so this month&apos;s return is partial:
      </div>
      <ul className="space-y-0.5">
        {staleWarning.missing.slice(0, 12).map((m) => (
          <li key={m.company_id} className="flex justify-between gap-3">
            <span className="truncate">{m.label}{m.ticker ? ` (${m.ticker})` : ''}</span>
            <span className="font-mono text-fg-faint shrink-0">{m.last_close ?? 'no data'}</span>
          </li>
        ))}
        {staleWarning.missing.length > 12 && (
          <li className="text-fg-faint">+{staleWarning.missing.length - 12} more</li>
        )}
      </ul>
      <div className="text-fg-faint">Fills in once those prices publish.</div>
    </div>
  ) : null;

  // Strategy returns (the base layer).
  const strategyReturns = useMemo(() => computeReturns(result.daily_records ?? []), [result.daily_records]);
  // The strategy's date window — the benchmark is clipped + rebased to it.
  const [winStart, winEnd] = useMemo(() => {
    const dr = result.daily_records ?? [];
    if (dr.length === 0) return [null, null] as const;
    return [String(dr[0].date).slice(0, 10), String(dr[dr.length - 1].date).slice(0, 10)] as const;
  }, [result.daily_records]);
  const benchmarkReturns = useMemo(() => {
    if (!benchPrices || !winStart || !winEnd) return null;
    return computeReturns(benchmarkCurve(benchPrices, winStart, winEnd));
  }, [benchPrices, winStart, winEnd]);
  const excess = useMemo(
    () => (benchmarkReturns ? excessReturns(strategyReturns, benchmarkReturns) : null),
    [strategyReturns, benchmarkReturns],
  );
  // Which layer the grid + drill-down show; falls back to the strategy when the
  // benchmark isn't loaded (or the requested view has no data yet).
  const active: Returns =
    view === 'benchmark' && benchmarkReturns ? benchmarkReturns
      : view === 'excess' && excess ? excess
        : strategyReturns;
  const { years, byKey, yearTotals, maxAbs, maxAbsYear, dailyByMonth, maxAbsDaily } = active;
  // The strategy's stale-price warning only makes sense on the strategy layer.
  const showStale = view === 'strategy';

  // Close the drill-down when a new backtest loads or the month vanishes.
  useEffect(() => {
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
      {/* Benchmark comparison controls */}
      <div className="flex items-center gap-2 flex-wrap mb-3 px-1">
        <span className="text-fg-muted text-xs">Compare vs</span>
        <div className="relative" ref={pickerRef}>
          <button
            type="button"
            onClick={() => setPickerOpen((o) => !o)}
            disabled={loadingBench}
            className="text-xs px-2.5 py-1 rounded-lg border border-neutral-700 bg-page hover:border-accent-500/60 text-fg inline-flex items-center gap-1.5 disabled:opacity-60 disabled:cursor-wait"
          >
            {loadingBench ? (
              <svg className="animate-spin w-3 h-3 text-accent-300" viewBox="0 0 24 24" fill="none">
                <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
              </svg>
            ) : null}
            <span className={benchId != null ? 'font-mono text-warn-300' : 'text-fg-subtle'}>
              {benchId != null ? benchLabel : 'a benchmark…'}
            </span>
            <svg className={`w-3 h-3 text-fg-subtle transition-transform ${pickerOpen ? 'rotate-180' : ''}`} viewBox="0 0 20 20" fill="currentColor">
              <path fillRule="evenodd" d="M5.23 7.21a.75.75 0 011.06.02L10 11.06l3.71-3.83a.75.75 0 111.08 1.04l-4.25 4.39a.75.75 0 01-1.08 0L5.21 8.27a.75.75 0 01.02-1.06z" clipRule="evenodd" />
            </svg>
          </button>
          {pickerOpen && benchmarkOptions.length > 0 && (
            <div className="absolute left-0 mt-1 w-64 bg-card border border-neutral-700 rounded-lg shadow-xl z-50 max-h-72 overflow-auto">
              {benchmarkOptions.map((b) => (
                <button
                  key={b.benchmark_id}
                  type="button"
                  onClick={() => void chooseBenchmark(b.benchmark_id)}
                  className="w-full text-left px-3 py-2 text-xs hover:bg-overlay/[0.03] text-fg flex items-center gap-2"
                >
                  <span className="font-mono text-warn-300">{b.ticker}</span>
                  <span className="text-fg-subtle truncate">{b.name}</span>
                  {b.benchmark_id === benchId && <span className="ml-auto text-[11px] text-accent-300">selected</span>}
                </button>
              ))}
            </div>
          )}
        </div>
        {benchId != null && benchmarkReturns && (
          <>
            <div className="inline-flex rounded-lg border border-neutral-700 overflow-hidden text-xs">
              {([['excess', `Excess vs ${benchLabel}`], ['strategy', 'Strategy'], ['benchmark', benchLabel]] as const).map(([v, lbl]) => (
                <button
                  key={v}
                  type="button"
                  onClick={() => setView(v)}
                  className={`px-2.5 py-1 transition-colors ${view === v ? 'bg-accent-600 text-white' : 'text-fg-subtle hover:bg-overlay/[0.04]'}`}
                >
                  {lbl}
                </button>
              ))}
            </div>
            <button
              type="button"
              onClick={clearBenchmark}
              className="text-[12px] text-fg-faint hover:text-fg-soft underline"
            >
              clear
            </button>
          </>
        )}
      </div>
      {benchStatus && (
        <div className={`mb-3 px-1 text-[12px] flex items-center gap-1.5 ${
          benchStatus.tone === 'ok' ? 'text-pos-400' : benchStatus.tone === 'warn' ? 'text-warn-400' : 'text-accent-300'
        }`}>
          {loadingBench ? (
            <svg className="animate-spin w-3 h-3 shrink-0" viewBox="0 0 24 24" fill="none">
              <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
              <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
            </svg>
          ) : (
            <span aria-hidden className="shrink-0">{benchStatus.tone === 'ok' ? '✓' : '⚠'}</span>
          )}
          <span>{benchStatus.msg}</span>
        </div>
      )}
      {view === 'excess' && (
        <div className="mb-2 px-1 text-[12px] text-fg-subtle">
          Showing <span className="text-fg-soft">strategy − {benchLabel}</span>{' '}per month/day — green = strategy outperformed, red = underperformed.
        </div>
      )}
      <div className="overflow-x-auto">
        <table className="w-full text-[12px]" style={{ borderCollapse: 'collapse' }}>
          <thead>
            <tr className="text-fg-faint">
              <th className="px-2 py-1 text-left font-medium" />
              {MONTHS.map((m) => <th key={m} className="px-1.5 py-1 text-center font-medium">{m}</th>)}
              <th className="px-2 py-1 text-right font-medium">Year</th>
            </tr>
          </thead>
          <tbody>
            {years.map((y) => {
              const isPreGoLiveYear = goLiveYear != null && y < goLiveYear;
              return (
              <tr key={y}>
                <td className="px-2 py-1 font-mono text-fg-soft" style={{ border: '1px solid var(--color-card)', ...(isPreGoLiveYear ? { opacity: 0.4 } : {}) }}>{y}</td>
                {MONTHS.map((_, mi) => {
                  const key = `${y}-${String(mi + 1).padStart(2, '0')}`;
                  const v = byKey.get(key);
                  const isSel = selected === key;
                  const isPreGoLive = goLiveMonth != null && key < goLiveMonth;
                  const isGoLive = goLiveMonth != null && key === goLiveMonth;
                  const isStale = showStale && staleWarning != null && key === staleWarning.month;
                  // Go-live outline (red, matching the equity-curve marker)
                  // wins over the click-selection ring when both apply.
                  const ring = isSel
                    ? 'inset 0 0 0 2px var(--color-accent-500)'
                    : isGoLive
                      ? 'inset 0 0 0 2px var(--color-neg-400)'
                      : undefined;
                  // Incomplete-data cell: some holdings are carried forward at a
                  // stale close, so the return is PARTIAL — but still the best
                  // number we can compute from the prices we have. Show it (so
                  // the month is never blank) on a warning-tinted cell with a ⚠
                  // info icon listing the lagging holdings. Falls back to just
                  // the ⚠ if there's no computable value yet.
                  if (isStale) {
                    return (
                      <td
                        key={mi}
                        onClick={v == null ? undefined : () => setSelected(isSel ? null : key)}
                        title={v == null ? undefined : `${monthLabel(key)}: ${fmt(v)}% (partial — some holdings on stale prices) — click for daily`}
                        className={`px-1.5 py-1 text-center font-mono ${v == null ? '' : 'cursor-pointer'}`}
                        style={{ background: 'color-mix(in srgb, var(--color-warn-500) 22%, transparent)', color: 'var(--color-fg-strong)', ...(ring ? { boxShadow: ring } : {}) }}
                      >
                        <span className="inline-flex items-center justify-center gap-0.5">
                          {v != null && <span>{fmt(v)}</span>}
                          <CellInfoTip trigger={<span aria-hidden className="text-warn-400 text-[12px] leading-none">⚠</span>}>
                            {staleTip}
                          </CellInfoTip>
                        </span>
                      </td>
                    );
                  }
                  return (
                    <td
                      key={mi}
                      onClick={v == null ? undefined : () => setSelected(isSel ? null : key)}
                      title={v == null ? undefined : `${monthLabel(key)}: ${fmt(v)}%${isGoLive ? ' — go-live month' : ''}${isPreGoLive ? ' — pre go-live (backtest)' : ''} — click for daily`}
                      className={`px-1.5 py-1 text-center font-mono ${v == null ? '' : 'cursor-pointer'}`}
                      style={{ ...tint(v, maxAbs), ...(ring ? { boxShadow: ring } : {}), ...(isPreGoLive ? { opacity: 0.4 } : {}) }}
                    >
                      {v == null ? '' : fmt(v)}
                    </td>
                  );
                })}
                <td className="px-2 py-1 text-right font-mono font-medium" style={{ ...tint(yearTotals.get(y), maxAbsYear), ...(isPreGoLiveYear ? { opacity: 0.4 } : {}) }}>
                  {(() => { const yv = yearTotals.get(y); return yv == null ? '' : fmt(yv); })()}
                </td>
              </tr>
              );
            })}
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
              <button type="button" onClick={() => setSelected(null)} className="text-[12px] text-fg-faint hover:text-fg-soft">✕ close</button>
            </div>
            <div className="flex flex-wrap gap-1">
              {days.map((d) => (
                <div
                  key={d.date}
                  title={`${d.date}: ${fmt(d.ret)}%`}
                  className="flex flex-col items-center justify-center rounded w-9 h-9 text-[10px] font-mono leading-tight"
                  style={tint(d.ret, maxAbsDaily)}
                >
                  <span className="text-fg-faint">{Number(d.date.slice(8, 10))}</span>
                  <span className="text-fg-strong">{fmt(d.ret)}</span>
                </div>
              ))}
            </div>
            <div className="text-[12px] text-fg-subtle mt-2 flex flex-wrap gap-x-4 gap-y-1">
              <span>month <span className={`font-mono ${total >= 0 ? 'text-pos-400' : 'text-neg-400'}`}>{fmt(total)}%</span></span>
              <span>up days <span className="font-mono text-fg-soft">{up}/{days.length}</span></span>
              <span>best <span className="font-mono text-pos-400">{fmt(best)}%</span></span>
              <span>worst <span className="font-mono text-neg-400">{fmt(worst)}%</span></span>
            </div>
          </div>
        );
      })()}
      {markerDate && (
        <p className="text-[11px] text-fg-faint mt-2 px-2 flex flex-wrap items-center gap-x-3 gap-y-1">
          <span className="inline-flex items-center gap-1.5">
            <span className="inline-block w-3 h-3 rounded-sm" style={{ boxShadow: 'inset 0 0 0 2px var(--color-neg-400)' }} />
            Go-live <span className="font-mono text-fg-subtle">{markerDate}</span>
          </span>
          <span className="inline-flex items-center gap-1.5">
            <span className="inline-block w-3 h-3 rounded-sm" style={{ background: 'var(--color-pos-500)', opacity: 0.4 }} />
            Dimmed = pre go-live (backtested context)
          </span>
          {liveThrough && (
            <span>Live data through <span className="font-mono text-fg-subtle">{liveThrough}</span></span>
          )}
        </p>
      )}
      <p className="text-[11px] text-fg-faint mt-2 px-2">
        Calendar-month % returns from the daily equity curve (resampled to month-end). Green = up, red = down; intensity scales with magnitude. The first month is measured from inception; &ldquo;Year&rdquo; compounds that year&apos;s months. <span className="text-fg-subtle">Click a month to drill into its daily returns.</span>
      </p>
    </CollapsibleCard>
  );
}
