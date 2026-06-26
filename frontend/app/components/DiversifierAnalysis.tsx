'use client';

import { Fragment, useState } from 'react';
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, Legend,
} from 'recharts';
import { dialog } from '../../lib/dialog';
import { chartTheme } from '../../lib/chartTheme';
import LoadingDots from './LoadingDots';
import { useDiversifier } from './diversifier/useDiversifier';
import type { CorrelationResponse, DiversifierResult, DrawdownInfo, OptimizeResponse } from '../../lib/types/api';

const fmtPct = (v: number | null | undefined, dp = 1) =>
  v == null ? '—' : `${(v * 100).toFixed(dp)}%`;
const fmtNum = (v: number | null | undefined, dp = 2) =>
  v == null ? '—' : v.toFixed(dp);
const fmtSigned = (v: number | null | undefined, dp = 2) =>
  v == null ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(dp)}`;
// Fraction (0.03) → signed percent ("+3.0%").
const fmtSignedPct = (v: number | null | undefined, dp = 1) =>
  v == null ? '—' : `${v >= 0 ? '+' : ''}${(v * 100).toFixed(dp)}%`;
const pnlClass = (v: number | null | undefined) =>
  v == null ? 'text-fg-subtle' : v >= 0 ? 'text-pos-400' : 'text-neg-400';

/** Correlation colour: lower = better diversifier. */
function corrClass(c: number | null): string {
  if (c == null) return 'text-fg-subtle';
  if (c <= 0.3) return 'text-pos-400';
  if (c <= 0.7) return 'text-warn-400';
  return 'text-neg-400';
}

export default function DiversifierAnalysis() {
  const d = useDiversifier();
  const [tickerInput, setTickerInput] = useState('');

  const canRun = d.selectedRunId != null && d.selectedEtfIds.size > 0 && !d.running;

  return (
    <div className="flex flex-col h-full">
      <div className="px-8 py-5 border-b border-neutral-800/60">
        <h1 className="text-lg font-semibold text-fg-strong">Diversifier</h1>
        <p className="text-xs text-fg-subtle mt-0.5">
          Pick a backtest, add ETFs, and see which ones are least correlated with your
          strategy — and how much each lifts Sharpe/Sortino when blended in.
        </p>
      </div>

      <div className="flex-1 overflow-auto px-8 py-5 space-y-5">
        {d.error && (
          <div className="bg-neg-500/10 border border-neg-500/20 rounded-lg px-4 py-2.5 text-sm text-neg-400">
            {d.error}
          </div>
        )}

        {/* ── Strategy + params ─────────────────────────────────────── */}
        <div className="bg-card rounded-xl border border-neutral-800/40 p-5">
          <h3 className="text-fg-muted text-xs font-medium mb-3 uppercase tracking-wider">Strategy</h3>
          <div className="flex flex-wrap items-end gap-3">
            <div className="flex-1 min-w-64">
              <label className="text-fg-subtle text-xs block mb-1">Saved backtest</label>
              <select
                value={d.selectedRunId ?? ''}
                onChange={(e) => d.selectBacktest(e.target.value ? Number(e.target.value) : null)}
                className="w-full bg-page border border-neutral-700 rounded-lg px-3 py-2 text-fg-strong text-sm focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none"
              >
                <option value="">{d.loadingLists ? 'Loading…' : 'Select a backtest…'}</option>
                {d.backtests.map((b) => (
                  <option key={b.run_id} value={b.run_id}>
                    {b.name}
                  </option>
                ))}
              </select>
            </div>

            {d.variantOptions && d.variantOptions.length > 0 && (
              <div className="min-w-48">
                <label className="text-fg-subtle text-xs block mb-1">Variant</label>
                <select
                  value={d.variantKey ?? ''}
                  onChange={(e) => d.selectVariant(e.target.value || null)}
                  className="w-full bg-page border border-neutral-700 rounded-lg px-3 py-2 text-fg-strong text-sm font-mono focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none"
                >
                  <option value="">Pick variant…</option>
                  {d.variantOptions.map((k) => (
                    <option key={k} value={k}>{k}</option>
                  ))}
                </select>
              </div>
            )}

            <div className="w-28">
              <label className="text-fg-subtle text-xs block mb-1">Risk-free %</label>
              <input
                type="number"
                step="0.5"
                value={d.riskFreePct}
                onChange={(e) => d.setRiskFreePct(Number(e.target.value))}
                className="w-full bg-page border border-neutral-700 rounded-lg px-3 py-2 text-fg-strong text-sm font-mono focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none"
              />
            </div>
            <div className="w-28">
              <label className="text-fg-subtle text-xs block mb-1" title="Minimum weight kept in your strategy; the ETF sleeve gets at most the rest (100 − this). 0 = optimizer chooses freely.">
                Min strategy %
              </label>
              <input
                type="number"
                step="5"
                min={0}
                max={100}
                value={d.minStrategyPct}
                onChange={(e) => d.setMinStrategyPct(Number(e.target.value))}
                className="w-full bg-page border border-neutral-700 rounded-lg px-3 py-2 text-fg-strong text-sm font-mono focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none"
              />
            </div>
            <div>
              <label className="text-fg-subtle text-xs block mb-1" title="Which metric the blend optimizer maximizes">
                Optimize for
              </label>
              <div className="inline-flex rounded-lg border border-neutral-700 overflow-hidden">
                {(['sharpe', 'sortino'] as const).map((o) => (
                  <button
                    key={o}
                    onClick={() => d.setObjective(o)}
                    className={`px-3 py-2 text-sm font-medium transition-colors ${
                      d.objective === o ? 'bg-accent-600 text-fg-strong' : 'bg-page text-fg-muted hover:text-fg-strong'
                    }`}
                  >
                    {o === 'sharpe' ? 'Sharpe' : 'Sortino'}
                  </button>
                ))}
              </div>
            </div>
            <button
              onClick={d.runCorrelation}
              disabled={!canRun}
              className="px-4 py-2 rounded-lg text-sm font-medium bg-accent-600 hover:bg-accent-500 text-fg-strong transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
            >
              {d.running ? <LoadingDots /> : 'Compute correlation'}
            </button>
            <button
              onClick={d.runOptimize}
              disabled={d.selectedRunId == null || d.selectedEtfIds.size === 0 || d.optimizing}
              title="Find the best weights across your strategy + all selected ETFs at once"
              className="px-4 py-2 rounded-lg text-sm font-medium border border-accent-500 text-accent-400 hover:bg-accent-600/10 transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
            >
              {d.optimizing ? <LoadingDots /> : 'Optimize portfolio'}
            </button>
          </div>

          {/* Selected backtest's saved headline stats — the baseline to beat. */}
          {d.backtestStats && d.backtestStats.sharpe_ratio != null && (
            <div className="flex flex-wrap gap-x-6 gap-y-1 mt-4 pt-4 border-t border-neutral-800/40 text-sm">
              <span className="text-fg-subtle text-xs uppercase tracking-wider self-center">Backtest (reported)</span>
              <span className="text-fg-muted">Sharpe <span className="font-mono text-fg-strong">{fmtNum(d.backtestStats.sharpe_ratio)}</span></span>
              <span className="text-fg-muted">Sortino <span className="font-mono text-fg-strong">{fmtNum(d.backtestStats.sortino_ratio)}</span></span>
              <span className="text-fg-muted">Ann. return <span className="font-mono text-fg">{fmtNum(d.backtestStats.annualized_return_pct, 1)}%</span></span>
              <span className="text-fg-muted">Max DD <span className="font-mono text-fg">{fmtNum(d.backtestStats.max_drawdown_pct, 1)}%</span></span>
              {d.backtestStats.period_from && (
                <span className="text-fg-subtle">{d.backtestStats.period_from}–{d.backtestStats.period_to}</span>
              )}
            </div>
          )}
        </div>

        {/* ── ETF universe ──────────────────────────────────────────── */}
        <div className="bg-card rounded-xl border border-neutral-800/40 p-5">
          <div className="flex items-center justify-between mb-3 gap-3 flex-wrap">
            <h3 className="text-fg-muted text-xs font-medium uppercase tracking-wider">
              ETFs {d.selectedEtfIds.size > 0 && <span className="text-accent-400">· {d.selectedEtfIds.size} selected</span>}
            </h3>
            <div className="flex items-center gap-4">
              <label className="flex items-center gap-1.5 text-xs text-fg-muted" title="Keep only ETFs whose history starts before Jan 1 of this year — young funds shorten the optimizer's common window">
                Require history before
                <input
                  type="text"
                  inputMode="numeric"
                  maxLength={4}
                  placeholder="e.g. 2010"
                  value={d.cutoffYear}
                  onChange={(e) => d.setCutoffYear(e.target.value)}
                  className="w-24 bg-page border border-neutral-700 rounded px-2 py-1 text-xs font-mono text-fg-strong focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none placeholder-fg-faint"
                />
                {d.cutoffYear && (
                  <button onClick={() => d.setCutoffYear('')} className="text-fg-subtle hover:text-fg-strong" title="Clear filter">✕</button>
                )}
              </label>
              {d.visibleEtfs.length > 0 && (
                <button
                  onClick={d.toggleSelectAll}
                  className="text-xs font-medium text-accent-400 hover:text-accent-500 transition-colors"
                >
                  {d.visibleEtfs.every((e) => d.selectedEtfIds.has(e.benchmark_id)) ? 'Clear all' : 'Select all'}
                </button>
              )}
            </div>
          </div>

          <div className="flex items-end gap-3 mb-4">
            <div>
              <label className="text-fg-subtle text-xs block mb-1">Ticker (from the GuruFocus ETF URL)</label>
              <input
                value={tickerInput}
                onChange={(e) => setTickerInput(e.target.value)}
                placeholder="e.g. DEM"
                className="bg-page border border-neutral-700 rounded-lg px-3 py-2 text-fg-strong text-sm font-mono w-40 focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none placeholder-fg-faint"
                onKeyDown={(e) => {
                  if (e.key === 'Enter') {
                    d.addEtf(tickerInput);
                    setTickerInput('');
                  }
                }}
              />
            </div>
            <button
              onClick={() => { d.addEtf(tickerInput); setTickerInput(''); }}
              disabled={d.adding || !tickerInput.trim()}
              className="px-4 py-2 rounded-lg text-sm font-medium bg-accent-600 hover:bg-accent-500 text-fg-strong transition-colors disabled:opacity-40"
            >
              {d.adding ? <LoadingDots /> : 'Add & fetch prices'}
            </button>
          </div>

          {d.etfs.length === 0 ? (
            <p className="text-sm text-fg-subtle">{d.loadingLists ? 'Loading…' : 'No ETFs yet — add one above.'}</p>
          ) : d.visibleEtfs.length === 0 ? (
            <p className="text-sm text-fg-subtle">No ETFs with history before {d.cutoffYear}. Adjust the filter.</p>
          ) : (
            <div className="overflow-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-fg-subtle text-xs border-b border-neutral-800/60">
                    <th className="text-left font-medium py-2 px-2 w-8"></th>
                    <th className="text-left font-medium py-2 px-2">Ticker</th>
                    <th className="text-left font-medium py-2 px-2">Name</th>
                    <th className="text-left font-medium py-2 px-2">Prices from</th>
                    <th className="text-left font-medium py-2 px-2">Prices to</th>
                    <th className="text-right font-medium py-2 px-2"></th>
                  </tr>
                </thead>
                <tbody>
                  {d.visibleEtfs.map((e) => (
                    <tr key={e.benchmark_id} className="group border-b border-neutral-800/40 hover:bg-overlay/[0.02]">
                      <td className="py-2.5 px-2">
                        <input
                          type="checkbox"
                          checked={d.selectedEtfIds.has(e.benchmark_id)}
                          onChange={() => d.toggleEtf(e.benchmark_id)}
                          className="accent-accent-500 w-4 h-4 cursor-pointer"
                        />
                      </td>
                      <td className="py-2.5 px-2 font-mono text-fg-strong">{e.ticker}</td>
                      <td className="py-2.5 px-2 text-fg">{e.name}</td>
                      <td className="py-2.5 px-2 font-mono text-fg-muted">{e.price_from ?? '—'}</td>
                      <td className="py-2.5 px-2 font-mono text-fg-muted">{e.price_to ?? '—'}</td>
                      <td className="py-2.5 px-2 text-right whitespace-nowrap">
                        <button
                          onClick={() => d.refreshEtf(e.benchmark_id)}
                          disabled={d.busyEtfId === e.benchmark_id}
                          className="opacity-0 group-hover:opacity-100 text-xs text-fg-muted hover:text-accent-400 transition-all disabled:opacity-50 mr-3"
                        >
                          {d.busyEtfId === e.benchmark_id ? '…' : 'Refresh'}
                        </button>
                        <button
                          onClick={async () => {
                            if (await dialog.confirm(`Delete ETF "${e.ticker}"?`, { destructive: true, confirmLabel: 'Delete' })) {
                              d.deleteEtf(e.benchmark_id);
                            }
                          }}
                          disabled={d.busyEtfId === e.benchmark_id}
                          className="opacity-0 group-hover:opacity-100 text-xs text-fg-muted hover:text-neg-400 transition-all disabled:opacity-50"
                        >
                          Delete
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>

        {/* ── Portfolio optimization ────────────────────────────────── */}
        {d.optimizeResult && <OptimizeCard result={d.optimizeResult} />}

        {/* ── Per-ETF correlation results ───────────────────────────── */}
        {d.result && <ResultsCard result={d.result} />}
      </div>
    </div>
  );
}

function StatPair({ label, before, after, dp = 2 }: { label: string; before: number | null | undefined; after: number | null | undefined; dp?: number }) {
  const lift = before != null && after != null ? after - before : null;
  return (
    <div className="flex flex-col">
      <span className="text-fg-subtle text-xs uppercase tracking-wider">{label}</span>
      <span className="font-mono text-sm">
        <span className="text-fg-muted">{fmtNum(before, dp)}</span>
        <span className="text-fg-subtle mx-1.5">→</span>
        <span className="text-fg-strong">{fmtNum(after, dp)}</span>
        {lift != null && Math.abs(lift) > 0.005 && (
          <span className={`ml-2 ${lift > 0 ? 'text-pos-400' : 'text-neg-400'}`}>{fmtSigned(lift, dp)}</span>
        )}
      </span>
    </div>
  );
}

function DrawdownTable({ title, rows, accent }: { title: string; rows: DrawdownInfo[]; accent?: boolean }) {
  return (
    <div>
      <div className={`text-xs font-medium mb-1.5 ${accent ? 'text-accent-400' : 'text-fg-muted'}`}>{title}</div>
      {rows.length === 0 ? (
        <p className="text-xs text-fg-subtle py-2">No drawdowns over this window.</p>
      ) : (
        <table className="w-full text-xs">
          <thead>
            <tr className="text-fg-subtle border-b border-neutral-800/60">
              <th className="text-left font-medium py-1.5 pr-2">Depth</th>
              <th className="text-left font-medium py-1.5 px-2">Peak → trough</th>
              <th className="text-left font-medium py-1.5 px-2">Recovered</th>
              <th className="text-right font-medium py-1.5 pl-2" title="Months from peak to trough">Len</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((d, i) => (
              <tr key={i} className="border-b border-neutral-800/40">
                <td className="py-1.5 pr-2 font-mono text-neg-400 font-medium">{d.depth_pct.toFixed(1)}%</td>
                <td className="py-1.5 px-2 font-mono text-fg-muted">{d.peak_date} → {d.trough_date}</td>
                <td className="py-1.5 px-2 font-mono text-fg-muted">{d.recovery_date ?? <span className="text-warn-400">ongoing</span>}</td>
                <td className="py-1.5 pl-2 text-right font-mono text-fg-muted">{d.length_months}mo</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}

function OptimizeCard({ result: r }: { result: OptimizeResponse }) {
  const sorted = [...r.weights].sort((a, b) => b.weight - a.weight);
  const objLabel = r.objective === 'sortino' ? 'Sortino' : 'Sharpe';
  // Log scale plots the growth multiple (1 + cum%/100) — always positive — so
  // the early years stay visible against a strategy that's up many-fold.
  const [logScale, setLogScale] = useState(true);
  // Track COLLAPSED years (empty by default → all years expanded, including
  // on a fresh optimize run).
  const [collapsedYears, setCollapsedYears] = useState<Set<number>>(new Set());
  const toggleYear = (year: number) =>
    setCollapsedYears((prev) => {
      const next = new Set(prev);
      if (next.has(year)) next.delete(year);
      else next.add(year);
      return next;
    });
  const chartData = r.curve.map((p) => ({
    date: p.date,
    before: p.before,
    after: p.after,
    beforeG: 1 + p.before / 100,
    afterG: 1 + p.after / 100,
  }));
  return (
    <div className="bg-card rounded-xl border border-accent-500/30 p-5">
      <div className="flex items-baseline justify-between mb-3">
        <h3 className="text-accent-400 text-xs font-medium uppercase tracking-wider">
          Optimized portfolio · maximizing {objLabel}
        </h3>
        <span className="text-fg-subtle text-xs">
          {r.months} mo · {r.period_from}–{r.period_to}
          {r.limited_by && <span className="text-warn-400"> · window limited by {r.limited_by}</span>}
        </span>
      </div>

      {/* Before → after */}
      <div className="flex flex-wrap gap-x-10 gap-y-3 mb-5">
        <StatPair label="Sharpe" before={r.before.sharpe} after={r.after.sharpe} />
        <StatPair label="Sortino" before={r.before.sortino} after={r.after.sortino} />
        <StatPair label="Ann. return" before={r.before.ann_return != null ? r.before.ann_return * 100 : null} after={r.after.ann_return != null ? r.after.ann_return * 100 : null} dp={1} />
        <StatPair label="Vol" before={r.before.ann_vol != null ? r.before.ann_vol * 100 : null} after={r.after.ann_vol != null ? r.after.ann_vol * 100 : null} dp={1} />
        <StatPair label="YTD" before={r.ytd_before != null ? r.ytd_before * 100 : null} after={r.ytd_after != null ? r.ytd_after * 100 : null} dp={1} />
        <StatPair label="Median month" before={r.before.median_month != null ? r.before.median_month * 100 : null} after={r.after.median_month != null ? r.after.median_month * 100 : null} dp={2} />
        <StatPair label="Win rate (mo)" before={r.before.win_rate != null ? r.before.win_rate * 100 : null} after={r.after.win_rate != null ? r.after.win_rate * 100 : null} dp={0} />
      </div>

      {/* Before/after equity curve over the common window */}
      {r.curve.length > 1 && (
        <div className="mb-5">
          <div className="flex items-center justify-between mb-2">
            <div className="text-fg-subtle text-xs uppercase tracking-wider">
              Equity curve (cumulative return{logScale ? ', log' : ''})
            </div>
            <label className="flex items-center gap-1.5 text-xs text-fg-muted cursor-pointer">
              <input
                type="checkbox"
                checked={logScale}
                onChange={(e) => setLogScale(e.target.checked)}
                className="accent-accent-500 w-3.5 h-3.5"
              />
              Log scale
            </label>
          </div>
          <ResponsiveContainer width="100%" height={260}>
            <LineChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke={chartTheme.grid} />
              <XAxis
                dataKey="date"
                tick={{ fill: chartTheme.axisTick, fontSize: 11 }}
                tickLine={false}
                interval={Math.max(0, Math.floor(chartData.length / 10) - 1)}
                tickFormatter={(d: string) => (typeof d === 'string' ? d.slice(0, 7) : '')}
              />
              <YAxis
                tick={{ fill: chartTheme.axisTick, fontSize: 11 }}
                tickLine={false}
                scale={logScale ? 'log' : 'linear'}
                domain={logScale ? ['auto', 'auto'] : undefined}
                allowDataOverflow={logScale}
                tickFormatter={(v: number) => (logScale ? `${v % 1 === 0 ? v : v.toFixed(1)}×` : `${v}%`)}
              />
              <Tooltip
                {...chartTheme.tooltip}
                labelFormatter={(d) => String(d)}
                formatter={(value, name) => {
                  // Always report the cumulative % regardless of axis mode.
                  const pct = logScale ? (Number(value) - 1) * 100 : Number(value);
                  const isBefore = name === 'before' || name === 'beforeG';
                  return [`${pct >= 0 ? '+' : ''}${pct.toFixed(1)}%`, isBefore ? 'Strategy alone' : 'Optimized'];
                }}
              />
              <Legend
                wrapperStyle={{ fontSize: 12, color: chartTheme.axisLabel }}
                formatter={(v) => (v === 'before' || v === 'beforeG' ? 'Strategy alone' : 'Optimized')}
              />
              <Line type="monotone" dataKey={logScale ? 'beforeG' : 'before'} stroke={chartTheme.universe} strokeWidth={1.5} strokeDasharray="4 3" dot={false} name={logScale ? 'beforeG' : 'before'} />
              <Line type="monotone" dataKey={logScale ? 'afterG' : 'after'} stroke={chartTheme.accent} strokeWidth={2} dot={false} name={logScale ? 'afterG' : 'after'} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* Per-year returns + volatility, before vs after */}
      {r.annual.length > 0 && (
        <div className="mb-5">
          <div className="text-fg-subtle text-xs uppercase tracking-wider mb-2">Returns &amp; volatility per year <span className="text-fg-faint normal-case">· click a year for months</span></div>
          <div className="overflow-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-fg-subtle text-xs border-b border-neutral-800/60">
                  <th className="text-left font-medium py-1.5 pr-2">Year</th>
                  <th className="text-right font-medium py-1.5 px-2">Return strat</th>
                  <th className="text-right font-medium py-1.5 px-2 text-accent-400/80">Return opt</th>
                  <th className="text-right font-medium py-1.5 px-2 border-l border-neutral-800/40">Vol strat</th>
                  <th className="text-right font-medium py-1.5 pl-2 text-accent-400/80">Vol opt</th>
                </tr>
              </thead>
              <tbody>
                {r.annual.map((y) => {
                  const open = !collapsedYears.has(y.year);
                  return (
                    <Fragment key={y.year}>
                      <tr className="border-b border-neutral-800/40 hover:bg-overlay/[0.02] cursor-pointer" onClick={() => toggleYear(y.year)}>
                        <td className="py-1.5 pr-2 font-mono text-fg-strong">
                          <span className="inline-block w-3 text-fg-subtle">{open ? '▾' : '▸'}</span>{y.year}
                        </td>
                        <td className={`py-1.5 px-2 text-right font-mono ${pnlClass(y.return_before)}`}>{fmtSignedPct(y.return_before)}</td>
                        <td className={`py-1.5 px-2 text-right font-mono ${pnlClass(y.return_after)}`}>{fmtSignedPct(y.return_after)}</td>
                        <td className="py-1.5 px-2 text-right font-mono text-fg-muted border-l border-neutral-800/40">{y.vol_before != null ? `${(y.vol_before * 100).toFixed(1)}%` : '—'}</td>
                        <td className="py-1.5 pl-2 text-right font-mono text-fg-muted">{y.vol_after != null ? `${(y.vol_after * 100).toFixed(1)}%` : '—'}</td>
                      </tr>
                      {open && (y.months ?? []).map((m) => (
                        <tr key={m.month} className="border-b border-neutral-800/20 bg-overlay/[0.015]">
                          <td className="py-1 pr-2 pl-5 font-mono text-xs text-fg-subtle">{m.month}</td>
                          <td className={`py-1 px-2 text-right font-mono text-xs ${pnlClass(m.return_before)}`}>{fmtSignedPct(m.return_before)}</td>
                          <td className={`py-1 px-2 text-right font-mono text-xs ${pnlClass(m.return_after)}`}>{fmtSignedPct(m.return_after)}</td>
                          <td className="border-l border-neutral-800/40"></td>
                          <td></td>
                        </tr>
                      ))}
                    </Fragment>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Weight breakdown */}
      <div className="space-y-1.5">
        {sorted.map((w) => (
          <div key={w.label} className="flex items-center gap-3">
            <div className="w-28 shrink-0 text-sm">
              <span className={`font-mono ${w.label === 'Strategy' ? 'text-accent-400 font-medium' : 'text-fg-strong'}`}>{w.label}</span>
            </div>
            <div className="flex-1 h-5 bg-inset rounded overflow-hidden">
              <div
                className={`h-full ${w.label === 'Strategy' ? 'bg-accent-500/70' : 'bg-pos-500/50'}`}
                style={{ width: `${Math.max(w.weight * 100, w.weight > 0 ? 1 : 0)}%` }}
              />
            </div>
            <div className="w-14 shrink-0 text-right font-mono text-sm text-fg-strong">{(w.weight * 100).toFixed(1)}%</div>
            {/* Always reserve the name column so every bar track (flex-1) has
                the same width — the Strategy row has no name and would
                otherwise stretch its bar into this space. */}
            <div className="hidden md:block w-56 shrink-0 truncate text-xs text-fg-subtle">{w.name ?? ''}</div>
          </div>
        ))}
      </div>

      {/* Top-10 worst drawdowns: strategy alone vs optimized */}
      {(r.drawdowns_before.length > 0 || r.drawdowns_after.length > 0) && (
        <div className="mt-6">
          <div className="text-fg-subtle text-xs uppercase tracking-wider mb-2">Top 40 worst drawdowns</div>
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-x-6 gap-y-4">
            <DrawdownTable title="Strategy alone" rows={r.drawdowns_before} />
            <DrawdownTable title="Optimized" rows={r.drawdowns_after} accent />
          </div>
        </div>
      )}

      <p className="text-xs text-fg-subtle mt-4 leading-relaxed">
        Long-only weights summing to 100%, found by maximizing {objLabel} over the common window where every selected
        ETF has data{r.limited_by ? ` (bounded by ${r.limited_by})` : ''}. Your strategy is held at ≥ your Min strategy %
        (lower it to 0% to let the optimizer choose freely). A 100% Strategy result means no ETF mix beat your
        strategy alone on {objLabel} over this window.
      </p>
    </div>
  );
}

function ResultsCard({ result }: { result: CorrelationResponse }) {
  const s = result.strategy;
  return (
    <div className="bg-card rounded-xl border border-neutral-800/40 p-5">
      <div className="flex items-baseline justify-between mb-1">
        <h3 className="text-fg-muted text-xs font-medium uppercase tracking-wider">Results</h3>
        <span className="text-fg-strong text-sm font-medium">{s.name}</span>
      </div>

      <div className="overflow-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="text-fg-subtle text-xs border-b border-neutral-800/60">
              <th className="text-left font-medium py-2 px-2">ETF</th>
              <th className="text-right font-medium py-2 px-2" title="Pearson correlation of monthly returns vs the strategy (lower = better diversifier)">Corr</th>
              <th className="text-right font-medium py-2 px-2" title="Months both series overlap — each ETF's analysis is over its own window">Overlap</th>
              <th className="text-right font-medium py-2 px-2">ETF ann.</th>
              <th className="text-right font-medium py-2 px-2">ETF Sharpe</th>
              <th className="text-right font-medium py-2 px-2" title="ETF weight that maximizes the chosen objective">Best mix</th>
              <th className="text-right font-medium py-2 px-2 border-l border-neutral-800/40" title="Strategy's OWN Sharpe over this ETF's overlap window — the blend baseline">Strat Sharpe<span className="text-fg-faint">*</span></th>
              <th className="text-right font-medium py-2 px-2">Blend Sharpe</th>
              <th className="text-right font-medium py-2 px-2">ΔSharpe</th>
              <th className="text-right font-medium py-2 px-2 border-l border-neutral-800/40" title="Strategy's OWN Sortino over this ETF's overlap window">Strat Sortino<span className="text-fg-faint">*</span></th>
              <th className="text-right font-medium py-2 px-2">Blend Sortino</th>
              <th className="text-right font-medium py-2 px-2">ΔSortino</th>
            </tr>
          </thead>
          <tbody>
            {result.results.map((r: DiversifierResult) => (
              <tr key={r.benchmark_id} className="border-b border-neutral-800/40 hover:bg-overlay/[0.02]">
                <td className="py-2.5 px-2">
                  <span className="font-mono text-fg-strong">{r.ticker}</span>
                  <span className="text-fg-subtle text-xs ml-2">{r.name}</span>
                </td>
                <td className={`py-2.5 px-2 text-right font-mono font-medium ${corrClass(r.correlation ?? null)}`}>
                  {r.correlation == null ? '—' : r.correlation.toFixed(2)}
                </td>
                <td className="py-2.5 px-2 text-right font-mono text-fg-muted">{r.overlap_months}mo</td>
                <td className="py-2.5 px-2 text-right font-mono text-fg">{fmtPct(r.etf_ann_return)}</td>
                <td className="py-2.5 px-2 text-right font-mono text-fg">{fmtNum(r.etf_sharpe)}</td>
                <td className="py-2.5 px-2 text-right font-mono text-fg">{fmtPct(r.blend_weight, 0)}</td>
                <td className="py-2.5 px-2 text-right font-mono text-fg-muted border-l border-neutral-800/40">{fmtNum(r.strategy_sharpe)}</td>
                <td className="py-2.5 px-2 text-right font-mono text-fg-strong">{fmtNum(r.blend_sharpe)}</td>
                <td className={`py-2.5 px-2 text-right font-mono ${(r.sharpe_lift ?? 0) > 0.005 ? 'text-pos-400' : 'text-fg-subtle'}`}>
                  {fmtSigned(r.sharpe_lift)}
                </td>
                <td className="py-2.5 px-2 text-right font-mono text-fg-muted border-l border-neutral-800/40">{fmtNum(r.strategy_sortino)}</td>
                <td className="py-2.5 px-2 text-right font-mono text-fg-strong">{fmtNum(r.blend_sortino)}</td>
                <td className={`py-2.5 px-2 text-right font-mono ${(r.sortino_lift ?? 0) > 0.005 ? 'text-pos-400' : 'text-fg-subtle'}`}>
                  {fmtSigned(r.sortino_lift)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className="text-xs text-fg-subtle mt-3 leading-relaxed">
        Sorted by correlation (lowest first). <span className="text-fg-faint">*</span> Each row is computed over
        that ETF&apos;s own overlap window using <span className="font-medium">monthly</span> returns (needed to align with
        ETF prices), so <span className="font-medium">Strat Sharpe/Sortino</span> — the strategy alone over that window —
        is the baseline the blend is measured against, and <span className="font-medium">Δ = blend − Strat</span> on the
        same window. These won&apos;t match the headline above, which is the backtest&apos;s reported (daily-based) figure
        over its full history — both because an ETF may have less history (e.g. launched 2019) and because monthly vs
        daily returns annualize differently. A 0% mix means no allocation improved the objective, so Δ = 0.
      </p>
    </div>
  );
}
