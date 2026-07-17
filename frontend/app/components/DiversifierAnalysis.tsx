'use client';

import { Fragment, useState } from 'react';
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, Legend,
} from 'recharts';
import { chartTheme } from '../../lib/chartTheme';
import LoadingDots from './LoadingDots';
import BenchmarkSection from './diversifier/BenchmarkSection';
import ManualPortfolioSection from './diversifier/ManualPortfolioSection';
import SavedPortfoliosSection from './diversifier/SavedPortfoliosSection';
import { useDiversifier } from './diversifier/useDiversifier';
import { useFxCurrencies } from '../../lib/hooks/apiData';
import type { AssetWeight, CorrelationResponse, DiversifierResult, DrawdownInfo, OptimizeResponse } from '../../lib/types/api';

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
  const fxCurrencies = useFxCurrencies();

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
            <div>
              <label className="text-fg-subtle text-xs block mb-1" title="The optimizer searches the strategy weight over this range on a 2.5% grid (the diversifier sleeve = 100 − strategy is split among the funds, also in 2.5% steps). Set min = max to pin it.">
                Strategy weight % (min–max)
              </label>
              <div className="flex items-center gap-1.5">
                <input
                  type="number"
                  step="2.5"
                  min={0}
                  max={100}
                  value={d.coreMinPct}
                  onChange={(e) => d.setCoreMinPct(Number(e.target.value))}
                  className="w-20 bg-page border border-neutral-700 rounded-lg px-3 py-2 text-fg-strong text-sm font-mono focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none"
                />
                <span className="text-fg-subtle text-xs">to</span>
                <input
                  type="number"
                  step="2.5"
                  min={0}
                  max={100}
                  value={d.coreMaxPct}
                  onChange={(e) => d.setCoreMaxPct(Number(e.target.value))}
                  className="w-20 bg-page border border-neutral-700 rounded-lg px-3 py-2 text-fg-strong text-sm font-mono focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none"
                />
              </div>
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
            <div>
              <label className="text-fg-subtle text-xs block mb-1" title="An index/ETF to compare the optimized portfolio against over the same window — its per-year return + vol and overall Sharpe/Sortino show alongside. Not part of the portfolio.">
                Compare vs
              </label>
              <select
                value={d.compareBenchmarkId ?? ''}
                onChange={(e) => d.setCompareBenchmarkId(e.target.value ? Number(e.target.value) : null)}
                className="bg-page border border-neutral-700 rounded-lg px-3 py-2 text-fg-strong text-sm focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none max-w-[14rem]"
              >
                <option value="">None</option>
                {d.etfs.map((e) => (
                  <option key={e.benchmark_id} value={e.benchmark_id}>{e.ticker} · {e.name}</option>
                ))}
              </select>
            </div>
            <div className="w-28">
              <label className="text-fg-subtle text-xs block mb-1" title="How many seeded restarts the optimizer runs per strategy-weight step. Higher = lower chance of missing the global optimum, but slower (~1s each with many ETFs). Deterministic for a given value — same settings always give the same result.">
                Search restarts
              </label>
              <input
                type="number"
                step="1"
                min={1}
                max={32}
                value={d.searchRestarts}
                onChange={(e) => d.setSearchRestarts(Math.max(1, Math.min(32, Math.round(Number(e.target.value)) || 1)))}
                className="w-full bg-page border border-neutral-700 rounded-lg px-3 py-2 text-fg-strong text-sm font-mono focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none"
              />
            </div>
            <div className="w-24">
              <label className="text-fg-subtle text-xs block mb-1" title="RNG seed for the optimizer's random restarts. Fixed → fully reproducible (same settings = same result). Change it to probe whether a different starting point finds a better solution — at the default restarts it almost never does.">
                Seed
              </label>
              <input
                type="number"
                step="1"
                min={0}
                value={d.searchSeed}
                onChange={(e) => d.setSearchSeed(Math.max(0, Math.round(Number(e.target.value)) || 0))}
                className="w-full bg-page border border-neutral-700 rounded-lg px-3 py-2 text-fg-strong text-sm font-mono focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none"
              />
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
              title="Optimize the diversifier sleeve around your strategy, with a drift-rebalance band"
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

        {/* Shared history filter. */}
        <div className="flex items-center gap-2">
          <label className="flex items-center gap-1.5 text-xs text-fg-muted" title="Keep only funds whose history starts before Jan 1 of this year — young funds shorten the optimizer's common window">
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
        </div>

        {/* ── Funds (ETFs & bonds, treated the same) ────────────────── */}
        <BenchmarkSection
          title="Funds (ETFs & bonds)"
          hint="Add any GuruFocus-priced fund by ticker; all selected funds are optimized into the diversifier sleeve."
          rows={d.visibleEtfs}
          selectedIds={d.selectedEtfIds}
          onToggle={d.toggleEtf}
          onSelectAll={d.toggleSelectAll}
          accent="accent"
          onAdd={(t, isin) => d.addEtf(t, { isin })}
          adding={d.adding}
          addPlaceholder="e.g. GLD"
          onRefresh={d.refreshEtf}
          onDelete={d.deleteEtf}
          onSetIsin={d.setBenchmarkIsin}
          onSetCurrency={d.setBenchmarkCurrency}
          currencyOptions={fxCurrencies}
          busyId={d.busyEtfId}
          emptyText={d.cutoffYear ? `No funds with history before ${d.cutoffYear}.` : 'No funds yet — add one above.'}
          loading={d.loadingLists}
        />

        {/* ── Portfolio optimization ────────────────────────────────── */}
        {d.optimizeResult && d.compareBenchmarkId != null && d.optimizeResult.benchmark?.benchmark_id !== d.compareBenchmarkId && (
          <div className="rounded-lg bg-warn-500/10 border border-warn-500/20 px-4 py-2.5 text-sm text-warn-400">
            Click <span className="font-medium">Optimize portfolio</span>{' '}to apply the selected benchmark comparison.
          </div>
        )}
        {d.optimizeResult && <OptimizeCard result={d.optimizeResult} onSetIsin={d.setBenchmarkIsin} />}

        {/* ── Manual portfolio backtest (below the optimizer) ───────── */}
        {d.selectedRunId != null && (
          <ManualPortfolioSection
            funds={d.etfs.filter((e) => d.selectedEtfIds.has(e.benchmark_id))}
            weights={d.manualWeights}
            defaults={d.manualDefaults}
            onChange={d.setManualField}
            onReset={d.resetManualWeights}
            onRun={d.runSimulate}
            running={d.simulating}
            canRun={!d.simulating}
            onSave={d.savePortfolio}
            saving={d.savingPortfolio}
            scheduleFreq={d.scheduleFreq}
            onScheduleFreqChange={d.setScheduleFreq}
            onScheduleAsNew={d.scheduleAsStrategy}
            availableFunds={d.etfs.filter((e) => !d.selectedEtfIds.has(e.benchmark_id))}
            onAddFund={d.toggleEtf}
            onAddByTicker={(t) => d.addEtf(t, { select: true })}
            adding={d.adding}
          />
        )}
        {d.manualResult && <OptimizeCard result={d.manualResult} title="Portfolio backtest · manual weights" onSetIsin={d.setBenchmarkIsin} />}

        {/* ── Saved portfolios (named overlays, on-demand state) ────── */}
        <SavedPortfoliosSection
          portfolios={d.savedPortfolios}
          state={d.portfolioState}
          onView={d.viewPortfolioState}
          onDelete={d.deletePortfolio}
        />

        {/* ── Per-ETF correlation results ───────────────────────────── */}
        {d.result && <ResultsCard result={d.result} />}
      </div>
    </div>
  );
}

function StatPair({ label, before, after, bench, benchLabel, dp = 2 }: { label: string; before: number | null | undefined; after: number | null | undefined; bench?: number | null; benchLabel?: string; dp?: number }) {
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
      {benchLabel && (
        <span className="font-mono text-xs text-warn-400/90 mt-0.5" title={`${benchLabel} (benchmark)`}>
          {benchLabel} {fmtNum(bench, dp)}
        </span>
      )}
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

function WeightBreakdown({ weights, onSetIsin }: {
  weights: AssetWeight[];
  /** Save an ISIN to a result ETF's benchmark (PATCH /api/benchmarks/{id}).
   * When provided, ETF rows get an inline ISIN editor. */
  onSetIsin?: (benchmarkId: number, isin: string) => void;
}) {
  // Hide the funds the optimizer gave (effectively) zero weight — they only
  // clutter the allocation. Threshold matches the 1-decimal display, so a row
  // is hidden exactly when it would render as "0.0%".
  const shown = weights.filter((w) => w.weight * 100 >= 0.05);
  const hidden = weights.length - shown.length;
  const sorted = [...shown].sort((a, b) => b.weight - a.weight);
  return (
    <div className="space-y-1.5">
      {sorted.map((w) => (
        <div key={w.label} className="flex items-center gap-3">
          <div className="w-28 shrink-0 text-sm">
            <span className={`font-mono ${w.group === 'strategy' ? 'text-accent-400 font-medium' : 'text-fg-strong'}`}>{w.label}</span>
          </div>
          <div className="flex-1 h-5 bg-inset rounded overflow-hidden">
            <div className={`h-full ${w.group === 'strategy' ? 'bg-accent-500/70' : 'bg-pos-500/50'}`} style={{ width: `${Math.max(w.weight * 100, w.weight > 0 ? 1 : 0)}%` }} />
          </div>
          <div className="w-14 shrink-0 text-right font-mono text-sm text-fg-strong">{(w.weight * 100).toFixed(1)}%</div>
          {/* ISIN — editable for ETF rows (writes to the benchmark in the DB) */}
          <div className="hidden sm:block w-36 shrink-0">
            {w.benchmark_id != null && onSetIsin ? (
              <input
                key={`${w.benchmark_id}-${w.isin ?? ''}`}
                defaultValue={w.isin ?? ''}
                placeholder="add ISIN"
                title="Edit this ETF's ISIN — saved to its benchmark on Enter / blur"
                className="w-full bg-page border border-neutral-700 rounded px-2 py-0.5 text-xs font-mono text-fg-strong focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none placeholder-fg-faint"
                onKeyDown={(ev) => { if (ev.key === 'Enter') (ev.target as HTMLInputElement).blur(); }}
                onBlur={(ev) => {
                  const v = ev.target.value.trim().toUpperCase();
                  if (v !== (w.isin ?? '')) onSetIsin(w.benchmark_id as number, v);
                }}
              />
            ) : (
              <span className="text-xs font-mono text-fg-faint">{w.isin ?? ''}</span>
            )}
          </div>
          <div className="hidden lg:block w-48 shrink-0 truncate text-xs text-fg-subtle">{w.name ?? ''}</div>
        </div>
      ))}
      {hidden > 0 && (
        <p className="text-xs text-fg-subtle pt-1">
          {hidden} fund{hidden === 1 ? '' : 's'} at 0% hidden
        </p>
      )}
    </div>
  );
}

/** Cumulative return % (from window start) of a benchmark's monthly returns,
 * aligned to `dates` — flat across any date the benchmark lacks. Module-level so
 * the running product isn't a render-time mutation. */
function benchCumulativePct(dates: string[], monthly: Record<string, number>): number[] {
  const out: number[] = [];
  let g = 1;
  for (const d of dates) {
    const br = monthly[d];
    if (br != null) g *= 1 + br;
    out.push((g - 1) * 100);
  }
  return out;
}

function OptimizeCard({ result: r, title, onSetIsin }: {
  result: OptimizeResponse;
  title?: string;
  onSetIsin?: (benchmarkId: number, isin: string) => void;
}) {
  const objLabel = r.objective === 'sortino' ? 'Sortino' : 'Sharpe';
  const heading = title ?? `Optimized portfolio · maximizing ${objLabel}`;
  // Log scale plots the growth multiple (1 + cum%/100) — always positive — so
  // the early years stay visible against a strategy that's up many-fold.
  const [logScale, setLogScale] = useState(true);
  // Track EXPANDED years (empty by default → all years collapsed; click a
  // year to reveal its months, matching the section hint).
  const [expandedYears, setExpandedYears] = useState<Set<number>>(new Set());
  const toggleYear = (year: number) =>
    setExpandedYears((prev) => {
      const next = new Set(prev);
      if (next.has(year)) next.delete(year);
      else next.add(year);
      return next;
    });
  // Compare-benchmark lookups (per-year stats, per-month returns, drawdowns).
  const bench = r.benchmark ?? null;
  const benchYear = new Map((bench?.annual ?? []).map((y) => [y.year, y]));
  const benchMonthly = bench?.monthly ?? {};
  const seriesLabel = (name: string) =>
    name === 'before' || name === 'beforeG' ? 'Strategy alone'
    : name === 'bench' || name === 'benchG' ? (bench?.ticker ?? 'Benchmark')
    : 'Optimized';
  // Equity curve: compound the benchmark's monthly returns along the same dates
  // as before/after (flat across any month it lacks data) so all three align.
  const benchCumPct = bench ? benchCumulativePct(r.curve.map((p) => p.date), benchMonthly) : [];
  const chartData = r.curve.map((p, i) => ({
    date: p.date,
    before: p.before,
    after: p.after,
    bench: bench ? benchCumPct[i] : undefined,
    beforeG: 1 + p.before / 100,
    afterG: 1 + p.after / 100,
    benchG: bench ? 1 + benchCumPct[i] / 100 : undefined,
  }));
  return (
    <div className="bg-card rounded-xl border border-accent-500/30 p-5">
      <div className="flex items-baseline justify-between mb-3">
        <h3 className="text-accent-400 text-xs font-medium uppercase tracking-wider">
          {heading}
        </h3>
        <span className="text-fg-subtle text-xs">
          {r.months} mo · {r.period_from}–{r.period_to}
          {r.limited_by && <span className="text-warn-400"> · window limited by {r.limited_by}</span>}
        </span>
      </div>

      {/* Before → after (→ benchmark). Each stat shows strategy → optimized, with
          the compare-benchmark value beneath when one is selected. */}
      <div className="flex flex-wrap gap-x-10 gap-y-3 mb-5">
        <StatPair label="Sharpe" before={r.before.sharpe} after={r.after.sharpe} bench={bench?.stats.sharpe} benchLabel={bench?.ticker} />
        <StatPair label="Sortino" before={r.before.sortino} after={r.after.sortino} bench={bench?.stats.sortino} benchLabel={bench?.ticker} />
        <StatPair label="Ann. return" before={r.before.ann_return != null ? r.before.ann_return * 100 : null} after={r.after.ann_return != null ? r.after.ann_return * 100 : null} bench={bench?.stats.ann_return != null ? bench.stats.ann_return * 100 : null} benchLabel={bench?.ticker} dp={1} />
        <StatPair label="Vol" before={r.before.ann_vol != null ? r.before.ann_vol * 100 : null} after={r.after.ann_vol != null ? r.after.ann_vol * 100 : null} bench={bench?.stats.ann_vol != null ? bench.stats.ann_vol * 100 : null} benchLabel={bench?.ticker} dp={1} />
        <StatPair label="YTD" before={r.ytd_before != null ? r.ytd_before * 100 : null} after={r.ytd_after != null ? r.ytd_after * 100 : null} bench={bench?.ytd != null ? bench.ytd * 100 : null} benchLabel={bench?.ticker} dp={1} />
        <StatPair label="Median month" before={r.before.median_month != null ? r.before.median_month * 100 : null} after={r.after.median_month != null ? r.after.median_month * 100 : null} bench={bench?.stats.median_month != null ? bench.stats.median_month * 100 : null} benchLabel={bench?.ticker} dp={2} />
        <StatPair label="Win rate (mo)" before={r.before.win_rate != null ? r.before.win_rate * 100 : null} after={r.after.win_rate != null ? r.after.win_rate * 100 : null} bench={bench?.stats.win_rate != null ? bench.stats.win_rate * 100 : null} benchLabel={bench?.ticker} dp={0} />
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
                  return [`${pct >= 0 ? '+' : ''}${pct.toFixed(1)}%`, seriesLabel(String(name))];
                }}
              />
              <Legend
                wrapperStyle={{ fontSize: 12, color: chartTheme.axisLabel }}
                formatter={(v) => seriesLabel(String(v))}
              />
              <Line type="monotone" dataKey={logScale ? 'beforeG' : 'before'} stroke={chartTheme.universe} strokeWidth={1.5} strokeDasharray="4 3" dot={false} name={logScale ? 'beforeG' : 'before'} />
              {bench && <Line type="monotone" dataKey={logScale ? 'benchG' : 'bench'} stroke={chartTheme.warn} strokeWidth={1.5} strokeDasharray="2 2" dot={false} name={logScale ? 'benchG' : 'bench'} connectNulls />}
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
                  {bench && <th className="text-right font-medium py-1.5 px-2 text-warn-400/90">Return {bench.ticker}</th>}
                  <th className="text-right font-medium py-1.5 px-2 border-l border-neutral-800/40">Vol strat</th>
                  <th className="text-right font-medium py-1.5 px-2 text-accent-400/80">Vol opt</th>
                  {bench && <th className="text-right font-medium py-1.5 pl-2 text-warn-400/90">Vol {bench.ticker}</th>}
                </tr>
              </thead>
              <tbody>
                {r.annual.map((y) => {
                  const open = expandedYears.has(y.year);
                  return (
                    <Fragment key={y.year}>
                      <tr className="border-b border-neutral-800/40 hover:bg-overlay/[0.02] cursor-pointer" onClick={() => toggleYear(y.year)}>
                        <td className="py-1.5 pr-2 font-mono text-fg-strong">
                          <span className="inline-block w-3 text-fg-subtle">{open ? '▾' : '▸'}</span>{y.year}
                        </td>
                        <td className={`py-1.5 px-2 text-right font-mono ${pnlClass(y.return_before)}`}>{fmtSignedPct(y.return_before)}</td>
                        <td className={`py-1.5 px-2 text-right font-mono ${pnlClass(y.return_after)}`}>{fmtSignedPct(y.return_after)}</td>
                        {bench && <td className={`py-1.5 px-2 text-right font-mono ${pnlClass(benchYear.get(y.year)?.ret ?? null)}`}>{fmtSignedPct(benchYear.get(y.year)?.ret ?? null)}</td>}
                        <td className="py-1.5 px-2 text-right font-mono text-fg-muted border-l border-neutral-800/40">{y.vol_before != null ? `${(y.vol_before * 100).toFixed(1)}%` : '—'}</td>
                        <td className="py-1.5 px-2 text-right font-mono text-fg-muted">{y.vol_after != null ? `${(y.vol_after * 100).toFixed(1)}%` : '—'}</td>
                        {bench && <td className="py-1.5 pl-2 text-right font-mono text-warn-400/90">{(() => { const v = benchYear.get(y.year)?.vol; return v != null ? `${(v * 100).toFixed(1)}%` : '—'; })()}</td>}
                      </tr>
                      {open && (y.months ?? []).map((m) => (
                        <tr key={m.month} className="border-b border-neutral-800/20 bg-overlay/[0.015]">
                          <td className="py-1 pr-2 pl-5 font-mono text-xs text-fg-subtle">{m.month}</td>
                          <td className={`py-1 px-2 text-right font-mono text-xs ${pnlClass(m.return_before)}`}>{fmtSignedPct(m.return_before)}</td>
                          <td className={`py-1 px-2 text-right font-mono text-xs ${pnlClass(m.return_after)}`}>{fmtSignedPct(m.return_after)}</td>
                          {bench && <td className={`py-1 px-2 text-right font-mono text-xs ${pnlClass(benchMonthly[m.month] ?? null)}`}>{fmtSignedPct(benchMonthly[m.month] ?? null)}</td>}
                          <td className="border-l border-neutral-800/40"></td>
                          <td></td>
                          {bench && <td></td>}
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

      {/* Weight breakdown, grouped: Core (strategy + bonds) then Diversifiers */}
      <WeightBreakdown weights={r.weights} onSetIsin={onSetIsin} />

      {/* Top-40 worst drawdowns: strategy alone vs optimized (vs benchmark) */}
      {(r.drawdowns_before.length > 0 || r.drawdowns_after.length > 0 || (bench?.drawdowns?.length ?? 0) > 0) && (
        <div className="mt-6">
          <div className="text-fg-subtle text-xs uppercase tracking-wider mb-2">Top 40 worst drawdowns</div>
          <div className={`grid grid-cols-1 ${bench ? 'lg:grid-cols-3' : 'lg:grid-cols-2'} gap-x-6 gap-y-4`}>
            <DrawdownTable title="Strategy alone" rows={r.drawdowns_before} />
            <DrawdownTable title="Optimized" rows={r.drawdowns_after} accent />
            {bench && <DrawdownTable title={`${bench.ticker} (benchmark)`} rows={bench.drawdowns ?? []} />}
          </div>
        </div>
      )}

      {/* Rebalance cadence */}
      <div className="mt-4 rounded-lg bg-inset/60 border border-neutral-800/40 px-4 py-3 text-sm">
        {r.objective === 'manual' ? (
          (r.rebalance_count ?? 0) > 0 ? (
            <span className="text-fg">
              Reset to target <span className="font-mono text-fg-strong">{r.rebalance_count}×</span> over {r.months} months
              {r.rebalance_freq_months != null && <> — about <span className="font-mono text-fg-strong">once every {r.rebalance_freq_months.toFixed(1)} months</span></>}.
              {(r.rebalance_dates ?? []).length > 0 && (
                <span className="block text-xs text-fg-subtle mt-1 font-mono">{(r.rebalance_dates ?? []).join(' · ')}</span>
              )}
            </span>
          ) : (
            <span className="text-fg-muted">No holding left its band over this window — no rebalances needed.</span>
          )
        ) : (
          <span className="text-fg">Rebalanced <span className="font-medium text-fg-strong">monthly</span> back to the target weights — weights drift with prices during the month, then reset to target each month.</span>
        )}
      </div>

      <p className="text-xs text-fg-subtle mt-3 leading-relaxed">
        {r.objective === 'manual' ? (
          <>These are your hand-set target weights, reset whenever any holding drifts outside its band.{' '}
            <span className="font-medium">After</span> is that band-rebalanced portfolio</>
        ) : (
          <>The strategy weight is searched within your min–max range; the diversifier sleeve (the rest) is split {objLabel}-optimally
            across the selected funds (every weight a multiple of 2.5%), and the portfolio is rebalanced back to those weights every month.{' '}
            <span className="font-medium">After</span> is that monthly-rebalanced blend</>
        )}{' '}
        over the common window where every selected fund has data{r.limited_by ? ` (bounded by ${r.limited_by})` : ''};
        {' '}<span className="font-medium">Before</span>{' '}is your strategy alone.
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
        Sorted by correlation (lowest first). <span className="text-fg-faint">*</span>{' '}Each row is computed over
        that ETF&apos;s own overlap window using <span className="font-medium">monthly</span>{' '}returns (needed to align with
        ETF prices), so <span className="font-medium">Strat Sharpe/Sortino</span> — the strategy alone over that window —
        is the baseline the blend is measured against, and <span className="font-medium">Δ = blend − Strat</span>{' '}on the
        same window. These won&apos;t match the headline above, which is the backtest&apos;s reported (daily-based) figure
        over its full history — both because an ETF may have less history (e.g. launched 2019) and because monthly vs
        daily returns annualize differently. A 0% mix means no allocation improved the objective, so Δ = 0.
      </p>
    </div>
  );
}
