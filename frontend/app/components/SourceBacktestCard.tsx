'use client';

import { useEffect, useMemo, useState } from 'react';
import { API_URL } from '../../lib/apiUrl';
import { apiFetch } from '../../lib/apiFetch';
import type { BacktestResult, UniverseEntry } from '../../lib/stores/momentum';
import BacktestResultView from './momentum/BacktestResultView';
import type { ScoringConfig } from './momentum/MonthlyHoldingsTable';
import LoadingDots from './LoadingDots';

type LoadedBacktest = {
  run_id: number;
  name: string | null;
  // The strategy params the run was saved with — used to rebuild the
  // scoring config so the holdings table's per-category score columns
  // render exactly as they did on /backtest.
  config?: {
    signal_weights?: Record<string, number> | null;
    category_weights?: Record<string, number> | null;
    index_universe?: string | null;
    universe_label?: string | null;
  } | null;
  // The result blob — a BacktestResult plus the `universe` payload bundled
  // at save time (drives the per-company exchange lookup).
  result: (BacktestResult & { universe?: UniverseEntry[] }) | null;
};

/** Cumulative return of a `(date, cum%)` series from `goLive` through the
 * end, rebased: `(1+endCum)/(1+baseCum) − 1`. `baseCum` is the cum at the
 * last point on/before go-live (or the curve's first point when go-live
 * precedes the data). Returns null for an empty series. The series'
 * `cum` is the run-to-date cumulative return; rebasing isolates the
 * go-live → latest slice. */
function rebasedReturn(
  series: { date: string; cum: number }[],
  goLive: string,
): { ret: number; from: string; to: string } | null {
  if (series.length === 0) return null;
  let baseIdx = -1;
  for (let i = 0; i < series.length; i++) {
    if (series[i].date <= goLive) baseIdx = i;
    else break;
  }
  const base = baseIdx >= 0 ? series[baseIdx] : series[0];
  const end = series[series.length - 1];
  const ret = ((1 + end.cum / 100) / (1 + base.cum / 100) - 1) * 100;
  return { ret, from: base.date, to: end.date };
}

/** "Scheduled-from-backtest" panel.
 *
 * Renders the source backtest exactly as it appears on /backtest — the
 * full equity-curve card (log toggle, summary stats, yearly breakdown +
 * alpha subplots, benchmark comparison), sector timeline, and the
 * per-month holdings table — via the shared `BacktestResultView`. The
 * strategy's live forward performance is shown separately in the run-
 * history table below this card in `ScheduledStrategyDetail`.
 */
type LiveCurve = {
  cutover_date: string;
  points: { date: string; cumulative_return_pct: number }[];
  as_of_date: string;
};

export default function SourceBacktestCard({
  runId,
  markerDate,
  liveCurve,
}: {
  runId: number;
  /** "Go-live" date (YYYY-MM-DD) for the red dashed marker on the equity
   * curve — the strategy's configured start_date (or its scheduled-at
   * date as a fallback). */
  markerDate?: string;
  /** Live extension of the backtest daily curve (from /runs). When present,
   * backtest daily points before `cutover_date` are kept and `points` are
   * appended, so the equity curve + monthly-returns heatmap track the
   * latest priced day rather than ending where the backtest was saved. */
  liveCurve?: LiveCurve | null;
}) {
  const [data, setData] = useState<LoadedBacktest | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    // Reset state on every runId change so the loading panel reappears
    // and stale data clears before the next fetch resolves.
    // eslint-disable-next-line react-hooks/set-state-in-effect
    setLoading(true);
    setError(null);
    setData(null);
    apiFetch(`${API_URL}/api/momentum/backtests/${runId}`)
      .then(async (r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((body: LoadedBacktest) => {
        if (!cancelled) setData(body);
      })
      .catch((e: unknown) => {
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => { cancelled = true; };
  }, [runId]);

  // The saved result with ONLY its strategy daily curve extended by the live
  // forward tail — everything the held portfolio drives (equity curve line,
  // per-series summary stats, monthly-returns heatmap, daily-return
  // distribution, the "Since go-live" callout below) then tracks the latest
  // priced day. `monthly_records`, `universe_daily_records`, and `summary`
  // are left frozen: the holdings table + sector timeline are backtest
  // history, and the equal-weight universe baseline / alpha can't be extended
  // (that needs every company's latest price, beyond the monthly API budget).
  // The equity card lets the active line outrun the universe via
  // `activeDefinesWindowEnd`. Backtest points before the cutover are kept;
  // the live points (already on the same cumulative scale) are appended.
  const liveExtendedResult = useMemo(() => {
    const base = data?.result;
    if (!base) return base ?? null;
    const pts = liveCurve?.points ?? [];
    if (pts.length === 0) return base;
    const kept = (base.daily_records ?? []).filter(
      (d) => d.date.slice(0, 10) < liveCurve!.cutover_date,
    );
    return { ...base, daily_records: [...kept, ...pts] };
  }, [data, liveCurve]);

  // Rebuild the scoring config from the saved run's params so the holdings
  // table's category-score columns match /backtest. Empty maps are a safe
  // fallback — the columns still render from each holding's category_scores.
  const scoringConfig = useMemo<ScoringConfig>(() => ({
    universe_label: data?.config?.universe_label ?? null,
    index_universe: data?.config?.index_universe ?? null,
    signal_weights: data?.config?.signal_weights ?? {},
    category_weights: data?.config?.category_weights ?? {},
  }), [data]);

  // Performance since the go-live date. The strategy line uses the
  // live-extended curve so it tracks the latest priced day; the equal-weight
  // universe baseline comes from the frozen backtest (no live extension), so
  // alpha is computed over the window the two share (through the backtest
  // horizon) while the headline strategy return runs through the latest day.
  const sinceGoLive = useMemo(() => {
    const r = liveExtendedResult;
    if (!r || !markerDate) return null;
    const monthly = r.monthly_records ?? [];
    const daily = r.daily_records ?? [];
    const uniDaily = data?.result?.universe_daily_records ?? [];
    const frozenMonthly = data?.result?.monthly_records ?? [];
    const useDaily = daily.length > 0;
    const stratSeries = useDaily
      ? daily.map((d) => ({ date: d.date.slice(0, 10), cum: d.cumulative_return_pct }))
      : monthly.map((m) => ({ date: m.date.slice(0, 10), cum: m.cumulative_return_pct ?? 0 }));
    const uniSeries = useDaily && uniDaily.length > 0
      ? uniDaily.map((d) => ({ date: d.date.slice(0, 10), cum: d.cumulative_return_pct }))
      : frozenMonthly
          .map((m) => ({ date: m.date.slice(0, 10), cum: m.universe_cumulative_return_pct }))
          .filter((p): p is { date: string; cum: number } => p.cum != null);
    const strat = rebasedReturn(stratSeries, markerDate);
    if (!strat) return null;
    const uni = uniSeries.length > 0 ? rebasedReturn(uniSeries, markerDate) : null;
    // Window-match alpha: re-measure the strategy through the universe's last
    // day so we compare like-for-like rather than strat(→latest) − uni(→btEnd).
    let alpha: number | null = null;
    if (uni) {
      const stratThroughBench = rebasedReturn(
        stratSeries.filter((p) => p.date <= uni.to), markerDate,
      );
      alpha = stratThroughBench ? stratThroughBench.ret - uni.ret : null;
    }
    return {
      strat: strat.ret,
      uni: uni?.ret ?? null,
      alpha,
      from: strat.from,
      to: strat.to,
      // The benchmark's last day — shown when it lags the strategy's so the
      // "vs universe / alpha" window is unambiguous.
      benchTo: uni && uni.to !== strat.to ? uni.to : null,
    };
  }, [data, liveExtendedResult, markerDate]);

  // Latest priced day reflected in the live-extended views (for the heatmap
  // caption); null when there's no live extension.
  const liveThrough = (liveCurve?.points?.length ?? 0) > 0 ? liveCurve!.as_of_date : null;

  if (loading) {
    return (
      <div className="bg-card border border-neutral-800/40 rounded-lg px-4 py-3">
        <LoadingDots label="Loading source backtest" />
      </div>
    );
  }
  if (error) {
    return (
      <div className="bg-neg-500/10 border border-neg-500/20 rounded-lg px-4 py-3 text-xs text-neg-300">
        Couldn&apos;t load source backtest #{runId}: {error}
      </div>
    );
  }
  if (!data?.result || (data.result.monthly_records?.length ?? 0) === 0) {
    return (
      <div className="bg-warn-500/10 border border-warn-500/20 rounded-lg px-4 py-3 text-xs text-warn-300">
        Source backtest #{runId} loaded but has no results to display.
      </div>
    );
  }

  return (
    <div className="space-y-4">
      {sinceGoLive && (
        <div className="bg-card border border-neutral-800/40 rounded-lg px-4 py-3">
          <div className="flex flex-wrap items-center gap-x-5 gap-y-1.5">
            <div className="flex items-baseline gap-2">
              <span className="text-xs text-fg-muted font-medium">Since go-live</span>
              <span className={`font-mono font-semibold text-base ${sinceGoLive.strat >= 0 ? 'text-pos-400' : 'text-neg-400'}`}>
                {sinceGoLive.strat >= 0 ? '+' : ''}{sinceGoLive.strat.toFixed(2)}%
              </span>
            </div>
            {sinceGoLive.uni != null && (
              <div className="flex items-baseline gap-1.5 text-xs">
                <span className="text-fg-subtle">vs universe</span>
                <span className={`font-mono ${sinceGoLive.uni >= 0 ? 'text-pos-400/80' : 'text-neg-400/80'}`}>
                  {sinceGoLive.uni >= 0 ? '+' : ''}{sinceGoLive.uni.toFixed(2)}%
                </span>
                {sinceGoLive.benchTo && (
                  <span className="text-fg-faint font-mono">(→ {sinceGoLive.benchTo})</span>
                )}
              </div>
            )}
            {sinceGoLive.alpha != null && (
              <div className="flex items-baseline gap-1.5 text-xs">
                <span className="text-fg-subtle">alpha</span>
                <span className={`font-mono font-medium ${sinceGoLive.alpha >= 0 ? 'text-pos-400' : 'text-neg-400'}`}>
                  {sinceGoLive.alpha >= 0 ? '+' : ''}{sinceGoLive.alpha.toFixed(2)}%
                </span>
              </div>
            )}
            <span className="text-[11px] text-fg-subtle font-mono ml-auto">{sinceGoLive.from} → {sinceGoLive.to}</span>
          </div>
          <p className="text-[10px] text-fg-faint mt-1.5">
            Return from the go-live date through the latest priced day ({sinceGoLive.to}) — the held portfolio is marked to market by the price-update job past the backtest horizon{sinceGoLive.benchTo ? `; the universe baseline + alpha run through ${sinceGoLive.benchTo} (no live benchmark)` : ''}. Per-period live performance is in the run history below.
          </p>
        </div>
      )}
      <div className="text-[10px] uppercase tracking-wider text-fg-subtle">
        Source backtest{data.name ? ` · ${data.name}` : ''}
      </div>
      <BacktestResultView
        result={liveExtendedResult ?? data.result}
        universe={data.result.universe ?? []}
        loadedRunId={runId}
        activeStrategyLabel={data.name ?? undefined}
        scoringConfig={scoringConfig}
        markerDate={markerDate}
        liveThrough={liveThrough ?? undefined}
        activeDefinesWindowEnd={liveThrough != null}
        defaultCollapsed
      />
    </div>
  );
}
