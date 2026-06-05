'use client';

import { useCallback, useEffect, useRef, useState } from 'react';
import dynamic from 'next/dynamic';
import LoadingDots from './LoadingDots';
import DatePartsPicker from './DatePartsPicker';
import { apiFetch } from '../../lib/apiFetch';
import { StrategyConfigDetail } from './schedule/StrategyConfigDetail';
import type { IngestRun } from './schedule/types';

import { API_URL } from '../../lib/apiUrl';

// Recharts is ~100KB gzipped — only ship it when a strategy detail
// actually opens AND has a linked backtest_run_id.
const SourceBacktestCard = dynamic(() => import('./SourceBacktestCard'), { ssr: false });

type RunHistoryEntry = {
  snapshot_id: number;
  created_at: string;
  as_of_date: string;
  latest_price_date: string | null;
  holdings_count: number;
  kind: 'rebalance' | 'price_update' | null;
  is_backfill: boolean;
  period_return_pct: number | null;
  sector_counts: Record<string, number>;
  ingest_run: IngestRun | null;
};

type BackfillState = {
  status: 'running' | 'done' | 'error' | null;
  progress_pct: number | null;
  message: string | null;
  error: string | null;
  started_at: string | null;
  finished_at: string | null;
};

export type StrategyRunHistory = {
  id: number;
  name: string;
  frequency: string | null;
  config: Record<string, unknown>;
  enabled: boolean;
  created_at: string;
  /** Configurable go-live date (YYYY-MM-DD). Red dashed marker on the
   * source-backtest equity curve. Null → defaults to created_at. */
  start_date: string | null;
  last_run_at: string | null;
  next_due_at: string | null;
  backfill: BackfillState | null;
  /** When this scheduled strategy was created from a /backtest variant,
   * the variant's BacktestResult was persisted as a backtest_run before
   * the scheduled_strategy row was inserted. The detail panel fetches
   * that result and renders its equity curve / monthly history on
   * expansion. Null for strategies added manually via /schedule. */
  backtest_run_id: number | null;
  runs: RunHistoryEntry[];
};

/** Per-strategy expanded view: strategy params + the source-backtest
 * equity curve (with the go-live marker). */
export default function ScheduledStrategyDetail({
  strategyId,
  initialData,
  onLoaded,
}: {
  strategyId: number;
  /** Parent-supplied cache hit. When non-null we render immediately and
   * fetch silently in the background (stale-while-revalidate). */
  initialData?: StrategyRunHistory | null;
  /** Called after every successful fetch so the parent can update its
   * cache. */
  onLoaded?: (data: StrategyRunHistory) => void;
  onMutated?: () => void;
}) {
  const [data, setData] = useState<StrategyRunHistory | null>(initialData ?? null);
  // Only show the spinner on a cold load. Cache hits render the previous
  // payload immediately while the silent revalidate runs.
  const [loading, setLoading] = useState(initialData == null);
  const [error, setError] = useState<string | null>(null);
  // Latest onLoaded held in a ref so its identity changing on every
  // parent render doesn't invalidate `load` and re-trigger the fetch
  // effect — that would loop, since onLoaded mutates parent state.
  const onLoadedRef = useRef(onLoaded);
  useEffect(() => { onLoadedRef.current = onLoaded; }, [onLoaded]);
  const [showConfig, setShowConfig] = useState(false);
  const [savingStartDate, setSavingStartDate] = useState(false);

  const load = useCallback(async () => {
    try {
      const r = await fetch(`${API_URL}/api/scheduled-strategies/${strategyId}/runs?limit=100`);
      if (!r.ok) {
        setError(`Failed to load strategy detail (${r.status})`);
        // Clear data so the polling effect below sees a non-running
        // status and stops firing. Otherwise a deleted strategy (404)
        // keeps its stale `backfill.status='running'` indefinitely and
        // the interval hammers the endpoint forever.
        setData(null);
        return;
      }
      const body = (await r.json()) as StrategyRunHistory;
      setData(body);
      setError(null);
      onLoadedRef.current?.(body);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, [strategyId]);

  useEffect(() => {
    void load();
  }, [load]);

  // Poll while the backfill is running so the progress bar advances
  // in near real-time. Stops as soon as status moves to 'done' / 'error'
  // — or when `data` is cleared (e.g. on 404 from a deleted strategy).
  useEffect(() => {
    if (data?.backfill?.status !== 'running') return;
    const id = setInterval(() => { void load(); }, 2000);
    return () => clearInterval(id);
  }, [data?.backfill?.status, load]);

  // Persist the configurable go-live date. Empty string clears it (falls
  // back to created_at). PATCHes then reloads so the marker re-derives
  // from the authoritative server value.
  const saveStartDate = async (value: string) => {
    setSavingStartDate(true);
    try {
      const body = value
        ? { start_date: value }
        : { clear_start_date: true };
      const r = await apiFetch(`${API_URL}/api/scheduled-strategies/${strategyId}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      if (r.ok) await load();
    } finally {
      setSavingStartDate(false);
    }
  };

  if (loading) {
    return <div className="px-5 py-4 bg-sidebar text-xs text-fg-subtle border-t border-neutral-800/30"><LoadingDots label="Loading strategy" /></div>;
  }
  if (error) {
    return <div className="px-5 py-4 bg-sidebar text-xs text-neg-300 border-t border-neutral-800/30">{error}</div>;
  }
  if (!data) return null;

  // Go-live cutoff (YYYY-MM-DD): the configured start_date, or the
  // strategy's scheduled date when unset.
  const effectiveStart = (data.start_date ?? data.created_at).slice(0, 10);

  return (
    <div className="px-5 py-4 bg-sidebar border-t border-neutral-800/30 space-y-4">
      {/* Strategy params (collapsible — verbose enough to want to hide
          unless the user is checking what's actually scheduled). */}
      <div>
        <button
          type="button"
          onClick={() => setShowConfig((v) => !v)}
          className="text-xs text-fg-muted hover:text-fg-strong transition-colors mb-2"
        >
          {showConfig ? '▾' : '▸'} Strategy params
        </button>
        {showConfig && data.config && <StrategyConfigDetail cfg={data.config} />}
      </div>

      {/* Backfill progress bar — visible while the backfill is in
          flight (status='running'), shows the engine's live message
          + percentage. Hidden once the backfill lands or errors. */}
      {data.backfill && data.backfill.status === 'running' && (
        <div className="bg-accent-500/5 border border-accent-500/20 rounded-lg px-4 py-3 space-y-2">
          <div className="flex items-center justify-between gap-3 text-xs">
            <span className="text-accent-300 font-medium">Running backfill…</span>
            <span className="text-accent-300/80 font-mono">{data.backfill.progress_pct ?? 0}%</span>
          </div>
          <div className="h-1 bg-accent-500/15 rounded-full overflow-hidden">
            <div
              className="h-full bg-accent-500 transition-all duration-300"
              style={{ width: `${data.backfill.progress_pct ?? 0}%` }}
            />
          </div>
          {data.backfill.message && (
            <div className="text-[11px] text-accent-200/70 font-mono truncate" title={data.backfill.message}>
              {data.backfill.message}
            </div>
          )}
        </div>
      )}

      {data.backfill && data.backfill.status === 'error' && (
        <div className="bg-neg-500/10 border border-neg-500/20 rounded-lg px-4 py-3 text-xs text-neg-300">
          <div className="font-medium mb-0.5">Backfill failed</div>
          <div className="font-mono whitespace-pre-wrap">{data.backfill.error ?? 'Unknown error'}</div>
        </div>
      )}

      {/* Go-live date editor. Drives the red dashed marker on the equity
          curve below. Defaults to the strategy's created_at when never set. */}
      {data.backtest_run_id != null && (() => {
        const isCustom = data.start_date != null;
        return (
          <>
            <div className="flex items-center flex-wrap gap-x-3 gap-y-1.5 text-xs">
              <span className="text-fg-muted">Go-live date</span>
              <DatePartsPicker
                value={effectiveStart}
                onChange={(iso) => { if (iso) void saveStartDate(iso); }}
                minYear={2002}
                maxYear={new Date().getUTCFullYear() + 1}
              />
              <span className="inline-flex items-center gap-1.5 text-[11px] text-fg-subtle">
                <span className="inline-block w-3 border-t-2 border-dashed border-neg-400" />
                marker on the curve
              </span>
              {!isCustom && (
                <span className="text-[11px] text-fg-faint">defaults to scheduled date</span>
              )}
              {isCustom && (
                <button
                  type="button"
                  disabled={savingStartDate}
                  onClick={() => void saveStartDate('')}
                  className="text-[11px] text-fg-subtle hover:text-fg-soft underline disabled:opacity-50"
                >
                  reset
                </button>
              )}
              {savingStartDate && <span className="text-[11px] text-accent-300">saving…</span>}
            </div>

            {/* Source backtest — the variant's full equity curve, sector
                timeline + per-month holdings, exactly as on /backtest, with
                the red dashed go-live marker at the date above. */}
            <SourceBacktestCard runId={data.backtest_run_id} markerDate={effectiveStart} />
          </>
        );
      })()}
    </div>
  );
}
