'use client';

import { useEffect, useMemo, useState } from 'react';
import {
  CartesianGrid,
  Line,
  LineChart,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import { API_URL } from '../../lib/apiUrl';
import LoadingDots from './LoadingDots';

type PeriodRecord = {
  date: string;
  cumulative_return_pct?: number | null;
  portfolio_return_pct?: number | null;
};

/** Minimal subset of a run-history entry we need for the live-extension
 * math. Mirrors the relevant fields of `RunHistoryEntry` in
 * `ScheduledStrategyDetail.tsx` without dragging the full type in. */
type LiveSnapshot = {
  snapshot_id: number;
  as_of_date: string;
  created_at: string;
  kind: 'rebalance' | 'price_update' | null;
  is_backfill: boolean;
  period_return_pct: number | null;
  latest_price_date: string | null;
};

type Summary = {
  annualized_return_pct?: number | null;
  total_return_pct?: number | null;
  sharpe_ratio?: number | null;
  sortino_ratio?: number | null;
  max_drawdown_pct?: number | null;
};

type LoadedBacktest = {
  run_id: number;
  name: string | null;
  result: {
    summary?: Summary | null;
    monthly_records?: PeriodRecord[] | null;
  };
};

function fmtPct(v: number | null | undefined, digits = 2): string {
  if (v == null || !Number.isFinite(v)) return '—';
  return `${v >= 0 ? '+' : ''}${v.toFixed(digits)}%`;
}

function fmtNum(v: number | null | undefined, digits = 2): string {
  if (v == null || !Number.isFinite(v)) return '—';
  return v.toFixed(digits);
}

/** "Scheduled-from-backtest" panel.
 *
 * Renders the source backtest's equity curve inline with a red vertical
 * line at `scheduledAt`. Everything LEFT of the red line is the data
 * that was already computed when the user ran the variant on /backtest;
 * everything RIGHT of it would eventually be live snapshots (extending
 * the curve past the marker is a follow-up — for now the run-history
 * table below the card carries the live data with its own visual
 * styling).
 */
export default function SourceBacktestCard({
  runId,
  scheduledAt,
  liveSnapshots = [],
}: {
  runId: number;
  scheduledAt: string;
  /** Snapshots produced by the pipeline AFTER this strategy was
   * scheduled. Used to extend the equity curve + monthly returns
   * past the red "scheduled at" marker. Filtered to live rebalances
   * (non-backfill, kind=='rebalance') and chronologically sorted. */
  liveSnapshots?: LiveSnapshot[];
}) {
  const [data, setData] = useState<LoadedBacktest | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    // Reset state on every runId change so the loading panel reappears
    // and stale data clears before the next fetch resolves. React 19's
    // "no setState in effect" rule fires on the setLoading line but
    // the alternatives (useReducer dispatch, key-based remount) add
    // more noise than they save for a fire-once fetch.
    // eslint-disable-next-line react-hooks/set-state-in-effect
    setLoading(true);
    setError(null);
    setData(null);
    fetch(`${API_URL}/api/momentum/backtests/${runId}`)
      .then(async (r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((body: LoadedBacktest) => {
        if (!cancelled) setData(body);
      })
      .catch((e: unknown) => {
        if (!cancelled) {
          setError(e instanceof Error ? e.message : String(e));
        }
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => { cancelled = true; };
  }, [runId]);

  // Memoize the `?? []` fallback so the useMemo blocks below see a
  // stable reference when `data` is null (instead of a fresh `[]` on
  // every render, which trips react-hooks/exhaustive-deps).
  const monthly = useMemo<PeriodRecord[]>(
    () => data?.result?.monthly_records ?? [],
    [data],
  );
  const summary: Summary = data?.result?.summary ?? {};

  // The vertical line lands on the calendar day this strategy was
  // pinned to the schedule. The chart's x-axis uses the monthly record
  // dates (`YYYY-MM-DD`); the ReferenceLine accepts the same string.
  // We render even if the date is past the chart range — Recharts just
  // clips it.
  const scheduledAtDay = useMemo(() => scheduledAt.slice(0, 10), [scheduledAt]);

  // Chain-link live rebalance returns onto the backtest's final
  // cumulative-return baseline. The chart then plots BOTH series —
  // backtest in indigo (purple), live in emerald — meeting at the red
  // scheduled-at line. The math: live_cum[i] = backtest_cum_at_end ×
  // ∏ (1 + live_period_return[k]/100) for k=0..i. Mathematically
  // identical to how `cumulative_return_pct` was computed in the
  // backtest's monthly_records, so the curve reads continuously.
  const liveSeries = useMemo<{ date: string; cum: number; period_ret: number | null }[]>(() => {
    if (!monthly.length) return [];
    // Take only live rebalances post-schedule. Price-updates are
    // intra-period revaluations of the LAST rebalance's holdings —
    // not new "periods" — so they don't get their own chart point
    // (the open period's MTD is reflected on the latest rebalance's
    // row in the table below instead).
    const rebals = liveSnapshots
      .filter((s) => !s.is_backfill && s.kind === 'rebalance')
      .filter((s) => s.as_of_date >= scheduledAtDay)
      .sort((a, b) => a.as_of_date.localeCompare(b.as_of_date));
    const lastBacktestCum = monthly[monthly.length - 1].cumulative_return_pct ?? 0;
    let factor = 1 + lastBacktestCum / 100;
    return rebals.map((s) => {
      const ret = s.period_return_pct;
      if (ret != null && Number.isFinite(ret)) {
        factor *= 1 + ret / 100;
      }
      return {
        date: s.as_of_date,
        cum: (factor - 1) * 100,
        period_ret: ret,
      };
    });
  }, [monthly, liveSnapshots, scheduledAtDay]);

  // Combined chart data — single x-axis, two y-fields. `cum_backtest`
  // is non-null up to and including the backtest's last point; `cum_live`
  // is non-null from the backtest's last point onward. Including the
  // backtest's last point in BOTH series (as the seed of the live line)
  // makes them visually meet at the red marker instead of leaving a
  // 1-period gap.
  const chartData = useMemo(() => {
    const rows: Array<{ date: string; cum_backtest: number | null; cum_live: number | null }> = [];
    for (const r of monthly) {
      rows.push({
        date: r.date.slice(0, 10),
        cum_backtest: r.cumulative_return_pct ?? null,
        cum_live: null,
      });
    }
    if (liveSeries.length > 0 && rows.length > 0) {
      // Seed the live series at the backtest's final point so the
      // two lines kiss at the boundary.
      rows[rows.length - 1].cum_live = rows[rows.length - 1].cum_backtest;
      for (const p of liveSeries) {
        rows.push({
          date: p.date,
          cum_backtest: null,
          cum_live: p.cum,
        });
      }
    }
    return rows;
  }, [monthly, liveSeries]);

  const liveSummary = useMemo(() => {
    if (liveSeries.length === 0 || monthly.length === 0) return null;
    const lastBacktest = monthly[monthly.length - 1].cumulative_return_pct ?? 0;
    const finalLive = liveSeries[liveSeries.length - 1].cum;
    const sinceSched = ((1 + finalLive / 100) / (1 + lastBacktest / 100) - 1) * 100;
    return {
      n_periods: liveSeries.length,
      final_cum: finalLive,
      since_scheduled: sinceSched,
      first_date: liveSeries[0].date,
      last_date: liveSeries[liveSeries.length - 1].date,
    };
  }, [liveSeries, monthly]);

  if (loading) {
    return (
      <div className="bg-[#151821] border border-gray-800/40 rounded-lg px-4 py-3">
        <LoadingDots label="Loading source backtest" />
      </div>
    );
  }
  if (error) {
    return (
      <div className="bg-rose-500/10 border border-rose-500/20 rounded-lg px-4 py-3 text-xs text-rose-300">
        Couldn&apos;t load source backtest #{runId}: {error}
      </div>
    );
  }
  if (!data || monthly.length === 0) {
    return (
      <div className="bg-amber-500/10 border border-amber-500/20 rounded-lg px-4 py-3 text-xs text-amber-300">
        Source backtest #{runId} loaded but has no monthly_records.
      </div>
    );
  }

  return (
    <div className="bg-[#151821] border border-gray-800/40 rounded-lg overflow-hidden">
      <div className="px-4 py-2.5 border-b border-gray-800/40 flex items-baseline gap-3 flex-wrap">
        <div className="text-sm font-medium text-white">Source backtest</div>
        <div className="text-[11px] text-gray-500">
          The variant you scheduled on /backtest, persisted verbatim. The red line marks the day this strategy was scheduled — live pipeline snapshots produced after that date show up in the run history below.
        </div>
        <a
          href={`/backtest?load=${runId}`}
          className="ml-auto text-[11px] text-indigo-300 hover:text-indigo-200 underline"
          title="Open the full backtest view on /backtest with this run loaded"
        >
          Open in /backtest →
        </a>
      </div>

      <div className="px-4 py-3">
        <div className="h-56 w-full">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart
              data={chartData}
              margin={{ top: 8, right: 16, bottom: 0, left: 0 }}
            >
              <CartesianGrid stroke="#1f2937" strokeDasharray="3 3" />
              <XAxis
                dataKey="date"
                stroke="#4b5563"
                tick={{ fill: '#6b7280', fontSize: 10 }}
                minTickGap={32}
              />
              <YAxis
                stroke="#4b5563"
                tick={{ fill: '#6b7280', fontSize: 10 }}
                tickFormatter={(v) => `${v}%`}
                width={48}
              />
              <Tooltip
                contentStyle={{
                  backgroundColor: '#0f1117',
                  border: '1px solid #374151',
                  borderRadius: 8,
                  fontSize: 12,
                }}
                labelStyle={{ color: '#9ca3af' }}
                formatter={(value, name) => {
                  const v = typeof value === 'number' ? value : null;
                  return [
                    fmtPct(v),
                    name === 'cum_backtest' ? 'Backtest' : 'Live',
                  ];
                }}
              />
              <ReferenceLine
                x={scheduledAtDay}
                stroke="#f87171"
                strokeWidth={2}
                strokeDasharray="4 3"
                label={{
                  value: `scheduled ${scheduledAtDay}`,
                  position: 'top',
                  fill: '#fca5a5',
                  fontSize: 10,
                }}
              />
              <Line
                type="monotone"
                dataKey="cum_backtest"
                stroke="#818cf8"
                strokeWidth={2}
                dot={false}
                isAnimationActive={false}
                connectNulls
                name="cum_backtest"
              />
              <Line
                type="monotone"
                dataKey="cum_live"
                stroke="#34d399"
                strokeWidth={2.5}
                dot={{ r: 2, fill: '#34d399' }}
                isAnimationActive={false}
                connectNulls
                name="cum_live"
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
        <div className="mt-2 flex items-center gap-4 text-[11px] text-gray-500">
          <span className="inline-flex items-center gap-1.5">
            <span className="inline-block w-3 h-0.5 bg-indigo-400"></span>
            Backtest
          </span>
          <span className="inline-flex items-center gap-1.5">
            <span className="inline-block w-3 h-0.5 bg-emerald-400"></span>
            Live (post-schedule)
          </span>
          <span className="inline-flex items-center gap-1.5">
            <span className="inline-block w-3 h-px border-t border-dashed border-rose-400"></span>
            Scheduled
          </span>
        </div>

        {/* Headline stats from the saved backtest. */}
        <div className="mt-3">
          <div className="text-[10px] uppercase tracking-wider text-indigo-300/80 mb-1.5">Backtest</div>
          <div className="grid grid-cols-2 sm:grid-cols-5 gap-2 text-xs">
            <Stat label="Annualized" value={fmtPct(summary.annualized_return_pct)} positive={summary.annualized_return_pct} />
            <Stat label="Total" value={fmtPct(summary.total_return_pct)} positive={summary.total_return_pct} />
            <Stat label="Sharpe" value={fmtNum(summary.sharpe_ratio)} />
            <Stat label="Sortino" value={fmtNum(summary.sortino_ratio)} />
            <Stat label="Max DD" value={fmtPct(summary.max_drawdown_pct)} positive={summary.max_drawdown_pct} />
          </div>
        </div>

        {/* Live stats — only render once at least one post-schedule
            rebalance has landed. Three columns: # of live periods,
            return since the schedule date (clean "live performance"
            number), final cumulative across backtest+live combined. */}
        {liveSummary && (
          <div className="mt-4">
            <div className="text-[10px] uppercase tracking-wider text-emerald-300/80 mb-1.5">
              Live · {liveSummary.first_date} → {liveSummary.last_date}
            </div>
            <div className="grid grid-cols-2 sm:grid-cols-3 gap-2 text-xs">
              <Stat label="Periods" value={String(liveSummary.n_periods)} />
              <Stat label="Since scheduled" value={fmtPct(liveSummary.since_scheduled)} positive={liveSummary.since_scheduled} />
              <Stat label="Total (incl. backtest)" value={fmtPct(liveSummary.final_cum)} positive={liveSummary.final_cum} />
            </div>
          </div>
        )}

        {/* Monthly returns table. Backtest rows + live extension. The
            live rows get an emerald row + "live" badge so the cutover
            is unmistakable even at a glance. Capped at the most recent
            24 entries so a 20-year backtest doesn't paint a 240-row
            block; the user can open the saved run on /backtest for the
            full table. */}
        <div className="mt-4">
          <div className="text-[10px] uppercase tracking-wider text-gray-500 mb-1.5">
            Monthly returns (latest 24)
          </div>
          <MonthlyTable monthly={monthly} liveSeries={liveSeries} scheduledAtDay={scheduledAtDay} />
        </div>
      </div>
    </div>
  );
}

function MonthlyTable({
  monthly, liveSeries, scheduledAtDay,
}: {
  monthly: PeriodRecord[];
  liveSeries: { date: string; cum: number; period_ret: number | null }[];
  scheduledAtDay: string;
}) {
  type Row = {
    date: string;
    period_ret: number | null;
    cum: number | null;
    kind: 'backtest' | 'live';
  };
  const rows: Row[] = [
    ...monthly.map((r): Row => ({
      date: r.date.slice(0, 10),
      period_ret: r.portfolio_return_pct ?? null,
      cum: r.cumulative_return_pct ?? null,
      kind: 'backtest',
    })),
    ...liveSeries.map((s): Row => ({
      date: s.date, period_ret: s.period_ret, cum: s.cum, kind: 'live',
    })),
  ];
  const recent = rows.slice(-24);
  return (
    <div className="overflow-hidden rounded border border-gray-800/40">
      <table className="w-full text-xs">
        <thead>
          <tr className="text-[10px] text-gray-500 uppercase tracking-wider bg-[#0f1117]">
            <th className="px-3 py-1.5 text-left font-medium">Date</th>
            <th className="px-3 py-1.5 text-right font-medium">Period return</th>
            <th className="px-3 py-1.5 text-right font-medium">Cumulative</th>
            <th className="px-3 py-1.5 text-left font-medium">Source</th>
          </tr>
        </thead>
        <tbody>
          {recent.map((r) => {
            const periodCls = r.period_ret == null
              ? 'text-gray-600'
              : r.period_ret >= 0 ? 'text-emerald-400' : 'text-rose-400';
            const cumCls = r.cum == null
              ? 'text-gray-600'
              : r.cum >= 0 ? 'text-emerald-400' : 'text-rose-400';
            const rowCls = r.kind === 'live' ? 'bg-emerald-500/[0.04]' : '';
            return (
              <tr key={`${r.kind}-${r.date}`} className={`border-t border-gray-800/30 ${rowCls}`}>
                <td className="px-3 py-1.5 text-gray-300 font-mono">{r.date}</td>
                <td className={`px-3 py-1.5 text-right font-mono ${periodCls}`}>
                  {r.period_ret == null ? '—' : fmtPct(r.period_ret)}
                </td>
                <td className={`px-3 py-1.5 text-right font-mono ${cumCls}`}>
                  {r.cum == null ? '—' : fmtPct(r.cum)}
                </td>
                <td className="px-3 py-1.5">
                  {r.kind === 'live' ? (
                    <span className="text-[10px] uppercase tracking-wider px-1.5 py-0.5 rounded border bg-emerald-500/15 text-emerald-300 border-emerald-500/30">
                      live
                    </span>
                  ) : (
                    <span className="text-[10px] uppercase tracking-wider px-1.5 py-0.5 rounded border bg-indigo-500/15 text-indigo-300 border-indigo-500/30">
                      backtest
                    </span>
                  )}
                  {r.date === scheduledAtDay && (
                    <span className="ml-1.5 text-[10px] text-rose-300 font-mono">← scheduled</span>
                  )}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function Stat({
  label, value, positive,
}: {
  label: string; value: string; positive?: number | null;
}) {
  const tone = positive == null || !Number.isFinite(positive)
    ? 'text-gray-300'
    : positive >= 0 ? 'text-emerald-400' : 'text-rose-400';
  return (
    <div className="bg-[#0f1117] border border-gray-800/40 rounded px-2 py-1.5">
      <div className="text-[10px] uppercase tracking-wider text-gray-500">{label}</div>
      <div className={`text-sm font-mono ${tone}`}>{value}</div>
    </div>
  );
}
