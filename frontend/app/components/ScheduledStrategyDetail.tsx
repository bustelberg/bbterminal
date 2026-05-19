'use client';

import { useCallback, useEffect, useState } from 'react';
import SnapshotHoldings from './SnapshotHoldings';
import { StrategyConfigDetail, type IngestRun } from './Schedule';

const API_URL = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000';

type RunHistoryEntry = {
  snapshot_id: number;
  created_at: string;
  as_of_date: string;
  latest_price_date: string | null;
  holdings_count: number;
  ingest_run: IngestRun | null;
};

type StrategyRunHistory = {
  id: number;
  backtest_run_id: number;
  enabled: boolean;
  created_at: string;
  backtest_name: string | null;
  backtest_config: Record<string, unknown> | null;
  runs: RunHistoryEntry[];
};

function fmtTimestamp(iso: string | null): string {
  if (!iso) return '—';
  try {
    return new Date(iso).toLocaleString(undefined, {
      year: 'numeric', month: 'short', day: '2-digit',
      hour: '2-digit', minute: '2-digit',
    });
  } catch {
    return iso;
  }
}

function StatusBadge({ status }: { status: 'running' | 'ok' | 'error' }) {
  const cls = status === 'ok'
    ? 'bg-emerald-500/15 text-emerald-300 border-emerald-500/30'
    : status === 'error'
      ? 'bg-rose-500/10 text-rose-300 border-rose-500/30'
      : 'bg-amber-500/15 text-amber-300 border-amber-500/30';
  return (
    <span className={`inline-flex items-center text-[10px] uppercase tracking-wider px-1.5 py-0.5 rounded border ${cls}`}>
      {status}
    </span>
  );
}

/** Per-strategy expanded view: shows the strategy's params + the list of
 * pipeline runs that produced a snapshot for it. Each row is clickable
 * — click to expand the snapshot's holdings inline. */
export default function ScheduledStrategyDetail({
  strategyId,
  onMutated: _onMutated,
}: {
  strategyId: number;
  onMutated?: () => void;
}) {
  const [data, setData] = useState<StrategyRunHistory | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [expandedSnapshotId, setExpandedSnapshotId] = useState<number | null>(null);
  const [showConfig, setShowConfig] = useState(false);

  const load = useCallback(async () => {
    try {
      const r = await fetch(`${API_URL}/api/scheduled-strategies/${strategyId}/runs?limit=100`);
      if (!r.ok) {
        setError(`Failed to load run history (${r.status})`);
        return;
      }
      const body = (await r.json()) as StrategyRunHistory;
      setData(body);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, [strategyId]);

  useEffect(() => {
    void load();
  }, [load]);

  if (loading) {
    return <div className="px-5 py-4 bg-[#0b0d13] text-xs text-gray-500 border-t border-gray-800/30">Loading run history…</div>;
  }
  if (error) {
    return <div className="px-5 py-4 bg-[#0b0d13] text-xs text-rose-300 border-t border-gray-800/30">{error}</div>;
  }
  if (!data) return null;

  return (
    <div className="px-5 py-4 bg-[#0b0d13] border-t border-gray-800/30 space-y-4">
      {/* Strategy params (collapsible — verbose enough to want to hide
          unless the user is checking what's actually scheduled). */}
      <div>
        <button
          type="button"
          onClick={() => setShowConfig((v) => !v)}
          className="text-xs text-gray-400 hover:text-white transition-colors mb-2"
        >
          {showConfig ? '▾' : '▸'} Strategy params
        </button>
        {showConfig && data.backtest_config && <StrategyConfigDetail cfg={data.backtest_config} />}
      </div>

      {/* Run history */}
      <div>
        <div className="text-[10px] uppercase tracking-wider text-gray-500 mb-2">
          Run history ({data.runs.length})
        </div>
        {data.runs.length === 0 ? (
          <div className="text-xs text-gray-500">
            No pipeline runs have computed this strategy yet. Trigger the weekly or monthly job above (or wait for the next scheduled tick).
          </div>
        ) : (
          <div className="bg-[#151821] border border-gray-800/40 rounded-lg overflow-hidden">
            <div className="divide-y divide-gray-800/30">
              {data.runs.map((entry) => {
                const isExpanded = expandedSnapshotId === entry.snapshot_id;
                const run = entry.ingest_run;
                return (
                  <div key={entry.snapshot_id}>
                    <button
                      type="button"
                      onClick={() => setExpandedSnapshotId(isExpanded ? null : entry.snapshot_id)}
                      className="w-full px-4 py-2 flex items-center gap-3 text-left hover:bg-white/[0.02] transition-colors"
                    >
                      <span className="text-gray-500 font-mono text-xs w-4 shrink-0">{isExpanded ? '▾' : '▸'}</span>
                      <span className="text-gray-300 font-mono text-xs w-40 shrink-0">
                        {fmtTimestamp(entry.created_at)}
                      </span>
                      {run && <StatusBadge status={run.status} />}
                      <span className="text-gray-400 text-xs font-mono">
                        {entry.holdings_count} holdings
                      </span>
                      <span className="text-gray-500 text-xs ml-auto font-mono">
                        as of {entry.latest_price_date ?? entry.as_of_date}
                        {run && (
                          <span className="text-gray-600">
                            {' · '}#{run.run_id} · {run.job_name}
                          </span>
                        )}
                      </span>
                    </button>
                    {isExpanded && (
                      <div className="px-4 py-3 border-t border-gray-800/30 bg-[#0f1117]">
                        <SnapshotHoldings snapshotId={entry.snapshot_id} />
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
