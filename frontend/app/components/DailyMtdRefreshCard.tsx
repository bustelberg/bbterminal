'use client';

import { useCallback, useEffect, useState } from 'react';
import { apiFetch } from '../../lib/apiFetch';
import type { IngestRun } from './Schedule';
import LoadingDots from './LoadingDots';
import { API_URL } from '../../lib/apiUrl';

// Daily job fires Wed-Sat 02:00 UTC. UTC weekday: 0=Sun, 1=Mon, ... 6=Sat.
const DAILY_TICK_DAYS = new Set([3, 4, 5, 6]);

function computeNextDailyTickUtc(now: Date): Date {
  const next = new Date(Date.UTC(
    now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), 2, 0, 0, 0,
  ));
  for (let i = 0; i < 14; i++) {
    if (DAILY_TICK_DAYS.has(next.getUTCDay()) && next.getTime() > now.getTime()) {
      return next;
    }
    next.setUTCDate(next.getUTCDate() + 1);
  }
  return next;
}

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

function fmtDate(d: Date): string {
  return d.toLocaleString(undefined, {
    year: 'numeric', month: 'short', day: '2-digit',
    hour: '2-digit', minute: '2-digit',
  });
}

function statusBadgeCls(status: IngestRun['status'] | undefined): string {
  if (status === 'ok') return 'bg-emerald-500/15 text-emerald-300 border-emerald-500/30';
  if (status === 'error') return 'bg-rose-500/10 text-rose-300 border-rose-500/30';
  if (status === 'running') return 'bg-amber-500/15 text-amber-300 border-amber-500/30';
  return 'bg-gray-500/10 text-gray-400 border-gray-500/30';
}

export default function DailyMtdRefreshCard() {
  const [runs, setRuns] = useState<IngestRun[] | null>(null);
  const [triggering, setTriggering] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [expanded, setExpanded] = useState(false);

  const load = useCallback(async () => {
    try {
      const r = await fetch(
        `${API_URL}/api/ingest/runs?job_name=daily_holdings_refresh&limit=10`,
      );
      if (!r.ok) return;
      const data = (await r.json()) as IngestRun[];
      setRuns(Array.isArray(data) ? data : []);
    } catch {
      // silent — the card will just say "No runs yet"
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  // Poll every 5s while the last run is in 'running' so the UI moves
  // through the phases live. Idle otherwise.
  const lastStatus = runs?.[0]?.status;
  useEffect(() => {
    if (lastStatus !== 'running') return;
    const id = setInterval(() => void load(), 5_000);
    return () => clearInterval(id);
  }, [lastStatus, load]);

  const runNow = useCallback(async () => {
    setTriggering(true);
    setError(null);
    try {
      const r = await apiFetch(
        `${API_URL}/api/ingest/scheduled-refresh/trigger?job_name=daily_holdings_refresh`,
        { method: 'POST' },
      );
      if (!r.ok) {
        const body = await r.text().catch(() => '');
        setError(`Trigger failed: ${r.status} ${body.slice(0, 200)}`);
        return;
      }
      await load();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setTriggering(false);
    }
  }, [load]);

  const last = runs?.[0] ?? null;
  const isRunning = last?.status === 'running';
  const nextTick = !isRunning ? computeNextDailyTickUtc(new Date()) : null;

  const summaryLine = (() => {
    if (!last) return 'No runs yet — fires next Wed-Sat 02:00 UTC or click Run now';
    if (last.status === 'running') return last.current_message || 'running…';
    if (last.status === 'error') return last.error_summary || 'failed';
    const mom = last.momentum_summary || [];
    const ok = mom.filter((m) => m.status === 'ok').length;
    const err = mom.filter((m) => m.status === 'error').length;
    const denom = last.companies_total ?? '?';
    return `${ok} strateg${ok === 1 ? 'y' : 'ies'} refreshed${err > 0 ? `, ${err} failed` : ''} · ${last.companies_processed}/${denom} companies`;
  })();

  return (
    <div className="bg-[#151821] rounded-xl border border-gray-800/40">
      <div className="px-5 py-3 border-b border-gray-800/40 flex items-center justify-between gap-3">
        <button
          type="button"
          onClick={() => setExpanded((v) => !v)}
          className="flex items-center gap-3 flex-1 min-w-0 text-left"
        >
          <span className="text-gray-500 font-mono text-xs w-4 shrink-0">{expanded ? '▾' : '▸'}</span>
          <div className="flex-1 min-w-0">
            <h3 className="text-sm font-medium text-white">Daily MTD refresh</h3>
            <p className="text-xs text-gray-500 mt-0.5">
              Wed-Sat 02:00 UTC · pools held companies across all enabled strategies, refreshes prices, then persists MTD onto each strategy&apos;s latest snapshot
            </p>
          </div>
        </button>
        <div className="flex items-center gap-3 shrink-0">
          {last && (
            <span className={`inline-flex items-center text-[10px] uppercase tracking-wider px-1.5 py-0.5 rounded border ${statusBadgeCls(last.status)}`}>
              {last.status}
            </span>
          )}
          <button
            type="button"
            onClick={() => void runNow()}
            disabled={triggering || isRunning}
            className="text-xs px-3 py-1.5 rounded-lg bg-indigo-600 hover:bg-indigo-500 text-white transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {triggering ? 'Starting…' : isRunning ? 'Running…' : 'Run now'}
          </button>
        </div>
      </div>
      <div className="px-5 py-3 text-xs text-gray-400 flex items-center gap-6 flex-wrap">
        <div>
          <span className="text-gray-500">Last:</span>{' '}
          <span className="text-gray-300 font-mono">{fmtTimestamp(last?.finished_at ?? last?.started_at ?? null)}</span>
        </div>
        <div>
          <span className="text-gray-500">Next:</span>{' '}
          <span className="text-gray-300 font-mono">{nextTick ? fmtDate(nextTick) : '—'}</span>
        </div>
        <div className="text-gray-400 truncate flex-1 min-w-0" title={summaryLine}>
          {summaryLine}
        </div>
      </div>
      {error && (
        <div className="px-5 py-2 bg-rose-500/10 border-t border-rose-500/20 text-xs text-rose-300 flex items-center justify-between">
          <span>{error}</span>
          <button type="button" onClick={() => setError(null)} className="text-rose-200 hover:text-white">dismiss</button>
        </div>
      )}
      {expanded && (
        <div className="border-t border-gray-800/30">
          {runs === null ? (
            <div className="px-5 py-4 text-sm text-gray-500"><LoadingDots label="Loading" /></div>
          ) : runs.length === 0 ? (
            <div className="px-5 py-4 text-sm text-gray-500">No runs yet.</div>
          ) : (
            <div className="divide-y divide-gray-800/30">
              {runs.map((r) => {
                const mom = r.momentum_summary || [];
                const okStrat = mom.filter((m) => m.status === 'ok').length;
                const detail = [
                  r.companies_total
                    ? `${r.companies_processed}/${r.companies_total} companies · ${r.prices_refreshed}p / ${r.volumes_refreshed}v`
                    : null,
                  mom.length > 0 ? `${okStrat}/${mom.length} strategies ok` : null,
                  r.error_summary,
                ].filter(Boolean).join(' · ');
                return (
                  <div key={r.run_id} className="px-5 py-2 text-xs flex items-center gap-3 hover:bg-white/[0.02]">
                    <span className={`inline-flex items-center text-[10px] uppercase tracking-wider px-1.5 py-0.5 rounded border shrink-0 ${statusBadgeCls(r.status)}`}>
                      {r.status}
                    </span>
                    <span className="text-gray-300 font-mono shrink-0">{fmtTimestamp(r.finished_at ?? r.started_at)}</span>
                    <span className="text-gray-500 text-[10px] uppercase tracking-wider shrink-0">{r.triggered_by}</span>
                    <span className="text-gray-400 truncate flex-1 min-w-0" title={detail}>{detail}</span>
                  </div>
                );
              })}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
