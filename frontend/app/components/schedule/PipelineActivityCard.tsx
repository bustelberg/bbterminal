'use client';

import { useEffect, useState } from 'react';
import Spinner from '../Spinner';
import LoadingDots from '../LoadingDots';
import { fmtClock } from '../../../lib/format';
import { API_URL } from '../../../lib/apiUrl';
import { relTime } from './utils';
import type { ScheduleUpcoming } from './types';

/** "Pipeline activity" strip at the top of /schedule. Running jobs (with
 * a live spinner + phase) render first; then every scheduled job in
 * chronological fire order. Polls the live scheduler every 3s so the view
 * reflects ANY activity — scheduled ticks, the startup catch-up one-shots,
 * or a manual Run-now — not just something this tab kicked off. */
export default function PipelineActivityCard() {
  const [data, setData] = useState<ScheduleUpcoming | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  // Local clock so relative times tick down between polls.
  const [nowMs, setNowMs] = useState<number>(() => Date.now());

  useEffect(() => {
    let cancelled = false;
    const poll = async () => {
      try {
        const r = await fetch(`${API_URL}/api/schedule/upcoming`);
        if (!r.ok || cancelled) return;
        const d = (await r.json()) as ScheduleUpcoming;
        if (!cancelled) { setData(d); setLoadError(null); }
      } catch (e) {
        if (!cancelled) setLoadError(e instanceof Error ? e.message : String(e));
      }
    };
    void poll();
    const id = window.setInterval(poll, 3000);
    return () => { cancelled = true; window.clearInterval(id); };
  }, []);

  // Tick the local clock every 30s so "in 18h" stays roughly current
  // without hammering re-renders.
  useEffect(() => {
    const id = window.setInterval(() => setNowMs(Date.now()), 30000);
    return () => window.clearInterval(id);
  }, []);

  const running = data?.running ?? [];
  const jobs = data?.jobs ?? [];

  return (
    <div className="space-y-3">
      <div className="flex items-baseline justify-between">
        <h2 className="text-sm uppercase tracking-wider text-fg-muted font-medium">
          Pipeline activity
        </h2>
        <p className="text-xs text-fg-faint">
          What&apos;s running now + what fires next
        </p>
      </div>

      <div className="bg-card rounded-xl border border-neutral-800/40">
        {loadError && data == null && (
          <div className="px-5 py-3 text-xs text-neg-300">
            Failed to load schedule activity: {loadError}
          </div>
        )}
        {!loadError && data == null && (
          <div className="px-5 py-3 text-sm text-fg-subtle">
            <LoadingDots label="Loading" />
          </div>
        )}

        {data && data.scheduler_enabled === false && running.length === 0 && (
          <div className="px-5 py-3 text-xs text-warn-300/90">
            In-process scheduler is disabled (DISABLE_SCHEDULER) — no jobs will fire automatically.
          </div>
        )}

        {data && (
          <div>
            {/* Running now */}
            {running.length > 0 && (
              <div className="px-5 py-3 border-b border-neutral-800/30">
                <div className="text-[10px] uppercase tracking-wider text-accent-300/80 mb-2">
                  Running now
                </div>
                <div className="space-y-2">
                  {running.map((r) => (
                    <div key={r.run_id} className="flex items-center gap-3 text-sm">
                      <Spinner className="h-3.5 w-3.5 text-accent-400 shrink-0" />
                      <span className="text-fg-strong font-medium shrink-0">{r.label}</span>
                      {r.current_phase && (
                        <span className="text-[10px] uppercase tracking-wider px-1.5 py-0.5 rounded border bg-accent-500/10 text-accent-300 border-accent-500/30 font-mono shrink-0">
                          {r.current_phase}
                        </span>
                      )}
                      <span className="text-xs text-fg-subtle truncate min-w-0" title={r.current_message ?? ''}>
                        {r.current_message ?? ''}
                      </span>
                      <span className="text-[10px] text-fg-faint font-mono ml-auto shrink-0">
                        run #{r.run_id} · {r.triggered_by}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* Upcoming */}
            <div className="px-5 py-3">
              <div className="text-[10px] uppercase tracking-wider text-fg-subtle mb-2">
                Upcoming
              </div>
              {jobs.length === 0 ? (
                <div className="text-xs text-fg-subtle">
                  No jobs scheduled.
                </div>
              ) : (
                <div className="space-y-1.5">
                  {jobs.map((j) => (
                    <div key={j.id} className="flex items-center gap-3 text-sm">
                      <span className="shrink-0 w-4 flex justify-center">
                        {j.running
                          ? <Spinner className="h-3.5 w-3.5 text-accent-400" />
                          : <span className="text-fg-faint">•</span>}
                      </span>
                      <span className={`shrink-0 ${j.running ? 'text-fg-strong' : 'text-fg'}`}>
                        {j.label}
                      </span>
                      <span className="text-xs text-fg-faint truncate min-w-0" title={j.cadence}>
                        {j.description}
                      </span>
                      <span className="ml-auto shrink-0 flex items-baseline gap-2 font-mono text-xs">
                        <span className={j.running ? 'text-accent-300' : 'text-fg-soft'}>
                          {j.running ? 'running' : relTime(j.next_run_at, nowMs)}
                        </span>
                        {!j.running && (
                          <span className="text-fg-faint">{fmtClock(j.next_run_at)}</span>
                        )}
                      </span>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
