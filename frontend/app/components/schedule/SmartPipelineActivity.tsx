'use client';

import { useEffect, useState } from 'react';
import Spinner from '../Spinner';
import LoadingDots from '../LoadingDots';
import { API_URL } from '../../../lib/apiUrl';
import { relTime } from './utils';
import type { ScheduleUpcoming, HeldCompaniesResponse, HeldCompany } from './types';

/** Minimal, transparent view of the daily pipeline: one status line (what
 * it's doing right now, or when it next runs) + the held companies it keeps
 * priced, with each company's latest close so you can watch them refresh. */
export default function SmartPipelineActivity() {
  const [upcoming, setUpcoming] = useState<ScheduleUpcoming | null>(null);
  const [held, setHeld] = useState<HeldCompaniesResponse | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [nowMs, setNowMs] = useState<number>(() => Date.now());

  useEffect(() => {
    let cancelled = false;
    const poll = async () => {
      try {
        const [u, h] = await Promise.all([
          fetch(`${API_URL}/api/schedule/upcoming`),
          fetch(`${API_URL}/api/scheduled-strategies/held-companies`),
        ]);
        if (cancelled) return;
        if (u.ok) setUpcoming((await u.json()) as ScheduleUpcoming);
        if (h.ok) setHeld((await h.json()) as HeldCompaniesResponse);
        setLoadError(null);
      } catch (e) {
        if (!cancelled) setLoadError(e instanceof Error ? e.message : String(e));
      }
    };
    void poll();
    const id = window.setInterval(poll, 3000);
    return () => { cancelled = true; window.clearInterval(id); };
  }, []);

  useEffect(() => {
    const id = window.setInterval(() => setNowMs(Date.now()), 15000);
    return () => window.clearInterval(id);
  }, []);

  const running = upcoming?.running?.[0] ?? null;
  const smartJob = upcoming?.jobs?.find((j) => j.id === 'smart_daily') ?? null;
  const schedulerOff = upcoming?.scheduler_enabled === false;
  const loading = upcoming == null && held == null;

  // Live price-refresh progress (held set), shown while the prices phase runs.
  const total = running?.companies_total ?? 0;
  const done = running?.companies_processed ?? 0;
  const showBar = !!running && total > 0 && running.current_phase === 'prices';
  const pct = total > 0 ? Math.min(100, Math.round((done / total) * 100)) : 0;

  const fresh = held?.freshness_summary;

  return (
    <div className="space-y-3">
      <h2 className="text-sm uppercase tracking-wider text-fg-muted font-medium">
        Smart pipeline activity
      </h2>

      <div className="bg-card rounded-xl border border-neutral-800/40">
        {/* Status line */}
        <div className="px-5 py-3 border-b border-neutral-800/30">
          {loading && !loadError && <LoadingDots label="Loading" />}
          {loadError && loading && <span className="text-xs text-neg-300">Failed to load: {loadError}</span>}

          {!loading && schedulerOff && !running && (
            <span className="text-xs text-warn-300/90">Scheduler disabled — nothing runs automatically.</span>
          )}

          {running ? (
            <div className="space-y-1.5">
              <div className="flex items-center gap-2.5 text-sm">
                <Spinner className="h-3.5 w-3.5 text-accent-400 shrink-0" />
                <span className="text-fg-strong">{running.current_message ?? running.current_phase ?? 'Running…'}</span>
              </div>
              {showBar && (
                <div className="flex items-center gap-2">
                  <div className="flex-1 h-1.5 rounded-full bg-inset overflow-hidden">
                    <div className="h-full bg-accent-500 transition-all" style={{ width: `${pct}%` }} />
                  </div>
                  <span className="text-[11px] font-mono text-fg-faint shrink-0">{done}/{total}</span>
                </div>
              )}
            </div>
          ) : !loading && !schedulerOff && (
            <div className="flex items-center gap-2.5 text-sm">
              <span className="h-1.5 w-1.5 rounded-full bg-pos-500 shrink-0" />
              <span className="text-fg">Idle</span>
              {smartJob?.next_run_at && (
                <span className="text-xs text-fg-faint ml-auto font-mono">next check {relTime(smartJob.next_run_at, nowMs)}</span>
              )}
            </div>
          )}
        </div>

        {/* Held companies + freshness */}
        {held && (
          <div className="px-5 py-3">
            <div className="flex items-center justify-between gap-2 mb-2">
              <span className="text-xs text-fg-soft">
                Held companies <span className="text-fg-faint">· {held.total_companies}</span>
              </span>
              <span className="flex items-center gap-2 text-[11px] font-mono">
                {fresh?.latest_close_date && <span className="text-fg-faint">through {fresh.latest_close_date}</span>}
                {(fresh?.fresh_count ?? 0) > 0 && <span className="text-pos-400">{fresh!.fresh_count} fresh</span>}
                {(fresh?.stale_count ?? 0) > 0 && <span className="text-warn-300">{fresh!.stale_count} stale</span>}
                {(fresh?.missing_count ?? 0) > 0 && <span className="text-neg-400">{fresh!.missing_count} missing</span>}
              </span>
            </div>

            {held.total_companies === 0 ? (
              <div className="text-xs text-fg-subtle">No holdings yet.</div>
            ) : (
              <div className="max-h-80 overflow-auto rounded-lg border border-neutral-800/40">
                <table className="w-full text-xs">
                  <tbody className="divide-y divide-neutral-800/20">
                    {held.companies.map((c) => (
                      <HeldRow key={c.company_id} c={c} expected={fresh?.expected_close_date ?? null} />
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

function HeldRow({ c, expected }: { c: HeldCompany; expected: string | null }) {
  const d = c.latest_close_price_date;
  // Fresh when the close is at/after the last settled trading day; stale when
  // behind it (new prices to fetch); missing when there's no close at all.
  const tone = d == null ? 'text-neg-400' : (expected && d >= expected) ? 'text-pos-400' : 'text-warn-300';
  return (
    <tr className="hover:bg-overlay/[0.02]">
      <td className="px-3 py-1.5 font-mono text-fg whitespace-nowrap">
        {c.ticker ?? '—'}
        {c.exchange && <span className="text-fg-faint">·{c.exchange}</span>}
      </td>
      <td className="px-3 py-1.5 text-fg-soft truncate max-w-[240px]">{c.company_name ?? '—'}</td>
      <td className="px-3 py-1.5 text-fg-subtle whitespace-nowrap">{c.sector ?? '—'}</td>
      <td className={`px-3 py-1.5 text-right font-mono whitespace-nowrap ${tone}`}>{d ?? 'none'}</td>
    </tr>
  );
}
