'use client';

import { useEffect, useState } from 'react';
import Spinner from '../Spinner';
import { apiFetch } from '../../../lib/apiFetch';
import { fmtTimestamp } from '../../../lib/format';
import { API_URL } from '../../../lib/apiUrl';
import TemplateRecentChanges from './TemplateRecentChanges';
import type { TemplateRefreshStatus, UniverseTemplateSummary } from './types';

/** Per-template row in the templates section. Collapsible — click to
 * expand into a live progress bar (while refreshing) + the recent
 * additions/removals diff. Shows a busy spinner whenever a refresh is in
 * flight (manual or scheduled), last refresh + next-refresh ETA, and a
 * per-row "Refresh" button that triggers a catch-up on demand. */
export default function TemplateRow({ t, status }: { t: UniverseTemplateSummary; status?: TemplateRefreshStatus }) {
  const [expanded, setExpanded] = useState(false);
  const [triggering, setTriggering] = useState(false);
  const neverRefreshed = t.last_refreshed_at == null;
  const busy = status?.status === 'running' || triggering;

  // Once the poll confirms this template is running, drop the optimistic
  // local flag (the registry status now drives the spinner).
  useEffect(() => {
    if (status?.status === 'running') setTriggering(false);
  }, [status?.status]);

  const triggerRefresh = () => {
    setTriggering(true);
    setExpanded(true); // reveal the progress bar immediately
    void (async () => {
      try {
        const r = await apiFetch(
          `${API_URL}/api/universe-templates/${encodeURIComponent(t.template_key)}/refresh`,
          { method: 'POST', headers: { Accept: 'text/event-stream' } },
        );
        // Drain + discard so the connection closes cleanly; the status
        // poll in the parent drives the spinner / progress bar / refetch.
        const reader = r.body?.getReader();
        if (reader) { while (true) { const { done } = await reader.read(); if (done) break; } }
      } catch {
        // The status poll surfaces any error state.
      } finally {
        setTriggering(false);
      }
    })();
  };

  return (
    <div>
      <div className="w-full px-5 py-3 flex items-center gap-3 hover:bg-white/[0.02] transition-colors">
        <button
          type="button"
          onClick={() => setExpanded((v) => !v)}
          className="flex items-center gap-3 flex-1 min-w-0 text-left"
        >
          <span className="text-gray-500 text-xs font-mono w-3 inline-block shrink-0">
            {expanded ? '▾' : '▸'}
          </span>
          <div className="flex-1 min-w-0">
            <div className="flex items-center gap-2 flex-wrap">
              <span className={`text-sm font-medium ${neverRefreshed ? 'text-amber-200' : 'text-white'}`}>
                {t.label}
              </span>
              <span className="text-[10px] uppercase tracking-wider px-1.5 py-0.5 rounded border bg-gray-500/10 text-gray-400 border-gray-500/30 font-mono">
                {t.template_key}
              </span>
              {busy && <Spinner className="h-3.5 w-3.5 text-indigo-400" />}
              {neverRefreshed && !busy && (
                <span className="text-[10px] uppercase tracking-wider px-1.5 py-0.5 rounded border bg-amber-500/15 text-amber-300 border-amber-500/40">
                  never refreshed
                </span>
              )}
            </div>
            <div className="text-xs text-gray-500 mt-0.5 font-mono">
              {neverRefreshed ? (
                <span className="text-amber-300/80">no membership data yet</span>
              ) : (
                <>
                  {t.latest_membership_count} member{t.latest_membership_count === 1 ? '' : 's'}
                  {t.latest_captured_month && ` · latest month ${t.latest_captured_month}`}
                  {' · last refresh '}
                  {fmtTimestamp(t.last_refreshed_at)}
                </>
              )}
            </div>
          </div>
        </button>
        <button
          type="button"
          onClick={triggerRefresh}
          disabled={busy}
          className="shrink-0 text-xs px-3 py-1.5 rounded-lg bg-indigo-600 hover:bg-indigo-500 disabled:opacity-50 disabled:cursor-not-allowed text-white transition-colors"
        >
          {busy ? 'Refreshing…' : 'Refresh'}
        </button>
      </div>
      {expanded && (
        <>
          {status && (status.status === 'running' || status.status === 'error') && (
            <div className="px-5 pt-3 pb-1 space-y-1">
              <div className="flex items-center justify-between text-[11px]">
                <span className={status.status === 'error' ? 'text-rose-300' : 'text-indigo-300'}>
                  {status.status === 'error'
                    ? 'Refresh failed'
                    : (status.message || 'Refreshing…')}
                </span>
                {status.status === 'running' && status.pct != null && (
                  <span className="font-mono text-gray-400">{status.pct}%</span>
                )}
              </div>
              <div className="h-1.5 bg-gray-800 rounded-full overflow-hidden">
                <div
                  className={`h-full transition-all duration-300 ${status.status === 'error' ? 'bg-rose-500' : 'bg-indigo-500'}`}
                  // Indeterminate (no pct) → a partial bar so the user still
                  // sees motion via the changing message above.
                  style={{ width: `${status.status === 'error' ? 100 : (status.pct ?? 35)}%` }}
                />
              </div>
              {status.status === 'error' && status.error && (
                <div className="text-[11px] text-rose-300/80 font-mono truncate" title={status.error}>
                  {status.error}
                </div>
              )}
            </div>
          )}
          <TemplateRecentChanges templateKey={t.template_key} />
        </>
      )}
    </div>
  );
}
