'use client';

import { useCallback, useEffect, useRef, useState } from 'react';
import LoadingDots from '../LoadingDots';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import TemplateRow from './TemplateRow';
import type { TemplateRefreshStatus, UniverseTemplateSummary } from './types';

/** Template universes section on /schedule. Lists every registered
 * `UniverseTemplate` with its current state and surfaces the
 * `last_refreshed_at IS NULL` case so the dev→prod gap (new template
 * deployed but pipeline hasn't run yet) is visible at a glance instead
 * of silently producing empty membership chips on /companies.
 *
 * Pairs with `_maybe_bootstrap_templates` in `backend/scheduler.py` —
 * the bootstrap should normally fix any never-refreshed templates
 * within ~10 minutes of a fresh deploy, but if it gets short-circuited
 * (another pipeline already running, etc.) the banner here gives the
 * user a manual "Run pipeline now" button. */
export default function TemplateUniversesCard() {
  const [templates, setTemplates] = useState<UniverseTemplateSummary[] | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [triggering, setTriggering] = useState(false);
  const [triggerError, setTriggerError] = useState<string | null>(null);
  // Live per-template refresh status, polled from the in-process registry
  // so the busy spinner + progress bar reflect ANY refresh (manual click,
  // scheduled month-end tick, or the daily pipeline) — not just one the
  // current tab triggered.
  const [statuses, setStatuses] = useState<Record<string, TemplateRefreshStatus>>({});

  const load = useCallback(async () => {
    try {
      const r = await fetch(`${API_URL}/api/universe-templates`);
      if (!r.ok) {
        setLoadError(`${r.status} ${r.statusText}`);
        return;
      }
      const data = (await r.json()) as UniverseTemplateSummary[];
      setTemplates(Array.isArray(data) ? data : []);
    } catch (e) {
      setLoadError(e instanceof Error ? e.message : String(e));
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  // Poll the cheap (no-DB) status endpoint. When a template transitions
  // out of 'running' (a refresh just finished), refetch the summary so its
  // latest-month / last-refreshed / member-count update.
  const runningKeysRef = useRef<Set<string>>(new Set());
  useEffect(() => {
    let cancelled = false;
    const poll = async () => {
      try {
        const r = await fetch(`${API_URL}/api/universe-templates/refresh-status`);
        if (!r.ok || cancelled) return;
        const data = (await r.json()) as Record<string, TemplateRefreshStatus>;
        if (cancelled) return;
        setStatuses(data);
        const nowRunning = new Set(
          Object.entries(data).filter(([, s]) => s.status === 'running').map(([k]) => k),
        );
        // A key that was running last poll but isn't now → just finished.
        let finished = false;
        runningKeysRef.current.forEach((k) => { if (!nowRunning.has(k)) finished = true; });
        runningKeysRef.current = nowRunning;
        if (finished) void load();
      } catch {
        // Network blip — next tick retries.
      }
    };
    void poll();
    const id = window.setInterval(poll, 3000);
    return () => { cancelled = true; window.clearInterval(id); };
  }, [load]);

  const triggerPipeline = useCallback(async () => {
    setTriggering(true);
    setTriggerError(null);
    try {
      const r = await apiFetch(
        `${API_URL}/api/ingest/scheduled-refresh/trigger?job_name=manual`,
        { method: 'POST' },
      );
      if (!r.ok) {
        const body = await r.text().catch(() => '');
        setTriggerError(`${r.status} ${body.slice(0, 200)}`);
        return;
      }
      // Refetch after a short delay so `last_refreshed_at` updates once
      // the pipeline writes its first checkpoint. The user can also
      // expand the run via the existing strategy-history view.
      window.setTimeout(() => void load(), 5000);
    } catch (e) {
      setTriggerError(e instanceof Error ? e.message : String(e));
    } finally {
      setTriggering(false);
    }
  }, [load]);

  const unrefreshed = templates?.filter((t) => t.last_refreshed_at == null) ?? [];

  return (
    <div className="space-y-3">
      <div className="flex items-baseline justify-between">
        <h2 className="text-sm uppercase tracking-wider text-gray-400 font-medium">
          Template universes
        </h2>
        <p className="text-xs text-gray-600">
          Canonical universes refreshed by the pipeline
        </p>
      </div>

      {unrefreshed.length > 0 && (
        <div className="bg-amber-500/10 border border-amber-500/30 rounded-lg px-4 py-3 text-sm">
          <div className="flex items-start justify-between gap-3">
            <div className="min-w-0">
              <div className="text-amber-300 font-medium mb-1">
                {unrefreshed.length} template{unrefreshed.length === 1 ? '' : 's'} never refreshed in this environment
              </div>
              <div className="text-gray-300 text-xs leading-relaxed">
                {unrefreshed.map((t) => t.template_key).join(', ')} —
                the pipeline will populate {unrefreshed.length === 1 ? 'it' : 'them'} on
                the next Tuesday 02:00 UTC tick, or click below to trigger now.
                The in-process scheduler also auto-fires a bootstrap on app
                start; if you just deployed, give it a minute.
              </div>
              {triggerError && (
                <div className="text-rose-300 text-xs mt-2">Trigger failed: {triggerError}</div>
              )}
            </div>
            <button
              type="button"
              onClick={() => void triggerPipeline()}
              disabled={triggering}
              className="text-xs px-3 py-1.5 rounded-lg bg-amber-500/20 hover:bg-amber-500/30 text-amber-100 border border-amber-500/40 transition-colors disabled:opacity-50 disabled:cursor-not-allowed shrink-0"
            >
              {triggering ? 'Triggering…' : 'Run pipeline now'}
            </button>
          </div>
        </div>
      )}

      <div className="bg-[#151821] rounded-xl border border-gray-800/40">
        {loadError && (
          <div className="px-5 py-3 text-xs text-rose-300">
            Failed to load templates: {loadError}
          </div>
        )}
        {!loadError && templates == null && (
          <div className="px-5 py-3 text-sm text-gray-500">
            <LoadingDots label="Loading" />
          </div>
        )}
        {!loadError && templates?.length === 0 && (
          <div className="px-5 py-4 text-sm text-gray-500">
            No templates registered.
          </div>
        )}
        {templates && templates.length > 0 && (
          <div className="divide-y divide-gray-800/30">
            {templates.map((t) => (
              <TemplateRow key={t.template_key} t={t} status={statuses[t.template_key]} />
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
