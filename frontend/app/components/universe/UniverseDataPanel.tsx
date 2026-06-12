'use client';

import { useCallback, useEffect, useState } from 'react';

import ProgressTimeline from '../ProgressTimeline';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import type { UniverseRow } from './types';

type Freshness = {
  member_count: number;
  with_data: number;
  latest_date: string | null;
};

/** Per-universe data row on /universe: shows coverage (members with GuruFocus
 * data / total) + the most recent close-price date we hold, and a "Fetch data"
 * button that streams a bulk earnings/price ingest (financials + analyst
 * estimates + indicators + prices) over every company that has ever been a
 * member. Self-contained — owns its freshness fetch + the SSE stream. */
export default function UniverseDataPanel({ universe }: { universe: UniverseRow }) {
  const [fresh, setFresh] = useState<Freshness | null>(null);
  const [freshLoading, setFreshLoading] = useState(true);
  const [force, setForce] = useState(false);
  const [fetching, setFetching] = useState(false);
  const [log, setLog] = useState<string[]>([]);
  const [result, setResult] = useState<{ ok: boolean; message: string } | null>(null);

  const loadFreshness = useCallback(async () => {
    setFreshLoading(true);
    try {
      const r = await apiFetch(`${API_URL}/api/universe/${universe.universe_id}/data-freshness`);
      if (r.ok) setFresh((await r.json()) as Freshness);
    } catch {
      /* leave fresh null — the readout shows a dash */
    } finally {
      setFreshLoading(false);
    }
  }, [universe.universe_id]);

  useEffect(() => { void loadFreshness(); }, [loadFreshness]);

  const fetchData = useCallback(async () => {
    if (fetching) return;
    setFetching(true);
    setLog([]);
    setResult(null);
    try {
      const resp = await apiFetch(
        `${API_URL}/api/universe/${universe.universe_id}/fetch-data?force=${force}`,
        { method: 'POST', headers: { 'Accept': 'text/event-stream' } },
      );
      if (!resp.ok || !resp.body) {
        const d = (await resp.json().catch(() => ({}))) as { detail?: string };
        setResult({ ok: false, message: d.detail ?? `Fetch failed (${resp.status})` });
        return;
      }
      const reader = resp.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        const parts = buffer.split('\n\n');
        buffer = parts.pop() ?? '';
        for (const part of parts) {
          const lines = part.split('\n').filter((l) => l.startsWith('data: '));
          if (!lines.length) continue;
          const payload = lines.map((l) => l.slice(6)).join('\n');
          try {
            const evt = JSON.parse(payload);
            if (evt.type === 'progress' && evt.message) setLog((l) => [...l, evt.message]);
            else if (evt.type === 'done') setResult({ ok: true, message: evt.message });
            else if (evt.type === 'error') setResult({ ok: false, message: evt.message });
          } catch {
            // Non-JSON keepalive lines — ignore.
          }
        }
      }
      await loadFreshness(); // reflect the just-fetched data in the readout
    } catch (e) {
      setResult({ ok: false, message: e instanceof Error ? e.message : String(e) });
    } finally {
      setFetching(false);
    }
  }, [fetching, force, universe.universe_id, loadFreshness]);

  const coverage = fresh
    ? `${fresh.with_data}/${fresh.member_count} members have data`
    : (freshLoading ? 'Checking…' : '—');
  const latest = fresh?.latest_date
    ? `latest ${fresh.latest_date}`
    : (fresh && fresh.with_data === 0 ? 'no data yet' : '');

  return (
    <div className="px-5 py-3 border-t border-neutral-800/40">
      <div className="flex items-center justify-between gap-3 flex-wrap">
        <div className="text-xs text-fg-muted">
          <span className="text-fg-subtle uppercase tracking-wider mr-2">Data</span>
          {coverage}
          {latest && <span className="text-fg-faint"> · {latest}</span>}
        </div>
        <div className="flex items-center gap-3 shrink-0">
          <label
            className="flex items-center gap-1.5 text-xs text-fg-muted cursor-pointer"
            title="Re-fetch every member even if its cached data is already fresh (slower, more API calls)."
          >
            <input
              type="checkbox"
              checked={force}
              onChange={(e) => setForce(e.target.checked)}
              disabled={fetching}
              className="h-3.5 w-3.5 rounded border-neutral-700 bg-page text-accent-600 focus:ring-1 focus:ring-accent-500/30"
            />
            Force
          </label>
          <button
            type="button"
            onClick={() => void fetchData()}
            disabled={fetching}
            title="Fetch financials + analyst estimates + indicators + prices for every company that has ever been a member of this universe. Skips sources already fresh unless Force is on. Heavy for large universes."
            className="px-3 py-1.5 rounded-lg text-xs font-medium bg-accent-600 hover:bg-accent-500 text-fg-strong transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {fetching ? 'Fetching…' : 'Fetch data'}
          </button>
        </div>
      </div>
      {(log.length > 0 || result) && (
        <div className="mt-3">
          <ProgressTimeline
            steps={[]}
            log={log}
            doneSummary={result?.ok ? result.message : null}
            errorMessage={result && !result.ok ? result.message : null}
            running={fetching}
            defaultLogOpen
            title="Data fetch progress"
            onDismiss={() => { setLog([]); setResult(null); }}
          />
        </div>
      )}
    </div>
  );
}
