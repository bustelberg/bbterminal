'use client';

import { useCallback, useEffect, useRef, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import Spinner from '../Spinner';

type Status = {
  running?: boolean;
  done?: boolean;
  message?: string;
  processed?: number;
  total?: number;
  set?: number;
  error?: string | null;
};

/** Admin-only button that triggers the server-side market-cap backfill
 * (GuruFocus → EUR snapshot for every company) and polls its status. It's a
 * long (~hour), rarely-run maintenance job, so the work runs in a backend
 * thread and we just poll the latest progress message every 3s. On completion
 * it calls `onRefreshed` so the table reloads and the new caps appear in the
 * Mkt Cap column. */
export default function MarketCapRefreshButton({ onRefreshed }: { onRefreshed: () => void }) {
  const [running, setRunning] = useState(false);
  const [message, setMessage] = useState('');
  const [processed, setProcessed] = useState(0);
  const [total, setTotal] = useState(0);
  const interval = useRef<number | null>(null);

  const stop = useCallback(() => {
    if (interval.current != null) { window.clearInterval(interval.current); interval.current = null; }
  }, []);
  useEffect(() => stop, [stop]);

  const start = useCallback(async () => {
    setRunning(true);
    setMessage('Starting…');
    setProcessed(0);
    setTotal(0);
    try {
      const r = await apiFetch(`${API_URL}/api/companies/market-cap/refresh`, { method: 'POST' });
      const d = await r.json();
      if (!d.started && !d.running) { setRunning(false); return; }
    } catch {
      setRunning(false);
      return;
    }
    // Poll the status every second — the backend now emits per-company, so this
    // shows near-real-time progress (one GuruFocus call ≈ every 1.5s).
    stop();
    interval.current = window.setInterval(async () => {
      try {
        const sr = await apiFetch(`${API_URL}/api/companies/market-cap/refresh/status`);
        const s: Status = await sr.json();
        setMessage(s.message ?? '');
        setProcessed(s.processed ?? 0);
        setTotal(s.total ?? 0);
        if (!s.running) {
          stop();
          setRunning(false);
          onRefreshed();
        }
      } catch {
        stop();
        setRunning(false);
      }
    }, 1000);
  }, [onRefreshed, stop]);

  const pct = total > 0 ? Math.min(100, Math.round((processed / total) * 100)) : 0;

  return (
    <button
      type="button"
      onClick={start}
      disabled={running}
      title={running ? message : "Re-fetch every company's market cap from GuruFocus and store it in EUR. Runs in the background (~an hour); progress updates live."}
      className="relative overflow-hidden px-3 py-2 rounded-lg text-sm font-medium bg-card border border-neutral-800/60 text-fg-muted hover:text-fg-strong transition-colors disabled:opacity-60 disabled:cursor-not-allowed inline-flex items-center gap-2"
    >
      {/* Live progress fill — width tracks processed/total, behind the label. */}
      {running && (
        <span
          aria-hidden
          className="absolute inset-y-0 left-0 bg-accent-500/15 transition-[width] duration-500 ease-out"
          style={{ width: `${pct}%` }}
        />
      )}
      <span className="relative inline-flex items-center gap-2">
        {running && <Spinner size={12} />}
        <span className="truncate max-w-[18rem]">
          {running
            ? total > 0
              ? `Refreshing market caps… ${processed}/${total} (${pct}%)`
              : (message || 'Refreshing market caps…')
            : 'Refresh market caps'}
        </span>
      </span>
    </button>
  );
}
