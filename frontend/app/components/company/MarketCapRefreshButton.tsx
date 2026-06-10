'use client';

import { useCallback, useEffect, useRef, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import Spinner from '../Spinner';

type Status = { running?: boolean; done?: boolean; message?: string; set?: number; error?: string | null };

/** Admin-only button that triggers the server-side market-cap backfill
 * (GuruFocus → EUR snapshot for every company) and polls its status. It's a
 * long (~hour), rarely-run maintenance job, so the work runs in a backend
 * thread and we just poll the latest progress message every 3s. On completion
 * it calls `onRefreshed` so the table reloads and the new caps' ⓘ tooltips
 * appear. */
export default function MarketCapRefreshButton({ onRefreshed }: { onRefreshed: () => void }) {
  const [running, setRunning] = useState(false);
  const [message, setMessage] = useState('');
  const interval = useRef<number | null>(null);

  const stop = useCallback(() => {
    if (interval.current != null) { window.clearInterval(interval.current); interval.current = null; }
  }, []);
  useEffect(() => stop, [stop]);

  const start = useCallback(async () => {
    setRunning(true);
    setMessage('Starting…');
    try {
      const r = await apiFetch(`${API_URL}/api/companies/market-cap/refresh`, { method: 'POST' });
      const d = await r.json();
      if (!d.started && !d.running) { setRunning(false); return; }
    } catch {
      setRunning(false);
      return;
    }
    // Poll the status until the backend thread finishes, then reload the table.
    stop();
    interval.current = window.setInterval(async () => {
      try {
        const sr = await apiFetch(`${API_URL}/api/companies/market-cap/refresh/status`);
        const s: Status = await sr.json();
        setMessage(s.message ?? '');
        if (!s.running) {
          stop();
          setRunning(false);
          onRefreshed();
        }
      } catch {
        stop();
        setRunning(false);
      }
    }, 3000);
  }, [onRefreshed, stop]);

  return (
    <button
      type="button"
      onClick={start}
      disabled={running}
      title="Re-fetch every company's market cap from GuruFocus and store it in EUR. Runs in the background (~an hour); the table updates when it finishes."
      className="px-3 py-2 rounded-lg text-sm font-medium bg-card border border-neutral-800/60 text-fg-muted hover:text-fg-strong transition-colors disabled:opacity-60 disabled:cursor-not-allowed inline-flex items-center gap-2"
    >
      {running && <Spinner size={12} />}
      <span className="truncate max-w-[16rem]">{running ? (message || 'Refreshing market caps…') : 'Refresh market caps'}</span>
    </button>
  );
}
