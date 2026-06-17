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
  verified?: number;
  mismatch?: number;
  error?: string | null;
};

/** Admin-only button that verifies every company's stored ISIN against
 * OpenFIGI (catching wrong-ISIN traps — an ISIN that resolves to a different
 * company) and polls progress. Batched, so it's quick (a couple of minutes for
 * ~2,800 companies). On completion it calls `onVerified` so the table reloads
 * and the OpenFIGI column reflects the new statuses. */
export default function OpenFigiVerifyButton({ onVerified }: { onVerified: () => void }) {
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
      const r = await apiFetch(`${API_URL}/api/companies/openfigi/verify`, { method: 'POST' });
      const d = await r.json();
      if (!d.started && !d.running) { setRunning(false); return; }
    } catch {
      setRunning(false);
      return;
    }
    stop();
    interval.current = window.setInterval(async () => {
      try {
        const sr = await apiFetch(`${API_URL}/api/companies/openfigi/verify/status`);
        const s: Status = await sr.json();
        setMessage(s.message ?? '');
        setProcessed(s.processed ?? 0);
        setTotal(s.total ?? 0);
        if (!s.running) {
          stop();
          setRunning(false);
          onVerified();
        }
      } catch {
        stop();
        setRunning(false);
      }
    }, 1000);
  }, [onVerified, stop]);

  const pct = total > 0 ? Math.min(100, Math.round((processed / total) * 100)) : 0;

  return (
    <button
      type="button"
      onClick={start}
      disabled={running}
      title={running ? message : "Verify every company's stored ISIN against OpenFIGI — flags ISINs that resolve to a DIFFERENT company (wrong-ISIN traps). Runs in the background (~a couple of minutes); fills the OpenFIGI column."}
      className="relative overflow-hidden px-3 py-2 rounded-lg text-sm font-medium bg-card border border-neutral-800/60 text-fg-muted hover:text-fg-strong transition-colors disabled:opacity-60 disabled:cursor-not-allowed inline-flex items-center gap-2"
    >
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
              ? `Verifying ISINs… ${processed}/${total} (${pct}%)`
              : (message || 'Verifying ISINs…')
            : 'Verify OpenFIGI'}
        </span>
      </span>
    </button>
  );
}
