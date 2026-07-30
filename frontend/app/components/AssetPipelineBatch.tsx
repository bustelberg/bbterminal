'use client';

import { useCallback, useEffect, useRef, useState } from 'react';
import { apiFetch } from '../../lib/apiFetch';
import { API_URL } from '../../lib/apiUrl';

type Storage = { analysis_assets: number; executions: number; price_rows: number; volume_rows: number; est_price_mb: number };
type QStatus = { pending: number; done: number; failed: number; total: number; working: boolean };

/** Ingest-queue status panel. The CSV upload (above) writes ISINs into the DB
 * queue; the standalone worker (scripts/asset_queue_worker.py) drains it in the
 * background. This just shows the worker's progress + lets you re-queue the
 * mis-mapped rows for a clean re-resolve. */
export default function AssetPipelineBatch({ onIngested }: { onIngested?: () => void }) {
  const [storage, setStorage] = useState<Storage | null>(null);
  const [qstatus, setQstatus] = useState<QStatus | null>(null);
  const [busy, setBusy] = useState(false);
  const [msg, setMsg] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const lastDoneRef = useRef(0);

  const loadStorage = useCallback(async () => {
    try {
      const r = await apiFetch(`${API_URL}/api/asset-pipeline/storage`);
      if (r.ok) setStorage((await r.json()) as Storage);
    } catch { /* ignore */ }
  }, []);

  // Poll the worker's queue status; refresh the grid whenever `done` advances.
  const loadStatus = useCallback(async () => {
    try {
      const r = await apiFetch(`${API_URL}/api/asset-pipeline/queue/status`);
      if (!r.ok) return;
      const s = (await r.json()) as QStatus;
      setQstatus(s);
      if (s.done !== lastDoneRef.current) { lastDoneRef.current = s.done; onIngested?.(); void loadStorage(); }
    } catch { /* ignore */ }
  }, [onIngested, loadStorage]);
  useEffect(() => {
    void loadStorage();
    void loadStatus();
    const id = setInterval(() => { if (!document.hidden) void loadStatus(); }, 4000);
    return () => clearInterval(id);
  }, [loadStorage, loadStatus]);

  const requeueSuspects = async () => {
    setBusy(true); setError(null); setMsg(null);
    try {
      const r = await apiFetch(`${API_URL}/api/asset-pipeline/queue/requeue-suspects`, { method: 'POST' });
      const b = await r.json().catch(() => null);
      if (!r.ok) setError(b?.detail ?? `HTTP ${r.status}`);
      else { setMsg(`Re-queued ${(b.queued ?? 0).toLocaleString()} mis-mapped rows for a clean re-resolve.`); void loadStatus(); }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally { setBusy(false); }
  };

  const outstanding = qstatus ? qstatus.done + qstatus.pending + qstatus.failed : 0;
  const pct = outstanding > 0 ? Math.round(((qstatus!.done + qstatus!.failed) / outstanding) * 100) : 0;

  return (
    <section className="bg-card border border-neutral-800/40 rounded-xl p-5 space-y-3">
      <div className="flex items-center justify-between gap-3 flex-wrap">
        <h3 className="text-sm font-semibold text-fg-strong">Ingest queue</h3>
        {storage && (
          <span className="text-[11px] font-mono text-fg-faint">
            stored: {storage.analysis_assets} assets · {storage.executions} executions · {storage.price_rows.toLocaleString()} price / {storage.volume_rows.toLocaleString()} vol rows · ≈{storage.est_price_mb} MB
          </span>
        )}
      </div>

      {/* Worker status */}
      {qstatus && qstatus.total > 0 ? (
        <div className="space-y-1.5">
          <div className="flex items-center gap-3 text-[11px] font-mono">
            <span className={`inline-flex items-center gap-1.5 ${qstatus.pending > 0 ? 'text-accent-300' : 'text-fg-faint'}`}>
              {qstatus.pending > 0 && <span className="inline-block h-1.5 w-1.5 rounded-full bg-accent-500 animate-pulse" />}
              worker: {qstatus.pending > 0 ? 'processing' : 'idle'}
            </span>
            <span className="text-fg-muted">{qstatus.pending.toLocaleString()} pending</span>
            <span className="text-pos-400">{qstatus.done.toLocaleString()} done</span>
            {qstatus.failed > 0 && <span className="text-neg-400">{qstatus.failed.toLocaleString()} failed</span>}
          </div>
          <div className="h-1.5 rounded-full bg-inset overflow-hidden">
            <div className="h-full bg-accent-500 transition-all" style={{ width: `${pct}%` }} />
          </div>
        </div>
      ) : (
        <div className="text-[11px] text-fg-faint">Queue empty. Upload a CSV above to add ISINs.</div>
      )}

      <div className="flex items-center gap-2 flex-wrap">
        <button
          type="button" onClick={requeueSuspects} disabled={busy}
          title="Re-resolve rows whose stored company differs from OpenFIGI (fixes throttle-corrupted mappings)"
          className="text-xs px-3 py-1.5 rounded-lg border border-neutral-700 text-fg-muted hover:text-warn-300 hover:border-warn-500/50 disabled:opacity-40 transition-colors"
        >Fix mis-mapped rows</button>
        {qstatus?.pending === 0 && <span className="text-[11px] text-fg-faint">Worker idle — start it with <span className="font-mono">uv run python scripts/asset_queue_worker.py</span></span>}
      </div>

      {msg && <div className="text-[11px] text-fg-soft bg-inset rounded-lg px-3 py-2">{msg}</div>}
      {error && <div className="bg-neg-500/10 border border-neg-500/20 rounded-lg px-3 py-2 text-xs text-neg-300">{error}</div>}

      <p className="text-[10px] text-fg-faint">
        Uploads store ISINs in the DB queue; the standalone background worker resolves them through the throttled Yahoo + OpenFIGI layers (the only consumer, so no rate-limit competition). Navigate away freely — it keeps going and survives backend restarts; the grid updates live as rows land.
      </p>
    </section>
  );
}
