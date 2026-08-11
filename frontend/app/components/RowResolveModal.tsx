'use client';

import { useEffect, useRef, useState } from 'react';
import { apiFetch } from '../../lib/apiFetch';
import { API_URL } from '../../lib/apiUrl';
import type { AssetGridRow } from '../../lib/types/api';

type SourceOutcome = { found: boolean; name?: string | null; figi?: string | null;
  symbol?: string | null; currency?: string | null; rows?: number; analysis_id?: number | null };
type Result = {
  isin: string; status: string; identity_status?: string | null;
  openfigi: SourceOutcome; yfinance: SourceOutcome; message?: string;
};

/** Per-row manual resolve — requests OpenFIGI + yfinance for one ISIN and shows
 * a live progress popup with the per-source outcome. Closing it signals the
 * table to reload so the row reflects what was persisted. */
export default function RowResolveModal({ row, onClose }: {
  row: AssetGridRow;
  onClose: (didResolve: boolean) => void;
}) {
  const [phase, setPhase] = useState<'loading' | 'done' | 'error'>('loading');
  const [result, setResult] = useState<Result | null>(null);
  const [error, setError] = useState<string | null>(null);
  const didResolve = useRef(false);

  useEffect(() => {
    let alive = true;
    (async () => {
      try {
        const r = await apiFetch(`${API_URL}/api/asset-pipeline/rows/refresh`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ identifier: row.isin }),
        });
        const b = await r.json().catch(() => null);
        if (!alive) return;
        if (!r.ok) { setError(b?.detail ?? `HTTP ${r.status}`); setPhase('error'); return; }
        setResult(b as Result);
        didResolve.current = true;   // DB changed regardless of found/missing
        setPhase('done');
      } catch (e) {
        if (!alive) return;
        setError(e instanceof Error ? e.message : String(e));
        setPhase('error');
      }
    })();
    return () => { alive = false; };
  }, [row.isin]);

  const close = () => onClose(didResolve.current);

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-scrim/60 p-4"
      onClick={close}>
      <div className="bg-elevated border border-neutral-800/40 rounded-xl shadow-xl w-full max-w-md p-5 space-y-4"
        onClick={(e) => e.stopPropagation()}>
        <div className="flex items-center justify-between">
          <h3 className="text-sm font-semibold text-fg-strong">Resolve row</h3>
          <span className="font-mono text-xs text-fg-subtle">{row.isin}</span>
        </div>

        {phase === 'loading' && (
          <div className="space-y-3 py-2">
            <div className="loading-bar h-0.5 w-full rounded-full" aria-hidden />
            <p className="text-xs text-fg-muted">Requesting OpenFIGI + yfinance…</p>
          </div>
        )}

        {phase === 'error' && (
          <div className="bg-neg-500/10 border border-neg-500/20 rounded-lg px-3 py-2 text-xs text-neg-300">{error}</div>
        )}

        {phase === 'done' && result && (
          <div className="space-y-2.5">
            <SourceLine label="OpenFIGI" ok={result.openfigi.found}
              detail={result.openfigi.found ? (result.openfigi.name ?? result.openfigi.figi ?? 'found') : 'not found'} />
            <SourceLine label="yfinance" ok={result.yfinance.found}
              detail={result.yfinance.found
                ? `${result.yfinance.symbol} · ${result.yfinance.currency ?? '?'} · ${(result.yfinance.rows ?? 0).toLocaleString()} bars`
                : 'not found'} />
            {result.openfigi.found && result.yfinance.found && (
              <div className="text-[12px] text-fg-subtle">
                Name match:{' '}
                <span className={result.identity_status === 'verified' ? 'text-pos-400'
                  : result.identity_status === 'mismatch' ? 'text-warn-400' : 'text-fg-faint'}>
                  {result.identity_status ?? '—'}
                </span>
              </div>
            )}
            {result.message && <p className="text-[12px] text-fg-faint">{result.message}</p>}
          </div>
        )}

        <div className="flex justify-end pt-1">
          <button type="button" onClick={close}
            className="text-xs px-4 py-2 rounded-lg bg-accent-600 hover:bg-accent-500 text-white transition-colors">
            {phase === 'loading' ? 'Close' : 'Done'}
          </button>
        </div>
      </div>
    </div>
  );
}

function SourceLine({ label, ok, detail }: { label: string; ok: boolean; detail: string }) {
  return (
    <div className="flex items-center gap-2 text-xs">
      <span className={`text-[10px] uppercase tracking-wider font-semibold px-1.5 py-0.5 rounded border ${
        ok ? 'bg-pos-500/10 text-pos-400 border-pos-500/20' : 'bg-neg-500/10 text-neg-400 border-neg-500/20'}`}>
        {ok ? '✓ found' : '✗ missing'}
      </span>
      <span className="text-fg-soft font-medium w-16">{label}</span>
      <span className="text-fg-muted truncate">{detail}</span>
    </div>
  );
}
