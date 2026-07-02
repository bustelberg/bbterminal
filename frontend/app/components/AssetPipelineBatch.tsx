'use client';

import { useCallback, useEffect, useRef, useState } from 'react';
import { runSSE } from '../../lib/stream';
import { apiFetch } from '../../lib/apiFetch';
import { API_URL } from '../../lib/apiUrl';

type IngestItem = {
  type: 'item'; i: number; total: number; input: string; status: 'ok' | 'error' | 'skipped';
  analysis?: string; execution?: string; asset_class?: string;
  leveraged?: boolean; wrapper?: string | null; rows?: number; error?: string; reason?: string;
};
type IngestSummary = {
  type: 'summary'; processed: number; ok: number; failed: number; skipped: number;
  unique_assets: number; defaults_set: number;
  analysis_assets: number; executions: number; price_rows: number; volume_rows: number; est_price_mb: number;
};
type Storage = { analysis_assets: number; executions: number; price_rows: number; volume_rows: number; est_price_mb: number };
type Frame = IngestItem | IngestSummary | { type: 'error'; error: string };

export default function AssetPipelineBatch({ isins, onIngested }: { isins: string[]; onIngested?: () => void }) {
  const [running, setRunning] = useState(false);
  const [progress, setProgress] = useState<{ i: number; total: number } | null>(null);
  const [log, setLog] = useState<IngestItem[]>([]);
  const [summary, setSummary] = useState<IngestSummary | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [storage, setStorage] = useState<Storage | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  const loadStorage = useCallback(async () => {
    try {
      const r = await apiFetch(`${API_URL}/api/asset-pipeline/storage`);
      if (r.ok) setStorage((await r.json()) as Storage);
    } catch { /* ignore */ }
  }, []);
  useEffect(() => { void loadStorage(); }, [loadStorage]);

  const run = async () => {
    if (running || !isins.length) return;
    setRunning(true); setError(null); setSummary(null); setLog([]);
    setProgress({ i: 0, total: isins.length });
    const ac = new AbortController();
    abortRef.current = ac;
    try {
      await runSSE(
        `${API_URL}/api/asset-pipeline/ingest`,
        { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ identifiers: isins }) },
        (data) => {
          const d = data as Frame;
          if (d.type === 'item') { setProgress({ i: d.i, total: d.total }); setLog((l) => [d, ...l].slice(0, 12)); }
          else if (d.type === 'summary') { setSummary(d); void loadStorage(); onIngested?.(); }
          else if (d.type === 'error') { setError(d.error); }
        },
        ac.signal,
      );
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setRunning(false);
    }
  };
  const stop = () => { abortRef.current?.abort(); setRunning(false); };

  const pct = progress && progress.total > 0 ? Math.round((progress.i / progress.total) * 100) : 0;

  return (
    <section className="bg-card border border-neutral-800/40 rounded-xl p-5 space-y-3">
      <div className="flex items-center justify-between gap-3 flex-wrap">
        <h3 className="text-sm font-semibold text-fg-strong">Batch ingest &amp; storage</h3>
        {storage && (
          <span className="text-[11px] font-mono text-fg-faint">
            stored: {storage.analysis_assets} assets · {storage.executions} executions · {storage.price_rows.toLocaleString()} price / {storage.volume_rows.toLocaleString()} vol rows · ≈{storage.est_price_mb} MB
          </span>
        )}
      </div>

      <div className="flex items-center gap-2">
        <button
          type="button" onClick={() => void run()} disabled={running || isins.length === 0}
          className="text-sm px-4 py-2 rounded-lg bg-accent-600 hover:bg-accent-500 text-white disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
        >
          {running ? `Ingesting… ${progress?.i ?? 0}/${progress?.total ?? 0}` : `Ingest all ${isins.length} ISINs`}
        </button>
        {running && (
          <button type="button" onClick={stop} className="text-xs px-3 py-1.5 rounded-lg border border-neutral-700 text-fg-muted hover:text-neg-300 hover:border-neg-500/50 transition-colors">Stop</button>
        )}
        {isins.length === 0 && <span className="text-xs text-fg-faint">Upload a CSV above to enable.</span>}
      </div>

      {(running || log.length > 0) && (
        <div className="space-y-2">
          <div className="h-1.5 rounded-full bg-inset overflow-hidden">
            <div className="h-full bg-accent-500 transition-all" style={{ width: `${pct}%` }} />
          </div>
          <div className="max-h-48 overflow-auto rounded-lg border border-neutral-800/40 divide-y divide-neutral-800/20">
            {log.map((it) => (
              <div key={`${it.i}-${it.input}`} className="flex items-center gap-2 px-3 py-1 text-[11px] font-mono">
                <span className="text-fg-faint w-14 shrink-0">{it.i}/{it.total}</span>
                <span className={`w-3 shrink-0 ${it.status === 'ok' ? 'text-pos-400' : it.status === 'skipped' ? 'text-fg-faint' : 'text-neg-400'}`}>
                  {it.status === 'ok' ? '✓' : it.status === 'skipped' ? '⊘' : '✗'}
                </span>
                <span className="text-fg-soft w-28 shrink-0 truncate">{it.input}</span>
                {it.status === 'ok' ? (
                  <span className="text-fg-muted truncate">
                    → <span className="text-fg">{it.analysis}</span>
                    {it.execution && it.execution !== it.analysis && <span className="text-fg-subtle"> (trade {it.execution}{it.wrapper ? `, ${it.wrapper}` : ''})</span>}
                    {it.leveraged && <span className="text-warn-300"> ⚠lev</span>}
                    <span className="text-fg-faint"> · {it.rows?.toLocaleString()} rows</span>
                  </span>
                ) : it.status === 'skipped' ? (
                  <span className="text-fg-faint truncate">skipped · {it.asset_class ?? 'n/a'}</span>
                ) : (
                  <span className="text-neg-300 truncate">{it.error}</span>
                )}
              </div>
            ))}
          </div>
        </div>
      )}

      {error && <div className="bg-neg-500/10 border border-neg-500/20 rounded-lg px-3 py-2 text-xs text-neg-300">{error}</div>}

      {summary && (
        <div className="bg-inset rounded-lg px-4 py-3 space-y-3 text-xs">
          <div className="flex flex-wrap gap-x-4 gap-y-1">
            <span className="text-fg-soft font-medium">Done.</span>
            <span className="text-pos-400">{summary.ok} ok</span>
            {summary.skipped > 0 && <span className="text-fg-faint">{summary.skipped} skipped (bonds)</span>}
            {summary.failed > 0 && <span className="text-neg-400">{summary.failed} failed</span>}
            <span className="text-fg-muted">{summary.unique_assets} unique assets</span>
          </div>
          <div>
            <div className="text-fg-faint uppercase tracking-wide text-[10px] mb-1">What&apos;s stored now</div>
            <div className="grid grid-cols-2 sm:grid-cols-5 gap-2 max-w-3xl">
              <Stat label="analysis assets" v={summary.analysis_assets} />
              <Stat label="executions" v={summary.executions} />
              <Stat label="price rows" v={summary.price_rows} />
              <Stat label="volume rows" v={summary.volume_rows} />
              <Stat label="≈ size" v={`${summary.est_price_mb} MB`} />
            </div>
          </div>
          <div className="border-t border-neutral-800/40 pt-2">
            <div className="text-fg-faint uppercase tracking-wide text-[10px] mb-1">Schema</div>
            <dl className="grid grid-cols-[auto_1fr] gap-x-3 gap-y-0.5 text-[11px]">
              <dt className="font-mono text-fg-soft">asset_analysis</dt><dd className="text-fg-muted">unique symbol (dedup key) — backtest series</dd>
              <dt className="font-mono text-fg-soft">asset_execution</dt><dd className="text-fg-muted">per ISIN → asset (N:1); <span className="font-mono">is_default</span> = best to trade</dd>
              <dt className="font-mono text-fg-soft">asset_price</dt><dd className="text-fg-muted">daily <span className="font-mono">close</span>+<span className="font-mono">volume</span>, PK <span className="font-mono">(analysis_id, date)</span> — once per asset</dd>
            </dl>
            <p className="text-fg-faint mt-1 text-[10px]">≈88 B/row; ~40y ≈ 10k rows ≈ 0.9 MB. Extra ISINs of the same asset add ~0 price rows.</p>
          </div>
        </div>
      )}
    </section>
  );
}

function Stat({ label, v }: { label: string; v: number | string }) {
  return (
    <div className="bg-card rounded-lg px-3 py-2 border border-neutral-800/40">
      <div className="text-fg-faint text-[10px] uppercase tracking-wide">{label}</div>
      <div className="font-mono text-fg-strong text-sm">{typeof v === 'number' ? v.toLocaleString() : v}</div>
    </div>
  );
}
