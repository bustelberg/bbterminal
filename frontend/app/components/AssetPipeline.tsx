'use client';

import { useState } from 'react';
import { apiFetch } from '../../lib/apiFetch';
import { API_URL } from '../../lib/apiUrl';
import AssetNav from './AssetNav';
import AssetPipelineTable from './AssetPipelineTable';

type ScanCol = { name: string; count: number; isins: string[] };
type Scan = { filename: string; rows: number; columns: ScanCol[] };

/** Asset pipeline. Upload a CSV/Excel of ISINs → pick the ISIN column → the
 * valid ISINs are stored in the DB queue, and the standalone worker resolves
 * each to its yfinance instrument. This page is the upload + the results grid. */
export default function AssetPipeline() {
  const [catalogReload, setCatalogReload] = useState(0);
  const [scan, setScan] = useState<Scan | null>(null);
  const [pickedCol, setPickedCol] = useState('');
  const [scanning, setScanning] = useState(false);
  const [enqueuing, setEnqueuing] = useState(false);
  const [msg, setMsg] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const onFile = async (file: File) => {
    setScanning(true); setError(null); setMsg(null); setScan(null); setPickedCol('');
    try {
      const fd = new FormData();
      fd.append('file', file);
      const r = await apiFetch(`${API_URL}/api/asset-pipeline/upload/scan`, { method: 'POST', body: fd });
      const b = await r.json().catch(() => null);
      if (!r.ok) { setError(b?.detail ?? `HTTP ${r.status}`); return; }
      const s = b as Scan;
      setScan(s);
      setPickedCol(s.columns.find((c) => c.count > 0)?.name ?? s.columns[0]?.name ?? '');
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally { setScanning(false); }
  };

  const col = scan?.columns.find((c) => c.name === pickedCol) ?? null;

  const enqueue = async () => {
    if (!col || !col.isins.length) return;
    setEnqueuing(true); setError(null); setMsg(null);
    try {
      const r = await apiFetch(`${API_URL}/api/asset-pipeline/queue`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ identifiers: col.isins }),
      });
      const b = await r.json().catch(() => null);
      if (!r.ok) { setError(b?.detail ?? `HTTP ${r.status}`); return; }
      setMsg(`Found ${col.isins.length.toLocaleString()} valid ISINs in “${col.name}” · `
        + `${(b.queued ?? 0).toLocaleString()} new added · `
        + `${(b.skipped_existing ?? 0).toLocaleString()} already in DB (dupes). The background worker will process them.`);
      setCatalogReload((x) => x + 1);
      setScan(null); setPickedCol('');
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally { setEnqueuing(false); }
  };

  return (
    <div className="min-h-screen bg-page text-fg">
      <AssetNav />
      <div className="px-8 py-5 border-b border-neutral-800/40">
        <h1 className="text-xl font-semibold text-fg-strong">Asset Pipeline</h1>
        <p className="text-sm text-fg-subtle mt-1">
          Upload a CSV or Excel file of ISINs and pick the ISIN column — the background worker resolves each to its yfinance instrument (OpenFIGI-anchored) and stores prices.
        </p>
      </div>

      <div className="px-8 py-6 space-y-4">
        <section className="bg-card border border-neutral-800/40 rounded-xl p-5">
          <div className="flex items-center gap-3 flex-wrap">
            <label className={`text-sm px-4 py-2 rounded-lg bg-accent-600 hover:bg-accent-500 text-white transition-colors ${scanning ? 'opacity-40 cursor-not-allowed' : 'cursor-pointer'}`}>
              {scanning ? 'Scanning…' : 'Upload CSV / Excel'}
              <input
                type="file" className="hidden" disabled={scanning}
                accept=".csv,.tsv,.txt,.xlsx,.xlsm,.xls,text/csv,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/vnd.ms-excel"
                onChange={(e) => { const f = e.target.files?.[0]; if (f) void onFile(f); e.target.value = ''; }}
              />
            </label>
            {scan && <span className="text-[11px] text-fg-faint font-mono">{scan.filename} · {scan.rows.toLocaleString()} rows</span>}
          </div>

          {/* Column picker — choose which column holds the ISINs. */}
          {scan && (
            <div className="mt-3 flex items-center gap-3 flex-wrap">
              <label className="text-sm text-fg-muted flex items-center gap-2">
                ISIN column
                <select value={pickedCol} onChange={(e) => setPickedCol(e.target.value)}
                  className="bg-page border border-neutral-700 rounded-lg px-2 py-1.5 text-sm max-w-[260px] focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30">
                  {scan.columns.map((c) => (
                    <option key={c.name} value={c.name}>{c.name} ({c.count.toLocaleString()} ISINs)</option>
                  ))}
                </select>
              </label>
              <button
                type="button" onClick={() => void enqueue()} disabled={enqueuing || !col || col.count === 0}
                className="text-sm px-4 py-2 rounded-lg bg-accent-600 hover:bg-accent-500 text-white disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
              >
                {enqueuing ? 'Adding…' : `Add ${(col?.count ?? 0).toLocaleString()} ISINs to queue`}
              </button>
              {col && col.count === 0 && <span className="text-[11px] text-warn-300">No valid ISINs in this column — pick another.</span>}
            </div>
          )}

          {msg && <div className="mt-3 text-[11px] text-pos-400 bg-pos-500/10 border border-pos-500/20 rounded-lg px-3 py-2">{msg}</div>}
          {error && <div className="mt-3 text-[11px] text-neg-300 bg-neg-500/10 border border-neg-500/20 rounded-lg px-3 py-2">{error}</div>}
        </section>

        <AssetPipelineTable reloadSignal={catalogReload} />
      </div>
    </div>
  );
}
