'use client';

import { useState } from 'react';
import { apiFetch } from '../../lib/apiFetch';
import { API_URL } from '../../lib/apiUrl';
import AssetPipelineTable from './AssetPipelineTable';

type ScanCol = { name: string; count: number; isins: string[] };
type Scan = { filename: string; rows: number; columns: ScanCol[] };

/** SHAPE only — 2 letters, 9 alphanumerics, 1 check digit. Mirrors the backend's ISIN_RE.
 * It exists to catch a typo before a round-trip, NOT to decide validity: whether an ISIN
 * resolves to an instrument is the resolver's call, and a regex has no opinion on it. */
const ISIN_RE = /^[A-Z]{2}[A-Z0-9]{9}[0-9]$/;

/** Asset pipeline. Upload a CSV/Excel of ISINs → pick the ISIN column → the
 * valid ISINs are stored in the DB queue, and the standalone worker resolves
 * each to its yfinance instrument. This page is the upload + the results grid. */
export default function AssetPipeline() {
  const [catalogReload, setCatalogReload] = useState(0);
  const [scan, setScan] = useState<Scan | null>(null);
  const [pickedCol, setPickedCol] = useState('');
  const [scanning, setScanning] = useState(false);
  const [enqueuing, setEnqueuing] = useState(false);
  const [leonteqUploading, setLeonteqUploading] = useState(false);
  const [msg, setMsg] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [isin, setIsin] = useState('');
  const [adding, setAdding] = useState(false);

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

  // Leonteq (lynqs) CSV — id, ticker, name, productType, ric, isin, currency.
  // Posts the whole file: replaces the Leonteq-Verified set (name/ccy/productType
  // per ISIN) AND queues the ISINs for ingestion. No column-pick — shape is known.
  const onLeonteqFile = async (file: File) => {
    setLeonteqUploading(true); setError(null); setMsg(null); setScan(null); setPickedCol('');
    try {
      const fd = new FormData();
      fd.append('file', file);
      const r = await apiFetch(`${API_URL}/api/asset-pipeline/leonteq/upload`, { method: 'POST', body: fd });
      const b = await r.json().catch(() => null);
      if (!r.ok) { setError(b?.detail ?? `HTTP ${r.status}`); return; }
      setMsg(`Leonteq list uploaded: ${(b.members ?? 0).toLocaleString()} verified instruments `
        + `(${(b.valid_isins ?? 0).toLocaleString()} valid ISINs of ${(b.rows ?? 0).toLocaleString()} rows) · `
        + `${(b.seeded ?? 0).toLocaleString()} now shown in the grid (queued) · `
        + `${(b.queue?.queued ?? 0).toLocaleString()} to resolve, `
        + `${(b.queue?.skipped_existing ?? 0).toLocaleString()} already stored. The background worker enriches queued rows with yfinance + OpenFIGI data.`);
      setCatalogReload((x) => x + 1);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally { setLeonteqUploading(false); }
  };

  // Add ONE row by ISIN. Resolves synchronously (OpenFIGI-anchored) and stores the price
  // series, so the row lands fully populated rather than sitting in the queue as 'queued'.
  //
  // A 422 is NOT a failure: the backend records an unresolvable ISIN in the grid anyway
  // (as not_found/bond) and returns the resolver's reason. So we reload the grid on 422
  // too — the row IS there — and say why it's unmapped instead of claiming nothing
  // happened.
  const isinLooksValid = ISIN_RE.test(isin.trim().toUpperCase());

  const addIsin = async () => {
    const ident = isin.trim().toUpperCase();
    if (!ident) return;
    setAdding(true); setError(null); setMsg(null);
    try {
      const r = await apiFetch(`${API_URL}/api/asset-pipeline/store`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ identifier: ident }),
      });
      const b = await r.json().catch(() => null);
      // 409 — already in the grid. The backend REFUSES to re-resolve it, because a
      // re-resolve can silently repoint a good row onto a thin foreign listing when
      // Yahoo's search comes back empty (it repointed Alphabet from GOOGL to a Vienna
      // line with 1/75,000th the liquidity). Not an error — just nothing to do.
      if (r.status === 409) {
        setMsg(b?.detail ?? `${ident} is already in the grid.`);
        setIsin('');
        return;
      }
      // 422 — a NEW ISIN that got recorded but not resolved. The row IS there, unmapped.
      if (r.status === 422) {
        setError(`${ident} added to the grid but NOT resolved: ${b?.detail ?? 'no instrument found'}. `
          + `It shows as an unmapped row — use the row's Resolve action to retry.`);
        setCatalogReload((x) => x + 1);
        setIsin('');
        return;
      }
      if (!r.ok) { setError(b?.detail ?? `HTTP ${r.status}`); return; }
      const sym = b?.analysis?.symbol ?? b?.symbol ?? '';
      const bars = b?.rows ?? b?.stored_rows ?? null;
      setMsg(`${ident} added${sym ? ` → ${sym}` : ''}${bars != null ? ` · ${Number(bars).toLocaleString()} price bars stored` : ''}.`);
      setIsin('');
      setCatalogReload((x) => x + 1);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally { setAdding(false); }
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
            <label className={`text-sm px-4 py-2 rounded-lg border border-accent-500/40 text-accent-300 hover:bg-accent-500/10 transition-colors ${leonteqUploading ? 'opacity-40 cursor-not-allowed' : 'cursor-pointer'}`}>
              {leonteqUploading ? 'Uploading…' : 'Upload Leonteq (lynqs) CSV'}
              <input
                type="file" className="hidden" disabled={leonteqUploading}
                accept=".csv,.tsv,.txt,.xlsx,.xlsm,.xls,text/csv,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/vnd.ms-excel"
                onChange={(e) => { const f = e.target.files?.[0]; if (f) void onLeonteqFile(f); e.target.value = ''; }}
              />
            </label>
            {scan && <span className="text-[12px] text-fg-faint font-mono">{scan.filename} · {scan.rows.toLocaleString()} rows</span>}
          </div>

          {/* Add a single row by ISIN — the one-off counterpart to the bulk upload. */}
          <div className="mt-3 flex items-center gap-2 flex-wrap">
            <input
              value={isin}
              onChange={(e) => setIsin(e.target.value.toUpperCase())}
              onKeyDown={(e) => { if (e.key === 'Enter' && isinLooksValid) void addIsin(); }}
              placeholder="Add one ISIN — e.g. US0378331005"
              spellCheck={false}
              className="bg-page border border-neutral-700 rounded-lg px-3 py-2 text-sm font-mono text-fg w-64 focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30"
            />
            <button
              type="button" onClick={() => void addIsin()} disabled={adding || !isinLooksValid}
              className="text-sm px-4 py-2 rounded-lg border border-accent-500/40 text-accent-300 hover:bg-accent-500/10 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
            >
              {adding ? 'Resolving…' : 'Add ISIN'}
            </button>
            {/* Format-only check, on purpose: it catches a typo without a round-trip, but
                whether the ISIN actually resolves is the backend's call, not a regex's. */}
            {isin && !isinLooksValid && (
              <span className="text-[12px] text-warn-300">
                Not an ISIN — 2 letters, 9 alphanumerics, 1 check digit (12 chars).
              </span>
            )}
            <span className="text-[12px] text-fg-faint">
              Resolves immediately (OpenFIGI-anchored) and stores its price series, so the row
              lands complete rather than queued.
            </span>
          </div>
          <p className="mt-2 text-[12px] text-fg-faint">
            <span className="text-accent-300 font-medium">Leonteq (lynqs) CSV</span> — columns id, ticker, name, productType, ric, isin, currency — replaces the Leonteq-Verified set (name/currency/productType shown in the grid + a badge on each verified row) and queues its ISINs for ingestion.
          </p>

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
              {col && col.count === 0 && <span className="text-[12px] text-warn-300">No valid ISINs in this column — pick another.</span>}
            </div>
          )}

          {msg && <div className="mt-3 text-[12px] text-pos-400 bg-pos-500/10 border border-pos-500/20 rounded-lg px-3 py-2">{msg}</div>}
          {error && <div className="mt-3 text-[12px] text-neg-300 bg-neg-500/10 border border-neg-500/20 rounded-lg px-3 py-2">{error}</div>}
        </section>

        <AssetPipelineTable reloadSignal={catalogReload} />
      </div>
    </div>
  );
}
