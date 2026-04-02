'use client';

import { useState, useRef, useEffect, useCallback } from 'react';

const API_URL = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000';

type Holding = {
  holding_name: string;
  quantity: number | null;
  currency: string;
  weight: number | null;
  start_value_eur: number | null;
  current_value_eur: number | null;
  ytd_return_eur: number | null;
  ytd_return_pct: number | null;
  ytd_return_local_pct: number | null;
};

type ParseResult = {
  holdings: Holding[];
  total_start_eur: number | null;
  total_current_eur: number | null;
  total_ytd_eur: number | null;
  total_ytd_pct: number | null;
};

type CachedPortfolio = {
  id: string;
  result: ParseResult;
  uploadedAt: string;
  fileName: string;
};

const CACHE_KEY = 'airs_portfolios';

function loadAll(): CachedPortfolio[] {
  try {
    const raw = localStorage.getItem(CACHE_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed) && parsed.result) {
      const entry: CachedPortfolio = { id: crypto.randomUUID(), ...parsed };
      localStorage.setItem(CACHE_KEY, JSON.stringify([entry]));
      return [entry];
    }
    return parsed;
  } catch { return []; }
}

function saveAll(entries: CachedPortfolio[]) {
  try { localStorage.setItem(CACHE_KEY, JSON.stringify(entries)); } catch {}
}

function addToCache(result: ParseResult, fileName: string): CachedPortfolio {
  const entries = loadAll();
  const entry: CachedPortfolio = { id: crypto.randomUUID(), result, uploadedAt: new Date().toISOString(), fileName };
  entries.unshift(entry);
  saveAll(entries);
  return entry;
}

function removeFromCache(id: string) {
  saveAll(loadAll().filter((e) => e.id !== id));
}

// ─── Formatters ───────────────────────────────────────────────────────────────

function fmtEur(v: number | null): string {
  if (v == null) return '—';
  return v.toLocaleString('nl-NL', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function fmtPct(v: number | null): string {
  if (v == null) return '—';
  return (v >= 0 ? '+' : '') + (v * 100).toFixed(2) + '%';
}

function returnColor(v: number | null): string {
  if (v == null) return 'text-gray-500';
  return v >= 0 ? 'text-emerald-400' : 'text-rose-400';
}

function fmtDate(iso: string): string {
  return new Date(iso).toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric', hour: '2-digit', minute: '2-digit' });
}

// ─── Detail view ──────────────────────────────────────────────────────────────

function PortfolioDetail({ portfolio, onBack }: { portfolio: CachedPortfolio; onBack: () => void }) {
  const { result } = portfolio;

  return (
    <div className="flex flex-col h-full">
      <div className="px-8 py-5 border-b border-gray-800/60 flex items-center justify-between gap-4">
        <div className="flex items-center gap-4">
          <button
            onClick={onBack}
            className="px-3 py-1.5 rounded-lg text-sm text-gray-400 hover:text-white hover:bg-white/5 transition-colors"
          >
            &larr; Back
          </button>
          <div>
            <h1 className="text-lg font-semibold text-white">{portfolio.fileName}</h1>
            <p className="text-xs text-gray-500 mt-0.5">
              {result.holdings.length} holdings — uploaded {fmtDate(portfolio.uploadedAt)}
            </p>
          </div>
        </div>
        {result.total_ytd_pct != null && (
          <div className="text-right">
            <span className={`text-lg font-semibold ${returnColor(result.total_ytd_pct)}`}>
              {fmtPct(result.total_ytd_pct)}
            </span>
            <p className="text-xs text-gray-500 mt-0.5">
              {fmtEur(result.total_ytd_eur)} EUR
            </p>
          </div>
        )}
      </div>

      <div className="flex-1 overflow-auto px-8 py-4">
        <div className="bg-[#151821] rounded-xl border border-gray-800/40 overflow-hidden">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-800/60 text-gray-500 text-xs">
                <th className="px-4 py-3 text-left font-medium">Holding</th>
                <th className="px-3 py-3 text-right font-medium w-16">Qty</th>
                <th className="px-3 py-3 text-right font-medium w-12">Ccy</th>
                <th className="px-3 py-3 text-right font-medium w-28">Start EUR</th>
                <th className="px-3 py-3 text-right font-medium w-28">Current EUR</th>
                <th className="px-3 py-3 text-right font-medium w-28">YTD EUR</th>
                <th className="px-3 py-3 text-right font-medium w-24">YTD % EUR</th>
                <th className="px-3 py-3 text-right font-medium w-20">Weight</th>
              </tr>
            </thead>
            <tbody>
              {result.holdings.map((h, i) => (
                <tr key={i} className="border-b border-gray-800/30 hover:bg-white/[0.02] transition-colors">
                  <td className="px-4 py-2.5 text-gray-200 font-medium">{h.holding_name}</td>
                  <td className="px-3 py-2.5 text-right text-gray-400 font-mono text-xs">{h.quantity ?? '—'}</td>
                  <td className="px-3 py-2.5 text-right text-gray-500 text-xs">{h.currency || '—'}</td>
                  <td className="px-3 py-2.5 text-right text-gray-400 font-mono text-xs">{fmtEur(h.start_value_eur)}</td>
                  <td className="px-3 py-2.5 text-right text-gray-300 font-mono text-xs">{fmtEur(h.current_value_eur)}</td>
                  <td className={`px-3 py-2.5 text-right font-mono text-xs ${returnColor(h.ytd_return_eur)}`}>{fmtEur(h.ytd_return_eur)}</td>
                  <td className={`px-3 py-2.5 text-right font-mono text-xs font-medium ${returnColor(h.ytd_return_pct)}`}>{fmtPct(h.ytd_return_pct)}</td>
                  <td className="px-3 py-2.5 text-right text-gray-500 font-mono text-xs">{h.weight != null ? (h.weight * 100).toFixed(2) + '%' : '—'}</td>
                </tr>
              ))}
            </tbody>
            <tfoot>
              <tr className="border-t border-gray-700/60 bg-white/[0.02]">
                <td className="px-4 py-3 text-white font-semibold">Total</td>
                <td></td>
                <td></td>
                <td className="px-3 py-3 text-right text-gray-300 font-mono text-xs font-medium">{fmtEur(result.total_start_eur)}</td>
                <td className="px-3 py-3 text-right text-white font-mono text-xs font-semibold">{fmtEur(result.total_current_eur)}</td>
                <td className={`px-3 py-3 text-right font-mono text-xs font-semibold ${returnColor(result.total_ytd_eur)}`}>{fmtEur(result.total_ytd_eur)}</td>
                <td className={`px-3 py-3 text-right font-mono text-xs font-semibold ${returnColor(result.total_ytd_pct)}`}>{fmtPct(result.total_ytd_pct)}</td>
                <td className="px-3 py-3 text-right text-gray-400 font-mono text-xs font-medium">
                  {(result.holdings.reduce((s, h) => s + (h.weight ?? 0), 0) * 100).toFixed(2)}%
                </td>
              </tr>
            </tfoot>
          </table>
        </div>
      </div>
    </div>
  );
}

// ─── Main ─────────────────────────────────────────────────────────────────────

export default function AirsPortfolioUpload() {
  const [portfolios, setPortfolios] = useState<CachedPortfolio[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [parsing, setParsing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [dragging, setDragging] = useState(false);
  const fileRef = useRef<HTMLInputElement>(null);
  const dragCounter = useRef(0);

  useEffect(() => { setPortfolios(loadAll()); }, []);

  const selectPortfolio = useCallback((id: string | null, pushState = true) => {
    setSelectedId(id);
    if (pushState) {
      history.pushState({ portfolioId: id ?? null }, '');
    }
  }, []);

  useEffect(() => {
    function onPopState(e: PopStateEvent) {
      setSelectedId(e.state?.portfolioId ?? null);
    }
    window.addEventListener('popstate', onPopState);
    return () => window.removeEventListener('popstate', onPopState);
  }, []);

  async function uploadFiles(files: File[]) {
    setParsing(true);
    setError(null);
    let lastEntry: CachedPortfolio | null = null;

    for (const file of files) {
      const form = new FormData();
      form.append('file', file);
      try {
        const res = await fetch(`${API_URL}/api/portfolios/parse`, {
          method: 'POST',
          body: form,
        });
        if (!res.ok) {
          const body = await res.json().catch(() => ({}));
          throw new Error(`${file.name}: ${body.detail ?? `HTTP ${res.status}`}`);
        }
        const data: ParseResult = await res.json();
        lastEntry = addToCache(data, file.name);
        setPortfolios(loadAll());
      } catch (err) {
        setError(err instanceof Error ? err.message : String(err));
      }
    }

    if (files.length === 1 && lastEntry) {
      selectPortfolio(lastEntry.id);
    }
    setParsing(false);
  }

  function handleDelete(id: string) {
    removeFromCache(id);
    setPortfolios(loadAll());
    if (selectedId === id) setSelectedId(null);
  }

  function handleInputChange(e: React.ChangeEvent<HTMLInputElement>) {
    const files = e.target.files;
    if (files && files.length > 0) uploadFiles(Array.from(files));
    if (fileRef.current) fileRef.current.value = '';
  }

  function handleDragEnter(e: React.DragEvent) { e.preventDefault(); dragCounter.current++; setDragging(true); }
  function handleDragLeave(e: React.DragEvent) { e.preventDefault(); dragCounter.current--; if (dragCounter.current === 0) setDragging(false); }
  function handleDragOver(e: React.DragEvent) { e.preventDefault(); }
  function handleDrop(e: React.DragEvent) {
    e.preventDefault();
    dragCounter.current = 0;
    setDragging(false);
    const files = Array.from(e.dataTransfer.files).filter((f) => /\.xlsx?$/i.test(f.name));
    if (files.length > 0) uploadFiles(files);
  }

  const selected = selectedId ? portfolios.find((p) => p.id === selectedId) : null;
  if (selected) {
    return <PortfolioDetail portfolio={selected} onBack={() => selectPortfolio(null)} />;
  }

  return (
    <div
      className="flex flex-col h-full relative"
      onDragEnter={handleDragEnter}
      onDragLeave={handleDragLeave}
      onDragOver={handleDragOver}
      onDrop={handleDrop}
    >
      {dragging && (
        <div className="absolute inset-0 z-50 bg-[#0f1117]/90 backdrop-blur-sm border-2 border-dashed border-indigo-500/50 rounded-xl flex items-center justify-center">
          <div className="text-center">
            <p className="text-base font-medium text-white">Drop Excel files here</p>
            <p className="text-sm text-gray-400 mt-1">Supports .xlsx and .xls</p>
          </div>
        </div>
      )}

      <div className="px-8 py-5 border-b border-gray-800/60 flex items-center justify-between gap-4">
        <div>
          <h1 className="text-lg font-semibold text-white">AIRS Portfolio</h1>
          <p className="text-xs text-gray-500 mt-0.5">
            {portfolios.length} portfolio{portfolios.length !== 1 ? 's' : ''} cached
          </p>
        </div>
        <div className="flex items-center gap-2">
          {portfolios.length > 0 && (
            <button
              onClick={() => { if (confirm('Delete all cached portfolios?')) { saveAll([]); setPortfolios([]); setSelectedId(null); } }}
              className="px-4 py-2 rounded-lg text-sm font-medium text-rose-400 hover:bg-rose-500/10 transition-colors"
            >
              Delete all
            </button>
          )}
          <label className="px-4 py-2 rounded-lg text-sm font-medium bg-indigo-600 hover:bg-indigo-500 text-white transition-colors cursor-pointer">
            {parsing ? 'Parsing...' : 'Upload Excel'}
            <input
              ref={fileRef}
              type="file"
              accept=".xlsx,.xls"
              multiple
              onChange={handleInputChange}
              disabled={parsing}
              className="hidden"
            />
          </label>
        </div>
      </div>

      {error && (
        <div className="mx-8 mt-4 px-4 py-3 text-sm text-rose-400 bg-rose-500/10 border border-rose-500/20 rounded-lg flex items-center justify-between">
          <span>{error}</span>
          <button onClick={() => setError(null)} className="text-gray-500 hover:text-white ml-3 text-xs">Dismiss</button>
        </div>
      )}

      <div className="flex-1 overflow-auto px-8 py-6">
        {portfolios.length === 0 && !parsing && (
          <div className="flex flex-col items-center justify-center py-20 text-center">
            <div className="w-16 h-16 rounded-2xl bg-gray-800/50 flex items-center justify-center mb-4">
              <svg className="w-8 h-8 text-gray-600" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M3 16.5v2.25A2.25 2.25 0 005.25 21h13.5A2.25 2.25 0 0021 18.75V16.5m-13.5-9L12 3m0 0l4.5 4.5M12 3v13.5" />
              </svg>
            </div>
            <p className="text-sm font-medium text-gray-400">Drag & drop Excel files here</p>
            <p className="text-xs text-gray-600 mt-1">or use the Upload button above</p>
          </div>
        )}

        {portfolios.length > 0 && (
          <div className="space-y-3">
            {portfolios.map((p) => (
              <div
                key={p.id}
                className="flex items-center gap-4 bg-[#151821] border border-gray-800/40 rounded-xl px-5 py-4 hover:bg-[#1a1d27] hover:border-gray-700/50 transition-all cursor-pointer group"
                onClick={() => selectPortfolio(p.id)}
              >
                <div className="flex-1 min-w-0">
                  <p className="text-sm font-medium text-white truncate group-hover:text-indigo-300 transition-colors">{p.fileName}</p>
                  <p className="text-xs text-gray-500 mt-1">
                    {p.result.holdings.length} holdings — {fmtDate(p.uploadedAt)}
                  </p>
                </div>
                {p.result.total_ytd_pct != null && (
                  <span className={`text-base font-semibold shrink-0 ${returnColor(p.result.total_ytd_pct)}`}>
                    {fmtPct(p.result.total_ytd_pct)}
                  </span>
                )}
                {p.result.total_current_eur != null && (
                  <span className="text-xs text-gray-500 shrink-0 font-mono">
                    {fmtEur(p.result.total_current_eur)} EUR
                  </span>
                )}
                <button
                  onClick={(e) => { e.stopPropagation(); handleDelete(p.id); }}
                  className="px-2 py-1 rounded-lg text-xs text-gray-600 hover:text-rose-400 hover:bg-rose-500/10 transition-colors shrink-0 opacity-0 group-hover:opacity-100"
                >
                  Delete
                </button>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
