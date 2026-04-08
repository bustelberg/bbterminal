'use client';

import { useState, useRef, useEffect, useCallback } from 'react';

const API_URL = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000';

type ProgressEvent = {
  type: 'progress';
  step: string;
  status: 'in_progress' | 'done';
  message: string;
};

type Portfolio = {
  portefeuille: string;
  depotbank: string;
  client: string;
  naam: string;
};

type PerfRow = {
  periode: string;
  beginvermogen: number | null;
  koersresultaat: number | null;
  opbrengsten: number | null;
  beleggingsresultaat: number | null;
  eindvermogen: number | null;
  rendement: number | null;
  cumulatief_rendement: number | null;
};

type PortfolioDetail = {
  portfolio_name: string;
  datum_van: string;
  datum_tot: string;
  rows: PerfRow[];
  cached?: boolean;
};

type YtdState = { status: 'loading' } | { status: 'done'; value: number | null; cached: boolean } | { status: 'error' };

function fmtEur(v: number | null): string {
  if (v == null) return '—';
  return v.toLocaleString('nl-NL', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function fmtPct(v: number | null): string {
  if (v == null) return '—';
  return (v >= 0 ? '+' : '') + v.toFixed(2) + '%';
}

function returnColor(v: number | null): string {
  if (v == null) return 'text-gray-500';
  return v >= 0 ? 'text-emerald-400' : 'text-rose-400';
}

// ─── Detail view ─────────────────────────────────────────────────────────────

function PortfolioDetailView({ detail, onBack }: { detail: PortfolioDetail; onBack: () => void }) {
  const last = detail.rows[detail.rows.length - 1];

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
            <h1 className="text-lg font-semibold text-white">{detail.portfolio_name}</h1>
            <p className="text-xs text-gray-500 mt-0.5">
              {detail.datum_van} to {detail.datum_tot} — {detail.rows.length} period{detail.rows.length !== 1 ? 's' : ''}
            </p>
          </div>
        </div>
        {last?.cumulatief_rendement != null && (
          <div className="text-right">
            <span className={`text-lg font-semibold ${returnColor(last.cumulatief_rendement)}`}>
              {fmtPct(last.cumulatief_rendement)}
            </span>
            <p className="text-xs text-gray-500 mt-0.5">YTD cumulative return</p>
          </div>
        )}
      </div>

      <div className="flex-1 overflow-auto px-8 py-4">
        <div className="bg-[#151821] rounded-xl border border-gray-800/40 overflow-hidden">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-800/60 text-gray-500 text-xs">
                <th className="px-4 py-3 text-left font-medium">Periode</th>
                <th className="px-3 py-3 text-right font-medium">Beginvermogen</th>
                <th className="px-3 py-3 text-right font-medium">Koersresultaat</th>
                <th className="px-3 py-3 text-right font-medium">Opbrengsten</th>
                <th className="px-3 py-3 text-right font-medium">Beleggingsresultaat</th>
                <th className="px-3 py-3 text-right font-medium">Eindvermogen</th>
                <th className="px-3 py-3 text-right font-medium">Rendement</th>
                <th className="px-3 py-3 text-right font-medium">Cumulatief</th>
              </tr>
            </thead>
            <tbody>
              {detail.rows.map((r, i) => (
                <tr key={i} className="border-b border-gray-800/30 hover:bg-white/[0.02] transition-colors">
                  <td className="px-4 py-2.5 text-gray-200 font-medium">{r.periode}</td>
                  <td className="px-3 py-2.5 text-right text-gray-400 font-mono text-xs">{fmtEur(r.beginvermogen)}</td>
                  <td className={`px-3 py-2.5 text-right font-mono text-xs ${returnColor(r.koersresultaat)}`}>{fmtEur(r.koersresultaat)}</td>
                  <td className="px-3 py-2.5 text-right text-gray-400 font-mono text-xs">{fmtEur(r.opbrengsten)}</td>
                  <td className={`px-3 py-2.5 text-right font-mono text-xs font-medium ${returnColor(r.beleggingsresultaat)}`}>{fmtEur(r.beleggingsresultaat)}</td>
                  <td className="px-3 py-2.5 text-right text-gray-300 font-mono text-xs">{fmtEur(r.eindvermogen)}</td>
                  <td className={`px-3 py-2.5 text-right font-mono text-xs font-medium ${returnColor(r.rendement)}`}>{fmtPct(r.rendement)}</td>
                  <td className={`px-3 py-2.5 text-right font-mono text-xs font-medium ${returnColor(r.cumulatief_rendement)}`}>{fmtPct(r.cumulatief_rendement)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

// ─── Spinner ─────────────────────────────────────────────────────────────────

function Spinner({ className = 'h-3.5 w-3.5' }: { className?: string }) {
  return (
    <svg className={`animate-spin ${className}`} viewBox="0 0 24 24" fill="none">
      <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
      <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
    </svg>
  );
}

// ─── Main ────────────────────────────────────────────────────────────────────

export default function AirsPortfolioUpload() {
  const [scanning, setScanning] = useState(false);
  const [progress, setProgress] = useState<ProgressEvent[]>([]);
  const [portfolios, setPortfolios] = useState<Portfolio[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState<string | null>(null);
  const [detail, setDetail] = useState<PortfolioDetail | null>(null);
  const [ytdMap, setYtdMap] = useState<Record<string, YtdState>>({});
  const [ytdStatus, setYtdStatus] = useState<string | null>(null);
  const abortRef = useRef(false);

  const loadYtdReturns = useCallback(async (portfolioList: Portfolio[]) => {
    abortRef.current = false;
    const total = portfolioList.length;
    let loaded = 0;
    let fromCache = 0;
    let fromDownload = 0;

    for (const p of portfolioList) {
      if (abortRef.current) break;
      const name = p.portefeuille;

      setYtdMap((prev) => ({ ...prev, [name]: { status: 'loading' } }));
      setYtdStatus(`Loading YTD returns... ${loaded + 1}/${total} — ${name}`);

      try {
        const res = await fetch(`${API_URL}/api/airs/portfolio/${encodeURIComponent(name)}`);
        if (!res.ok) throw new Error();
        const data: PortfolioDetail = await res.json();
        const last = data.rows[data.rows.length - 1];
        const ytd = last?.cumulatief_rendement ?? null;
        const cached = data.cached ?? false;
        if (cached) fromCache++; else fromDownload++;
        setYtdMap((prev) => ({ ...prev, [name]: { status: 'done', value: ytd, cached } }));
      } catch {
        setYtdMap((prev) => ({ ...prev, [name]: { status: 'error' } }));
      }
      loaded++;
    }

    if (!abortRef.current) {
      const parts: string[] = [];
      if (fromCache > 0) parts.push(`${fromCache} from cache`);
      if (fromDownload > 0) parts.push(`${fromDownload} downloaded`);
      setYtdStatus(`YTD returns loaded — ${parts.join(', ')}`);
      setTimeout(() => setYtdStatus(null), 5000);
    }
  }, []);

  function startScan() {
    abortRef.current = true;
    setScanning(true);
    setProgress([]);
    setPortfolios([]);
    setError(null);
    setDetail(null);
    setYtdMap({});
    setYtdStatus(null);

    const eventSource = new EventSource(`${API_URL}/api/airs/scan`);
    eventSource.onmessage = (event) => {
      const data = JSON.parse(event.data);
      if (data.type === 'progress') {
        setProgress((prev) => {
          if (data.status === 'done') {
            // Replace all in_progress entries for this step with the done message
            const filtered = prev.filter((p) => !(p.step === data.step && p.status === 'in_progress'));
            return [...filtered, data];
          }
          return [...prev, data];
        });
      } else if (data.type === 'portfolios') {
        setPortfolios(data.data);
        // Start loading YTD returns in background
        loadYtdReturns(data.data);
      } else if (data.type === 'done') {
        setScanning(false);
        eventSource.close();
      } else if (data.type === 'error') {
        setError(data.message);
        setScanning(false);
        eventSource.close();
      }
    };
    eventSource.onerror = () => {
      setError('Connection lost');
      setScanning(false);
      eventSource.close();
    };
  }

  async function openPortfolio(name: string) {
    setLoading(name);
    setError(null);
    try {
      const res = await fetch(`${API_URL}/api/airs/portfolio/${encodeURIComponent(name)}`);
      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        throw new Error(body.detail ?? `HTTP ${res.status}`);
      }
      const data: PortfolioDetail = await res.json();
      setDetail(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(null);
    }
  }

  function statusIcon(status: string) {
    if (status === 'done') return <span className="text-emerald-400">&#10003;</span>;
    if (status === 'in_progress') return <span className="text-indigo-400 animate-pulse">&#9679;</span>;
    return <span className="text-gray-500">&#9675;</span>;
  }

  function renderYtd(name: string) {
    const state = ytdMap[name];
    if (!state) return <span className="text-gray-600">—</span>;
    if (state.status === 'loading') return <Spinner className="h-3 w-3 text-indigo-400" />;
    if (state.status === 'error') return <span className="text-rose-400 text-xs">err</span>;
    return (
      <span className={`font-mono text-xs font-medium ${returnColor(state.value)}`}>
        {fmtPct(state.value)}
      </span>
    );
  }

  if (detail) {
    return <PortfolioDetailView detail={detail} onBack={() => setDetail(null)} />;
  }

  return (
    <div className="flex flex-col h-full">
      <div className="px-8 py-5 border-b border-gray-800/60 flex items-center justify-between gap-4">
        <div>
          <h1 className="text-lg font-semibold text-white">AIRS Portfolio Scanner</h1>
          <p className="text-xs text-gray-500 mt-0.5">
            Scan broker system for available portfolios
          </p>
        </div>
        <button
          onClick={startScan}
          disabled={scanning || loading !== null}
          className="px-4 py-2 rounded-lg text-sm font-medium bg-indigo-600 hover:bg-indigo-500 text-white transition-colors disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2"
        >
          {scanning && <Spinner className="h-4 w-4" />}
          {scanning ? 'Scanning...' : 'Start Scan'}
        </button>
      </div>

      {error && (
        <div className="mx-8 mt-4 px-4 py-3 text-sm text-rose-400 bg-rose-500/10 border border-rose-500/20 rounded-lg flex items-center justify-between">
          <span>{error}</span>
          <button onClick={() => setError(null)} className="text-gray-500 hover:text-white ml-3 text-xs">Dismiss</button>
        </div>
      )}

      {/* YTD loading status bar */}
      {ytdStatus && (
        <div className="mx-8 mt-4 px-4 py-2.5 text-xs text-gray-400 bg-[#151821] border border-gray-800/40 rounded-lg flex items-center gap-2">
          {ytdStatus.startsWith('Loading') && <Spinner className="h-3 w-3 text-indigo-400" />}
          {ytdStatus.startsWith('YTD returns loaded') && <span className="text-emerald-400">&#10003;</span>}
          {ytdStatus}
        </div>
      )}

      <div className="flex-1 overflow-auto px-8 py-6 space-y-6">
        {/* Progress log */}
        {progress.length > 0 && (
          <div className="bg-[#151821] rounded-xl border border-gray-800/40 px-5 py-4">
            <h2 className="text-sm font-medium text-gray-400 mb-3">Progress</h2>
            <div className="space-y-1.5">
              {progress.map((p, i) => (
                <div key={i} className="flex items-center gap-2.5 text-sm">
                  {statusIcon(p.status)}
                  <span className={p.status === 'done' ? 'text-gray-400' : 'text-gray-200'}>
                    {p.message}
                  </span>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Portfolio table */}
        {portfolios.length > 0 && (
          <div className="bg-[#151821] rounded-xl border border-gray-800/40 overflow-hidden">
            <div className="px-5 py-3 border-b border-gray-800/40">
              <h2 className="text-sm font-medium text-gray-400">
                Portfolios ({portfolios.length} found) — click to view details
              </h2>
            </div>
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-800/60 text-gray-500 text-xs">
                  <th className="px-5 py-3 text-left font-medium w-10">#</th>
                  <th className="px-3 py-3 text-left font-medium">Portefeuille</th>
                  <th className="px-3 py-3 text-left font-medium w-20">Dp</th>
                  <th className="px-3 py-3 text-left font-medium w-24">Client</th>
                  <th className="px-3 py-3 text-left font-medium">Naam</th>
                  <th className="px-3 py-3 text-right font-medium w-24">YTD</th>
                </tr>
              </thead>
              <tbody>
                {portfolios.map((p, i) => (
                  <tr
                    key={i}
                    className="border-b border-gray-800/30 hover:bg-white/[0.02] transition-colors cursor-pointer group"
                    onClick={() => !loading && openPortfolio(p.portefeuille)}
                  >
                    <td className="px-5 py-2.5 text-gray-500 font-mono text-xs">{i + 1}</td>
                    <td className="px-3 py-2.5 text-gray-200 font-medium group-hover:text-indigo-300 transition-colors">
                      {loading === p.portefeuille ? (
                        <span className="flex items-center gap-2">
                          <Spinner className="h-3.5 w-3.5 text-indigo-400" />
                          {p.portefeuille}
                        </span>
                      ) : p.portefeuille}
                    </td>
                    <td className="px-3 py-2.5 text-gray-400 text-xs">{p.depotbank}</td>
                    <td className="px-3 py-2.5 text-gray-400 text-xs">{p.client}</td>
                    <td className="px-3 py-2.5 text-gray-300">{p.naam}</td>
                    <td className="px-3 py-2.5 text-right">{renderYtd(p.portefeuille)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        {/* Empty state */}
        {progress.length === 0 && portfolios.length === 0 && !scanning && (
          <div className="flex flex-col items-center justify-center py-20 text-center">
            <div className="w-16 h-16 rounded-2xl bg-gray-800/50 flex items-center justify-center mb-4">
              <svg className="w-8 h-8 text-gray-600" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M21 21l-5.197-5.197m0 0A7.5 7.5 0 105.196 5.196a7.5 7.5 0 0010.607 10.607z" />
              </svg>
            </div>
            <p className="text-sm font-medium text-gray-400">No scan results yet</p>
            <p className="text-xs text-gray-600 mt-1">Click &quot;Start Scan&quot; to connect to the broker system</p>
          </div>
        )}
      </div>
    </div>
  );
}
