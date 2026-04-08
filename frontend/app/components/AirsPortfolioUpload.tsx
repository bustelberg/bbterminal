'use client';

import { useState, useRef, useEffect, useCallback } from 'react';

const API_URL = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000';

type StepStatus = 'idle' | 'in_progress' | 'done' | 'error';

type ScanSteps = {
  login: { status: StepStatus; message: string };
  navigate: { status: StepStatus; message: string };
  scrape: { status: StepStatus; message: string };
  ytd: { status: StepStatus; message: string };
};

const INITIAL_STEPS: ScanSteps = {
  login: { status: 'idle', message: 'Log in to broker' },
  navigate: { status: 'idle', message: 'Navigate to portfolios' },
  scrape: { status: 'idle', message: 'Read portfolio table' },
  ytd: { status: 'idle', message: 'Load YTD returns' },
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

type CachedPortfolio = {
  portefeuille: string;
  cumulatief_rendement: number | null;
  periode: string;
  fetched_at: string | null;
};

type YtdState =
  | { status: 'loading' }
  | { status: 'done'; value: number | null; asOf: string | null; fetchedAt: string | null }
  | { status: 'error' };

function fmtDate(iso: string | null): string {
  if (!iso) return '—';
  const d = new Date(iso + 'T00:00:00');
  return d.toLocaleDateString('en-GB', { day: 'numeric', month: 'long', year: 'numeric' });
}

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

// ─── Spinner ─────────────────────────────────────────────────────────────────

function Spinner({ className = 'h-3.5 w-3.5' }: { className?: string }) {
  return (
    <svg className={`animate-spin ${className}`} viewBox="0 0 24 24" fill="none">
      <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
      <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
    </svg>
  );
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

// ─── Main ────────────────────────────────────────────────────────────────────

export default function AirsPortfolioUpload() {
  const [scanning, setScanning] = useState(false);
  const [steps, setSteps] = useState<ScanSteps | null>(null);
  const [portfolios, setPortfolios] = useState<Portfolio[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState<string | null>(null);
  const [detail, setDetail] = useState<PortfolioDetail | null>(null);
  const [ytdMap, setYtdMap] = useState<Record<string, YtdState>>({});
  const [initialLoading, setInitialLoading] = useState(true);
  const [sortKey, setSortKey] = useState<'portefeuille' | 'ytd' | 'asOf'>('portefeuille');
  const [sortAsc, setSortAsc] = useState(true);
  const [showZeroReturn, setShowZeroReturn] = useState(false);
  const abortRef = useRef(false);

  const loadYtdReturns = useCallback(async (portfolioList: Portfolio[], updateSteps: boolean) => {
    abortRef.current = false;
    const total = portfolioList.length;
    let loaded = 0;
    let fromCache = 0;
    let fromDownload = 0;

    if (updateSteps) {
      setSteps((prev) => prev ? { ...prev, ytd: { status: 'in_progress', message: `Loading YTD returns (0/${total})...` } } : prev);
    }

    for (const p of portfolioList) {
      if (abortRef.current) break;
      const name = p.portefeuille;

      setYtdMap((prev) => {
        if (prev[name]?.status === 'done') return prev;
        return { ...prev, [name]: { status: 'loading' } };
      });

      if (updateSteps) {
        setSteps((prev) => prev ? { ...prev, ytd: { status: 'in_progress', message: `Loading YTD returns (${loaded + 1}/${total})...` } } : prev);
      }

      try {
        const res = await fetch(`${API_URL}/api/airs/portfolio/${encodeURIComponent(name)}`);
        if (!res.ok) throw new Error();
        const data: PortfolioDetail = await res.json();
        const last = data.rows[data.rows.length - 1];
        const ytd = last?.cumulatief_rendement ?? null;
        const asOf = last?.periode ?? null;
        if (data.cached) fromCache++; else fromDownload++;
        setYtdMap((prev) => ({ ...prev, [name]: { status: 'done', value: ytd, asOf, fetchedAt: null } }));
      } catch {
        setYtdMap((prev) => ({ ...prev, [name]: { status: 'error' } }));
      }
      loaded++;
    }

    if (!abortRef.current && updateSteps) {
      const parts: string[] = [];
      if (fromCache > 0) parts.push(`${fromCache} cached`);
      if (fromDownload > 0) parts.push(`${fromDownload} downloaded`);
      setSteps((prev) => prev ? { ...prev, ytd: { status: 'done', message: `YTD returns loaded (${parts.join(', ')})` } } : prev);
    }
  }, []);

  // Auto-load cached portfolios on mount
  useEffect(() => {
    (async () => {
      try {
        const res = await fetch(`${API_URL}/api/airs/portfolios`);
        if (!res.ok) return;
        const data: CachedPortfolio[] = await res.json();
        if (data.length > 0) {
          // Build portfolio list from cached data (no depotbank/client/naam in cache)
          const list: Portfolio[] = data.map((d) => ({
            portefeuille: d.portefeuille,
            depotbank: '',
            client: '',
            naam: '',
          }));
          setPortfolios(list);
          // Pre-fill YTD from the cached summary
          const map: Record<string, YtdState> = {};
          for (const d of data) {
            map[d.portefeuille] = {
              status: 'done',
              value: d.cumulatief_rendement,
              asOf: d.periode,
              fetchedAt: d.fetched_at,
            };
          }
          setYtdMap(map);
        }
      } catch {
        // ignore — user can still scan manually
      } finally {
        setInitialLoading(false);
      }
    })();
  }, []);

  function startScan() {
    abortRef.current = true;
    setScanning(true);
    setSteps({ ...INITIAL_STEPS });
    setError(null);
    setDetail(null);

    const eventSource = new EventSource(`${API_URL}/api/airs/scan`);
    eventSource.onmessage = (event) => {
      const data = JSON.parse(event.data);
      if (data.type === 'progress') {
        const step = data.step as keyof ScanSteps;
        if (step in INITIAL_STEPS) {
          setSteps((prev) => prev ? { ...prev, [step]: { status: data.status, message: data.message } } : prev);
        }
      } else if (data.type === 'portfolios') {
        setPortfolios(data.data);
        setYtdMap({});
        loadYtdReturns(data.data, true);
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

  function statusIcon(status: StepStatus) {
    if (status === 'done') return <span className="text-emerald-400">&#10003;</span>;
    if (status === 'in_progress') return <span className="text-indigo-400 animate-pulse">&#9679;</span>;
    if (status === 'error') return <span className="text-rose-400">&#10007;</span>;
    return <span className="text-gray-500">&#9675;</span>;
  }

  function renderYtd(name: string) {
    const state = ytdMap[name];
    if (!state) return { ytd: <span className="text-gray-600">—</span>, asOf: <span className="text-gray-600">—</span> };
    if (state.status === 'loading') return { ytd: <Spinner className="h-3 w-3 text-indigo-400" />, asOf: null };
    if (state.status === 'error') return { ytd: <span className="text-rose-400 text-xs">err</span>, asOf: null };
    return {
      ytd: (
        <span className={`font-mono text-xs font-medium ${returnColor(state.value)}`}>
          {fmtPct(state.value)}
        </span>
      ),
      asOf: (
        <span className="text-gray-500 text-xs">
          {fmtDate(state.asOf)}
        </span>
      ),
    };
  }

  function toggleSort(key: 'portefeuille' | 'ytd' | 'asOf') {
    if (sortKey === key) {
      setSortAsc((prev) => !prev);
    } else {
      setSortKey(key);
      setSortAsc(key === 'portefeuille'); // alphabetical defaults asc, others desc
    }
  }

  const filteredPortfolios = showZeroReturn
    ? portfolios
    : portfolios.filter((p) => {
        const state = ytdMap[p.portefeuille];
        if (state?.status !== 'done') return true; // keep loading/error/unknown
        return state.value !== 0;
      });

  const sortedPortfolios = [...filteredPortfolios].sort((a, b) => {
    const dir = sortAsc ? 1 : -1;
    if (sortKey === 'portefeuille') {
      return dir * a.portefeuille.localeCompare(b.portefeuille);
    }
    const stateA = ytdMap[a.portefeuille];
    const stateB = ytdMap[b.portefeuille];
    if (sortKey === 'ytd') {
      const va = stateA?.status === 'done' ? stateA.value ?? -Infinity : -Infinity;
      const vb = stateB?.status === 'done' ? stateB.value ?? -Infinity : -Infinity;
      return dir * (va - vb);
    }
    // asOf
    const da = stateA?.status === 'done' ? stateA.asOf ?? '' : '';
    const db = stateB?.status === 'done' ? stateB.asOf ?? '' : '';
    return dir * da.localeCompare(db);
  });

  const sortArrow = (key: string) => {
    if (sortKey !== key) return null;
    return <span className="ml-1 text-indigo-400">{sortAsc ? '\u25B2' : '\u25BC'}</span>;
  };

  if (detail) {
    return <PortfolioDetailView detail={detail} onBack={() => setDetail(null)} />;
  }

  return (
    <div className="flex flex-col h-full">
      <div className="px-8 py-5 border-b border-gray-800/60 flex items-center justify-between gap-4">
        <div>
          <h1 className="text-lg font-semibold text-white">AIRS Portfolio Scanner</h1>
          <p className="text-xs text-gray-500 mt-0.5">
            {portfolios.length > 0
              ? `${sortedPortfolios.length} of ${portfolios.length} portfolio${portfolios.length !== 1 ? 's' : ''}`
              : 'Scan broker system for available portfolios'}
          </p>
          {portfolios.length > 0 && (
            <label className="flex items-center gap-1.5 text-xs text-gray-500 mt-1 cursor-pointer select-none">
              <input
                type="checkbox"
                checked={showZeroReturn}
                onChange={(e) => setShowZeroReturn(e.target.checked)}
                className="rounded border-gray-600 bg-[#0f1117] text-indigo-500 focus:ring-indigo-500/30 focus:ring-1 h-3.5 w-3.5"
              />
              Show 0% return portfolios
            </label>
          )}
        </div>
        <button
          onClick={startScan}
          disabled={scanning || loading !== null}
          className="px-4 py-2 rounded-lg text-sm font-medium bg-indigo-600 hover:bg-indigo-500 text-white transition-colors disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2"
        >
          {scanning && <Spinner className="h-4 w-4" />}
          {scanning ? 'Scanning...' : portfolios.length > 0 ? 'Rescan' : 'Start Scan'}
        </button>
      </div>

      {error && (
        <div className="mx-8 mt-4 px-4 py-3 text-sm text-rose-400 bg-rose-500/10 border border-rose-500/20 rounded-lg flex items-center justify-between">
          <span>{error}</span>
          <button onClick={() => setError(null)} className="text-gray-500 hover:text-white ml-3 text-xs">Dismiss</button>
        </div>
      )}

      <div className="flex-1 overflow-auto px-8 py-6 space-y-6">
        {/* Progress steps (fixed 4-step display) */}
        {steps && (
          <div className="bg-[#151821] rounded-xl border border-gray-800/40 px-5 py-4">
            <h2 className="text-sm font-medium text-gray-400 mb-3">Progress</h2>
            <div className="space-y-1.5">
              {(Object.entries(steps) as [string, { status: StepStatus; message: string }][]).map(([key, step]) => (
                <div key={key} className="flex items-center gap-2.5 text-sm">
                  {statusIcon(step.status)}
                  <span className={step.status === 'done' ? 'text-gray-400' : step.status === 'error' ? 'text-rose-400' : step.status === 'in_progress' ? 'text-gray-200' : 'text-gray-600'}>
                    {step.message}
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
                Portfolios — click to view details
              </h2>
            </div>
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-800/60 text-gray-500 text-xs">
                  <th className="px-5 py-3 text-left font-medium w-10">#</th>
                  <th className="px-3 py-3 text-left font-medium cursor-pointer hover:text-gray-300 select-none" onClick={() => toggleSort('portefeuille')}>Portefeuille{sortArrow('portefeuille')}</th>
                  <th className="px-3 py-3 text-right font-medium w-24 cursor-pointer hover:text-gray-300 select-none" onClick={() => toggleSort('ytd')}>YTD{sortArrow('ytd')}</th>
                  <th className="px-3 py-3 text-right font-medium w-28 cursor-pointer hover:text-gray-300 select-none" onClick={() => toggleSort('asOf')}>As of{sortArrow('asOf')}</th>
                </tr>
              </thead>
              <tbody>
                {sortedPortfolios.map((p, i) => {
                  const ytdInfo = renderYtd(p.portefeuille);
                  return (
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
                      <td className="px-3 py-2.5 text-right">{ytdInfo.ytd}</td>
                      <td className="px-3 py-2.5 text-right">{ytdInfo.asOf}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}

        {/* Empty state */}
        {portfolios.length === 0 && !scanning && !initialLoading && (
          <div className="flex flex-col items-center justify-center py-20 text-center">
            <div className="w-16 h-16 rounded-2xl bg-gray-800/50 flex items-center justify-center mb-4">
              <svg className="w-8 h-8 text-gray-600" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M21 21l-5.197-5.197m0 0A7.5 7.5 0 105.196 5.196a7.5 7.5 0 0010.607 10.607z" />
              </svg>
            </div>
            <p className="text-sm font-medium text-gray-400">No portfolios yet</p>
            <p className="text-xs text-gray-600 mt-1">Click &quot;Start Scan&quot; to connect to the broker system</p>
          </div>
        )}

        {/* Initial loading */}
        {initialLoading && (
          <div className="flex items-center justify-center py-20 gap-3 text-gray-400">
            <Spinner className="h-5 w-5" />
            <span className="text-sm">Loading portfolios...</span>
          </div>
        )}
      </div>
    </div>
  );
}
