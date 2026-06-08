'use client';

import { useMemo, useState, useRef, useEffect, useCallback } from 'react';

import {
  airsScanStore,
  startAirsScan,
  type ScanSteps,
  type StepStatus,
  type Portfolio,
} from '../../lib/stores/airsScan';
import ProgressTimeline, { type StepDef, type StepState } from './ProgressTimeline';
import type { Column } from '../../lib/tableExport';
import TableDownloadButton from './TableDownloadButton';
import Spinner from './Spinner';
import LoadingDots from './LoadingDots';
import { API_URL } from '../../lib/apiUrl';
import { apiFetch } from '../../lib/apiFetch';

const AIRS_STEPS: StepDef[] = [
  { key: 'login', label: 'Log in to AirSPMS' },
  { key: 'navigate', label: 'Navigate to portfolio list' },
  { key: 'scrape', label: 'Scrape portfolios' },
  { key: 'ytd', label: 'Load YTD returns' },
];

// API_URL imported from lib/apiUrl above — single source of truth.

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
  if (v == null) return 'text-fg-subtle';
  return v >= 0 ? 'text-pos-400' : 'text-neg-400';
}

// ─── Spinner ─────────────────────────────────────────────────────────────────

// Spinner moved to ./Spinner — shared with MomentumBacktester (and future call sites).

// ─── Detail view ─────────────────────────────────────────────────────────────

function PortfolioDetailView({ detail, onBack }: { detail: PortfolioDetail; onBack: () => void }) {
  const last = detail.rows[detail.rows.length - 1];
  const perfRowExportColumns = useMemo<Column<PerfRow>[]>(() => [
    { key: 'periode', header: 'Periode', accessor: (r) => r.periode },
    { key: 'beginvermogen', header: 'Beginvermogen', accessor: (r) => r.beginvermogen },
    { key: 'koersresultaat', header: 'Koersresultaat', accessor: (r) => r.koersresultaat },
    { key: 'opbrengsten', header: 'Opbrengsten', accessor: (r) => r.opbrengsten },
    { key: 'beleggingsresultaat', header: 'Beleggingsresultaat', accessor: (r) => r.beleggingsresultaat },
    { key: 'eindvermogen', header: 'Eindvermogen', accessor: (r) => r.eindvermogen },
    { key: 'rendement', header: 'Rendement (%)', accessor: (r) => r.rendement },
    { key: 'cumulatief_rendement', header: 'Cumulatief (%)', accessor: (r) => r.cumulatief_rendement },
  ], []);

  return (
    <div className="flex flex-col h-full">
      <div className="px-8 py-5 border-b border-neutral-800/60 flex items-center justify-between gap-4">
        <div className="flex items-center gap-4">
          <button
            onClick={onBack}
            className="px-3 py-1.5 rounded-lg text-sm text-fg-muted hover:text-fg-strong hover:bg-overlay/5 transition-colors"
          >
            &larr; Back
          </button>
          <div>
            <h1 className="text-lg font-semibold text-fg-strong">{detail.portfolio_name}</h1>
            <p className="text-xs text-fg-subtle mt-0.5">
              {detail.datum_van} to {detail.datum_tot} — {detail.rows.length} period{detail.rows.length !== 1 ? 's' : ''}
            </p>
          </div>
        </div>
        {last?.cumulatief_rendement != null && (
          <div className="text-right">
            <span className={`text-lg font-semibold ${returnColor(last.cumulatief_rendement)}`}>
              {fmtPct(last.cumulatief_rendement)}
            </span>
            <p className="text-xs text-fg-subtle mt-0.5">YTD cumulative return</p>
          </div>
        )}
      </div>

      <div className="flex-1 overflow-auto px-8 py-4">
        <div className="bg-card rounded-xl border border-neutral-800/40 overflow-hidden">
          <div className="px-4 py-2 border-b border-neutral-800/40 flex items-center justify-end">
            <TableDownloadButton
              rows={detail.rows}
              columns={perfRowExportColumns}
              filename={`portfolio_${detail.portfolio_name}`}
              title={`Download ${detail.rows.length} period rows as CSV / XLSX`}
            />
          </div>
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-neutral-800/60 text-fg-subtle text-xs">
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
                <tr key={i} className="border-b border-neutral-800/30 hover:bg-overlay/[0.02] transition-colors">
                  <td className="px-4 py-2.5 text-fg font-medium">{r.periode}</td>
                  <td className="px-3 py-2.5 text-right text-fg-muted font-mono text-xs">{fmtEur(r.beginvermogen)}</td>
                  <td className={`px-3 py-2.5 text-right font-mono text-xs ${returnColor(r.koersresultaat)}`}>{fmtEur(r.koersresultaat)}</td>
                  <td className="px-3 py-2.5 text-right text-fg-muted font-mono text-xs">{fmtEur(r.opbrengsten)}</td>
                  <td className={`px-3 py-2.5 text-right font-mono text-xs font-medium ${returnColor(r.beleggingsresultaat)}`}>{fmtEur(r.beleggingsresultaat)}</td>
                  <td className="px-3 py-2.5 text-right text-fg-soft font-mono text-xs">{fmtEur(r.eindvermogen)}</td>
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
  // Scan-driven state lives in a module-scoped store so the SSE stream keeps
  // running when the user navigates away from /airs-portfolio.
  const scanning = airsScanStore.use((s) => s.scanning);
  const steps = airsScanStore.use((s) => s.steps);
  const portfoliosFromStore = airsScanStore.use((s) => s.portfolios);
  const error = airsScanStore.use((s) => s.error);
  const errorKind = airsScanStore.use((s) => s.errorKind);
  const errorDetail = airsScanStore.use((s) => s.errorDetail);
  const portfolios = portfoliosFromStore ?? [];
  const setPortfolios = (next: Portfolio[]) => airsScanStore.set({ portfolios: next });
  // Always reset kind + detail alongside the message so a manual setError
  // call (e.g. an unrelated portfolio-fetch failure) doesn't inherit a
  // stale ip_forbidden classification from a previous scan.
  const setError = (next: string | null) =>
    airsScanStore.set({ error: next, errorKind: null, errorDetail: null });
  const setSteps = (updater: (prev: ScanSteps | null) => ScanSteps | null) => {
    airsScanStore.set((s) => ({ steps: updater(s.steps) }));
  };

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
        const res = await apiFetch(`${API_URL}/api/airs/portfolio/${encodeURIComponent(name)}`);
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
        const res = await apiFetch(`${API_URL}/api/airs/portfolios`);
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
    setDetail(null);
    startAirsScan({
      onPortfolios: (list) => {
        setYtdMap({});
        loadYtdReturns(list, true);
      },
    });
  }

  async function openPortfolio(name: string) {
    setLoading(name);
    setError(null);
    try {
      const res = await apiFetch(`${API_URL}/api/airs/portfolio/${encodeURIComponent(name)}`);
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

  function renderYtd(name: string) {
    const state = ytdMap[name];
    if (!state) return { ytd: <span className="text-fg-faint">—</span>, asOf: <span className="text-fg-faint">—</span> };
    if (state.status === 'loading') return { ytd: <Spinner className="h-3 w-3 text-accent-400" />, asOf: null };
    if (state.status === 'error') return { ytd: <span className="text-neg-400 text-xs">err</span>, asOf: null };
    return {
      ytd: (
        <span className={`font-mono text-xs font-medium ${returnColor(state.value)}`}>
          {fmtPct(state.value)}
        </span>
      ),
      asOf: (
        <span className="text-fg-subtle text-xs">
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

  const portfolioListExportColumns = useMemo<Column<Portfolio>[]>(() => [
    { key: 'portefeuille', header: 'Portefeuille', accessor: (p) => p.portefeuille },
    { key: 'naam', header: 'Naam', accessor: (p) => p.naam },
    { key: 'client', header: 'Client', accessor: (p) => p.client },
    { key: 'depotbank', header: 'Depotbank', accessor: (p) => p.depotbank },
    { key: 'ytd', header: 'YTD (%)', accessor: (p) => {
      const s = ytdMap[p.portefeuille];
      return s?.status === 'done' ? s.value ?? null : null;
    }},
    { key: 'as_of', header: 'As of', accessor: (p) => {
      const s = ytdMap[p.portefeuille];
      return s?.status === 'done' ? s.asOf ?? '' : '';
    }},
  ], [ytdMap]);

  const sortArrow = (key: string) => {
    if (sortKey !== key) return null;
    return <span className="ml-1 text-accent-400">{sortAsc ? '\u25B2' : '\u25BC'}</span>;
  };

  if (detail) {
    return <PortfolioDetailView detail={detail} onBack={() => setDetail(null)} />;
  }

  return (
    <div className="flex flex-col h-full">
      <div className="px-8 py-5 border-b border-neutral-800/60 flex items-center justify-between gap-4">
        <div>
          <h1 className="text-lg font-semibold text-fg-strong">AIRS Portfolio Scanner</h1>
          <p className="text-xs text-fg-subtle mt-0.5">
            {portfolios.length > 0
              ? `${sortedPortfolios.length} of ${portfolios.length} portfolio${portfolios.length !== 1 ? 's' : ''}`
              : 'Scan broker system for available portfolios'}
          </p>
          {portfolios.length > 0 && (
            <label className="flex items-center gap-1.5 text-xs text-fg-subtle mt-1 cursor-pointer">
              <input
                type="checkbox"
                checked={showZeroReturn}
                onChange={(e) => setShowZeroReturn(e.target.checked)}
                className="rounded border-neutral-600 bg-page text-accent-500 focus:ring-accent-500/30 focus:ring-1 h-3.5 w-3.5"
              />
              Show 0% return portfolios
            </label>
          )}
        </div>
        <button
          onClick={startScan}
          disabled={scanning || loading !== null}
          className="px-4 py-2 rounded-lg text-sm font-medium bg-accent-600 hover:bg-accent-500 text-fg-strong transition-colors disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2"
        >
          {scanning && <Spinner className="h-4 w-4" />}
          {scanning ? 'Scanning...' : portfolios.length > 0 ? 'Rescan' : 'Start Scan'}
        </button>
      </div>

      {error && errorKind === 'ip_forbidden' && (
        <div className="mx-8 mt-4 px-4 py-3 text-sm bg-neg-500/10 border border-neg-500/20 rounded-lg">
          <div className="flex items-start justify-between gap-3">
            <div className="min-w-0">
              <div className="text-neg-300 font-medium mb-1">
                AirSPMS access denied (403 Forbidden)
              </div>
              <div className="text-fg-soft text-xs leading-relaxed">
                {error}
              </div>
              {errorDetail && (
                <details className="mt-2">
                  <summary className="text-fg-subtle text-[11px] cursor-pointer hover:text-fg-soft">
                    Show technical details
                  </summary>
                  <pre className="mt-2 text-[10px] text-fg-subtle font-mono whitespace-pre-wrap break-all bg-scrim/30 border border-neutral-800 rounded p-2">
                    {errorDetail}
                  </pre>
                </details>
              )}
            </div>
            <button
              onClick={() => setError(null)}
              className="text-fg-subtle hover:text-fg-strong text-xs shrink-0"
            >
              Dismiss
            </button>
          </div>
        </div>
      )}
      {error && errorKind !== 'ip_forbidden' && (
        <div className="mx-8 mt-4 px-4 py-3 text-sm text-neg-400 bg-neg-500/10 border border-neg-500/20 rounded-lg flex items-center justify-between">
          <span>{error}</span>
          <button onClick={() => setError(null)} className="text-fg-subtle hover:text-fg-strong ml-3 text-xs">Dismiss</button>
        </div>
      )}

      <div className="flex-1 overflow-auto px-8 py-6 space-y-6">
        {/* Progress steps (fixed 4-step display) */}
        {steps && (
          <ProgressTimeline
            steps={AIRS_STEPS}
            state={Object.fromEntries(
              (Object.entries(steps) as [string, { status: StepStatus; message: string }][]).map(([k, v]) => [
                k,
                { status: v.status === 'idle' ? 'pending' : v.status, message: v.message } as StepState,
              ])
            )}
            running={scanning}
            errorMessage={error}
            title="Scanner Progress"
          />
        )}

        {/* Portfolio table */}
        {portfolios.length > 0 && (
          <div className="bg-card rounded-xl border border-neutral-800/40 overflow-hidden">
            <div className="px-5 py-3 border-b border-neutral-800/40 flex items-center justify-between">
              <h2 className="text-sm font-medium text-fg-muted">
                Portfolios — click to view details
              </h2>
              <TableDownloadButton
                rows={sortedPortfolios}
                columns={portfolioListExportColumns}
                filename="airs_portfolios"
                title={`Download ${sortedPortfolios.length} portfolios as CSV / XLSX`}
              />
            </div>
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-neutral-800/60 text-fg-subtle text-xs">
                  <th className="px-5 py-3 text-left font-medium w-10">#</th>
                  <th className="px-3 py-3 text-left font-medium cursor-pointer hover:text-fg-soft" onClick={() => toggleSort('portefeuille')}>Portefeuille{sortArrow('portefeuille')}</th>
                  <th className="px-3 py-3 text-right font-medium w-24 cursor-pointer hover:text-fg-soft" onClick={() => toggleSort('ytd')}>YTD{sortArrow('ytd')}</th>
                  <th className="px-3 py-3 text-right font-medium w-28 cursor-pointer hover:text-fg-soft" onClick={() => toggleSort('asOf')}>As of{sortArrow('asOf')}</th>
                </tr>
              </thead>
              <tbody>
                {sortedPortfolios.map((p, i) => {
                  const ytdInfo = renderYtd(p.portefeuille);
                  return (
                    <tr
                      key={i}
                      className="border-b border-neutral-800/30 hover:bg-overlay/[0.02] transition-colors cursor-pointer group"
                      onClick={() => !loading && openPortfolio(p.portefeuille)}
                    >
                      <td className="px-5 py-2.5 text-fg-subtle font-mono text-xs">{i + 1}</td>
                      <td className="px-3 py-2.5 text-fg font-medium group-hover:text-accent-300 transition-colors">
                        {loading === p.portefeuille ? (
                          <span className="flex items-center gap-2">
                            <Spinner className="h-3.5 w-3.5 text-accent-400" />
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
            <div className="w-16 h-16 rounded-2xl bg-neutral-800/50 flex items-center justify-center mb-4">
              <svg className="w-8 h-8 text-fg-faint" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M21 21l-5.197-5.197m0 0A7.5 7.5 0 105.196 5.196a7.5 7.5 0 0010.607 10.607z" />
              </svg>
            </div>
            <p className="text-sm font-medium text-fg-muted">No portfolios yet</p>
            <p className="text-xs text-fg-faint mt-1">Click &quot;Start Scan&quot; to connect to the broker system</p>
          </div>
        )}

        {/* Initial loading */}
        {initialLoading && (
          <div className="flex items-center justify-center py-20 gap-3 text-fg-muted">
            <Spinner className="h-5 w-5" />
            <span className="text-sm"><LoadingDots label="Loading portfolios" /></span>
          </div>
        )}
      </div>
    </div>
  );
}
