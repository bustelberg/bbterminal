'use client';

import { useState, useEffect, useRef, useMemo, useCallback, useDeferredValue } from 'react';

import ApiUsageBadge, { type ApiUsageBadgeHandle } from './ApiUsageBadge';
import {
  earningsRefreshA,
  earningsRefreshB,
  type EarningsRefreshController,
} from '../../lib/stores/earnings';
import { trackedFetch } from '../../lib/loading';

import { API_URL } from '../../lib/apiUrl';
import { useFxToEur } from '../../lib/hooks/useFxToEur';
import { useIsAdmin } from '../../lib/hooks/useEffectiveRole';
import InfoTip from './InfoTip';
import SectionLoader from './SectionLoader';
import Spinner from './Spinner';
import CompanyPicker from './earnings/CompanyPicker';
import PortfolioPicker from './earnings/PortfolioPicker';
import PortfolioManagerModal from './earnings/PortfolioManagerModal';
import AttributionMatrix from './earnings/AttributionMatrix';
import { usePortfolios, type Portfolio } from './earnings/usePortfolios';
import type { PortfolioMemberMetrics } from './earnings/portfolioBreakdown';
import { apiFetch } from '../../lib/apiFetch';
import RefreshButton from './earnings/RefreshButton';
import LogPanel from './earnings/LogPanel';
import SnapshotStats from './earnings/SnapshotStats';
import ForwardPEChart from './earnings/ForwardPEChart';
import RelativeGrowthChart from './earnings/RelativeGrowthChart';
import FCFShareChart from './earnings/FCFShareChart';
import MetricBandChart from './earnings/MetricBandChart';
import BandScorecard from './earnings/BandScorecard';
import { SNAPSHOT_BAND_CHARTS } from './earnings/snapshotBandCharts';
import type { Company, MetricRow, ChartCadence } from './earnings/types';
import { expectedStaleSources } from './earnings/utils';

// Metric codes (`MC`), types (`Company`, `MetricRow`, `Cadence`), and pure
// helpers (extractors, time-series builders, statistical helpers, number
// formatters) live in `./earnings/types.ts` and `./earnings/utils.ts` and are
// imported above. Keeping the giant block out of this file makes the
// component itself easier to navigate.

// ---------------------------------------------------------------------------
// SSE log reader — thin hook over the module-scoped earningsRefreshStore so
// streams keep running while the user navigates away from this page.
// ---------------------------------------------------------------------------

function useSSERefresh(
  controller: EarningsRefreshController,
  onApiCalls?: (region: string, count: number) => void,
) {
  const logs = controller.store.use((s) => s.logs);
  const running = controller.store.use((s) => s.running);
  const logEndRef = useRef<HTMLDivElement>(null);

  const start = (url: string, onDone?: () => void) => {
    controller.start(url, { onApiCalls, onDone });
  };

  useEffect(() => {
    const el = logEndRef.current;
    if (el?.parentElement) {
      el.parentElement.scrollTop = el.parentElement.scrollHeight;
    }
  }, [logs]);

  return { logs, running, start, logEndRef, clearLogs: controller.clearLogs };
}

// `CompanyPicker`, `RefreshButton`, `LogPanel`, `InfoTip`, `SectionLoader`
// live in their own files now (`./earnings/*` for earnings-specific,
// `./InfoTip` + `./SectionLoader` for the shared ones). Imported above.

// Sub-components (`SnapshotStats`, `ForwardPEChart`, `RelativeGrowthChart`,
// `FCFShareChart`, `EGMCalculator`, `ReverseDCF`) plus their `tooltipStyle`
// constant + `dcfValue`/`solveImpliedGrowth` helpers all moved to
// `./earnings/*.tsx`. Imported above.

// Per-side source toggle: a comparison side is either a single stock or a
// saved portfolio (weighted basket). Tiny segmented control.
function ModeToggle({ value, onChange }: { value: 'company' | 'portfolio'; onChange: (m: 'company' | 'portfolio') => void }) {
  return (
    <div className="flex items-center gap-0.5 rounded-lg border border-neutral-700 p-0.5">
      {(['company', 'portfolio'] as const).map((m) => (
        <button
          key={m}
          type="button"
          onClick={() => onChange(m)}
          className={`px-2 py-1 rounded-md text-xs font-medium transition-colors ${value === m ? 'bg-accent-600 text-fg-strong' : 'text-fg-muted hover:text-fg-strong hover:bg-overlay/5'}`}
        >
          {m === 'company' ? 'Stock' : 'Portfolio'}
        </button>
      ))}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------

export default function EarningsDashboard() {
  const [companies, setCompanies] = useState<Company[]>([]);
  const [selected, setSelected] = useState<Company | null>(null);
  const [metrics, setMetrics] = useState<MetricRow[]>([]);
  const [loadingMetrics, setLoadingMetrics] = useState(false);
  const [noCache, setNoCache] = useState(false);
  // Comparison company: optional second selection that overlays charts +
  // adds a second column to the snapshot table. Read-only — never
  // triggers SSE refresh, never participates in the staleness
  // auto-refresh path. If the user wants fresh data for the comparison
  // company they can swap A ↔ B and refresh.
  const [compareCompany, setCompareCompany] = useState<Company | null>(null);
  const [compareMetrics, setCompareMetrics] = useState<MetricRow[]>([]);
  const [loadingCompareMetrics, setLoadingCompareMetrics] = useState(false);
  // Portfolio mode: either comparison side can be flipped from a single
  // company to a saved portfolio (a weighted basket whose metrics are
  // aggregated server-side into the same MetricRow[] shape). `aMode`/`bMode`
  // pick the source; `selectedPortfolio`/`comparePortfolio` hold the basket.
  const isAdmin = useIsAdmin();
  const portfolioApi = usePortfolios();
  // Which side (if any) opened the portfolio manager. 'header' = the global
  // button (no auto-select); 'a'/'b' = opened from a side's picker, so a
  // newly created/saved portfolio is auto-selected onto that side.
  const [managerSide, setManagerSide] = useState<'header' | 'a' | 'b' | null>(null);
  const [aMode, setAMode] = useState<'company' | 'portfolio'>('company');
  const [bMode, setBMode] = useState<'company' | 'portfolio'>('company');
  const [selectedPortfolio, setSelectedPortfolio] = useState<Portfolio | null>(null);
  const [comparePortfolio, setComparePortfolio] = useState<Portfolio | null>(null);
  // Per-member metrics for a portfolio side — feeds the ranked "by impact"
  // holdings list in chart tooltips. Fetched lazily when a portfolio is picked.
  const [breakdownA, setBreakdownA] = useState<PortfolioMemberMetrics[] | null>(null);
  const [breakdownB, setBreakdownB] = useState<PortfolioMemberMetrics[] | null>(null);
  const aIsPortfolio = aMode === 'portfolio';
  const bIsPortfolio = bMode === 'portfolio';
  // "A is set" / "B is set" regardless of which source each side uses.
  const hasA = aIsPortfolio ? !!selectedPortfolio : !!selected;
  const hasB = bIsPortfolio ? !!comparePortfolio : !!compareCompany;
  const currentYear = new Date().getFullYear();
  const [startYear, setStartYear] = useState(2015);
  const [startYearInput, setStartYearInput] = useState('2015');
  const [startYearError, setStartYearError] = useState('');
  // Quarterly (default, matches Snapshot Stats) vs annual data for the charts.
  // The toggle button keys off `chartCadence` so its highlight flips instantly;
  // the charts read `deferredCadence` so the heavy series rebuild + recharts
  // re-render happens as a non-blocking deferred update (snappy click, data
  // catches up a beat later) instead of stalling the click.
  const [chartCadence, setChartCadence] = useState<ChartCadence>('annual');
  const deferredCadence = useDeferredValue(chartCadence);
  // Snapshot Stats lives at the bottom and is collapsed by default.
  const [snapshotOpen, setSnapshotOpen] = useState(false);
  // Hide impossible extreme outliers — off by default (show everything).
  const [hideOutliers, setHideOutliers] = useState(false);

  const applyStartYear = useCallback((raw: string) => {
    const v = parseInt(raw, 10);
    if (isNaN(v) || v < 2015) {
      setStartYearError('Min 2015');
    } else if (v > currentYear) {
      setStartYearError(`Max ${currentYear}`);
    } else {
      setStartYear(v);
      setStartYearInput(String(v));
      setStartYearError('');
    }
  }, [currentYear]);

  const nudgeStartYear = useCallback((delta: number) => {
    const next = startYear + delta;
    if (next >= 2015 && next <= currentYear) {
      setStartYear(next);
      setStartYearInput(String(next));
      setStartYearError('');
    }
  }, [startYear, currentYear]);

  const chartMetrics = useMemo(
    () => metrics.filter((m) => m.target_date >= `${startYear}-01-01`),
    [metrics, startYear],
  );
  const chartCompareMetrics = useMemo(
    () => compareMetrics.filter((m) => m.target_date >= `${startYear}-01-01`),
    [compareMetrics, startYear],
  );
  // Breakdown filtered to the start-year window so the portfolio line + the
  // ranked holdings respect the year selector (same as chartMetrics).
  const chartBreakdownA = useMemo(
    () => (breakdownA ? breakdownA.map((m) => ({ ...m, metrics: m.metrics.filter((r) => r.target_date >= `${startYear}-01-01`) })) : null),
    [breakdownA, startYear],
  );
  const chartBreakdownB = useMemo(
    () => (breakdownB ? breakdownB.map((m) => ({ ...m, metrics: m.metrics.filter((r) => r.target_date >= `${startYear}-01-01`) })) : null),
    [breakdownB, startYear],
  );

  // Real names for chart hover tooltips (the pills stay short A/B). Portfolio
  // sides use the portfolio name.
  const chartNameA = aIsPortfolio
    ? selectedPortfolio?.name
    : (selected ? (selected.company_name ?? selected.gurufocus_ticker) : undefined);
  const chartNameB = bIsPortfolio
    ? comparePortfolio?.name
    : (compareCompany ? (compareCompany.company_name ?? compareCompany.gurufocus_ticker) : undefined);
  // Short A/B pill tags.
  const labelA = aIsPortfolio ? selectedPortfolio?.name : selected?.gurufocus_ticker;
  const labelB = bIsPortfolio ? comparePortfolio?.name : compareCompany?.gurufocus_ticker;

  // Native-currency → EUR converters (FX history per currency), so currency-
  // denominated charts (FCF/share) compare directly. EUR companies → identity.
  // Portfolio metrics are already aggregated in EUR server-side → pass
  // `undefined` so the hook yields the identity converter.
  const fxA = useFxToEur(aIsPortfolio ? undefined : selected?.currency);
  const fxB = useFxToEur(bIsPortfolio ? undefined : compareCompany?.currency);

  const usageBadgeRef = useRef<ApiUsageBadgeHandle>(null);

  const sse = useSSERefresh(earningsRefreshA, (region, count) => {
    usageBadgeRef.current?.addSessionCalls(region, count);
  });
  // Independent SSE slot for the comparison company. Shares the usage
  // badge counter (API calls are real API calls regardless of which
  // pane fired them) but has its own running flag, log buffer, and
  // refresh queue. Calling refresh on A while B is also refreshing is
  // fine — both run in parallel against the backend.
  const sseB = useSSERefresh(earningsRefreshB, (region, count) => {
    usageBadgeRef.current?.addSessionCalls(region, count);
  });

  useEffect(() => {
    trackedFetch('Loading companies', `${API_URL}/api/companies`)
      .then((r) => r.json())
      .then((data) => setCompanies(Array.isArray(data) ? data : []))
      .catch(() => {});
  }, []);

  const autoRefreshedFor = useRef<number | null>(null);
  const autoRefreshedForB = useRef<number | null>(null);
  const [refreshingSources, setRefreshingSources] = useState<Set<string>>(new Set());
  const [refreshingSourcesB, setRefreshingSourcesB] = useState<Set<string>>(new Set());

  const loadMetrics = useCallback((opts?: { silent?: boolean }) => {
    const url = aMode === 'portfolio'
      ? (selectedPortfolio ? `${API_URL}/api/earnings/portfolios/${selectedPortfolio.id}/metrics` : null)
      : (selected ? `${API_URL}/api/earnings/${selected.company_id}/metrics` : null);
    if (!url) return Promise.resolve<MetricRow[]>([]);
    const who = aMode === 'portfolio' ? selectedPortfolio?.name : selected?.gurufocus_ticker;
    const silent = opts?.silent ?? false;
    if (!silent) setLoadingMetrics(true);
    return trackedFetch(`Loading earnings metrics for ${who}`, url)
      .then((r) => r.json())
      .then((data) => {
        const rows: MetricRow[] = Array.isArray(data) ? data : [];
        setMetrics(rows);
        return rows;
      })
      .catch(() => {
        if (!silent) setMetrics([]);
        return [] as MetricRow[];
      })
      .finally(() => { if (!silent) setLoadingMetrics(false); });
  }, [aMode, selected, selectedPortfolio]);

  // Mirror of `loadMetrics` for the comparison company. Kept as a
  // standalone callback (not a generalized factory) so the side-A code
  // path stays a copy-paste read with the same shape.
  const loadCompareMetrics = useCallback((opts?: { silent?: boolean }) => {
    const url = bMode === 'portfolio'
      ? (comparePortfolio ? `${API_URL}/api/earnings/portfolios/${comparePortfolio.id}/metrics` : null)
      : (compareCompany ? `${API_URL}/api/earnings/${compareCompany.company_id}/metrics` : null);
    if (!url) return Promise.resolve<MetricRow[]>([]);
    const who = bMode === 'portfolio' ? comparePortfolio?.name : compareCompany?.gurufocus_ticker;
    const silent = opts?.silent ?? false;
    if (!silent) setLoadingCompareMetrics(true);
    return trackedFetch(`Loading earnings metrics for ${who} (compare)`, url)
      .then((r) => r.json())
      .then((data) => {
        const rows: MetricRow[] = Array.isArray(data) ? data : [];
        setCompareMetrics(rows);
        return rows;
      })
      .catch(() => {
        if (!silent) setCompareMetrics([]);
        return [] as MetricRow[];
      })
      .finally(() => { if (!silent) setLoadingCompareMetrics(false); });
  }, [bMode, compareCompany, comparePortfolio]);

  const refresh = useCallback((
    source: string,
    force = true,
    silent = false,
    trackedSources?: string[],
  ) => {
    if (!selected || aMode === 'portfolio') return;
    const endpoint = source === 'all' ? 'refresh-all' : `refresh/${source}`;
    const tracking = trackedSources
      ?? (source === 'all'
        ? ['prices', 'indicators', 'financials', 'analyst_estimates']
        : [source]);
    setRefreshingSources(new Set(tracking));
    sse.start(`${API_URL}/api/earnings/${selected.company_id}/${endpoint}?force=${force}`, () => {
      loadMetrics({ silent });
      usageBadgeRef.current?.refresh();
      setRefreshingSources(new Set());
    });
  }, [selected, aMode, sse, loadMetrics]);

  // Refresh for the comparison company. Independent from A's refresh —
  // both can be in flight simultaneously without sharing log state.
  const refreshCompare = useCallback((
    source: string,
    force = true,
    silent = false,
    trackedSources?: string[],
  ) => {
    if (!compareCompany || bMode === 'portfolio') return;
    const endpoint = source === 'all' ? 'refresh-all' : `refresh/${source}`;
    const tracking = trackedSources
      ?? (source === 'all'
        ? ['prices', 'indicators', 'financials', 'analyst_estimates']
        : [source]);
    setRefreshingSourcesB(new Set(tracking));
    sseB.start(`${API_URL}/api/earnings/${compareCompany.company_id}/${endpoint}?force=${force}`, () => {
      loadCompareMetrics({ silent });
      usageBadgeRef.current?.refresh();
      setRefreshingSourcesB(new Set());
    });
  }, [compareCompany, bMode, sseB, loadCompareMetrics]);

  // Comparison company → mirror of the side-A effect below. Wipe its
  // log panel + refreshing-sources, load the DB metrics, then kick off
  // a silent refresh-all (force=false) when today's date implies any
  // source is due. The auto-refresh only fires once per (compare_id)
  // selection, tracked via autoRefreshedForB.
  useEffect(() => {
    sseB.clearLogs();
    setRefreshingSourcesB(new Set());
    if (bMode === 'portfolio') {
      // Portfolios are read-only aggregates — load, never auto-refresh.
      if (comparePortfolio) loadCompareMetrics(); else setCompareMetrics([]);
      autoRefreshedForB.current = null;
      return;
    }
    if (!compareCompany) {
      setCompareMetrics([]);
      autoRefreshedForB.current = null;
      return;
    }
    const companyId = compareCompany.company_id;
    loadCompareMetrics().then((rows) => {
      if (autoRefreshedForB.current === companyId) return;
      autoRefreshedForB.current = companyId;
      const stale = expectedStaleSources(rows);
      if (stale.length > 0) {
        refreshCompare('all', false, true, stale);
      }
    });
    // Same scoping reasoning as the A-side effect below — only re-run
    // when loadCompareMetrics's identity changes (i.e. compareCompany
    // changed). The other deps are read once on mount.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [loadCompareMetrics]);

  // Run on every (selected â†’ loadMetrics) change: wipe the SSE log
  // panel from the previous company's refresh and re-fetch metrics for
  // the new one. After the initial DB read settles, kick off a silent
  // `refresh-all` (force=false) when today's date implies any source is
  // due for new data — old values stay on screen until the SSE round-trip
  // replaces them.
  useEffect(() => {
    sse.clearLogs();
    setRefreshingSources(new Set());
    if (aMode === 'portfolio') {
      // Portfolios are read-only aggregates — load, never auto-refresh.
      if (selectedPortfolio) loadMetrics(); else setMetrics([]);
      autoRefreshedFor.current = null;
      return;
    }
    if (!selected) { autoRefreshedFor.current = null; return; }
    const companyId = selected.company_id;
    loadMetrics().then((rows) => {
      if (autoRefreshedFor.current === companyId) return;
      autoRefreshedFor.current = companyId;
      const stale = expectedStaleSources(rows);
      if (stale.length > 0) {
        refresh('all', false, true, stale);
      }
    });
    // Intentionally depends on `loadMetrics` only — `refresh`, `selected`,
    // `sse` are read once on mount; adding them re-fires the effect on
    // every keystroke that touches their identity.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [loadMetrics]);

  // Drop a portfolio selection when that portfolio is deleted (or absent
  // after a reload) so the charts don't keep requesting a gone id.
  useEffect(() => {
    const ids = new Set(portfolioApi.portfolios.map((p) => p.id));
    if (selectedPortfolio && !ids.has(selectedPortfolio.id)) setSelectedPortfolio(null);
    if (comparePortfolio && !ids.has(comparePortfolio.id)) setComparePortfolio(null);
  }, [portfolioApi.portfolios, selectedPortfolio, comparePortfolio]);

  // Lazily load per-member metrics for whichever side is a portfolio (drives
  // the ranked holdings breakdown in chart tooltips).
  useEffect(() => {
    if (aMode !== 'portfolio' || !selectedPortfolio) { setBreakdownA(null); return; }
    let cancelled = false;
    apiFetch(`${API_URL}/api/earnings/portfolios/${selectedPortfolio.id}/member-metrics`)
      .then((r) => r.json())
      .then((d) => { if (!cancelled) setBreakdownA(Array.isArray(d) ? d : null); })
      .catch(() => { if (!cancelled) setBreakdownA(null); });
    return () => { cancelled = true; };
  }, [aMode, selectedPortfolio]);

  useEffect(() => {
    if (bMode !== 'portfolio' || !comparePortfolio) { setBreakdownB(null); return; }
    let cancelled = false;
    apiFetch(`${API_URL}/api/earnings/portfolios/${comparePortfolio.id}/member-metrics`)
      .then((r) => r.json())
      .then((d) => { if (!cancelled) setBreakdownB(Array.isArray(d) ? d : null); })
      .catch(() => { if (!cancelled) setBreakdownB(null); });
    return () => { cancelled = true; };
  }, [bMode, comparePortfolio]);

  return (
    <div className="px-8 py-5 space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold text-fg-strong">Earnings Dashboard</h1>
        <div className="flex items-center gap-3">
          {isAdmin && (
            <button
              type="button"
              onClick={() => setManagerSide('header')}
              className="px-3 py-1.5 rounded-lg text-sm font-medium bg-card border border-neutral-800/60 text-fg-muted hover:text-fg-strong transition-colors"
            >
              Portfolios
            </button>
          )}
          <ApiUsageBadge ref={usageBadgeRef} />
        </div>
      </div>

      {/* Picker(s). Each side can be a single stock or a saved portfolio
          (weighted basket, aggregated server-side). The secondary picker is
          hidden until side A is set. Portfolio sides are read-only — refresh +
          the staleness auto-refresh only touch a single primary company. */}
      <div className="flex items-center gap-4 flex-wrap">
        <div className="flex items-center gap-2">
          <ModeToggle value={aMode} onChange={setAMode} />
          {aIsPortfolio ? (
            <>
              <PortfolioPicker portfolios={portfolioApi.portfolios} selected={selectedPortfolio} onSelect={setSelectedPortfolio} />
              {isAdmin && (
                <button
                  type="button"
                  onClick={() => setManagerSide('a')}
                  className="px-2.5 py-2 rounded-lg text-sm font-medium bg-card border border-neutral-800/60 text-fg-muted hover:text-fg-strong transition-colors whitespace-nowrap"
                  title="Create / edit portfolios"
                >+ New / Manage</button>
              )}
            </>
          ) : (
            <CompanyPicker companies={companies} selected={selected} onSelect={setSelected} className="w-72" />
          )}
        </div>
        {hasA && (
          <>
            <div className="flex items-center gap-2">
              <span className="text-fg-subtle text-sm">vs</span>
              <ModeToggle value={bMode} onChange={setBMode} />
              {bIsPortfolio ? (
                <>
                  <PortfolioPicker portfolios={portfolioApi.portfolios} selected={comparePortfolio} onSelect={setComparePortfolio} />
                  {isAdmin && (
                    <button
                      type="button"
                      onClick={() => setManagerSide('b')}
                      className="px-2.5 py-2 rounded-lg text-sm font-medium bg-card border border-neutral-800/60 text-fg-muted hover:text-fg-strong transition-colors whitespace-nowrap"
                      title="Create / edit portfolios"
                    >+ New / Manage</button>
                  )}
                </>
              ) : (
                <CompanyPicker
                  // Filter A out of the secondary picker so the same company
                  // can't sit on both sides (only when A is itself a company).
                  companies={!aIsPortfolio && selected ? companies.filter((c) => c.company_id !== selected.company_id) : companies}
                  selected={compareCompany}
                  onSelect={setCompareCompany}
                  className="w-72"
                />
              )}
              {hasB && (
                <button
                  type="button"
                  onClick={() => { setCompareCompany(null); setComparePortfolio(null); }}
                  className="text-fg-subtle hover:text-neg-400 transition-colors px-2 py-1 text-sm"
                  title="Clear comparison"
                >×</button>
              )}
              {loadingCompareMetrics && <Spinner size={12} />}
            </div>
            {/* Refresh applies to a single company only — hidden in portfolio mode. */}
            {!aIsPortfolio && (
              <>
                <RefreshButton label="Refresh All" running={sse.running} onClick={() => refresh('all', noCache)} />
                <label className="flex items-center gap-2 text-sm text-fg-muted cursor-pointer" title="Bypass GuruFocus storage cache and re-fetch every source from the API.">
                  <input
                    type="checkbox"
                    checked={noCache}
                    onChange={(e) => setNoCache(e.target.checked)}
                    disabled={sse.running}
                    className="h-4 w-4 rounded border-neutral-700 bg-page text-accent-600 focus:ring-1 focus:ring-accent-500/30"
                  />
                  Don&apos;t use cache
                </label>
              </>
            )}
          </>
        )}
      </div>

      {!hasA && (
        <div className="text-fg-subtle py-12 text-center">Select a {aIsPortfolio ? 'portfolio' : 'company'} to view earnings data</div>
      )}

      {hasA && (
        <>
          <div className="text-fg-muted text-sm flex items-center gap-2 flex-wrap">
            <span>
              {/* A:/B: tags only make sense when comparing two entities. */}
              {hasB && <span className="text-accent-400 font-mono mr-1">A:</span>}
              {aIsPortfolio
                ? `${selectedPortfolio?.name} (${selectedPortfolio?.members.length} companies, equal-wtd EUR)`
                : `${selected?.company_name || selected?.gurufocus_ticker} — ${selected?.gurufocus_ticker}.${selected?.gurufocus_exchange}`}
            </span>
            {hasB && (
              <>
                <span className="text-fg-faint">·</span>
                <span>
                  <span className="text-warn-400 font-mono mr-1">B:</span>
                  {bIsPortfolio
                    ? `${comparePortfolio?.name} (${comparePortfolio?.members.length} companies, EUR)`
                    : `${compareCompany?.company_name || compareCompany?.gurufocus_ticker} — ${compareCompany?.gurufocus_ticker}.${compareCompany?.gurufocus_exchange}`}
                </span>
              </>
            )}
          </div>

          <div className="space-y-2">
            {/* SSE refresh logs only exist for single-company sides. */}
            {!aIsPortfolio && (
              <LogPanel
                logs={sse.logs}
                logEndRef={sse.logEndRef}
                running={sse.running}
                onClose={sse.clearLogs}
                label={hasB ? selected?.gurufocus_ticker : undefined}
              />
            )}
            {!bIsPortfolio && compareCompany && (
              <LogPanel
                logs={sseB.logs}
                logEndRef={sseB.logEndRef}
                running={sseB.running}
                onClose={sseB.clearLogs}
                label={compareCompany.gurufocus_ticker}
              />
            )}
          </div>

          {/* Allocation × Selection attribution — only when BOTH sides are
              portfolios (mixes one's sector weights with the other's returns). */}
          {aIsPortfolio && bIsPortfolio && selectedPortfolio && comparePortfolio && (
            <AttributionMatrix portfolioA={selectedPortfolio} portfolioB={comparePortfolio} />
          )}

          {/* Charts container */}
          <section className="bg-card rounded-xl border border-accent-500/20 p-5 space-y-5">
            <div className="flex items-center gap-3">
              <h2 className="text-fg-strong font-medium">Charts</h2>
              <div className="flex items-center gap-1.5">
                <span className="text-fg-subtle text-sm">From</span>
                <button
                  onClick={() => nudgeStartYear(-1)}
                  disabled={startYear <= 2015}
                  className="w-6 h-6 flex items-center justify-center rounded text-fg-muted hover:text-fg-strong hover:bg-overlay/10 transition-colors disabled:opacity-30 disabled:cursor-not-allowed text-sm"
                >&#9666;</button>
                <div className="relative">
                  <input
                    type="text"
                    value={startYearInput}
                    onChange={(e) => setStartYearInput(e.target.value)}
                    onBlur={() => applyStartYear(startYearInput)}
                    onKeyDown={(e) => { if (e.key === 'Enter') applyStartYear(startYearInput); }}
                    className={`w-16 bg-page border rounded-lg px-2 py-1 text-fg-strong text-sm font-mono text-center outline-none transition-colors ${startYearError ? 'border-neg-500 focus:border-neg-500 focus:ring-1 focus:ring-neg-500/30' : 'border-neutral-700 focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30'}`}
                  />
                  {startYearError && (
                    <div className="absolute top-full left-1/2 -translate-x-1/2 mt-1 text-neg-400 text-[10px] whitespace-nowrap">{startYearError}</div>
                  )}
                </div>
                <button
                  onClick={() => nudgeStartYear(1)}
                  disabled={startYear >= currentYear}
                  className="w-6 h-6 flex items-center justify-center rounded text-fg-muted hover:text-fg-strong hover:bg-overlay/10 transition-colors disabled:opacity-30 disabled:cursor-not-allowed text-sm"
                >&#9656;</button>
              </div>
              {/* Quarterly / Annual cadence — switches the banded charts
                  between per-quarter and fiscal-year data. */}
              <div className="flex items-center gap-0.5 ml-1 rounded-lg border border-neutral-700 p-0.5" title="Quarterly matches the Snapshot Stats values; Annual uses fiscal-year figures.">
                {(['quarterly', 'annual'] as const).map((c) => (
                  <button
                    key={c}
                    type="button"
                    onClick={() => setChartCadence(c)}
                    className={`px-2.5 py-1 rounded-md text-xs font-medium transition-colors ${chartCadence === c ? 'bg-accent-600 text-fg-strong' : 'text-fg-muted hover:text-fg-strong hover:bg-overlay/5'}`}
                  >
                    {c === 'quarterly' ? 'Quarterly' : 'Annual'}
                  </button>
                ))}
              </div>
              {/* Hide extreme outliers — off by default; also covers Forward P/E. */}
              <button
                type="button"
                onClick={() => setHideOutliers((v) => !v)}
                aria-pressed={hideOutliers}
                title="Drop impossible extreme outliers (e.g. a -10000% margin glitch) so the axis stays sane. Off by default."
                className={`ml-1 px-2.5 py-1 rounded-lg text-xs font-medium border transition-colors ${hideOutliers ? 'bg-accent-600 text-fg-strong border-transparent' : 'text-fg-muted border-neutral-700 hover:text-fg-strong hover:bg-overlay/5'}`}
              >
                Hide outliers
              </button>
            </div>
            {/* At-a-glance scorecards — one green/amber/red circle per banded
                chart, coloured by each company's latest value vs its rubric.
                A second row appears for the comparison company. */}
            <div className="space-y-1.5">
              <BandScorecard
                charts={SNAPSHOT_BAND_CHARTS}
                metrics={chartMetrics}
                cadence={deferredCadence}
                hideOutliers={hideOutliers}
                label={hasB ? chartNameA : undefined}
                labelMinCh={Math.max(chartNameA?.length ?? 0, chartNameB?.length ?? 0)}
                breakdown={aIsPortfolio ? chartBreakdownA ?? undefined : undefined}
              />
              {hasB && (
                <BandScorecard
                  charts={SNAPSHOT_BAND_CHARTS}
                  metrics={chartCompareMetrics}
                  cadence={deferredCadence}
                  hideOutliers={hideOutliers}
                  label={chartNameB}
                  labelMinCh={Math.max(chartNameA?.length ?? 0, chartNameB?.length ?? 0)}
                  breakdown={bIsPortfolio ? chartBreakdownB ?? undefined : undefined}
                />
              )}
            </div>
            <div className="grid grid-cols-1 xl:grid-cols-3 gap-5">
              {/* FCF Yield */}
              <div className="bg-page rounded-lg border border-accent-500/20 p-4 space-y-2 overflow-hidden min-w-0">
                <div className="flex items-center justify-between gap-2">
                  <h3 className="text-fg-strong text-sm font-medium flex items-center gap-1.5"><span className="truncate">Forward P/E</span> <InfoTip text="Forward P/E — the share price divided by next-fiscal-year consensus EPS estimate; how much you pay today per dollar of earnings the company is expected to make next year. Computed as price ÷ FY1 EPS estimate (GuruFocus indicators) and plotted over time. Lower = cheaper relative to expected earnings; compare a stock to its own 'period avg' to judge whether it's trading rich or cheap versus its own history rather than in absolute terms." />{(refreshingSources.has('indicators') || refreshingSourcesB.has('indicators')) && <Spinner size={10} />}</h3>
                  <RefreshButton label="Refresh" running={sse.running} onClick={() => refresh('indicators')} />
                </div>
                {loadingMetrics ? <SectionLoader label="Forward P/E" /> : (
                  <ForwardPEChart
                    metrics={chartMetrics}
                    metricsB={hasB ? chartCompareMetrics : undefined}
                    nameA={chartNameA}
                    nameB={chartNameB}
                    hideOutliers={hideOutliers}
                    loadingB={loadingCompareMetrics}
                    breakdownA={aIsPortfolio ? chartBreakdownA ?? undefined : undefined}
                    breakdownB={bIsPortfolio ? chartBreakdownB ?? undefined : undefined}
                  />
                )}
              </div>

              {/* Relative Growth */}
              <div className="bg-page rounded-lg border border-accent-500/20 p-4 space-y-2 overflow-hidden min-w-0">
                <div className="flex items-center justify-between gap-2">
                  <h3 className="text-fg-strong text-sm font-medium flex items-center gap-1.5"><span className="truncate">Share Price vs. Owners Earnings</span> <InfoTip text="Share Price vs Owner Earnings — compares how fast the stock price has grown against the company's underlying earning power (Owner Earnings ≈ EPS excluding non-recurring items). Both series are indexed to 100 at the first overlapping year and drawn on a log scale, so slope = growth rate: parallel lines mean a flat valuation multiple, price outpacing earnings = multiple expansion (getting more expensive), earnings outpacing price = de-rating (getting cheaper). Dividends are deliberately excluded — a dividend is a distribution of EPS, not earnings on top of it." />{(refreshingSources.has('prices') || refreshingSourcesB.has('prices')) && <Spinner size={10} />}</h3>
                  <RefreshButton label="Refresh" running={sse.running} onClick={() => refresh('prices')} />
                </div>
                {loadingMetrics ? <SectionLoader label="Relative Growth" /> : (
                  <RelativeGrowthChart
                    metrics={chartMetrics}
                    metricsB={hasB ? chartCompareMetrics : undefined}
                    nameA={chartNameA}
                    nameB={chartNameB}
                    loadingB={loadingCompareMetrics}
                    breakdownA={aIsPortfolio ? chartBreakdownA ?? undefined : undefined}
                    breakdownB={bIsPortfolio ? chartBreakdownB ?? undefined : undefined}
                  />
                )}
              </div>

              {/* FCF/share Growth */}
              <div className="bg-page rounded-lg border border-accent-500/20 p-4 space-y-2 overflow-hidden min-w-0">
                <div className="flex items-center justify-between gap-2">
                  <h3 className="text-fg-strong text-sm font-medium flex items-center gap-1.5"><span className="truncate">FCF / share</span> <InfoTip text="Free Cash Flow per share — the cash a business throws off after funding its capital spending, divided by shares outstanding; the real fuel for dividends, buybacks and debt paydown. Computed as Free Cash Flow ÷ shares (GuruFocus financials) and converted to EUR at the historical FX rate so companies in different currencies compare directly. A rising line = growing per-share cash generation; negative values (red dots) mean the company burned cash that period." />{(refreshingSources.has('financials') || refreshingSourcesB.has('financials')) && <Spinner size={10} />}</h3>
                  <RefreshButton label="Refresh" running={sse.running} onClick={() => refresh('financials')} />
                </div>
                {loadingMetrics ? <SectionLoader label="FCF/share" /> : (
                  <FCFShareChart
                    metrics={chartMetrics}
                    metricsB={hasB ? chartCompareMetrics : undefined}
                    nameA={chartNameA}
                    nameB={chartNameB}
                    toEurA={fxA.toEur}
                    toEurB={fxB.toEur}
                    loadingB={loadingCompareMetrics}
                    breakdownA={aIsPortfolio ? chartBreakdownA ?? undefined : undefined}
                    breakdownB={bIsPortfolio ? chartBreakdownB ?? undefined : undefined}
                  />
                )}
              </div>

              {/* Banded snapshot-stat charts driven by config
                  (Capital Intensity / Allocation / Profitability / Outlook /
                  Valuation). Value Creation + Historical Growth are excluded. */}
              {SNAPSHOT_BAND_CHARTS.map((cfg) => (
                <div key={cfg.key} className="bg-page rounded-lg border border-accent-500/20 p-4 space-y-2 overflow-hidden min-w-0">
                  <div className="flex items-center justify-between gap-2">
                    <h3 className="text-fg-strong text-sm font-medium flex items-center gap-1.5"><span className="truncate">{cfg.title}</span> <InfoTip text={cfg.headerInfo} />{(refreshingSources.has(cfg.source) || refreshingSourcesB.has(cfg.source)) && <Spinner size={10} />}</h3>
                    <RefreshButton label="Refresh" running={sse.running} onClick={() => refresh(cfg.source)} />
                  </div>
                  {loadingMetrics ? <SectionLoader label={cfg.title} /> : (
                    <MetricBandChart
                      metrics={chartMetrics}
                      metricsB={hasB ? chartCompareMetrics : undefined}
                      nameA={chartNameA}
                      nameB={chartNameB}
                      cadence={deferredCadence}
                      hideOutliers={hideOutliers}
                      loadingB={loadingCompareMetrics}
                      buildSeries={cfg.buildSeries}
                      band={cfg.band}
                      format={cfg.format}
                      axisFormat={cfg.axisFormat}
                      subtitle={cfg.subtitle}
                      cadenceLabel={cfg.cadenceLabel}
                      infoText={cfg.chartInfo}
                      emptyText={cfg.emptyText}
                      breakdownA={aIsPortfolio ? chartBreakdownA ?? undefined : undefined}
                      breakdownB={bIsPortfolio ? chartBreakdownB ?? undefined : undefined}
                    />
                  )}
                </div>
              ))}
            </div>
          </section>

          {/* Snapshot Stats — collapsed by default, at the bottom. */}
          <section className="bg-card rounded-xl border border-accent-500/20">
            <div className="flex items-center justify-between gap-2 px-5 py-4">
              <button
                type="button"
                onClick={() => setSnapshotOpen((v) => !v)}
                className="flex items-center gap-2 text-left"
                aria-expanded={snapshotOpen}
              >
                <span className="text-fg-faint text-sm w-3">{snapshotOpen ? '▾' : '▸'}</span>
                <h2 className="text-fg-strong font-medium">Snapshot Stats</h2>
              </button>
              <RefreshButton label="Refresh" running={sse.running} onClick={() => refresh('indicators')} />
            </div>
            {snapshotOpen && (
              <div className="px-5 pb-5 space-y-4">
                {aIsPortfolio || bIsPortfolio ? (
                  // Snapshot Stats are single-company fundamentals (incl.
                  // derived stats like interest coverage + CAGRs that can't be
                  // cleanly weighted across holdings). Rather than show
                  // misleading aggregates, point the user to the charts above,
                  // whose portfolio lines ARE weighted-member averages.
                  <p className="text-fg-subtle text-sm py-6 text-center">
                    Snapshot Stats aren’t aggregated for portfolios — open a single company to see them.
                    The charts above show the portfolio’s weighted-average trends.
                  </p>
                ) : loadingMetrics ? <SectionLoader label="snapshot stats" /> : (
                  <SnapshotStats
                    metrics={metrics}
                    metricsB={hasB ? compareMetrics : undefined}
                    labelA={labelA}
                    labelB={labelB}
                    refreshingSources={refreshingSources}
                    refreshingSourcesB={refreshingSourcesB}
                    loadingB={loadingCompareMetrics}
                  />
                )}
              </div>
            )}
          </section>
        </>
      )}

      {managerSide !== null && (
        <PortfolioManagerModal
          companies={companies}
          portfolios={portfolioApi.portfolios}
          create={portfolioApi.create}
          update={portfolioApi.update}
          remove={portfolioApi.remove}
          onSaved={(p) => {
            // Auto-select the just-saved portfolio onto the side that opened
            // the manager AND close, so "create → use" is one flow and the
            // user immediately sees its charts. Opened from the header → stay
            // open (managing several) and don't change a side.
            if (managerSide === 'a') { setAMode('portfolio'); setSelectedPortfolio(p); setManagerSide(null); }
            else if (managerSide === 'b') { setBMode('portfolio'); setComparePortfolio(p); setManagerSide(null); }
          }}
          onClose={() => setManagerSide(null)}
        />
      )}
    </div>
  );
}
