'use client';

import { useState, useEffect, useRef, useMemo, useCallback } from 'react';

import ApiUsageBadge, { type ApiUsageBadgeHandle } from './ApiUsageBadge';
import {
  earningsRefreshA,
  earningsRefreshB,
  type EarningsRefreshController,
} from '../../lib/stores/earnings';
import { trackedFetch } from '../../lib/loading';

import { API_URL } from '../../lib/apiUrl';
import InfoTip from './InfoTip';
import SectionLoader from './SectionLoader';
import Spinner from './Spinner';
import CompanyPicker from './earnings/CompanyPicker';
import RefreshButton from './earnings/RefreshButton';
import LogPanel from './earnings/LogPanel';
import SnapshotStats from './earnings/SnapshotStats';
import ForwardPEChart from './earnings/ForwardPEChart';
import RelativeGrowthChart from './earnings/RelativeGrowthChart';
import FCFShareChart from './earnings/FCFShareChart';
import type { Company, MetricRow } from './earnings/types';
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
  const currentYear = new Date().getFullYear();
  const [startYear, setStartYear] = useState(2015);
  const [startYearInput, setStartYearInput] = useState('2015');
  const [startYearError, setStartYearError] = useState('');

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
    if (!selected) return Promise.resolve<MetricRow[]>([]);
    const silent = opts?.silent ?? false;
    if (!silent) setLoadingMetrics(true);
    return trackedFetch(
      `Loading earnings metrics for ${selected.gurufocus_ticker}`,
      `${API_URL}/api/earnings/${selected.company_id}/metrics`,
    )
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
  }, [selected]);

  // Mirror of `loadMetrics` for the comparison company. Kept as a
  // standalone callback (not a generalized factory) so the side-A code
  // path stays a copy-paste read with the same shape.
  const loadCompareMetrics = useCallback((opts?: { silent?: boolean }) => {
    if (!compareCompany) return Promise.resolve<MetricRow[]>([]);
    const silent = opts?.silent ?? false;
    if (!silent) setLoadingCompareMetrics(true);
    return trackedFetch(
      `Loading earnings metrics for ${compareCompany.gurufocus_ticker} (compare)`,
      `${API_URL}/api/earnings/${compareCompany.company_id}/metrics`,
    )
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
  }, [compareCompany]);

  const refresh = useCallback((
    source: string,
    force = true,
    silent = false,
    trackedSources?: string[],
  ) => {
    if (!selected) return;
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
  }, [selected, sse, loadMetrics]);

  // Refresh for the comparison company. Independent from A's refresh —
  // both can be in flight simultaneously without sharing log state.
  const refreshCompare = useCallback((
    source: string,
    force = true,
    silent = false,
    trackedSources?: string[],
  ) => {
    if (!compareCompany) return;
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
  }, [compareCompany, sseB, loadCompareMetrics]);

  // Comparison company → mirror of the side-A effect below. Wipe its
  // log panel + refreshing-sources, load the DB metrics, then kick off
  // a silent refresh-all (force=false) when today's date implies any
  // source is due. The auto-refresh only fires once per (compare_id)
  // selection, tracked via autoRefreshedForB.
  useEffect(() => {
    sseB.clearLogs();
    setRefreshingSourcesB(new Set());
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

  return (
    <div className="px-8 py-5 space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold text-fg-strong">Earnings Dashboard</h1>
        <ApiUsageBadge ref={usageBadgeRef} />
      </div>

      {/* Company picker(s). The secondary picker is hidden until the
          user clicks "+ Compare" and lets them pick a second company for
          side-by-side stats + chart overlays. The comparison company is
          read-only — refresh buttons + the staleness auto-refresh only
          touch the primary `selected`. */}
      <div className="flex items-center gap-4 flex-wrap">
        <CompanyPicker companies={companies} selected={selected} onSelect={setSelected} />
        {selected && (
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
            <div className="flex items-center gap-2">
              <span className="text-fg-subtle text-sm">vs</span>
              {/* Filter A out of the secondary picker's options so the user
                  can't pick the same company on both sides. */}
              <CompanyPicker
                companies={companies.filter((c) => c.company_id !== selected.company_id)}
                selected={compareCompany}
                onSelect={setCompareCompany}
              />
              {compareCompany && (
                <button
                  type="button"
                  onClick={() => setCompareCompany(null)}
                  className="text-fg-subtle hover:text-neg-400 transition-colors px-2 py-1 text-sm"
                  title="Clear comparison"
                >×</button>
              )}
              {loadingCompareMetrics && <Spinner size={12} />}
            </div>
          </>
        )}
      </div>

      {!selected && (
        <div className="text-fg-subtle py-12 text-center">Select a company to view earnings data</div>
      )}

      {selected && (
        <>
          <div className="text-fg-muted text-sm flex items-center gap-2 flex-wrap">
            <span><span className="text-accent-400 font-mono mr-1">A:</span>{selected.company_name || selected.gurufocus_ticker} — {selected.gurufocus_ticker}.{selected.gurufocus_exchange}</span>
            {compareCompany && (
              <>
                <span className="text-fg-faint">·</span>
                <span><span className="text-warn-400 font-mono mr-1">B:</span>{compareCompany.company_name || compareCompany.gurufocus_ticker} — {compareCompany.gurufocus_ticker}.{compareCompany.gurufocus_exchange}</span>
              </>
            )}
          </div>

          <div className="space-y-2">
            <LogPanel
              logs={sse.logs}
              logEndRef={sse.logEndRef}
              running={sse.running}
              onClose={sse.clearLogs}
              label={compareCompany ? selected.gurufocus_ticker : undefined}
            />
            {compareCompany && (
              <LogPanel
                logs={sseB.logs}
                logEndRef={sseB.logEndRef}
                running={sseB.running}
                onClose={sseB.clearLogs}
                label={compareCompany.gurufocus_ticker}
              />
            )}
          </div>

          {/* Snapshot Stats */}
          <section className="bg-card rounded-xl border border-accent-500/20 p-5 space-y-4">
            <div className="flex items-center justify-between">
              <h2 className="text-fg-strong font-medium">Snapshot Stats</h2>
              <RefreshButton label="Refresh" running={sse.running} onClick={() => refresh('indicators')} />
            </div>
            {loadingMetrics ? <SectionLoader label="snapshot stats" /> : (
              <SnapshotStats
                metrics={metrics}
                metricsB={compareCompany ? compareMetrics : undefined}
                labelA={selected.gurufocus_ticker}
                labelB={compareCompany?.gurufocus_ticker}
                refreshingSources={refreshingSources}
                refreshingSourcesB={refreshingSourcesB}
                loadingB={loadingCompareMetrics}
              />
            )}
          </section>

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
            </div>
            <div className="grid grid-cols-1 xl:grid-cols-3 gap-5">
              {/* FCF Yield */}
              <div className="bg-page rounded-lg border border-accent-500/20 p-4 space-y-2 overflow-hidden min-w-0">
                <div className="flex items-center justify-between gap-2">
                  <h3 className="text-fg-strong text-sm font-medium flex items-center gap-1.5"><span className="truncate">Forward P/E</span> <InfoTip text="Forward Price-to-Earnings ratio over time. Shows how much investors pay per dollar of expected earnings. Compare to the period average (red dashed) to spot relative cheapness or richness." />{(refreshingSources.has('indicators') || refreshingSourcesB.has('indicators')) && <Spinner size={10} />}</h3>
                  <RefreshButton label="Refresh" running={sse.running} onClick={() => refresh('indicators')} />
                </div>
                {loadingMetrics ? <SectionLoader label="Forward P/E" /> : (
                  <ForwardPEChart
                    metrics={chartMetrics}
                    metricsB={compareCompany ? chartCompareMetrics : undefined}
                    labelA={selected.gurufocus_ticker}
                    labelB={compareCompany?.gurufocus_ticker}
                    loadingB={loadingCompareMetrics}
                  />
                )}
              </div>

              {/* Relative Growth */}
              <div className="bg-page rounded-lg border border-accent-500/20 p-4 space-y-2 overflow-hidden min-w-0">
                <div className="flex items-center justify-between gap-2">
                  <h3 className="text-fg-strong text-sm font-medium flex items-center gap-1.5"><span className="truncate">Relative Growth (log)</span> <InfoTip text="Tracks whether the share price is growing in line with Owner Earnings (EPS, excluding non-recurring items). On a log scale, parallel lines mean the valuation multiple is stable. Divergence signals re-rating." />{(refreshingSources.has('prices') || refreshingSourcesB.has('prices')) && <Spinner size={10} />}</h3>
                  <RefreshButton label="Refresh" running={sse.running} onClick={() => refresh('prices')} />
                </div>
                {loadingMetrics ? <SectionLoader label="Relative Growth" /> : (
                  <RelativeGrowthChart
                    metrics={chartMetrics}
                    metricsB={compareCompany ? chartCompareMetrics : undefined}
                    labelA={selected.gurufocus_ticker}
                    labelB={compareCompany?.gurufocus_ticker}
                    loadingB={loadingCompareMetrics}
                  />
                )}
              </div>

              {/* FCF/share Growth */}
              <div className="bg-page rounded-lg border border-accent-500/20 p-4 space-y-2 overflow-hidden min-w-0">
                <div className="flex items-center justify-between gap-2">
                  <h3 className="text-fg-strong text-sm font-medium flex items-center gap-1.5"><span className="truncate">FCF/share Growth</span> <InfoTip text="Free Cash Flow per share over time. Shows the trajectory of cash generation. Negative values are highlighted with red dots." />{(refreshingSources.has('financials') || refreshingSourcesB.has('financials')) && <Spinner size={10} />}</h3>
                  <RefreshButton label="Refresh" running={sse.running} onClick={() => refresh('financials')} />
                </div>
                {loadingMetrics ? <SectionLoader label="FCF/share" /> : (
                  <FCFShareChart
                    metrics={chartMetrics}
                    metricsB={compareCompany ? chartCompareMetrics : undefined}
                    labelA={selected.gurufocus_ticker}
                    labelB={compareCompany?.gurufocus_ticker}
                    loadingB={loadingCompareMetrics}
                  />
                )}
              </div>
            </div>
          </section>
        </>
      )}
    </div>
  );
}
