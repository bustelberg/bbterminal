'use client';

import { useState, useEffect, useRef, useMemo, useCallback } from 'react';

import ApiUsageBadge, { type ApiUsageBadgeHandle } from './ApiUsageBadge';
import {
  earningsRefreshStore,
  startEarningsRefresh,
  clearEarningsLogs,
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
// SSE log reader â€” thin hook over the module-scoped earningsRefreshStore so
// streams keep running while the user navigates away from this page.
// ---------------------------------------------------------------------------

function useSSERefresh(onApiCalls?: (region: string, count: number) => void) {
  const logs = earningsRefreshStore.use((s) => s.logs);
  const running = earningsRefreshStore.use((s) => s.running);
  const logEndRef = useRef<HTMLDivElement>(null);

  const start = (url: string, onDone?: () => void) => {
    startEarningsRefresh(url, { onApiCalls, onDone });
  };

  useEffect(() => {
    const el = logEndRef.current;
    if (el?.parentElement) {
      el.parentElement.scrollTop = el.parentElement.scrollHeight;
    }
  }, [logs]);

  return { logs, running, start, logEndRef, clearLogs: clearEarningsLogs };
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

  const usageBadgeRef = useRef<ApiUsageBadgeHandle>(null);

  const sse = useSSERefresh((region, count) => {
    usageBadgeRef.current?.addSessionCalls(region, count);
  });

  useEffect(() => {
    trackedFetch('Loading companies', `${API_URL}/api/companies`)
      .then((r) => r.json())
      .then((data) => setCompanies(Array.isArray(data) ? data : []))
      .catch(() => {});
  }, []);

  const autoRefreshedFor = useRef<number | null>(null);
  const [refreshingSources, setRefreshingSources] = useState<Set<string>>(new Set());

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

  // Run on every (selected â†’ loadMetrics) change: wipe the SSE log
  // panel from the previous company's refresh and re-fetch metrics for
  // the new one. After the initial DB read settles, kick off a silent
  // `refresh-all` (force=false) when today's date implies any source is
  // due for new data — old values stay on screen until the SSE round-trip
  // replaces them.
  // eslint-disable-next-line react-hooks/set-state-in-effect, react-hooks/exhaustive-deps
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
  }, [loadMetrics]);

  return (
    <div className="px-8 py-5 space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold text-white">Earnings Dashboard</h1>
        <ApiUsageBadge ref={usageBadgeRef} />
      </div>

      {/* Company picker */}
      <div className="flex items-center gap-4">
        <CompanyPicker companies={companies} selected={selected} onSelect={setSelected} />
        {selected && (
          <>
            <RefreshButton label="Refresh All" running={sse.running} onClick={() => refresh('all', noCache)} />
            <label className="flex items-center gap-2 text-sm text-gray-400 cursor-pointer select-none" title="Bypass GuruFocus storage cache and re-fetch every source from the API.">
              <input
                type="checkbox"
                checked={noCache}
                onChange={(e) => setNoCache(e.target.checked)}
                disabled={sse.running}
                className="h-4 w-4 rounded border-gray-700 bg-[#0f1117] text-indigo-600 focus:ring-1 focus:ring-indigo-500/30"
              />
              Don&apos;t use cache
            </label>
          </>
        )}
      </div>

      {!selected && (
        <div className="text-gray-500 py-12 text-center">Select a company to view earnings data</div>
      )}

      {selected && (
        <>
          <div className="text-gray-400 text-sm">
            {selected.company_name || selected.gurufocus_ticker} â€” {selected.gurufocus_ticker}.{selected.gurufocus_exchange}
          </div>

          <LogPanel logs={sse.logs} logEndRef={sse.logEndRef} running={sse.running} onClose={sse.clearLogs} />

          {/* Snapshot Stats */}
          <section className="bg-[#151821] rounded-xl border border-indigo-500/20 p-5 space-y-4">
            <div className="flex items-center justify-between">
              <h2 className="text-white font-medium">Snapshot Stats</h2>
              <RefreshButton label="Refresh" running={sse.running} onClick={() => refresh('indicators')} />
            </div>
            {loadingMetrics ? <SectionLoader label="snapshot stats" /> : <SnapshotStats metrics={metrics} refreshingSources={refreshingSources} />}
          </section>

          {/* Charts container */}
          <section className="bg-[#151821] rounded-xl border border-indigo-500/20 p-5 space-y-5">
            <div className="flex items-center gap-3">
              <h2 className="text-white font-medium">Charts</h2>
              <div className="flex items-center gap-1.5">
                <span className="text-gray-500 text-sm">From</span>
                <button
                  onClick={() => nudgeStartYear(-1)}
                  disabled={startYear <= 2015}
                  className="w-6 h-6 flex items-center justify-center rounded text-gray-400 hover:text-white hover:bg-white/10 transition-colors disabled:opacity-30 disabled:cursor-not-allowed text-sm"
                >&#9666;</button>
                <div className="relative">
                  <input
                    type="text"
                    value={startYearInput}
                    onChange={(e) => setStartYearInput(e.target.value)}
                    onBlur={() => applyStartYear(startYearInput)}
                    onKeyDown={(e) => { if (e.key === 'Enter') applyStartYear(startYearInput); }}
                    className={`w-16 bg-[#0f1117] border rounded-lg px-2 py-1 text-white text-sm font-mono text-center outline-none transition-colors ${startYearError ? 'border-rose-500 focus:border-rose-500 focus:ring-1 focus:ring-rose-500/30' : 'border-gray-700 focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30'}`}
                  />
                  {startYearError && (
                    <div className="absolute top-full left-1/2 -translate-x-1/2 mt-1 text-rose-400 text-[10px] whitespace-nowrap">{startYearError}</div>
                  )}
                </div>
                <button
                  onClick={() => nudgeStartYear(1)}
                  disabled={startYear >= currentYear}
                  className="w-6 h-6 flex items-center justify-center rounded text-gray-400 hover:text-white hover:bg-white/10 transition-colors disabled:opacity-30 disabled:cursor-not-allowed text-sm"
                >&#9656;</button>
              </div>
            </div>
            <div className="grid grid-cols-1 xl:grid-cols-3 gap-5">
              {/* FCF Yield */}
              <div className="bg-[#0f1117] rounded-lg border border-indigo-500/20 p-4 space-y-2 overflow-hidden min-w-0">
                <div className="flex items-center justify-between gap-2">
                  <h3 className="text-white text-sm font-medium flex items-center gap-1.5"><span className="truncate">Forward P/E</span> <InfoTip text="Forward Price-to-Earnings ratio over time. Shows how much investors pay per dollar of expected earnings. Compare to the period average (red dashed) to spot relative cheapness or richness." />{refreshingSources.has('indicators') && <Spinner size={10} />}</h3>
                  <RefreshButton label="Refresh" running={sse.running} onClick={() => refresh('indicators')} />
                </div>
                {loadingMetrics ? <SectionLoader label="Forward P/E" /> : <ForwardPEChart metrics={chartMetrics} />}
              </div>

              {/* Relative Growth */}
              <div className="bg-[#0f1117] rounded-lg border border-indigo-500/20 p-4 space-y-2 overflow-hidden min-w-0">
                <div className="flex items-center justify-between gap-2">
                  <h3 className="text-white text-sm font-medium flex items-center gap-1.5"><span className="truncate">Relative Growth (log)</span> <InfoTip text="Tracks whether the share price is growing in line with Owner Earnings (EPS + Dividends). On a log scale, parallel lines mean the valuation multiple is stable. Divergence signals re-rating." />{refreshingSources.has('prices') && <Spinner size={10} />}</h3>
                  <RefreshButton label="Refresh" running={sse.running} onClick={() => refresh('prices')} />
                </div>
                {loadingMetrics ? <SectionLoader label="Relative Growth" /> : <RelativeGrowthChart metrics={chartMetrics} />}
              </div>

              {/* FCF/share Growth */}
              <div className="bg-[#0f1117] rounded-lg border border-indigo-500/20 p-4 space-y-2 overflow-hidden min-w-0">
                <div className="flex items-center justify-between gap-2">
                  <h3 className="text-white text-sm font-medium flex items-center gap-1.5"><span className="truncate">FCF/share Growth</span> <InfoTip text="Free Cash Flow per share over time. Shows the trajectory of cash generation. Negative values are highlighted with red dots." />{refreshingSources.has('financials') && <Spinner size={10} />}</h3>
                  <RefreshButton label="Refresh" running={sse.running} onClick={() => refresh('financials')} />
                </div>
                {loadingMetrics ? <SectionLoader label="FCF/share" /> : <FCFShareChart metrics={chartMetrics} />}
              </div>
            </div>
          </section>
        </>
      )}
    </div>
  );
}
