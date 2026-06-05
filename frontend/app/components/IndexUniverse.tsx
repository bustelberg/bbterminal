'use client';

import { useState, useEffect, useCallback, useMemo } from 'react';
import { dialog } from '../../lib/dialog';
import ProgressTimeline from './ProgressTimeline';
import {
  sp500ImportStore,
  startSp500Import,
  clearSp500ImportLogs,
  sp500GfCheckStore,
  startSp500GfCheck,
  clearSp500GfCheck,
} from '../../lib/stores/sp500';
import { trackedFetch } from '../../lib/loading';
import type { Column } from '../../lib/tableExport';
import TableDownloadButton from './TableDownloadButton';
import LoadingDots from './LoadingDots';
import { API_URL } from '../../lib/apiUrl';

type IndexEntry = {
  index_name: string;
  start_month: string;
  end_month: string;
  month_count: number;
  total_unique_tickers: number;
};
type MonthEntry = { month: string; ticker_count: number };
type TickerEntry = {
  ticker: string;
  company_id: number | null;
  company_name: string | null;
  exchange: string | null;
  gurufocus_url: string;
};
type ChangeEntry = { date: string; month: string; added: string | null; removed: string | null };

export default function IndexUniverse() {
  // SSE / progress (persisted in module store)
  const running = sp500ImportStore.use((s) => s.running);
  const logs = sp500ImportStore.use((s) => s.logs);

  // Indexes
  const [indexes, setIndexes] = useState<IndexEntry[]>([]);
  const [selectedIndex, setSelectedIndex] = useState<string | null>(null);

  // Months
  const [months, setMonths] = useState<MonthEntry[]>([]);
  const [selectedMonth, setSelectedMonth] = useState<string | null>(null);
  const [loadingMonth, setLoadingMonth] = useState(false);

  // Tickers for selected month
  const [tickers, setTickers] = useState<TickerEntry[]>([]);
  const [tickerFilter, setTickerFilter] = useState('');

  // Changelog
  const [changes, setChanges] = useState<ChangeEntry[]>([]);
  const [activeTab, setActiveTab] = useState<'months' | 'changelog' | 'cumulative'>('months');

  // Cumulative universe
  const [cumulative, setCumulative] = useState<TickerEntry[]>([]);
  const [loadingCumulative, setLoadingCumulative] = useState(false);
  const [cumulativeFilter, setCumulativeFilter] = useState('');

  // GuruFocus check (persisted in module store)
  const checkingGF = sp500GfCheckStore.use((s) => s.checkingGF);
  const gfLogs = sp500GfCheckStore.use((s) => s.gfLogs);
  const gfResult = sp500GfCheckStore.use((s) => s.gfResult);
  const [showMissing, setShowMissing] = useState(false);

  // Load indexes on mount
  const loadIndexes = useCallback(() => {
    trackedFetch('Loading indexes', `${API_URL}/api/index-universe/indexes`)
      .then(r => r.json())
      .then(data => setIndexes(data))
      .catch(() => {});
  }, []);

  useEffect(() => { loadIndexes(); }, [loadIndexes]);

  // Load months for selected index
  const loadMonths = useCallback((idx: string) => {
    trackedFetch(`Loading ${idx} months`, `${API_URL}/api/index-universe/months?index=${encodeURIComponent(idx)}`)
      .then(r => r.json())
      .then(data => setMonths(data))
      .catch(() => {});
  }, []);

  // Load changes for selected index
  const loadChanges = useCallback((idx: string) => {
    trackedFetch(`Loading ${idx} change history`, `${API_URL}/api/index-universe/changes?index=${encodeURIComponent(idx)}`)
      .then(r => r.json())
      .then(data => setChanges(data))
      .catch(() => {});
  }, []);

  // Load cumulative tickers
  const loadCumulative = useCallback((idx: string) => {
    setLoadingCumulative(true);
    setCumulative([]);
    trackedFetch(`Loading ${idx} cumulative tickers`, `${API_URL}/api/index-universe/cumulative?index=${encodeURIComponent(idx)}`)
      .then(r => r.json())
      .then(data => { setCumulative(data); setLoadingCumulative(false); })
      .catch(() => setLoadingCumulative(false));
  }, []);

  // Select an index
  const selectIndex = (idx: string) => {
    setSelectedIndex(idx);
    setSelectedMonth(null);
    setTickers([]);
    clearSp500GfCheck();
    setChanges([]);
    setCumulative([]);
    loadMonths(idx);
    loadChanges(idx);
  };

  // Select a month → load tickers
  const selectMonth = (month: string) => {
    if (!selectedIndex) return;
    setSelectedMonth(month);
    setLoadingMonth(true);
    setTickers([]);
    trackedFetch(`Loading ${selectedIndex} tickers for ${month}`, `${API_URL}/api/index-universe/tickers?index=${encodeURIComponent(selectedIndex)}&month=${month}`)
      .then(r => r.json())
      .then(data => { setTickers(data); setLoadingMonth(false); })
      .catch(() => setLoadingMonth(false));
  };

  // Import S&P 500
  const runImport = () => {
    startSp500Import(() => {
      loadIndexes();
      setSelectedIndex('SP500');
      loadMonths('SP500');
      loadChanges('SP500');
    });
  };

  // Check GuruFocus coverage
  const runGFCheck = () => {
    if (!selectedIndex) return;
    startSp500GfCheck(selectedIndex);
  };

  // Delete index
  const deleteIndex = async (idx: string) => {
    if (!(await dialog.confirm(`Delete all data for ${idx}?`, { destructive: true, confirmLabel: 'Delete' }))) return;
    trackedFetch(`Deleting ${idx}`, `${API_URL}/api/index-universe/indexes/${encodeURIComponent(idx)}`, { method: 'DELETE' })
      .then(() => {
        loadIndexes();
        if (selectedIndex === idx) {
          setSelectedIndex(null);
          setMonths([]);
          setSelectedMonth(null);
          setTickers([]);
          setChanges([]);
          clearSp500GfCheck();
        }
      });
  };

  // Filtered tickers
  const filteredTickers = tickerFilter
    ? tickers.filter(t =>
        t.ticker.toLowerCase().includes(tickerFilter.toLowerCase()) ||
        (t.company_name || '').toLowerCase().includes(tickerFilter.toLowerCase())
      )
    : tickers;

  // Same shape powers both the per-month and cumulative ticker tables.
  const tickerExportColumns = useMemo<Column<TickerEntry>[]>(() => [
    { key: 'ticker', header: 'Ticker', accessor: (t) => t.ticker },
    { key: 'exchange', header: 'Exchange', accessor: (t) => t.exchange ?? '' },
    { key: 'company_id', header: 'Company ID', accessor: (t) => t.company_id ?? '' },
    { key: 'company_name', header: 'Company', accessor: (t) => t.company_name ?? '' },
    { key: 'gurufocus_url', header: 'GuruFocus URL', accessor: (t) => t.gurufocus_url },
  ], []);
  const filteredCumulative = useMemo(() => (
    cumulativeFilter
      ? cumulative.filter(t =>
          t.ticker.toLowerCase().includes(cumulativeFilter.toLowerCase()) ||
          (t.company_name || '').toLowerCase().includes(cumulativeFilter.toLowerCase()) ||
          (t.exchange || '').toLowerCase().includes(cumulativeFilter.toLowerCase())
        )
      : cumulative
  ), [cumulative, cumulativeFilter]);

  // Changelog: group by year
  const changesByYear = useMemo(() => {
    const grouped: Record<string, ChangeEntry[]> = {};
    for (const c of changes) {
      const year = c.date.slice(0, 4);
      if (!grouped[year]) grouped[year] = [];
      grouped[year].push(c);
    }
    return grouped;
  }, [changes]);

  const changeYears = useMemo(() => Object.keys(changesByYear).sort(), [changesByYear]);

  // Base month (first month in data, Jan 2000)
  const baseMonth = months.length > 0 ? months[0] : null;

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="shrink-0 px-8 py-5 border-b border-neutral-800/40">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-fg-strong text-lg font-semibold">Index Universe</h1>
            <p className="text-fg-subtle text-sm mt-0.5">Import and manage index constituent histories</p>
          </div>
          <button
            onClick={runImport}
            disabled={running}
            className="px-4 py-2 rounded-lg text-sm font-medium bg-accent-600 hover:bg-accent-500 text-fg-strong disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
          >
            {running ? 'Importing...' : 'Import S&P 500'}
          </button>
        </div>
      </div>

      <div className="flex-1 overflow-auto px-8 py-6 space-y-6">
        {/* Progress log */}
        {logs.length > 0 && (
          <ProgressTimeline
            steps={[]}
            log={logs}
            running={running}
            defaultLogOpen
            title="Import Progress"
            onDismiss={!running ? clearSp500ImportLogs : undefined}
          />
        )}

        {/* Stored Indexes */}
        <div className="bg-card rounded-xl border border-neutral-800/40">
          <div className="px-5 py-4 border-b border-neutral-800/40">
            <h3 className="text-sm font-medium text-fg-strong">Stored Indexes</h3>
          </div>
          <div className="p-5">
            {indexes.length === 0 ? (
              <p className="text-sm text-fg-subtle">No indexes imported yet. Click &quot;Import S&amp;P 500&quot; to get started.</p>
            ) : (
              <div className="grid gap-3">
                {indexes.map(idx => (
                  <div
                    key={idx.index_name}
                    onClick={() => selectIndex(idx.index_name)}
                    className={`flex items-center justify-between px-4 py-3 rounded-lg cursor-pointer transition-colors border ${
                      selectedIndex === idx.index_name
                        ? 'bg-accent-600/10 border-accent-500/30'
                        : 'bg-page border-neutral-800/40 hover:bg-overlay/[0.02]'
                    }`}
                  >
                    <div>
                      <span className="text-fg-strong font-medium text-sm">{idx.index_name}</span>
                      <span className="text-fg-subtle text-xs ml-3">
                        {idx.start_month} — {idx.end_month}
                      </span>
                    </div>
                    <div className="flex items-center gap-4">
                      <span className="text-xs text-fg-muted">
                        {idx.month_count} months &middot; {idx.total_unique_tickers} tickers
                      </span>
                      <button
                        onClick={e => { e.stopPropagation(); deleteIndex(idx.index_name); }}
                        className="text-fg-faint hover:text-neg-400 text-xs transition-colors"
                      >
                        Delete
                      </button>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>

        {/* Selected Index content */}
        {selectedIndex && (
          <>
            {/* Tab bar */}
            <div className="flex gap-1 bg-card rounded-lg p-1 border border-neutral-800/40 w-fit">
              <button
                onClick={() => setActiveTab('months')}
                className={`px-4 py-1.5 rounded-md text-xs font-medium transition-colors ${
                  activeTab === 'months'
                    ? 'bg-accent-600 text-fg-strong'
                    : 'text-fg-muted hover:text-fg-strong'
                }`}
              >
                Months
              </button>
              <button
                onClick={() => setActiveTab('changelog')}
                className={`px-4 py-1.5 rounded-md text-xs font-medium transition-colors ${
                  activeTab === 'changelog'
                    ? 'bg-accent-600 text-fg-strong'
                    : 'text-fg-muted hover:text-fg-strong'
                }`}
              >
                Changelog ({changes.length})
              </button>
              <button
                onClick={() => { setActiveTab('cumulative'); if (selectedIndex && cumulative.length === 0) loadCumulative(selectedIndex); }}
                className={`px-4 py-1.5 rounded-md text-xs font-medium transition-colors ${
                  activeTab === 'cumulative'
                    ? 'bg-accent-600 text-fg-strong'
                    : 'text-fg-muted hover:text-fg-strong'
                }`}
              >
                All Tickers
              </button>
            </div>

            {/* Months tab */}
            {activeTab === 'months' && (
              <>
                {/* Month grid */}
                <div className="bg-card rounded-xl border border-neutral-800/40">
                  <div className="px-5 py-4 border-b border-neutral-800/40 flex items-center justify-between">
                    <h3 className="text-sm font-medium text-fg-strong">
                      {selectedIndex} — Months ({months.length})
                      {baseMonth && (
                        <span className="text-fg-subtle font-normal ml-2">
                          Base: {baseMonth.month} ({baseMonth.ticker_count} tickers)
                        </span>
                      )}
                    </h3>
                    <button
                      onClick={runGFCheck}
                      disabled={checkingGF}
                      className="px-3 py-1.5 rounded-lg text-xs font-medium bg-warn-600/20 text-warn-400 hover:bg-warn-600/30 disabled:opacity-50 disabled:cursor-not-allowed transition-colors border border-warn-600/20"
                    >
                      {checkingGF ? 'Checking...' : 'Check GuruFocus Coverage'}
                    </button>
                  </div>
                  <div className="p-5">
                    {months.length === 0 ? (
                      <p className="text-sm text-fg-subtle"><LoadingDots label="Loading months" /></p>
                    ) : (
                      <div className="flex flex-wrap gap-1.5">
                        {months.map(m => (
                          <button
                            key={m.month}
                            onClick={() => selectMonth(m.month)}
                            className={`px-2 py-1 rounded text-xs font-mono transition-colors ${
                              selectedMonth === m.month
                                ? 'bg-accent-600 text-fg-strong'
                                : 'bg-page text-fg-muted hover:bg-overlay/[0.04] hover:text-fg'
                            }`}
                            title={`${m.ticker_count} tickers`}
                          >
                            {m.month}
                          </button>
                        ))}
                      </div>
                    )}
                  </div>
                </div>

                {/* GuruFocus check results */}
                {(gfLogs.length > 0 || gfResult) && (
                  <div className="bg-card rounded-xl border border-neutral-800/40">
                    <div className="px-5 py-4 border-b border-neutral-800/40">
                      <h3 className="text-sm font-medium text-fg-strong">GuruFocus Coverage</h3>
                    </div>
                    <div className="p-5 space-y-4">
                      {gfLogs.length > 0 && !gfResult && (
                        <ProgressTimeline
                          steps={[]}
                          log={gfLogs}
                          running={checkingGF}
                          defaultLogOpen
                        />
                      )}
                      {gfResult && (
                        <div className="space-y-4">
                          <div>
                            <div className="flex items-center justify-between mb-2">
                              <span className="text-sm text-fg-soft">
                                {gfResult.available_count} / {gfResult.total} tickers available
                              </span>
                              <span className={`text-sm font-mono font-medium ${
                                gfResult.coverage_pct >= 80 ? 'text-pos-400' :
                                gfResult.coverage_pct >= 50 ? 'text-warn-400' : 'text-neg-400'
                              }`}>
                                {gfResult.coverage_pct}%
                              </span>
                            </div>
                            <div className="w-full h-2 bg-page rounded-full overflow-hidden">
                              <div
                                className={`h-full rounded-full transition-all ${
                                  gfResult.coverage_pct >= 80 ? 'bg-pos-500' :
                                  gfResult.coverage_pct >= 50 ? 'bg-warn-500' : 'bg-neg-500'
                                }`}
                                style={{ width: `${gfResult.coverage_pct}%` }}
                              />
                            </div>
                          </div>
                          {gfResult.missing_count > 0 && (
                            <div>
                              <button
                                onClick={() => setShowMissing(!showMissing)}
                                className="text-xs text-fg-subtle hover:text-fg-soft transition-colors"
                              >
                                {showMissing ? 'Hide' : 'Show'} {gfResult.missing_count} missing tickers
                              </button>
                              {showMissing && (
                                <div className="mt-2 flex flex-wrap gap-1.5">
                                  {gfResult.missing.map(t => (
                                    <span key={t} className="px-2 py-0.5 bg-neg-500/10 text-neg-400 text-xs font-mono rounded">
                                      {t}
                                    </span>
                                  ))}
                                </div>
                              )}
                            </div>
                          )}
                        </div>
                      )}
                    </div>
                  </div>
                )}

                {/* Tickers for selected month */}
                {selectedMonth && (
                  <div className="bg-card rounded-xl border border-neutral-800/40">
                    <div className="px-5 py-4 border-b border-neutral-800/40 flex items-center justify-between">
                      <h3 className="text-sm font-medium text-fg-strong">
                        {selectedMonth} — {tickers.length} tickers
                        {tickers.length > 0 && (
                          <span className="text-fg-subtle font-normal ml-2">
                            ({tickers.filter(t => t.company_id).length} matched to companies)
                          </span>
                        )}
                      </h3>
                      <input
                        type="text"
                        placeholder="Filter..."
                        value={tickerFilter}
                        onChange={e => setTickerFilter(e.target.value)}
                        className="px-3 py-1.5 bg-page border border-neutral-700 rounded-lg text-sm text-fg placeholder-fg-faint focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none w-48"
                      />
                      <TableDownloadButton
                        rows={filteredTickers}
                        columns={tickerExportColumns}
                        filename={`index_${selectedMonth ?? 'tickers'}`}
                        title={`Download ${filteredTickers.length} tickers as CSV / XLSX`}
                      />
                    </div>
                    <div className="overflow-x-auto max-h-[500px] overflow-y-auto">
                      {loadingMonth ? (
                        <div className="px-5 py-8 text-center text-fg-subtle text-sm"><LoadingDots label="Loading" /></div>
                      ) : (
                        <table className="w-full text-sm">
                          <thead className="sticky top-0 bg-card">
                            <tr className="text-left text-xs text-fg-subtle border-b border-neutral-800/40">
                              <th className="px-5 py-2.5 font-medium w-12">#</th>
                              <th className="px-3 py-2.5 font-medium">Ticker</th>
                              <th className="px-3 py-2.5 font-medium">Exchange</th>
                              <th className="px-3 py-2.5 font-medium">Company</th>
                              <th className="px-3 py-2.5 font-medium">GuruFocus</th>
                            </tr>
                          </thead>
                          <tbody>
                            {filteredTickers.map((t, i) => (
                              <tr key={t.ticker} className="border-b border-neutral-800/20 hover:bg-overlay/[0.02]">
                                <td className="px-5 py-2.5 text-fg-faint font-mono">{i + 1}</td>
                                <td className="px-3 py-2.5 text-fg-strong font-mono font-medium">{t.ticker}</td>
                                <td className="px-3 py-2.5 text-fg-muted font-mono text-xs">{t.exchange || '—'}</td>
                                <td className="px-3 py-2.5 text-fg-soft">{t.company_name || '—'}</td>
                                <td className="px-3 py-2.5">
                                  <a
                                    href={t.gurufocus_url}
                                    target="_blank"
                                    rel="noopener noreferrer"
                                    className="text-xs text-accent-400 hover:text-accent-300 transition-colors"
                                  >
                                    View
                                  </a>
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      )}
                    </div>
                  </div>
                )}
              </>
            )}

            {/* Changelog tab */}
            {activeTab === 'changelog' && (
              <div className="bg-card rounded-xl border border-neutral-800/40">
                <div className="px-5 py-4 border-b border-neutral-800/40">
                  <h3 className="text-sm font-medium text-fg-strong">
                    S&amp;P 500 Changes — {changes.length} total
                    {baseMonth && (
                      <span className="text-fg-subtle font-normal ml-2">
                        Starting from {baseMonth.month} ({baseMonth.ticker_count} tickers)
                      </span>
                    )}
                  </h3>
                </div>
                <div className="overflow-y-auto max-h-[600px]">
                  {changes.length === 0 ? (
                    <div className="px-5 py-8 text-center text-fg-subtle text-sm">No changelog data. Run import first.</div>
                  ) : (
                    <div className="divide-y divide-neutral-800/30">
                      {changeYears.map(year => (
                        <div key={year}>
                          <div className="px-5 py-2.5 bg-page/50 sticky top-0">
                            <span className="text-xs font-medium text-fg-muted">{year}</span>
                            <span className="text-xs text-fg-faint ml-2">
                              ({changesByYear[year].length} changes)
                            </span>
                          </div>
                          <div className="divide-y divide-neutral-800/20">
                            {changesByYear[year].map((c, i) => (
                              <div key={`${c.date}-${i}`} className="px-5 py-2 flex items-center gap-4 hover:bg-overlay/[0.02]">
                                <span className="text-xs text-fg-faint font-mono w-20 shrink-0">{c.date}</span>
                                <div className="flex items-center gap-3 flex-1 min-w-0">
                                  {c.added && (
                                    <span className="inline-flex items-center gap-1 text-xs">
                                      <span className="text-pos-500">+</span>
                                      <span className="text-pos-400 font-mono font-medium">{c.added}</span>
                                    </span>
                                  )}
                                  {c.removed && (
                                    <span className="inline-flex items-center gap-1 text-xs">
                                      <span className="text-neg-500">&minus;</span>
                                      <span className="text-neg-400 font-mono font-medium">{c.removed}</span>
                                    </span>
                                  )}
                                </div>
                              </div>
                            ))}
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              </div>
            )}

            {/* Cumulative tab */}
            {activeTab === 'cumulative' && (
              <div className="bg-card rounded-xl border border-neutral-800/40">
                <div className="px-5 py-4 border-b border-neutral-800/40 flex items-center justify-between">
                  <h3 className="text-sm font-medium text-fg-strong">
                    All Tickers — {cumulative.length} unique
                    {cumulative.length > 0 && (
                      <>
                        <span className="text-fg-subtle font-normal ml-2">
                          ({cumulative.filter(t => t.company_id).length} matched)
                        </span>
                        <span className="text-fg-subtle font-normal ml-2">
                          Exchanges: {[...new Set(cumulative.map(t => t.exchange).filter(Boolean))].sort().join(', ') || '—'}
                        </span>
                      </>
                    )}
                  </h3>
                  <div className="flex items-center gap-2">
                    <input
                      type="text"
                      placeholder="Filter..."
                      value={cumulativeFilter}
                      onChange={e => setCumulativeFilter(e.target.value)}
                      className="px-3 py-1.5 bg-page border border-neutral-700 rounded-lg text-sm text-fg placeholder-fg-faint focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none w-48"
                    />
                    <TableDownloadButton
                      rows={filteredCumulative}
                      columns={tickerExportColumns}
                      filename="index_all_tickers"
                      title={`Download ${filteredCumulative.length} tickers as CSV / XLSX`}
                    />
                  </div>
                </div>
                <div className="overflow-x-auto max-h-[600px] overflow-y-auto">
                  {loadingCumulative ? (
                    <div className="px-5 py-8 text-center text-fg-subtle text-sm"><LoadingDots label="Loading" /></div>
                  ) : (
                    <table className="w-full text-sm">
                      <thead className="sticky top-0 bg-card">
                        <tr className="text-left text-xs text-fg-subtle border-b border-neutral-800/40">
                          <th className="px-5 py-2.5 font-medium w-12">#</th>
                          <th className="px-3 py-2.5 font-medium">Ticker</th>
                          <th className="px-3 py-2.5 font-medium">Exchange</th>
                          <th className="px-3 py-2.5 font-medium">Company</th>
                          <th className="px-3 py-2.5 font-medium">GuruFocus</th>
                        </tr>
                      </thead>
                      <tbody>
                        {(cumulativeFilter
                          ? cumulative.filter(t =>
                              t.ticker.toLowerCase().includes(cumulativeFilter.toLowerCase()) ||
                              (t.company_name || '').toLowerCase().includes(cumulativeFilter.toLowerCase()) ||
                              (t.exchange || '').toLowerCase().includes(cumulativeFilter.toLowerCase())
                            )
                          : cumulative
                        ).map((t, i) => (
                          <tr key={t.ticker} className="border-b border-neutral-800/20 hover:bg-overlay/[0.02]">
                            <td className="px-5 py-2.5 text-fg-faint font-mono">{i + 1}</td>
                            <td className="px-3 py-2.5 text-fg-strong font-mono font-medium">{t.ticker}</td>
                            <td className="px-3 py-2.5 text-fg-muted font-mono text-xs">{t.exchange || '—'}</td>
                            <td className="px-3 py-2.5 text-fg-soft">{t.company_name || '—'}</td>
                            <td className="px-3 py-2.5">
                              <a
                                href={t.gurufocus_url}
                                target="_blank"
                                rel="noopener noreferrer"
                                className="text-xs text-accent-400 hover:text-accent-300 transition-colors"
                              >
                                View
                              </a>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  )}
                </div>
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}
