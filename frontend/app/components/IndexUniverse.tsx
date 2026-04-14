'use client';

import { useState, useRef, useEffect, useCallback, useMemo } from 'react';

const API_URL = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000';

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
type GFResult = {
  available: string[];
  missing: string[];
  total: number;
  available_count: number;
  missing_count: number;
  coverage_pct: number;
};

export default function IndexUniverse() {
  // SSE / progress
  const [running, setRunning] = useState(false);
  const [logs, setLogs] = useState<string[]>([]);
  const logRef = useRef<HTMLDivElement>(null);

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

  // GuruFocus check
  const [checkingGF, setCheckingGF] = useState(false);
  const [gfLogs, setGfLogs] = useState<string[]>([]);
  const [gfResult, setGfResult] = useState<GFResult | null>(null);
  const [showMissing, setShowMissing] = useState(false);
  const gfLogRef = useRef<HTMLDivElement>(null);

  // Auto-scroll logs
  useEffect(() => {
    logRef.current?.scrollTo(0, logRef.current.scrollHeight);
  }, [logs]);
  useEffect(() => {
    gfLogRef.current?.scrollTo(0, gfLogRef.current.scrollHeight);
  }, [gfLogs]);

  // Load indexes on mount
  const loadIndexes = useCallback(() => {
    fetch(`${API_URL}/api/index-universe/indexes`)
      .then(r => r.json())
      .then(data => setIndexes(data))
      .catch(() => {});
  }, []);

  useEffect(() => { loadIndexes(); }, [loadIndexes]);

  // Load months for selected index
  const loadMonths = useCallback((idx: string) => {
    fetch(`${API_URL}/api/index-universe/months?index=${encodeURIComponent(idx)}`)
      .then(r => r.json())
      .then(data => setMonths(data))
      .catch(() => {});
  }, []);

  // Load changes for selected index
  const loadChanges = useCallback((idx: string) => {
    fetch(`${API_URL}/api/index-universe/changes?index=${encodeURIComponent(idx)}`)
      .then(r => r.json())
      .then(data => setChanges(data))
      .catch(() => {});
  }, []);

  // Load cumulative tickers
  const loadCumulative = useCallback((idx: string) => {
    setLoadingCumulative(true);
    setCumulative([]);
    fetch(`${API_URL}/api/index-universe/cumulative?index=${encodeURIComponent(idx)}`)
      .then(r => r.json())
      .then(data => { setCumulative(data); setLoadingCumulative(false); })
      .catch(() => setLoadingCumulative(false));
  }, []);

  // Select an index
  const selectIndex = (idx: string) => {
    setSelectedIndex(idx);
    setSelectedMonth(null);
    setTickers([]);
    setGfResult(null);
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
    fetch(`${API_URL}/api/index-universe/tickers?index=${encodeURIComponent(selectedIndex)}&month=${month}`)
      .then(r => r.json())
      .then(data => { setTickers(data); setLoadingMonth(false); })
      .catch(() => setLoadingMonth(false));
  };

  // SSE reader helper
  const readSSE = (
    url: string,
    method: string,
    onProgress: (msg: string) => void,
    onDone: (evt: Record<string, unknown>) => void,
    onFinish: () => void,
  ) => {
    fetch(url, { method }).then(res => {
      const reader = res.body!.getReader();
      const decoder = new TextDecoder();
      let buffer = '';

      function read() {
        reader.read().then(({ done, value }) => {
          if (done) { onFinish(); return; }
          buffer += decoder.decode(value, { stream: true });
          const lines = buffer.split('\n');
          buffer = lines.pop() || '';
          for (const line of lines) {
            if (!line.startsWith('data: ')) continue;
            try {
              const evt = JSON.parse(line.slice(6));
              if (evt.type === 'progress') onProgress(evt.message);
              else if (evt.type === 'done') { onProgress(evt.message || 'Done'); onDone(evt); }
              else if (evt.type === 'error') onProgress(`ERROR: ${evt.message}`);
            } catch {}
          }
          read();
        });
      }
      read();
    }).catch(() => onFinish());
  };

  // Import S&P 500
  const runImport = () => {
    setRunning(true);
    setLogs([]);
    readSSE(
      `${API_URL}/api/index-universe/import-sp500`,
      'POST',
      msg => setLogs(prev => [...prev, msg]),
      () => {
        loadIndexes();
        setSelectedIndex('SP500');
        loadMonths('SP500');
        loadChanges('SP500');
      },
      () => setRunning(false),
    );
  };

  // Check GuruFocus coverage
  const runGFCheck = () => {
    if (!selectedIndex) return;
    setCheckingGF(true);
    setGfLogs([]);
    setGfResult(null);
    readSSE(
      `${API_URL}/api/index-universe/check-gurufocus?index=${encodeURIComponent(selectedIndex)}`,
      'POST',
      msg => setGfLogs(prev => [...prev, msg]),
      evt => { if (evt.data) setGfResult(evt.data as GFResult); },
      () => setCheckingGF(false),
    );
  };

  // Delete index
  const deleteIndex = (idx: string) => {
    if (!confirm(`Delete all data for ${idx}?`)) return;
    fetch(`${API_URL}/api/index-universe/indexes/${encodeURIComponent(idx)}`, { method: 'DELETE' })
      .then(() => {
        loadIndexes();
        if (selectedIndex === idx) {
          setSelectedIndex(null);
          setMonths([]);
          setSelectedMonth(null);
          setTickers([]);
          setChanges([]);
          setGfResult(null);
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
      <div className="shrink-0 px-8 py-5 border-b border-gray-800/40">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-white text-lg font-semibold">Index Universe</h1>
            <p className="text-gray-500 text-sm mt-0.5">Import and manage index constituent histories</p>
          </div>
          <button
            onClick={runImport}
            disabled={running}
            className="px-4 py-2 rounded-lg text-sm font-medium bg-indigo-600 hover:bg-indigo-500 text-white disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
          >
            {running ? 'Importing...' : 'Import S&P 500'}
          </button>
        </div>
      </div>

      <div className="flex-1 overflow-auto px-8 py-6 space-y-6">
        {/* Progress log */}
        {logs.length > 0 && (
          <div className="bg-[#151821] rounded-xl border border-gray-800/40">
            <div className="flex items-center justify-between px-5 py-3 border-b border-gray-800/40">
              <div className="flex items-center gap-2">
                <h3 className="text-sm font-medium text-white">Import Progress</h3>
                {running && <span className="w-2 h-2 rounded-full bg-indigo-400 animate-pulse" />}
              </div>
              {!running && (
                <button onClick={() => setLogs([])} className="text-xs text-gray-500 hover:text-gray-300">Clear</button>
              )}
            </div>
            <div ref={logRef} className="px-5 py-3 max-h-48 overflow-y-auto font-mono text-xs text-gray-400 space-y-0.5">
              {logs.map((l, i) => <div key={i}>{l}</div>)}
            </div>
          </div>
        )}

        {/* Stored Indexes */}
        <div className="bg-[#151821] rounded-xl border border-gray-800/40">
          <div className="px-5 py-4 border-b border-gray-800/40">
            <h3 className="text-sm font-medium text-white">Stored Indexes</h3>
          </div>
          <div className="p-5">
            {indexes.length === 0 ? (
              <p className="text-sm text-gray-500">No indexes imported yet. Click &quot;Import S&amp;P 500&quot; to get started.</p>
            ) : (
              <div className="grid gap-3">
                {indexes.map(idx => (
                  <div
                    key={idx.index_name}
                    onClick={() => selectIndex(idx.index_name)}
                    className={`flex items-center justify-between px-4 py-3 rounded-lg cursor-pointer transition-colors border ${
                      selectedIndex === idx.index_name
                        ? 'bg-indigo-600/10 border-indigo-500/30'
                        : 'bg-[#0f1117] border-gray-800/40 hover:bg-white/[0.02]'
                    }`}
                  >
                    <div>
                      <span className="text-white font-medium text-sm">{idx.index_name}</span>
                      <span className="text-gray-500 text-xs ml-3">
                        {idx.start_month} — {idx.end_month}
                      </span>
                    </div>
                    <div className="flex items-center gap-4">
                      <span className="text-xs text-gray-400">
                        {idx.month_count} months &middot; {idx.total_unique_tickers} tickers
                      </span>
                      <button
                        onClick={e => { e.stopPropagation(); deleteIndex(idx.index_name); }}
                        className="text-gray-600 hover:text-rose-400 text-xs transition-colors"
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
            <div className="flex gap-1 bg-[#151821] rounded-lg p-1 border border-gray-800/40 w-fit">
              <button
                onClick={() => setActiveTab('months')}
                className={`px-4 py-1.5 rounded-md text-xs font-medium transition-colors ${
                  activeTab === 'months'
                    ? 'bg-indigo-600 text-white'
                    : 'text-gray-400 hover:text-white'
                }`}
              >
                Months
              </button>
              <button
                onClick={() => setActiveTab('changelog')}
                className={`px-4 py-1.5 rounded-md text-xs font-medium transition-colors ${
                  activeTab === 'changelog'
                    ? 'bg-indigo-600 text-white'
                    : 'text-gray-400 hover:text-white'
                }`}
              >
                Changelog ({changes.length})
              </button>
              <button
                onClick={() => { setActiveTab('cumulative'); if (selectedIndex && cumulative.length === 0) loadCumulative(selectedIndex); }}
                className={`px-4 py-1.5 rounded-md text-xs font-medium transition-colors ${
                  activeTab === 'cumulative'
                    ? 'bg-indigo-600 text-white'
                    : 'text-gray-400 hover:text-white'
                }`}
              >
                All Tickers
              </button>
            </div>

            {/* Months tab */}
            {activeTab === 'months' && (
              <>
                {/* Month grid */}
                <div className="bg-[#151821] rounded-xl border border-gray-800/40">
                  <div className="px-5 py-4 border-b border-gray-800/40 flex items-center justify-between">
                    <h3 className="text-sm font-medium text-white">
                      {selectedIndex} — Months ({months.length})
                      {baseMonth && (
                        <span className="text-gray-500 font-normal ml-2">
                          Base: {baseMonth.month} ({baseMonth.ticker_count} tickers)
                        </span>
                      )}
                    </h3>
                    <button
                      onClick={runGFCheck}
                      disabled={checkingGF}
                      className="px-3 py-1.5 rounded-lg text-xs font-medium bg-amber-600/20 text-amber-400 hover:bg-amber-600/30 disabled:opacity-50 disabled:cursor-not-allowed transition-colors border border-amber-600/20"
                    >
                      {checkingGF ? 'Checking...' : 'Check GuruFocus Coverage'}
                    </button>
                  </div>
                  <div className="p-5">
                    {months.length === 0 ? (
                      <p className="text-sm text-gray-500">Loading months...</p>
                    ) : (
                      <div className="flex flex-wrap gap-1.5">
                        {months.map(m => (
                          <button
                            key={m.month}
                            onClick={() => selectMonth(m.month)}
                            className={`px-2 py-1 rounded text-xs font-mono transition-colors ${
                              selectedMonth === m.month
                                ? 'bg-indigo-600 text-white'
                                : 'bg-[#0f1117] text-gray-400 hover:bg-white/[0.04] hover:text-gray-200'
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
                  <div className="bg-[#151821] rounded-xl border border-gray-800/40">
                    <div className="px-5 py-4 border-b border-gray-800/40">
                      <h3 className="text-sm font-medium text-white">GuruFocus Coverage</h3>
                    </div>
                    <div className="p-5 space-y-4">
                      {gfLogs.length > 0 && !gfResult && (
                        <div ref={gfLogRef} className="max-h-32 overflow-y-auto font-mono text-xs text-gray-400 space-y-0.5">
                          {gfLogs.map((l, i) => <div key={i}>{l}</div>)}
                        </div>
                      )}
                      {gfResult && (
                        <div className="space-y-4">
                          <div>
                            <div className="flex items-center justify-between mb-2">
                              <span className="text-sm text-gray-300">
                                {gfResult.available_count} / {gfResult.total} tickers available
                              </span>
                              <span className={`text-sm font-mono font-medium ${
                                gfResult.coverage_pct >= 80 ? 'text-emerald-400' :
                                gfResult.coverage_pct >= 50 ? 'text-amber-400' : 'text-rose-400'
                              }`}>
                                {gfResult.coverage_pct}%
                              </span>
                            </div>
                            <div className="w-full h-2 bg-[#0f1117] rounded-full overflow-hidden">
                              <div
                                className={`h-full rounded-full transition-all ${
                                  gfResult.coverage_pct >= 80 ? 'bg-emerald-500' :
                                  gfResult.coverage_pct >= 50 ? 'bg-amber-500' : 'bg-rose-500'
                                }`}
                                style={{ width: `${gfResult.coverage_pct}%` }}
                              />
                            </div>
                          </div>
                          {gfResult.missing_count > 0 && (
                            <div>
                              <button
                                onClick={() => setShowMissing(!showMissing)}
                                className="text-xs text-gray-500 hover:text-gray-300 transition-colors"
                              >
                                {showMissing ? 'Hide' : 'Show'} {gfResult.missing_count} missing tickers
                              </button>
                              {showMissing && (
                                <div className="mt-2 flex flex-wrap gap-1.5">
                                  {gfResult.missing.map(t => (
                                    <span key={t} className="px-2 py-0.5 bg-rose-500/10 text-rose-400 text-xs font-mono rounded">
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
                  <div className="bg-[#151821] rounded-xl border border-gray-800/40">
                    <div className="px-5 py-4 border-b border-gray-800/40 flex items-center justify-between">
                      <h3 className="text-sm font-medium text-white">
                        {selectedMonth} — {tickers.length} tickers
                        {tickers.length > 0 && (
                          <span className="text-gray-500 font-normal ml-2">
                            ({tickers.filter(t => t.company_id).length} matched to companies)
                          </span>
                        )}
                      </h3>
                      <input
                        type="text"
                        placeholder="Filter..."
                        value={tickerFilter}
                        onChange={e => setTickerFilter(e.target.value)}
                        className="px-3 py-1.5 bg-[#0f1117] border border-gray-700 rounded-lg text-sm text-gray-200 placeholder-gray-600 focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none w-48"
                      />
                    </div>
                    <div className="overflow-x-auto max-h-[500px] overflow-y-auto">
                      {loadingMonth ? (
                        <div className="px-5 py-8 text-center text-gray-500 text-sm">Loading...</div>
                      ) : (
                        <table className="w-full text-sm">
                          <thead className="sticky top-0 bg-[#151821]">
                            <tr className="text-left text-xs text-gray-500 border-b border-gray-800/40">
                              <th className="px-5 py-2.5 font-medium w-12">#</th>
                              <th className="px-3 py-2.5 font-medium">Ticker</th>
                              <th className="px-3 py-2.5 font-medium">Exchange</th>
                              <th className="px-3 py-2.5 font-medium">Company</th>
                              <th className="px-3 py-2.5 font-medium">GuruFocus</th>
                            </tr>
                          </thead>
                          <tbody>
                            {filteredTickers.map((t, i) => (
                              <tr key={t.ticker} className="border-b border-gray-800/20 hover:bg-white/[0.02]">
                                <td className="px-5 py-2.5 text-gray-600 font-mono">{i + 1}</td>
                                <td className="px-3 py-2.5 text-white font-mono font-medium">{t.ticker}</td>
                                <td className="px-3 py-2.5 text-gray-400 font-mono text-xs">{t.exchange || '—'}</td>
                                <td className="px-3 py-2.5 text-gray-300">{t.company_name || '—'}</td>
                                <td className="px-3 py-2.5">
                                  <a
                                    href={t.gurufocus_url}
                                    target="_blank"
                                    rel="noopener noreferrer"
                                    className="text-xs text-indigo-400 hover:text-indigo-300 transition-colors"
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
              <div className="bg-[#151821] rounded-xl border border-gray-800/40">
                <div className="px-5 py-4 border-b border-gray-800/40">
                  <h3 className="text-sm font-medium text-white">
                    S&amp;P 500 Changes — {changes.length} total
                    {baseMonth && (
                      <span className="text-gray-500 font-normal ml-2">
                        Starting from {baseMonth.month} ({baseMonth.ticker_count} tickers)
                      </span>
                    )}
                  </h3>
                </div>
                <div className="overflow-y-auto max-h-[600px]">
                  {changes.length === 0 ? (
                    <div className="px-5 py-8 text-center text-gray-500 text-sm">No changelog data. Run import first.</div>
                  ) : (
                    <div className="divide-y divide-gray-800/30">
                      {changeYears.map(year => (
                        <div key={year}>
                          <div className="px-5 py-2.5 bg-[#0f1117]/50 sticky top-0">
                            <span className="text-xs font-medium text-gray-400">{year}</span>
                            <span className="text-xs text-gray-600 ml-2">
                              ({changesByYear[year].length} changes)
                            </span>
                          </div>
                          <div className="divide-y divide-gray-800/20">
                            {changesByYear[year].map((c, i) => (
                              <div key={`${c.date}-${i}`} className="px-5 py-2 flex items-center gap-4 hover:bg-white/[0.02]">
                                <span className="text-xs text-gray-600 font-mono w-20 shrink-0">{c.date}</span>
                                <div className="flex items-center gap-3 flex-1 min-w-0">
                                  {c.added && (
                                    <span className="inline-flex items-center gap-1 text-xs">
                                      <span className="text-emerald-500">+</span>
                                      <span className="text-emerald-400 font-mono font-medium">{c.added}</span>
                                    </span>
                                  )}
                                  {c.removed && (
                                    <span className="inline-flex items-center gap-1 text-xs">
                                      <span className="text-rose-500">&minus;</span>
                                      <span className="text-rose-400 font-mono font-medium">{c.removed}</span>
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
              <div className="bg-[#151821] rounded-xl border border-gray-800/40">
                <div className="px-5 py-4 border-b border-gray-800/40 flex items-center justify-between">
                  <h3 className="text-sm font-medium text-white">
                    All Tickers — {cumulative.length} unique
                    {cumulative.length > 0 && (
                      <>
                        <span className="text-gray-500 font-normal ml-2">
                          ({cumulative.filter(t => t.company_id).length} matched)
                        </span>
                        <span className="text-gray-500 font-normal ml-2">
                          Exchanges: {[...new Set(cumulative.map(t => t.exchange).filter(Boolean))].sort().join(', ') || '—'}
                        </span>
                      </>
                    )}
                  </h3>
                  <input
                    type="text"
                    placeholder="Filter..."
                    value={cumulativeFilter}
                    onChange={e => setCumulativeFilter(e.target.value)}
                    className="px-3 py-1.5 bg-[#0f1117] border border-gray-700 rounded-lg text-sm text-gray-200 placeholder-gray-600 focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none w-48"
                  />
                </div>
                <div className="overflow-x-auto max-h-[600px] overflow-y-auto">
                  {loadingCumulative ? (
                    <div className="px-5 py-8 text-center text-gray-500 text-sm">Loading...</div>
                  ) : (
                    <table className="w-full text-sm">
                      <thead className="sticky top-0 bg-[#151821]">
                        <tr className="text-left text-xs text-gray-500 border-b border-gray-800/40">
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
                          <tr key={t.ticker} className="border-b border-gray-800/20 hover:bg-white/[0.02]">
                            <td className="px-5 py-2.5 text-gray-600 font-mono">{i + 1}</td>
                            <td className="px-3 py-2.5 text-white font-mono font-medium">{t.ticker}</td>
                            <td className="px-3 py-2.5 text-gray-400 font-mono text-xs">{t.exchange || '—'}</td>
                            <td className="px-3 py-2.5 text-gray-300">{t.company_name || '—'}</td>
                            <td className="px-3 py-2.5">
                              <a
                                href={t.gurufocus_url}
                                target="_blank"
                                rel="noopener noreferrer"
                                className="text-xs text-indigo-400 hover:text-indigo-300 transition-colors"
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
