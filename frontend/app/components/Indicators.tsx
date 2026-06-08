'use client';

import { useState, useEffect, useMemo } from 'react';
import {
  AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer,
  CartesianGrid,
} from 'recharts';
import DatePartsPicker from './DatePartsPicker';
import { apiFetch } from '../../lib/apiFetch';
import { chartTheme } from '../../lib/chartTheme';
import { guruFocusUrl } from '../../lib/gurufocusUrl';
import TableDownloadButton from './TableDownloadButton';
import LoadingDots from './LoadingDots';
import { API_URL } from '../../lib/apiUrl';

type DataPoint = { date: string; value: number };

type FetchResult = {
  success: boolean;
  symbol: string;
  company_id?: number;
  indicator?: string;
  metric_code?: string;
  total_points?: number;
  rows_loaded?: number;
  date_range?: { first: string; last: string };
  source?: string;
  recent?: DataPoint[];
  logs?: string[];
  error?: string;
  raw_preview?: string;
};

export default function Indicators() {
  const [exchange, setExchange] = useState('TPE');
  const [ticker, setTicker] = useState('2330');
  const [indicator, setIndicator] = useState('price');
  const [forceRefresh, setForceRefresh] = useState(false);
  const [fromDate, setFromDate] = useState('');
  const [toDate, setToDate] = useState('');
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<FetchResult | null>(null);
  const [showLogs, setShowLogs] = useState(false);
  const [exchanges, setExchanges] = useState<Record<string, string[]> | null>(null);
  const [exchangesSource, setExchangesSource] = useState<string>('');
  const [exchangesLoading, setExchangesLoading] = useState(true);
  const [exchangeSearch, setExchangeSearch] = useState('');
  const [currencyMap, setCurrencyMap] = useState<Record<string, { country: string; currency: string }>>({});
  const [currencyMapLoaded, setCurrencyMapLoaded] = useState(false);
  const [currencyMapLoading, setCurrencyMapLoading] = useState(false);

  useEffect(() => {
    (async () => {
      try {
        const res = await apiFetch(`${API_URL}/api/gurufocus/exchanges`);
        const data = await res.json();
        setExchanges(data.exchanges);
        setExchangesSource(data.source);
      } catch {}
      setExchangesLoading(false);
    })();
  }, []);

  const loadCurrencyMap = async (force = false) => {
    setCurrencyMapLoading(true);
    try {
      const res = await apiFetch(`${API_URL}/api/gurufocus/exchange-currencies?force_refresh=${force}`);
      const data = await res.json();
      const map: Record<string, { country: string; currency: string }> = {};
      for (const m of data.mapping) {
        map[m.exchange_code] = { country: m.country, currency: m.currency };
      }
      setCurrencyMap(map);
      setCurrencyMapLoaded(true);
    } catch {}
    setCurrencyMapLoading(false);
  };

  const fetchIndicator = async () => {
    setLoading(true);
    setResult(null);
    try {
      const res = await apiFetch(`${API_URL}/api/indicators/fetch`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          exchange,
          ticker,
          indicator,
          force_refresh: forceRefresh,
          from_date: fromDate || null,
          to_date: toDate || null,
        }),
      });
      const data = await res.json();
      setResult(data);
    } catch (e) {
      setResult({
        success: false,
        symbol: `${exchange}:${ticker}`,
        error: e instanceof Error ? e.message : String(e),
      });
    } finally {
      setLoading(false);
    }
  };

  const chartData = useMemo(() => result?.recent ?? [], [result]);

  const yDomain = useMemo(() => {
    if (chartData.length === 0) return [0, 1] as [number, number];
    const values = chartData.map(d => d.value);
    const min = Math.min(...values);
    const max = Math.max(...values);
    const pad = (max - min) * 0.05 || max * 0.05;
    return [min - pad, max + pad] as [number, number];
  }, [chartData]);

  return (
    <div className="px-8 py-5 space-y-6">
      <div>
        <h1 className="text-xl font-semibold text-fg-strong">Indicators</h1>
        <p className="text-sm text-fg-muted mt-1">
          Fetch indicator data from GuruFocus, cache in storage, store in DB
        </p>
      </div>

      {/* Input form */}
      <div className="bg-card rounded-xl border border-neutral-800/40 p-5">
        <div className="flex flex-wrap items-end gap-4">
          <div>
            <label className="block text-xs text-fg-muted mb-1">Exchange</label>
            <input
              type="text"
              value={exchange}
              onChange={e => setExchange(e.target.value)}
              placeholder="TPE"
              className="bg-page border border-neutral-700 rounded-lg px-3 py-2 text-sm text-fg-strong w-28 focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none"
            />
          </div>
          <div>
            <label className="block text-xs text-fg-muted mb-1">Ticker</label>
            <input
              type="text"
              value={ticker}
              onChange={e => setTicker(e.target.value)}
              placeholder="2330"
              className="bg-page border border-neutral-700 rounded-lg px-3 py-2 text-sm text-fg-strong w-32 focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none"
            />
          </div>
          <div>
            <label className="block text-xs text-fg-muted mb-1">Indicator</label>
            <select
              value={indicator}
              onChange={e => setIndicator(e.target.value)}
              className="bg-page border border-neutral-700 rounded-lg px-3 py-2 text-sm text-fg-strong w-32 focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none"
            >
              <option value="price">price</option>
              <option value="volume">volume</option>
            </select>
          </div>
          <div>
            <label className="block text-xs text-fg-muted mb-1">From</label>
            <DatePartsPicker
              value={fromDate}
              onChange={setFromDate}
              minYear={1990}
              maxYear={new Date().getFullYear()}
              allowEmpty
            />
          </div>
          <div>
            <label className="block text-xs text-fg-muted mb-1">To</label>
            <DatePartsPicker
              value={toDate}
              onChange={setToDate}
              minYear={1990}
              maxYear={new Date().getFullYear() + 1}
              allowEmpty
            />
          </div>
          <div className="flex items-center gap-2">
            <input
              type="checkbox"
              id="force-refresh"
              checked={forceRefresh}
              onChange={e => setForceRefresh(e.target.checked)}
              className="rounded border-neutral-600 bg-page text-accent-500 focus:ring-accent-500/30"
            />
            <label htmlFor="force-refresh" className="text-xs text-fg-muted">Force refresh</label>
          </div>
          <button
            onClick={fetchIndicator}
            disabled={loading || !exchange || !ticker}
            className="px-4 py-2 rounded-lg bg-accent-600 hover:bg-accent-500 text-fg-strong text-sm font-medium disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
          >
            {loading ? 'Fetching...' : 'Fetch'}
          </button>
        </div>
        <p className="text-xs text-fg-subtle mt-3">
          GuruFocus URL: <span className="font-mono text-fg-muted">{guruFocusUrl(ticker, exchange)}</span>
          {!fromDate && !toDate && (
            <span className="ml-3 text-fg-faint">Tip: set From/To to see the full series per date.</span>
          )}
        </p>
      </div>

      {/* Result */}
      {result && (
        <div className="space-y-4">
          {/* Status banner */}
          {result.success ? (
            <div className="bg-pos-500/10 border border-pos-500/20 rounded-lg px-4 py-3">
              <div className="flex items-center justify-between">
                <div>
                  <span className="text-pos-400 font-medium text-sm">{result.symbol}</span>
                  <span className="text-fg-muted text-sm ml-2">
                    {result.indicator} &middot; {result.total_points?.toLocaleString()} data points &middot;
                    {result.rows_loaded?.toLocaleString()} rows loaded &middot;
                    source: {result.source}
                  </span>
                </div>
                <span className="text-xs text-fg-subtle font-mono">
                  company_id={result.company_id} &middot; metric_code={result.metric_code}
                </span>
              </div>
              {result.date_range && (
                <p className="text-xs text-fg-subtle mt-1">
                  Range: {result.date_range.first} to {result.date_range.last}
                </p>
              )}
            </div>
          ) : (
            <div className="bg-neg-500/10 border border-neg-500/20 rounded-lg px-4 py-3">
              <span className="text-neg-400 font-medium text-sm">{result.symbol}</span>
              <span className="text-neg-400/80 text-sm ml-2">{result.error}</span>
              {result.raw_preview && (
                <pre className="text-xs text-fg-subtle mt-2 overflow-x-auto">{result.raw_preview}</pre>
              )}
            </div>
          )}

          {/* Chart of recent data */}
          {result.success && chartData.length > 0 && (
            <div className="bg-card rounded-xl border border-neutral-800/40 p-5">
              <h2 className="text-sm font-medium text-fg-strong mb-3">
                Last {chartData.length} data points
              </h2>
              <ResponsiveContainer width="100%" height={300}>
                <AreaChart data={chartData}>
                  <defs>
                    <linearGradient id="indGradient" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor={chartTheme.accent} stopOpacity={0.3} />
                      <stop offset="95%" stopColor={chartTheme.accent} stopOpacity={0} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" stroke={chartTheme.grid} />
                  <XAxis
                    dataKey="date"
                    tick={{ fill: chartTheme.axisTick, fontSize: 11 }}
                    tickLine={false}
                    interval={Math.max(0, Math.floor(chartData.length / 6) - 1)}
                  />
                  <YAxis
                    tick={{ fill: chartTheme.axisTick, fontSize: 11 }}
                    tickLine={false}
                    domain={yDomain}
                    width={70}
                    tickFormatter={(v: number) => {
                      if (v >= 1_000_000) return `${(v / 1_000_000).toFixed(1)}M`;
                      if (v >= 1_000) return `${(v / 1_000).toFixed(1)}K`;
                      return v.toFixed(v < 10 ? 2 : 0);
                    }}
                  />
                  <Tooltip
                    contentStyle={chartTheme.tooltipPopover.contentStyle}
                    labelStyle={chartTheme.tooltipPopover.labelStyle}
                    formatter={(value) => {
                      const v = Number(value);
                      const formatted = v >= 1_000_000
                        ? `${(v / 1_000_000).toFixed(2)}M`
                        : v >= 1_000
                          ? v.toLocaleString(undefined, { maximumFractionDigits: 2 })
                          : v.toFixed(4);
                      return [formatted, indicator];
                    }}
                  />
                  <Area
                    type="monotone"
                    dataKey="value"
                    stroke={chartTheme.accent}
                    strokeWidth={1.5}
                    fill="url(#indGradient)"
                    dot={false}
                  />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          )}

          {/* Date → value table */}
          {result.success && chartData.length > 0 && (
            <div className="bg-card rounded-xl border border-neutral-800/40">
              <div className="px-5 py-3 border-b border-neutral-800/40 flex items-center justify-between">
                <h2 className="text-sm font-medium text-fg-strong">
                  Values per date <span className="text-fg-subtle font-normal">({chartData.length.toLocaleString()} rows)</span>
                </h2>
                <div className="flex items-center gap-3">
                  <span className="text-xs text-fg-subtle">
                    {fromDate || toDate ? 'Filtered window' : 'Last 30 trading days (set From/To to widen)'}
                  </span>
                  <TableDownloadButton
                    rows={[...chartData].reverse()}
                    columns={[
                      { key: 'date', header: 'Date', accessor: (d: { date: string; value: number }) => d.date },
                      { key: 'value', header: indicator === 'volume' ? 'Volume' : 'Price', accessor: (d: { date: string; value: number }) => d.value },
                    ]}
                    filename={`${exchange || 'X'}_${ticker || 'TKR'}_${indicator || 'price'}`}
                    title={`Download ${chartData.length} rows as CSV / XLSX`}
                  />
                </div>
              </div>
              <div className="overflow-x-auto max-h-[500px] overflow-y-auto">
                <table className="w-full text-sm">
                  <thead className="sticky top-0 bg-card z-10">
                    <tr className="text-fg-muted text-xs uppercase tracking-wider">
                      <th className="text-left px-4 py-2 font-medium w-12">#</th>
                      <th className="text-left px-4 py-2 font-medium">Date</th>
                      <th className="text-right px-4 py-2 font-medium">{indicator === 'volume' ? 'Volume' : 'Price'}</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-neutral-800/30">
                    {[...chartData].reverse().map((d, i) => (
                      <tr key={d.date} className="hover:bg-overlay/[0.02]">
                        <td className="px-4 py-1.5 text-fg-subtle font-mono text-xs">{i + 1}</td>
                        <td className="px-4 py-1.5 text-fg font-mono text-xs">{d.date}</td>
                        <td className="px-4 py-1.5 text-fg font-mono text-right">
                          {indicator === 'volume'
                            ? d.value.toLocaleString(undefined, { maximumFractionDigits: 0 })
                            : d.value.toLocaleString(undefined, { minimumFractionDigits: 4, maximumFractionDigits: 4 })}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {/* Logs */}
          {result.logs && result.logs.length > 0 && (
            <div className="bg-card rounded-xl border border-neutral-800/40">
              <button
                onClick={() => setShowLogs(!showLogs)}
                className="w-full px-5 py-3 flex items-center justify-between text-sm text-fg-muted hover:text-fg transition-colors"
              >
                <span>Logs ({result.logs.length})</span>
                <span>{showLogs ? '\u25B2' : '\u25BC'}</span>
              </button>
              {showLogs && (
                <div className="px-5 pb-4 space-y-1">
                  {result.logs.map((log, i) => (
                    <p key={i} className="text-xs font-mono text-fg-subtle">{log}</p>
                  ))}
                </div>
              )}
            </div>
          )}
        </div>
      )}

      {/* Supported exchanges table */}
      <div className="bg-card rounded-xl border border-neutral-800/40">
        <div className="px-5 py-3 border-b border-neutral-800/40 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <h2 className="text-sm font-medium text-fg-strong">Supported GuruFocus Exchanges</h2>
            {exchanges && (
              <span className="text-xs text-fg-subtle">
                {Object.keys(exchanges).length} countries &middot; {Object.values(exchanges).flat().length} exchanges
                &middot; source: {exchangesSource}
              </span>
            )}
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={() => loadCurrencyMap(false)}
              disabled={currencyMapLoading}
              className="px-3 py-1.5 rounded-lg text-xs font-medium transition-colors disabled:opacity-50 bg-page border border-neutral-700 text-fg-soft hover:border-accent-500 hover:text-accent-400"
            >
              {currencyMapLoading ? <LoadingDots label="Loading" /> : currencyMapLoaded ? 'Currencies loaded' : 'Load currencies'}
            </button>
            {currencyMapLoaded && (
              <button
                onClick={() => loadCurrencyMap(true)}
                disabled={currencyMapLoading}
                className="px-2 py-1.5 rounded-lg text-xs text-fg-subtle hover:text-fg-soft transition-colors disabled:opacity-50"
              >
                refresh
              </button>
            )}
            <input
              type="text"
              value={exchangeSearch}
              onChange={e => setExchangeSearch(e.target.value)}
              placeholder="Search..."
              className="bg-page border border-neutral-700 rounded-lg px-3 py-1.5 text-xs text-fg-strong w-40 focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none"
            />
            <TableDownloadButton
              rows={(() => {
                const out: { country: string; exchange_code: string; currency: string | null }[] = [];
                const q = exchangeSearch.toLowerCase();
                for (const [country, codes] of Object.entries(exchanges ?? {})) {
                  const passes = !q || country.toLowerCase().includes(q)
                    || codes.some(c => c.toLowerCase().includes(q))
                    || (currencyMapLoaded && codes.some(c => currencyMap[c]?.currency.toLowerCase().includes(q)));
                  if (!passes) continue;
                  for (const code of codes) {
                    out.push({
                      country,
                      exchange_code: code,
                      currency: currencyMapLoaded ? currencyMap[code]?.currency ?? null : null,
                    });
                  }
                }
                return out;
              })()}
              columns={[
                { key: 'country', header: 'Country', accessor: (r) => r.country },
                { key: 'exchange_code', header: 'Exchange', accessor: (r) => r.exchange_code },
                { key: 'currency', header: 'Currency', accessor: (r) => r.currency ?? '' },
              ]}
              filename="gurufocus_exchanges"
              title="Download supported exchanges as CSV / XLSX"
            />
          </div>
        </div>
        {exchangesLoading ? (
          <div className="px-5 py-4 text-fg-muted text-sm"><LoadingDots label="Loading exchanges" /></div>
        ) : exchanges ? (
          <div className="overflow-x-auto max-h-[500px] overflow-y-auto">
            <table className="w-full text-sm">
              <thead className="sticky top-0 bg-card z-10">
                <tr className="text-fg-muted text-xs uppercase tracking-wider">
                  <th className="text-left px-4 py-2.5 font-medium w-8">#</th>
                  <th className="text-left px-4 py-2.5 font-medium">Country</th>
                  <th className="text-left px-4 py-2.5 font-medium">Exchanges</th>
                  {currencyMapLoaded && <th className="text-left px-4 py-2.5 font-medium">Currency</th>}
                  <th className="text-right px-4 py-2.5 font-medium">Count</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-neutral-800/30">
                {Object.entries(exchanges)
                  .filter(([country, codes]) => {
                    if (!exchangeSearch) return true;
                    const q = exchangeSearch.toLowerCase();
                    return country.toLowerCase().includes(q)
                      || codes.some(c => c.toLowerCase().includes(q))
                      || (currencyMapLoaded && codes.some(c => currencyMap[c]?.currency.toLowerCase().includes(q)));
                  })
                  .map(([country, codes], i) => {
                    const curr = currencyMapLoaded ? currencyMap[codes[0]]?.currency : null;
                    return (
                      <tr key={country} className="hover:bg-overlay/[0.02]">
                        <td className="px-4 py-2.5 text-fg-subtle font-mono text-xs">{i + 1}</td>
                        <td className="px-4 py-2.5 text-fg-strong">{country}</td>
                        <td className="px-4 py-2.5">
                          <div className="flex flex-wrap gap-1.5">
                            {codes.map(code => (
                              <button
                                key={code}
                                onClick={() => setExchange(code)}
                                className="px-2 py-0.5 rounded bg-accent-500/10 border border-accent-500/20 text-accent-400 text-xs font-mono hover:bg-accent-500/20 transition-colors cursor-pointer"
                              >
                                {code}
                              </button>
                            ))}
                          </div>
                        </td>
                        {currencyMapLoaded && (
                          <td className="px-4 py-2.5 font-mono text-xs text-fg-soft">
                            {curr ?? <span className="text-fg-faint">-</span>}
                          </td>
                        )}
                        <td className="px-4 py-2.5 text-right text-fg-muted font-mono text-xs">{codes.length}</td>
                      </tr>
                    );
                  })}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="px-5 py-4 text-fg-subtle text-sm">Failed to load exchanges</div>
        )}
      </div>
    </div>
  );
}
