'use client';

import { useState, useEffect, useCallback, useMemo } from 'react';
import {
  AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer,
  CartesianGrid,
} from 'recharts';
import { trackedFetch } from '../../lib/loading';
import type { Column } from '../../lib/tableExport';
import TableDownloadButton from './TableDownloadButton';

const API_URL = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000';

type CurrencyMeta = { name: string; country: string };

type CoverageData = {
  ecb_currencies: string[];
  acwi_currencies: string[];
  currency_counts: Record<string, number>;
  currency_info: Record<string, CurrencyMeta>;
  covered: string[];
  missing: string[];
  eur_count: number;
  unmapped_exchanges: Record<string, number>;
};

type LatestRate = {
  currency: string;
  name: string;
  country: string;
  rate: number;
  date: string;
  source?: string;
};

type HistoryPoint = {
  date: string;
  rate: number;
};

function FxHistoryChart({ currency, history, loading, refreshing, onClose }: {
  currency: string;
  history: HistoryPoint[];
  loading: boolean;
  refreshing: boolean;
  onClose: () => void;
}) {
  const [inverted, setInverted] = useState(false);

  // Downsample for chart performance — weekly points
  const chartData = useMemo(() => {
    const src = history.length <= 500
      ? history
      : (() => {
          const step = Math.max(1, Math.floor(history.length / 500));
          const sampled = history.filter((_, i) => i % step === 0);
          if (sampled[sampled.length - 1] !== history[history.length - 1]) {
            sampled.push(history[history.length - 1]);
          }
          return sampled;
        })();
    if (!inverted) return src;
    return src.map(h => ({ date: h.date, rate: 1 / h.rate }));
  }, [history, inverted]);

  const stats = useMemo(() => {
    if (history.length === 0) return null;
    const rates = history.map(h => inverted ? 1 / h.rate : h.rate);
    const latest = inverted
      ? { date: history[history.length - 1].date, rate: 1 / history[history.length - 1].rate }
      : history[history.length - 1];
    const first = inverted
      ? { date: history[0].date, rate: 1 / history[0].rate }
      : history[0];
    return { latest, first, min: Math.min(...rates), max: Math.max(...rates) };
  }, [history, inverted]);

  // Y-axis domain with ~5% padding
  const yDomain = useMemo(() => {
    if (!stats) return [0, 1] as [number, number];
    const range = stats.max - stats.min || stats.max * 0.1;
    const pad = range * 0.05;
    return [stats.min - pad, stats.max + pad] as [number, number];
  }, [stats]);

  // Determine decimal places based on rate magnitude
  const decimals = stats && stats.latest.rate < 10 ? 4 : stats && stats.latest.rate < 1000 ? 2 : 0;

  const label = inverted ? `EUR/${currency}` : `${currency}/EUR`;
  const description = inverted
    ? `EUR per 1 ${currency}`
    : `${currency} per 1 EUR`;

  return (
    <div className="bg-[#151821] rounded-xl border border-gray-800/40 p-5">
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-3">
          <h2 className="text-sm font-medium text-white">{label} History (from 2000)</h2>
          <span className="text-xs text-gray-500">{description}</span>
          {refreshing && (
            <span className="text-xs text-indigo-400 animate-pulse">refreshing…</span>
          )}
        </div>
        <div className="flex items-center gap-3">
          <button
            onClick={() => setInverted(!inverted)}
            className="px-2.5 py-1 rounded-md text-xs border transition-colors bg-[#0f1117] border-gray-700 text-gray-300 hover:border-indigo-500 hover:text-indigo-400"
          >
            Flip
          </button>
          <button onClick={onClose} className="text-xs text-gray-500 hover:text-gray-300">close</button>
        </div>
      </div>
      {loading ? (
        <p className="text-gray-400 text-sm">Loading history...</p>
      ) : history.length === 0 ? (
        <p className="text-gray-500 text-sm">No data available</p>
      ) : stats && (
        <div>
          <div className="grid grid-cols-4 gap-4 mb-4">
            <div>
              <div className="text-lg font-mono text-white">{stats.latest.rate.toFixed(decimals)}</div>
              <div className="text-xs text-gray-400">Latest ({stats.latest.date})</div>
            </div>
            <div>
              <div className="text-lg font-mono text-gray-300">{stats.first.rate.toFixed(decimals)}</div>
              <div className="text-xs text-gray-400">First ({stats.first.date})</div>
            </div>
            <div>
              <div className="text-lg font-mono text-gray-300">{stats.min.toFixed(decimals)}</div>
              <div className="text-xs text-gray-400">Min</div>
            </div>
            <div>
              <div className="text-lg font-mono text-gray-300">{stats.max.toFixed(decimals)}</div>
              <div className="text-xs text-gray-400">Max</div>
            </div>
          </div>
          <div className="text-xs text-gray-500 mb-3">
            {history.length.toLocaleString()} daily data points from {stats.first.date} to {stats.latest.date}
          </div>
          <ResponsiveContainer width="100%" height={300}>
            <AreaChart data={chartData}>
              <defs>
                <linearGradient id="fxGradient" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#818cf8" stopOpacity={0.3} />
                  <stop offset="95%" stopColor="#818cf8" stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
              <XAxis
                dataKey="date"
                tick={{ fill: '#6b7280', fontSize: 11 }}
                tickLine={false}
                interval={Math.max(0, Math.floor(chartData.length / 8) - 1)}
                tickFormatter={(d: string) => d.slice(0, 7)}
              />
              <YAxis
                tick={{ fill: '#6b7280', fontSize: 11 }}
                tickLine={false}
                domain={yDomain}
                tickFormatter={(v: number) => v.toFixed(decimals)}
                width={65}
              />
              <Tooltip
                contentStyle={{
                  backgroundColor: '#1e2230',
                  border: '1px solid rgba(107,114,128,0.3)',
                  borderRadius: '8px',
                  fontSize: 12,
                }}
                labelStyle={{ color: '#9ca3af' }}
                formatter={(value) => [Number(value).toFixed(decimals), label]}
                labelFormatter={(l) => String(l)}
              />
              <Area
                type="monotone"
                dataKey="rate"
                stroke="#818cf8"
                strokeWidth={1.5}
                fill="url(#fxGradient)"
                dot={false}
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      )}
    </div>
  );
}

export default function FxRates() {
  const [coverage, setCoverage] = useState<CoverageData | null>(null);
  const [latestRates, setLatestRates] = useState<LatestRate[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedCurrency, setSelectedCurrency] = useState<string | null>(null);
  const [history, setHistory] = useState<HistoryPoint[]>([]);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [historyRefreshing, setHistoryRefreshing] = useState(false);
  const [syncing, setSyncing] = useState(false);
  const [syncMessage, setSyncMessage] = useState<string | null>(null);
  const [latestSource, setLatestSource] = useState<string | null>(null);

  const reload = useCallback(async () => {
    setError(null);
    try {
      const [covRes, ratesRes] = await Promise.all([
        trackedFetch('Loading ACWI currency coverage', `${API_URL}/api/fx/coverage`),
        trackedFetch('Loading latest FX rates', `${API_URL}/api/fx/latest`),
      ]);
      if (!covRes.ok || !ratesRes.ok) throw new Error('Failed to fetch FX data');
      setCoverage(await covRes.json());
      const ratesData = await ratesRes.json();
      setLatestRates(ratesData.rates);
      setLatestSource(ratesData.source ?? null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, []);

  useEffect(() => {
    (async () => {
      setLoading(true);
      await reload();
      setLoading(false);
    })();
  }, [reload]);

  const syncFromEcb = useCallback(async () => {
    setSyncing(true);
    setSyncMessage('Syncing from ECB / Yahoo…');
    try {
      const res = await trackedFetch(
        'Syncing FX rates from ECB',
        `${API_URL}/api/fx/sync`,
        { method: 'POST' },
      );
      if (!res.ok) throw new Error('Sync failed');
      const data = await res.json();
      const failed = data.failed?.length ?? 0;
      const synced = data.synced?.length ?? 0;
      setSyncMessage(
        failed > 0
          ? `Synced ${synced}, failed ${failed}: ${data.failed.join(', ')}`
          : `Synced ${synced} currencies`,
      );
      await reload();
    } catch (e) {
      setSyncMessage(`Sync failed: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setSyncing(false);
    }
  }, [reload]);

  const loadHistory = useCallback(async (currency: string) => {
    setSelectedCurrency(currency);
    setHistoryLoading(true);
    setHistoryRefreshing(false);

    // Phase 1: render whatever is cached in our DB instantly.
    let cached: { rates: HistoryPoint[]; is_stale?: boolean; is_fetchable?: boolean } = { rates: [] };
    try {
      const res = await trackedFetch(
        `Loading ${currency} history`,
        `${API_URL}/api/fx/history/${currency}?start_date=2000-01-01`,
      );
      if (!res.ok) throw new Error('Failed to fetch history');
      cached = await res.json();
      setHistory(cached.rates);
    } catch {
      setHistory([]);
    } finally {
      setHistoryLoading(false);
    }

    // Phase 2: stale-while-revalidate. If we have no rows or the latest row
    // is older than today, fire a background refresh and swap in fresh data
    // when ECB returns. Skip currencies the backend can't fetch.
    if (cached.is_fetchable && cached.is_stale) {
      setHistoryRefreshing(true);
      try {
        const res = await trackedFetch(
          `Refreshing ${currency} from ECB`,
          `${API_URL}/api/fx/history/${currency}/refresh?start_date=2000-01-01`,
          { method: 'POST' },
        );
        if (res.ok) {
          const data = await res.json();
          // Bail out if the user switched currencies while we were waiting.
          setSelectedCurrency(prev => {
            if (prev === currency) setHistory(data.rates);
            return prev;
          });
        }
      } catch {
        // leave the cached chart in place
      } finally {
        setHistoryRefreshing(false);
      }
    }
  }, []);

  if (loading) {
    return (
      <div className="px-8 py-5">
        <h1 className="text-xl font-semibold text-white mb-4">FX Rates</h1>
        <p className="text-gray-400">Loading ECB exchange rates...</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="px-8 py-5">
        <h1 className="text-xl font-semibold text-white mb-4">FX Rates</h1>
        <div className="bg-rose-500/10 border border-rose-500/20 rounded-lg px-4 py-3 text-rose-400">{error}</div>
      </div>
    );
  }

  const rateMap = Object.fromEntries(latestRates.map(r => [r.currency, r]));
  const ci = (code: string) => coverage?.currency_info?.[code] ?? { name: code, country: '' };
  const rateDate = latestRates[0]?.date ?? '';

  // Flatten the on-screen rate table (EUR + covered ACWI + missing) into
  // a single export. Status mirrors the column label: base / ecb /
  // yahoo / pegged / missing.
  type RateExportRow = {
    currency: string;
    name: string;
    country: string;
    rate_per_eur: number | null;
    eur_per_unit: number | null;
    acwi_holdings: number;
    status: string;
  };
  const exportRows = useMemo<RateExportRow[]>(() => {
    const out: RateExportRow[] = [
      {
        currency: 'EUR',
        name: 'Euro',
        country: 'Eurozone',
        rate_per_eur: 1,
        eur_per_unit: 1,
        acwi_holdings: coverage?.currency_counts['EUR'] ?? 0,
        status: 'base',
      },
    ];
    for (const c of coverage?.covered ?? []) {
      const r = rateMap[c];
      out.push({
        currency: c,
        name: ci(c).name,
        country: ci(c).country,
        rate_per_eur: r?.rate ?? null,
        eur_per_unit: r ? 1 / r.rate : null,
        acwi_holdings: coverage?.currency_counts[c] ?? 0,
        status: r?.source ?? 'ecb',
      });
    }
    for (const c of coverage?.missing ?? []) {
      out.push({
        currency: c,
        name: ci(c).name,
        country: ci(c).country,
        rate_per_eur: null,
        eur_per_unit: null,
        acwi_holdings: coverage?.currency_counts[c] ?? 0,
        status: 'missing',
      });
    }
    return out;
  }, [coverage, rateMap]);
  const exportColumns = useMemo<Column<RateExportRow>[]>(() => [
    { key: 'currency', header: 'Currency', accessor: (r) => r.currency },
    { key: 'name', header: 'Name', accessor: (r) => r.name },
    { key: 'country', header: 'Country', accessor: (r) => r.country },
    { key: 'rate_per_eur', header: 'Rate (per 1 EUR)', accessor: (r) => r.rate_per_eur },
    { key: 'eur_per_unit', header: 'Inverse (EUR per 1)', accessor: (r) => r.eur_per_unit },
    { key: 'acwi_holdings', header: 'ACWI Holdings', accessor: (r) => r.acwi_holdings },
    { key: 'status', header: 'Status', accessor: (r) => r.status },
    { key: 'as_of', header: 'As of', accessor: () => rateDate },
  ], [rateDate]);

  const totalAcwi = coverage ? Object.values(coverage.currency_counts).reduce((a, b) => a + b, 0) : 0;
  const coveredCount = coverage ? coverage.covered.reduce((sum, c) => sum + (coverage.currency_counts[c] || 0), 0) : 0;
  const eurCount = coverage?.eur_count ?? 0;
  const convertibleCount = coveredCount + eurCount;
  const convertiblePct = totalAcwi > 0 ? ((convertibleCount / totalAcwi) * 100).toFixed(1) : '0';

  return (
    <div className="px-8 py-5 space-y-6">
      <div className="flex items-start justify-between gap-4">
        <div>
          <h1 className="text-xl font-semibold text-white">FX Rates</h1>
          <p className="text-sm text-gray-400 mt-1">ECB daily exchange rates vs EUR &mdash; for converting ACWI prices to a common currency</p>
          {latestSource === 'ecb_live' && (
            <p className="text-xs text-amber-400 mt-2">
              Loaded live from ECB (local <span className="font-mono">fx_rate</span> table is empty). Click <b>Sync from ECB</b> once to persist data for fast loads.
            </p>
          )}
          {syncMessage && (
            <p className="text-xs text-gray-500 mt-2">{syncMessage}</p>
          )}
        </div>
        <button
          onClick={syncFromEcb}
          disabled={syncing}
          className="shrink-0 px-3 py-1.5 rounded-lg text-xs font-medium bg-indigo-600 hover:bg-indigo-500 text-white disabled:opacity-50 transition-colors"
        >
          {syncing ? 'Syncing…' : 'Sync from ECB'}
        </button>
      </div>

      {/* Coverage summary */}
      {coverage && (
        <div className="bg-[#151821] rounded-xl border border-gray-800/40 p-5">
          <h2 className="text-sm font-medium text-white mb-3">ACWI Currency Coverage</h2>
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-4 mb-4">
            <div>
              <div className="text-2xl font-mono text-white">{coverage.acwi_currencies.length}</div>
              <div className="text-xs text-gray-400">Currencies in ACWI</div>
            </div>
            <div>
              <div className="text-2xl font-mono text-emerald-400">{coverage.covered.length + 1}</div>
              <div className="text-xs text-gray-400">Convertible to EUR (incl. EUR)</div>
            </div>
            <div>
              <div className="text-2xl font-mono text-rose-400">{coverage.missing.length}</div>
              <div className="text-xs text-gray-400">Missing from ECB</div>
            </div>
            <div>
              <div className="text-2xl font-mono text-indigo-400">{convertiblePct}%</div>
              <div className="text-xs text-gray-400">Holdings convertible ({convertibleCount}/{totalAcwi})</div>
            </div>
          </div>

          {coverage.missing.length > 0 && (
            <div className="mt-3">
              <p className="text-xs text-gray-400 mb-1">Missing currencies (no ECB rate):</p>
              <div className="flex flex-wrap gap-2">
                {coverage.missing.map(c => (
                  <span key={c} className="px-2 py-0.5 rounded bg-rose-500/10 border border-rose-500/20 text-rose-400 text-xs font-mono">
                    {c} <span className="text-rose-400/60">({coverage.currency_counts[c]})</span>
                  </span>
                ))}
              </div>
            </div>
          )}
          {Object.keys(coverage.unmapped_exchanges).length > 0 && (
            <div className="mt-3">
              <p className="text-xs text-gray-400 mb-1">Unmapped exchanges (no currency assigned):</p>
              <div className="flex flex-wrap gap-2">
                {Object.entries(coverage.unmapped_exchanges).map(([exch, count]) => (
                  <span key={exch} className="px-2 py-0.5 rounded bg-amber-500/10 border border-amber-500/20 text-amber-400 text-xs">
                    {exch} <span className="text-amber-400/60">({count})</span>
                  </span>
                ))}
              </div>
            </div>
          )}
        </div>
      )}

      {/* Latest rates table, grouped by ACWI vs non-ACWI */}
      <div className="bg-[#151821] rounded-xl border border-gray-800/40">
        <div className="px-5 py-3 border-b border-gray-800/40 flex items-center justify-between">
          <h2 className="text-sm font-medium text-white">Latest ECB Rates vs EUR</h2>
          <div className="flex items-center gap-3">
            <span className="text-xs text-gray-500">as of {rateDate}</span>
            <TableDownloadButton
              rows={exportRows}
              columns={exportColumns}
              filename="fx_rates"
              title={`Download ${exportRows.length} FX rates as CSV / XLSX`}
            />
          </div>
        </div>
        <div className="overflow-x-auto max-h-[600px] overflow-y-auto">
          <table className="w-full text-sm">
            <thead className="sticky top-0 bg-[#151821] z-10">
              <tr className="text-gray-400 text-xs uppercase tracking-wider">
                <th className="text-left px-4 py-2.5 font-medium">Currency</th>
                <th className="text-left px-4 py-2.5 font-medium">Name</th>
                <th className="text-left px-4 py-2.5 font-medium">Country</th>
                <th className="text-right px-4 py-2.5 font-medium">Rate (per 1 EUR)</th>
                <th className="text-right px-4 py-2.5 font-medium">Inverse (EUR per 1)</th>
                <th className="text-right px-4 py-2.5 font-medium">ACWI Holdings</th>
                <th className="text-center px-4 py-2.5 font-medium">Status</th>
                <th className="text-left px-4 py-2.5 font-medium"></th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-800/30">
              {/* EUR row first */}
              <tr className="hover:bg-white/[0.02]">
                <td className="px-4 py-2.5 font-mono text-white font-medium">EUR</td>
                <td className="px-4 py-2.5 text-gray-300">Euro</td>
                <td className="px-4 py-2.5 text-gray-400">Eurozone</td>
                <td className="px-4 py-2.5 text-right font-mono text-gray-300">1.0000</td>
                <td className="px-4 py-2.5 text-right font-mono text-gray-300">1.0000</td>
                <td className="px-4 py-2.5 text-right font-mono text-gray-300">{coverage?.currency_counts['EUR'] ?? 0}</td>
                <td className="px-4 py-2.5 text-center"><span className="text-emerald-400 text-xs">base</span></td>
                <td className="px-4 py-2.5"></td>
              </tr>
              {/* ACWI currencies with ECB rates */}
              {coverage?.covered.map(c => {
                const r = rateMap[c];
                return (
                  <tr key={c} className="hover:bg-white/[0.02]">
                    <td className="px-4 py-2.5 font-mono text-white font-medium">{c}</td>
                    <td className="px-4 py-2.5 text-gray-300">{ci(c).name}</td>
                    <td className="px-4 py-2.5 text-gray-400">{ci(c).country}</td>
                    <td className="px-4 py-2.5 text-right font-mono text-gray-300">{r ? r.rate.toFixed(4) : '-'}</td>
                    <td className="px-4 py-2.5 text-right font-mono text-gray-300">{r ? (1 / r.rate).toFixed(4) : '-'}</td>
                    <td className="px-4 py-2.5 text-right font-mono text-gray-300">{coverage.currency_counts[c] ?? 0}</td>
                    <td className="px-4 py-2.5 text-center">
                      <span className="text-emerald-400 text-xs">{r?.source === 'pegged' ? 'pegged' : r?.source === 'yahoo' ? 'yahoo' : 'ecb'}</span>
                    </td>
                    <td className="px-4 py-2.5">
                      <button
                        onClick={() => loadHistory(c)}
                        className="text-xs text-indigo-400 hover:text-indigo-300 transition-colors"
                      >
                        history
                      </button>
                    </td>
                  </tr>
                );
              })}
              {/* Missing ACWI currencies */}
              {coverage?.missing.map(c => (
                <tr key={c} className="hover:bg-white/[0.02]">
                  <td className="px-4 py-2.5 font-mono text-white font-medium">{c}</td>
                  <td className="px-4 py-2.5 text-gray-300">{ci(c).name}</td>
                  <td className="px-4 py-2.5 text-gray-400">{ci(c).country}</td>
                  <td className="px-4 py-2.5 text-right font-mono text-gray-500">-</td>
                  <td className="px-4 py-2.5 text-right font-mono text-gray-500">-</td>
                  <td className="px-4 py-2.5 text-right font-mono text-gray-300">{coverage.currency_counts[c] ?? 0}</td>
                  <td className="px-4 py-2.5 text-center"><span className="text-rose-400 text-xs">missing</span></td>
                  <td className="px-4 py-2.5"></td>
                </tr>
              ))}
              {/* ECB-only currencies (not in ACWI) */}
              {latestRates.filter(r => !coverage?.acwi_currencies.includes(r.currency)).map(r => (
                <tr key={r.currency} className="hover:bg-white/[0.02] opacity-50">
                  <td className="px-4 py-2.5 font-mono text-gray-400">{r.currency}</td>
                  <td className="px-4 py-2.5 text-gray-500">{r.name}</td>
                  <td className="px-4 py-2.5 text-gray-500">{r.country}</td>
                  <td className="px-4 py-2.5 text-right font-mono text-gray-500">{r.rate.toFixed(4)}</td>
                  <td className="px-4 py-2.5 text-right font-mono text-gray-500">{(1 / r.rate).toFixed(4)}</td>
                  <td className="px-4 py-2.5 text-right font-mono text-gray-600">0</td>
                  <td className="px-4 py-2.5 text-center"><span className="text-gray-600 text-xs">ecb only</span></td>
                  <td className="px-4 py-2.5">
                    <button
                      onClick={() => loadHistory(r.currency)}
                      className="text-xs text-gray-500 hover:text-gray-400 transition-colors"
                    >
                      history
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* History panel */}
      {selectedCurrency && (
        <FxHistoryChart
          currency={selectedCurrency}
          history={history}
          loading={historyLoading}
          refreshing={historyRefreshing}
          onClose={() => setSelectedCurrency(null)}
        />
      )}
    </div>
  );
}
