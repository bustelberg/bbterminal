'use client';

import { useState, useEffect, useCallback, useMemo } from 'react';
import {
  AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer,
  CartesianGrid,
} from 'recharts';

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

function FxHistoryChart({ currency, history, loading, onClose }: {
  currency: string;
  history: HistoryPoint[];
  loading: boolean;
  onClose: () => void;
}) {
  // Downsample for chart performance — weekly points
  const chartData = useMemo(() => {
    if (history.length <= 500) return history;
    const step = Math.max(1, Math.floor(history.length / 500));
    const sampled = history.filter((_, i) => i % step === 0);
    // Always include the last point
    if (sampled[sampled.length - 1] !== history[history.length - 1]) {
      sampled.push(history[history.length - 1]);
    }
    return sampled;
  }, [history]);

  const stats = useMemo(() => {
    if (history.length === 0) return null;
    const rates = history.map(h => h.rate);
    return {
      latest: history[history.length - 1],
      first: history[0],
      min: Math.min(...rates),
      max: Math.max(...rates),
    };
  }, [history]);

  // Y-axis domain with ~5% padding
  const yDomain = useMemo(() => {
    if (!stats) return [0, 1] as [number, number];
    const range = stats.max - stats.min || stats.max * 0.1;
    const pad = range * 0.05;
    return [stats.min - pad, stats.max + pad] as [number, number];
  }, [stats]);

  // Determine decimal places based on rate magnitude
  const decimals = stats && stats.latest.rate < 10 ? 4 : stats && stats.latest.rate < 1000 ? 2 : 0;

  return (
    <div className="bg-[#151821] rounded-xl border border-gray-800/40 p-5">
      <div className="flex items-center justify-between mb-3">
        <h2 className="text-sm font-medium text-white">{currency}/EUR History (from 2000)</h2>
        <button onClick={onClose} className="text-xs text-gray-500 hover:text-gray-300">close</button>
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
                formatter={(value) => [Number(value).toFixed(decimals), `${currency}/EUR`]}
                labelFormatter={(label) => String(label)}
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

  useEffect(() => {
    async function load() {
      setLoading(true);
      setError(null);
      try {
        const [covRes, ratesRes] = await Promise.all([
          fetch(`${API_URL}/api/fx/coverage`),
          fetch(`${API_URL}/api/fx/latest`),
        ]);
        if (!covRes.ok || !ratesRes.ok) throw new Error('Failed to fetch FX data');
        const covData = await covRes.json();
        const ratesData = await ratesRes.json();
        setCoverage(covData);
        setLatestRates(ratesData.rates);
      } catch (e: any) {
        setError(e.message);
      } finally {
        setLoading(false);
      }
    }
    load();
  }, []);

  const loadHistory = useCallback(async (currency: string) => {
    setSelectedCurrency(currency);
    setHistoryLoading(true);
    try {
      const res = await fetch(`${API_URL}/api/fx/history/${currency}?start_date=2000-01-01`);
      if (!res.ok) throw new Error('Failed to fetch history');
      const data = await res.json();
      setHistory(data.rates);
    } catch {
      setHistory([]);
    } finally {
      setHistoryLoading(false);
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

  const totalAcwi = coverage ? Object.values(coverage.currency_counts).reduce((a, b) => a + b, 0) : 0;
  const coveredCount = coverage ? coverage.covered.reduce((sum, c) => sum + (coverage.currency_counts[c] || 0), 0) : 0;
  const eurCount = coverage?.eur_count ?? 0;
  const convertibleCount = coveredCount + eurCount;
  const convertiblePct = totalAcwi > 0 ? ((convertibleCount / totalAcwi) * 100).toFixed(1) : '0';

  return (
    <div className="px-8 py-5 space-y-6">
      <div>
        <h1 className="text-xl font-semibold text-white">FX Rates</h1>
        <p className="text-sm text-gray-400 mt-1">ECB daily exchange rates vs EUR &mdash; for converting ACWI prices to a common currency</p>
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
          <span className="text-xs text-gray-500">as of {rateDate}</span>
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
          onClose={() => setSelectedCurrency(null)}
        />
      )}
    </div>
  );
}
