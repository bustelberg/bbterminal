'use client';

import { useState, useEffect, useMemo } from 'react';
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer,
  CartesianGrid,
} from 'recharts';

const API_URL = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

type SignalDef = {
  key: string;
  label: string;
  description: string;
  default_weight: number;
};

type Holding = {
  company_id: number;
  ticker: string;
  company_name: string;
  sector: string;
  score: number;
  weight: number;
  forward_return_pct: number | null;
};

type MonthlyRecord = {
  date: string;
  holdings: Holding[];
  portfolio_return_pct: number | null;
  cumulative_return_pct: number;
};

type Summary = {
  total_return_pct: number;
  annualized_return_pct: number;
  max_drawdown_pct: number;
  sharpe_ratio: number | null;
  avg_monthly_turnover_pct: number;
  total_months: number;
  avg_holdings: number;
};

type BacktestResult = {
  monthly_records: MonthlyRecord[];
  summary: Summary;
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const fmtPct = (v: number | null) => (v != null ? `${v >= 0 ? '+' : ''}${v.toFixed(2)}%` : '—');

const tooltipStyle = {
  contentStyle: { background: '#1a1d27', border: '1px solid rgba(75,85,99,0.4)', borderRadius: 8, fontSize: 13 },
  labelStyle: { color: '#9ca3af' },
  itemStyle: { color: '#e5e7eb' },
};

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export default function MomentumBacktester() {
  // Signal definitions from backend
  const [signalDefs, setSignalDefs] = useState<SignalDef[]>([]);
  const [weights, setWeights] = useState<Record<string, number>>({});

  // Config
  const currentYear = new Date().getFullYear();
  const [startDate, setStartDate] = useState(`${currentYear - 10}-01`);
  const [endDate, setEndDate] = useState(`${currentYear}-01`);
  const [topSectors, setTopSectors] = useState(4);
  const [topPerSector, setTopPerSector] = useState(6);
  const [skipPriceFetch, setSkipPriceFetch] = useState(false);
  const [maxCompanies, setMaxCompanies] = useState(0);

  // State
  const [running, setRunning] = useState(false);
  const [progress, setProgress] = useState<{ pct: number; message: string }[]>([]);
  const [result, setResult] = useState<BacktestResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [expandedMonth, setExpandedMonth] = useState<string | null>(null);

  // Load signal definitions
  useEffect(() => {
    fetch(`${API_URL}/api/momentum/signals`)
      .then((r) => r.json())
      .then((d) => {
        const defs: SignalDef[] = d.signals ?? [];
        setSignalDefs(defs);
        const w: Record<string, number> = {};
        defs.forEach((s) => (w[s.key] = s.default_weight));
        setWeights(w);
      })
      .catch(() => {});
  }, []);

  // Chart data
  const chartData = useMemo(() => {
    if (!result) return [];
    return result.monthly_records.map((r) => ({
      date: r.date,
      cumReturn: r.cumulative_return_pct,
      monthReturn: r.portfolio_return_pct,
    }));
  }, [result]);

  // Run backtest
  const runBacktest = async () => {
    setRunning(true);
    setProgress([]);
    setResult(null);
    setError(null);
    setExpandedMonth(null);

    try {
      const resp = await fetch(`${API_URL}/api/momentum/backtest`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          start_date: `${startDate}-01`,
          end_date: `${endDate}-01`,
          signal_weights: weights,
          top_n_sectors: topSectors,
          top_n_per_sector: topPerSector,
          skip_price_fetch: skipPriceFetch,
          max_companies: maxCompanies,
        }),
      });

      if (!resp.ok || !resp.body) {
        setError(`Request failed: ${resp.status}`);
        setRunning(false);
        return;
      }

      const reader = resp.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });

        const lines = buffer.split('\n');
        buffer = lines.pop() ?? '';

        for (const line of lines) {
          if (!line.startsWith('data: ')) continue;
          try {
            const data = JSON.parse(line.slice(6));
            if (data.type === 'progress') {
              setProgress((prev) => [...prev, { pct: data.pct, message: data.message }]);
            } else if (data.type === 'result') {
              setResult(data.data);
            } else if (data.type === 'done') {
              setRunning(false);
            } else if (data.type === 'error') {
              setError(data.message);
              setRunning(false);
            }
          } catch {}
        }
      }
      setRunning(false);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Unknown error');
      setRunning(false);
    }
  };

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="px-8 py-5 border-b border-gray-800/60">
        <h1 className="text-lg font-semibold text-white">Momentum Backtester</h1>
        <p className="text-xs text-gray-500 mt-0.5">
          Price momentum portfolio — equal-weight, monthly rebalancing, sector-filtered
        </p>
      </div>

      <div className="flex-1 overflow-auto px-8 py-5 space-y-5">
        {/* Config Panel */}
        <div className="bg-[#151821] rounded-xl border border-gray-800/40 p-5">
          <div className="flex flex-wrap items-end gap-5 mb-5">
            {/* Date Range */}
            <div>
              <label className="text-gray-500 text-xs block mb-1">Start</label>
              <input
                type="month"
                value={startDate}
                onChange={(e) => setStartDate(e.target.value)}
                className="bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-white text-sm font-mono focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
              />
            </div>
            <div>
              <label className="text-gray-500 text-xs block mb-1">End</label>
              <input
                type="month"
                value={endDate}
                onChange={(e) => setEndDate(e.target.value)}
                className="bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-white text-sm font-mono focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
              />
            </div>
            <div>
              <label className="text-gray-500 text-xs block mb-1">Top Sectors</label>
              <input
                type="number"
                min={1}
                max={20}
                value={topSectors}
                onChange={(e) => setTopSectors(Number(e.target.value))}
                className="w-16 bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-white text-sm font-mono text-center focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
              />
            </div>
            <div>
              <label className="text-gray-500 text-xs block mb-1">Per Sector</label>
              <input
                type="number"
                min={1}
                max={20}
                value={topPerSector}
                onChange={(e) => setTopPerSector(Number(e.target.value))}
                className="w-16 bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-white text-sm font-mono text-center focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
              />
            </div>
            <div>
              <label className="text-gray-500 text-xs block mb-1">Max Companies</label>
              <input
                type="number"
                min={0}
                max={500}
                value={maxCompanies}
                onChange={(e) => setMaxCompanies(Number(e.target.value))}
                className="w-20 bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-white text-sm font-mono text-center focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
                title="0 = all companies, otherwise limit alphabetically"
              />
              <span className="text-gray-600 text-xs ml-1">0 = all</span>
            </div>
            <label className="flex items-center gap-2 cursor-pointer self-center pt-4">
              <input
                type="checkbox"
                checked={skipPriceFetch}
                onChange={(e) => setSkipPriceFetch(e.target.checked)}
                className="accent-indigo-500 w-4 h-4 cursor-pointer"
              />
              <span className="text-gray-400 text-xs">Skip price fetch</span>
            </label>
            <button
              onClick={runBacktest}
              disabled={running}
              className="px-5 py-2 rounded-lg text-sm font-medium bg-indigo-600 hover:bg-indigo-500 text-white transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {running ? 'Running...' : 'Run Backtest'}
            </button>
          </div>

          {/* Signal Weights */}
          <div>
            <h3 className="text-gray-400 text-xs font-medium mb-3 uppercase tracking-wider">Signal Weights</h3>
            <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-x-5 gap-y-3">
              {signalDefs.map((s) => (
                <div key={s.key} className="flex items-center gap-2">
                  <input
                    type="range"
                    min={0}
                    max={10}
                    step={1}
                    value={weights[s.key] ?? 0}
                    onChange={(e) => setWeights((prev) => ({ ...prev, [s.key]: Number(e.target.value) }))}
                    className="flex-1 h-1 accent-indigo-500 cursor-pointer"
                    title={s.description}
                  />
                  <span className="text-gray-500 text-xs w-5 text-right font-mono">{weights[s.key] ?? 0}</span>
                  <span className="text-gray-400 text-xs truncate w-20" title={s.description}>{s.label}</span>
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Progress */}
        {running && progress.length > 0 && (
          <div className="bg-[#151821] rounded-xl border border-gray-800/40 p-4">
            <div className="flex items-center gap-3 mb-3">
              <div className="h-1.5 flex-1 bg-gray-800 rounded-full overflow-hidden">
                <div
                  className="h-full bg-indigo-500 rounded-full transition-all duration-300"
                  style={{ width: `${progress[progress.length - 1]?.pct ?? 0}%` }}
                />
              </div>
              <span className="text-gray-400 text-xs font-mono">{progress[progress.length - 1]?.pct ?? 0}%</span>
            </div>
            <div className="max-h-32 overflow-auto space-y-0.5">
              {progress.map((p, i) => (
                <div key={i} className="text-gray-500 text-xs">{p.message}</div>
              ))}
            </div>
          </div>
        )}

        {/* Error */}
        {error && (
          <div className="bg-rose-500/10 border border-rose-500/20 rounded-lg p-4 text-rose-400 text-sm">
            {error}
          </div>
        )}

        {/* Results */}
        {result && (
          <>
            {/* Summary Stats */}
            <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-7 gap-4">
              {[
                { label: 'Total Return', value: fmtPct(result.summary.total_return_pct), color: result.summary.total_return_pct >= 0 ? 'text-emerald-400' : 'text-rose-400' },
                { label: 'Annualized', value: fmtPct(result.summary.annualized_return_pct), color: result.summary.annualized_return_pct >= 0 ? 'text-emerald-400' : 'text-rose-400' },
                { label: 'Max Drawdown', value: fmtPct(result.summary.max_drawdown_pct), color: 'text-rose-400' },
                { label: 'Sharpe Ratio', value: result.summary.sharpe_ratio != null ? result.summary.sharpe_ratio.toFixed(2) : '—', color: 'text-white' },
                { label: 'Avg Turnover', value: fmtPct(result.summary.avg_monthly_turnover_pct), color: 'text-gray-300' },
                { label: 'Months', value: String(result.summary.total_months), color: 'text-gray-300' },
                { label: 'Avg Holdings', value: result.summary.avg_holdings.toFixed(1), color: 'text-gray-300' },
              ].map((s) => (
                <div key={s.label} className="bg-[#151821] rounded-xl border border-gray-800/40 p-4">
                  <div className="text-gray-500 text-xs mb-1">{s.label}</div>
                  <div className={`font-mono text-lg font-semibold ${s.color}`}>{s.value}</div>
                </div>
              ))}
            </div>

            {/* Equity Curve */}
            <div className="bg-[#151821] rounded-xl border border-gray-800/40 p-5">
              <h3 className="text-white text-sm font-medium mb-4">Equity Curve (Cumulative Return %)</h3>
              <ResponsiveContainer width="100%" height={350}>
                <LineChart data={chartData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
                  <XAxis
                    dataKey="date"
                    tick={{ fill: '#6b7280', fontSize: 11 }}
                    tickLine={false}
                    interval={Math.max(0, Math.floor(chartData.length / 12) - 1)}
                  />
                  <YAxis
                    tick={{ fill: '#6b7280', fontSize: 11 }}
                    tickLine={false}
                    tickFormatter={(v: number) => `${v}%`}
                  />
                  <Tooltip
                    {...tooltipStyle}
                    formatter={(value, name) => {
                      const v = Number(value);
                      return [`${v >= 0 ? '+' : ''}${v.toFixed(2)}%`, name === 'cumReturn' ? 'Cumulative' : 'Monthly'];
                    }}
                  />
                  <Line
                    type="monotone"
                    dataKey="cumReturn"
                    stroke="#818cf8"
                    strokeWidth={2}
                    dot={false}
                    name="Cumulative"
                  />
                </LineChart>
              </ResponsiveContainer>
            </div>

            {/* Monthly Portfolio Table */}
            <div className="bg-[#151821] rounded-xl border border-gray-800/40">
              <div className="px-5 py-4 border-b border-gray-800/40">
                <h3 className="text-white text-sm font-medium">Monthly Portfolios</h3>
              </div>
              <div className="max-h-[500px] overflow-auto">
                <table className="w-full text-sm">
                  <thead className="sticky top-0 bg-[#151821]">
                    <tr className="text-gray-500 text-xs border-b border-gray-800/40">
                      <th className="text-left px-5 py-2.5 font-medium">Month</th>
                      <th className="text-right px-3 py-2.5 font-medium">Holdings</th>
                      <th className="text-right px-3 py-2.5 font-medium">Return</th>
                      <th className="text-right px-5 py-2.5 font-medium">Cumulative</th>
                    </tr>
                  </thead>
                  <tbody>
                    {result.monthly_records.map((r) => (
                      <>
                        <tr
                          key={r.date}
                          className="border-b border-gray-800/20 hover:bg-white/[0.02] cursor-pointer transition-colors"
                          onClick={() => setExpandedMonth(expandedMonth === r.date ? null : r.date)}
                        >
                          <td className="px-5 py-2.5 text-gray-300 font-mono">
                            <span className="text-gray-600 mr-2">{expandedMonth === r.date ? '▾' : '▸'}</span>
                            {r.date}
                          </td>
                          <td className="text-right px-3 py-2.5 text-gray-400 font-mono">{r.holdings.length}</td>
                          <td className={`text-right px-3 py-2.5 font-mono ${r.portfolio_return_pct != null ? (r.portfolio_return_pct >= 0 ? 'text-emerald-400' : 'text-rose-400') : 'text-gray-600'}`}>
                            {fmtPct(r.portfolio_return_pct)}
                          </td>
                          <td className={`text-right px-5 py-2.5 font-mono ${r.cumulative_return_pct >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                            {fmtPct(r.cumulative_return_pct)}
                          </td>
                        </tr>
                        {expandedMonth === r.date && r.holdings.length > 0 && (
                          <tr key={`${r.date}-detail`}>
                            <td colSpan={4} className="bg-[#0f1117] px-5 py-3">
                              <table className="w-full text-xs">
                                <thead>
                                  <tr className="text-gray-600">
                                    <th className="text-left py-1 font-medium">Ticker</th>
                                    <th className="text-left py-1 font-medium">Company</th>
                                    <th className="text-left py-1 font-medium">Sector</th>
                                    <th className="text-right py-1 font-medium">Score</th>
                                    <th className="text-right py-1 font-medium">Weight</th>
                                    <th className="text-right py-1 font-medium">Return</th>
                                  </tr>
                                </thead>
                                <tbody>
                                  {r.holdings
                                    .sort((a, b) => b.score - a.score)
                                    .map((h) => (
                                      <tr key={h.company_id} className="border-t border-gray-800/20">
                                        <td className="py-1.5 text-indigo-400 font-mono">{h.ticker}</td>
                                        <td className="py-1.5 text-gray-300 truncate max-w-[200px]">{h.company_name}</td>
                                        <td className="py-1.5 text-gray-500">{h.sector}</td>
                                        <td className="text-right py-1.5 text-gray-300 font-mono">{h.score.toFixed(1)}</td>
                                        <td className="text-right py-1.5 text-gray-400 font-mono">{(h.weight * 100).toFixed(1)}%</td>
                                        <td className={`text-right py-1.5 font-mono ${h.forward_return_pct != null ? (h.forward_return_pct >= 0 ? 'text-emerald-400' : 'text-rose-400') : 'text-gray-600'}`}>
                                          {fmtPct(h.forward_return_pct)}
                                        </td>
                                      </tr>
                                    ))}
                                </tbody>
                              </table>
                            </td>
                          </tr>
                        )}
                      </>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>

            {/* Disclaimer */}
            <p className="text-gray-600 text-xs">
              Note: Uses current company universe applied retroactively (survivorship bias). Returns are hypothetical and do not account for transaction costs.
            </p>
          </>
        )}
      </div>
    </div>
  );
}
