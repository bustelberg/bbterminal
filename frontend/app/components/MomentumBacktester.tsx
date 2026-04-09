'use client';

import { useState, useEffect, useMemo } from 'react';
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer,
  CartesianGrid, Legend,
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
  group?: string;
};

type Holding = {
  company_id: number;
  ticker: string;
  company_name: string;
  sector: string;
  score: number;
  category_scores: Record<string, number | null>;
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

type UniverseEntry = {
  company_id: number;
  ticker: string;
  exchange: string;
  company_name: string;
  sector: string;
};

type SavedRun = {
  run_id: number;
  name: string;
  created_at: string;
  config: Record<string, unknown>;
  summary: Summary;
};

type BenchmarkOption = {
  benchmark_id: number;
  ticker: string;
  name: string;
};

type BenchmarkPrice = {
  target_date: string;
  price: number;
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
  const [categories, setCategories] = useState<string[]>([]);
  const [categoryWeights, setCategoryWeights] = useState<Record<string, number>>({});

  // Config
  const currentYear = new Date().getFullYear();
  const [startDate, setStartDate] = useState('2017-01');
  const [endDate, setEndDate] = useState(`${currentYear}-01`);
  const [topSectors, setTopSectors] = useState(4);
  const [topPerSector, setTopPerSector] = useState(6);
  const [skipPriceFetch, setSkipPriceFetch] = useState(false);
  const [maxCompanies, setMaxCompanies] = useState(0);

  // State
  const [running, setRunning] = useState(false);
  const [progress, setProgress] = useState<{ pct: number; message: string }[]>([]);
  const [result, setResult] = useState<BacktestResult | null>(null);
  const [universe, setUniverse] = useState<UniverseEntry[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [expandedMonth, setExpandedMonth] = useState<string | null>(null);

  // Save/load state
  const [savedRuns, setSavedRuns] = useState<SavedRun[]>([]);
  const [saveName, setSaveName] = useState('');
  const [saving, setSaving] = useState(false);
  const [loadedRunId, setLoadedRunId] = useState<number | null>(null);

  // Benchmark state
  const [benchmarkOptions, setBenchmarkOptions] = useState<BenchmarkOption[]>([]);
  const [selectedBenchmarkId, setSelectedBenchmarkId] = useState<number | null>(null);
  const [benchmarkPrices, setBenchmarkPrices] = useState<BenchmarkPrice[]>([]);
  const [logScale, setLogScale] = useState(false);

  // Load signal definitions + saved runs
  useEffect(() => {
    fetch(`${API_URL}/api/momentum/signals`)
      .then((r) => r.json())
      .then((d) => {
        const defs: SignalDef[] = d.signals ?? [];
        setSignalDefs(defs);
        const w: Record<string, number> = {};
        defs.forEach((s) => (w[s.key] = s.default_weight));
        setWeights(w);
        const cats: string[] = d.categories ?? [];
        setCategories(cats);
        const cw: Record<string, number> = {};
        cats.forEach((c) => (cw[c] = 50));
        setCategoryWeights(cw);
      })
      .catch(() => {});
    loadSavedRuns();
    fetch(`${API_URL}/api/benchmarks`)
      .then((r) => r.json())
      .then((data) => setBenchmarkOptions(data))
      .catch(() => {});
  }, []);

  const loadSavedRuns = () => {
    fetch(`${API_URL}/api/momentum/backtests`)
      .then((r) => r.json())
      .then((data) => setSavedRuns(data))
      .catch(() => {});
  };

  // Fetch benchmark prices when selection or date range changes
  useEffect(() => {
    if (!selectedBenchmarkId || !result) {
      setBenchmarkPrices([]);
      return;
    }
    const dates = result.monthly_records.map((r) => r.date);
    if (dates.length === 0) return;
    const sd = `${dates[0]}-01`;
    const ed = `${dates[dates.length - 1]}-28`;
    fetch(`${API_URL}/api/benchmarks/${selectedBenchmarkId}/prices?start_date=${sd}&end_date=${ed}`)
      .then((r) => r.json())
      .then((data) => setBenchmarkPrices(data))
      .catch(() => setBenchmarkPrices([]));
  }, [selectedBenchmarkId, result]);

  // Compute benchmark monthly returns aligned to backtest months
  const benchmarkReturns = useMemo(() => {
    if (!result || benchmarkPrices.length === 0) return null;

    const months = result.monthly_records.map((r) => r.date);
    const priceByMonth = new Map<string, number>();

    // For each month, find the first available price on or after the 1st
    for (const bp of benchmarkPrices) {
      const month = bp.target_date.slice(0, 7);
      if (!priceByMonth.has(month)) {
        priceByMonth.set(month, bp.price);
      }
    }

    // Compute cumulative return aligned with the strategy:
    // The strategy record for month[i] includes the forward return earned
    // during that month (price change month[i] → month[i+1]).
    // So benchmark cumReturns[month[i]] should also include that same period's return.
    const cumReturns: Record<string, number> = {};
    let cumFactor = 1.0;
    const monthlyRets: number[] = [];

    for (let i = 0; i < months.length; i++) {
      const p0 = priceByMonth.get(months[i]);
      const p1 = i < months.length - 1 ? priceByMonth.get(months[i + 1]) : null;
      if (p0 && p1 && p0 > 0) {
        const ret = (p1 / p0 - 1) * 100;
        monthlyRets.push(ret);
        cumFactor *= (1 + ret / 100);
      }
      cumReturns[months[i]] = (cumFactor - 1) * 100;
    }

    // Summary stats
    const totalReturn = (cumFactor - 1) * 100;
    const years = monthlyRets.length / 12;
    const annualized = years > 0 ? (Math.pow(cumFactor, 1 / years) - 1) * 100 : 0;

    // Max drawdown
    let peak = 1.0;
    let maxDd = 0;
    let factor = 1.0;
    for (const ret of monthlyRets) {
      factor *= (1 + ret / 100);
      peak = Math.max(peak, factor);
      const dd = ((factor / peak) - 1) * 100;
      maxDd = Math.min(maxDd, dd);
    }

    // Sharpe
    let sharpe: number | null = null;
    if (monthlyRets.length >= 12) {
      const mean = monthlyRets.reduce((a, b) => a + b, 0) / monthlyRets.length;
      const std = Math.sqrt(monthlyRets.reduce((a, b) => a + (b - mean) ** 2, 0) / monthlyRets.length);
      if (std > 0) sharpe = (mean / std) * Math.sqrt(12);
    }

    return { cumReturns, totalReturn, annualized, maxDd, sharpe };
  }, [result, benchmarkPrices]);

  // Chart data — prepend a 0% origin so both lines start from the same point
  const chartData = useMemo(() => {
    if (!result) return [];
    const firstDate = result.monthly_records[0]?.date;
    const origin = firstDate
      ? { date: `${firstDate} (start)`, cumReturn: 0, monthReturn: null, benchmark: selectedBenchmarkId ? 0 : null }
      : null;
    const points = result.monthly_records.map((r) => ({
      date: r.date,
      cumReturn: r.cumulative_return_pct,
      monthReturn: r.portfolio_return_pct,
      benchmark: benchmarkReturns?.cumReturns[r.date] ?? null,
    }));
    return origin ? [origin, ...points] : points;
  }, [result, benchmarkReturns, selectedBenchmarkId]);

  // Log-scale chart data: ln(1 + cumReturn/100) * 100
  const displayChartData = useMemo(() => {
    if (!logScale) return chartData;
    return chartData.map((p) => ({
      ...p,
      cumReturn: p.cumReturn != null ? Math.log(1 + p.cumReturn / 100) * 100 : null,
      benchmark: p.benchmark != null ? Math.log(1 + p.benchmark / 100) * 100 : null,
    }));
  }, [chartData, logScale]);

  // Run backtest
  const runBacktest = async () => {
    setRunning(true);
    setProgress([]);
    setResult(null);
    setUniverse([]);
    setError(null);
    setExpandedMonth(null);
    setLoadedRunId(null);

    try {
      const resp = await fetch(`${API_URL}/api/momentum/backtest`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          start_date: `${startDate}-01`,
          end_date: `${endDate}-01`,
          signal_weights: weights,
          category_weights: categoryWeights,
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
      let receivedDone = false;
      let receivedResult = false;
      let lastEventTime = Date.now();

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        lastEventTime = Date.now();
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
              receivedResult = true;
              if (data.universe) setUniverse(data.universe);
            } else if (data.type === 'done') {
              receivedDone = true;
              setRunning(false);
            } else if (data.type === 'error') {
              setError(data.message);
              setRunning(false);
            } else if (data.type === 'keepalive') {
              // ignore, just keeps connection alive
            }
          } catch (parseErr) {
            console.warn('SSE parse error:', line, parseErr);
          }
        }
      }
      // Stream ended — if we never got a 'done' event, something went wrong
      if (!receivedDone) {
        if (receivedResult) {
          // Got result but stream cut before 'done' — still usable
          setRunning(false);
        } else {
          const elapsed = Math.round((Date.now() - lastEventTime) / 1000);
          setError(`Stream disconnected unexpectedly (last event ${elapsed}s ago). This can happen due to proxy timeouts — try again with "Skip data fetch" checked if prices are already loaded.`);
          setRunning(false);
        }
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Unknown error');
      setRunning(false);
    }
  };

  const saveBacktest = async () => {
    if (!result || !saveName.trim()) return;
    setSaving(true);
    try {
      const resp = await fetch(`${API_URL}/api/momentum/backtests`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: saveName.trim(),
          config: {
            start_date: `${startDate}-01`,
            end_date: `${endDate}-01`,
            signal_weights: weights,
            category_weights: categoryWeights,
            top_n_sectors: topSectors,
            top_n_per_sector: topPerSector,
          },
          summary: result.summary,
          monthly_records: result.monthly_records,
          universe,
        }),
      });
      if (resp.ok) {
        const saved = await resp.json();
        setSaveName('');
        setLoadedRunId(saved.run_id);
        loadSavedRuns();
      }
    } catch {}
    setSaving(false);
  };

  const loadBacktest = async (runId: number) => {
    try {
      const resp = await fetch(`${API_URL}/api/momentum/backtests/${runId}`);
      if (!resp.ok) return;
      const data = await resp.json();

      // Restore config
      const cfg = data.config ?? {};
      if (cfg.start_date) setStartDate(cfg.start_date.slice(0, 7));
      if (cfg.end_date) setEndDate(cfg.end_date.slice(0, 7));
      if (cfg.signal_weights) setWeights(cfg.signal_weights);
      if (cfg.category_weights) setCategoryWeights(cfg.category_weights);
      if (cfg.top_n_sectors) setTopSectors(cfg.top_n_sectors);
      if (cfg.top_n_per_sector) setTopPerSector(cfg.top_n_per_sector);

      // Restore result
      setResult({ monthly_records: data.monthly_records, summary: data.summary });
      setUniverse(data.universe ?? []);
      setLoadedRunId(runId);
      setError(null);
      setProgress([]);
      setExpandedMonth(null);
    } catch {
      setError('Failed to load backtest');
    }
  };

  const deleteBacktest = async (runId: number) => {
    try {
      await fetch(`${API_URL}/api/momentum/backtests/${runId}`, { method: 'DELETE' });
      if (loadedRunId === runId) setLoadedRunId(null);
      loadSavedRuns();
    } catch {}
  };

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="px-8 py-5 border-b border-gray-800/60 flex items-center justify-between">
        <div>
          <h1 className="text-lg font-semibold text-white">Momentum Backtester</h1>
          <p className="text-xs text-gray-500 mt-0.5">
            Price momentum portfolio — equal-weight, monthly rebalancing, sector-filtered
          </p>
        </div>
        {savedRuns.length > 0 && (
          <div className="flex items-center gap-2">
            <select
              value={loadedRunId ?? ''}
              onChange={(e) => {
                const id = Number(e.target.value);
                if (id) loadBacktest(id);
              }}
              className="bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:border-indigo-500 transition-colors"
            >
              <option value="">Load saved backtest...</option>
              {savedRuns.map((r) => (
                <option key={r.run_id} value={r.run_id}>
                  {r.name} ({new Date(r.created_at).toLocaleDateString()})
                </option>
              ))}
            </select>
            {loadedRunId && (
              <button
                onClick={() => { if (confirm('Delete this saved backtest?')) deleteBacktest(loadedRunId); }}
                className="px-2.5 py-1.5 rounded-lg text-xs text-gray-500 hover:text-rose-400 hover:bg-rose-500/10 transition-colors"
              >
                Delete
              </button>
            )}
          </div>
        )}
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
              <span className="text-gray-400 text-xs">Skip data fetch</span>
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
          <div className="space-y-4">
            {['price', 'volume'].map((group) => {
              const groupSignals = signalDefs.filter((s) => (s.group ?? 'price') === group);
              if (groupSignals.length === 0) return null;
              return (
                <div key={group}>
                  <h3 className="text-gray-400 text-xs font-medium mb-2.5 uppercase tracking-wider">
                    {group === 'price' ? 'Price Momentum' : 'Volume Confirmation'}
                  </h3>
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-x-8 gap-y-2.5">
                    {groupSignals.map((s) => (
                      <div key={s.key} className="flex items-center gap-3">
                        <div className="w-36 shrink-0 flex items-center gap-1.5">
                          <span className="text-gray-300 text-xs font-medium">{s.label}</span>
                          <span className="relative group/tip">
                            <span className="text-gray-600 hover:text-gray-400 cursor-help text-xs">&#9432;</span>
                            <span className="absolute bottom-full left-1/2 -translate-x-1/2 mb-1.5 hidden group-hover/tip:block w-64 px-3 py-2 rounded-lg bg-gray-800 border border-gray-700 text-gray-300 text-xs leading-relaxed shadow-xl z-50 pointer-events-none">
                              {s.description}
                            </span>
                          </span>
                        </div>
                        <input
                          type="range"
                          min={0}
                          max={10}
                          step={1}
                          value={weights[s.key] ?? 0}
                          onChange={(e) => setWeights((prev) => ({ ...prev, [s.key]: Number(e.target.value) }))}
                          className="flex-1 h-1 accent-indigo-500 cursor-pointer"
                        />
                        <span className="text-gray-500 text-xs w-5 text-right font-mono shrink-0">{weights[s.key] ?? 0}</span>
                      </div>
                    ))}
                  </div>
                </div>
              );
            })}
          {/* Category Weights */}
          {categories.length > 1 && (
            <div>
              <h3 className="text-gray-400 text-xs font-medium mb-2.5 uppercase tracking-wider">Category Weights</h3>
              <div className="flex items-center gap-6">
                {categories.map((cat) => (
                  <div key={cat} className="flex items-center gap-2">
                    <span className="text-gray-300 text-xs font-medium w-28">
                      {cat === 'price' ? 'Price Momentum' : cat === 'volume' ? 'Volume Confirmation' : cat}
                    </span>
                    <input
                      type="range"
                      min={0}
                      max={100}
                      step={5}
                      value={categoryWeights[cat] ?? 50}
                      onChange={(e) => setCategoryWeights((prev) => ({ ...prev, [cat]: Number(e.target.value) }))}
                      className="w-32 h-1 accent-indigo-500 cursor-pointer"
                    />
                    <span className="text-gray-500 text-xs w-8 text-right font-mono">{categoryWeights[cat] ?? 50}%</span>
                  </div>
                ))}
              </div>
            </div>
          )}
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
            {/* Benchmark selector */}
            <div className="flex items-center gap-3">
              <label className="text-gray-400 text-sm">Compare against</label>
              <select
                value={selectedBenchmarkId ?? ''}
                onChange={(e) => setSelectedBenchmarkId(e.target.value ? Number(e.target.value) : null)}
                className="bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-white text-sm focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
              >
                <option value="">No benchmark</option>
                {benchmarkOptions.map((b) => (
                  <option key={b.benchmark_id} value={b.benchmark_id}>{b.ticker} — {b.name}</option>
                ))}
              </select>
              {selectedBenchmarkId && (
                <span className="text-gray-500 text-xs">
                  {benchmarkPrices.length > 0
                    ? `${benchmarkPrices.length} daily prices loaded (${benchmarkPrices[0]?.target_date} → ${benchmarkPrices[benchmarkPrices.length - 1]?.target_date})`
                    : 'Loading prices…'}
                </span>
              )}
            </div>

            {/* Summary Stats */}
            <div className="bg-[#151821] rounded-xl border border-gray-800/40 overflow-hidden">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-gray-800/40 text-gray-500 text-xs">
                    <th className="px-4 py-2.5 text-left font-medium"></th>
                    <th className="px-3 py-2.5 text-right font-medium">Total Return</th>
                    <th className="px-3 py-2.5 text-right font-medium">Annualized</th>
                    <th className="px-3 py-2.5 text-right font-medium">Max Drawdown</th>
                    <th className="px-3 py-2.5 text-right font-medium">Sharpe</th>
                    <th className="px-3 py-2.5 text-right font-medium">Turnover</th>
                    <th className="px-3 py-2.5 text-right font-medium">Months</th>
                    <th className="px-4 py-2.5 text-right font-medium">Avg Holdings</th>
                  </tr>
                </thead>
                <tbody>
                  <tr className="border-b border-gray-800/30">
                    <td className="px-4 py-2.5 text-gray-200 font-medium">Strategy</td>
                    <td className={`px-3 py-2.5 text-right font-mono ${result.summary.total_return_pct >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>{fmtPct(result.summary.total_return_pct)}</td>
                    <td className={`px-3 py-2.5 text-right font-mono ${result.summary.annualized_return_pct >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>{fmtPct(result.summary.annualized_return_pct)}</td>
                    <td className="px-3 py-2.5 text-right font-mono text-rose-400">{fmtPct(result.summary.max_drawdown_pct)}</td>
                    <td className="px-3 py-2.5 text-right font-mono text-white">{result.summary.sharpe_ratio != null ? result.summary.sharpe_ratio.toFixed(2) : '—'}</td>
                    <td className="px-3 py-2.5 text-right font-mono text-gray-300">{fmtPct(result.summary.avg_monthly_turnover_pct)}</td>
                    <td className="px-3 py-2.5 text-right font-mono text-gray-300">{result.summary.total_months}</td>
                    <td className="px-4 py-2.5 text-right font-mono text-gray-300">{result.summary.avg_holdings.toFixed(1)}</td>
                  </tr>
                  {benchmarkReturns && (
                    <tr className="border-b border-gray-800/30 bg-white/[0.01]">
                      <td className="px-4 py-2.5 text-amber-400 font-medium">
                        {benchmarkOptions.find((b) => b.benchmark_id === selectedBenchmarkId)?.ticker ?? 'Benchmark'}
                      </td>
                      <td className={`px-3 py-2.5 text-right font-mono ${benchmarkReturns.totalReturn >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>{fmtPct(benchmarkReturns.totalReturn)}</td>
                      <td className={`px-3 py-2.5 text-right font-mono ${benchmarkReturns.annualized >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>{fmtPct(benchmarkReturns.annualized)}</td>
                      <td className="px-3 py-2.5 text-right font-mono text-rose-400">{fmtPct(benchmarkReturns.maxDd)}</td>
                      <td className="px-3 py-2.5 text-right font-mono text-white">{benchmarkReturns.sharpe != null ? benchmarkReturns.sharpe.toFixed(2) : '—'}</td>
                      <td className="px-3 py-2.5 text-right font-mono text-gray-600">—</td>
                      <td className="px-3 py-2.5 text-right font-mono text-gray-300">{result.summary.total_months}</td>
                      <td className="px-4 py-2.5 text-right font-mono text-gray-600">—</td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>

            {/* Equity Curve */}
            <div className="bg-[#151821] rounded-xl border border-gray-800/40 p-5">
              <div className="flex items-center justify-between mb-4">
                <h3 className="text-white text-sm font-medium">Equity Curve ({logScale ? 'Log' : 'Cumulative'} Return %)</h3>
                <label className="flex items-center gap-2 text-xs text-gray-400 cursor-pointer select-none">
                  <input
                    type="checkbox"
                    checked={logScale}
                    onChange={(e) => setLogScale(e.target.checked)}
                    className="accent-indigo-500 w-3.5 h-3.5"
                  />
                  Log scale
                </label>
              </div>
              <ResponsiveContainer width="100%" height={350}>
                <LineChart data={displayChartData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
                  <XAxis
                    dataKey="date"
                    tick={{ fill: '#6b7280', fontSize: 11 }}
                    tickLine={false}
                    interval={Math.max(0, Math.floor(displayChartData.length / 12) - 1)}
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
                      const label = name === 'cumReturn' ? 'Strategy' : name === 'benchmark' ? (benchmarkOptions.find((b) => b.benchmark_id === selectedBenchmarkId)?.ticker ?? 'Benchmark') : String(name);
                      return [`${v >= 0 ? '+' : ''}${v.toFixed(2)}%`, label];
                    }}
                  />
                  {benchmarkReturns && (
                    <Legend
                      wrapperStyle={{ fontSize: 12, color: '#9ca3af' }}
                    />
                  )}
                  <Line
                    type="monotone"
                    dataKey="cumReturn"
                    stroke="#818cf8"
                    strokeWidth={2}
                    dot={false}
                    name="Strategy"
                  />
                  {benchmarkReturns && (
                    <Line
                      type="monotone"
                      dataKey="benchmark"
                      stroke="#f59e0b"
                      strokeWidth={1.5}
                      strokeDasharray="4 3"
                      dot={false}
                      name={benchmarkOptions.find((b) => b.benchmark_id === selectedBenchmarkId)?.ticker ?? 'Benchmark'}
                      connectNulls
                    />
                  )}
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
                                    {categories.map((cat) => (
                                      <th key={cat} className="text-right py-1 font-medium">
                                        {cat === 'price' ? 'Price' : cat === 'volume' ? 'Vol' : cat}
                                      </th>
                                    ))}
                                    <th className="text-right py-1 font-medium">Total</th>
                                    <th className="text-right py-1 font-medium">Return</th>
                                  </tr>
                                </thead>
                                <tbody>
                                  {[...r.holdings]
                                    .sort((a, b) => {
                                      const sec = a.sector.localeCompare(b.sector);
                                      return sec !== 0 ? sec : b.score - a.score;
                                    })
                                    .map((h) => (
                                      <tr key={h.company_id} className="border-t border-gray-800/20">
                                        <td className="py-1.5 text-indigo-400 font-mono">{h.ticker}</td>
                                        <td className="py-1.5 text-gray-300 truncate max-w-[200px]">{h.company_name}</td>
                                        <td className="py-1.5 text-gray-500">{h.sector}</td>
                                        {categories.map((cat) => (
                                          <td key={cat} className="text-right py-1.5 text-gray-400 font-mono">
                                            {h.category_scores?.[cat] != null ? h.category_scores[cat]!.toFixed(0) : '—'}
                                          </td>
                                        ))}
                                        <td className="text-right py-1.5 text-white font-mono font-medium">{h.score.toFixed(1)}</td>
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

            {/* Save */}
            {!loadedRunId && (
              <div className="bg-[#151821] rounded-xl border border-gray-800/40 p-4 flex items-center gap-3">
                <input
                  value={saveName}
                  onChange={(e) => setSaveName(e.target.value)}
                  placeholder="Backtest name..."
                  className="bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-sm text-white flex-1 max-w-xs focus:outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 placeholder-gray-600 transition-colors"
                  onKeyDown={(e) => { if (e.key === 'Enter') saveBacktest(); }}
                />
                <button
                  onClick={saveBacktest}
                  disabled={saving || !saveName.trim()}
                  className="px-4 py-2 rounded-lg text-sm font-medium bg-indigo-600 hover:bg-indigo-500 text-white transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  {saving ? 'Saving...' : 'Save Backtest'}
                </button>
              </div>
            )}
            {loadedRunId && (
              <div className="bg-indigo-500/10 border border-indigo-500/20 rounded-lg px-4 py-3 text-indigo-400 text-sm flex items-center gap-2">
                <span>Loaded from saved run</span>
                <span className="text-indigo-300 font-medium">
                  {savedRuns.find((r) => r.run_id === loadedRunId)?.name}
                </span>
              </div>
            )}

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
