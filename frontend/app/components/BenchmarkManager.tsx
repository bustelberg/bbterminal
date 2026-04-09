'use client';

import { useState, useEffect } from 'react';

const API_URL = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000';

type Benchmark = {
  benchmark_id: number;
  ticker: string;
  name: string;
  created_at: string;
  price_from: string | null;
  price_to: string | null;
};

export default function BenchmarkManager() {
  const [benchmarks, setBenchmarks] = useState<Benchmark[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Add form
  const [ticker, setTicker] = useState('');
  const [name, setName] = useState('');
  const [adding, setAdding] = useState(false);

  // Refresh state
  const [refreshingId, setRefreshingId] = useState<number | null>(null);

  const load = async () => {
    setLoading(true);
    try {
      const res = await fetch(`${API_URL}/api/benchmarks`);
      setBenchmarks(await res.json());
    } catch {
      setError('Failed to load benchmarks');
    }
    setLoading(false);
  };

  useEffect(() => { load(); }, []);

  const handleAdd = async () => {
    if (!ticker.trim() || !name.trim()) return;
    setAdding(true);
    setError(null);
    try {
      const res = await fetch(`${API_URL}/api/benchmarks`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ticker: ticker.trim(), name: name.trim() }),
      });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.detail ?? `HTTP ${res.status}`);
      }
      const result = await res.json();
      setTicker('');
      setName('');
      await load();
      setError(null);
    } catch (e) {
      setError(`Add failed: ${e instanceof Error ? e.message : e}`);
    }
    setAdding(false);
  };

  const handleRefresh = async (id: number) => {
    setRefreshingId(id);
    setError(null);
    try {
      const res = await fetch(`${API_URL}/api/benchmarks/${id}/refresh`, { method: 'POST' });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.detail ?? `HTTP ${res.status}`);
      }
    } catch (e) {
      setError(`Refresh failed: ${e instanceof Error ? e.message : e}`);
    }
    setRefreshingId(null);
  };

  const handleDelete = async (id: number, ticker: string) => {
    if (!confirm(`Delete benchmark "${ticker}"?`)) return;
    setError(null);
    try {
      const res = await fetch(`${API_URL}/api/benchmarks/${id}`, { method: 'DELETE' });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.detail ?? `HTTP ${res.status}`);
      }
      setBenchmarks((prev) => prev.filter((b) => b.benchmark_id !== id));
    } catch (e) {
      setError(`Delete failed: ${e instanceof Error ? e.message : e}`);
    }
  };

  return (
    <div className="flex flex-col h-full">
      <div className="px-8 py-5 border-b border-gray-800/60">
        <h1 className="text-lg font-semibold text-white">Benchmarks</h1>
        <p className="text-xs text-gray-500 mt-0.5">
          Manage ETF/index benchmarks for backtest comparison. Prices fetched from GuruFocus.
        </p>
      </div>

      <div className="flex-1 overflow-auto px-8 py-5 space-y-5">
        {/* Add Benchmark */}
        <div className="bg-[#151821] rounded-xl border border-gray-800/40 p-5">
          <h3 className="text-gray-400 text-xs font-medium mb-3 uppercase tracking-wider">Add Benchmark</h3>
          <div className="flex items-end gap-3">
            <div>
              <label className="text-gray-500 text-xs block mb-1">Ticker</label>
              <input
                value={ticker}
                onChange={(e) => setTicker(e.target.value)}
                placeholder="e.g. ACWI"
                className="bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-white text-sm font-mono w-32 focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none placeholder-gray-600"
                onKeyDown={(e) => { if (e.key === 'Enter') handleAdd(); }}
              />
            </div>
            <div className="flex-1 max-w-xs">
              <label className="text-gray-500 text-xs block mb-1">Name</label>
              <input
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="e.g. MSCI All Country World"
                className="w-full bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-white text-sm focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none placeholder-gray-600"
                onKeyDown={(e) => { if (e.key === 'Enter') handleAdd(); }}
              />
            </div>
            <button
              onClick={handleAdd}
              disabled={adding || !ticker.trim() || !name.trim()}
              className="px-5 py-2 rounded-lg text-sm font-medium bg-indigo-600 hover:bg-indigo-500 text-white transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {adding ? 'Fetching prices...' : 'Add'}
            </button>
          </div>
          <p className="text-gray-600 text-xs mt-2">
            Enter the GuruFocus ticker for any US-listed ETF (e.g. SPY, ACWI, QQQ, VTI). Prices will be fetched automatically.
          </p>
        </div>

        {error && (
          <div className="bg-rose-500/10 border border-rose-500/20 rounded-lg px-4 py-3 text-rose-400 text-sm flex items-center justify-between">
            <span>{error}</span>
            <button onClick={() => setError(null)} className="text-gray-500 hover:text-white ml-3 text-xs">Dismiss</button>
          </div>
        )}

        {/* Benchmarks Table */}
        <div className="bg-[#151821] rounded-xl border border-gray-800/40 overflow-hidden">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-800/60 text-gray-500">
                <th className="px-5 py-3 text-left text-xs font-medium">Ticker</th>
                <th className="px-3 py-3 text-left text-xs font-medium">Name</th>
                <th className="px-3 py-3 text-left text-xs font-medium">Price Range</th>
                <th className="px-3 py-3 text-right text-xs font-medium w-40">Actions</th>
              </tr>
            </thead>
            <tbody>
              {loading && (
                <tr><td colSpan={4} className="px-5 py-8 text-center text-gray-600">Loading...</td></tr>
              )}
              {!loading && benchmarks.length === 0 && (
                <tr><td colSpan={4} className="px-5 py-8 text-center text-gray-600">No benchmarks yet. Add one above.</td></tr>
              )}
              {benchmarks.map((b) => (
                <tr key={b.benchmark_id} className="border-b border-gray-800/30 hover:bg-white/[0.02] transition-colors group">
                  <td className="px-5 py-2.5 text-indigo-400 font-mono font-medium">{b.ticker}</td>
                  <td className="px-3 py-2.5 text-gray-200">{b.name}</td>
                  <td className="px-3 py-2.5 text-gray-500 text-xs font-mono">
                    {b.price_from && b.price_to
                      ? `${b.price_from} → ${b.price_to}`
                      : <span className="text-gray-600">No data</span>}
                  </td>
                  <td className="px-3 py-2.5 text-right">
                    <div className="flex gap-1.5 justify-end opacity-0 group-hover:opacity-100 transition-opacity">
                      <button
                        onClick={() => handleRefresh(b.benchmark_id)}
                        disabled={refreshingId === b.benchmark_id}
                        className="px-2.5 py-1 rounded-lg text-xs text-gray-400 hover:text-white hover:bg-white/5 transition-colors disabled:opacity-50"
                      >
                        {refreshingId === b.benchmark_id ? 'Refreshing...' : 'Refresh Prices'}
                      </button>
                      <button
                        onClick={() => handleDelete(b.benchmark_id, b.ticker)}
                        className="px-2.5 py-1 rounded-lg text-xs text-gray-600 hover:text-rose-400 hover:bg-rose-500/10 transition-colors"
                      >
                        Delete
                      </button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
