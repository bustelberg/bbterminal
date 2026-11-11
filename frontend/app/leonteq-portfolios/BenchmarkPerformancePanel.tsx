'use client';

import { useEffect, useState } from 'react';
import { Bar, BarChart, CartesianGrid, Legend, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import { apiFetch } from '../../lib/apiFetch';
import { API_URL } from '../../lib/apiUrl';
import { chartTheme } from '../../lib/chartTheme';
import { ExpectedJobFailure, startLocalJob } from '../../lib/stores/jobs';
import { refreshRequest } from './refreshProgress';
import { monthlyFailure, queueMonthly } from './MonthlyReturns';
import type { PortfolioConnection } from './ConnectionHoldingsView';

type Month = { month: string; leonteq_pct: number | null; benchmark_pct: number | null; difference_pp: number | null; completed: boolean; mtd?: boolean; benchmark_price_date: string | null };
type Performance = { portfolio: string; benchmark: string; year: number; years: number[]; months: Month[]; retrieved_at: string | null; benchmark_fetched_at?: string | null; saved_returns: boolean; benchmark_available: boolean; benchmark_ticker: string | null };
const benchmarks = { ACWI: 'ACWI', SP500: 'S&P 500', BONDS: 'Global bonds', ALTERNATIVES: 'EUR high yield' };
const control = 'rounded-lg border border-neutral-800/40 bg-elevated px-3 py-2 text-sm disabled:opacity-50';
const pct = (value: number | null, suffix = '%') => value == null ? '—' : `${value > 0 ? '+' : ''}${value.toFixed(2)}${suffix}`;
const fetched = (value?: string | null) => value && Number.isFinite(Date.parse(value)) ? new Date(value).toLocaleString('en-GB', { day: 'numeric', month: 'short', year: 'numeric', hour: '2-digit', minute: '2-digit', timeZone: 'Europe/Amsterdam' }).toLowerCase() : 'Unknown';

export default function BenchmarkPerformancePanel() {
  const [links, setLinks] = useState<PortfolioConnection[]>([]);
  const [name, setName] = useState('');
  const [benchmark, setBenchmark] = useState<keyof typeof benchmarks>('ACWI');
  const [year, setYear] = useState(new Date().getFullYear());
  const [data, setData] = useState<Performance | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState('');
  const [revision, setRevision] = useState(0);
  useEffect(() => {
    let active = true;
    void apiFetch(`${API_URL}/api/leonteq/portfolio-links?kind=dynamic`, { cache: 'no-store', noReadCache: true })
      .then(async response => {
        if (!response.ok) throw new Error('Could not load portfolio connections.');
        const result = await response.json();
        if (active) { setLinks(result.links); setName(result.links[0]?.leonteq_name ?? ''); setLoading(false); }
      }).catch(cause => { if (active) { setError(cause.message); setLoading(false); } });
    return () => { active = false; };
  }, []);
  useEffect(() => {
    if (!name) return;
    let active = true;
    setLoading(true); setData(null); setError('');
    const query = new URLSearchParams({ name, benchmark, year: String(year) });
    void apiFetch(`${API_URL}/api/leonteq/portfolios/benchmark-performance?${query}`, { cache: 'no-store', noReadCache: true })
      .then(async response => {
        const result = await response.json();
        if (!response.ok) throw new Error(typeof result.detail === 'string' ? result.detail : 'Could not load monthly performance.');
        if (active) setData(result);
      }).catch(cause => { if (active) setError(cause.message); })
      .finally(() => { if (active) setLoading(false); });
    return () => { active = false; };
  }, [name, benchmark, year, revision]);

  function refresh() {
    const link = links.find(link => link.leonteq_name === name);
    if (!link || refreshing) return;
    setRefreshing(true); setError('');
    startLocalJob(`Monthly performance · ${name} vs ${benchmarks[benchmark]}`, 'leonteq.performance', async (_signal, report) => {
    try {
      report({ done: 0, total: 2, message: `Checking the latest ${benchmarks[benchmark]} prices…` });
      const prices = await apiFetch(`${API_URL}/api/benchmarks/proxy/${benchmark}/refresh`, { method: 'POST' });
      const priceResult = await prices.json();
      if (!prices.ok) throw new Error(typeof priceResult.detail === 'string' ? priceResult.detail : 'Could not refresh the benchmark. Please retry.');
      report({ done: 1, total: 2, message: `Updating ${name} returns from Leonteq…` });
      const response = await queueMonthly(() => refreshRequest(`${API_URL}/api/leonteq/portfolios/monthly-performance`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name, airs_portfolio_id: link.airs_portfolio_id, refresh: true }),
      }, `Monthly performance · ${name}`));
      const result = await response.json();
      if (!response.ok || !result.success || result.refresh_error) throw new Error(monthlyFailure(result, response.ok ? undefined : response.status));
      setRevision(value => value + 1);
      return 'Leonteq returns and benchmark prices updated.';
    } catch (cause) {
      const message = cause instanceof Error ? cause.message : 'Refresh failed. Please retry.';
      setError(message); throw new ExpectedJobFailure(message);
    }
    finally { setRefreshing(false); }
    }, { cancellable: false });
  }

  const years = [...new Set([new Date().getFullYear(), year, ...(data?.years ?? [])])].sort((a, b) => b - a);
  const chart = data?.months.map(row => ({ ...row, label: new Date(`${row.month}-01T12:00:00Z`).toLocaleDateString('en-GB', { month: 'long', timeZone: 'UTC' }) })) ?? [];
  const hasValues = chart.some(row => row.leonteq_pct != null || row.benchmark_pct != null);
  return <section aria-label="Monthly performance" aria-busy={loading || refreshing} className="space-y-5">
    <div><h2 className="text-lg font-semibold">Monthly performance</h2><p className="mt-1 text-sm text-fg-muted">Leonteq and benchmark returns, month by month.</p></div>
    <div className="flex flex-wrap items-end gap-4 rounded-xl border border-neutral-800/40 bg-elevated/40 p-4">
      <label className="grid flex-1 gap-2 text-sm">Portfolio<select className={control} value={name} disabled={refreshing || !links.length} onChange={event => setName(event.target.value)}>
        {!links.length && <option value="">No connected portfolios</option>}{links.map(link => <option key={link.leonteq_name} value={link.leonteq_name}>{link.leonteq_name}</option>)}
      </select></label>
      <label className="grid gap-2 text-sm">Benchmark<select className={control} value={benchmark} disabled={refreshing} onChange={event => setBenchmark(event.target.value as keyof typeof benchmarks)}>{Object.entries(benchmarks).map(([key, label]) => <option key={key} value={key}>{label}</option>)}</select></label>
      <label className="grid gap-2 text-sm">Year<select className={control} value={year} disabled={refreshing} onChange={event => setYear(Number(event.target.value))}>{years.map(value => <option key={value} value={value}>{value}</option>)}</select></label>
      <button className={control} disabled={loading || refreshing || !name} onClick={() => void refresh()}>{refreshing ? 'Refreshing…' : 'Refresh returns'}</button>
    </div>
    {error && <p role="alert" className="text-sm text-neg-300">{error}</p>}
    {loading && <p role="status" className="text-sm text-fg-muted">Loading monthly performance…</p>}
    {!loading && !links.length && !error && <p className="text-sm text-fg-muted">Connect a Leonteq portfolio to AIRS Dynamic in Connections first.</p>}
    {data && <div className="rounded-xl border border-neutral-800/40 bg-elevated/20 p-4 md:p-6">
      <div className="mb-6 flex flex-wrap items-baseline justify-between gap-2"><h3 className="font-semibold">{name} <span className="font-normal text-fg-muted">vs {benchmarks[benchmark]} · {year}</span></h3><span className="text-xs text-fg-muted">Monthly return · %{chart.some(row => row.mtd) ? ' · Current month MTD' : ''}</span></div>
      {hasValues ? <div className="overflow-x-auto"><div className="h-80 min-w-[960px]" role="img" aria-label={`Monthly returns for ${name} versus ${benchmarks[benchmark]} in ${year}; exact values in the table below`}>
        <ResponsiveContainer width="100%" height="100%" minWidth={0} minHeight={320} initialDimension={{ width: 700, height: 320 }}>
          <BarChart data={chart} barGap={1} barSize={18} margin={{ top: 12, right: 8, bottom: 8, left: 0 }} accessibilityLayer>
            <CartesianGrid vertical={false} stroke={chartTheme.grid} strokeDasharray="3 3" />
            <XAxis dataKey="label" interval={0} tickMargin={12} height={45} tick={{ fill: chartTheme.axisTick, fontSize: 12, fontWeight: 500 }} axisLine={{ stroke: chartTheme.grid }} tickLine={false} />
            <YAxis tickFormatter={(value: number) => `${value}%`} tick={{ fill: chartTheme.axisTick, fontSize: 11 }} axisLine={false} tickLine={false} />
            <ReferenceLine y={0} stroke={chartTheme.zeroLine} />
            <Tooltip formatter={value => pct(Number(value))} contentStyle={chartTheme.tooltip.contentStyle} cursor={{ fill: chartTheme.grid, opacity: 0.2 }} />
            <Legend />
            <Bar dataKey="leonteq_pct" name="Leonteq" fill={chartTheme.accent} radius={[3, 3, 0, 0]} maxBarSize={26} isAnimationActive={false} />
            <Bar dataKey="benchmark_pct" name={benchmarks[benchmark]} fill={chartTheme.compare} radius={[3, 3, 0, 0]} maxBarSize={26} isAnimationActive={false} />
          </BarChart>
        </ResponsiveContainer>
      </div></div> : <p className="py-12 text-center text-sm text-fg-muted">No monthly returns available for {year}.</p>}
      {!data.saved_returns && <p className="mt-3 text-sm text-warn-300">No saved Leonteq returns yet. Use Refresh returns to fetch them.</p>}
      {!data.benchmark_available && <p className="mt-3 text-sm text-warn-300">Saved benchmark prices are unavailable for this year.</p>}
      <p className="mt-4 text-xs leading-relaxed text-fg-muted">{benchmarks[benchmark]} fetched {fetched(data.benchmark_fetched_at)} · Leonteq fetched {fetched(data.retrieved_at)}</p>
      <details className="mt-5 text-sm"><summary className="cursor-pointer text-fg-muted">Monthly values</summary><div className="mt-3 overflow-x-auto"><table className="w-full text-sm tabular-nums">
        <caption className="sr-only">Monthly return values · {name} vs {benchmarks[benchmark]} · {year}</caption>
        <thead><tr>{['Month', 'Leonteq', benchmarks[benchmark], 'Difference (pp)'].map(label => <th key={label} className="px-3 py-2 text-right first:text-left">{label}</th>)}</tr></thead>
        <tbody>{chart.map(row => <tr key={row.month} className="border-t border-neutral-800/30"><th scope="row" className="px-3 py-2 text-left font-normal">{row.label}{row.mtd ? ' (MTD)' : ''}</th><td className="px-3 py-2 text-right">{pct(row.leonteq_pct)}</td><td className="px-3 py-2 text-right">{pct(row.benchmark_pct)}</td><td className="px-3 py-2 text-right">{pct(row.difference_pp, 'pp')}</td></tr>)}</tbody>
      </table></div></details>
    </div>}
  </section>;
}
