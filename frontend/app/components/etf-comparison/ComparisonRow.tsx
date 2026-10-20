'use client';

import { useEffect, useState } from 'react';
import { CartesianGrid, Legend, Line, LineChart, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { chartTheme } from '../../../lib/chartTheme';
import type { EtfComparisonResult } from '../../../lib/types/api';

const pct = (value: number) => `${value >= 0 ? '+' : ''}${value.toFixed(2)}%`;

export default function ComparisonRow({ etf, benchmark, revision }: {
  etf: { name: string; ticker: string; isin: string }; benchmark: string; revision: number;
}) {
  const [start, setStart] = useState(() => `${new Date().getFullYear()}-01-01`);
  const [answer, setAnswer] = useState<{ key: string; data?: EtfComparisonResult; error?: string } | null>(null);
  const key = `${etf.ticker}/${benchmark}/${start}/${revision}`;
  const current = answer?.key === key ? answer : null;
  const data = current?.data;

  useEffect(() => {
    if (!start || !benchmark) return;
    const controller = new AbortController();
    void (async () => {
      try {
        const params = new URLSearchParams({ ticker: etf.ticker, start, benchmark, valuation: 'eur-v2' });
        const response = await apiFetch(`${API_URL}/api/benchmarks/etf-comparison?${params}`, { signal: controller.signal });
        const body = await response.json();
        if (!response.ok) throw new Error(typeof body.detail === 'string' ? body.detail : `Unable to compare ${etf.ticker} (HTTP ${response.status}).`);
        if (!controller.signal.aborted) setAnswer({ key, data: body as EtfComparisonResult });
      } catch (error) {
        if (!controller.signal.aborted) setAnswer({ key, error: error instanceof Error ? error.message : String(error) });
      }
    })();
    return () => controller.abort();
  }, [benchmark, etf.ticker, key, start]);

  return <section aria-label={etf.name} className="grid grid-cols-1 overflow-hidden rounded-xl border border-neutral-800/40 bg-card lg:grid-cols-[18rem_minmax(0,1fr)]">
    <div className="space-y-4 border-b border-neutral-800/20 p-5 lg:border-b-0 lg:border-r">
      <div>
        <h2 className="text-sm font-semibold leading-relaxed text-fg-strong">{etf.name}</h2>
        <a href={`https://finance.yahoo.com/quote/${encodeURIComponent(etf.ticker)}/`} target="_blank" rel="noreferrer" className="mt-1 inline-block font-mono text-xs text-accent-400 hover:underline">{etf.ticker} ↗</a>
        <p className="mt-1 text-[10px] text-fg-faint">{etf.isin}</p>
      </div>
      <label className="flex flex-col gap-1.5 text-xs font-medium text-fg-soft">Starting date
        <input aria-label={`Starting date for ${etf.name}`} type="date" required value={start} max={new Date().toLocaleDateString('en-CA')}
          onChange={(event) => setStart(event.target.value)} className="w-full rounded-lg border border-neutral-700 bg-page px-3 py-2 text-sm text-fg focus:border-accent-500 focus:outline-none focus:ring-1 focus:ring-accent-500/30" />
      </label>
      {data && <>
        <dl className="space-y-2 text-xs">
          <div className="flex justify-between gap-2"><dt className="text-fg-muted">ETF return · EUR</dt><dd className="font-mono text-fg-strong">{pct(data.total_return_pct)}</dd></div>
          <div className="flex justify-between gap-2"><dt className="text-fg-muted">{data.benchmark} · EUR</dt><dd className="font-mono text-fg-strong">{pct(data.benchmark_return_pct)}</dd></div>
          <div className="flex justify-between gap-2 border-t border-neutral-800/20 pt-2"><dt className="text-fg-muted">Excess return</dt><dd className="font-mono text-fg-strong">{data.excess_pp >= 0 ? '+' : ''}{data.excess_pp.toFixed(2)} pp</dd></div>
        </dl>
        <p className="text-[11px] text-fg-faint">{data.start} → {data.end}<br />Historical FX conversion · EUR</p>
        {data.start !== data.requested_start && <p className="text-[11px] text-fg-faint">Aligned to the first available shared close.</p>}
      </>}
    </div>
    <div className="min-w-0 p-4" aria-live="polite">
      {!data && <div className="grid h-56 place-items-center px-4 text-center text-sm text-fg-muted">
        {current?.error ? <p role="alert" className="text-neg-400">{current.error}</p>
          : !start ? <p>Choose a starting date.</p> : <p role="status">Loading {etf.ticker} vs {benchmark}…</p>}
      </div>}
      {data && <div className="h-56" role="img" aria-label={`${etf.name} ${pct(data.total_return_pct)} versus ${data.benchmark} ${pct(data.benchmark_return_pct)} from ${data.start} to ${data.end}`}>
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={data.points} margin={{ top: 8, right: 12, bottom: 4, left: 0 }}>
            <CartesianGrid stroke={chartTheme.grid} strokeDasharray="3 3" />
            <XAxis dataKey="date" minTickGap={45} tick={{ fill: chartTheme.axisTick, fontSize: 11 }} />
            <YAxis tickFormatter={(value: number) => `${value.toFixed(0)}%`} tick={{ fill: chartTheme.axisTick, fontSize: 11 }} />
            <Tooltip formatter={(value) => pct(Number(value))} contentStyle={chartTheme.tooltip.contentStyle} />
            <Legend /><ReferenceLine y={0} stroke={chartTheme.zeroLine} />
            <Line type="linear" dataKey="etf_pct" name={data.ticker} stroke={chartTheme.accent} strokeWidth={2} dot={false} isAnimationActive={false} />
            <Line type="linear" dataKey="benchmark_pct" name={data.benchmark} stroke={chartTheme.compare} strokeWidth={1.8} strokeDasharray="5 3" dot={false} isAnimationActive={false} />
          </LineChart>
        </ResponsiveContainer>
      </div>}
    </div>
  </section>;
}
