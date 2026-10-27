'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';
import {
  CartesianGrid, Line, LineChart, ReferenceDot, ReferenceLine,
  ResponsiveContainer, Tooltip, XAxis, YAxis,
} from 'recharts';
import { apiFetch } from '../../lib/apiFetch';
import { API_URL } from '../../lib/apiUrl';
import { chartTheme } from '../../lib/chartTheme';
import type { components } from '../../lib/api-types';
import { useLang } from '../../lib/i18n';

type Snapshot = components['schemas']['MarketSnapshot'];
type Range = '1y' | '5y' | 'all';

export default function MarketPage() {
  const [lang] = useLang();
  return <main className="mx-auto max-w-[100rem] space-y-8 p-4 sm:p-6 lg:p-8">
    <h1 className="text-2xl font-semibold text-fg-strong">{lang === 'nl' ? 'Markt' : 'Market'}</h1>
    <MarketSection endpoint="nasdaq" name="Nasdaq Composite" symbol="^IXIC" />
    <MarketSection endpoint="acwi" name="iShares MSCI ACWI ETF" symbol="ACWI" />
  </main>;
}

function MarketSection({ endpoint, name, symbol }: { endpoint: 'nasdaq' | 'acwi'; name: string; symbol: string }) {
  const [lang] = useLang();
  const nl = lang === 'nl';
  const [data, setData] = useState<Snapshot | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [range, setRange] = useState<Range>('1y');
  const load = useCallback(async (signal?: AbortSignal) => {
    setLoading(true);
    setError(null);
    try {
      const response = await apiFetch(`${API_URL}/api/market/${endpoint}`, { signal });
      if (!response.ok) throw new Error('Yahoo Finance data is temporarily unavailable.');
      const snapshot: Snapshot = await response.json();
      if (!signal?.aborted) setData(snapshot);
    } catch {
      if (!signal?.aborted) setError('Yahoo Finance');
    } finally {
      if (!signal?.aborted) setLoading(false);
    }
  }, [endpoint]);
  useEffect(() => {
    const controller = new AbortController();
    void load(controller.signal);
    return () => controller.abort();
  }, [load]);
  const points = useMemo(() => {
    if (!data) return [];
    const end = new Date(data.latest_at);
    const cutoff = new Date(end);
    cutoff.setUTCFullYear(end.getUTCFullYear() - (range === '1y' ? 1 : 5));
    const start = cutoff.toISOString().slice(0, 10);
    const selected = data.points.filter(point => range === 'all' || point.date >= start);
    // Keep the full daily series in the response; bound SVG work for the decades-long view.
    const stride = Math.max(1, Math.ceil(selected.length / 1200));
    return selected.filter((point, index) => index % stride === 0
      || index === selected.length - 1 || point.date === data.peak_date);
  }, [data, range]);
  const locale = nl ? 'nl-NL' : 'en-GB';
  const number = (value: number) => value.toLocaleString(locale, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  const day = (value: string) => new Date(`${value}T12:00:00Z`).toLocaleDateString(locale, { day: 'numeric', month: 'short', year: 'numeric' });
  const peakVisible = points.some(point => point.date === data?.peak_date);
  const button = 'cursor-pointer rounded-lg border border-neutral-800/40 px-3 py-2 text-xs font-medium hover:bg-overlay/5 disabled:cursor-wait disabled:opacity-50';

  return <section className="space-y-5" aria-label={name}>
    <header className="flex flex-wrap items-start justify-between gap-3">
      <div>
        <h2 className="text-xl font-semibold text-fg-strong">{name}</h2>
        <p className="mt-1 text-sm text-fg-muted">{symbol} · {endpoint === 'nasdaq'
          ? (nl ? 'Indexpunten, USD-basis' : 'Index points, USD basis')
          : (nl ? 'ETF-koers in USD · exclusief uitkeringen' : 'ETF price in USD · excluding distributions')}</p>
      </div>
      <button type="button" className={button} disabled={loading} onClick={() => void load()}>
        {loading ? (nl ? 'Laden…' : 'Loading…') : (nl ? 'Vernieuwen' : 'Refresh')}
      </button>
    </header>
    {error && <p role="alert" className="rounded-lg border border-neg-500/30 p-3 text-sm text-neg-500">
      {nl ? 'Yahoo Finance is tijdelijk niet beschikbaar. Probeer opnieuw.' : 'Yahoo Finance is temporarily unavailable. Please retry.'}
      {data && (nl ? ' Hieronder staat de vorige momentopname.' : ' The previous snapshot remains below.')}
    </p>}
    {!data && loading && <p role="status" className="py-16 text-center text-sm text-fg-muted">{nl ? `Historie van ${name} laden…` : `Loading ${name} history…`}</p>}
    {data && <>
      <div className="grid gap-3 sm:grid-cols-3">
        <section className="rounded-xl border border-neutral-800/40 bg-card p-4">
          <h2 className="text-xs text-fg-muted">{endpoint === 'nasdaq'
            ? (nl ? 'Laatste indexstand' : 'Latest index level') : (nl ? 'Laatste ETF-koers · USD' : 'Latest ETF price · USD')}</h2>
          <p className="mt-2 font-mono text-2xl text-fg-strong">{number(data.latest)}</p>
          <p className="mt-2 text-xs text-fg-faint">{new Date(data.latest_at).toLocaleString(locale, { timeZone: 'Europe/Amsterdam', dateStyle: 'medium', timeStyle: 'short' })} · Amsterdam</p>
        </section>
        <section className="rounded-xl border border-neutral-800/40 bg-card p-4">
          <h2 className="text-xs text-fg-muted">{nl ? 'Hoogste geregistreerde stand' : 'Highest recorded level'}</h2>
          <p className="mt-2 font-mono text-2xl text-fg-strong">{number(data.peak)}</p>
          <p className="mt-2 text-xs text-fg-muted">{day(data.peak_date)} · {nl ? 'Daghoogte' : 'Daily high'}</p>
        </section>
        <section className="rounded-xl border border-neutral-800/40 bg-card p-4">
          <h2 className="text-xs text-fg-muted">{nl ? 'Onder de top' : 'Below the peak'}</h2>
          <p className={`mt-2 font-mono text-2xl ${data.from_peak_pct < 0 ? 'text-neg-500' : 'text-fg-strong'}`}>{number(data.from_peak_pct)}%</p>
          <p className="mt-2 text-xs text-fg-faint">{nl ? 'Laatste stand ÷ top − 1' : 'Latest level ÷ peak − 1'}</p>
        </section>
      </div>
      <section className="min-w-0 rounded-xl border border-neutral-800/40 bg-card p-3 sm:p-5">
        <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
          <h2 className="text-sm font-semibold text-fg-strong">{name}</h2>
          <div role="group" aria-label={nl ? 'Grafiekperiode' : 'Chart period'} className="flex gap-1">
            {(['1y', '5y', 'all'] as const).map(value => <button key={value} type="button" aria-pressed={range === value}
              onClick={() => setRange(value)} className={`${button} ${range === value ? 'bg-accent-600 text-white' : 'text-fg-muted'}`}>
              {value === 'all' ? (nl ? 'Alles' : 'All history') : value === '1y' ? (nl ? '1 jaar' : '1 year') : (nl ? '5 jaar' : '5 years')}
            </button>)}
          </div>
        </div>
        <div className="h-[24rem] min-w-0 sm:h-[32rem]" role="img" aria-label={`${data.name}: ${number(data.latest)}; ${nl ? 'top' : 'peak'} ${number(data.peak)}, ${day(data.peak_date)}; ${number(data.from_peak_pct)}%`}>
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={points} margin={{ top: 36, right: 15, bottom: 8, left: 4 }}>
              <CartesianGrid stroke={chartTheme.grid} vertical={false} />
              <XAxis dataKey="date" minTickGap={65} tick={{ fill: chartTheme.axisTick, fontSize: 11 }} tickFormatter={value => day(String(value))} />
              <YAxis width={65} domain={[0, Math.ceil(data.peak * 1.08)]} tick={{ fill: chartTheme.axisTick, fontSize: 11 }} tickFormatter={value => Number(value).toLocaleString(locale)} />
              <Tooltip {...chartTheme.tooltip} labelFormatter={value => day(String(value))} formatter={value => [number(Number(value)), endpoint === 'nasdaq' ? (nl ? 'Indexstand' : 'Index level') : (nl ? 'ETF-koers · USD' : 'ETF price · USD')]} />
              <ReferenceLine y={data.peak} stroke={chartTheme.warn} strokeDasharray="5 4"
                label={{ value: `${nl ? 'Top' : 'Peak'} ${number(data.peak)} · ${day(data.peak_date)}`, position: 'insideTopLeft', fill: chartTheme.warn, fontSize: 11 }} />
              <Line type="linear" dataKey="close" stroke={chartTheme.accent} strokeWidth={1.8} dot={false} isAnimationActive={false} />
              {peakVisible && <ReferenceDot x={data.peak_date} y={data.peak} r={5} fill={chartTheme.warn} stroke="white" strokeWidth={2} />}
            </LineChart>
          </ResponsiveContainer>
        </div>
        <a className="mt-2 inline-block text-xs text-accent-500 hover:underline" href={`https://finance.yahoo.com/quote/${encodeURIComponent(symbol)}/`} target="_blank" rel="noopener noreferrer">Yahoo Finance · {symbol}</a>
      </section>
    </>}
  </section>;
}
