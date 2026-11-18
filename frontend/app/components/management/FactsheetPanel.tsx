'use client';

import { useEffect, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { useLang } from '../../../lib/i18n';
import type { components } from '../../../lib/api-types';

type Factsheet = components['schemas']['FactsheetResponse'];
const pct = (number: number | null | undefined) => number == null ? '—' : `${number.toFixed(2)}%`;
const hfriSource = 'https://mondovisione.com/_assets/files/HFR-Media-Reference-Guide-2024.09.09.pdf#page=31';
const hfriFxSource = 'https://data.ecb.europa.eu/data/datasets/EXR/EXR.M.USD.EUR.SP00.E';
// Each of the guide's 180 USD return factors is multiplied by opening FX / closing FX (USD per EUR).
const hfri = { totalReturn: 175.43797018699414, cagr: 6.987963437853262, volatility: 7.995606801522687 };

export default function FactsheetPanel() {
  const [lang] = useLang();
  const nl = lang === 'nl';
  const [data, setData] = useState<Factsheet | null>(null);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(true);
  const [revision, setRevision] = useState(0);
  const [showOptionalBenchmarks, setShowOptionalBenchmarks] = useState(false);
  useEffect(() => {
    let active = true;
    async function load() {
      try {
        let response = await apiFetch(`${API_URL}/api/benchmarks/factsheet`, { cache: 'no-store', noReadCache: true });
        let body = await response.json();
        if (!response.ok) throw new Error(typeof body.detail === 'string' ? body.detail : `HTTP ${response.status}`);
        if (!active) return;
        setData(body as Factsheet);
        if (!body.ready || body.stale || revision > 0) {
          response = await apiFetch(`${API_URL}/api/benchmarks/factsheet/refresh${revision > 0 ? '?force=true' : ''}`, { method: 'POST' });
          body = await response.json();
          if (!response.ok) throw new Error(typeof body.detail === 'string' ? body.detail : `HTTP ${response.status}`);
          if (active) setData(body as Factsheet);
        }
      } catch (cause) {
        if (active) setError(cause instanceof Error ? cause.message : String(cause));
      } finally {
        if (active) setLoading(false);
      }
    }
    void load();
    return () => { active = false; };
  }, [revision]);
  const entries = (data?.rows ?? []).filter(row => showOptionalBenchmarks || row.ticker !== 'QAI').flatMap(row => [
    { row, stats: row.period, period: `${data?.period_years ?? 15} ${nl ? 'jaar' : 'years'}`, missing: row.reason },
    ...(row.available_history ? [{ row, stats: row.available_history, period: nl ? 'Beschikbare historie' : 'Available history', missing: null }] : []),
  ]);
  return <section className="space-y-4" aria-label="Factsheet">
    <div className="flex flex-wrap items-center justify-between gap-3">
      <p className="text-sm text-fg-muted">{nl ? 'Totaalrendementen in EUR met USD/EUR-valutabewegingen en herbelegde ETF-uitkeringen. ETF-volatiliteit op dagbasis (√252), HFRI op maandbasis (√12).' : 'Total returns in EUR include USD/EUR currency movements and reinvested ETF distributions. Volatility uses daily ETF returns (√252) and monthly HFRI returns (√12).'}</p>
      <div className="flex flex-wrap items-center gap-3">
        <label className="flex cursor-pointer items-center gap-2 text-sm text-fg-muted">
          <input type="checkbox" checked={showOptionalBenchmarks} onChange={event => setShowOptionalBenchmarks(event.target.checked)} />
          {nl ? 'Toon QAI en HFRIFWI' : 'Show QAI and HFRIFWI'}
        </label>
        <button type="button" disabled={loading} onClick={() => { setError(''); setLoading(true); setRevision(current => current + 1); }} className="rounded-lg border border-neutral-800/40 px-3 py-2 text-sm disabled:opacity-50">{loading ? (nl ? 'Berekenen…' : 'Calculating…') : (nl ? 'Vernieuwen' : 'Refresh')}</button>
      </div>
    </div>
    <div className="overflow-x-auto rounded-xl border border-neutral-800/40"><table className="w-full text-sm">
      <caption className="sr-only">Benchmark Factsheet</caption>
      <thead className="bg-elevated text-xs text-fg-muted"><tr>
        <th className="px-3 py-2 text-left">Benchmark</th>
        <th className="px-3 py-2 text-left">{nl ? 'Periode' : 'Period'}</th>
        <th className="px-3 py-2 text-right">{nl ? 'Totaalrendement' : 'Total return'}</th>
        <th className="px-3 py-2 text-right">CAGR</th>
        <th className="px-3 py-2 text-right">{nl ? 'Volatiliteit p.j.' : 'Annualized volatility'}</th>
      </tr></thead>
      <tbody className="divide-y divide-neutral-800/30">
        {error && <tr><td colSpan={5} role="alert" className="px-3 py-3 text-neg-300 [overflow-wrap:anywhere]">{error}</td></tr>}
        {loading && <tr><td colSpan={5} role="status" className="px-3 py-3 text-fg-muted">{nl ? 'Rendement en risico berekenen…' : 'Calculating returns and risk…'}</td></tr>}
        {entries.map(({ row, stats, period, missing }) => <tr key={`${row.ticker}-${period}`}>
          <td className="px-3 py-3"><a href={row.source_url ?? `https://finance.yahoo.com/quote/${encodeURIComponent(row.ticker)}/`} target="_blank" rel="noopener noreferrer"
            className="font-medium text-accent-600 hover:underline" title={`${row.ticker} · ${row.source_url ? 'justETF' : 'Yahoo Finance'}`}>{row.ticker}</a>
            <div className="text-xs text-fg-muted">{row.name}</div>
            {row.isin && <div className="text-xs text-fg-muted">{row.isin}</div>}
            <a href={`https://finance.yahoo.com/quote/${encodeURIComponent(row.ticker)}/history/`} target="_blank" rel="noopener noreferrer"
              className="text-xs text-accent-600 hover:underline">{nl ? 'Historische koersen' : 'Historical data'}</a>
          </td>
          <td className="max-w-sm px-3 py-3"><div>{period}</div><div className="text-xs text-fg-muted">{stats ? `${stats.start} → ${stats.end}` : `${row.requested_start} → ${row.end}`}</div>
            {missing && <div className="mt-1 text-xs text-fg-muted">{missing}</div>}</td>
          <td className="px-3 py-3 text-right tabular-nums">{pct(stats?.total_return_pct)}</td>
          <td className="px-3 py-3 text-right tabular-nums" title={stats ? `${stats.observations} daily returns · ${stats.years.toFixed(2)} years` : undefined}>{pct(stats?.cagr_pct)}</td>
          <td className="px-3 py-3 text-right tabular-nums">{pct(stats?.volatility_pct)}</td>
        </tr>)}
        {showOptionalBenchmarks && <tr>
          <td className="px-3 py-3"><a href={hfriSource} target="_blank" rel="noopener noreferrer"
            className="font-medium text-accent-600 hover:underline">HFRIFWI</a>
            <div className="text-xs text-fg-muted">HFRI Fund Weighted Composite Index</div>
            <div className="text-xs text-fg-muted">HFR Media Reference Guide, p. 31</div>
            <a href={hfriFxSource} target="_blank" rel="noopener noreferrer" className="text-xs text-accent-600 hover:underline">
              {nl ? 'ECB-wisselkoersen per maandultimo' : 'ECB month-end exchange rates'}
            </a>
          </td>
          <td className="px-3 py-3"><div>{nl ? '15 jaar · EUR, niet afgedekt' : '15 years · EUR, unhedged'}</div>
            <div className="text-xs text-fg-muted">2009-08-31 → 2024-08-31</div>
          </td>
          <td className="px-3 py-3 text-right tabular-nums">{pct(hfri.totalReturn)}</td>
          <td className="px-3 py-3 text-right tabular-nums" title={nl ? '180 maandrendementen' : '180 monthly returns'}>{pct(hfri.cagr)}</td>
          <td className="px-3 py-3 text-right tabular-nums">{pct(hfri.volatility)}</td>
        </tr>}
      </tbody>
    </table></div>
    {data?.prepared_at && <p className="text-xs text-fg-muted" title={data.method ?? undefined}>{nl ? 'Bijgewerkt' : 'Updated'}: {data.prepared_at}</p>}
  </section>;
}
