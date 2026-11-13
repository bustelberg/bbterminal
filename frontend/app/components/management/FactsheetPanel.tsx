'use client';

import { useEffect, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { useLang } from '../../../lib/i18n';
import type { components } from '../../../lib/api-types';

type Factsheet = components['schemas']['FactsheetResponse'];
const pct = (number: number | null | undefined) => number == null ? '—' : `${number.toFixed(2)}%`;

export default function FactsheetPanel() {
  const [lang] = useLang();
  const nl = lang === 'nl';
  const [data, setData] = useState<Factsheet | null>(null);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(true);
  const [revision, setRevision] = useState(0);
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
  const entries = (data?.rows ?? []).flatMap(row => [
    { row, stats: row.period, period: `${data?.period_years ?? 15} ${nl ? 'jaar' : 'years'}`, missing: row.reason },
    ...(row.available_history ? [{ row, stats: row.available_history, period: nl ? 'Beschikbare historie' : 'Available history', missing: null }] : []),
  ]);
  return <section className="space-y-4" aria-label="Factsheet">
    <div className="flex flex-wrap items-center justify-between gap-3">
      <p className="text-sm text-fg-muted">{nl ? 'Totaalrendement in EUR, inclusief herbelegde uitkeringen en USD/EUR-valutabewegingen. Dagvolatiliteit geannualiseerd met √252.' : 'EUR total returns, including reinvested distributions and USD/EUR currency movements. Daily volatility annualized using √252.'}</p>
      <button type="button" disabled={loading} onClick={() => { setError(''); setLoading(true); setRevision(current => current + 1); }} className="rounded-lg border border-neutral-800/40 px-3 py-2 text-sm disabled:opacity-50">{loading ? (nl ? 'Berekenen…' : 'Calculating…') : (nl ? 'Vernieuwen' : 'Refresh')}</button>
    </div>
    <div className="overflow-x-auto rounded-xl border border-neutral-800/40"><table className="w-full text-sm">
      <caption className="sr-only">ACWI / AGG Factsheet</caption>
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
          <td className="px-3 py-3"><a href={`https://finance.yahoo.com/quote/${encodeURIComponent(row.ticker)}/`} target="_blank" rel="noopener noreferrer"
            className="font-medium text-accent-600 hover:underline" title={`${row.ticker} · Yahoo Finance`}>{row.ticker}</a>
            <div className="text-xs text-fg-muted">{row.name}</div>
            <a href={`https://finance.yahoo.com/quote/${encodeURIComponent(row.ticker)}/history/`} target="_blank" rel="noopener noreferrer"
              className="text-xs text-accent-600 hover:underline">{nl ? 'Historische koersen' : 'Historical data'}</a>
          </td>
          <td className="max-w-sm px-3 py-3"><div>{period}</div><div className="text-xs text-fg-muted">{stats ? `${stats.start} → ${stats.end}` : `${row.requested_start} → ${row.end}`}</div>
            {missing && <div className="mt-1 text-xs text-fg-muted">{missing}</div>}</td>
          <td className="px-3 py-3 text-right tabular-nums">{pct(stats?.total_return_pct)}</td>
          <td className="px-3 py-3 text-right tabular-nums" title={stats ? `${stats.observations} daily returns · ${stats.years.toFixed(2)} years` : undefined}>{pct(stats?.cagr_pct)}</td>
          <td className="px-3 py-3 text-right tabular-nums">{pct(stats?.volatility_pct)}</td>
        </tr>)}
      </tbody>
    </table></div>
    {data?.prepared_at && <p className="text-xs text-fg-muted" title={data.method ?? undefined}>{nl ? 'Bijgewerkt' : 'Updated'}: {data.prepared_at}</p>}
  </section>;
}
