'use client';

import { useEffect, useRef, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { useLang } from '../../../lib/i18n';
import type { components } from '../../../lib/api-types';

type Portfolio = components['schemas']['ValuationPortfolio'];
type Bundle = components['schemas']['ValuationBundle'];
const value = (number: number | null | undefined, suffix = '') => number == null || !Number.isFinite(number) ? '—' : `${number.toFixed(2)}${suffix}`;
const selectionKey = (portfolio: Portfolio) => portfolio.account ?? String(portfolio.id);

export default function ValuationPanel() {
  const [lang] = useLang();
  const nl = lang === 'nl';
  const [portfolios, setPortfolios] = useState<Portfolio[]>([]);
  const [selected, setSelected] = useState('');
  const [bundle, setBundle] = useState<{ id: string; data: Bundle } | null>(null);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const [retry, setRetry] = useState(0);
  const [sortDiscount, setSortDiscount] = useState(false);
  const refreshFor = useRef<string | null>(null);
  const selectedId = portfolios.find(portfolio => selectionKey(portfolio) === selected)?.id;
  useEffect(() => {
    let active = true;
    void apiFetch(`${API_URL}/api/airs/valuation/portfolios`, { cache: 'no-store', noReadCache: true })
      .then(async response => {
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        return await response.json() as Portfolio[];
      }).then(rows => { if (active) setPortfolios(rows); })
      .catch(cause => { if (active) setError(String(cause)); });
    return () => { active = false; };
  }, [retry]);
  useEffect(() => {
    if (!selected || selectedId == null) return;
    let active = true;
    const force = refreshFor.current === selected;
    if (force) refreshFor.current = null;
    void apiFetch(`${API_URL}/api/airs/valuation/portfolios/${selectedId}`, { cache: 'no-store', noReadCache: true })
      .then(async response => {
        const body = await response.json();
        if (!response.ok) throw new Error(typeof body.detail === 'string' ? body.detail : `HTTP ${response.status}`);
        return body as Bundle;
      }).then(async data => {
        if (!active) return;
        setBundle({ id: selected, data });
        if (!data.ready || data.stale || force) {
          const response = await apiFetch(`${API_URL}/api/airs/valuation/portfolios/${selectedId}/refresh${force ? '?force=true' : ''}`, { method: 'POST' });
          const prepared = await response.json();
          if (!response.ok) throw new Error(typeof prepared.detail === 'string' ? prepared.detail : `HTTP ${response.status}`);
          if (active) setBundle({ id: selected, data: prepared as Bundle });
        }
      })
      .catch(cause => { if (active) setError(cause instanceof Error ? cause.message : String(cause)); })
      .finally(() => { if (active) setLoading(false); });
    return () => { active = false; };
  }, [selected, selectedId, retry]);
  const data = bundle?.id === selected ? bundle.data : null;
  const rows = [...(data?.rows ?? [])].sort((a, b) => sortDiscount
    ? (b.discount_pct ?? -Infinity) - (a.discount_pct ?? -Infinity) || a.name.localeCompare(b.name)
    : a.name.localeCompare(b.name));
  return <section className="space-y-4" aria-label={nl ? 'Waardering' : 'Valuation'}>
    <div className="flex flex-wrap items-end gap-3">
      <label className="space-y-1 text-sm"><span className="block">{nl ? 'Portefeuille' : 'Portfolio'}</span>
        <select value={selected} onChange={event => { setSelected(event.target.value); setLoading(!!event.target.value); setError(''); }} className="rounded-lg border border-neutral-800/40 bg-card px-3 py-2">
          <option value="">{nl ? 'Selecteer een portefeuille…' : 'Select a portfolio…'}</option>
          {portfolios.map(portfolio => <option key={selectionKey(portfolio)} value={selectionKey(portfolio)}
            disabled={portfolio.id == null} title={portfolio.unavailable_reason ?? undefined}>
            {portfolio.name}{portfolio.id == null ? (nl ? ' — geen gekoppeld AIRS-model' : ' — no linked AIRS model') : ''}
          </option>)}
        </select>
      </label>
      <button type="button" onClick={() => { refreshFor.current = selected; setLoading(!!selected); setError(''); setRetry(current => current + 1); }} disabled={loading} className="rounded-lg border border-neutral-800/40 px-3 py-2 text-sm disabled:opacity-50">{loading ? (nl ? 'Berekenen…' : 'Calculating…') : (nl ? 'Vernieuwen' : 'Refresh')}</button>
    </div>
    {(selected || error) && <>
      <div className="overflow-x-auto rounded-xl border border-neutral-800/40"><table className="w-full text-sm">
        <caption className="sr-only">{nl ? 'Historische waardering per bedrijf' : 'Historical valuation by company'}</caption>
        <thead className="bg-elevated text-xs text-fg-muted"><tr>
          <th className="px-3 py-2 text-left">{nl ? 'Bedrijf' : 'Company'}</th>
          <th className="px-3 py-2 text-right">{nl ? 'Koers' : 'Price'}</th>
          <th className="px-3 py-2 text-right">{nl ? 'Actuele K/W' : 'Current P/E'}</th>
          <th className="px-3 py-2 text-right" title={nl ? 'Mediane positieve K/W over vijf jaar' : 'Five-year median positive P/E'}>{nl ? 'Historische K/W' : 'Historical P/E'}</th>
          <th className="px-3 py-2 text-right" title={String(data?.method?.description ?? '')}>{nl ? 'Waardeschatting' : 'Value estimate'}</th>
          <th className="px-3 py-2 text-right"><button type="button" onClick={() => setSortDiscount(current => !current)} aria-pressed={sortDiscount}>{nl ? 'Korting' : 'Discount'} {sortDiscount ? '↓' : '↕'}</button></th>
        </tr></thead>
        <tbody className="divide-y divide-neutral-800/30">
          {error && <tr><td colSpan={6} role="alert" className="whitespace-normal px-3 py-2 text-neg-300 [overflow-wrap:anywhere]">{error}</td></tr>}
          {loading && <tr><td colSpan={6} role="status" className="px-3 py-4 text-center text-fg-muted">{nl ? 'Laden…' : 'Loading…'}</td></tr>}
          {rows.map((row, index) => <tr key={row.isin ?? `${row.name}-${index}`}>
          <td className="px-3 py-2 font-medium" title={row.isin ?? undefined}>{row.name}</td>
          <td className="whitespace-nowrap px-3 py-2 text-right tabular-nums" title={row.price_date ?? undefined}>{value(row.price)} {row.price != null && row.currency}</td>
          <td className="px-3 py-2 text-right tabular-nums">{value(row.current_pe, '×')}</td>
          <td className="px-3 py-2 text-right tabular-nums">{value(row.median_pe, '×')}</td>
          <td className="whitespace-nowrap px-3 py-2 text-right tabular-nums" title={row.reason ?? (data?.stale ? (nl ? 'Dagelijkse update loopt achter' : 'Daily update overdue') : `${row.observations} observations · ${row.history_from ?? ''} → ${row.history_to ?? ''}`)}>
            {row.estimated_value != null ? <>{value(row.estimated_value)} {row.currency}</>
              : <span className="text-xs text-fg-muted">{row.status === 'pending' ? (nl ? 'Nog niet berekend' : 'Pending') : (nl ? 'Onvoldoende gegevens' : 'Insufficient data')}</span>}
          </td>
          <td className="px-3 py-2 text-right tabular-nums">{value(row.discount_pct, '%')}</td>
        </tr>)}
          {!loading && !error && !rows.length && <tr><td colSpan={6} className="px-3 py-6 text-center text-fg-muted">{data?.detail ?? (nl ? 'Geen bedrijven in deze portefeuille.' : 'No companies in this portfolio.')}</td></tr>}
        </tbody>
      </table></div>
    </>}
  </section>;
}
