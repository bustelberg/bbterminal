'use client';

import { useEffect, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { useLang } from '../../../lib/i18n';
import type { components } from '../../../lib/api-types';

type AllocationRow = Omit<components['schemas']['FactsheetAllocationRow'], 'drawdowns_pct'> & {
  drawdowns_pct?: number[];
  max_drawdown_pct?: number;
};
type Factsheet = Omit<components['schemas']['FactsheetResponse'], 'allocations'> & {
  allocations?: (Omit<components['schemas']['FactsheetAllocations'], 'rows'> & { rows: AllocationRow[] }) | null;
};
const allocationSortValue = {
  weights: (row: AllocationRow) => row.weights[2],
  cagr: (row: AllocationRow) => row.cagr_pct,
  sharpe: (row: AllocationRow) => row.sharpe,
  volatility: (row: AllocationRow) => row.volatility_pct,
  drawdown: (row: AllocationRow) => row.drawdowns_pct?.[0] ?? row.max_drawdown_pct,
};
type AllocationSortKey = keyof typeof allocationSortValue;
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
  const [equityWeight, setEquityWeight] = useState(60);
  const [allocationSort, setAllocationSort] = useState<{ key: AllocationSortKey; ascending: boolean }>({
    key: 'weights', ascending: true,
  });
  useEffect(() => {
    let active = true;
    async function load() {
      try {
        let response = await apiFetch(`${API_URL}/api/benchmarks/factsheet`, { cache: 'no-store', noReadCache: true });
        let body = await response.json();
        if (!response.ok) throw new Error(typeof body.detail === 'string' ? body.detail : `HTTP ${response.status}`);
        if (!active) return;
        setData(body as Factsheet);
        if (revision > 0) {
          response = await apiFetch(`${API_URL}/api/benchmarks/factsheet/refresh?force=true`, { method: 'POST' });
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
  const allocationRows = (data?.allocations?.rows ?? [])
    .filter(row => row.weights[0] === equityWeight)
    .sort((a, b) => {
      const read = allocationSortValue[allocationSort.key];
      const left = read(a), right = read(b);
      if (left == null) return right == null ? 0 : 1;
      if (right == null) return -1;
      return allocationSort.ascending ? left - right : right - left;
    });
  const allocationHeaders: [AllocationSortKey, string][] = [
    ['weights', 'ACWI / SYBA.DE / HDG'],
    ['cagr', 'CAGR'],
    ['sharpe', 'Sharpe'],
    ['volatility', nl ? 'Volatiliteit p.j.' : 'Annualized volatility'],
    ['drawdown', nl ? '3 grootste drawdowns' : '3 largest drawdowns'],
  ];
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
        <th className="px-3 py-2 text-right" title={nl
          ? 'Gemiddeld verlies in de slechtste 5% van de dagrendementen van deze ETF, in EUR.'
          : 'Average loss in this ETF’s worst 5% of daily returns in EUR.'}>
          {nl ? 'Dagelijkse CVaR (95%)' : 'Daily CVaR (95%)'}
        </th>
      </tr></thead>
      <tbody className="divide-y divide-neutral-800/30">
        {error && <tr><td colSpan={6} role="alert" className="px-3 py-3 text-neg-300 [overflow-wrap:anywhere]">{error}</td></tr>}
        {loading && <tr><td colSpan={6} role="status" className="px-3 py-3 text-fg-muted">{nl ? 'Rendement en risico berekenen…' : 'Calculating returns and risk…'}</td></tr>}
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
          <td className="px-3 py-3 text-right tabular-nums">{pct(stats?.cvar_95_pct)}</td>
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
          <td className="px-3 py-3 text-right" title={nl ? 'Alleen maandrendementen beschikbaar' : 'Only monthly returns are available'}>—</td>
        </tr>}
      </tbody>
    </table></div>
    {data?.prepared_at && <p className="text-xs text-fg-muted" title={data.method ?? undefined}>{nl ? 'Bijgewerkt' : 'Updated'}: {data.prepared_at}</p>}
    <section className="space-y-2" aria-labelledby="factsheet-correlations">
      <h2 id="factsheet-correlations" className="text-sm font-semibold">{nl ? 'Correlaties' : 'Correlations'}</h2>
      {data?.correlations ? <>
        <p className="text-xs text-fg-muted">
          {nl ? 'Pearson-correlatie van rendementen in EUR op gedeelde handelsdagen' : 'Pearson correlation of EUR returns on shared trading dates'}
          {' · '}{data.correlations.start} → {data.correlations.end}
          {' · '}{data.correlations.observations.toLocaleString(lang)} {nl ? 'waarnemingen' : 'observations'}
        </p>
        <div className="rounded-xl border border-neutral-800/40">
          <table className="w-full table-fixed text-sm" aria-labelledby="factsheet-correlations">
            <thead className="bg-elevated text-fg-muted"><tr>
              <th className="px-2 py-2 text-left">ETF</th>
              {data.correlations.tickers.map(ticker => <th key={ticker} scope="col" className="px-2 py-2 text-right">{ticker}</th>)}
            </tr></thead>
            <tbody className="divide-y divide-neutral-800/30">
              {data.correlations.matrix.map((row, index) => <tr key={data.correlations!.tickers[index]}>
                <th scope="row" className="px-2 py-2 text-left font-medium">{data.correlations!.tickers[index]}</th>
                {row.map((value, column) => <td key={column} className="px-2 py-2 text-right tabular-nums">
                  {value == null ? '—' : value.toFixed(2)}
                </td>)}
              </tr>)}
            </tbody>
          </table>
        </div>
      </> : !loading && <p className="text-xs text-fg-muted">{nl ? 'Geen gedeelde koershistorie beschikbaar. Vernieuw de factsheet.' : 'Shared price history is unavailable. Refresh the factsheet.'}</p>}
    </section>
    {!!data?.downside?.length && <section className="space-y-2" aria-labelledby="factsheet-downside">
      <h2 id="factsheet-downside" className="text-sm font-semibold">{nl ? 'ACWI en HDG bij koersdalingen' : 'ACWI and HDG on down days'}</h2>
      <p className="text-xs text-fg-muted">{nl
        ? 'De crisisperiodes tellen alleen dagen waarop ACWI daalde. Alle rendementen zijn in EUR.'
        : 'Each selloff includes only dates when ACWI fell. Returns are in EUR. Correlation uses at least three observations.'}</p>
      <div className="overflow-x-auto rounded-xl border border-neutral-800/40">
        <table className="w-full text-sm" aria-labelledby="factsheet-downside">
          <thead className="bg-elevated text-fg-muted"><tr>
            {(nl ? ['Selectie', 'Dagen', 'Correlatie', 'Gem. ACWI', 'Gem. HDG', 'HDG steeg']
              : ['Sample', 'Days', 'Correlation', 'Avg. ACWI', 'Avg. HDG', 'HDG rose']).map((heading, index) =>
              <th key={heading} scope="col" className={`px-2 py-2 ${index ? 'text-right' : 'text-left'}`}>{heading}</th>)}
          </tr></thead>
          <tbody className="divide-y divide-neutral-800/30">
            {data.downside.map(row => <tr key={row.key} className="tabular-nums">
              <th scope="row" className="px-2 py-2 text-left font-medium">
                {row.key === 'worst' ? (nl ? 'Slechtste 5% ACWI-dagen' : 'Worst 5% of ACWI days')
                  : row.key === 'covid' ? 'COVID-19' : 'Liberation Day'}
                <div className="text-xs font-normal text-fg-muted">{row.start} → {row.end}</div>
              </th>
              <td className="px-2 py-2 text-right">{row.observations}</td>
              <td className="px-2 py-2 text-right">{row.correlation?.toFixed(2) ?? '—'}</td>
              <td className="px-2 py-2 text-right">{pct(row.acwi_mean_pct)}</td>
              <td className="px-2 py-2 text-right">{pct(row.hdg_mean_pct)}</td>
              <td className="px-2 py-2 text-right">{row.observations ? `${row.hdg_positive} / ${row.observations}` : '—'}</td>
            </tr>)}
          </tbody>
        </table>
      </div>
    </section>}
    {data?.allocations && <section className="space-y-2" aria-labelledby="factsheet-allocations">
      <h2 id="factsheet-allocations" className="text-sm font-semibold">
        {nl ? 'Allocaties met kwartaalherweging' : 'Allocations with quarterly rebalancing'}
      </h2>
      <label className="inline-flex items-center gap-2 text-sm">
        {nl ? 'Risicoprofiel' : 'Risk profile'}
        <select className="rounded border border-neutral-800/40 bg-elevated p-2"
          value={equityWeight} onChange={e => setEquityWeight(Number(e.target.value))}>
          <option value={100}>Offensief · 100% ACWI</option>
          <option value={80}>Bep. Offensief · 80% ACWI</option>
          <option value={60}>Neutraal · 60% ACWI</option>
          <option value={40}>Defensief · 40% ACWI</option>
        </select>
      </label>
      <p className="text-xs text-fg-muted">
        {data.allocations.start} → {data.allocations.end}{' · '}
        {nl ? 'EUR, vaste doelgewichten, zonder transactiekosten of belastingen. Sharpe gebruikt 0% als kasrente.'
          : 'EUR, fixed target weights, excluding trading costs and taxes. Sharpe uses a 0% cash rate.'}
      </p>
      <div className="overflow-x-auto rounded-xl border border-neutral-800/40">
        <table className="w-full text-sm" aria-labelledby="factsheet-allocations">
          <thead className="bg-elevated text-fg-muted"><tr>
            {allocationHeaders.map(([key, heading]) => {
              const active = allocationSort.key === key;
              return <th key={key} scope="col" className={`px-2 py-2 ${key === 'weights' ? 'text-left' : 'text-right'}`}
                aria-sort={active ? (allocationSort.ascending ? 'ascending' : 'descending') : undefined}>
                <button type="button" className="text-inherit hover:underline"
                  onClick={() => setAllocationSort(previous => ({
                    key, ascending: previous.key === key ? !previous.ascending : key === 'weights' || key === 'volatility',
                  }))}>
                  {heading} <span aria-hidden="true">{active ? (allocationSort.ascending ? '↑' : '↓') : '↕'}</span>
                </button>
              </th>;
            })}
          </tr></thead>
          <tbody className="divide-y divide-neutral-800/30">
            {!allocationRows.length && <tr><td colSpan={5} className="px-2 py-3 text-fg-muted">
              {nl ? 'Geen allocaties beschikbaar voor dit profiel.' : 'No allocations available for this profile.'}
            </td></tr>}
            {allocationRows.map(row => {
              const drawdowns = row.drawdowns_pct ?? (row.max_drawdown_pct == null ? [] : [row.max_drawdown_pct]);
              return <tr key={row.weights.join('/')} className="tabular-nums">
              <th scope="row" className="px-2 py-2 text-left font-medium">{row.weights.map(weight => `${weight}%`).join(' / ')}</th>
              <td className="px-2 py-2 text-right">{pct(row.cagr_pct)}</td>
              <td className="px-2 py-2 text-right">{row.sharpe?.toFixed(2) ?? '—'}</td>
              <td className="px-2 py-2 text-right">{pct(row.volatility_pct)}</td>
              <td className="px-2 py-2 text-right">
                {drawdowns.length ? drawdowns.map((depth, index) =>
                  <span key={index} className="block">{pct(depth)}</span>) : '—'}
              </td>
              </tr>;
            })}
          </tbody>
        </table>
      </div>
    </section>}
  </section>;
}
