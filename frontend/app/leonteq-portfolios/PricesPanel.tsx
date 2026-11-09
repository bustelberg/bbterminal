'use client';

import { useRef, useState } from 'react';
import { API_URL } from '../../lib/apiUrl';
import { refreshRequest } from './refreshProgress';

type PriceRow = {
  source: string; name: string; isin: string | null; symbol: string | null;
  source_price: number | string | null; source_currency: string | null; source_date: string | null;
  yahoo_price: number | null; yahoo_currency: string | null; yahoo_date: string | null;
  difference_pct: number | null; alert: boolean; status: 'ok' | 'alert' | 'unavailable' | 'cash'; reason: string;
};
export type PriceResult = {
  success: boolean; checked_at: string; threshold_pct: number;
  summary: { portfolios: number; yahoo_symbols: number; checked: number; alerts: number; unavailable: number; cash: number };
  portfolios: { leonteq_name: string; airs_name: string | null; airs_portfolio_id: number; rows: PriceRow[] }[];
  errors: string[]; warnings?: string[]; refresh_error?: string | null;
};

const price = (value: number | string | null, currency: string | null) => value == null ? '—'
  : `${Number(value).toLocaleString('en-GB', { minimumFractionDigits: 2, maximumFractionDigits: 2 })} ${currency ?? ''}`;

const valuationDate = (value: string | null) => {
  if (!value) return 'Unknown date';
  const date = new Date(`${value}T00:00:00Z`);
  return Number.isNaN(date.getTime()) ? 'Unknown date'
    : date.toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric', timeZone: 'UTC' }).toLowerCase();
};

function instrumentRows(rows: PriceRow[]) {
  const groups: { key: string; rows: PriceRow[] }[] = [];
  for (const row of rows) {
    const identity = row.isin?.trim().toUpperCase() || `name:${row.name.trim().toLowerCase()}`;
    const existing = groups.find(group => group.key === identity && !group.rows.some(item => item.source === row.source));
    if (existing) existing.rows.push(row);
    else groups.push({ key: identity, rows: [row] });
  }
  return groups;
}

export default function PricesPanel() {
  const [result, setResult] = useState<PriceResult | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [issuesOnly, setIssuesOnly] = useState(false);
  const [checkingRow, setCheckingRow] = useState<string | null>(null);
  const running = useRef(false);

  async function checkAll() {
    if (running.current) return;
    running.current = true; setBusy(true); setError(null);
    try {
      const response = await refreshRequest(`${API_URL}/api/leonteq/price-checks`, undefined, 'Prices · Check all linked portfolios');
      if (!response.ok) throw new Error('The price check could not complete. Saved results remain visible.');
      const data = await response.json() as PriceResult;
      if (!data.success || !data.summary || !Array.isArray(data.portfolios)) throw new Error('No verified price-check result was returned.');
      setResult(data);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : 'The price check could not complete. Please retry.');
    } finally { running.current = false; setBusy(false); }
  }

  async function recheck(portfolio: PriceResult['portfolios'][number], row: PriceRow, key: string) {
    if (running.current) return;
    running.current = true; setBusy(true); setCheckingRow(key); setError(null);
    try {
      const response = await refreshRequest(`${API_URL}/api/leonteq/price-checks`, {
        method: 'POST', body: JSON.stringify({ name: portfolio.leonteq_name, airs_portfolio_id: portfolio.airs_portfolio_id,
          source: row.source, isin: row.isin, holding_name: row.name }),
      }, `Prices · Recheck ${row.name}`);
      if (!response.ok) throw new Error('The row could not be rechecked. Previous results remain visible.');
      const update = await response.json() as PriceResult;
      const replacement = update.portfolios?.[0]?.rows?.[0];
      if (!update.success || !replacement) throw new Error('No fresh row result was returned. Run Check all linked portfolios.');
      setResult(previous => {
        if (!previous) return previous;
        const portfolios = previous.portfolios.map(p => ({ ...p, rows: p.rows.map(r =>
          p.leonteq_name === portfolio.leonteq_name && p.airs_portfolio_id === portfolio.airs_portfolio_id
          && r.source === row.source && r.isin === row.isin && r.name === row.name ? replacement : r) }));
        const rows = portfolios.flatMap(p => p.rows);
        return { ...previous, portfolios, warnings: [...new Set([...(previous.warnings ?? []), ...(update.warnings ?? [])])], summary: { ...previous.summary,
          checked: rows.filter(r => r.status === 'ok' || r.status === 'alert').length,
          alerts: rows.filter(r => r.status === 'alert').length,
          unavailable: rows.filter(r => r.status === 'unavailable').length,
          cash: rows.filter(r => r.status === 'cash').length,
          yahoo_symbols: new Set(rows.map(r => r.symbol).filter(Boolean)).size } };
      });
    } catch (cause) { setError(cause instanceof Error ? cause.message : 'The row could not be rechecked.'); }
    finally { running.current = false; setBusy(false); setCheckingRow(null); }
  }

  return <section className="space-y-5" aria-label="Prices" aria-busy={busy}>
    <div className="flex flex-wrap items-start justify-between gap-4">
      <div>
        <h2 className="text-lg font-semibold">Prices</h2>
        <p className="mt-1 max-w-3xl text-sm text-fg-muted">Check every Leonteq to AIRS Dynamic connection against Yahoo Finance. Price differences larger than 5% are flagged.</p>
      </div>
      <button type="button" onClick={() => void checkAll()} disabled={busy}
        className="rounded-lg bg-accent-600 px-4 py-2 text-sm font-medium text-white hover:bg-accent-500 disabled:opacity-50">
        {busy && !checkingRow ? 'Checking prices…' : 'Check all linked portfolios'}
      </button>
    </div>
    <p className="text-xs text-fg-muted">Each instrument shows Leonteq, AIRS and Yahoo Finance prices side by side. Each difference compares the named source with Yahoo on that source’s valuation date. If dates or currencies differ between sources, the Yahoo column shows both reference quotes. Minor quote units are normalized. Cash is excluded.</p>
    {error && <p role="alert" className="text-sm text-neg-300">{error}</p>}
    {!result && !busy && <p className="text-sm text-fg-muted">Select Check all linked portfolios to refresh both sources and fetch Yahoo prices. Progress appears in the bottom-right popup.</p>}
    {result && <>
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="text-sm">
          <p role={result.summary.alerts ? 'alert' : 'status'} className={result.summary.alerts ? 'font-medium text-neg-300' : 'text-fg'}>
            {result.summary.alerts} price differences over 5% · {result.summary.checked} prices checked · {result.summary.portfolios} linked portfolios
          </p>
          <p className={`mt-1 text-xs ${result.summary.unavailable ? 'text-warn-300' : 'text-fg-muted'}`}>{result.summary.unavailable} uncheckable · {result.summary.cash} cash rows excluded · {result.summary.yahoo_symbols} unique Yahoo symbols · Checked {new Date(result.checked_at).toLocaleString('en-GB')}</p>
        </div>
        <label className="flex items-center gap-2 text-sm"><input type="checkbox" checked={issuesOnly} onChange={event => setIssuesOnly(event.target.checked)} />Alerts and uncheckable only</label>
      </div>
      {(result.errors.length > 0 || (result.warnings?.length ?? 0) > 0) && <details className="rounded-lg border border-neutral-800/40 px-4 py-3 text-xs text-warn-300">
        <summary className="cursor-pointer font-medium">{new Set([...result.errors, ...(result.warnings ?? [])]).size} refresh notices · View details</summary>
        <ul className="mt-2 list-disc space-y-1 pl-4">{[...new Set([...result.errors, ...(result.warnings ?? [])])].map(message => <li key={message}>{message}</li>)}</ul>
      </details>}
      {!result.portfolios.length && <p className="text-sm text-fg-muted">No Leonteq to AIRS Dynamic connections are saved.</p>}
      {result.portfolios.map(portfolio => {
        const visible = instrumentRows(portfolio.rows).filter(group => !issuesOnly || group.rows.some(row => row.status === 'alert' || row.status === 'unavailable'));
        const allGood = ['Leonteq', 'AIRS'].every(source => portfolio.rows.some(row => row.source === source && row.status === 'ok'))
          && portfolio.rows.every(row => row.status === 'ok' || row.status === 'cash');
        return <section key={JSON.stringify([portfolio.leonteq_name, portfolio.airs_portfolio_id])} className="space-y-3" aria-label={`${portfolio.leonteq_name} → ${portfolio.airs_name}`}>
        <h3 className="grid grid-cols-[minmax(0,1fr)_auto_minmax(0,1fr)_1.5rem] items-center gap-3 font-medium">
          <span className="min-w-0 break-words"><span className="mb-1 block text-xs font-normal text-fg-muted">Leonteq</span>{portfolio.leonteq_name}</span>
          <span aria-hidden="true">→</span>
          <span className="min-w-0 break-words"><span className="mb-1 block text-xs font-normal text-fg-muted">AIRS</span>{portfolio.airs_name ?? `AIRS #${portfolio.airs_portfolio_id}`}</span>
          {allGood && <span role="img" aria-label="All price checks passed" title="All price checks passed" className="inline-flex h-6 w-6 shrink-0 items-center justify-center rounded-full bg-green-600 text-white"><svg aria-hidden="true" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round" className="h-4 w-4"><path d="m5 12 4 4L19 6" /></svg></span>}
          {!allGood && <span role="img" aria-label="Price checks need attention" title="Price checks need attention" className="inline-flex h-6 w-6 shrink-0 items-center justify-center rounded-full bg-red-600 text-white"><svg aria-hidden="true" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round" className="h-4 w-4"><path d="m6 6 12 12M18 6 6 18" /></svg></span>}
        </h3>
        {visible.length > 0 && <div className="overflow-x-auto rounded-xl border border-neutral-800/40">
          <table className="w-full whitespace-nowrap text-sm">
            <caption className="sr-only">Leonteq, AIRS and Yahoo Finance prices by instrument</caption>
            <thead className="bg-elevated text-xs text-fg-muted"><tr>{['Instrument / ISIN', 'Leonteq', 'AIRS', 'Yahoo Finance', 'Difference vs Yahoo', 'Result / Recheck'].map(label => <th key={label} className="px-4 py-3 text-left font-medium" scope="col">{label}</th>)}</tr></thead>
            <tbody className="divide-y divide-neutral-800/30">{visible.map((group, index) => {
              const first = group.rows[0];
              const instrumentSymbol = group.rows.find(row => row.symbol)?.symbol;
              const sameQuote = group.rows.every(row => row.symbol === first.symbol && row.yahoo_price === first.yahoo_price && row.yahoo_currency === first.yahoo_currency && row.yahoo_date === first.yahoo_date);
              const references = sameQuote ? [first] : group.rows;
              return <tr key={`${group.key}-${index}`} className={group.rows.some(row => row.alert) ? 'bg-red-500/10' : 'hover:bg-elevated/40'}>
              <th scope="row" className="px-4 py-3 text-left font-medium">{instrumentSymbol ? <a href={`https://finance.yahoo.com/quote/${encodeURIComponent(instrumentSymbol)}/`} target="_blank" rel="noreferrer" className="text-accent-300 hover:underline">{first.name}</a> : first.name}<div className="mt-1 text-xs font-normal text-fg-muted">{first.isin ?? 'No ISIN'}</div></th>
              {['Leonteq', 'AIRS'].map(source => {
                const row = group.rows.find(row => row.source === source);
                return <td key={source} className="px-4 py-3 tabular-nums">{row ? <>{price(row.source_price, row.source_currency)}<div className="mt-1 text-xs text-fg-muted">{valuationDate(row.source_date)}</div></> : <span className="text-xs text-fg-muted">Not reported</span>}</td>;
              })}
              <td className="space-y-3 px-4 py-3 tabular-nums">{references.map(row => <div key={row.source}>
                {!sameQuote && <div className="text-xs text-fg-muted">For {row.source}</div>}
                {price(row.yahoo_price, row.yahoo_currency)}<div className="mt-1 text-xs text-fg-muted">{valuationDate(row.yahoo_date)}</div>
              </div>)}</td>
              <td className="space-y-2 px-4 py-3 tabular-nums">{group.rows.map(row => <div key={row.source} className={row.alert ? 'font-semibold text-neg-300' : ''}><span className="mr-2 text-xs text-fg-muted">{row.source}</span>{row.difference_pct == null ? '—' : `${row.difference_pct > 0 ? '+' : ''}${row.difference_pct.toLocaleString('en-GB', { minimumFractionDigits: 2, maximumFractionDigits: 6 })}%`}</div>)}</td>
              <td className="max-w-80 space-y-3 whitespace-normal px-4 py-3 text-xs">{group.rows.map(row => <div key={row.source} className={row.alert ? 'text-neg-300' : row.status === 'unavailable' ? 'text-warn-300' : 'text-fg-muted'}><span className="font-medium">{row.source}: </span>{row.reason}
                <button type="button" disabled={busy} className="mt-2 block text-accent-300 hover:underline disabled:opacity-50"
                  aria-label={`Recheck ${row.source} ${row.name}`}
                  onClick={() => void recheck(portfolio, row, JSON.stringify([portfolio.leonteq_name, portfolio.airs_portfolio_id, row.source, row.isin, row.name]))}>
                  {checkingRow === JSON.stringify([portfolio.leonteq_name, portfolio.airs_portfolio_id, row.source, row.isin, row.name]) ? 'Rechecking…' : `Recheck ${row.source}`}
                </button>
              </div>)}</td>
            </tr>; })}</tbody>
          </table>
        </div>}
      </section>; })}
    </>}
  </section>;
}
