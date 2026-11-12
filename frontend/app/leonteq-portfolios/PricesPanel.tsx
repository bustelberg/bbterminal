'use client';

import { useRef, useState } from 'react';
import { API_URL } from '../../lib/apiUrl';
import { refreshRequest } from './refreshProgress';
import { matchHoldings } from './matchHoldings';

type PriceRow = {
  source: string; name: string; isin: string | null; symbol: string | null;
  source_price: number | string | null; source_currency: string | null; source_date: string | null;
  yahoo_price: number | null; yahoo_currency: string | null; yahoo_date: string | null;
  difference_pct: number | null; alert: boolean; status: 'ok' | 'alert' | 'unavailable' | 'cash'; reason: string;
  reason_code?: string;
  yahoo_name?: string | null;
  comparison_basis?: 'leonteq_airs';
  comparison_counted?: boolean;
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
  // Match company exposure as in Composition, solely for display. Each source keeps
  // its own security identity, quote, currency and independently calculated difference.
  const holdings = (source: string) => rows.filter(row => row.source === source)
    .map(row => ({ isin: row.isin, name: row.name, weight: null }));
  const pairs = matchHoldings(holdings('Leonteq'), holdings('AIRS')).matched.filter(pair => pair.company);
  for (const pair of pairs) {
    const left = groups.filter(group => group.key === pair.leonteq[0].isin?.trim().toUpperCase());
    const right = groups.filter(group => group.key === pair.airs[0].isin?.trim().toUpperCase());
    if (left.length !== 1 || right.length !== 1 || left[0] === right[0]
      || left[0].rows.length !== 1 || right[0].rows.length !== 1) continue;
    left[0].rows.push(...right[0].rows);
    groups.splice(groups.indexOf(right[0]), 1);
  }
  return groups;
}

function shortReason(row: PriceRow) {
  switch (row.reason_code) {
    case 'valuation_date_mismatch': return row.comparison_basis === 'leonteq_airs'
      ? 'Source dates differ' : `Yahoo close: ${valuationDate(row.yahoo_date)}; source: ${valuationDate(row.source_date)}`;
    case 'source_pair_missing': return `No unique ${row.source === 'Leonteq' ? 'AIRS' : 'Leonteq'} match`;
    case 'currency_mismatch': return row.comparison_basis === 'leonteq_airs' ? 'Source currencies differ'
      : `Currency mismatch: ${row.source_currency ?? '?'} / ${row.yahoo_currency ?? '?'}`;
    case 'listing_unresolved': return 'No verified Yahoo listing';
    case 'missing_isin': return 'Missing ISIN / listing';
    case 'missing_currency': return 'Missing source currency';
    case 'yahoo_request_failed': return 'Yahoo request failed';
    case 'yahoo_response_unavailable': return 'Yahoo quote unavailable';
    case 'no_yahoo_closes': return 'No completed Yahoo close';
    case 'no_prior_close': return 'No Yahoo close before source date';
    case 'invalid_price': return 'Missing or invalid price';
    case 'missing_source_date': return 'Missing source date';
    default: return row.reason;
  }
}

export default function PricesPanel() {
  const [result, setResult] = useState<PriceResult | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [issuesOnly, setIssuesOnly] = useState(true);
  const [uniqueCompanies, setUniqueCompanies] = useState(true);
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

  async function recheck(members: { portfolio: PriceResult['portfolios'][number]; rows: PriceRow[] }[], key: string) {
    if (running.current) return;
    running.current = true; setBusy(true); setCheckingRow(key); setError(null);
    const failures: string[] = [];
    for (const { portfolio, rows: sourceRows } of members) {
      for (const row of sourceRows) {
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
          const rows = portfolios.flatMap(p => p.rows).filter(row => row.comparison_counted !== false);
            return { ...previous, portfolios, warnings: [...new Set([...(previous.warnings ?? []), ...(update.warnings ?? [])])], summary: { ...previous.summary,
              checked: rows.filter(r => r.status === 'ok' || r.status === 'alert').length,
              alerts: rows.filter(r => r.status === 'alert').length,
              unavailable: rows.filter(r => r.status === 'unavailable').length,
              cash: rows.filter(r => r.status === 'cash').length,
              yahoo_symbols: new Set(rows.map(r => r.symbol).filter(Boolean)).size } };
          });
        } catch (cause) { failures.push(`${row.source}: ${cause instanceof Error ? cause.message : 'The row could not be rechecked.'}`); }
      }
    }
    if (failures.length) setError(failures.join(' '));
    running.current = false; setBusy(false); setCheckingRow(null);
  }

  const combined = new Map<string, { rows: PriceRow[]; members: { portfolio: PriceResult['portfolios'][number]; rows: PriceRow[] }[] }>();
  for (const portfolio of result?.portfolios ?? []) {
    for (const [index, group] of instrumentRows(portfolio.rows).entries()) {
      const first = group.rows[0];
      const identity = `${first.comparison_basis ?? 'yahoo'}:${first.isin?.trim().toUpperCase() || first.symbol || group.key}`;
      const key = uniqueCompanies ? identity : JSON.stringify([portfolio.leonteq_name, portfolio.airs_portfolio_id, group.key, index]);
      const entry = combined.get(key) ?? { rows: [], members: [] };
      entry.members.push({ portfolio, rows: group.rows });
      for (const row of group.rows) {
        if (!entry.rows.some(existing => JSON.stringify(existing) === JSON.stringify(row))) entry.rows.push(row);
      }
      combined.set(key, entry);
    }
  }
  const visibleRows = [...combined].filter(([, group]) => !issuesOnly || group.rows.some(row => row.status === 'alert' || row.status === 'unavailable'))
    .map(([key, group]) => ({ key, group, portfolio: group.members[0].portfolio }));
  const causes = new Map<string, { portfolio: PriceResult['portfolios'][number]; row: PriceRow }[]>();
  for (const portfolio of result?.portfolios ?? []) {
    for (const row of portfolio.rows.filter(row => row.status === 'unavailable' && row.comparison_counted !== false)) {
      const cause = row.reason_code?.replaceAll('_', ' ') ?? row.reason;
      causes.set(cause, [...(causes.get(cause) ?? []), { portfolio, row }]);
    }
  }

  return <section className="space-y-5" aria-label="Prices" aria-busy={busy}>
    <div className="flex flex-wrap items-start justify-between gap-4">
      <div>
        <h2 className="text-lg font-semibold">Prices</h2>
      </div>
      <button type="button" onClick={() => void checkAll()} disabled={busy}
        className="rounded-lg bg-accent-600 px-4 py-2 text-sm font-medium text-white hover:bg-accent-500 disabled:opacity-50">
        {busy && !checkingRow ? 'Checking prices…' : 'Check all linked portfolios'}
      </button>
    </div>
    {error && <p role="alert" className="text-sm text-neg-300">{error}</p>}
    {!result && !busy && <p className="text-sm text-fg-muted">Select Check all linked portfolios to refresh both sources and fetch Yahoo prices. Progress appears in the bottom-right popup.</p>}
    {result && <>
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex flex-wrap gap-4">
          <label className="flex items-center gap-2 text-sm"><input type="checkbox" checked={issuesOnly} onChange={event => setIssuesOnly(event.target.checked)} />Alerts and uncheckable only</label>
          <label className="flex items-center gap-2 text-sm"><input type="checkbox" checked={uniqueCompanies} onChange={event => setUniqueCompanies(event.target.checked)} />Unique companies</label>
        </div>
      </div>
      {(result.errors.length > 0 || (result.warnings?.length ?? 0) > 0) && <details className="rounded-lg border border-neutral-800/40 px-4 py-3 text-xs text-warn-300">
        <summary className="cursor-pointer font-medium">{new Set([...result.errors, ...(result.warnings ?? [])]).size} refresh notices · View details</summary>
        <ul className="mt-2 list-disc space-y-1 pl-4">{[...new Set([...result.errors, ...(result.warnings ?? [])])].map(message => <li key={message}>{message}</li>)}</ul>
      </details>}
      {causes.size > 0 && <details className="rounded-lg border border-neutral-800/40 px-3 py-2 text-xs text-warn-300">
        <summary className="cursor-pointer font-medium">{result.summary.unavailable} uncheckable comparisons · View causes</summary>
        {[...causes].map(([cause, entries]) => {
          const instruments = new Map<string, typeof entries>();
          for (const entry of entries) {
            const identity = uniqueCompanies ? entry.row.isin || entry.row.symbol || entry.row.name : JSON.stringify(entry);
            instruments.set(identity, [...(instruments.get(identity) ?? []), entry]);
          }
          return <details key={cause} className="mt-2">
          <summary className="cursor-pointer">{entries.length} · {cause}</summary>
          <ul className="mt-1 space-y-1 text-fg-muted">{[...instruments].map(([identity, items]) => <li key={identity}
            title={items.map(({ portfolio, row }) => `${row.name} · ${row.isin ?? 'No ISIN'} · ${row.symbol ?? row.comparison_basis ?? 'No Yahoo listing'}\n${row.source} · ${portfolio.leonteq_name} → ${portfolio.airs_name ?? portfolio.airs_portfolio_id}\nSource: ${price(row.source_price, row.source_currency)} · ${valuationDate(row.source_date)}; Yahoo: ${price(row.yahoo_price, row.yahoo_currency)} · ${valuationDate(row.yahoo_date)}\n${row.reason}`).join('\n\n')}>
            <span className="font-medium">{items[0].row.name}</span> · {[...new Set(items.map(item => shortReason(item.row)))].join('; ')}{items.length > 1 && <span> · {items.length} comparisons</span>}
          </li>)}</ul>
        </details>; })}
      </details>}
      {!result.portfolios.length && <p className="text-sm text-fg-muted">No Leonteq to AIRS Dynamic connections are saved.</p>}
      {result.portfolios.length > 0 && <div className="overflow-x-auto rounded-xl border border-neutral-800/40">
          <table className="w-full whitespace-nowrap text-xs leading-tight">
            <caption className="sr-only">Leonteq, AIRS and Yahoo Finance prices by portfolio and instrument</caption>
            <thead className="bg-elevated text-xs text-fg-muted"><tr>{['Leonteq name', 'AIRS name', 'Yahoo Finance name', 'Portfolio', 'Leonteq', 'AIRS', 'Yahoo Finance', 'Difference', 'Result / Recheck'].map(label => <th key={label} className="px-2 py-1.5 text-left font-medium" scope="col">{label}</th>)}</tr></thead>
            <tbody className="divide-y divide-neutral-800/30">{visibleRows.map(({ portfolio, group, key }) => {
              const first = group.rows[0];
              const direct = first.comparison_basis === 'leonteq_airs';
              const leonteqNames = [...new Set(group.rows.filter(row => row.source === 'Leonteq').map(row => row.name))];
              const airsNames = [...new Set(group.rows.filter(row => row.source === 'AIRS').map(row => row.name))];
              const leonteqIsins = [...new Set(group.rows.filter(row => row.source === 'Leonteq').map(row => row.isin ?? 'No ISIN'))];
              const airsIsins = [...new Set(group.rows.filter(row => row.source === 'AIRS').map(row => row.isin ?? 'No ISIN'))];
              const yahooNames = [...new Map(group.rows.filter(row => row.symbol).map(row => [JSON.stringify([row.symbol, row.yahoo_name]), row])).values()];
              const sameQuote = group.rows.every(row => row.symbol === first.symbol && row.yahoo_price === first.yahoo_price && row.yahoo_currency === first.yahoo_currency && row.yahoo_date === first.yahoo_date);
              const references = sameQuote ? [first] : group.rows;
              return <tr key={key} className={group.rows.some(row => row.alert) ? 'bg-red-500/10' : 'hover:bg-elevated/40'}>
              <th scope="row" className="max-w-44 px-2 py-1.5 text-left font-medium">{leonteqNames.length ? leonteqNames.map(name => <div key={name} className="truncate" title={name}>{name}</div>) : <span className="text-fg-muted">Not reported</span>}<div className="mt-0.5 text-[10px] font-normal text-fg-muted">{leonteqIsins.join(', ')}</div></th>
              <td className="max-w-44 px-2 py-1.5">{airsNames.length ? airsNames.map(name => <div key={name} className="truncate" title={name}>{name}</div>) : <span className="text-fg-muted">Not reported</span>}<div className="mt-0.5 text-[10px] text-fg-muted">{airsIsins.join(', ')}</div></td>
              <td className="max-w-44 px-2 py-1.5">{direct ? <span className="text-fg-muted">Not used</span> : yahooNames.length ? yahooNames.map((row, index) => <div key={index}><a href={`https://finance.yahoo.com/quote/${encodeURIComponent(row.symbol!)}/`} target="_blank" rel="noreferrer" className="block truncate text-accent-300 hover:underline" title={row.yahoo_name ?? 'Yahoo name unavailable'}>{row.yahoo_name ?? 'Name unavailable'}</a><div className="mt-0.5 text-[10px] text-fg-muted">{row.symbol}</div></div>) : <span className="text-fg-muted">Not reported</span>}</td>
              <td className="max-w-44 px-2 py-1.5"><div title={group.members.map(member => `${member.portfolio.leonteq_name} → ${member.portfolio.airs_name ?? member.portfolio.airs_portfolio_id}`).join('\n')}><div className="truncate" title={`Leonteq: ${portfolio.leonteq_name}`}><span className="mr-1 text-[10px] text-fg-muted">Leonteq</span>{portfolio.leonteq_name}</div><div className="mt-0.5 truncate" title={`AIRS: ${portfolio.airs_name ?? portfolio.airs_portfolio_id}`}><span className="mr-1 text-[10px] text-fg-muted">AIRS</span>{portfolio.airs_name ?? `AIRS #${portfolio.airs_portfolio_id}`}</div>{group.members.length > 1 && <details className="mt-0.5 text-[10px]"><summary className="cursor-pointer text-accent-300">+{group.members.length - 1} portfolios</summary>{group.members.slice(1).map(member => <div key={`${member.portfolio.leonteq_name}-${member.portfolio.airs_portfolio_id}`} className="mt-1 whitespace-normal">{member.portfolio.leonteq_name} → {member.portfolio.airs_name ?? member.portfolio.airs_portfolio_id}</div>)}</details>}</div></td>
              {['Leonteq', 'AIRS'].map(source => {
                const rows = group.rows.filter(row => row.source === source);
                return <td key={source} className="space-y-1 px-2 py-1.5 tabular-nums">{rows.length ? rows.map((row, index) => <div key={index} title={group.members.filter(member => member.rows.some(item => JSON.stringify(item) === JSON.stringify(row))).map(member => member.portfolio.leonteq_name).join(', ')}>{price(row.source_price, row.source_currency)}<div className="mt-0.5 text-[10px] text-fg-muted">{valuationDate(row.source_date)}</div></div>) : <span className="text-xs text-fg-muted">Not reported</span>}</td>;
              })}
              <td className="space-y-1 px-2 py-1.5 tabular-nums">{direct ? <span className="text-fg-muted">Not used</span> : references.map((row, index) => <div key={`${row.source}-${index}`}>
                {!sameQuote && <div className="text-xs text-fg-muted">For {row.source}</div>}
                {price(row.yahoo_price, row.yahoo_currency)}<div className="mt-0.5 text-[10px] text-fg-muted">{valuationDate(row.yahoo_date)}</div>
              </div>)}</td>
              <td className="space-y-1 px-2 py-1.5 tabular-nums">{group.rows.filter(row => row.comparison_counted !== false).map((row, index) => <div key={`${row.source}-${index}`} className={row.alert ? 'font-semibold text-neg-300' : ''}><span className="mr-1 text-[10px] text-fg-muted">{direct ? 'Leonteq vs AIRS' : `${row.source} vs Yahoo`}</span>{row.difference_pct == null ? '—' : `${row.difference_pct > 0 ? '+' : ''}${row.difference_pct.toLocaleString('en-GB', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}%`}</div>)}</td>
              <td className="max-w-64 px-2 py-1.5 text-[11px]"><div className="flex items-center gap-2"><div className="min-w-0 flex-1 space-y-1">{group.rows.filter(row => row.comparison_counted !== false).map((row, index) => <div key={`${row.source}-${index}`} className={`truncate ${row.alert ? 'text-neg-300' : row.status === 'unavailable' ? 'text-warn-300' : 'text-fg-muted'}`} title={`${row.source}: ${row.reason}`}><span className="font-medium">{row.source}: </span>{shortReason(row)}</div>)}</div>
                <button type="button" disabled={busy} className="shrink-0 text-accent-300 hover:underline disabled:opacity-50"
                  aria-label={`Recheck ${first.name} in ${portfolio.leonteq_name}`}
                  onClick={() => void recheck(group.members, key)}>
                  {checkingRow === key ? 'Rechecking…' : 'Recheck'}
                </button>
              </div></td>
            </tr>; })}
            {!visibleRows.length && <tr><td colSpan={9} className="px-4 py-6 text-center text-fg-muted">{issuesOnly ? 'No alerts or uncheckable prices.' : 'No instrument prices reported.'}</td></tr>}
            </tbody>
          </table>
        </div>}
    </>}
  </section>;
}
