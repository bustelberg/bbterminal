'use client';

import { useEffect, useRef, useState } from 'react';
import { API_URL } from '../../lib/apiUrl';
import { apiFetch } from '../../lib/apiFetch';
import { ExpectedJobFailure, startLocalJob } from '../../lib/stores/jobs';
import type { PortfolioConnection } from './ConnectionHoldingsView';
import type { AirsComposition } from './AirsCompositionView';
import { refreshRequest } from './refreshProgress';
import { recentWeightPlan, type RecentWeightRow, type TransactionBundle } from './recentWeightPlan';
import CashBalancePanel from './CashBalancePanel';

const button = 'rounded-lg border border-neutral-800/40 px-3 py-2 text-sm disabled:opacity-50';
const pct = (value: number | null) => value == null ? '—' : `${value.toFixed(2)}%`;
const date = (value?: string | null) => value ? new Date(`${value.slice(0, 10)}T12:00:00Z`).toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric', timeZone: 'UTC' }).toLowerCase() : 'Unknown date';

async function fetchBundle(link: PortfolioConnection, days: number): Promise<TransactionBundle> {
  const response = await refreshRequest(`${API_URL}/api/leonteq/recent-transactions`, {
    method: 'POST', body: JSON.stringify({ name: link.leonteq_name, airs_portfolio_id: link.airs_portfolio_id, days }),
  }, `Recent transactions · ${link.leonteq_name}`);
  const data = await response.json();
  if (!response.ok || !data.success || !Array.isArray(data.transactions)) throw new Error(data.detail ?? 'Could not fetch complete transactions and compositions.');
  return data as TransactionBundle;
}

export default function RecentTransactionsPanel({ active = true, onBusyChange }: { active?: boolean; onBusyChange?: (busy: boolean) => void }) {
  const [links, setLinks] = useState<PortfolioConnection[]>([]);
  const [selected, setSelected] = useState('');
  const [days, setDays] = useState(7);
  const [bundle, setBundle] = useState<TransactionBundle | null>(null);
  const [dummy, setDummy] = useState(true);
  const [busy, setBusy] = useState(false);
  const [workingRows, setWorkingRows] = useState<Set<string>>(new Set());
  const pendingRows = useRef(new Set<string>());
  const actionQueue = useRef(Promise.resolve());
  const [loading, setLoading] = useState(false);
  const [outcomes, setOutcomes] = useState<Record<string, string>>({});
  const [error, setError] = useState<string | null>(null);
  const inFlight = useRef(false), alive = useRef(true);
  useEffect(() => { alive.current = true; return () => { alive.current = false; }; }, []);
  useEffect(() => { onBusyChange?.(busy); }, [busy, onBusyChange]);
  useEffect(() => {
    if (!active) return;
    let current = true;
    setLoading(true);
    void apiFetch(`${API_URL}/api/leonteq/portfolio-links?kind=fixed`, { cache: 'no-store', noReadCache: true })
      .then(async response => { if (!response.ok) throw new Error('Could not load Fixed connections.'); return response.json() as Promise<{ links: PortfolioConnection[] }>; })
      .then(data => {
        if (!current) return;
        setLinks(data.links); setSelected(previous => previous && data.links.some(link => `${link.leonteq_name}:${link.airs_portfolio_id}` === previous)
          ? previous : data.links[0] ? `${data.links[0].leonteq_name}:${data.links[0].airs_portfolio_id}` : '');
      }).catch(cause => { if (current) setError(cause.message); })
      .finally(() => { if (current) setLoading(false); });
    return () => { current = false; };
  }, [active]);
  const link = links.find(link => `${link.leonteq_name}:${link.airs_portfolio_id}` === selected);
  const rows = bundle ? recentWeightPlan(bundle) : [];
  const total = bundle?.airs.rows.reduce<number | null>((sum, row) => sum == null || row.Percentage == null ? null : sum + row.Percentage, 0) ?? null;
  function reset() { setBundle(null); setOutcomes({}); setError(null); }

  async function fetchTrades() {
    if (!link || inFlight.current) return;
    inFlight.current = true; setBusy(true); setError(null);
    try {
      const result = await fetchBundle(link, days);
      if (alive.current) { setBundle(result); setOutcomes({}); }
    } catch (cause) { if (alive.current) setError(cause instanceof Error ? cause.message : 'Fetch failed. Previous results remain visible.'); }
    finally { inFlight.current = false; if (alive.current) setBusy(false); }
  }

  function apply(row: RecentWeightRow) {
    if (!link || !bundle || row.reason || row.aligned || row.current == null || row.target == null || inFlight.current || outcomes[row.key]) return;
    if (pendingRows.current.has(row.key)) return;
    pendingRows.current.add(row.key);
    setWorkingRows(new Set(pendingRows.current)); setBusy(true); setError(null);
    const previousAction = actionQueue.current;
    let releaseAction!: () => void;
    actionQueue.current = new Promise<void>(resolve => { releaseAction = resolve; });
    const connection = link, reviewed = bundle;
    startLocalJob(`${dummy ? 'Simulate' : row.operation === 'add' ? 'Add AIRS holding' : 'Set AIRS weight'} · ${row.name}`, 'leonteq.rebalance.recent', async (_signal, report) => {
      try {
        // Keep AIRS session writes sequential while each row owns its progress state.
        await previousAction;
        if (dummy) {
          report({ done: 1, total: 1, message: 'Simulated. No AIRS weights were changed.' });
          if (alive.current) setOutcomes(current => ({ ...current, [row.key]: row.operation === 'add' ? 'Simulated add' : 'Simulated' }));
          return 'Simulation complete. No AIRS changes.';
        }
        report({ done: 0, total: 3, message: 'Rechecking booked trades and both current compositions…' });
        const fresh = await fetchBundle(connection, days);
        if (!alive.current) throw new Error('View closed before submission. No AIRS change was sent.');
        const current = recentWeightPlan(fresh).find(item => item.key === row.key);
        if (!current || current.reason || current.operation !== row.operation || current.current !== row.current || current.target !== row.target
            || current.trades.map(trade => trade.id).sort().join('|') !== row.trades.map(trade => trade.id).sort().join('|')
            || current.rowId !== row.rowId || current.airsIsin !== row.airsIsin || fresh.airs.datum !== reviewed.airs.datum) {
          throw new Error('The reviewed connection, weights or composition date changed. Fetch transactions and review again.');
        }
        report({ done: 1, total: 3, message: row.operation === 'add' ? 'Looking up the ISIN and adding this one holding to AIRS Fixed…' : 'Submitting this one reviewed weight to AIRS Fixed…' });
        // Submit once; an uncertain write must never be automatically retried.
        const adding = row.operation === 'add';
        const response = await apiFetch(`${API_URL}/api/leonteq/airs/model-portfolios/${connection.airs_portfolio_id}/positions${adding ? '' : `/${row.rowId}/percentage`}`, {
          method: adding ? 'POST' : 'PATCH', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(adding ? { datum: fresh.airs.datum, isin: row.isin, percentage: row.target, leonteq_name: connection.leonteq_name }
            : { datum: fresh.airs.datum, expected_percentage: row.current, percentage: row.target }),
        });
        const body = await response.json().catch(() => null);
        if (!response.ok) throw new Error(typeof body?.detail === 'string' ? body.detail : 'AIRS did not confirm the save. Fetch transactions before retrying.');
        const verified = body as AirsComposition;
        const saved = verified.rows.filter(item => adding ? item.ISINCode === row.isin : item.id === row.rowId);
        if (verified.datum !== fresh.airs.datum || saved.length !== 1 || saved[0].Percentage !== row.target || saved[0].ISINCode !== row.airsIsin
            || (adding ? !saved[0].id || fresh.airs.rows.some(item => item.id === saved[0].id) : saved[0].Fonds !== row.airsName)) {
          throw new Error('The AIRS save outcome is uncertain. Fetch transactions before retrying.');
        }
        report({ done: 3, total: 3, message: 'AIRS Fixed weight saved and verified.' });
        if (alive.current) { setBundle({ ...fresh, airs: verified }); setOutcomes(value => ({ ...value, [row.key]: 'Applied and verified' })); }
        return 'AIRS Fixed weight saved and verified.';
      } catch (cause) {
        const message = cause instanceof Error ? cause.message : 'Could not verify this action. Fetch transactions again.';
        if (alive.current) { setError(message); setOutcomes(value => ({ ...value, [row.key]: 'Recheck required' })); }
        throw new ExpectedJobFailure(message);
      } finally {
        pendingRows.current.delete(row.key);
        releaseAction();
        if (alive.current) { setWorkingRows(new Set(pendingRows.current)); setBusy(pendingRows.current.size > 0); }
      }
    }, { cancellable: false });
  }

  return <section aria-label="Recent transactions" className="space-y-4" aria-busy={busy}>
    <div><h2 className="text-lg font-semibold">Recent transactions</h2><p className="mt-1 text-sm text-fg-muted">Execute individual AIRS Fixed changes from booked Leonteq trades, using the current Leonteq weights.</p></div>
    <div className="flex flex-wrap items-end gap-4 rounded-xl border border-neutral-800/40 bg-elevated/40 p-4">
      <label className="grid flex-1 gap-2 text-sm">Fixed connection<select className={`${button} bg-elevated`} value={selected} disabled={busy || loading} onChange={event => { setSelected(event.target.value); reset(); }}>
        {!links.length && <option value="">No Fixed connections</option>}{links.map(link => <option key={`${link.leonteq_name}:${link.airs_portfolio_id}`} value={`${link.leonteq_name}:${link.airs_portfolio_id}`}>{link.leonteq_name} → {link.airs_name}</option>)}
      </select></label>
      <label className="grid gap-2 text-sm">Period<select className={`${button} bg-elevated`} value={days} disabled={busy} onChange={event => { setDays(Number(event.target.value)); reset(); }}>{[7, 30, 90].map(value => <option key={value} value={value}>Last {value} days</option>)}</select></label>
      <label className="flex items-center gap-2 py-2 text-sm"><input type="checkbox" checked={dummy} disabled={busy} onChange={event => { setDummy(event.target.checked); setOutcomes(current => Object.fromEntries(Object.entries(current).filter(([, status]) => !status.startsWith('Simulated')))); }} />Dummy mode</label>
      <button className={`${button} bg-accent-600 text-white`} disabled={busy || loading || !link} onClick={() => void fetchTrades()}>{busy && !workingRows.size ? 'Working…' : 'Fetch transactions'}</button>
    </div>
    <p className="text-xs text-fg-muted">{dummy ? 'Dummy · No AIRS changes' : 'Live · Individual writes to AIRS Fixed'} · Several trades in one instrument are combined. Targets come from current holdings, not transaction amounts.</p>
    {error && <p role="alert" className="text-sm text-neg-300">{error}</p>}
    {bundle && <>
      <p className="text-xs text-fg-muted">{bundle.transactions.length} booked trades · {rows.length} instruments · {date(bundle.from)} – {date(bundle.to)} · AIRS composition {date(bundle.airs.datum)} · Current total {pct(total)}</p>
      {!rows.length ? <p role="status" className="text-sm text-fg-muted">No booked Buy/Sell transactions in this period.</p> : <div className="overflow-x-auto rounded-xl border border-neutral-800/40"><table className="w-full whitespace-nowrap text-sm">
        <caption className="sr-only">Recent transaction weight actions</caption>
        <thead className="bg-elevated"><tr>{['Latest trade', 'Instrument / ISIN', 'Activity', 'AIRS Fixed', 'Leonteq now', 'Change', 'Status / Action'].map(label => <th key={label} scope="col" className="px-4 py-3 text-left font-medium">{label}</th>)}</tr></thead>
        <tbody className="divide-y divide-neutral-800/30">{rows.map(row => <tr key={row.key}>
          <td className="px-4 py-3">{date(row.latest)}</td><th scope="row" className="px-4 py-3 text-left font-medium">{row.name}<div className="text-xs font-normal text-fg-muted">{row.isin ?? 'No ISIN'}</div></th>
          <td className="px-4 py-3"><details><summary className="cursor-pointer">{row.trades.filter(trade => trade.event === 'Buy').length} buys · {row.trades.filter(trade => trade.event === 'Sell').length} sells</summary>
            <ul className="mt-2 space-y-1 text-xs">{row.trades.map(trade => <li key={trade.id}>{date(trade.date)} · {trade.event} · {trade.quantity.toLocaleString('en-GB')} units · {trade.price.toFixed(2)} {trade.currency} · #{trade.id}</li>)}</ul>
          </details></td>
          <td className="px-4 py-3">{row.operation === 'add' ? 'New · ' : ''}{pct(row.current)}<div className="mt-1 text-xs text-fg-muted">{row.airsName ?? (row.operation === 'add' ? 'New position' : 'No connected holding')}</div></td><td className="px-4 py-3">{pct(row.target)}</td><td className="px-4 py-3">{row.target != null && row.current != null ? `${row.target > row.current ? '+' : ''}${(row.target - row.current).toFixed(2)}pp` : '—'}
            {row.target != null && row.current != null && total != null && <div className="mt-1 text-xs text-fg-muted" title="Individual changes can temporarily leave the portfolio above or below 100%.">Total after: {pct(total - row.current + row.target)}</div>}
          </td>
          <td className="px-4 py-3"><span className={row.reason ? 'text-warn-300' : 'text-fg-muted'}>{outcomes[row.key] ?? (row.reason ? 'Needs review' : row.aligned ? 'Already aligned' : row.operation === 'add' ? 'New position' : 'Ready to execute')}</span>
            {row.reason && <p className="mt-1 max-w-80 whitespace-normal text-xs text-warn-300">{row.reason}</p>}
            <button type="button" className={`${button} ml-3 bg-accent-600 text-white`} disabled={(busy && !workingRows.size) || workingRows.has(row.key) || !!row.reason || row.aligned || !!outcomes[row.key]}
              aria-label={`Execute ${row.name}`} title={`${dummy ? 'Simulate' : 'Execute'}: ${row.operation === 'add' ? 'add to AIRS Fixed at' : 'set AIRS Fixed weight to'} ${pct(row.target)}`} onClick={() => apply(row)}>{workingRows.has(row.key) ? 'Working…' : 'Execute'}</button>
          </td>
        </tr>)}</tbody>
      </table></div>}
      {link && <CashBalancePanel portfolioId={link.airs_portfolio_id} composition={bundle.airs} leonteq={bundle.leonteq} dummy={dummy} disabled={busy}
        onBusyChange={setBusy} onComposition={airs => setBundle(current => current ? { ...current, airs } : current)} />}
    </>}
  </section>;
}
