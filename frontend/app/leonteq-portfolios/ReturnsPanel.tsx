'use client';

import { useEffect, useRef, useState } from 'react';
import { API_URL } from '../../lib/apiUrl';
import { apiFetch } from '../../lib/apiFetch';
import { ExpectedJobFailure, startLocalJob } from '../../lib/stores/jobs';
import type { PortfolioConnection } from './ConnectionHoldingsView';
import MonthlyReturns, { monthlyFailure, queueMonthly, type MonthlyData } from './MonthlyReturns';
import { refreshRequest } from './refreshProgress';

type LeonteqSnapshot = { success: boolean; retrieved_at?: string; portfolios?: { name: string; ytd: string | null }[] };
type AirsReturn = { dynamic_portefeuille: string; ytd_pct: number | null; periode?: string | null; as_of?: string | null };
type Row = PortfolioConnection & { leonteq: number | null; airs: number | null; difference: number | null; asOf?: string | null };
function parsePercent(value?: string | null) {
  if (!value) return null;
  const text = value.trim().replace(/%$/, '').trim().replace(',', '.');
  if (!/^[+-]?\d+(?:\.\d+)?$/.test(text)) return null;
  const number = Number(text);
  return Number.isFinite(number) ? number : null;
}
export function compareReturns(links: PortfolioConnection[], source: LeonteqSnapshot, accounts: AirsReturn[]): Row[] {
  return links.map((link) => {
    const sources = source.success ? source.portfolios?.filter((row) => row.name === link.leonteq_name) ?? [] : [];
    const targets = accounts.filter((row) => row.dynamic_portefeuille === link.airs_name);
    const leonteq = sources.length === 1 ? parsePercent(sources[0].ytd) : null;
    const account = targets.length === 1 ? targets[0] : null;
    const airs = account?.ytd_pct != null && Number.isFinite(account.ytd_pct) ? account.ytd_pct : null;
    return { ...link, leonteq, airs, difference: leonteq !== null && airs !== null ? leonteq - airs : null, asOf: account?.as_of ?? account?.periode };
  });
}

export default function ReturnsPanel() {
  const [rows, setRows] = useState<PortfolioConnection[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [states, setStates] = useState<Record<string, 'Queued' | 'Refreshing' | 'Updated' | 'Failed'>>({});
  const [reasons, setReasons] = useState<Record<string, string>>({});
  const [errorReports, setErrorReports] = useState<Record<string, unknown>>({});
  const [fresh, setFresh] = useState<Record<string, MonthlyData>>({});
  const inFlight = useRef(false);
  const keyFor = (row: PortfolioConnection) => JSON.stringify([row.leonteq_name, row.airs_portfolio_id]);
  async function refreshAll(only?: PortfolioConnection) {
    if (inFlight.current) return;
    inFlight.current = true; setRefreshing(true); setError(false);
    setReasons((current) => only ? { ...current, [keyFor(only)]: '' } : {});
    setErrorReports((current) => only ? { ...current, [keyFor(only)]: undefined } : {});
    startLocalJob(only ? `Returns · ${only.leonteq_name}` : 'Returns · Refresh all', 'leonteq.returns', async (_signal, report) => {
    let completed = 0, total = only ? 1 : 0;
    const setProgress = (message: string) => report({ done: completed, total, message });
    setProgress('Loading connections…');
    let failures = 0;
    try {
      let links = only ? [only] : [];
      if (!only) {
        const response = await apiFetch(`${API_URL}/api/leonteq/portfolio-links?kind=dynamic`, { cache: 'no-store', noReadCache: true });
        if (!response.ok) throw new Error('links_failed');
        links = (await response.json() as { links: PortfolioConnection[] }).links;
        setRows(links);
      }
      setStates((current) => ({ ...current, ...Object.fromEntries(links.map((link) => [keyFor(link), 'Queued' as const])) }));
      total = links.length;
      for (let index = 0; index < links.length; index++) {
        const link = links[index];
        setProgress(`${index + 1}/${links.length} · Waiting · ${link.leonteq_name} → ${link.airs_name}`);
        let diagnostic: unknown;
        try {
          const result = await queueMonthly(() => {
            setStates((current) => ({ ...current, [keyFor(link)]: 'Refreshing' }));
            setProgress(`${index + 1}/${links.length} · Refreshing ${link.leonteq_name} → ${link.airs_name}`);
            return refreshRequest(`${API_URL}/api/leonteq/portfolios/monthly-performance`, {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ name: link.leonteq_name, airs_portfolio_id: link.airs_portfolio_id, refresh: true }),
          }, `Returns · ${link.leonteq_name}`); });
          const data = await result.json().catch(() => ({})) as MonthlyData;
          diagnostic = { http_status: result.status, response: { success: data.success, status: data.status,
            refresh_error: data.refresh_error, refresh_errors: data.refresh_errors, saved_data: data.saved_data,
            retrieved_at: data.retrieved_at, detail: (data as MonthlyData & { detail?: unknown }).detail } };
          if (!result.ok || !data.success || !data.months) throw new Error(monthlyFailure(data, result.ok ? undefined : result.status));
          if (data.refresh_error) failures++;
          if (data.refresh_error) { setReasons((current) => ({ ...current, [keyFor(link)]: monthlyFailure(data) })); setErrorReports((current) => ({ ...current, [keyFor(link)]: diagnostic })); }
          setStates((current) => ({ ...current, [keyFor(link)]: data.refresh_error ? 'Failed' : 'Updated' }));
          setFresh((current) => ({ ...current, [keyFor(link)]: data }));
        } catch (cause) { failures++; setStates((current) => ({ ...current, [keyFor(link)]: 'Failed' })); setReasons((current) => ({ ...current, [keyFor(link)]: cause instanceof Error ? cause.message : 'Network error. Retry.' })); setErrorReports((current) => ({ ...current, [keyFor(link)]: diagnostic ?? { error_type: cause instanceof Error ? cause.name : 'UnknownError' } })); }
        completed = index + 1;
        setProgress(`${completed}/${total} checked`);
      }
      if (failures) throw new ExpectedJobFailure(`Refresh incomplete · ${failures} failed`, 'Use Copy .json on the failed tables for details.');
      return only ? `${only.leonteq_name} refreshed` : 'All returns refreshed';
    } catch (cause) { setError(true); throw cause; }
    finally { inFlight.current = false; setRefreshing(false); }
    }, { cancellable: false });
  }

  useEffect(() => {
    let active = true;
    setLoading(true); setError(false);
    async function read<T>(path: string) {
      const response = await apiFetch(`${API_URL}${path}`, { cache: 'no-store', noReadCache: true });
      if (!response.ok) throw new Error('read_failed');
      return await response.json() as T;
    }
    void read<{ links: PortfolioConnection[] }>('/api/leonteq/portfolio-links?kind=dynamic').then((links) => {
      if (active) setRows(links.links);
    }).catch(() => { if (active) setError(true); })
      .finally(() => { if (active) setLoading(false); });
    return () => { active = false; };
  }, []);
  return <section className="space-y-5" aria-label="Returns" aria-busy={loading || refreshing}>
    <div className="flex flex-wrap items-center justify-between gap-3">
      <div><h2 className="text-lg font-semibold">Returns</h2><p className="mt-1 text-xs text-fg-muted">Monthly & YTD · Difference = Leonteq − AIRS · Current month = MTD</p></div>
      <button type="button" disabled={loading || refreshing} onClick={() => void refreshAll()} className="rounded-lg border border-neutral-800/40 bg-elevated px-3 py-2 text-sm text-fg-muted transition-colors hover:bg-overlay/5 disabled:opacity-50">{refreshing ? 'Refreshing…' : 'Refresh all'}</button>
    </div>
    {loading && <p role="status" className="text-sm text-fg-muted">Loading returns…</p>}
    {error && !rows.length && <p role="alert" className="text-sm text-neg-300">Could not load returns.</p>}
    {rows.map((row) => <MonthlyReturns key={keyFor(row)} link={row} fresh={fresh[keyFor(row)]} refreshState={states[keyFor(row)]} refreshError={reasons[keyFor(row)]} errorReport={errorReports[keyFor(row)]} onRefresh={() => void refreshAll(row)} refreshDisabled={refreshing || loading} />)}
    {!loading && !error && !rows.length && <p className="text-sm text-fg-muted">No dynamic connections saved.</p>}
  </section>;
}
