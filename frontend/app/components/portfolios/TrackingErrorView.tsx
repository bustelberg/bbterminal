'use client';

import { useEffect, useRef, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { startJob } from '../../../lib/stores/jobs';
import { invalidateReadCache } from '../../../lib/readCache';
import type { SourceKey } from '../../../lib/provenance';
import type { TrackingError } from '../../../lib/types/api';
import InfoTip from '../InfoTip';
import LoadingDots from './LoadingDots';
import { riskRequestKey, useRiskResult } from './useRiskResult';
import { riskHistoryQuery, riskHistoryYears } from './riskHistoryWindow';
import type { ActiveShareHolding } from './ActiveSharePanel';

const historyRepairs = new Map<string, { expires: number; done: Promise<boolean> }>();
const pct = (value?: number | null) => value == null ? '—' : `${value.toFixed(2)}%`;
const dateLabel = (value?: string | null) => value
  ? new Date(`${value}T12:00:00Z`).toLocaleDateString('en-GB', { day: 'numeric', month: 'long', year: 'numeric', timeZone: 'UTC' }) : '—';

export default function TrackingErrorView({ holdings, portfolioName }: {
  holdings: ActiveShareHolding[];
  portefeuille?: string | null;
  benchmark: string;
  benchmarkWeights?: Record<string, number>;
  portfolioName: string;
  portfolioAsOf?: string | null;
  portfolioFetchedAt?: string | null;
  portfolioSource: SourceKey;
}) {
  // Fully invested stock basket versus fully invested ACWI, independent of bond policy.
  const requestUrl = `${API_URL}/api/airs/portfolio/tracking-error?benchmark=ACWI&frequency=monthly&${riskHistoryQuery()}&view=stock-cagr-v1`;
  const requestBody = JSON.stringify({ holdings });
  const identity = riskRequestKey(requestUrl, requestBody);
  const [version, setVersion] = useState(0);
  const [status, setStatus] = useState<'idle' | 'repairing' | 'done' | 'failed'>('idle');
  const attempts = useRef(new Set<string>());
  const { data, error } = useRiskResult<TrackingError>(`${identity}\nrepair:${version}`, async () => {
    const response = await apiFetch(requestUrl, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: requestBody, noReadCache: true,
    });
    const body = await response.json().catch(() => null);
    if (!response.ok) throw new Error(body?.detail ?? `HTTP ${response.status}`);
    return body as TrackingError;
  }, 'stock-cagr', 'Stock performance could not be loaded');

  useEffect(() => {
    if (!data?.history_issues?.length || attempts.current.has(identity)) return;
    attempts.current.add(identity);
    const missing = new Set(data.history_issues.filter(issue => issue.kind === 'holding').map(issue => issue.isin));
    const existing = historyRepairs.get(identity);
    const done = existing && existing.expires > Date.now() ? existing.done : startJob(
      `${API_URL}/api/airs/portfolio/tracking-error/repair/job?benchmark=ACWI&years=${riskHistoryYears() + 1}&full_history=true&yahoo_only=true`,
      'Updating stock performance history',
      { headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ holdings: holdings.filter(holding => holding.isin && missing.has(holding.isin)) }) },
    ).then(job => job.done).then(job => job.status === 'done').catch(() => false);
    // A finished job does not prove that history is complete. Keep a short retry cooldown.
    historyRepairs.set(identity, { expires: Date.now() + 5 * 60_000, done });
    let active = true;
    setStatus('repairing');
    void done.then(ok => {
      if (!active) return;
      setStatus(ok ? 'done' : 'failed');
      if (ok) {
        invalidateReadCache('stock performance history updated');
        setVersion(value => value + 1);
      }
    });
    return () => { active = false; };
    // Identity contains the full request; response updates must not cancel the subscription.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [identity, !!data]);

  const available = !!data?.available && !!data.historical_comparison?.available;
  const updating = (!data && !error) || (!available && status === 'repairing');
  const details = <div className="space-y-2 text-xs">
    <p>Today’s included direct stocks, weighted to 100% and rebalanced annually. Yahoo Finance prices in EUR; the same dates are used for ACWI. This is hypothetical performance.</p>
    <p>Included stocks represent {pct(data?.covered_portfolio_pct)} of the original portfolio. Funds, ETFs, bonds, certificates and cash are excluded.</p>
    {!!data?.excluded_holdings?.length && <ul className="space-y-1">{data.excluded_holdings.map((holding, index) =>
      <li key={`${holding.isin}:${index}`}>{holding.name}: {holding.reason}</li>)}</ul>}
    {!available && data?.reason && <p>{data.reason}</p>}
  </div>;

  return <div className="mx-auto w-full max-w-2xl space-y-3">
    <p className="text-center text-xs text-fg-muted">Hypothetical annualised return · EUR <InfoTip wide content={details} /></p>
    {available && <p className="text-center text-xs text-fg-faint">{dateLabel(data.window_from)} to {dateLabel(data.window_to)}</p>}
    <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
      {[{ label: `${portfolioName} · stocks CAGR`, value: data?.historical_comparison?.portfolio_cagr_pct },
        { label: 'ACWI CAGR', value: data?.benchmark_history?.cagr_pct }].map(tile =>
        <div key={tile.label} className="min-w-0 rounded-lg border border-neutral-800/40 bg-elevated p-5">
          <div className="text-sm text-fg-muted">{tile.label}</div>
          <div className="mt-2 font-mono text-3xl tabular-nums text-fg-strong">{updating ? <LoadingDots /> : pct(available ? tile.value : null)}</div>
        </div>)}
    </div>
    {updating && <p role="status" className="text-center text-xs text-fg-muted">Loading Yahoo price history…</p>}
    {error && <p role="alert" className="text-center text-xs text-neg-300">{error}</p>}
    {data && !available && !updating && <p className="text-center text-xs text-fg-muted">Complete Yahoo history since 1 January 2021 is not available yet.</p>}
  </div>;
}
