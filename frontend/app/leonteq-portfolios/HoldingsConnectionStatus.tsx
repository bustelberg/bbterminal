'use client';

import { useEffect, useState } from 'react';
import { apiFetch } from '../../lib/apiFetch';
import { API_URL } from '../../lib/apiUrl';
import { readConnectionHoldings, connectionSnapshotKey, type PortfolioConnection, type ConnectionHoldingsCache } from './ConnectionHoldingsView';
import { matchHoldings, type HoldingRule } from './matchHoldings';

export default function HoldingsConnectionStatus({ link, nl, cache, rulesCache }: {
  link: PortfolioConnection; nl: boolean; cache: ConnectionHoldingsCache; rulesCache: Map<string, HoldingRule[]>;
}) {
  const [status, setStatus] = useState<'loading' | 'all' | 'some' | 'unknown'>('loading');
  useEffect(() => {
    let active = true;
    const params = new URLSearchParams({ leonteq_name: link.leonteq_name, airs_portfolio_id: String(link.airs_portfolio_id) });
    const rulesKey = JSON.stringify([link.leonteq_name, link.airs_portfolio_id]);
    const leftKey = connectionSnapshotKey(link, 'leonteq'), rightKey = connectionSnapshotKey(link, 'airs');
    void Promise.all([
      cache.get(leftKey) ?? readConnectionHoldings(link, 'leonteq', false, nl, true),
      cache.get(rightKey) ?? readConnectionHoldings(link, 'airs', false, nl, true),
      rulesCache.has(rulesKey) ? { rules: rulesCache.get(rulesKey)! } : apiFetch(`${API_URL}/api/leonteq/portfolio-links/holdings?${params}`, { cache: 'no-store', noReadCache: true })
        .then(async response => { if (!response.ok) throw new Error('rules'); return await response.json() as { rules: HoldingRule[] }; }),
    ]).then(([left, right, rules]) => {
      if (!active) return;
      cache.set(leftKey, left); cache.set(rightKey, right); rulesCache.set(rulesKey, rules.rules);
      if (left.warning || right.warning || right.noFixed || !left.rows.length || !right.rows.length) { setStatus('unknown'); return; }
      const result = matchHoldings(left.rows, right.rows, rules.rules);
      setStatus(!result.leonteqOnly.length && !result.airsOnly.length && !result.sold.some(row => row.conflict) ? 'all' : 'some');
    }).catch(() => { if (active) setStatus('unknown'); });
    return () => { active = false; };
  }, [link, nl, cache, rulesCache]);
  if (status === 'loading') return <span className="text-xs text-fg-muted">{nl ? 'Controleren…' : 'Checking…'}</span>;
  const all = status === 'all';
  const label = all ? (nl ? 'Alles gekoppeld' : 'All connected') : status === 'unknown'
    ? (nl ? 'Niet gecontroleerd' : 'Not verified') : (nl ? 'Niet alles gekoppeld' : 'Not all connected');
  return <span className="inline-flex items-center gap-2 whitespace-nowrap text-xs">
    <span aria-hidden="true" className={`inline-flex h-5 w-5 shrink-0 items-center justify-center rounded-full text-white ${all ? 'bg-green-600' : 'bg-red-600'}`}>
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round" className="h-3 w-3"><path d={all ? 'm5 12 4 4L19 6' : 'm6 6 12 12M18 6 6 18'} /></svg>
    </span>{label}
  </span>;
}
