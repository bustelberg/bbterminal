'use client';

import { useCallback, useEffect, useRef, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import type { AirsAccountIsins, AirsPortfolioOverview } from '../../../lib/types/api';
import { newPriceDifferences, summarizePrices, type PriceSummary } from './priceCheck';

export type PriceCheckState = { summary: PriceSummary | null; pending: boolean; error: boolean; checkedAt?: string };

export function usePriceChecks(accounts: AirsPortfolioOverview[]) {
  const [checks, setChecks] = useState<Record<string, PriceCheckState>>({});
  const [alerts, setAlerts] = useState<Record<string, number>>({});
  const previous = useRef<Record<string, PriceSummary>>({});
  const baselines = useRef<Record<string, PriceSummary | null>>({});
  const versions = useRef<Record<string, number>>({});
  const requestedAlerts = useRef(new Set<string>());
  const alive = useRef(true);
  useEffect(() => { alive.current = true; return () => { alive.current = false; }; }, []);

  const refresh = useCallback(async (account: string, alert = true) => {
    if (alert) requestedAlerts.current.add(account);
    const version = (versions.current[account] ?? 0) + 1;
    versions.current[account] = version;
    setChecks((current) => ({ ...current, [account]: { summary: previous.current[account] ?? null, pending: true, error: false } }));
    try {
      // Explicit refresh completion: read the saved check, never run validation here.
      const response = await apiFetch(`${API_URL}/api/airs/accounts/${encodeURIComponent(account)}/isins?stored_check=true`, { noReadCache: true });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const data = await response.json() as AirsAccountIsins;
      if (data.portefeuille !== account || !Array.isArray(data.rows)) throw new Error('Invalid price-check response');
      if (!alive.current || versions.current[account] !== version) return;
      const summary = summarizePrices(data);
      if (requestedAlerts.current.delete(account)) {
        const before = Object.hasOwn(baselines.current, account) ? baselines.current[account] : previous.current[account];
        const added = newPriceDifferences(before, summary);
        delete baselines.current[account];
        setAlerts((current) => {
          const next = { ...current };
          if (added) next[account] = added; else delete next[account];
          return next;
        });
      }
      previous.current[account] = summary;
      setChecks((current) => ({ ...current, [account]: { summary, pending: false, error: false, checkedAt: data.checked_at ?? undefined } }));
    } catch {
      if (!alive.current || versions.current[account] !== version) return;
      requestedAlerts.current.delete(account);
      delete baselines.current[account];
      setChecks((current) => ({ ...current, [account]: { summary: previous.current[account] ?? null, pending: false, error: true } }));
    }
  }, []);

  // Results arrive with the overview. No page-load requests or vendor work per account.
  const signature = JSON.stringify(accounts.map((row) => [row.dynamic_portefeuille, row.saved_price_check]));
  useEffect(() => {
    const saved = JSON.parse(signature) as [string, { result?: AirsAccountIsins; checked_at?: string } | null][];
    setChecks((current) => {
      const next = { ...current };
      for (const [account, check] of saved) {
        if (!check?.result || !Array.isArray(check.result.rows)) continue;
        const summary = summarizePrices(check.result);
        previous.current[account] = summary;
        if (!next[account]?.pending) next[account] = { summary, pending: false, error: false, checkedAt: check.checked_at };
      }
      return next;
    });
  }, [signature]);

  const dismissAlerts = useCallback(() => setAlerts({}), []);
  const beginRefresh = useCallback((accounts: string[]) => {
    for (const account of accounts) baselines.current[account] = previous.current[account] ?? null;
  }, []);
  const refreshMany = useCallback(async (accounts: string[]) => {
    const queue = [...accounts];
    const worker = async () => { while (queue.length) await refresh(queue.shift()!); };
    await Promise.all([worker(), worker()]);
  }, [refresh]);
  return { checks, alerts, refresh, refreshMany, beginRefresh, dismissAlerts };
}
