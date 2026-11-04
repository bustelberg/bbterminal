'use client';

import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { loadPrefetchedAnalysis, readPrefetchedAnalysis } from '../../../lib/analysisPrefetch';
import { readGeneration } from '../../../lib/readCache';
import type { PerformanceBundle } from './monthlyReport';

const revisions = new Map<string, number>();
const key = (account: string) => `performance-bundle|${account}|${readGeneration()}|${revisions.get(account) ?? 0}`;

export function invalidatePerformanceBundle(account: string): void {
  revisions.set(account, (revisions.get(account) ?? 0) + 1);
}

export function readPerformanceBundle(account: string): PerformanceBundle | null {
  return readPrefetchedAnalysis<PerformanceBundle>(key(account)) ?? null;
}

export function loadPerformanceBundle(account: string): Promise<PerformanceBundle> {
  return loadPrefetchedAnalysis(key(account), async () => {
    const response = await apiFetch(`${API_URL}/api/airs/accounts/${encodeURIComponent(account)}/performance`,
      { noReadCache: true, cache: 'no-store' });
    const body = await response.json();
    if (!response.ok) throw new Error(body.detail ?? `HTTP ${response.status}`);
    return body as PerformanceBundle;
  });
}

export function prefetchPerformanceBundle(account: string): void {
  void loadPerformanceBundle(account).catch(() => undefined);
}
