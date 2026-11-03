'use client';

import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { loadPrefetchedAnalysis, readPrefetchedAnalysis } from '../../../lib/analysisPrefetch';
import { readGeneration } from '../../../lib/readCache';
import type { MonthlyReport } from './monthlyReport';

export const currentPerformanceMonth = () => new Intl.DateTimeFormat('sv-SE', {
  timeZone: 'Europe/Amsterdam', year: 'numeric', month: '2-digit',
}).format(new Date());
const key = (account: string, month: string, revision: number) => `performance-tiles|${account}|${month}|${readGeneration()}|${revision}`;

export function readPerformanceTiles(account: string, month: string, revision = 0): MonthlyReport | null {
  return readPrefetchedAnalysis<MonthlyReport>(key(account, month, revision)) ?? null;
}

export function loadPerformanceTiles(account: string, month: string, revision = 0): Promise<MonthlyReport> {
  return loadPrefetchedAnalysis(key(account, month, revision), async () => {
    const response = await apiFetch(`${API_URL}/api/airs/accounts/${encodeURIComponent(account)}/monthly-report?month=${month}&include_details=false`,
      { noReadCache: true, cache: 'no-store' });
    const body = await response.json();
    if (!response.ok) throw new Error(body.detail ?? `HTTP ${response.status}`);
    return body as MonthlyReport;
  });
}

export function prefetchPerformanceTiles(account: string): void {
  void loadPerformanceTiles(account, currentPerformanceMonth()).catch(() => undefined);
}
