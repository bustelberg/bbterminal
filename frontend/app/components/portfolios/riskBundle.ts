'use client';

import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { runSSE } from '../../../lib/stream';
import { readGeneration } from '../../../lib/readCache';
import { readRiskResult, seedRiskResult, useRiskResult } from './useRiskResult';
import type { RiskBundleState } from './riskBundleContext';

type Bundle = { views: Record<string, unknown> };
const pending = new Map<string, Promise<Bundle>>();
const bundleKey = (body: string) => `risk-bundle\n${body}`;

async function waitQuietly(jobId: string): Promise<void> {
  let status = 'running';
  await runSSE(`${API_URL}/api/jobs/${encodeURIComponent(jobId)}/stream`, { method: 'GET' }, (raw) => {
    const event = raw as { type?: string; status?: string };
    if (event.type === 'job' && event.status) status = event.status;
  });
  if (status !== 'done') throw new Error('Portfolio risk could not be prepared. Please try again.');
}

async function fetchBundle(body: string): Promise<Bundle> {
    const read = (prepare = true) => apiFetch(`${API_URL}/api/airs/portfolio/risk-bundle${prepare ? '' : '?prepare_if_missing=false'}`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body, noReadCache: true,
    });
    for (let attempt = 0; attempt < 3; attempt++) {
      const response = await read();
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.detail ?? 'Could not load portfolio risk.');
      if (payload.ready) return payload;
      if (response.status !== 202 || !payload.job_id) throw new Error('Could not load portfolio risk.');
      await waitQuietly(payload.job_id);
    }
    // The last job can finish on the final iteration. Re-read its published result.
    const response = await read(false);
    const payload = await response.json();
    if (!response.ok || !payload.ready) throw new Error('Risk data is still updating. Please try again shortly.');
    return payload;
}

export function loadRiskBundle(body: string, _name: string): Promise<Bundle> {
  const generation = readGeneration();
  const key = `${generation}\n${body}`;
  const existing = pending.get(key);
  if (existing) return existing;
  const request = fetchBundle(body).then((result) => {
    seedRiskResult(bundleKey(body), result, generation);
    return result;
  }).finally(() => pending.delete(key));
  pending.set(key, request);
  return request;
}

export function prefetchRiskBundle(body: string, name: string): void {
  if (readRiskResult(bundleKey(body))) return;
  void loadRiskBundle(body, name).catch(() => undefined);
}

export function useRiskBundle(body: string, name: string): RiskBundleState {
  const { data, error } = useRiskResult<Bundle>(bundleKey(body),
    () => loadRiskBundle(body, name), 'risk-bundle', 'could not load prepared portfolio risk');
  // Do not publish a mix of new and old tab results: all views arrive together.
  return { views: data?.views ?? {}, error };
}
