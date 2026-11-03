'use client';

import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { watchJob } from '../../../lib/stores/jobs';
import { useRiskResult } from './useRiskResult';
import type { RiskBundleState } from './riskBundleContext';

export function useRiskBundle(body: string, name: string): RiskBundleState {
  const { data, error } = useRiskResult<{ views: Record<string, unknown> }>(`risk-bundle\n${body}`, async () => {
    const read = () => apiFetch(`${API_URL}/api/airs/portfolio/risk-bundle`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body, noReadCache: true,
    });
    let response = await read();
    let payload = await response.json();
    if (!response.ok) throw new Error(payload.detail ?? 'Could not load portfolio risk.');
    if (response.status === 202) {
      const job = await watchJob(payload.job_id, name);
      if (job.status !== 'done') throw new Error('Portfolio risk could not be prepared. Please try again.');
      response = await read(); payload = await response.json();
      if (!response.ok || !payload.ready) throw new Error('Portfolio data changed. Please reopen Risk to try again.');
    }
    return payload;
  }, 'risk-bundle', 'could not load prepared portfolio risk');
  // Do not publish a mix of new and old tab results: all views arrive together.
  return { views: data?.views ?? {}, error };
}
