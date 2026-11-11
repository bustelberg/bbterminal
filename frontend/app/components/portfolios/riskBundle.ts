'use client';

import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { useCallback, useEffect, useState } from 'react';
import { readGeneration } from '../../../lib/readCache';
import { seedRiskResult, useRiskResult } from './useRiskResult';
import type { RiskBundleState } from './riskBundleContext';

type Bundle = { views: Record<string, unknown>; ready?: boolean; available?: boolean; detail?: string };
const pending = new Map<string, Promise<Bundle>>();
const bundleKey = (body: string) => `risk-bundle\n${body}`;

async function fetchBundle(body: string): Promise<Bundle> {
  const response = await apiFetch(`${API_URL}/api/airs/portfolio/risk-bundle`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body, noReadCache: true,
  });
  const payload = await response.json();
  if (!response.ok) throw new Error(payload.detail ?? 'Could not load portfolio risk.');
  if (!payload.ready && response.status !== 202) throw new Error(payload.detail ?? 'Could not load portfolio risk.');
  if (!payload.ready) return { ...payload, views: payload.views ?? {}, available: false };
  return payload;
}

export function loadRiskBundle(body: string): Promise<Bundle> {
  const generation = readGeneration();
  const key = `${generation}\n${body}`;
  const existing = pending.get(key);
  if (existing) return existing;
  const request = fetchBundle(body).then((result) => {
    if (result.ready !== false) seedRiskResult(bundleKey(body), result, generation);
    return result;
  }).finally(() => pending.delete(key));
  pending.set(key, request);
  return request;
}

export function useRiskBundle(body: string): RiskBundleState {
  const [revision, setRevision] = useState(0);
  const reload = useCallback(() => setRevision((value) => value + 1), []);
  const key = revision ? `${bundleKey(body)}\nreload:${revision}` : bundleKey(body);
  const { data, error } = useRiskResult<Bundle>(key,
    () => loadRiskBundle(body), 'risk-bundle', 'could not load portfolio risk');
  useEffect(() => {
    if (data?.ready !== false) return;
    const timer = setTimeout(reload, 10_000);
    return () => clearTimeout(timer);
  }, [data, reload]);
  return { views: data?.views ?? {}, error: error ?? data?.detail ?? null, reload };
}
