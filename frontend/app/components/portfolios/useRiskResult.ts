'use client';

import { useContext, useEffect, useState } from 'react';
import { RiskBundleContext } from './riskBundleContext';
import { traceError } from '../../../lib/debugTrace';
import { readGeneration, READ_TTL_MS } from '../../../lib/readCache';

type Loaded<T> = { key: string; value: T };
type Cached = { generation: number; value: unknown; expiresAt: number };

const results = new Map<string, Cached>();

export function seedRiskResult(key: string, value: unknown, generation: number): void {
  if (generation === readGeneration()) results.set(key, { generation, value, expiresAt: Date.now() + READ_TTL_MS });
}

function cached<T>(key: string): T | null {
  const hit = results.get(key);
  if (!hit) return null;
  if (hit.generation !== readGeneration() || hit.expiresAt <= Date.now()) {
    results.delete(key);
    return null;
  }
  return hit.value as T;
}

export const readRiskResult = cached;

/**
 * Parsed Risk answers, keyed by the exact URL and POST body.
 *
 * `apiFetch` already avoids the network request, but replaying a cached Response still resolves on
 * a later microtask. Clearing component state first therefore flashes the full loading screen.
 * Keeping the parsed answer makes a previously visited basis/view available during render itself.
 * The cache follows `readCache`'s generation, so any ingest/refresh invalidates both layers.
 */
export function useRiskResult<T>(key: string, load: () => Promise<T>, traceScope: string,
  traceMessage: string, enabled = true): { data: T | null; error: string | null } {
  const bundle = useContext(RiskBundleContext);
  const interactive = enabled && !bundle;
  const [loaded, setLoaded] = useState<Loaded<T> | null>(null);
  const [failure, setFailure] = useState<{ key: string; message: string } | null>(null);
  const [retry, setRetry] = useState(0);
  const hit = cached<T>(key);
  const data = bundle ? (bundle.views[traceScope] as T | undefined) ?? null : hit ?? (loaded?.key === key ? loaded.value : null);
  const error = bundle ? bundle.error : failure?.key === key ? failure.message : null;

  useEffect(() => {
    const answer = data as { available?: boolean; reason?: string; history_issues?: Array<{ kind?: string }> } | null;
    const benchmarkMissing = /benchmark.*history|history.*benchmark/i.test(answer?.reason ?? '')
      || answer?.history_issues?.some(issue => issue.kind === 'benchmark');
    if (!interactive || answer?.available !== false || !benchmarkMissing) return;
    const timer = setTimeout(() => {
      results.delete(key);
      setRetry(value => value + 1);
    }, 60_000);
    return () => clearTimeout(timer);
  }, [data, interactive, key, retry]);

  useEffect(() => {
    if (!interactive) return;
    if (cached<T>(key) != null) return;
    let cancelled = false;
    void load().then((value) => {
      if (cancelled) return;
      const answer = value as { available?: boolean };
      if (answer?.available !== false) seedRiskResult(key, value, readGeneration());
      setFailure(null);
      setLoaded({ key, value });
    }).catch((e: unknown) => {
      traceError(traceScope, traceMessage, e);
      if (!cancelled) {
        setFailure({ key, message: e instanceof Error ? e.message : String(e) });
      }
    });
    return () => { cancelled = true; };
    // `key` is the complete request identity, including URL and body. A new loader with the same
    // key is the same read; depending on its render-time function identity would refetch forever.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key, interactive, retry]);

  return { data, error };
}

export function riskRequestKey(url: string, body: string): string {
  return `${url}\n${body}`;
}
