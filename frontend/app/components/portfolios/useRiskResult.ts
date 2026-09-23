'use client';

import { useEffect, useState } from 'react';
import { traceError } from '../../../lib/debugTrace';
import { readGeneration } from '../../../lib/readCache';

type Loaded<T> = { key: string; value: T };
type Cached = { generation: number; value: unknown };

const results = new Map<string, Cached>();

function cached<T>(key: string): T | null {
  const hit = results.get(key);
  if (!hit) return null;
  if (hit.generation !== readGeneration()) {
    results.delete(key);
    return null;
  }
  return hit.value as T;
}

/**
 * Parsed Risk answers, keyed by the exact URL and POST body.
 *
 * `apiFetch` already avoids the network request, but replaying a cached Response still resolves on
 * a later microtask. Clearing component state first therefore flashes the full loading screen.
 * Keeping the parsed answer makes a previously visited basis/view available during render itself.
 * The cache follows `readCache`'s generation, so any ingest/refresh invalidates both layers.
 */
export function useRiskResult<T>(key: string, load: () => Promise<T>, traceScope: string,
  traceMessage: string): { data: T | null; error: string | null } {
  const [loaded, setLoaded] = useState<Loaded<T> | null>(null);
  const [failure, setFailure] = useState<{ key: string; message: string } | null>(null);
  const hit = cached<T>(key);
  const data = hit ?? (loaded?.key === key ? loaded.value : null);
  const error = failure?.key === key ? failure.message : null;

  useEffect(() => {
    if (cached<T>(key) != null) return;
    let cancelled = false;
    void load().then((value) => {
      if (cancelled) return;
      results.set(key, { generation: readGeneration(), value });
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
  }, [key]);

  return { data, error };
}

export function riskRequestKey(url: string, body: string): string {
  return `${url}\n${body}`;
}
