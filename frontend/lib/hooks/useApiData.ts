'use client';

/**
 * Generic data-fetching hook for component-local GETs.
 *
 * Replaces the copy-pasted `setLoading(true) / try fetch / setError /
 * finally setLoading(false)` block that was hand-rolled in ~20 places.
 * Standardizes on a single fetch path — `apiFetch` — so the request
 * carries the Supabase JWT (harmless on public GETs, required on
 * admin-gated ones).
 *
 *   const { data, loading, error, reload } = useApiData<Snapshot>(
 *     `/api/momentum/current-picks/${id}`,
 *   );
 *
 * Refetches whenever `path` changes (encode query params in the path),
 * when `reload()` is called, or when `enabled` flips true. Pass
 * `enabled: false` to defer (e.g. until a panel is expanded). Pass
 * `fallbackData` for the call sites that intentionally swallow failures
 * and just render empty — `error` then stays null and `data` becomes the
 * fallback. Pass `transform` to reshape the parsed JSON before storing.
 *
 * NOT for: cached endpoints shared across components (use the named
 * hooks in `apiData.ts`), optimistic-update lists (need a `setData`),
 * polled/awaited imperative loaders, or SSE streams.
 */

import { useCallback, useEffect, useRef, useState } from 'react';
import { API_URL } from '../apiUrl';
import { apiFetch } from '../apiFetch';

/** Resolve a hook `path` to a full URL: absolute http(s) URLs pass
 * through unchanged; everything else is treated as a backend path and
 * prefixed with `API_URL`. */
export function resolveApiUrl(path: string): string {
  return /^https?:\/\//.test(path) ? path : `${API_URL}${path}`;
}

export type UseApiDataResult<T> = {
  data: T | null;
  loading: boolean;
  error: string | null;
  reload: () => void;
};

export type UseApiDataOptions<T, R> = {
  /** When false, no request is issued and `loading` stays false. */
  enabled?: boolean;
  /** Map the parsed JSON (`R`) into the stored shape (`T`). */
  transform?: (raw: R) => T;
  /** Stored instead of surfacing an error on failure — for call sites
   * that intentionally render empty rather than show an error. */
  fallbackData?: T;
};

export function useApiData<T, R = T>(
  path: string | null | undefined,
  opts: UseApiDataOptions<T, R> = {},
): UseApiDataResult<T> {
  const { enabled = true, transform, fallbackData } = opts;
  const active = enabled && !!path;

  const [data, setData] = useState<T | null>(fallbackData ?? null);
  const [loading, setLoading] = useState<boolean>(active);
  const [error, setError] = useState<string | null>(null);
  const [tick, setTick] = useState(0);

  // Hold the latest transform/fallback in refs so passing them inline
  // (new identity each render) doesn't re-trigger the fetch effect —
  // only path/enabled/tick do. Synced in an effect, never written during
  // render (react-hooks/refs). `useRef(x)` seeds the correct initial
  // value, so the first fetch already reads the right transform/fallback.
  const transformRef = useRef(transform);
  const fallbackRef = useRef(fallbackData);
  useEffect(() => {
    transformRef.current = transform;
    fallbackRef.current = fallbackData;
  });

  useEffect(() => {
    if (!active || !path) {
      // Reset loading if the hook is disabled mid-flight (e.g. a panel
      // collapses): the prior fetch's `.finally` is cancelled, so nothing
      // else clears it.
      // eslint-disable-next-line react-hooks/set-state-in-effect
      setLoading(false);
      return;
    }
    let cancelled = false;
    setLoading(true);
    setError(null);
    apiFetch(resolveApiUrl(path))
      .then(async (r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        const raw = (await r.json()) as R;
        return transformRef.current ? transformRef.current(raw) : (raw as unknown as T);
      })
      .then((value) => { if (!cancelled) setData(value); })
      .catch((e) => {
        if (cancelled) return;
        if (fallbackRef.current !== undefined) setData(fallbackRef.current as T);
        else setError(e instanceof Error ? e.message : String(e));
      })
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [path, active, tick]);

  const reload = useCallback(() => setTick((n) => n + 1), []);
  return { data, loading, error, reload };
}
