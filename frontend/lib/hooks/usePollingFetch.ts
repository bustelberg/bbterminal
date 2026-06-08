import { useEffect, useState } from 'react';
import { apiFetch } from '../apiFetch';

/**
 * Fetch a JSON endpoint on mount and re-fetch on an interval, with the
 * cancelled-flag + cleanup boilerplate handled once. For live status
 * views that need fresh data every few seconds (not the cached
 * `apiData` hooks, which are for stable, shared data with a 5-min TTL).
 *
 *   const { data, error } = usePollingFetch<Foo>(`${API_URL}/api/foo`, 3000);
 *
 * Returns the latest successfully-decoded payload (null until the first
 * success) and the last error (cleared on the next success). A non-2xx
 * response is ignored (keeps the prior data) rather than surfaced as an
 * error. Pass `url = null` to disable.
 */
export function usePollingFetch<T>(
  url: string | null,
  intervalMs: number,
): { data: T | null; error: string | null } {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!url) return;
    let cancelled = false;
    const poll = async () => {
      try {
        const r = await apiFetch(url);
        if (cancelled || !r.ok) return;
        const json = (await r.json()) as T;
        if (!cancelled) { setData(json); setError(null); }
      } catch (e) {
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      }
    };
    void poll();
    const id = window.setInterval(poll, intervalMs);
    return () => { cancelled = true; window.clearInterval(id); };
  }, [url, intervalMs]);

  return { data, error };
}
