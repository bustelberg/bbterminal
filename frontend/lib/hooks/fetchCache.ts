/**
 * Module-level fetch cache + in-flight request coalescer used by the
 * data-fetching hooks under `lib/hooks/`. Solves three problems:
 *
 *   1. Re-mount thrash. When a user navigates between pages and back,
 *      previously-fetched data renders instantly (within TTL) instead
 *      of refetching every time.
 *   2. Duplicate parallel fetches. Two components mounting at the same
 *      moment that both want `/api/universe-templates` would otherwise
 *      issue two identical requests; we collapse them to one.
 *   3. Synchronous initial-render. Hooks can peek the cache during
 *      `useState`'s lazy initializer to render with data on the first
 *      paint when a hit exists — no "Loading…" flash on revisit.
 *
 * Pattern previously implemented inline in `AddScheduledStrategyForm`;
 * lifted here so every endpoint hook benefits from the same behavior.
 */

const DEFAULT_TTL_MS = 5 * 60 * 1000; // 5 minutes

type CacheEntry<T> = { value: T; expiresAt: number };

// Per-key state. Two maps because we need to distinguish "have data"
// from "currently fetching" (the latter is a Promise we want to share
// across concurrent callers, not a value).
const _values = new Map<string, CacheEntry<unknown>>();
const _inflight = new Map<string, Promise<unknown>>();

/** Returns the cached value if still fresh, otherwise null. Call this
 * from `useState` lazy initializers to render with data on first paint
 * when there's a cache hit. */
export function peekCache<T>(key: string): T | null {
  const cached = _values.get(key);
  if (cached && cached.expiresAt > Date.now()) return cached.value as T;
  return null;
}

/** Fetch-with-cache. Returns the cached value if fresh, otherwise
 * invokes `fetcher` and stores the result for `ttlMs`. Concurrent
 * callers for the same key share the same in-flight promise. */
export function getCachedOrFetch<T>(
  key: string,
  fetcher: () => Promise<T>,
  ttlMs: number = DEFAULT_TTL_MS,
): Promise<T> {
  const cached = _values.get(key);
  if (cached && cached.expiresAt > Date.now()) {
    return Promise.resolve(cached.value as T);
  }
  const inflight = _inflight.get(key);
  if (inflight) return inflight as Promise<T>;
  const p = fetcher()
    .then((value) => {
      _values.set(key, { value, expiresAt: Date.now() + ttlMs });
      _inflight.delete(key);
      return value;
    })
    .catch((e) => {
      _inflight.delete(key);
      throw e;
    });
  _inflight.set(key, p);
  return p;
}

/** Drop the cached value for `key`, forcing the next call to refetch.
 * Use after a mutation (e.g. user just saved a backtest → invalidate
 * `momentum-backtests` so the saved-runs list is rebuilt fresh). */
export function invalidateCache(key: string): void {
  _values.delete(key);
}
