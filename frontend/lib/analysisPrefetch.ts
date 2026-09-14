/**
 * Short-lived client-side request sharing for the Analyse modal.
 *
 * The modal's API call is deliberately one payload, so starting it on pointer/focus intent makes
 * the click feel instant without changing the server response or its freshness rules. Results are
 * only retained briefly; the server-side fingerprinted cache remains the source of truth.
 */
type Loader<T> = () => Promise<T>;

const TTL_MS = 30_000;
const pending = new Map<string, Promise<unknown>>();
const completed = new Map<string, { value: unknown; expiresAt: number }>();

function shared<T>(key: string, load: Loader<T>): Promise<T> {
  const cached = completed.get(key);
  if (cached && cached.expiresAt > Date.now()) return Promise.resolve(cached.value as T);
  if (cached) completed.delete(key);

  const running = pending.get(key);
  if (running) return running as Promise<T>;

  const promise = load()
    .then((value) => {
      completed.set(key, { value, expiresAt: Date.now() + TTL_MS });
      return value;
    })
    .finally(() => pending.delete(key));
  pending.set(key, promise);
  return promise;
}

export function prefetchAnalysis<T>(key: string, load: Loader<T>): void {
  void shared(key, load).catch(() => undefined);
}

export function loadPrefetchedAnalysis<T>(key: string, load: Loader<T>): Promise<T> {
  return shared(key, load);
}
