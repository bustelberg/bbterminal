import { createStore } from './store';
import { apiFetch } from './apiFetch';

export type LoadingItem = {
  id: number;
  label: string;
  startedAt: number;
};

type LoadingState = { items: LoadingItem[] };

export const loadingStore = createStore<LoadingState>({ items: [] });

let nextId = 1;

/**
 * Wrap a promise so it shows up in the global loading panel with a label.
 * Use anywhere a fetch / async operation should surface progress to the user.
 *
 *   const data = await track('Loading FX coverage', fetch(url).then(r => r.json()));
 */
export function track<T>(label: string, p: Promise<T>): Promise<T> {
  const id = nextId++;
  const item: LoadingItem = { id, label, startedAt: Date.now() };
  loadingStore.set((s) => ({ items: [...s.items, item] }));
  return p.finally(() => {
    loadingStore.set((s) => ({ items: s.items.filter((i) => i.id !== id) }));
  });
}

/**
 * Convenience for `track('label', fetch(url, init))` — returns the raw Response
 * so callers can still call `.json()` / `.ok` etc.
 */
export function trackedFetch(label: string, input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
  const urlStr = typeof input === 'string' ? input : input.toString();
  return track(label, apiFetch(urlStr, init));
}
