'use client';

import { trace } from './debugTrace';

/**
 * An in-memory cache for the FUNDAMENTAL READS — the requests behind the Fundamental modal's tabs
 * (Long Equity · Quick Valuation · Deep Valuation · Old charts) and their drill-downs.
 *
 * WHY IT EXISTS. Switching tabs inside one open modal was already free (`OwnerEarningsModal` keeps
 * a visited tab MOUNTED), but everything else about those screens re-paid full price: closing the
 * modal and opening the same holding again, opening a drill-down whose card had just fetched the
 * identical body, flipping the cadence to quarterly and back, ticking a benchmark off and on. Each
 * of those is the same request with the same answer, and the Long Equity tab alone is ~14 of them.
 *
 * ⚠ IT IS AN ALLOWLIST, NEVER "CACHE EVERY GET". Half this app is a live dashboard — usage badges,
 * run status, price coverage, the schedule stream — and serving those from memory would show a
 * finished job as still running. Only the paths below are cached, all of them derived from
 * `metric_data`, which changes ONLY by an ingest. Missing a new read endpoint here costs a
 * speed-up; it can never cost correctness. Adding a live one would.
 *
 * ⚠ EVERY SUCCESSFUL WRITE DROPS THE WHOLE CACHE, and that rule lives in `apiFetch` rather than at
 * the ingest sites. There are ~15 "Fetch financials" buttons across the cards and drill-downs, each
 * with its own reload path; a cache that depends on fifteen callers remembering to invalidate it is
 * a cache that will serve a chart saying "No revenue ingested" straight after the ingest that
 * loaded it. Anything that is not a GET (bar the read POSTs below) clears everything — a wasted
 * refetch is the worst that costs.
 */

/** A response flattened into something replayable. A `Response` body can be consumed exactly once,
 *  so what is stored is the TEXT and every hit gets a brand-new `Response` built from it. */
export type CachedRead = {
  status: number;
  statusText: string;
  contentType: string | null;
  body: string;
};

/**
 * How long an answer is trusted.
 *
 * ⚠ THE BACKSTOP, NOT THE MECHANISM. Invalidation is what keeps this correct; the TTL only bounds
 * how long a read can lag an ingest that happened somewhere this tab cannot see (the scheduler, a
 * second browser tab, a colleague). Ten minutes is longer than a modal session and shorter than
 * anyone's memory of what they were looking at.
 */
export const READ_TTL_MS = 10 * 60_000;

/**
 * The memory ceiling, over the stored bodies.
 *
 * ⚠ THESE PAYLOADS ARE NOT SMALL — one company's `/metrics` is 12,375 rows for ASML — so an
 * unbounded map here is a tab that grows for as long as it is open. Eviction is oldest-first, which
 * is right for this access pattern: the reader moves through holdings and rarely comes back past
 * the last few.
 */
export const READ_MAX_BYTES = 32 * 1024 * 1024;

/** Statuses worth keeping. ⚠ A 404 IS AN ANSWER HERE — "this ISIN has no company record", which is
 *  true for ~87% of the grid and is what the modal renders as its empty state. A 5xx or a 401 is
 *  NOT: those are transient, and remembering one would keep a page broken after the cause is gone. */
const STORABLE = new Set([200, 404]);

/** The reads. Anchored at both ends: `/fundamental-coverage` is cached, `/fundamental-coverage/
 *  ingest` is a write and must not match it. */
const CACHEABLE: RegExp[] = [
  /^\/api\/earnings\/by-isin\/[^/]+\/metrics$/,
  /^\/api\/earnings\/by-isin\/[^/]+\/growth-estimates$/,
  // The eleven derived Long Equity cards + their drill-down modals, which POST the identical body.
  /^\/api\/earnings\/[a-z0-9-]+-inputs$/,
  /^\/api\/earnings\/fundamental-coverage$/,
  /^\/api\/earnings\/fundamental-blend(-metrics|-breakdown|-matrix)?$/,
  /^\/api\/earnings\/relative-growth-breakdown$/,
  /^\/api\/earnings\/portfolio-revenue-matrix$/,
  /^\/api\/asset-pipeline\/latest-close\/isin\/[^/]+$/,
  /^\/api\/asset-pipeline\/fundamentals\/isin\/[^/]+$/,
];

/**
 * POSTs that are reads but cannot be cached — the SSE streams.
 *
 * ⚠ WITHOUT THIS THE BLEND WOULD WIPE THE CACHE EVERY TIME IT RAN. `fundamental-blend-metrics/
 * stream` is a POST, so the "any write invalidates" rule would fire on the single most expensive
 * read on the page, clearing the twelve entries the tab beside it had just filled. A stream is
 * never stored (the body is consumed incrementally by `runSSE`), only exempted.
 */
const NON_MUTATING: RegExp[] = [
  /^\/api\/earnings\/fundamental-blend-metrics\/stream$/,
  // ⚠ THE ANALYSE MODAL'S OWN READ, AND IT IS THE SCREEN THE FUNDAMENTAL MODAL OPENS FROM. An
  // ad-hoc basket cannot be named in a URL, so its analysis is POSTed (the model-portfolio twin is
  // a plain GET) — and it is re-POSTed on every benchmark switch. Treated as a write, the parent
  // screen would wipe the child's cache each time the reader touched it. Not cached either: it is
  // priced off `asset_price`, which the daily refresh moves, and the modal has its own explicit
  // re-read (`refreshSeq`) that a cache would silently answer from before the refresh.
  /^\/api\/airs\/basket\/analysis$/,
];

/** The path, without origin or query — what the allowlists match on. */
export function pathOf(url: string): string {
  try {
    return new URL(url, typeof window !== 'undefined' ? window.location.origin : 'http://x').pathname;
  } catch {
    return url;
  }
}

/** Can this request be served from, and stored in, the cache? ⚠ A non-string body (FormData, a
 *  Blob, a stream) is refused rather than keyed on `[object Object]`, which would collide two
 *  different uploads into one answer. */
export function isCacheableRead(method: string, url: string, body?: BodyInit | null): boolean {
  if (method !== 'GET' && method !== 'POST') return false;
  if (body != null && typeof body !== 'string') return false;
  const p = pathOf(url);
  return CACHEABLE.some((re) => re.test(p));
}

/** Does this request change server state — i.e. must every cached read be dropped after it? */
export function isMutation(method: string, url: string): boolean {
  if (method === 'GET' || method === 'HEAD') return false;
  const p = pathOf(url);
  if (NON_MUTATING.some((re) => re.test(p))) return false;
  return !(method === 'POST' && CACHEABLE.some((re) => re.test(p)));
}

/**
 * The identity of a request.
 *
 * ⚠ THE BODY IS PART OF IT, AND IT IS THE WHOLE POINT for the `*-inputs` endpoints: every card
 * POSTs to the same URL and the holdings/universe/cadence in the body is the only thing telling
 * `margin-inputs` for this book from `margin-inputs` for the S&P.
 *
 * ⚠ SO IS THE "view as user" PREVIEW. Role-filtered endpoints answer an admin and a previewed user
 * differently, and a cache that ignored the header would serve one to the other — the exact bug
 * `X-View-As` was added to prevent, replayed from memory.
 */
export function readKey(method: string, url: string, body: BodyInit | null | undefined,
                        viewAsUser: boolean): string {
  return `${method}\n${url}\n${viewAsUser ? 'as-user' : 'as-self'}\n${typeof body === 'string' ? body : ''}`;
}

type Entry = { at: number; bytes: number; read: Promise<CachedRead> };

const store = new Map<string, Entry>();
let storedBytes = 0;
/** Bumped by every invalidation. Anything holding derived state keyed off this cache — see
 *  `blendMetrics`, whose blend arrives over SSE and so cannot live in the map — reads it to know
 *  its own copy is stale. */
let generation = 0;

export function readGeneration(): number {
  return generation;
}

/**
 * A cached (or in-flight) answer, or null.
 *
 * ⚠ IT RETURNS THE PROMISE, NOT THE VALUE, so twelve cards firing the same request in the same
 * render share ONE fetch instead of twelve. That is the dedupe the Long Equity tab needed most:
 * every card and its drill-down ask for the same benchmark body at the same moment.
 */
export function getRead(key: string): { read: Promise<CachedRead>; ageMs: number } | null {
  const e = store.get(key);
  if (!e) return null;
  const ageMs = Date.now() - e.at;
  if (ageMs > READ_TTL_MS) {
    store.delete(key);
    storedBytes -= e.bytes;
    return null;
  }
  return { read: e.read, ageMs };
}

/** Remember an in-flight read. Accounting happens when it settles — until then the entry exists
 *  purely so concurrent callers share it. */
export function putRead(key: string, read: Promise<CachedRead>): void {
  const gen = generation;
  const entry: Entry = { at: Date.now(), bytes: 0, read };
  store.set(key, entry);
  void read.then(
    (v) => {
      // ⚠ AN INVALIDATION THAT LANDED WHILE THIS WAS IN FLIGHT MUST WIN. Otherwise an ingest that
      // finishes mid-read is undone by the read: the pre-ingest answer arrives afterwards and
      // installs itself as current.
      if (gen !== generation || store.get(key) !== entry) return;
      if (!STORABLE.has(v.status)) { store.delete(key); return; }
      entry.bytes = v.body.length * 2;        // JS strings are UTF-16
      storedBytes += entry.bytes;
      evict(key);
    },
    () => { if (store.get(key) === entry) store.delete(key); },
  );
}

/** Oldest-first until we are back under the ceiling. Never the entry just stored — evicting the
 *  thing we are about to hand out would make a single oversized payload uncacheable AND uncounted. */
function evict(keep: string): void {
  for (const [k, e] of store) {
    if (storedBytes <= READ_MAX_BYTES) return;
    if (k === keep) continue;
    store.delete(k);
    storedBytes -= e.bytes;
  }
}

export function dropRead(key: string): void {
  const e = store.get(key);
  if (!e) return;
  store.delete(key);
  storedBytes -= e.bytes;
}

/**
 * Forget everything.
 *
 * ⚠ TRACED, ALWAYS. A cache is invisible when it works and invisible when it is wrong, so the one
 * line that says "these 14 answers were dropped because you ingested Fortinet" is the only way the
 * next reader can tell a stale chart from a fresh one. `reason` is not optional for that reason.
 */
export function invalidateReadCache(reason: string): void {
  const n = store.size;
  store.clear();
  storedBytes = 0;
  generation += 1;
  if (n) trace('cache', `dropped ${n} cached read${n === 1 ? '' : 's'} — ${reason}`);
}

/** For the console: `bb.readCacheStats()` is not wired up, but a panel or a test can ask. */
export function readCacheStats(): { entries: number; bytes: number; generation: number } {
  return { entries: store.size, bytes: storedBytes, generation };
}
