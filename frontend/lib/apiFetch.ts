'use client';

/**
 * Drop-in `fetch` wrapper that auto-attaches the Supabase session JWT
 * as `Authorization: Bearer <token>`. Use it for every call to the
 * backend (`/api/...`) — read or write — so admins glide through the
 * backend's admin-only-mutations middleware.
 *
 * Usage:
 *   await apiFetch(`${API_URL}/api/companies`, { method: 'POST', ... });
 *
 * For SSE responses (text/event-stream), keep using `apiFetch` — it
 * just forwards the response object, the caller reads `resp.body` as
 * usual.
 *
 * When the user isn't signed in (no session), no Authorization header
 * is attached and the request proceeds. Public endpoints + the login
 * flow still work; mutation endpoints will respond 401 which the UI
 * surfaces normally.
 *
 * ⚠ IT ALSO SERVES THE FUNDAMENTAL READS FROM MEMORY — an ALLOWLIST of
 * paths (`lib/readCache.ts`), never "every GET", because most of this
 * app is a live dashboard. Callers change nothing: a hit is replayed as
 * a normal `Response`. Any successful write invalidates the lot. Pass
 * `noReadCache: true` to force one call to the network.
 */

import { trace, traceRequest } from './debugTrace';
import {
  getRead, invalidateReadCache, isCacheableRead, isMutation, pathOf, putRead, readKey,
  type CachedRead,
} from './readCache';
import { createClient } from './supabase/client';

// Cache the access token between calls. Supabase rotates it ~hourly
// and we don't want to hit `getSession()` on every fetch (which is
// fast but does a localStorage read + JWT parse each time).
let _cachedToken: string | null = null;
let _cachedTokenExpiresAt = 0;

async function _getToken(): Promise<string | null> {
  const now = Date.now();
  // Refresh 60s before expiry — gives the Supabase client time to
  // rotate the token transparently.
  if (_cachedToken && now < _cachedTokenExpiresAt - 60_000) {
    return _cachedToken;
  }
  try {
    const supabase = createClient();
    const { data: { session } } = await supabase.auth.getSession();
    if (!session?.access_token) {
      _cachedToken = null;
      _cachedTokenExpiresAt = 0;
      return null;
    }
    _cachedToken = session.access_token;
    // `expires_at` is a Unix timestamp in seconds, possibly null.
    const exp = (session.expires_at ?? 0) * 1000;
    _cachedTokenExpiresAt = exp > 0 ? exp : now + 30 * 60_000; // fallback 30min
    return _cachedToken;
  } catch {
    return null;
  }
}

/** True when the admin "view as regular user" preview is active (the
 * `view_as=user` cookie set by the Sidebar toggle). The cookie lives on the
 * frontend origin and isn't sent cross-origin to the backend, so we forward
 * it as an `X-View-As` header instead — letting role-filtered endpoints
 * (e.g. /scheduled-strategies) preview the genuine user view. */
function _isViewingAsUser(): boolean {
  return typeof document !== 'undefined'
    && document.cookie.split('; ').some((c) => c.startsWith('view_as=user'));
}

/** `fetch`'s options, plus the one knob this wrapper adds. */
export type ApiFetchInit = RequestInit & {
  /** Go to the network even for a cached read — the escape hatch. A cache with no way to bypass it
   *  is a cache you cannot debug, and "is this stale or is the server wrong?" is the first question
   *  anyone asks of a number that looks off. */
  noReadCache?: boolean;
};

export async function apiFetch(
  url: string,
  init: ApiFetchInit = {},
): Promise<Response> {
  const token = await _getToken();
  const headers = new Headers(init.headers || {});
  if (token && !headers.has('authorization') && !headers.has('Authorization')) {
    headers.set('Authorization', `Bearer ${token}`);
  }
  if (_isViewingAsUser() && !headers.has('x-view-as')) {
    headers.set('X-View-As', 'user');
  }
  // ⚠ TRACED FROM HERE BECAUSE THIS IS THE ONE CHOKEPOINT. Every backend call in the app goes
  // through this function, so one line here logs method, path, status, duration and payload size
  // for every panel — including the calls nobody thought to instrument, which are exactly the
  // ones that go wrong in production. It never touches the body (see `debugTrace`): the size
  // comes from `content-length`, and cloning to count rows would double the memory of every
  // payload on the page.
  //
  // ⚠ THE TOKEN IS NEVER LOGGED, here or anywhere. It is a bearer credential; a console trace is
  // pasted into chats and screenshots.
  const method = (init.method ?? 'GET').toUpperCase();
  // ⚠ THE CACHE SITS HERE, AT THE CHOKEPOINT, FOR THE SAME REASON THE TRACE DOES: every card,
  // drill-down and tab in the Fundamental modal already calls `apiFetch`, so there is nothing to
  // remember at ~40 call sites and nothing for a new one to forget. What may be cached is an
  // explicit allowlist in `readCache`; everything else falls straight through.
  if (!init.noReadCache && isCacheableRead(method, url, init.body)) {
    return _servedFromCache(method, url, init, headers);
  }
  const done = traceRequest(method, url);
  try {
    const resp = await fetch(url, { ...init, headers });
    done(resp);
    // ⚠ ANY SUCCESSFUL WRITE DROPS EVERY CACHED READ. The fundamental reads are derived from
    // `metric_data`, which changes only by an ingest — and an ingest is a POST from one of ~15
    // "Fetch financials" buttons. Invalidating from here rather than from each of them is what
    // makes it impossible for a new button to ship without it.
    if (resp.ok && isMutation(method, url)) invalidateReadCache(`${method} ${pathOf(url)}`);
    return resp;
  } catch (e) {
    // A network failure never reaches the caller's `resp.ok` check — it throws, and without this
    // the console shows nothing at all for a request that was made and died.
    done(null, e);
    throw e;
  }
}

/**
 * A cacheable read: from memory when we have it, otherwise fetched once and shared.
 *
 * ⚠ THE CALLER'S `signal` IS NOT PASSED TO THE FETCH, DELIBERATELY. The response belongs to
 * everyone waiting on it, so one component unmounting must not cancel the request the other eleven
 * are still waiting for. The aborting caller still gets its `AbortError` (below) — it simply stops
 * being the reason the request lives or dies, and the answer it walked away from lands in the cache
 * for whoever asks next.
 */
async function _servedFromCache(
  method: string, url: string, init: ApiFetchInit, headers: Headers,
): Promise<Response> {
  const key = readKey(method, url, init.body, _isViewingAsUser());
  const hit = getRead(key);
  if (hit) {
    // ⚠ SAID OUT LOUD. A request served from memory makes no network entry at all, so without this
    // line the console shows a panel that rendered data it never asked for — which is exactly how
    // a stale answer stays invisible. Same shape as `traceRequest`'s line, with the age instead of
    // a duration.
    trace('api', `${method} ${pathOf(url)} → from memory (${Math.round(hit.ageMs / 1000)}s old, no request)`);
    return _replay(await _withAbort(hit.read, init.signal));
  }
  const done = traceRequest(method, url);
  const read: Promise<CachedRead> = (async () => {
    let resp: Response;
    try {
      resp = await fetch(url, { ...init, headers, signal: undefined });
    } catch (e) {
      done(null, e);
      throw e;
    }
    done(resp);
    return {
      status: resp.status,
      statusText: resp.statusText,
      contentType: resp.headers.get('content-type'),
      body: await resp.text(),
    };
  })();
  putRead(key, read);
  return _replay(await _withAbort(read, init.signal));
}

/** Reject this caller when its signal aborts, without touching the shared read. */
function _withAbort<T>(p: Promise<T>, signal?: AbortSignal | null): Promise<T> {
  if (!signal) return p;
  if (signal.aborted) return Promise.reject(new DOMException('Aborted', 'AbortError'));
  return new Promise<T>((resolve, reject) => {
    const onAbort = () => reject(new DOMException('Aborted', 'AbortError'));
    signal.addEventListener('abort', onAbort, { once: true });
    void p.then(resolve, reject).finally(() => signal.removeEventListener('abort', onAbort));
  });
}

/** A fresh `Response` per hit — a body can only be read once, so replaying the stored text is the
 *  only way this stays a drop-in for `fetch`. */
function _replay(v: CachedRead): Response {
  const bodyless = v.status === 204 || v.status === 205 || v.status === 304;
  return new Response(bodyless ? null : v.body, {
    status: v.status,
    statusText: v.statusText,
    headers: v.contentType ? { 'content-type': v.contentType } : {},
  });
}
