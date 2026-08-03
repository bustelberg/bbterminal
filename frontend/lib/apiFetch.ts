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
 */

import { traceRequest } from './debugTrace';
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

/** Invalidate the cached token — useful right after sign-out so the
 * next call doesn't reuse a dead token. */
export function clearApiFetchTokenCache(): void {
  _cachedToken = null;
  _cachedTokenExpiresAt = 0;
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

export async function apiFetch(
  url: string,
  init: RequestInit = {},
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
  const done = traceRequest(init.method ?? 'GET', url);
  try {
    const resp = await fetch(url, { ...init, headers });
    done(resp);
    return resp;
  } catch (e) {
    // A network failure never reaches the caller's `resp.ok` check — it throws, and without this
    // the console shows nothing at all for a request that was made and died.
    done(null, e);
    throw e;
  }
}
