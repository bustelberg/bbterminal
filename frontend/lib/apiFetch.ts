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

export async function apiFetch(
  url: string,
  init: RequestInit = {},
): Promise<Response> {
  const token = await _getToken();
  const headers = new Headers(init.headers || {});
  if (token && !headers.has('authorization') && !headers.has('Authorization')) {
    headers.set('Authorization', `Bearer ${token}`);
  }
  return fetch(url, { ...init, headers });
}
