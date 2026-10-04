import type { Session } from '@supabase/supabase-js'

/**
 * A one-navigation bridge between the email confirmation page and `/set-password`.
 *
 * `verifyOtp` gives us a fully usable session.  The normal SSR client persists it in cookies,
 * but a client-side route transition can mount `/set-password` before that cookie is visible to
 * its newly initialised client.  That made a freshly verified person look signed in at first and
 * then fail `updateUser` with "Auth session missing".  Keep the credentials only in this tab's
 * sessionStorage, consume them on the very next page, and let `setSession` write the ordinary
 * Supabase cookies again.  Nothing is put in the URL or retained after the hand-off.
 */
const KEY = 'bbterminal:pending-password-session'

type StoredSession = Pick<Session, 'access_token' | 'refresh_token'>

export function storePendingPasswordSession(session: Session | null): void {
  if (!session || typeof window === 'undefined') return
  const stored: StoredSession = {
    access_token: session.access_token,
    refresh_token: session.refresh_token,
  }
  window.sessionStorage.setItem(KEY, JSON.stringify(stored))
}

export function takePendingPasswordSession(): StoredSession | null {
  if (typeof window === 'undefined') return null
  const raw = window.sessionStorage.getItem(KEY)
  // One use, even if a malformed old value was left by a previous build.
  window.sessionStorage.removeItem(KEY)
  if (!raw) return null
  try {
    const stored: unknown = JSON.parse(raw)
    if (
      typeof stored === 'object' && stored !== null
      && typeof (stored as StoredSession).access_token === 'string'
      && typeof (stored as StoredSession).refresh_token === 'string'
    ) {
      return stored as StoredSession
    }
  } catch {
    // Treat a corrupt browser value exactly like no bridge at all.
  }
  return null
}
