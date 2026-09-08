/**
 * Delete what the account switcher left behind in every browser that ever used it.
 *
 * ⚠⚠ REMOVING A FEATURE DOES NOT REMOVE ITS DATA, AND HERE THE DATA IS THE HAZARD. The switcher
 * (deleted 2026-09-08 along with `POST /api/auth/impersonate`) wrote a `refresh_token` per account
 * into `localStorage` under `bbterminal_sessions`. Ship the deletion alone and every one of those
 * tokens simply stays there — unreadable by any code we still ship, invisible in the UI, valid
 * until it is revoked or expires, and readable by anything with access to the origin. The point of
 * removing the feature was the tokens; the code was only how they got there.
 *
 * ⚠ IT RUNS ON EVERY LOAD, NOT ONCE BEHIND A FLAG. A "have I cleaned up yet" marker is itself a
 * key in the same storage, and it would be wrong for exactly the browsers that matter — the one
 * that has not been opened since the switcher worked. Two `removeItem` calls against absent keys
 * cost nothing measurable, so there is no reason to be clever about when.
 *
 * ⚠ SAFE TO DELETE once every browser that used the switcher has loaded the app at least once.
 * There is no way to know that from here, so it should outlive one obvious round of use rather
 * than be removed at the first tidy-up. Nothing else imports these keys — that is the point.
 */

/** Refresh tokens, one per account the switcher had signed in as. */
const LEGACY_SESSIONS_KEY = 'bbterminal_sessions';
/** The "an admin is mid-switch" marker that decided whether the above survived. */
const LEGACY_IMPERSONATING_KEY = 'bbterminal_impersonating';

export const LEGACY_KEYS = [LEGACY_SESSIONS_KEY, LEGACY_IMPERSONATING_KEY] as const;

export function purgeLegacySessions(): void {
  if (typeof window === 'undefined') return;
  try {
    for (const key of LEGACY_KEYS) localStorage.removeItem(key);
  } catch (e) {
    // ⚠ A BLOCKED STORAGE IS NOT A REASON TO FAIL A PAGE LOAD. It also means there is nothing
    // stored to clean up: the same exception blocked the write that would have put it there.
    console.warn('[sessions] could not purge the retired session store:', e);
  }
}
