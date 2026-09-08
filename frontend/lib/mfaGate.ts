/**
 * When a signed-in person still has to prove a second factor — the whole rule, as a pure function.
 *
 * ⚠⚠ IT KEYS ON `nextLevel`, WHICH IS WHAT MAKES ENFORCEMENT OPT-IN. GoTrue reports two things:
 * `currentLevel` is what this session has actually proved, `nextLevel` is the highest it COULD
 * reach — `aal2` only when the user has a verified factor. So the gate fires for exactly the
 * people who enrolled, and nobody else is locked out of an app they never set up. That is the
 * property that lets this ship before everyone has a phone in the loop; a rule written as
 * "currentLevel !== 'aal2'" would instead lock out every account with no authenticator at all.
 *
 * ⚠ `nextLevel` ALSO GOES BACK DOWN. Remove your last factor and it returns to `aal1`, so the gate
 * stops firing without anything here having to know about unenrolment.
 */

/** Where an unsatisfied session is sent. */
export const MFA_PATH = '/mfa';

/** Where somebody who has never enrolled is sent. */
export const ENROL_PATH = '/account/security';

/**
 * Nobody may use the app without an authenticator (2026-09-08, on request).
 *
 * ⚠⚠ THIS IS THE HALF THAT MAKES IT MANDATORY RATHER THAN AVAILABLE. `requiresMfa` only ever fires
 * for people who already enrolled, so on its own the whole feature is a suggestion: sign up, ignore
 * the security page, and nothing ever asks. This sends anyone without a verified factor to the
 * enrolment page and keeps them there.
 *
 * ⚠ `nextLevel !== 'aal2'` IS THE TEST FOR "HAS NO VERIFIED FACTOR", and it is exact rather than a
 * proxy: `getAuthenticatorAssuranceLevel` computes `nextLevel` by filtering the session user's
 * factors to `status === 'verified'` (checked in `@supabase/auth-js`). An ABANDONED enrolment
 * leaves an `unverified` factor, which is correctly not counted — otherwise a closed tab would
 * promote someone to "enrolled" and then strand them at a challenge no code can answer.
 *
 * ⚠ THE ENROLMENT PAGE ITSELF IS EXEMPT, or the redirect is a loop. So is `/mfa` — a session with
 * no factor has nothing to prove there, but a stale tab can still ask for it.
 */
export function requiresEnrolment(
  { nextLevel, pathname, isPublic }: Omit<MfaGateInput, 'currentLevel'>,
): boolean {
  if (pathname === ENROL_PATH || pathname.startsWith(`${ENROL_PATH}/`)) return false;
  if (pathname === MFA_PATH || pathname.startsWith(`${MFA_PATH}/`)) return false;
  if (isPublic) return false;
  // ⚠ AN UNKNOWN LEVEL DOES NOT FORCE ENROLMENT. `null` means the session could not be read, which
  // is not evidence that nobody enrolled — and being wrong here redirects a fully set-up user to a
  // page telling them to do what they have already done.
  if (nextLevel == null) return false;
  return nextLevel !== 'aal2';
}

export type AalLevel = 'aal1' | 'aal2' | null | undefined;

export type MfaGateInput = {
  /** What this session has proved. */
  currentLevel: AalLevel;
  /** The highest this user could reach — `aal2` iff they have a verified factor. */
  nextLevel: AalLevel;
  pathname: string;
  /** True for the signed-out auth flow (`/login`, `/set-password`, `/auth/…`). */
  isPublic: boolean;
};

export function requiresMfa({ currentLevel, nextLevel, pathname, isPublic }: MfaGateInput): boolean {
  // ⚠ THE GATE PAGE ITSELF IS ALWAYS EXEMPT, or the redirect is a loop and the only way out of it
  // is clearing cookies. Its subroutes too — there are none today, and a loop is not the failure
  // to discover that there are.
  if (pathname === MFA_PATH || pathname.startsWith(`${MFA_PATH}/`)) return false;
  // ⚠ AND SO IS THE SIGNED-OUT FLOW. Somebody at `aal1` following a fresh sign-in link is mid-way
  // through authenticating; bouncing them to a challenge for a session they are still acquiring
  // strands them between two screens that each want the other to have happened first.
  if (isPublic) return false;
  return currentLevel === 'aal1' && nextLevel === 'aal2';
}

/**
 * Where to send them back to once they have proved it.
 *
 * ⚠ SAME-ORIGIN PATHS ONLY (leading `/`, not `//`) — this value comes off the URL and is handed
 * straight to a redirect, which is the shape of an open redirect. The identical rule guards
 * `?next=` in `/auth/confirm`.
 *
 * ⚠ NEVER BACK TO THE GATE. A `next` of `/mfa` would satisfy the challenge and then return to it.
 */
export function safeNext(next: string | null | undefined, fallback = '/'): string {
  if (!next) return fallback;
  if (!next.startsWith('/') || next.startsWith('//')) return fallback;
  if (next === MFA_PATH || next.startsWith(`${MFA_PATH}/`)) return fallback;
  return next;
}

/**
 * Is the two-factor gate enforced in this build?
 *
 * ⚠⚠ IT CAN ONLY EVER BE SWITCHED OFF IN DEVELOPMENT, AND THAT IS THE WHOLE DESIGN. A kill switch
 * for a security control is a way to turn the control off, so the question is not whether it is
 * convenient but who can reach it. `NODE_ENV` is inlined by Next at BUILD time and is `production`
 * for every Vercel deployment including previews — so on the deployed app this function is a
 * constant `true` and no environment variable, however misspelled or copied, can move it. The
 * failure mode "somebody set it on Vercel and nobody noticed for a month" is not mitigated here,
 * it is impossible.
 *
 * ⚠ WHY IT EXISTS AT ALL: the local stack's clock drifts (Docker Desktop on a laptop that sleeps),
 * GoTrue's TOTP window is ±1 step and is NOT configurable, so a dev machine a minute out cannot
 * enrol or verify at all — measured 2026-09-08, 59s behind. That is a local-environment problem
 * and blocking development on it buys nothing: Railway's clock is correct.
 *
 * ⚠⚠ THE BACKEND HAS ITS OWN SWITCH AND YOU NEED BOTH (`REQUIRE_MFA=0` in `backend/.env.local`).
 * Setting only this one leaves every API call answering 403 while the browser waves you through —
 * a BROKEN app rather than an open one, which is the right way round for a half-applied override
 * to fail, but it will look like a bug.
 */
export function mfaEnforced(
  env: { NODE_ENV?: string; NEXT_PUBLIC_DISABLE_MFA?: string } = process.env,
): boolean {
  if (env.NODE_ENV === 'production') return true;
  // ⚠ EXACTLY `'1'`. "true", "yes" and "" are the spellings people reach for, and a control that
  // silently accepts them turns a typo into an unguarded app.
  return env.NEXT_PUBLIC_DISABLE_MFA !== '1';
}
