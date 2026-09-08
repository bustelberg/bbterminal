/**
 * What went wrong enrolling or verifying an authenticator, in one sentence the person can act on.
 *
 * ⚠ SAME RULE AS `authError.ts`, FOR THE SAME REASON: the full diagnostic goes to the console, one
 * short line to the screen. GoTrue's own strings name internal states ("mfa_challenge_expired",
 * "unprocessable_entity"), which tell someone holding a phone with six digits on it nothing about
 * what to do with them.
 *
 * ⚠⚠ THE FIRST CASE IS THE ONE THAT WILL ACTUALLY HAPPEN, AND IT IS NOT THE USER'S FAULT. TOTP is
 * enabled per environment — `supabase/config.toml` locally, the dashboard for the hosted project —
 * and `db push` does not carry config. So the expected production failure is that enrolment works
 * on a laptop and 422s for everybody in prod, with a message about "MFA" that reads like a bug in
 * the code. Naming the cause here is what turns a support round trip into a dashboard toggle.
 */

export type MfaErrorInput = {
  /** GoTrue's `code` / `error_code`, e.g. 'mfa_challenge_expired' — more specific, so it wins. */
  code?: string | null;
  /** The message from a failed `enroll` / `challenge` / `verify` / `unenroll` call. */
  message?: string | null;
};

/** A short, actionable sentence. Never empty, never a bare library string. */
export function describeMfaError({ code, message }: MfaErrorInput): string {
  const hay = `${code ?? ''} ${message ?? ''}`.toLowerCase();

  // ⚠ CHECKED FIRST. Its message also contains the word "mfa" and often "disabled", which the
  // looser cases below would swallow — and it is the only one here that no user can resolve.
  if (hay.includes('disabled') || hay.includes('not enabled')
    || hay.includes('mfa_enroll_disabled') || hay.includes('mfa_verify_disabled')) {
    return 'Two-factor sign-in is not switched on for this environment yet. '
      + 'An admin has to enable the authenticator-app factor in Supabase before anyone can enrol.';
  }
  if (hay.includes('already exists') || hay.includes('friendly name')) {
    return 'You already have an authenticator with that name. Give this one a different name.';
  }
  if (hay.includes('too_many') || hay.includes('maximum number')) {
    return 'You have reached the maximum number of authenticators. Remove one you no longer use, '
      + 'then add this one.';
  }
  if (hay.includes('challenge_expired') || hay.includes('has expired')) {
    return 'That code took too long to arrive. Close this and start again — codes are only valid '
      + 'for a short window.';
  }
  // ⚠ AFTER the expiry case: an expired challenge is ALSO a verification failure, and "check your
  // code" is the wrong advice when the code was fine and the clock ran out.
  if (hay.includes('invalid') || hay.includes('verification failed') || hay.includes('incorrect')) {
    return 'That code did not match. Check you are reading the current one — they change every '
      + '30 seconds — and that your phone\'s clock is set automatically.';
  }
  if (hay.includes('not found') || hay.includes('no factor')) {
    return 'That authenticator is no longer registered. Reload the page to see the current list.';
  }
  // ⚠ A LAST RESORT THAT STILL SAYS WHAT TO DO. Returning the raw message here would put
  // "AuthApiError: unprocessable_entity" in front of someone setting up their phone.
  return 'That did not work. Try again, and if it keeps failing ask an admin to check the '
    + 'authentication settings.';
}
