/**
 * What went wrong on the way back from an email link, in one sentence the person can act on.
 *
 * ⚠⚠ IT EXISTS BECAUSE THE ONLY MESSAGE THIS FLOW EVER PRODUCED WAS "Auth session missing", AT THE
 * WRONG SCREEN, AT THE WRONG MOMENT. `/auth/confirm` discarded the result of
 * `exchangeCodeForSession` / `verifyOtp` and redirected to `/set-password` regardless — so every
 * failure below arrived at a working-looking password form, and the person learned something was
 * wrong only after choosing a password and pressing Save. At that point the library string names
 * the SYMPTOM (there is no session) and nothing about the CAUSE (the link was already used, or was
 * opened in a different browser from the one that asked for it).
 *
 * The two that actually happen in production, and never locally:
 *
 *   1. A DIFFERENT BROWSER. `createBrowserClient` uses PKCE, so requesting the link stores a code
 *      verifier in a cookie and `?code=` can only be exchanged by that same browser. Ask on the
 *      laptop, open the mail on the phone, and the exchange fails. Locally you are always in one
 *      browser, so it always works.
 *   2. AN EMAIL SCANNER GOT THERE FIRST. Corporate mail security (Outlook Safe Links and friends)
 *      fetches every URL in a message. The default Supabase template points at
 *      `/auth/v1/verify`, which CONSUMES the one-time token on that fetch — so by the time the
 *      human clicks, Supabase answers `?error=access_denied&error_code=otp_expired`. A local
 *      Mailpit inbox has no scanner.
 *
 * ⚠ THE FULL DETAIL GOES TO THE SERVER LOG, ONE SHORT LINE TO THE PERSON — the same rule the rest
 * of this app follows. These sentences are the short line; they name what to DO, because "invalid
 * flow state" is not an instruction.
 */

export type AuthErrorInput = {
  /** Supabase's `error` query param, e.g. 'access_denied'. */
  error?: string | null;
  /** Its `error_code`, e.g. 'otp_expired' — more specific than `error`, so it wins. */
  errorCode?: string | null;
  /** Its `error_description`, already URL-decoded. */
  errorDescription?: string | null;
  /** The message from a failed `exchangeCodeForSession` / `verifyOtp` call. */
  message?: string | null;
};

/** A short, actionable sentence. Never empty, never a bare library string. */
export function describeAuthError(
  { error, errorCode, errorDescription, message }: AuthErrorInput,
): string {
  const hay = `${errorCode ?? ''} ${error ?? ''} ${errorDescription ?? ''} ${message ?? ''}`
    .toLowerCase();

  // ⚠ THE CODE-VERIFIER CASE IS CHECKED FIRST because its `error_description` often ALSO contains
  // the word "invalid", and the generic invalid-link sentence below would swallow the one piece of
  // advice that actually resolves it: open the link where you asked for it.
  if (hay.includes('code verifier') || hay.includes('code_verifier')
    || hay.includes('flow state') || hay.includes('flow_state')) {
    return 'This link has to be opened in the same browser you requested it from. '
      + 'Request a new one here and open it on this device.';
  }
  if (hay.includes('otp_expired') || hay.includes('expired')) {
    return 'That link has expired or had already been used — some mail systems open links '
      + 'automatically before you do. Request a new one below.';
  }
  if (hay.includes('access_denied')) {
    return 'That link is no longer valid. Request a new one below.';
  }
  if (hay.includes('not found') || hay.includes('invalid')) {
    return 'That link could not be verified. Request a new one below.';
  }
  // ⚠ A LAST RESORT THAT STILL SAYS WHAT TO DO. Returning the raw message here would put
  // "AuthApiError: ..." in front of someone trying to create an account; returning nothing would
  // put them back where this started.
  return 'Sign-in could not be completed from that link. Request a new one below.';
}

/**
 * Was this redirect back from Supabase already an error, before we tried anything?
 *
 * ⚠ IT HAS TO BE CHECKED BEFORE THE EXCHANGE, NOT AFTER. When `/auth/v1/verify` rejects a token it
 * redirects with `?error=...` and NO `code` and NO `token_hash` — so a route that only looks for
 * those two sees an empty query, concludes nothing to do, and carries on to the password screen as
 * if it had succeeded. That is precisely the path that produced "Auth session missing".
 */
export function hasAuthError(params: URLSearchParams): boolean {
  return Boolean(params.get('error') || params.get('error_code')
    || params.get('error_description'));
}
