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

/**
 * The on-screen sentence for a DIAGNOSED code failure — see `lib/totp.explainCode`.
 *
 * ⚠⚠ IT REPLACES A GUESS WITH A MEASUREMENT. `describeMfaError` can only pattern-match GoTrue's
 * "Invalid TOTP code entered" and had to pick one of three causes; it picked the clock, which is
 * the RAREST, and told somebody to check a phone setting that was already right while the real
 * fault was a leftover entry in their authenticator app. Returns null when there is nothing
 * measured to say, so the caller falls back to the generic sentence rather than inventing one.
 */
export function explainVerdict(
  verdict: { kind: string; offsetSteps?: number },
  lang: 'en' | 'nl' = 'en',
  /** Browser-vs-server clock skew in seconds, from `lib/totp.serverSkewSeconds`. */
  browserSkewSec: number | null = null,
): string | null {
  const nl = lang === 'nl';
  if (verdict.kind === 'wrong-secret') {
    // ⚠ THE COMMON CASE, AND IT NAMES THE FIX. Every earlier attempt left an entry on the phone
    // bound to a secret this app has since discarded, and they all look identical in the app.
    return nl
      ? 'Je authenticator staat op een ANDERE code-reeks — waarschijnlijk een oude regel van een '
        + 'eerdere poging. Verwijder alle BBTerminal-regels uit je app en scan deze QR opnieuw.'
      : 'Your authenticator is on a DIFFERENT secret — almost certainly a leftover entry from an '
        + 'earlier attempt. Delete every BBTerminal entry in the app, then scan this QR again.';
  }
  if (verdict.kind === 'clock-skew') {
    const secs = Math.abs((verdict.offsetSteps ?? 0) * 30);
    /**
     * ⚠⚠ NAME THE DEVICE THAT IS ACTUALLY WRONG. The skew is between the PHONE and this BROWSER,
     * which on its own says nothing about which of them drifted — and the first version of this
     * sentence simply assumed the phone and sent somebody to change a setting that was already
     * right, while their laptop (and therefore the local GoTrue running on it) was 59 seconds
     * slow. `serverSkewSeconds` supplies the missing half.
     *
     * ⚠ THE 10-SECOND FLOOR is a deliberate refusal to over-claim: the `Date` header is
     * whole-second and carries the round trip, so a couple of seconds either way is noise, not a
     * finding.
     */
    if (browserSkewSec != null && Math.abs(browserSkewSec) >= 10) {
      const off = Math.abs(browserSkewSec);
      const dir = browserSkewSec > 0 ? (nl ? 'voor' : 'ahead') : (nl ? 'achter' : 'behind');
      return nl
        ? `De klok van DEZE COMPUTER loopt ${off} seconden ${dir} op de server — dat is de oorzaak, `
          + 'niet je telefoon. Synchroniseer de tijd van deze computer (Windows: '
          + '"Instellingen → Tijd en taal → Nu synchroniseren") en probeer opnieuw.'
        : `THIS COMPUTER's clock is ${off} seconds ${dir} of the server — that is the cause, not `
          + 'your phone. Sync the time on this computer (Windows: Settings → Time & language → '
          + '"Sync now") and try again.';
    }
    return nl
      ? `De klok van je telefoon en die van deze computer verschillen ongeveer ${secs} seconden. `
        + 'Zet op beide de tijd op automatisch en probeer het opnieuw.'
      : `Your phone's clock and this computer's differ by about ${secs} seconds. Set both to `
        + 'automatic time and try again.';
  }
  if (verdict.kind === 'matches') {
    // ⚠ SAY SO PLAINLY. The code was right for this secret and the server still said no — that is
    // ours to investigate, and pretending it was the reader's mistake wastes their time.
    return nl
      ? 'De code klopte bij deze QR, maar de server weigerde hem. Probeer het opnieuw; blijft het '
        + 'mislukken, dan ligt het aan de server en niet aan jou.'
      : 'That code was correct for this QR, but the server refused it. Try once more — if it keeps '
        + 'failing the problem is on our side, not yours.';
  }
  return null;
}
