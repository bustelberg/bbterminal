import { describe, expect, it } from 'vitest';
import { describeAuthError, describeSendError, hasAuthError } from './authError';

/**
 * The one thing a person creating an account ever saw when a link failed was "Auth session
 * missing", after choosing a password, on the screen where nothing could be done about it. What is
 * asserted here is that every failure now produces a sentence that names an ACTION — and that the
 * two production-only causes are told apart, because their fixes are different: one needs a new
 * link, the other needs the same link opened somewhere else.
 */
describe('describeAuthError', () => {
  it('tells someone to open the link where they asked for it, on a PKCE verifier failure', () => {
    // The cross-device case. `?code=` can only be exchanged by the browser that stored the
    // verifier — invisible locally, where there is only ever one browser.
    for (const message of [
      'invalid request: both auth code and code verifier should be non-empty',
      'invalid flow state, no valid flow state found',
    ]) {
      expect(describeAuthError({ message })).toMatch(/same browser/i);
    }
  });

  it('distinguishes an expired or already-used link, and blames the mail system, not the user', () => {
    const s = describeAuthError({
      error: 'access_denied', errorCode: 'otp_expired',
      errorDescription: 'Email link is invalid or has expired',
    });
    expect(s).toMatch(/expired|already been used/i);
    // ⚠ The scanner explanation earns its place: it is why a link "expires" seconds after being
    // sent, which otherwise reads as a bug in this app.
    expect(s).toMatch(/automatically/i);
    // ⚠⚠ AND IT MUST NOT BE THE VERIFIER SENTENCE. That description contains "invalid", and an
    // ordering mistake in the matcher sends the person to open the link on another device — which
    // cannot work, and reads as advice.
    expect(s).not.toMatch(/same browser/i);
  });

  it('never returns an empty message, and never a bare library string', () => {
    for (const input of [{}, { message: '' }, { error: 'something_new_from_supabase' }]) {
      const s = describeAuthError(input);
      expect(s.length).toBeGreaterThan(20);
      // A message a person can act on ends by telling them what to do.
      expect(s).toMatch(/request a new one|same browser/i);
    }
    expect(describeAuthError({ message: 'AuthApiError: kaboom' })).not.toContain('AuthApiError');
  });
});

describe('hasAuthError', () => {
  /**
   * ⚠⚠ THIS IS THE CHECK WHOSE ABSENCE CAUSED THE BUG. A rejected token comes back with `error`
   * set and NO `code` and NO `token_hash` — so a route that looks only for those two sees an empty
   * query, decides there is nothing to do, and carries on to the password form as though it had
   * signed the person in.
   */
  it('sees an error redirect that carries no code and no token_hash', () => {
    const p = new URLSearchParams(
      'error=access_denied&error_code=otp_expired&error_description=Email+link+is+invalid',
    );
    expect(hasAuthError(p)).toBe(true);
    expect(p.get('code')).toBeNull();
    expect(p.get('token_hash')).toBeNull();
  });

  it('is false for a normal successful callback', () => {
    expect(hasAuthError(new URLSearchParams('code=abc123'))).toBe(false);
    expect(hasAuthError(new URLSearchParams('token_hash=abc&type=magiclink'))).toBe(false);
    expect(hasAuthError(new URLSearchParams(''))).toBe(false);
  });

  it('fires on any one of the three params alone', () => {
    expect(hasAuthError(new URLSearchParams('error=access_denied'))).toBe(true);
    expect(hasAuthError(new URLSearchParams('error_code=otp_expired'))).toBe(true);
    expect(hasAuthError(new URLSearchParams('error_description=nope'))).toBe(true);
  });
});

describe('describeSendError', () => {
  /**
   * The built-in Supabase mail service is capped at 2 messages per hour PROJECT-WIDE and the cap
   * cannot be raised without custom SMTP. It bites hardest right after a link has failed, because
   * every failure path in this flow ends by telling the person to request another one.
   */
  it('explains the project-wide hourly cap instead of repeating "rate limit exceeded"', () => {
    for (const m of ['email rate limit exceeded', 'over_email_send_rate_limit', 'Too Many Requests']) {
      const s = describeSendError(m);
      expect(s).toMatch(/hour/i);
      // ⚠ "not per person" is the part that stops someone retrying with another address.
      expect(s).toMatch(/project-wide|not per person/i);
      expect(s).not.toMatch(/rate limit exceeded/i);
    }
  });

  it('keeps the per-address throttle separate — that one is seconds, not an hour', () => {
    const s = describeSendError('For security purposes, you can only request this after 51 seconds.');
    expect(s).toMatch(/a minute/i);
    // ⚠ Telling someone to wait an hour for a 51-second throttle is how a working app gets
    // abandoned; the two limits must not collapse into one sentence.
    expect(s).not.toMatch(/hour/i);
  });

  it('passes an unrecognised message through rather than inventing a cause', () => {
    expect(describeSendError('Signups not allowed for this instance'))
      .toBe('Signups not allowed for this instance');
    expect(describeSendError(null)).toMatch(/could not be sent/i);
  });
});
