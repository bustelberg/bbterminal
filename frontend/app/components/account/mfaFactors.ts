/**
 * The decisions `/account/security` makes about a list of authenticators, as pure functions.
 *
 * Extracted so they can be unit-tested — the page around them is `supabase.auth.mfa.*` calls and
 * JSX, neither of which this repo tests. Every rule below is one somebody could get wrong quietly.
 */

/** The shape `supabase.auth.mfa.listFactors()` returns, narrowed to what this page reads. */
export type Factor = {
  id: string;
  friendly_name?: string;
  factor_type: string;
  status: 'verified' | 'unverified';
  created_at: string;
};

/**
 * ⚠⚠ AN ABANDONED ENROLMENT LEAVES A REAL FACTOR BEHIND, AND IT COUNTS AGAINST THE CAP. Press
 * "Add", never scan the QR, close the tab — GoTrue keeps an `unverified` factor for ever. Do that
 * ten times and `max_enrolled_factors` is reached, at which point enrolling fails with a message
 * about too many factors while the page shows NONE, because an unverified factor protects nothing
 * and has no business in a list of your authenticators.
 *
 * So they are swept before each new enrolment rather than displayed: there is nothing a reader
 * would ever want to do with one except get rid of it.
 */
export function unverifiedIds(factors: readonly Factor[]): string[] {
  return factors.filter((f) => f.status === 'unverified').map((f) => f.id);
}

/** The authenticators that actually protect the account — the only ones worth showing. */
export function verifiedFactors(factors: readonly Factor[]): Factor[] {
  return factors.filter((f) => f.status === 'verified');
}

/**
 * Which factors a code may be checked against when removing `targetId`, and in what order.
 *
 * ⚠⚠ THE OTHERS COME FIRST, AND THAT IS THE WHOLE POINT. Removing an authenticator asks for a
 * current code — otherwise anyone holding a stolen session could strip the protection off the
 * account. But the commonest reason to remove one is that the device is GONE, and demanding a code
 * from the very phone you have lost makes the second authenticator (the one `max_enrolled_factors`
 * exists to allow) useless: you could enrol a spare and still be locked into keeping the dead one.
 *
 * ⚠ THE TARGET IS STILL IN THE LIST, LAST. With only one authenticator it is the only thing to
 * check against, and proving you still hold the device you are removing is a perfectly good proof.
 */
export function verifyOrder(factors: readonly Factor[], targetId: string): Factor[] {
  const verified = verifiedFactors(factors);
  return [
    ...verified.filter((f) => f.id !== targetId),
    ...verified.filter((f) => f.id === targetId),
  ];
}

/** A TOTP code as typed, reduced to what can be submitted. */
export function normaliseCode(raw: string): string {
  // ⚠ DIGITS ONLY, AND TRUNCATED. Authenticator apps display "123 456" and people paste the space
  // with it; some keyboards insert a non-breaking one. A stray character makes a correct code fail
  // verification, which sends the reader to check their phone's clock over a typo they cannot see.
  return raw.replace(/\D/g, '').slice(0, CODE_LENGTH);
}

export const CODE_LENGTH = 6;

export function isCompleteCode(code: string): boolean {
  return normaliseCode(code).length === CODE_LENGTH;
}

/**
 * The shared secret, spaced for reading aloud or typing by hand.
 *
 * ⚠ IT IS NOT DECORATION. It is the fallback for every case the QR cannot serve: a desktop
 * authenticator, a phone whose camera will not focus, a password manager's TOTP field, and a
 * screen reader. Base32 in one 32-character run is unreadable and mistypes silently.
 */
export function groupSecret(secret: string): string {
  return (secret.match(/.{1,4}/g) ?? []).join(' ');
}

/**
 * GoTrue's QR, reduced to the `<svg>` element itself.
 *
 * ⚠ IT ARRIVES AS A STANDALONE XML DOCUMENT, not a fragment — measured against the live local
 * stack: `<?xml version="1.0"?>` then a `<!DOCTYPE svg …>` then the element. Assigned through
 * `innerHTML` the parser is in HTML mode, where an XML declaration is not a declaration at all:
 * it becomes a bogus comment, and the doctype is dropped. Browsers do that quietly today, which is
 * exactly why it is worth cutting — the page would depend on error recovery nobody chose, in a
 * string from outside this codebase, for the one image the whole flow turns on.
 *
 * ⚠ FALLS BACK TO THE INPUT rather than to empty: an unrecognised shape should still be given to
 * the parser, because a QR that renders oddly is recoverable and a blank square is not.
 */
export function svgOnly(qr: string): string {
  const at = qr.indexOf('<svg');
  return at === -1 ? qr : qr.slice(at);
}

/**
 * A name for a new authenticator that will not collide with an existing one.
 *
 * ⚠ GOTRUE REJECTS A DUPLICATE `friendly_name` FOR THE SAME USER, and "Phone" is what everybody
 * types. The error is recoverable (`describeMfaError` explains it) but it arrives AFTER the QR has
 * been generated and scanned, which means starting over — so the default is made unique up front.
 */
export function suggestName(factors: readonly Factor[]): string {
  const taken = new Set(verifiedFactors(factors).map((f) => (f.friendly_name ?? '').toLowerCase()));
  if (!taken.has('authenticator')) return 'Authenticator';
  for (let n = 2; n <= 99; n += 1) {
    if (!taken.has(`authenticator ${n}`)) return `Authenticator ${n}`;
  }
  // ⚠ Unreachable below `max_enrolled_factors`, but a name is required, so never return ''.
  return `Authenticator ${Date.now()}`;
}
