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
 * GoTrue's QR, made safe to render at whatever size the page wants.
 *
 * ⚠⚠ IT ARRIVES AS `<svg width="219" height="219">` WITH NO `viewBox`, AND THAT COMBINATION IS A
 * TRAP. An SVG without a viewBox has no coordinate system to map onto its viewport, so CSS
 * width/height resizes the WINDOW and not the drawing: ask for anything under 219px and the image
 * is CLIPPED, not scaled. This page asked for `w-44` — 11rem, and `html{font-size:17.5px}` makes
 * that 192.5px — so it painted a QR with 12% missing off the right and bottom edges, which is
 * where two of the three finder patterns live.
 *
 * The result is a code that a phone may still partially decode into a WRONG secret, so the app
 * cheerfully shows six digits that can never verify. Reported as "the code is correct from Google
 * Auth, wtf is wrong" — and nothing was wrong with the code, the clock, or the server: the phone
 * had been handed a different secret from the one enrolled.
 *
 * ⚠ THE FIX IS A `viewBox`, NOT A BIGGER BOX. Pinning the CSS at 219px would work until somebody
 * changed the rem scale or the QR's density changed with a longer issuer. Giving it a coordinate
 * system makes it scale at any size, for ever.
 *
 * ⚠ It also drops the XML prolog: GoTrue sends a standalone document (`<?xml …?>`, a DOCTYPE),
 * and through `innerHTML` the parser is in HTML mode where an XML declaration becomes a bogus
 * comment. Browsers recover quietly today; depending on that is not a choice anybody made.
 *
 * ⚠ FALLS BACK TO THE INPUT rather than to empty — a QR that renders oddly is recoverable, a
 * blank square is not.
 */
export function qrSvg(raw: string): string {
  const at = raw.indexOf('<svg');
  if (at === -1) return raw;
  const svg = raw.slice(at);
  const tagEnd = svg.indexOf('>');
  if (tagEnd === -1) return svg;
  const tag = svg.slice(0, tagEnd + 1);
  if (/viewBox=/i.test(tag)) return svg;      // already scalable — leave it alone
  const w = /\swidth="(\d+(?:\.\d+)?)"/i.exec(tag);
  const h = /\sheight="(\d+(?:\.\d+)?)"/i.exec(tag);
  if (!w || !h) return svg;                   // nothing to derive a coordinate system from
  // ⚠ The fixed width/height go too. Left in place they still win over CSS in some engines, and
  // the whole point is to let the page decide the size.
  const fixed = tag
    .replace(/\s+width="[^"]*"/i, '')
    .replace(/\s+height="[^"]*"/i, '')
    .replace(/<svg/i, `<svg viewBox="0 0 ${w[1]} ${h[1]}"`);
  return fixed + svg.slice(tagEnd + 1);
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
