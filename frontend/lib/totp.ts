/**
 * Compute the TOTP code a secret SHOULD be showing right now — so a rejected code can be DIAGNOSED
 * rather than guessed at.
 *
 * ⚠⚠ IT EXISTS BECAUSE "Invalid TOTP code" IS THE LEAST INFORMATIVE FAILURE IN THE PRODUCT. Three
 * completely different faults produce it and the copy could only ever guess between them:
 *
 *   1. the phone holds a DIFFERENT secret (a stale entry from an earlier attempt, or a QR that
 *      did not decode cleanly) — by far the commonest, and nothing about the code is wrong;
 *   2. the clocks disagree by more than a window;
 *   3. the server genuinely rejected a correct code.
 *
 * The page knows the secret it just enrolled. Computing the expected code locally separates (1)
 * from (2)+(3) with certainty: if what the reader typed does not match what this secret produces,
 * their authenticator is on a different entry and no amount of clock-checking will help. Sending
 * somebody to their phone's date settings when the real fault is a leftover entry cost a real
 * afternoon (2026-09-08).
 *
 * ⚠ IT IS A DIAGNOSTIC, NEVER AN AUTHORISATION. The server verifies; this only explains. Nothing
 * here may ever gate a request — a check the client computes is a check an attacker controls.
 *
 * ⚠ SHA-1 IS CORRECT HERE and is not a security choice. RFC 6238 specifies HMAC-SHA-1 for TOTP,
 * and GoTrue's `otpauth://` URI says `algorithm=SHA1`; using anything stronger would compute codes
 * that match nobody.
 */

const B32 = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';

/** Base32 (RFC 4648, no padding) → bytes. Returns null on anything that is not valid base32. */
export function base32ToBytes(secret: string): Uint8Array | null {
  const clean = secret.replace(/=+$/, '').replace(/\s+/g, '').toUpperCase();
  if (!clean || /[^A-Z2-7]/.test(clean)) return null;
  let bits = '';
  for (const ch of clean) bits += B32.indexOf(ch).toString(2).padStart(5, '0');
  const bytes = bits.match(/.{8}/g) ?? [];
  return new Uint8Array(bytes.map((b) => parseInt(b, 2)));
}

/**
 * The 6-digit code for `secret` at `atMs`, or null if the secret is unreadable.
 *
 * ⚠ `step` AND `digits` ARE PARAMETERS BECAUSE THE `otpauth://` URI CARRIES THEM. GoTrue currently
 * sends `period=30&digits=6`; hardcoding them would make this quietly wrong the day it does not.
 */
export async function expectedTotp(
  secret: string,
  atMs: number = Date.now(),
  step = 30,
  digits = 6,
): Promise<string | null> {
  const key = base32ToBytes(secret);
  if (!key || typeof globalThis.crypto?.subtle === 'undefined') return null;
  const counter = Math.floor(atMs / 1000 / step);
  const msg = new Uint8Array(8);
  // ⚠ THE COUNTER IS 64-BIT BIG-ENDIAN. Writing it as a 32-bit value works until 2106 and then
  // silently stops; writing it little-endian never works at all.
  new DataView(msg.buffer).setBigUint64(0, BigInt(counter), false);
  const ck = await crypto.subtle.importKey(
    'raw', key as BufferSource, { name: 'HMAC', hash: 'SHA-1' }, false, ['sign'],
  );
  const mac = new Uint8Array(await crypto.subtle.sign('HMAC', ck, msg as BufferSource));
  const off = mac[mac.length - 1] & 0x0f;
  const bin = ((mac[off] & 0x7f) << 24) | (mac[off + 1] << 16) | (mac[off + 2] << 8) | mac[off + 3];
  return String(bin % 10 ** digits).padStart(digits, '0');
}

/** What a rejected code actually tells us. */
export type CodeVerdict =
  /** The typed code matches this secret in the current window — the fault is elsewhere. */
  | { kind: 'matches' }
  /** It matches, but for a window `offsetSteps` away — the clocks disagree. */
  | { kind: 'clock-skew'; offsetSteps: number }
  /** It matches no nearby window at all — the authenticator holds a different secret. */
  | { kind: 'wrong-secret' }
  /** We could not compute anything (no Web Crypto, unreadable secret). */
  | { kind: 'unknown' };

/**
 * Why did `typed` fail for `secret`?
 *
 * ⚠ THE WINDOW SEARCH IS ±10 STEPS (5 minutes each way). Wide enough to catch a phone whose clock
 * is minutes out — which is the whole point of distinguishing skew from a wrong secret — and
 * narrow enough that a random six digits will not collide by luck often (10⁶ codes, 21 windows).
 */
export async function explainCode(
  secret: string, typed: string, atMs: number = Date.now(),
): Promise<CodeVerdict> {
  const now = await expectedTotp(secret, atMs);
  if (now == null) return { kind: 'unknown' };
  if (now === typed) return { kind: 'matches' };
  for (let s = -10; s <= 10; s += 1) {
    if (s === 0) continue;
    // Sequential on purpose: the order is what identifies the NEAREST matching window, and 20
    // HMACs is nothing. Parallelising would lose the ordering for no useful speed.
    if (await expectedTotp(secret, atMs + s * 30_000) === typed) {
      return { kind: 'clock-skew', offsetSteps: s };
    }
  }
  return { kind: 'wrong-secret' };
}

/**
 * How far this BROWSER's clock is from the server's, in seconds (positive = browser ahead).
 *
 * ⚠⚠ WITHOUT THIS, A SKEW DIAGNOSIS BLAMES THE WRONG DEVICE. `explainCode` compares the typed
 * code against what this browser thinks the time is — so it can prove the phone and the browser
 * disagree, and cannot tell which of them is wrong. Measured 2026-09-08: the reader's iPhone was
 * correct and the LAPTOP was 59s slow (and so, being its Docker host, was GoTrue) — and the copy
 * confidently told them to go and change their phone's settings.
 *
 * ⚠ THE `Date` HEADER IS THE SERVER'S OWN CLOCK, which is the one that matters: GoTrue verifies
 * against it, not against the reader. Whole seconds only, and the round trip adds a little, so
 * this is accurate to a second or two — ample for a fault that is measured in half-minutes.
 *
 * ⚠ MEASURED AT THE MIDPOINT of the request, so half the latency is cancelled rather than being
 * silently added to the answer.
 */
export async function serverSkewSeconds(url: string): Promise<number | null> {
  try {
    const before = Date.now();
    const r = await fetch(url, { method: 'HEAD', cache: 'no-store' });
    const after = Date.now();
    const header = r.headers.get('date');
    if (!header) return null;
    const server = Date.parse(header);
    if (Number.isNaN(server)) return null;
    return Math.round(((before + after) / 2 - server) / 1000);
  } catch {
    // ⚠ A diagnostic that throws is worse than one that abstains — the caller falls back to the
    // sentence that names both devices rather than neither.
    return null;
  }
}
