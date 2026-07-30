/**
 * Backend API base URL. Reads `NEXT_PUBLIC_API_URL` (set in `frontend/.env.local` for dev and in
 * the Vercel project env for prod).
 *
 * Single source of truth — previously each component (33+ files) declared its own copy of this
 * same line. Import from here so the fallback can ever be changed in exactly one place.
 *
 * ⚠ A DEPLOYED PAGE MUST NEVER FALL BACK TO LOCALHOST, AND IT USED TO. `NEXT_PUBLIC_*` is INLINED
 * AT BUILD TIME, so a Vercel build with the variable unset baked `http://localhost:8000` into
 * every call — and the browser then asked the VISITOR'S OWN MACHINE for the data. Measured in
 * production 2026-07-30: Chrome showed real users "Access other apps and services on this device"
 * (its Local Network Access prompt), because a public origin was dialing loopback.
 *
 * That default was wrong in three compounding ways:
 *   1. It is silent. A missing variable produced a working-LOOKING build, not an error.
 *   2. It surfaces as a PERMISSION PROMPT, so the failure reads to the user as the site being
 *      invasive rather than misconfigured.
 *   3. It is worse than failing: a visitor who happens to run anything on :8000 gets silently
 *      talked to by our app — their machine answering for our backend.
 *
 * So localhost is now offered only to a page that is ITSELF on localhost. A deployed page with no
 * configured API URL gets '', which makes every call a same-origin relative request that fails
 * fast and visibly, instead of leaving the browser to negotiate access to the user's device.
 */

const DEV_FALLBACK = 'http://localhost:8000';

/** Hosts where an unset `NEXT_PUBLIC_API_URL` legitimately means "the backend is on this box". */
const LOCAL_HOSTS = new Set(['localhost', '127.0.0.1', '[::1]', '0.0.0.0']);

/**
 * Resolve the API base. Pure, so the rule that keeps loopback out of production is testable
 * without a browser.
 *
 * `origin` is the page's own origin, or null when there is no page (SSR, prerender, tests) — in
 * which case the dev fallback is safe: a server-side fetch cannot prompt anyone, and it is the
 * behaviour local `next dev` has always had.
 */
export function resolveApiUrl(configured: string | undefined | null, origin: string | null): string {
  const trimmed = (configured ?? '').trim();
  if (trimmed) return trimmed.replace(/\/+$/, '');
  if (origin === null) return DEV_FALLBACK;
  try {
    if (LOCAL_HOSTS.has(new URL(origin).hostname)) return DEV_FALLBACK;
  } catch {
    /* an origin we cannot parse is not one we will trust with a loopback default */
  }
  return '';
}

const pageOrigin = typeof window === 'undefined' ? null : window.location.origin;

export const API_URL = resolveApiUrl(process.env.NEXT_PUBLIC_API_URL, pageOrigin);

if (!API_URL && pageOrigin !== null) {
  // Loud, once, in the console — and the requests themselves 404 against our own origin, which is
  // a diagnosable failure rather than a permission dialog about the user's device.
  console.error(
    '[bbterminal] NEXT_PUBLIC_API_URL is not set for this build, so every API call will fail. '
    + 'It is inlined at BUILD time — set it in the Vercel project environment and REDEPLOY; '
    + 'changing it without a rebuild has no effect.',
  );
}
