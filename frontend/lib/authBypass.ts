/**
 * Whether the Playwright e2e auth short-circuit may fire.
 *
 * `E2E_BYPASS_AUTH=1` skips the entire session + role check in
 * `proxy.ts`. It's set only by `playwright.config.ts` and CI — never in
 * dev or on Vercel.
 *
 * The kill-switch is the `VERCEL` env var, which Vercel injects into
 * every deployment (production AND preview) and which is absent locally
 * and in CI. So even if someone mistakenly sets `E2E_BYPASS_AUTH=1` in
 * the Vercel dashboard, the bypass can NEVER fire on a real deployment.
 * We can't key off `NODE_ENV` here: the e2e suite runs a production
 * build (`next start`, NODE_ENV=production) and legitimately needs the
 * bypass, so a `NODE_ENV !== 'production'` guard would break e2e.
 *
 * Pure + dependency-free so it can be unit-tested without Next internals
 * (see authBypass.test.ts).
 *
 * @param bypassFlag  process.env.E2E_BYPASS_AUTH
 * @param onVercel    process.env.VERCEL (truthy only on Vercel deployments)
 */
export function isAuthBypassEnabled(
  bypassFlag: string | undefined,
  onVercel: string | undefined,
): boolean {
  return bypassFlag === '1' && !onVercel
}
