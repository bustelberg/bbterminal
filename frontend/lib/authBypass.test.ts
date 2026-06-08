import { describe, expect, it } from 'vitest'

import { isAuthBypassEnabled } from './authBypass'

// Regression guard for the L2 security fix: the Playwright auth bypass
// must never be able to fire on a Vercel deployment, even if the
// E2E_BYPASS_AUTH flag is mistakenly set there.
describe('isAuthBypassEnabled', () => {
  it('allows the bypass in local/CI e2e (flag set, not on Vercel)', () => {
    // Note: e2e runs a *production* build, so NODE_ENV is intentionally
    // NOT part of the check — only the flag + the Vercel kill-switch.
    expect(isAuthBypassEnabled('1', undefined)).toBe(true)
  })

  it('BLOCKS the bypass on Vercel even when the flag is set (the fix)', () => {
    expect(isAuthBypassEnabled('1', '1')).toBe(false)
  })

  it('blocks the bypass on Vercel regardless of the VERCEL value', () => {
    // Vercel sets VERCEL to "1"; any truthy string must block.
    expect(isAuthBypassEnabled('1', 'production')).toBe(false)
  })

  it('blocks the bypass when the flag is unset (normal dev/runtime)', () => {
    expect(isAuthBypassEnabled(undefined, undefined)).toBe(false)
    expect(isAuthBypassEnabled('0', undefined)).toBe(false)
    expect(isAuthBypassEnabled('true', undefined)).toBe(false)
  })
})
