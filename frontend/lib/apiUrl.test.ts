import { describe, expect, it } from 'vitest'

import { resolveApiUrl } from './apiUrl'

/**
 * ⚠ THE RULE THIS PINS SHIPPED BROKEN TO REAL USERS. `NEXT_PUBLIC_API_URL` is inlined at BUILD
 * time, so a Vercel build with it unset baked `http://localhost:8000` into every call — and the
 * deployed site then asked each VISITOR'S OWN MACHINE for the data. Chrome surfaced that as
 * "Access other apps and services on this device" (its Local Network Access prompt), i.e. the
 * failure reached users as the site being invasive rather than misconfigured.
 *
 * A silent localhost default is worse than no default at all, and this is where that stays fixed.
 */
describe('resolveApiUrl', () => {
  it('uses the configured URL wherever the page is served from', () => {
    expect(resolveApiUrl('https://api.example.com', 'https://app.example.com'))
      .toBe('https://api.example.com')
  })

  it('trims a trailing slash so callers can concatenate a path safely', () => {
    expect(resolveApiUrl('https://api.example.com/', null)).toBe('https://api.example.com')
  })

  it('falls back to localhost ONLY when the page is itself local', () => {
    for (const origin of ['http://localhost:3000', 'http://127.0.0.1:3000', 'http://[::1]:3000']) {
      expect(resolveApiUrl(undefined, origin)).toBe('http://localhost:8000')
    }
  })

  it('NEVER dials localhost from a deployed origin', () => {
    // The actual production bug. An empty base makes every call a same-origin relative request:
    // it fails fast and visibly instead of negotiating access to the user's device.
    expect(resolveApiUrl(undefined, 'https://bbterminal.vercel.app')).toBe('')
    expect(resolveApiUrl('', 'https://bbterminal.vercel.app')).toBe('')
    expect(resolveApiUrl('   ', 'https://bbterminal.vercel.app')).toBe('')
  })

  it('a hostname merely CONTAINING "localhost" is not local', () => {
    // `localhost.evil.com` resolves publicly; a substring test would hand it the dev default.
    expect(resolveApiUrl(undefined, 'https://localhost.evil.com')).toBe('')
    expect(resolveApiUrl(undefined, 'https://mylocalhost.io')).toBe('')
  })

  it('keeps the dev default when there is no page at all (SSR, prerender, tests)', () => {
    expect(resolveApiUrl(undefined, null)).toBe('http://localhost:8000')
  })

  it('does not trust an unparseable origin with a loopback default', () => {
    expect(resolveApiUrl(undefined, 'not-an-origin')).toBe('')
  })
})
