import { describe, expect, it } from 'vitest'

import { isUserAllowedPath, USER_ALLOWED_PATHS } from './userAllowedPaths'

// This list is the SINGLE source of truth shared by the route gate (proxy.ts),
// the home tiles (app/page.tsx), and the sidebar nav (Sidebar.tsx). Pinning it
// here guards against the drift that let the home page advertise admin-only
// pages (AIRS, Companies) to regular users while hiding /schedule.
describe('isUserAllowedPath', () => {
  it('allows exactly the user-facing pages', () => {
    expect(isUserAllowedPath('/')).toBe(true)
    expect(isUserAllowedPath('/earnings')).toBe(true)
    expect(isUserAllowedPath('/schedule')).toBe(true)
    expect(isUserAllowedPath('/management-dashboard')).toBe(true)
    expect(isUserAllowedPath('/forbidden')).toBe(true)
  })

  it('blocks admin-only pages', () => {
    for (const p of ['/companies', '/airs-portfolio', '/backtest', '/acwi', '/leonteq', '/benchmarks', '/api']) {
      expect(isUserAllowedPath(p)).toBe(false)
    }
  })

  it('allows subroutes of an allowed page', () => {
    expect(isUserAllowedPath('/earnings/123')).toBe(true)
    expect(isUserAllowedPath('/schedule/42')).toBe(true)
  })

  it("'/' matches only exactly, never as a prefix", () => {
    // Every path startsWith('/'), so the subroute form must not apply to root.
    expect(isUserAllowedPath('/companies')).toBe(false)
    expect(USER_ALLOWED_PATHS).toContain('/')
  })
})
