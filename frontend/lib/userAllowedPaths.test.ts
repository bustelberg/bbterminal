import { describe, expect, it } from 'vitest'

import { isUserAllowedPath, USER_ALLOWED_PATHS } from './userAllowedPaths'

// This list is the SINGLE source of truth shared by the route gate (proxy.ts),
// the home tiles (app/page.tsx), and the sidebar nav (Sidebar.tsx). Pinning it
// here guards against the drift that let the home page advertise admin-only
// pages (AIRS, Companies) to regular users while hiding /schedule.
describe('isUserAllowedPath', () => {
  it('allows exactly the user-facing pages', () => {
    expect(isUserAllowedPath('/')).toBe(true)
    expect(isUserAllowedPath('/schedule')).toBe(true)
    expect(isUserAllowedPath('/management-dashboard')).toBe(true)
    expect(isUserAllowedPath('/forbidden')).toBe(true)
  })

  it('is the WHOLE list — a page added here without a nav/gate review fails this', () => {
    // ⚠ PINNED AS A SET, NOT SPOT-CHECKED. Every entry hands a page to non-admins, and the page is
    // only half of it — whatever it FETCHES has to be allow-listed in `_auth_middleware.py` too,
    // or the user gets a screen of 403s that reads as a broken app rather than as a permission.
    // ⚠⚠ AND THE REMOVAL HALF IS PINNED BY THE CASES BELOW. Taking a page away leaves any API path
    // it alone needed open — a permission nobody can see, because it grants no reachable screen.
    expect([...USER_ALLOWED_PATHS].sort()).toEqual([
      '/', '/forbidden', '/management-dashboard', '/schedule',
    ])
  })

  it('⚠ blocks /earnings and /research-dashboard — removed from the user tier 2026-09-07', () => {
    // Both were user-visible and were taken away on request. `/research-dashboard` also gave up
    // its one API line: `/api/asset-pipeline/search` left `_USER_GET_RESOURCE_PATTERNS`, since it
    // had exactly one caller (that page's company picker) and nothing else in the app calls it.
    // ⚠ `/api/earnings` did NOT go with `/earnings`: /management-dashboard's Long Equity tab and
    // Fundamental modal are built on that namespace, so the page and the prefix are two different
    // permissions and only one of them was revoked.
    expect(isUserAllowedPath('/earnings')).toBe(false)
    expect(isUserAllowedPath('/earnings/123')).toBe(false)
    expect(isUserAllowedPath('/research-dashboard')).toBe(false)
  })

  it('blocks admin-only pages', () => {
    for (const p of ['/companies', '/airs-portfolio', '/backtest', '/acwi', '/leonteq', '/benchmarks', '/api']) {
      expect(isUserAllowedPath(p)).toBe(false)
    }
  })

  it('allows subroutes of an allowed page', () => {
    expect(isUserAllowedPath('/schedule/42')).toBe(true)
    expect(isUserAllowedPath('/management-dashboard/x')).toBe(true)
  })

  it("'/' matches only exactly, never as a prefix", () => {
    // Every path startsWith('/'), so the subroute form must not apply to root.
    expect(isUserAllowedPath('/companies')).toBe(false)
    expect(USER_ALLOWED_PATHS).toContain('/')
  })
})
