// @vitest-environment happy-dom
//
// ⚠ THE ESCAPE HATCH `vitest.config.ts` DOCUMENTS, AND THE FIRST FILE TO NEED IT. The suite runs
// on `node` because booting a DOM per file cost 50.5s of worker time against 1.0s of assertions —
// but `claimLangFor` reads and writes `window.localStorage`, which is the whole subject here, so
// there is nothing left to test without one. Per-file, so the other 91 files pay nothing.
import { beforeEach, describe, expect, it, vi } from 'vitest'

/**
 * `claimLangFor` — the half of "new users read Dutch" that the default value cannot deliver.
 *
 * ⚠ THE MODULE CACHES THE SNAPSHOT IN A LIVE `let`, so every case re-imports it (`resetModules` +
 * a dynamic import). Sharing one import across cases would leak `current` between them and the
 * suite would pass or fail on ordering rather than on behaviour.
 */
async function fresh() {
  vi.resetModules()
  return import('./i18n')
}

const KEY = 'bb:lang'
const OWNER = 'bb:lang:owner'

describe('claimLangFor', () => {
  beforeEach(() => {
    localStorage.clear()
  })

  it('hands a brand-new reader the Dutch default on a browser where someone chose English', async () => {
    // The reported case: an earlier account pressed EN on this machine, then a new user signs up.
    localStorage.setItem(KEY, 'en')
    localStorage.setItem(OWNER, 'admin@bustelberg.nl')

    const { claimLangFor } = await fresh()
    claimLangFor('new.user@bustelberg.nl')

    expect(localStorage.getItem(KEY)).toBeNull()
    expect(localStorage.getItem(OWNER)).toBe('new.user@bustelberg.nl')
  })

  it('leaves the reader who chose it alone, however often it is called', async () => {
    // ⚠ THE COMMON PATH. The Sidebar claims on every auth-state change — a token refresh in
    // another tab included — so a claim for the SAME reader has to be a no-op, or the preference
    // would be wiped by the app merely staying open.
    const { claimLangFor } = await fresh()
    localStorage.setItem(KEY, 'en')
    claimLangFor('reader@bustelberg.nl')      // first claim: unattributed choice, cleared once
    localStorage.setItem(KEY, 'en')           // they press EN again, now attributed
    claimLangFor('reader@bustelberg.nl')
    claimLangFor('reader@bustelberg.nl')

    expect(localStorage.getItem(KEY)).toBe('en')
  })

  it('forgets both the choice and its owner on sign-out', async () => {
    localStorage.setItem(KEY, 'en')
    localStorage.setItem(OWNER, 'reader@bustelberg.nl')

    const { claimLangFor } = await fresh()
    claimLangFor(null)

    expect(localStorage.getItem(KEY)).toBeNull()
    expect(localStorage.getItem(OWNER)).toBeNull()
  })

  it('is a no-op while nobody is signed in, so the login page cannot thrash storage', async () => {
    const { claimLangFor } = await fresh()
    claimLangFor(null)
    claimLangFor(null)
    expect(localStorage.getItem(OWNER)).toBeNull()
  })

  it('clears an unattributable choice exactly once, then honours the next one', async () => {
    // ⚠ THE ONE-TIME RESET on browsers that predate the owner key — deliberate, not tolerated:
    // the preference cannot be attributed to anybody, and an unattributed reader gets Dutch.
    localStorage.setItem(KEY, 'en')

    const { claimLangFor } = await fresh()
    claimLangFor('reader@bustelberg.nl')
    expect(localStorage.getItem(KEY)).toBeNull()

    localStorage.setItem(KEY, 'en')
    claimLangFor('reader@bustelberg.nl')
    expect(localStorage.getItem(KEY)).toBe('en')
  })
})
