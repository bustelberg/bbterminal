// @vitest-environment happy-dom
//
// ⚠ The suite runs on `node` (see `vitest.config.ts`); this module's whole subject is what leaves
// `localStorage`, so it takes the per-file escape hatch that config documents.
import { beforeEach, describe, expect, it } from 'vitest'

import { LEGACY_KEYS, purgeLegacySessions } from './purgeLegacySessions'

describe('purgeLegacySessions', () => {
  beforeEach(() => localStorage.clear())

  it('removes the retired switcher\'s refresh tokens', () => {
    // ⚠ THE POINT OF THE WHOLE MODULE. Deleting the account switcher removed the code that READ
    // these; the tokens themselves stayed valid in every browser that had used it.
    localStorage.setItem('bbterminal_sessions', JSON.stringify([
      { email: 'admin@bustelberg.nl', refresh_token: 'still-valid' },
    ]))
    localStorage.setItem('bbterminal_impersonating', '1')

    purgeLegacySessions()

    expect(localStorage.getItem('bbterminal_sessions')).toBeNull()
    expect(localStorage.getItem('bbterminal_impersonating')).toBeNull()
  })

  it('is a no-op on a browser that never used the switcher', () => {
    purgeLegacySessions()
    expect(localStorage.length).toBe(0)
  })

  it('runs repeatedly without complaint — it fires on every load, not once', () => {
    localStorage.setItem('bbterminal_sessions', '[]')
    purgeLegacySessions()
    purgeLegacySessions()
    expect(localStorage.getItem('bbterminal_sessions')).toBeNull()
  })

  it('touches nothing else in storage', () => {
    // ⚠ The app keeps real preferences alongside these (`bb:lang`, and whatever a panel has
    // remembered). A purge that reached wider would log people out of their own settings.
    localStorage.setItem('bb:lang', 'nl')
    localStorage.setItem('bb:lang:owner', 'reinier@bustelberg.nl')
    localStorage.setItem('bbterminal_sessions', '[]')

    purgeLegacySessions()

    expect(localStorage.getItem('bb:lang')).toBe('nl')
    expect(localStorage.getItem('bb:lang:owner')).toBe('reinier@bustelberg.nl')
    expect(localStorage.getItem('bbterminal_sessions')).toBeNull()
  })

  it('names exactly the two retired keys and no others', () => {
    // If a third key is ever added here it should be a deliberate edit, not a drive-by.
    expect([...LEGACY_KEYS]).toEqual(['bbterminal_sessions', 'bbterminal_impersonating'])
  })
})
