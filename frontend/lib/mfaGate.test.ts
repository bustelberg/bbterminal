import { describe, expect, it } from 'vitest'

import { MFA_PATH, requiresEnrolment, requiresMfa, safeNext } from './mfaGate'

const at = (currentLevel: 'aal1' | 'aal2' | null, nextLevel: 'aal1' | 'aal2' | null,
  pathname = '/schedule', isPublic = false) =>
  requiresMfa({ currentLevel, nextLevel, pathname, isPublic })

describe('requiresMfa', () => {
  it('challenges a session that has a factor and has not used it', () => {
    expect(at('aal1', 'aal2')).toBe(true)
  })

  it('⚠⚠ leaves alone anyone who has not enrolled — enforcement is opt-in', () => {
    // The property that lets this ship without a flag day. `nextLevel` is only 'aal2' when a
    // verified factor exists, so a rule written as `currentLevel !== 'aal2'` would instead lock
    // out every account in the app on the day it deployed.
    expect(at('aal1', 'aal1')).toBe(false)
  })

  it('lets a satisfied session through', () => {
    expect(at('aal2', 'aal2')).toBe(false)
  })

  it('⚠ stops firing by itself when the last factor is removed', () => {
    // Unenrol and GoTrue drops `nextLevel` back to 'aal1'. Nothing here needs to know that
    // happened — which is why the rule reads levels rather than tracking enrolment.
    expect(at('aal1', 'aal1')).toBe(false)
  })

  it('treats an unknown level as nothing to prove', () => {
    expect(at(null, null)).toBe(false)
    expect(at(undefined as never, undefined as never)).toBe(false)
  })

  it('⚠⚠ never challenges on the gate page itself — that is a redirect loop', () => {
    expect(at('aal1', 'aal2', MFA_PATH)).toBe(false)
    expect(at('aal1', 'aal2', `${MFA_PATH}/anything`)).toBe(false)
  })

  it('⚠ never challenges inside the signed-out flow', () => {
    // Somebody at aal1 following a fresh sign-in link is mid-way through authenticating. Bouncing
    // them to a challenge for a session they are still acquiring strands them between two screens
    // that each want the other to have happened first.
    expect(at('aal1', 'aal2', '/login', true)).toBe(false)
    expect(at('aal1', 'aal2', '/auth/confirm', true)).toBe(false)
  })

  it('DOES challenge on the security page — you may not edit factors unproven', () => {
    // /account/security is where a factor can be removed. Exempting it would let a session that
    // never proved the second factor take it off the account.
    expect(at('aal1', 'aal2', '/account/security')).toBe(true)
  })
})

describe('safeNext', () => {
  it('keeps a same-origin path', () => {
    expect(safeNext('/management-dashboard')).toBe('/management-dashboard')
    expect(safeNext('/schedule?tab=runs')).toBe('/schedule?tab=runs')
  })

  it('⚠ refuses a protocol-relative URL — that is an open redirect', () => {
    expect(safeNext('//evil.example.com')).toBe('/')
    expect(safeNext('https://evil.example.com')).toBe('/')
  })

  it('⚠ refuses a next that points back at the gate', () => {
    // Otherwise proving the factor returns to the challenge that was just satisfied.
    expect(safeNext(MFA_PATH)).toBe('/')
    expect(safeNext(`${MFA_PATH}/x`)).toBe('/')
  })

  it('falls back on absent or empty input', () => {
    expect(safeNext(null)).toBe('/')
    expect(safeNext(undefined)).toBe('/')
    expect(safeNext('')).toBe('/')
  })

  it('honours a caller-supplied fallback', () => {
    expect(safeNext(null, '/schedule')).toBe('/schedule')
  })
})

describe('requiresEnrolment', () => {
  const at = (nextLevel: 'aal1' | 'aal2' | null, pathname = '/schedule', isPublic = false) =>
    requiresEnrolment({ nextLevel, pathname, isPublic })

  it('⚠⚠ sends somebody with no authenticator to set one up', () => {
    // The half that makes two-factor MANDATORY. Without it, `requiresMfa` only ever fires for
    // people who already enrolled — so ignoring the security page means never being asked.
    expect(at('aal1')).toBe(true)
  })

  it('leaves an enrolled account alone', () => {
    expect(at('aal2')).toBe(false)
  })

  it('⚠ does not count an ABANDONED enrolment as enrolled', () => {
    // `nextLevel` filters the session's factors to `status === 'verified'`, so a half-finished
    // enrolment still reads as aal1. Counting it would promote someone to "enrolled" and then
    // strand them at a challenge no code can answer.
    expect(at('aal1')).toBe(true)
  })

  it('⚠⚠ never redirects away from the enrolment page — that is the loop', () => {
    expect(at('aal1', '/account/security')).toBe(false)
    expect(at('aal1', '/account/security/x')).toBe(false)
  })

  it('leaves the challenge page and the signed-out flow alone', () => {
    expect(at('aal1', '/mfa')).toBe(false)
    expect(at('aal1', '/login', true)).toBe(false)
    expect(at('aal1', '/auth/confirm', true)).toBe(false)
  })

  it('⚠ an unknown level does NOT force enrolment', () => {
    // null means the session could not be read, which is not evidence that nobody enrolled —
    // and being wrong here tells a fully set-up user to do what they have already done.
    expect(at(null)).toBe(false)
  })
})

describe('the two gates together', () => {
  it('⚠⚠ are mutually exclusive, so the order in proxy.ts cannot double-redirect', () => {
    // No factor  -> enrol, never challenge (there is nothing to challenge).
    expect(requiresEnrolment({ nextLevel: 'aal1', pathname: '/schedule', isPublic: false })).toBe(true)
    expect(requiresMfa({ currentLevel: 'aal1', nextLevel: 'aal1', pathname: '/schedule', isPublic: false })).toBe(false)
    // Has factor, unproved -> challenge, never enrol.
    expect(requiresEnrolment({ nextLevel: 'aal2', pathname: '/schedule', isPublic: false })).toBe(false)
    expect(requiresMfa({ currentLevel: 'aal1', nextLevel: 'aal2', pathname: '/schedule', isPublic: false })).toBe(true)
    // Done -> neither.
    expect(requiresEnrolment({ nextLevel: 'aal2', pathname: '/schedule', isPublic: false })).toBe(false)
    expect(requiresMfa({ currentLevel: 'aal2', nextLevel: 'aal2', pathname: '/schedule', isPublic: false })).toBe(false)
  })
})
