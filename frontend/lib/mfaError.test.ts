import { describe, expect, it } from 'vitest'

import { describeMfaError } from './mfaError'

describe('describeMfaError', () => {
  it('⚠⚠ names the environment as the cause when MFA is not switched on', () => {
    // The expected production failure: TOTP enabled in `config.toml` but not in the hosted
    // dashboard, so enrolment works locally and 422s for everybody in prod. The message must send
    // the reader to an admin, not to their own phone.
    const s = describeMfaError({ message: 'MFA enroll is disabled' })
    expect(s).toMatch(/not switched on|enable/i)
    expect(s).toMatch(/admin/i)
  })

  it('⚠ wins over the generic invalid-input case', () => {
    // GoTrue reports this as an unprocessable entity; a looser rule would call it a bad code and
    // send someone to check a clock that is fine.
    const s = describeMfaError({ code: 'mfa_enroll_disabled', message: 'invalid request' })
    expect(s).toMatch(/admin/i)
  })

  it('explains a duplicate name as something to change, not a failure', () => {
    expect(describeMfaError({ message: 'A factor with the friendly name already exists' }))
      .toMatch(/different name/i)
  })

  it('tells someone at the cap what to do about it', () => {
    expect(describeMfaError({ code: 'too_many_enrolled_mfa_factors' }))
      .toMatch(/remove one/i)
  })

  it('⚠ separates an expired challenge from a wrong code', () => {
    // Both are verification failures; only one is fixed by looking at the phone again.
    const expired = describeMfaError({ code: 'mfa_challenge_expired' })
    expect(expired).toMatch(/took too long|start again/i)
    expect(expired).not.toMatch(/clock/i)
  })

  it('gives the two things that actually cause a rejected code', () => {
    const s = describeMfaError({ message: 'Invalid TOTP code entered' })
    expect(s).toMatch(/30 seconds/)
    expect(s).toMatch(/clock/i)
  })

  it('never returns a raw library string or an empty one', () => {
    for (const input of [{}, { message: '' }, { message: 'AuthApiError: unprocessable_entity' }]) {
      const s = describeMfaError(input)
      expect(s.length).toBeGreaterThan(20)
      expect(s).not.toMatch(/AuthApiError|unprocessable/i)
    }
  })
})
