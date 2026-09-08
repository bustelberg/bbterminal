import { describe, expect, it } from 'vitest'

import { base32ToBytes, expectedTotp, explainCode } from './totp'

// ⚠⚠ RFC 6238's OWN TEST VECTOR, not a value captured from this implementation. A baseline
// snapshotted off the code under test pins that code's bugs as the contract — and the whole point
// of this module is to be authoritative about what a secret "should" be showing, so it has to
// agree with the standard rather than with itself.
//
// Secret "12345678901234567890" (ASCII) → base32 GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ.
// At T = 59s the 8-digit SHA-1 code is 94287082; the 6-digit form is its last six.
const RFC_SECRET = 'GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ'

describe('base32ToBytes', () => {
  it('decodes the RFC secret back to its ASCII bytes', () => {
    expect(new TextDecoder().decode(base32ToBytes(RFC_SECRET)!)).toBe('12345678901234567890')
  })

  it('tolerates the spacing the page prints for hand-entry', () => {
    // `groupSecret` shows "ABCD EFGH …" so it can be typed; a reader may paste it back with spaces.
    expect(base32ToBytes('GEZD GNBV GY3T QOJQ GEZD GNBV GY3T QOJQ'))
      .toEqual(base32ToBytes(RFC_SECRET))
  })

  it('refuses anything that is not base32 rather than returning noise', () => {
    // ⚠ `1`, `8` and `0` are not in the alphabet — silently mapping them to -1 would produce a
    // plausible-looking key and therefore a plausible-looking WRONG code.
    expect(base32ToBytes('not-base32!')).toBeNull()
    expect(base32ToBytes('ABC108')).toBeNull()
    expect(base32ToBytes('')).toBeNull()
  })
})

describe('expectedTotp', () => {
  it('⚠⚠ agrees with RFC 6238 at T=59s', async () => {
    expect(await expectedTotp(RFC_SECRET, 59_000)).toBe('287082')
  })

  it('agrees at the later RFC checkpoints too', async () => {
    // T=1111111109 → 07081804 ; T=1234567890 → 89005924  (8-digit forms)
    expect(await expectedTotp(RFC_SECRET, 1_111_111_109_000)).toBe('081804')
    expect(await expectedTotp(RFC_SECRET, 1_234_567_890_000)).toBe('005924')
  })

  it('holds the same code across one 30-second step and changes at the boundary', async () => {
    const a = await expectedTotp(RFC_SECRET, 60_000)
    const b = await expectedTotp(RFC_SECRET, 89_999)
    const c = await expectedTotp(RFC_SECRET, 90_000)
    expect(a).toBe(b)
    expect(c).not.toBe(a)
  })

  it('returns null on an unreadable secret rather than a wrong code', async () => {
    // ⚠ NOT `'not base32'` — strip the space and upper-case it and every letter of NOTBASE32 is
    // in the alphabet, so it decodes happily. The rejection has to be tested with a character
    // that genuinely is not: `1`, `8`, `0` and punctuation are the ones people actually mistype.
    expect(await expectedTotp('not-base32!')).toBeNull()
    expect(await expectedTotp('ABC1808')).toBeNull()
  })
})

describe('explainCode — the whole reason this module exists', () => {
  it('recognises a code that is simply right', async () => {
    const code = (await expectedTotp(RFC_SECRET, 59_000))!
    expect(await explainCode(RFC_SECRET, code, 59_000)).toEqual({ kind: 'matches' })
  })

  it('⚠⚠ separates a stale authenticator from a clock problem', async () => {
    // THE case this exists for. A code from a DIFFERENT secret matches no window of this one, so
    // the reader can be told their app is on an old entry instead of being sent, wrongly, to
    // check a clock that is fine.
    const other = 'JBSWY3DPEHPK3PXPJBSWY3DPEHPK3PXP'
    const fromOther = (await expectedTotp(other, 59_000))!
    expect(await explainCode(RFC_SECRET, fromOther, 59_000)).toEqual({ kind: 'wrong-secret' })
  })

  it('identifies skew, and by how much', async () => {
    // A phone two minutes fast: the code it shows belongs four steps ahead.
    const ahead = (await expectedTotp(RFC_SECRET, 59_000 + 4 * 30_000))!
    expect(await explainCode(RFC_SECRET, ahead, 59_000)).toEqual({ kind: 'clock-skew', offsetSteps: 4 })
  })

  it('finds the NEAREST window, so the reported drift is the smallest true one', async () => {
    const behind = (await expectedTotp(RFC_SECRET, 300_000 - 30_000))!
    const v = await explainCode(RFC_SECRET, behind, 300_000)
    expect(v).toEqual({ kind: 'clock-skew', offsetSteps: -1 })
  })

  it('says "unknown" rather than guessing when the secret cannot be read', async () => {
    expect(await explainCode('nonsense!!', '123456')).toEqual({ kind: 'unknown' })
  })

  it('⚠ treats six random digits as a wrong secret, not as skew', async () => {
    expect(await explainCode(RFC_SECRET, '000000', 59_000)).toEqual({ kind: 'wrong-secret' })
  })
})
