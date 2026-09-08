import { describe, expect, it } from 'vitest'

import {
  CODE_LENGTH, type Factor, groupSecret, isCompleteCode, normaliseCode, qrSvg, suggestName,
  unverifiedIds, verifiedFactors, verifyOrder,
} from './mfaFactors'

const factor = (id: string, status: Factor['status'], friendly_name?: string): Factor => ({
  id, status, friendly_name, factor_type: 'totp', created_at: '2026-09-08T00:00:00Z',
})

describe('unverified factors', () => {
  it('finds the leftovers an abandoned enrolment creates', () => {
    // ⚠ THE TRAP THIS EXISTS FOR: pressing Add and closing the tab leaves a REAL factor that
    // counts against `max_enrolled_factors`. Ten closed tabs and enrolment starts failing with
    // "too many factors" on a page listing none of them.
    const all = [factor('a', 'verified'), factor('b', 'unverified'), factor('c', 'unverified')]
    expect(unverifiedIds(all)).toEqual(['b', 'c'])
  })

  it('never sweeps a verified one', () => {
    expect(unverifiedIds([factor('a', 'verified')])).toEqual([])
  })

  it('shows only the factors that actually protect the account', () => {
    const all = [factor('a', 'verified'), factor('b', 'unverified')]
    expect(verifiedFactors(all).map((f) => f.id)).toEqual(['a'])
  })
})

describe('verifyOrder', () => {
  it('⚠⚠ tries the OTHER authenticators first, so a lost device can still be removed', () => {
    // The commonest reason to remove a factor is that the phone is gone. Checking the code
    // against the factor being removed first would make a spare device useless — you could enrol
    // one and still be unable to clear the dead entry.
    const all = [factor('lost', 'verified'), factor('spare', 'verified')]
    expect(verifyOrder(all, 'lost').map((f) => f.id)).toEqual(['spare', 'lost'])
  })

  it('still includes the target last, which is the only option when it is the only factor', () => {
    const all = [factor('only', 'verified')]
    expect(verifyOrder(all, 'only').map((f) => f.id)).toEqual(['only'])
  })

  it('never offers an unverified factor as proof', () => {
    const all = [factor('a', 'verified'), factor('half', 'unverified')]
    expect(verifyOrder(all, 'a').map((f) => f.id)).toEqual(['a'])
  })
})

describe('normaliseCode', () => {
  it('⚠ strips the space authenticator apps display', () => {
    // Apps show "123 456"; people copy the space with it. A stray character fails verification,
    // which sends the reader to check their phone's clock over a typo they cannot see.
    expect(normaliseCode('123 456')).toBe('123456')
  })

  it('strips a non-breaking space and stray punctuation too', () => {
    expect(normaliseCode('123 456')).toBe('123456')
    expect(normaliseCode('123-456')).toBe('123456')
  })

  it('truncates rather than accepting a longer string', () => {
    expect(normaliseCode('1234567890')).toBe('123456')
  })

  it('recognises a complete code and refuses a short one', () => {
    expect(isCompleteCode('123 456')).toBe(true)
    expect(isCompleteCode('12345')).toBe(false)
    expect(isCompleteCode('')).toBe(false)
    expect(CODE_LENGTH).toBe(6)
  })
})

describe('groupSecret', () => {
  it('breaks base32 into readable groups', () => {
    expect(groupSecret('ABCDEFGHIJKLMNOP')).toBe('ABCD EFGH IJKL MNOP')
  })

  it('leaves a short tail alone rather than padding it', () => {
    expect(groupSecret('ABCDEF')).toBe('ABCD EF')
  })

  it('handles an empty secret without producing " "', () => {
    expect(groupSecret('')).toBe('')
  })
})

describe('suggestName', () => {
  it('⚠ avoids the duplicate GoTrue rejects, which would only surface after the QR is scanned', () => {
    const all = [factor('a', 'verified', 'Authenticator')]
    expect(suggestName(all)).toBe('Authenticator 2')
  })

  it('ignores case when checking what is taken', () => {
    expect(suggestName([factor('a', 'verified', 'authenticator')])).toBe('Authenticator 2')
  })

  it('does not count an unverified leftover as taking the name', () => {
    // Those are swept before enrolment, so the name is free by the time it is used.
    expect(suggestName([factor('a', 'unverified', 'Authenticator')])).toBe('Authenticator')
  })

  it('never returns an empty name', () => {
    expect(suggestName([])).toBeTruthy()
  })
})

describe('qrSvg', () => {
  it('⚠⚠ gives the QR a viewBox, or CSS CLIPS it instead of scaling it', () => {
    // The real tag, measured: `<svg width="219" height="219">` and no viewBox. Without a
    // coordinate system, CSS width/height resizes the WINDOW, not the drawing — so `w-44`
    // (192.5px at this app's 17.5px rem) painted a QR with 12% cut off the right and bottom,
    // taking two of the three finder patterns with it. A phone can partially decode that into a
    // WRONG secret, and then every code it shows is valid-looking and unverifiable.
    const raw = '<?xml version="1.0"?>\n'
      + '<svg width="219" height="219" xmlns="http://www.w3.org/2000/svg"><rect/></svg>'
    const out = qrSvg(raw)
    expect(out).toContain('viewBox="0 0 219 219"')
    expect(out.startsWith('<svg')).toBe(true)
    // ⚠ The fixed size must GO. Left in, it still beats CSS in some engines.
    expect(/<svg[^>]*\swidth=/.test(out)).toBe(false)
    expect(/<svg[^>]*\sheight=/.test(out)).toBe(false)
    expect(out).toContain('<rect/>')
  })

  it('leaves an SVG that already scales alone', () => {
    const raw = '<svg viewBox="0 0 10 10" width="219" height="219"><rect/></svg>'
    expect(qrSvg(raw)).toBe(raw)
  })

  it('⚠ drops the XML prolog GoTrue actually sends', () => {
    // Through `innerHTML` the parser is in HTML mode, where an XML declaration becomes a bogus
    // comment — error recovery nobody chose, on the one image the whole enrolment turns on.
    const out = qrSvg('<?xml version="1.0"?>\n<!DOCTYPE svg><svg viewBox="0 0 1 1"><rect/></svg>')
    expect(out).toBe('<svg viewBox="0 0 1 1"><rect/></svg>')
  })

  it('⚠ falls back to the input rather than to empty', () => {
    // A QR that renders oddly is recoverable; a blank square is not.
    expect(qrSvg('not markup at all')).toBe('not markup at all')
    expect(qrSvg('<svg><rect/></svg>')).toBe('<svg><rect/></svg>')   // no size to derive from
  })
})
