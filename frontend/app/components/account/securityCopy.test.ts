import { describe, expect, it } from 'vitest'

import { SECURITY_COPY, type SecurityCopy } from './securityCopy'

const LANGS = ['en', 'nl'] as const

/** Every plain-string key. The two function keys are exercised separately. */
const stringKeys = (Object.keys(SECURITY_COPY.en) as (keyof SecurityCopy)[])
  .filter((k) => typeof SECURITY_COPY.en[k] === 'string')

describe('the security page copy', () => {
  it('covers every language with no empty string', () => {
    expect(stringKeys.length).toBeGreaterThan(20)
    for (const lang of LANGS) {
      for (const key of stringKeys) {
        expect(String(SECURITY_COPY[lang][key]).trim(), `${lang}.${key}`).not.toBe('')
      }
    }
  })

  it('⚠ actually translates — it is not the English block copied across', () => {
    // A copy module that compiles with the source language duplicated is the silent half of a
    // half-translated screen. These four are ordinary words, so they must differ.
    for (const key of ['title', 'remove', 'cancel', 'loading'] as const) {
      expect(SECURITY_COPY.nl[key], key).not.toBe(SECURITY_COPY.en[key])
    }
  })

  it('builds the removal heading around the authenticator name in both languages', () => {
    for (const lang of LANGS) {
      expect(SECURITY_COPY[lang].removeTitle('iPhone')).toContain('iPhone')
      expect(SECURITY_COPY[lang].addedOn('8 Sep 2026')).toContain('8 Sep 2026')
    }
  })

  it('⚠⚠ the clock warning blames THIS COMPUTER, never the phone', () => {
    // The measurement that settles it is browser-vs-SERVER, and in practice a phone on automatic
    // time is right while a laptop drifts. The first version of the failure copy assumed the
    // opposite and sent somebody to change a setting that was already correct.
    for (const lang of LANGS) {
      const en = lang === 'en';
      const msg = SECURITY_COPY[lang].clockWarning(59, false);
      expect(msg).toContain('59');
      expect(msg).toMatch(en ? /this computer/i : /deze computer/i);
      expect(msg).toMatch(en ? /phone is almost certainly fine/i : /telefoon ligt het vrijwel zeker niet/i);
    }
  });

  it('⚠ names the direction, since "out by 59s" does not say which way', () => {
    expect(SECURITY_COPY.en.clockWarning(59, true)).toMatch(/ahead/i);
    expect(SECURITY_COPY.en.clockWarning(59, false)).toMatch(/behind/i);
    expect(SECURITY_COPY.nl.clockWarning(59, true)).toMatch(/voor op/i);
    expect(SECURITY_COPY.nl.clockWarning(59, false)).toMatch(/achter op/i);
  });
})
