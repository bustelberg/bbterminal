/**
 * The `Tables` tab's two languages.
 *
 * ⚠⚠ THE TYPE ALREADY GUARANTEES THE DUTCH KEYS EXIST — IT GUARANTEES NOTHING ABOUT THE DUTCH. A
 * `TablesCopy` whose every field is the English string satisfies `tsc` completely, and that is the
 * realistic failure here: copy is added to `en`, pasted into `nl` to make the build pass, and the
 * intent to translate it later is lost the moment it compiles. Nothing on screen distinguishes an
 * untranslated string from one that is the same in both languages, so it has to be asserted.
 *
 * ⚠ WHICH IS WHY `SHARED` IS AN EXPLICIT ALLOW-LIST, AND WHY IT IS CURRENTLY EMPTY. It held
 * "ROIC" while the Dutch chip kept the English acronym; that label is now spelled out, so no string
 * survives untranslated. Keeping the mechanism with nothing in it is the point — the next genuine
 * overlap has to be added deliberately rather than slipping through as an oversight.
 *
 * Pure — no DOM required.
 */
import { describe, expect, it } from 'vitest';

import { LANGS, type Lang } from '../../../lib/i18n';
import { COPY, MEASURE_KEYS, RATE_KEYS } from './tablesCopy';

/** Strings that are legitimately identical in both languages. Every entry is a decision. */
const SHARED = new Set<string>([]);

describe('both languages are complete', () => {
  it.each(LANGS)('%s has a non-empty string for every row', (lang: Lang) => {
    const c = COPY[lang];
    for (const k of MEASURE_KEYS) {
      expect(c.chip[k].trim(), `chip.${k}`).not.toBe('');
      expect(c.rowLabel[k].trim(), `rowLabel.${k}`).not.toBe('');
      // Both SBC states, because the margin note branches on it.
      expect(c.rowNote[k](true).trim(), `rowNote.${k}(sbc)`).not.toBe('');
      expect(c.rowNote[k](false).trim(), `rowNote.${k}`).not.toBe('');
      expect(c.rowFormula[k](true).trim(), `rowFormula.${k}(sbc)`).not.toBe('');
      expect(c.rowFormula[k](false).trim(), `rowFormula.${k}`).not.toBe('');
    }
  });

  /**
   * ⚠⚠ EVERY FORMULA IS **LaTeX**, BECAUSE THE ⓘ TYPESETS IT (2026-08-31).
   *
   * This replaces a guard that no longer applies: while the tooltip was PROSE, `AboutCard` promoted
   * whatever preceded the first ' — ' to a bold heading, so a formula could be split mid-expression
   * — hence the old "no em dash, and comfortably longer than 48 characters" rule. The ⓘ now hands
   * the formula to `Formula`/KaTeX as the card's `worked`, where neither the em dash nor the length
   * means anything, and where the LENGTH RULE FOUGHT THE BRIEF: these tooltips are meant to be the
   * bare minimum.
   *
   * ⚠ WHAT REPLACES IT IS THE STRONGER CHECK — that the string really is an expression rather than
   * prose that would be set as a row of italic variables. `tablesCopy.latex.test.ts` renders every
   * one of them in STRICT mode, which is the half that catches a silent `%` truncation.
   */
  it.each(LANGS)('%s formulas are typeset expressions, not prose', (lang: Lang) => {
    const c = COPY[lang];
    for (const k of MEASURE_KEYS) {
      for (const sbc of [false, true]) {
        const f = c.rowFormula[k](sbc);
        expect(f, `rowFormula.${k}(${sbc}) is LaTeX`).toMatch(/\\[a-z]+/);
        // ⚠ AND NOT THE UNICODE LOOKALIKES IT REPLACED. `Σ(w × x) ÷ Σw` in the UI font is a
        // row of glyphs that resembles an expression: a summation with no limits, and `Σ`
        // given the advance width of a comma. Same rule as `lib/tipCard`'s `Worked`.
        for (const glyph of ['÷', '×', 'Σ', '^ (']) {
          expect(f, `rowFormula.${k}(${sbc}) still spells ${glyph} as a glyph`)
            .not.toContain(glyph);
        }
      }
    }
  });

  /**
   * ⚠ AND THE NOTE IS ONE SHORT SENTENCE. It is the card's TITLE (`AspectCard`'s `what`), read
   * beside a typeset formula; the essays that used to sit here were the reason nobody read either.
   * Asked for 2026-08-31: "the info icon text should be very short with bare minimum info".
   */
  it.each(LANGS)('%s row notes stay short enough to read at a glance', (lang: Lang) => {
    const c = COPY[lang];
    for (const k of MEASURE_KEYS) {
      for (const sbc of [false, true]) {
        expect(c.rowNote[k](sbc).length, `rowNote.${k}(${sbc})`).toBeLessThanOrEqual(150);
      }
    }
  });

  it('the Dutch is actually Dutch, not the English pasted across', () => {
    const en = COPY.en;
    const nl = COPY.nl;
    const same: string[] = [];
    const check = (label: string, a: string, b: string) => {
      if (a === b && !SHARED.has(a)) same.push(`${label}: ${JSON.stringify(a)}`);
    };

    check('title', en.title([5, 10]), nl.title([5, 10]));
    check('rowsLabel', en.rowsLabel, nl.rowsLabel);
    check('loading', en.loading, nl.loading);
    check('noRows', en.noRows, nl.noRows);
    check('colMeasure', en.colMeasure, nl.colMeasure);
    check('colExcess', en.colExcess, nl.colExcess);
    check('lastWindowLocked', en.lastWindowLocked, nl.lastWindowLocked);
    check('showWindow', en.showWindow(5), nl.showWindow(5));
    check('hideRow', en.hideRow('X'), nl.hideRow('X'));
    check('showRow', en.showRow('X'), nl.showRow('X'));
    check('rateTip', en.rateTip('FY2020', 'FY2025', 5), nl.rateTip('FY2020', 'FY2025', 5));
    check('meanTip', en.meanTip(4, 'FY2021', 'FY2025', 5), nl.meanTip(4, 'FY2021', 'FY2025', 5));
    for (const k of MEASURE_KEYS) {
      check(`chip.${k}`, en.chip[k], nl.chip[k]);
      check(`rowLabel.${k}`, en.rowLabel[k], nl.rowLabel[k]);
      check(`rowNote.${k}`, en.rowNote[k](true), nl.rowNote[k](true));
      // ⚠ A FORMULA IS MOSTLY SYMBOLS, which makes it the easiest thing in this file to leave in
      // English by accident — the ÷ and the Σ look translated. The words around them are not.
      check(`rowFormula.${k}`, en.rowFormula[k](true), nl.rowFormula[k](true));
    }

    expect(same, 'untranslated (add to SHARED if deliberate)').toEqual([]);
  });
});

describe('the interpolated strings actually interpolate', () => {
  it.each(LANGS)('%s puts its arguments in the sentence', (lang: Lang) => {
    const c = COPY[lang];
    expect(c.showWindow(10)).toContain('10');
    expect(c.hideRow('ROIC')).toContain('ROIC');
    expect(c.showRow('ROIC')).toContain('ROIC');

    const rate = c.rateTip('FY2019', 'FY2024', 5);
    expect(rate).toContain('FY2019');
    expect(rate).toContain('FY2024');
    expect(rate).toContain('5');

    const mean = c.meanTip(4, 'FY2021', 'FY2025', 5);
    expect(mean).toContain('FY2021');
    expect(mean).toContain('FY2025');
    // ⚠ AN INCOMPLETE WINDOW NAMES BOTH NUMBERS — "4" alone under a "5y" heading is the claim the
    // `(4/5)` badge exists to refuse, and the tooltip must not quietly drop the denominator.
    expect(mean).toContain('4');
    expect(mean).toContain('5');
    // A complete window says nothing about "of the N asked for" — there is nothing short about
    // it, so the sentence is strictly shorter than the one that has to qualify itself.
    expect(c.meanTip(5, 'FY2021', 'FY2025', null).length)
      .toBeLessThan(c.meanTip(4, 'FY2021', 'FY2025', 5).length);
  });

  it.each(LANGS)('%s titles follow the window selection', (lang: Lang) => {
    const c = COPY[lang];
    expect(c.title([5])).toContain('5');
    // Both windows shown: the heading spells them out rather than printing "5"…
    expect(c.title([5, 10])).not.toBe(c.title([5]));
  });
});

describe('the rate rows', () => {
  /**
   * ⚠ ONE RATE ROW PER LEVEL CHART ON THE LONG EQUITY TAB. The table exists so a reader does not
   * have to eyeball a compounding rate off a log axis, and it summarised three of the six level
   * charts until 2026-08-25. This pins the set rather than the count, so adding a seventh level
   * chart without a row is a visible omission rather than a silent one.
   */
  it('covers every level chart and nothing else', () => {
    expect([...RATE_KEYS].sort()).toEqual(
      ['fcfCagr', 'invCapCagr', 'priceCagr', 'revCagr', 'sharesCagr', 'epsCagr'].sort());
  });

  it('is a subset of the declared rows, so every rate has a label', () => {
    for (const k of RATE_KEYS) expect(MEASURE_KEYS).toContain(k);
  });

  /**
   * ⚠⚠ `epsFwd` IS A RATE AND IS DELIBERATELY NOT IN `RATE_KEYS`, which is exactly the kind of
   * omission somebody "fixes". The list gates the footnote clause about point-to-point rates
   * disagreeing with the Long Equity growth cards — a statement about measuring HISTORY. A forecast
   * has no card to disagree with, and pulling it in would print a caveat about a divergence that
   * cannot occur.
   */
  it('excludes the forward row on purpose', () => {
    expect(MEASURE_KEYS).toContain('epsFwd');
    expect(RATE_KEYS as readonly string[]).not.toContain('epsFwd');
  });
});
