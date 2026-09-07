/**
 * The Quick Valuation tab's copy, pinned in both languages.
 *
 * ⚠ THE COMPILER COVERS COMPLETENESS. What it cannot see is a Dutch entry copy-pasted from the
 * English, or the translated labels drifting from the ENGLISH ones in `BASIS` — which are still
 * used by the ⓘ prose and must stay in step with them as a SET (same bases, same fields), even
 * though the words differ.
 */
import { readFileSync } from 'node:fs';
import { join } from 'node:path';

import { describe, expect, it } from 'vitest';

import { QUICK_VALUATION_COPY as C } from './quickValuationCopy';
import { BASIS } from './quickValuation';

const LANGS = ['en', 'nl'] as const;
const BASES = ['fcf', 'eps'] as const;

describe('Quick Valuation copy', () => {
  it('covers both bases in both languages, with nothing blank', () => {
    for (const lang of LANGS) {
      expect(Object.keys(C[lang].basis).sort()).toEqual([...BASES].sort());
      for (const k of BASES) {
        for (const [field, v] of Object.entries(C[lang].basis[k])) {
          expect(v.trim(), `${lang}.${k}.${field}`).not.toBe('');
        }
      }
    }
  });

  it('⚠ the English labels still match `BASIS`, which the ⓘ prose reads', () => {
    // Two sources for one set of words is a drift risk; the English half is pinned to the config
    // so a rename there cannot leave the tab drawing the old label.
    for (const k of BASES) {
      expect(C.en.basis[k].tab).toBe(BASIS[k].tab);
      expect(C.en.basis[k].perShare).toBe(BASIS[k].perShare);
      expect(C.en.basis[k].yieldTitle).toBe(BASIS[k].yieldTitle);
      expect(C.en.basis[k].yieldInline).toBe(BASIS[k].yieldInline);
      expect(C.en.basis[k].multiple).toBe(BASIS[k].multiple);
      expect(C.en.basis[k].negativeYear).toBe(BASIS[k].negativeYear);
    }
  });

  it('⚠ translates the ordinary words', () => {
    expect(C.nl.priceCagr).not.toBe(C.en.priceCagr);
    expect(C.nl.currentSharePrice).not.toBe(C.en.currentSharePrice);
    expect(C.nl.priceTarget).not.toBe(C.en.priceTarget);
    expect(C.nl.median).not.toBe(C.en.median);
    expect(C.nl.avg).not.toBe(C.en.avg);
    expect(C.nl.latest).not.toBe(C.en.latest);
    expect(C.nl.legendSharePrice).not.toBe(C.en.legendSharePrice);
    expect(C.nl.noForwardFcf).not.toBe(C.en.noForwardFcf);
    expect(C.nl.priceVs('X')).not.toBe(C.en.priceVs('X'));
    expect(C.nl.indexedAt('2015')).not.toBe(C.en.indexedAt('2015'));
  });

  it('⚠ but keeps the terms a Dutch wealth manager actually says', () => {
    // The counterweight: `FCF`, `EPS`, `P/FCF`, `P/E` are what these are CALLED — the same
    // exception `managementCopy` records for "active share", "tracking error" and "Sharpe".
    expect(C.nl.basis.fcf.tab).toBe('FCF');
    expect(C.nl.basis.eps.tab).toBe('EPS');
    expect(C.nl.basis.fcf.multiple).toBe('P/FCF');
    expect(C.nl.basis.eps.multiple).toBe('P/E');
    expect(C.nl.basis.fcf.perShare).toContain('FCF');
  });

  it('⚠ the singular/plural switch works in both languages', () => {
    // English pluralises with an `s`; Dutch with `-en` on the compound. One year must not read as
    // "1 cash-burn years" in either.
    expect(C.en.notPlottable('1', 'cash-burn')).toContain('year ');
    expect(C.en.notPlottable('2', 'cash-burn')).toContain('years ');
    expect(C.nl.notPlottable('1', 'cash-burn')).toContain('cash-burnjaar ');
    expect(C.nl.notPlottable('2', 'cash-burn')).toContain('cash-burnjaren ');
  });
});

describe('⚠⚠ no DRAWN label is built from the English basis', () => {
  /**
   * THE MISTAKE THIS CATCHES WAS MADE THREE TIMES IN ONE SITTING, and each time it looked done.
   *
   * These components hold two label sets: `bl` (translated, for what is drawn) and `b` (English,
   * for the ⓘ prose, which is not translated yet). Wiring the tab meant rewriting ~20 labels, and
   * the ones missed — `${b.perShare} CAGR`, `Forward ${b.multiple}`, `As of`, `Loading…` — sat in
   * a Dutch tab in English with everything around them translated. Nothing failed; the tile simply
   * kept its old words.
   *
   * ⚠ SO THE RULE IS MECHANICAL: a `label=` or `name=` prop may not mention `b.`. The ⓘ cards
   * (`what=`, `where=`, `when=`, `how=`, `title=`) still may, and must — see `quickValuationCopy`.
   */
  const FILES = ['QuickValuationTab.tsx', 'MultipleHistoryChart.tsx', 'PriceTargetCalculator.tsx'];

  it.each(FILES)('%s draws no label off `b.`', (file) => {
    const src = readFileSync(join(__dirname, file), 'utf8');
    // `label={…b.…}` on one line — the shape every Stat/Row/legend label takes here.
    const offenders = src.split('\n')
      .map((line, i) => ({ line: line.trim(), n: i + 1 }))
      .filter(({ line }) => /\blabel=\{[^}]*\bb\.\w/.test(line));
    expect(offenders.map((o) => `${file}:${o.n} ${o.line.slice(0, 70)}`)).toEqual([]);
  });

  it('⚠ and the ⓘ cards still DO use it — the split is real, not a migration half-done', () => {
    // If this ever goes to zero, either the ⓘ prose got translated (in which case `b` should be
    // gone entirely and this test should be deleted) or someone "fixed" the split by pointing the
    // cards at the Dutch labels, which is the mixed-language sentence the module warns about.
    const src = readFileSync(join(__dirname, 'QuickValuationTab.tsx'), 'utf8');
    expect(/\bwhat=\{?[^\n]*\bb\.\w/.test(src) || /\bhow=\{?[^\n]*\bb\.\w/.test(src)).toBe(true);
  });
});
