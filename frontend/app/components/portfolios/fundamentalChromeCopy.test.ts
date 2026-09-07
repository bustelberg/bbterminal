/**
 * The Fundamental modal's chrome, pinned in both languages.
 *
 * ⚠ THE COMPILER COVERS COMPLETENESS — `FundamentalChromeCopy` will not build with a field missing
 * in either language. What it cannot see is a Dutch entry copy-pasted from the English and never
 * translated: it compiles, renders, and looks finished. Same shape as `analyseCopy.test.ts`.
 */
import { readFileSync } from 'node:fs';
import { join } from 'node:path';

import { describe, expect, it } from 'vitest';

import { FUNDAMENTAL_CHROME_COPY as C } from './fundamentalChromeCopy';

/** Flatten to `path -> string`, so the two languages can be compared leaf by leaf. */
const leaves = (o: object, prefix = ''): Record<string, string> =>
  Object.entries(o).reduce((acc: Record<string, string>, [k, v]) => {
    const path = prefix ? `${prefix}.${k}` : k;
    if (typeof v === 'string') acc[path] = v;
    else if (v && typeof v === 'object') Object.assign(acc, leaves(v, path));
    return acc;
  }, {});

const en = leaves(C.en);
const nl = leaves(C.nl);

describe('Fundamental modal chrome copy', () => {
  it('has the same complete shape in both languages, with nothing blank', () => {
    expect(Object.keys(nl).sort()).toEqual(Object.keys(en).sort());
    expect(Object.values(nl).filter((v) => !v.trim())).toEqual([]);
  });

  it('⚠ does not leave ordinary English labels in Dutch', () => {
    const sameByDesign = new Set([
      // ⚠ WHAT THESE TWO SCREENS ARE CALLED. Like `Sharpe` and `drawdown` elsewhere in this app,
      // they are the terms used out loud — a reader who has learned "Deep Valuation" should find
      // it under that name in either language. `Graphs`/`Tables` are ordinary words and do
      // translate, which is why they are NOT here.
      'tabs.quickval', 'tabs.deepval',
    ]);
    expect(Object.keys(en).filter((k) => en[k] === nl[k] && !sameByDesign.has(k))).toEqual([]);
  });

  it('⚠⚠ keeps the tab KEYS out of the copy — they are state, not words', () => {
    // `Tab` is `'longequity' | 'tables' | 'quickval' | 'deepval'`; `LongEquityTab` re-keys every
    // card's fetch off the cadence key beside it. A translated key would remount the tab and
    // refetch on a switch that should only repaint text. The keys appear here as the RECORD's
    // field names, never as its values.
    for (const lang of [C.en, C.nl]) {
      expect(Object.keys(lang.tabs).sort()).toEqual(['deepval', 'longequity', 'quickval', 'tables']);
      expect(lang.tabs.longequity).not.toBe('longequity');
    }
  });

  it('⚠⚠ the prose that NAMES a tab uses that language\'s own label for it', () => {
    /**
     * THE RULE CLAUDE.md ALREADY STATES AND NOTHING ENFORCED. When `longequity` was relabelled
     * `Graphs` (2026-09-03) the note recorded that "the prose that NAMED the tab did have to
     * follow — see `tablesCopy`". Translating the label to `Grafieken` broke exactly that in
     * Dutch, in three places, and the footnote then pointed a reader at a tab whose name is not on
     * screen in their language. Reported as a Dutch paragraph reading "het tabblad Graphs".
     *
     * ⚠ IT CHECKS BOTH DIRECTIONS. Containing the right label is not enough — a string that names
     * BOTH is a half-finished edit, and that is precisely the state this was in.
     */
    /**
     * ⚠ IT READS SOURCE, AND IT HAS TO. The three strings that name the tab live inside a
     * FUNCTION (`meanNote`) and a React node (the footnote), so `JSON.stringify(COPY[lang])`
     * cannot see any of them — the first cut of this test asserted over a serialised tree that
     * legitimately contains neither label, and passed the broken state.
     */
    const src = readFileSync(join(__dirname, 'tablesCopy.tsx'), 'utf8');
    const enBlock = src.slice(src.indexOf('const en: TablesCopy'), src.indexOf('const nl: TablesCopy'));
    const nlBlock = src.slice(src.indexOf('const nl: TablesCopy'), src.indexOf('export const COPY'));
    for (const [lang, block] of [['en', enBlock], ['nl', nlBlock]] as const) {
      const other = lang === 'en' ? 'nl' : 'en';
      const mine = C[lang].tabs.longequity;
      const theirs = C[other].tabs.longequity;
      expect(block.length, `${lang} block not found`).toBeGreaterThan(100);
      expect(block, `${lang} copy names the Graphs tab as "${theirs}"`).not.toContain(theirs);
      expect(block, `${lang} copy should name the Graphs tab as "${mine}"`).toContain(mine);
    }
  });

  it('⚠ translates the four refresh states, which are one control', () => {
    // The button becomes its own Cancel while a fill runs. Half-translated, it would change
    // language mid-press.
    for (const k of ['refresh', 'refreshUniverse', 'refreshing', 'cancel', 'cancelling'] as const) {
      expect(C.nl[k], k).not.toBe(C.en[k]);
    }
  });
});
