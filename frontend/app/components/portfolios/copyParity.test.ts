import { describe, expect, it } from 'vitest';

import { LANGS, type Lang } from '../../../lib/i18n';
import { MANAGEMENT_COPY } from '../management/managementCopy';
import { ANALYSE_COPY } from './analyseCopy';
import { ATTRIBUTION_COPY } from './attributionCopy';
import { DEEP_VALUATION_COPY } from './deepValuationCopy';
import { CHART_TITLES } from './longEquityCopy';
import { RISK_COPY } from './riskCopy';
import { BUCKET_DETAIL_COPY } from './bucketDetailCopy';
import { FUNDAMENTAL_CHROME_COPY } from './fundamentalChromeCopy';
import { QUICK_VALUATION_COPY } from './quickValuationCopy';

/**
 * Every copy module, in both languages, checked by one test.
 *
 *  The compiler already catches a missing key — each `nl` is declared as the module's own type,
 * so a string added to `en` and forgotten fails `tsc`. What it CANNOT catch is the two failures
 * that actually ship: a key that exists in Dutch and is still the English word, and a key that
 * exists and is empty. Both render as a screen that looks translated and is not, which is the one
 * state a reader has no way to report except as "it did not work".
 *
 *  One file for all of them, not one test per module, and that is the point. `managementCopy`
 * and `longEquityCopy` had their own; `riskCopy` and `attributionCopy` had none, which is how the
 * risk views could sit for weeks with a complete Dutch tree nothing rendered. A per-module test is
 * a thing somebody has to remember to write, and the evidence is that they do not. Adding a module
 * to `MODULES` is one line, and a module missing from it is visible here rather than nowhere.
 *
 *  Functions are checked for shape, not content. Half of this copy takes operands
 * (`benchMax(bench)`, `pairsMeasured(...)`), and calling them with invented arguments would assert
 * against a sentence nobody writes. What matters is that a key which is a function in English is a
 * function in Dutch — swapped for a bare string it fails at the call site, at render, as a crash.
 *
 *  The shared-string allowance is an explicit list, never a ratio. Some strings are correctly
 * identical: `Sharpe`, `R²`, `Active share`, AIRS's own Dutch field names. Left to a threshold
 * ("under 10% may match") the check passes while a whole section is untranslated; named one by one,
 * a new collision has to be argued for.
 *
 * Pure — no DOM, no network.
 */

const MODULES = {
  managementCopy: MANAGEMENT_COPY,
  riskCopy: RISK_COPY,
  analyseCopy: ANALYSE_COPY,
  attributionCopy: ATTRIBUTION_COPY,
  deepValuationCopy: DEEP_VALUATION_COPY,
  //  Every value here is a function (`(sbc: boolean) => string`), so this module is checked
  // for SHAPE only — which is still the check that matters: a title silently becoming a string
  // crashes the chart header at render.
  longEquityCopy: CHART_TITLES,
  //  ADDED 2026-09-07 WITH THE MODULES THEMSELVES. The registry is the point of this file —
  // a copy module absent from it is checked by nothing, and every one of these was written in
  // one sitting, which is exactly when a whole tree gets pasted from the English and forgotten.
  bucketDetailCopy: BUCKET_DETAIL_COPY,
  fundamentalChromeCopy: FUNDAMENTAL_CHROME_COPY,
  quickValuationCopy: QUICK_VALUATION_COPY,
} as const;

/**
 * Every leaf in one language's tree as `path -> 'fn'` or the string itself.
 *
 *  Walked, not listed. A hand-written list of paths is a second declaration of a shape whose whole
 * purpose is to grow a section at a time, and the one certainty about it is that it falls behind.
 */
function leaves(node: unknown, path = '', out: Record<string, string> = {}): Record<string, string> {
  if (typeof node === 'string') { out[path] = node; return out; }
  if (typeof node === 'function') { out[path] = '\u0000fn'; return out; }
  if (node && typeof node === 'object') {
    for (const [k, v] of Object.entries(node)) leaves(v, path ? `${path}.${k}` : k, out);
  }
  return out;
}

/**
 * Strings that are legitimately identical in both languages, in four NAMED groups.
 *
 *  Four groups and not one bag, because they are exempt for four different reasons and a future
 * collision has to be argued into one of them. A single flat list would let anything in — which is
 * the failure mode of a threshold ("under 10% may match"), just spelled differently.
 *
 *  The list grew once already, and this is where it was measured rather than guessed: the first
 * version held eight terms written from memory and the walk found nineteen real collisions across
 * five modules. Guessing which words survive translation is exactly the thing this test exists to
 * stop being done by eye.
 */

/** Said in English by a Dutch wealth manager. Translating them would be more Dutch and less clear. */
const FINANCE_EN = [
  'Sharpe', 'Sortino', 'R²', 'Active share', 'Tracking error', 'Information ratio', 'HHI',
  'Beta', 'Alpha', 'Drawdown', 'Overlap', 'Momentum', 'Multiple', 'Reverse DCF', 'Benchmark',
  'Benchmarks', 'YTD',
  //  The three P/E labels, same in both languages on request (2026-09-07: "Forward K/W should
  // still be P/E in dutch"). The Dutch block had been rendering `K/W`, which is a real Dutch term
  // and is what `deepValuationCopy`'s own vocabulary note already said NOT to use — it lists `P/E`
  // among the terms that stay. It is also what the Quick Valuation tab beside it renders in both
  // languages, and one modal calling the same ratio two things depending on the tab is worse than
  // either name. Only these exact labels; the SENTENCES that mention a P/E are still translated.
  'Forward P/E', 'Exit forward P/E', 'Max forward P/E',
  //  What two screens are called. A reader who has learned "Deep Valuation" should find it under
  // that name in either language; `Graphs`/`Tables` are ordinary words and DO translate, which is
  // why they are not here. Also pinned per-module in `fundamentalChromeCopy.test.ts`.
  'Quick Valuation', 'Deep Valuation',
  //  `cash-burn` IS SAID IN DUTCH TOO, and it is what a year of negative free cash flow is called
  // on the Quick Valuation tab. `verliesjaar` would name the EPS case, which is a different event —
  // the whole reason `negativeYear` is per basis rather than one word.
  'cash-burn',
];

/**
 * AIRS's OWN FIELD NAMES, which are already Dutch and appear unchanged in the ENGLISH tree.
 *
 *  That is the point, not an oversight — they are the names of the SOURCE FIELDS, so a reader
 * reconciling this screen against AirSPMS matches them by eye. Translating them into English would
 * break that link; re-translating them in Dutch would imply we had renamed something AIRS owns.
 */
const AIRS_FIELDS = [
  'Koers', 'Valuta', 'Rest', 'Beginwaarde', 'Huidige waarde', 'Werkelijk', 'Asset allocatie',
  'Res. YtD', 'Scan AIRS',
];

/** The same word in both languages. Nothing to translate. */
const SAME_WORD = ['Sector', 'sector', 'week', 'open', 'direct', 'Model', 'ISIN', 'EUR', 'Cash'];

/** A code identifier or symbol quoted verbatim — never prose. */
const IDENTIFIERS = ['signal_engine mom_12_1, EUR', 'ρ', 'Total', 'Ratio'];

const SHARED = new Set([...FINANCE_EN, ...AIRS_FIELDS, ...SAME_WORD, ...IDENTIFIERS]);

// Proper names and labels kept in English in both languages.
const SHARED_PATHS: Record<string, Set<string>> = {
  managementCopy: new Set([
    'page.tabs.bustelberg.label', 'page.tabs.toppenberg.label', 'page.tabs.topselecties.label',
  ]),
  deepValuationCopy: new Set([
    'egm.forwardPE', 'egm.fairValue', 'dcf.rowSbc', 'dcf.correctionSbc',
  ]),
  quickValuationCopy: new Set([
    'basis.fcf.yieldTitle', 'basis.fcf.yieldInline',
    'basis.eps.yieldTitle', 'basis.eps.yieldInline',
  ]),
};

/** A string worth comparing at all: prose, not a symbol, a number or a bare code. */
const comparable = (s: string) =>
  s.length > 3 && /[a-z]{3}/.test(s) && !SHARED.has(s.trim());

describe.each(Object.entries(MODULES))('%s', (name, copy) => {
  const EN = leaves(copy.en);
  const NL = leaves(copy.nl);

  it.each(LANGS)('%s has a non-empty value at every path', (lang: Lang) => {
    const entries = Object.entries(leaves(copy[lang]));
    expect(entries.length).toBeGreaterThan(0);
    for (const [path, value] of entries) {
      expect(value, `${name}.${lang}.${path} is empty`).not.toBe('');
    }
  });

  it('the two trees have exactly the same paths', () => {
    expect(Object.keys(NL).sort()).toEqual(Object.keys(EN).sort());
  });

  it('a key that is a function in English is a function in Dutch', () => {
    /**  A FUNCTION SWAPPED FOR A STRING FAILS AT RENDER, as `t.dd.benchMax is not a function` —
     *  a crash in a panel, not a translation gap, and nothing else would catch it. */
    const mismatched = Object.keys(EN).filter(
      (k) => (EN[k] === '\u0000fn') !== (NL[k] === '\u0000fn'),
    );
    expect(mismatched).toEqual([]);
  });

  it('no Dutch string is still the English one', () => {
    const same = Object.keys(EN).filter((k) => !SHARED_PATHS[name]?.has(k)).filter(
      (k) => EN[k] !== '\u0000fn' && comparable(EN[k]) && EN[k] === NL[k],
    );
    expect(same, `${name}: still English in nl — ${same.map((k) => `${k}="${EN[k]}"`).join(" | ")}`).toEqual([]);
  });
});
