import { afterEach, describe, expect, it, vi } from 'vitest';
import katex from 'katex';

import { withWorked, workedCagr } from './workedFormula';
import {
  workedAllocation, workedContribution, workedInteraction, workedReturn as workedAttrReturn,
  workedSelection, workedTotal, workedWeight,
} from './attributionFormulas';
import {
  workedFairValue, workedMarketCap, workedMaxPE, workedPriceMove,
} from './valuationFormulas';
import { meanSub, rateSub } from './tablesSubstitution';

/**
 * NOTHING THIS APP TYPESETS MAY MAKE KaTeX WARN.
 *
 * ⚠⚠ THE FOUR `*.latex.test.ts` FILES BESIDE THIS ONE WERE ALL GREEN WHILE EVERY WORKED LINE IN
 * THE APP WARNED, AND THIS FILE EXISTS BECAUSE OF THAT. They render with
 * `{ throwOnError: true, strict: 'error' }` and assert it does not throw — which catches a parse
 * error and an unescaped `%`, and does NOT catch a `newLineInDisplayMode`, because that is a
 * WARNING and `strict: 'error'` does not promote it. Reported from the browser console:
 *
 *     LaTeX-incompatible input and strict mode is set to 'warn':
 *     In LaTeX, \\ or \newline does nothing in display mode [newLineInDisplayMode]
 *
 * `withWorked` had joined its two halves with a bare `\\[4pt]` since it was written, so the warning
 * fired twice per card, on every card, for months. The fix is one `\begin{gathered}` in that
 * helper; this is the test that would have found it.
 *
 * ⚠⚠ IT RENDERS WITH THE **APP'S** OPTIONS, NOT WITH STRICT ONES. `lib/formula.tsx` uses
 * `{ displayMode: true, throwOnError: false, trust: false }`, and `strict` therefore defaults to
 * `'warn'` — which is precisely the configuration under which this class of defect is invisible
 * (it degrades, renders something plausible, and tells only the console). Testing under stricter
 * options than production uses is how a warning survives a green suite.
 *
 * Pure — no DOM, no network.
 */

/** Render exactly as `lib/formula.tsx` does, capturing anything KaTeX complains about. */
function warningsFrom(tex: string): string[] {
  const spy = vi.spyOn(console, 'warn').mockImplementation(() => {});
  try {
    katex.renderToString(tex, { displayMode: true, throwOnError: false, trust: false });
    return spy.mock.calls.map((c) => c.join(' '));
  } finally {
    spy.mockRestore();
  }
}

afterEach(() => { vi.restoreAllMocks(); });

/** ⚠ SHAPED LIKE A REAL ONE, not minimal: the substitution prints every operand, so a stub
 *  missing a label would render a shorter line than the app does and dodge the very break the
 *  warning fires on. */
const CAGR = {
  rate: 0.197, from: 100, to: 606.3, fromLabel: '2015', toLabel: '2025', years: 10,
} as unknown as Parameters<typeof workedCagr>[0];

/**
 * One real output per builder in the app. ⚠ THE TWO-LINE FORM, not the rule alone — the defect
 * only appears once there is a second line to break to, so a fixture with a missing operand (which
 * collapses to the rule) would pass while the shipped card warned.
 */
const CASES: [string, string][] = [
  ['withWorked, two lines', withWorked('a = b', '1 = 2')],
  ['withWorked, one line', withWorked('a = b', '')],
  ['attribution: weight', workedWeight('18.4%')],
  ['attribution: return', workedAttrReturn('12.7%')],
  ['attribution: contribution', workedContribution('4.1%', '22.6%', '+0.93pp')],
  ['attribution: allocation', workedAllocation('18.4%', '11.2%', '9.8%', '14.7%', '-0.36pp')],
  ['attribution: selection', workedSelection('11.2%', '15.3%', '9.8%', '+0.62pp')],
  ['attribution: interaction', workedInteraction('18.4%', '11.2%', '15.3%', '9.8%', '+0.40pp')],
  ['attribution: total', workedTotal('-0.36pp', '+0.62pp', '+0.40pp', '+0.66pp')],
  ['valuation: max P/E', workedMaxPE(20, 0.10, 0.003, 0.10, 10, 20.608)],
  ['valuation: fair value', workedFairValue(11.46, 20.608, 236.17)],
  ['valuation: price move', workedPriceMove(596.13, 281.42, '+111.8%')],
  ['valuation: market cap', workedMarketCap(281.42, 2540, 714806)],
  ['workedCagr', workedCagr(CAGR)],
  ['tables: rate substitution', rateSub(CAGR)],
  // ⚠ THE TRANSFORMED FORM, which is the one that adds a SECOND line of its own
  // (the inversion) inside a string `withWorked` then wraps again — a nested break.
  ['tables: mean substitution', withWorked('m', meanSub(
    new Map([[2021, 12.5], [2022, 11.0], [2023, 9.5], [2024, 10.2], [2025, 11.8]]),
    2025, 5, (v: number) => 100 / v))],
];

describe('no KaTeX warning reaches the console', () => {
  it.each(CASES)('%s', (_name, tex) => {
    expect(warningsFrom(tex)).toEqual([]);
  });
});

describe('the check itself works', () => {
  /**
   * ⚠ A TEST THAT CANNOT FAIL IS NOT A TEST. This is the exact string `withWorked` used to emit;
   * if `warningsFrom` ever stops capturing, every case above goes green for the wrong reason.
   */
  it('a bare line break in display mode IS caught', () => {
    const warned = warningsFrom(String.raw`a = b \\[4pt] 1 = 2`);
    expect(warned.join(' ')).toContain('newLineInDisplayMode');
  });

  it('and the gathered form is not', () => {
    expect(warningsFrom(String.raw`\begin{gathered} a = b \\[4pt] 1 = 2 \end{gathered}`))
      .toEqual([]);
  });
});
