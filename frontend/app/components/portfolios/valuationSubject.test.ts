/**
 * The Fundamental modal's A/B valuation switch — /research-dashboard's two companies.
 *
 * ⚠⚠ WHAT THIS GUARDS IS A HEAD THAT NAMES THE WRONG COMPANY. Quick and Deep Valuation are the two
 * tabs that cannot draw a pair, so the switch points them at B — and neither tab prints the company
 * anywhere a reader looks first, which makes the modal's 2xl heading the only thing on screen
 * saying whose cash flows are being valued. `valued` and `shown` are deliberately different
 * answers; in the component that difference was four ternaries nothing could see.
 *
 * Pure. The wiring that carries these answers into the tabs is checked by `tablesCompare.test.ts`'s
 * neighbour below.
 */
import { readFileSync } from 'node:fs';
import { join } from 'node:path';

import { describe, expect, it } from 'vitest';

import { valuationSubject } from './valuationSubject';

const A = { isin: 'NL0010273215', name: 'ASML Holding NV' };
const B = { isin: 'US67066G1040', name: 'NVIDIA Corporation' };
const at = (tab: string, side: 'a' | 'b', compare: typeof B | null = B) =>
  valuationSubject({ isin: A.isin, name: A.name, compare, side, tab });

describe('with only company A', () => {
  it('never leaves A, on any tab', () => {
    for (const tab of ['longequity', 'tables', 'quickval', 'deepval']) {
      const r = at(tab, 'a', null);
      expect(r.valued).toEqual({ isin: A.isin, name: A.name });
      expect(r.shown).toEqual({ isin: A.isin, name: A.name });
    }
  });

  it('⚠ and a side left on `b` collapses to A rather than valuing nobody', () => {
    // The side lives in `useState`, so it survives company B being cleared. Without the guard the
    // tabs would read `compare` after it had gone — or need an effect to reset the side, which is
    // one render too late.
    expect(at('quickval', 'b', null).valued).toEqual({ isin: A.isin, name: A.name });
  });

  it('offers no switch', () => {
    expect(at('quickval', 'a', null).switchable).toBe(false);
  });
});

describe('with both companies', () => {
  it('starts on A', () => {
    expect(at('quickval', 'a').valued).toEqual({ isin: A.isin, name: A.name });
    expect(at('quickval', 'a').shown).toEqual({ isin: A.isin, name: A.name });
  });

  it.each(['quickval', 'deepval'])('switches BOTH the tab and the head on %s', (tab) => {
    const r = at(tab, 'b');
    expect(r.valued).toEqual({ isin: B.isin, name: B.name });
    // ⚠⚠ THE HALF THAT IS EASY TO MISS. A head still reading "ASML Holding NV" over NVIDIA's
    // reverse DCF attributes every figure under it to the wrong company, in the one line that
    // attributes anything — and it is also what `scope` is built from, so the Refresh button
    // would refetch A while the reader watched B.
    expect(r.shown).toEqual({ isin: B.isin, name: B.name });
    expect(r.switchable).toBe(true);
  });

  it.each(['longequity', 'tables'])(
    '⚠⚠ keeps the tabs on B but puts the head back on A on %s', (tab) => {
      const r = at(tab, 'b');
      // The two valuation tabs stay MOUNTED once visited. Reverting `valued` off-tab would
      // silently re-point a hidden panel at the other company, so the reader returns to a
      // different valuation than the one they left.
      expect(r.valued).toEqual({ isin: B.isin, name: B.name });
      // Graphs and Tables draw A as the subject with B on the benchmark line — the head names the
      // subject there, and the switch is not rendered, so the two can never disagree on screen.
      expect(r.shown).toEqual({ isin: A.isin, name: A.name });
      expect(r.switchable).toBe(false);
      expect(r.onValuationTab).toBe(false);
    });
});

describe('⚠ absent fields come back as the empty string and null, never undefined', () => {
  // Every caller writes `name || isin` for a display label, and `undefined` there is the string
  // "undefined" one optional chain away.
  it('fills a missing isin and name', () => {
    const r = valuationSubject({ side: 'a', tab: 'quickval' });
    expect(r.valued).toEqual({ isin: '', name: null });
    expect(r.shown).toEqual({ isin: '', name: null });
  });
});

describe('the modal carries the two answers to the right places', () => {
  const src = readFileSync(join(__dirname, 'OwnerEarningsModal.tsx'), 'utf8');

  it('⚠⚠ keys Deep Valuation on the picked ISIN and does NOT key Quick Valuation', () => {
    // Deep Valuation reads that company's saved assumptions in a state INITIALISER (localStorage,
    // per ISIN), so it has to remount — carrying A's growth rate and exit multiple into B's
    // reverse DCF is a complete, confident valuation of the wrong assumptions. Quick Valuation's
    // own state is a VIEW (which basis is charted), and resetting it would draw B on a different
    // basis from the A the reader had just set up, which is the one thing a comparison must not
    // do quietly.
    expect(src).toContain('<DeepValuationTab key={valued.isin}');
    expect(src).toContain('<QuickValuationTab isin={valued.isin}');
    expect(src).not.toContain('<QuickValuationTab key=');
  });

  it('⚠ scopes the fundamentals refresh to the company on screen', () => {
    expect(src).toContain("? { kind: 'company', isin: shownIsin, name: shownName || shownIsin }");
  });

  it('⚠ and neither valuation tab is handed the modal\'s own isin any more', () => {
    for (const bad of ['<QuickValuationTab isin={isin}', '<DeepValuationTab key={isin}']) {
      expect(src, `${bad} bypasses the switch`).not.toContain(bad);
    }
  });
});
