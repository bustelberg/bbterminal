/**
 * ⚠ A WRONG TITLE SPLIT IS WORSE THAN NO SPLIT, AND IT LOOKS FINE.
 *
 * Promoting the first clause of a sentence to a bold heading leaves a body that begins mid-thought.
 * The card still renders, still looks designed, and reads as gibberish — nothing errors, nothing
 * is empty, so no other check would catch it. Hence the conservative rule: a title must be SHORT
 * and must carry no sentence punctuation of its own.
 */
import { describe, expect, it } from 'vitest';

import { splitTipTitle } from './tipCard';

describe('splitTipTitle', () => {
  it('promotes a short leading term to the card title', () => {
    const out = splitTipTitle('Forward P/E — the price divided by next-year EPS.');
    expect(out.title).toBe('Forward P/E');
    expect(out.body).toBe('the price divided by next-year EPS.');
  });

  it('keeps text without an em dash whole', () => {
    const t = 'Diluted EPS for the most recent fiscal year. Used as the base for EGM.';
    expect(splitTipTitle(t)).toEqual({ body: t });
  });

  it('refuses a leading fragment that is really a sentence', () => {
    // ⚠ The dangerous case. This has a dash but the head is prose; bolding it and starting the
    // body at "so this bucket" would read as a broken card.
    const t = 'Funds, cash and unclassified holdings are not a sector bet. '
      + 'They are not decomposed — just the holdings in them.';
    expect(splitTipTitle(t)).toEqual({ body: t });
  });

  it('refuses a long leading fragment even with no punctuation', () => {
    const head = 'a'.repeat(60);
    expect(splitTipTitle(`${head} — tail`)).toEqual({ body: `${head} — tail` });
  });

  it('refuses when the head carries a colon or semicolon', () => {
    expect(splitTipTitle('Note: the weight — as of today').title).toBeUndefined();
    expect(splitTipTitle('One; two — three').title).toBeUndefined();
  });

  it('never returns an empty body', () => {
    // A trailing dash with nothing after it must not produce a titled card with no content.
    const t = 'Weight — ';
    expect(splitTipTitle(t)).toEqual({ body: t });
  });

  it('splits on the FIRST dash only, so the body keeps its own', () => {
    const out = splitTipTitle('Region — the domicile — never the venue');
    expect(out.title).toBe('Region');
    expect(out.body).toBe('the domicile — never the venue');
  });

  it('ignores a dash without surrounding spaces', () => {
    const t = 'Debt-to-Equity is total debt over equity.';
    expect(splitTipTitle(t)).toEqual({ body: t });
  });

  it('preserves paragraph breaks in the body', () => {
    // Call sites use \n\n for paragraphs and the card renders `whitespace-pre-line`.
    const out = splitTipTitle('Term — first line.\n\nsecond paragraph.');
    expect(out.body).toContain('\n\n');
  });
});
