import { describe, expect, it } from 'vitest';
import { trimStop } from './provenanceText';

/**
 * The provenance card writes "A formula on the data: {how}." — it supplies the final period. A
 * `how` that carries its own therefore renders "..", which is invisible in the source (the string
 * reads perfectly well on its own line) and only shows up on screen. Measured on the segment
 * start-weight card: "…gives the book's return.."
 */
describe('trimStop', () => {
  it('drops a trailing period so the card can supply its own', () => {
    expect(trimStop('a sum of the Start wt column')).toBe('a sum of the Start wt column');
    expect(trimStop('a sum of the Start wt column.')).toBe('a sum of the Start wt column');
  });

  it('tolerates trailing whitespace after the period', () => {
    expect(trimStop('a count of the rows below.  ')).toBe('a count of the rows below');
  });

  it('leaves interior periods alone', () => {
    // ⚠ A `how` is often several sentences, and a formula is full of them.
    expect(trimStop("Now ÷ Start − 1 = €1.50 ÷ €1.00 − 1. Priced rows only."))
      .toBe("Now ÷ Start − 1 = €1.50 ÷ €1.00 − 1. Priced rows only");
  });

  it('does not eat a decimal or an ellipsis into nothing', () => {
    expect(trimStop('the ratio is 0.9985')).toBe('the ratio is 0.9985');
    expect(trimStop('')).toBe('');
  });
});


/**
 * ⚠⚠ THE `copied` BRANCH SUPPLIES ITS OWN PERIOD TOO, SINCE 2026-09-01. It used to ignore `how`
 * entirely — a copied figure has no arithmetic to explain — which left nowhere to put the caveat a
 * reader still needs about a number we did NOT compute, and the only way to surface one was to
 * mis-tag the field as a formula. The Return tile did exactly that: "A formula on the data: AIRS's
 * own cumulatief_rendement…" over a figure read straight off the sheet.
 *
 * Now it reads "…not computed here. {how}." — so a `how` carrying its own full stop renders "..",
 * the same invisible defect this file already guards on the formula branch.
 */
describe('a copied caveat obeys the same full-stop rule', () => {
  it('the Return tile’s own string is punctuation-safe', () => {
    const how = 'Flow-aware and includes income, over the calendar year — a deposit does not flatter it.';
    expect(trimStop(how)).toBe(
      'Flow-aware and includes income, over the calendar year — a deposit does not flatter it');
  });

  it('an em dash before the stop is left alone', () => {
    expect(trimStop('read as reported — nothing is recomputed.'))
      .toBe('read as reported — nothing is recomputed');
  });
});
