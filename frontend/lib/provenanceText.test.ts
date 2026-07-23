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
