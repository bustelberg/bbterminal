import { describe, expect, it } from 'vitest';
import { classWeightedReturn, type ClassReturnRow } from './classReturn';

const row = (start_value_eur: number | null, result_eur: number | null): ClassReturnRow =>
  ({ start_value_eur, result_eur });

describe('classWeightedReturn', () => {
  it('is the class’s result over its opening value', () => {
    // 90% of the class opened in a name that made +10%, 10% in one that lost 10%.
    const r = classWeightedReturn([row(900, 90), row(100, -10)]);
    expect(r.pct).toBeCloseTo(8, 10);          // 80 / 1000
    expect(r.resultEur).toBe(80);
    expect(r.startEur).toBe(1000);
    expect(r.legs).toBe(2);
  });

  it('INCLUDES what was banked on a position that was trimmed', () => {
    // ⚠ THE BUG THIS REPLACED. The old form averaged each row's own return, which describes only
    // the shares still held — so a trim that banked real money appeared in the Result column and
    // in no percentage anywhere. Measured on AITopSelectie: EUR 6,307 realised, 0.63pp missing.
    const heldOnly = classWeightedReturn([row(1000, 100)]);
    const withRealised = classWeightedReturn([row(1000, 163)]);   // 100 unrealised + 63 realised
    expect(heldOnly.pct).toBeCloseTo(10, 10);
    expect(withRealised.pct).toBeCloseTo(16.3, 10);
  });

  it('is NOT a weighted average of per-row rates', () => {
    // Those rates each sit on their own denominator, so averaging them is not the class's return.
    // Here the small position doubled and the large one was flat: the honest class figure is
    // +9.09%, not the 50% an equal-weighted average of rates would suggest.
    const r = classWeightedReturn([row(1000, 0), row(100, 100)]);
    expect(r.pct).toBeCloseTo(100 / 1100 * 100, 10);
  });

  it('leaves a row with no opening value out of BOTH sides', () => {
    // ⚠ Summing every row's result over only the priced rows' opening value would divide one
    // population by another — the rate would exceed the truth by whatever the excluded rows made.
    const r = classWeightedReturn([row(1000, 100), row(null, 500)]);
    expect(r.pct).toBeCloseTo(10, 10);          // NOT 60
    expect(r.legs).toBe(1);
    expect(r.rows).toBe(2);
  });

  it('reports how much of the class’s result the rate speaks for', () => {
    const r = classWeightedReturn([row(1000, 100), row(null, 100)]);
    expect(r.coveredPct).toBeCloseTo(50, 10);   // half the money made is outside the ratio
  });

  it('reports full coverage when every row that made money has an opening value', () => {
    const r = classWeightedReturn([row(1000, 100), row(500, 50)]);
    expect(r.coveredPct).toBeCloseTo(100, 10);
  });

  it('treats a zero or negative opening value as a non-member', () => {
    const r = classWeightedReturn([row(1000, 100), row(0, 999)]);
    expect(r.pct).toBeCloseTo(10, 10);
    expect(r.legs).toBe(1);
  });

  it('returns null — not 0 — when nothing in the class was held at the open', () => {
    expect(classWeightedReturn([]).pct).toBeNull();
    expect(classWeightedReturn([row(null, null)]).pct).toBeNull();
    // Cash: a real row, real exposure today, no starting money to measure a rate against.
    const r = classWeightedReturn([row(null, 0)]);
    expect(r.pct).toBeNull();
    expect(r.rows).toBe(1);
  });

  describe('cash', () => {
    it('returns 0%, not a dash, when told there is no opening value to divide by', () => {
      // ⚠ AIRS books no Beginwaarde for a cash line. Without the flag this is `null`, which the
      // table renders as "—" — and a dash says "we could not work this out" about the one asset
      // whose return is certain.
      const r = classWeightedReturn([row(null, 0)], true);
      expect(r.pct).toBe(0);
      expect(r.coveredPct).toBe(100);
    });

    it('is 0% and NOT null, which is the whole distinction', () => {
      expect(classWeightedReturn([row(null, 0)]).pct).toBeNull();
      expect(classWeightedReturn([row(null, 0)], true).pct).toBe(0);
    });

    it('keeps any income it earned visible in the euro figure', () => {
      // Only the PRICE leg is asserted to be zero — interest credited to the account is real
      // money and still belongs in the Result column.
      const r = classWeightedReturn([row(null, 240)], true);
      expect(r.pct).toBe(0);
      expect(r.resultEur).toBe(240);
    });

    it('does not hijack a class that DOES have an opening value', () => {
      // The flag is a fallback for "nothing to divide by", not an override. A cash-like class
      // that AIRS did value must still report its real rate.
      const r = classWeightedReturn([row(1000, 50)], true);
      expect(r.pct).toBeCloseTo(5, 10);
    });
  });

  it('handles a class that lost money without flipping the coverage sign', () => {
    // ⚠ Coverage is on ABSOLUTE result: a loss counts as much as a gain toward "is this rate
    // describing all the money", and a signed ratio would go negative and read as nonsense.
    const r = classWeightedReturn([row(1000, -100), row(null, -100)]);
    expect(r.pct).toBeCloseTo(-10, 10);
    expect(r.coveredPct).toBeCloseTo(50, 10);
  });
});
