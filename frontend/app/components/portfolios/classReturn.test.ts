import { describe, expect, it } from 'vitest';
import { classWeightedReturn, type ClassReturnRow } from './classReturn';

const row = (weight_pct: number | null, own_return_pct: number | null): ClassReturnRow =>
  ({ weight_pct, own_return_pct });

describe('classWeightedReturn', () => {
  it('weights by the OPENING share, not by an equal split', () => {
    // 90% of the class opened in a +10% name, 10% in a −10% one.
    const r = classWeightedReturn([row(45, 10), row(5, -10)]);
    expect(r.pct).toBeCloseTo(8, 10);      // not 0, which an unweighted mean would give
    expect(r.legs).toBe(2);
    expect(r.coveredPct).toBeCloseTo(100, 10);
  });

  it('is invariant to the denominator the weights arrive on', () => {
    // `weight_pct` (priced-book share) and `weight_start_pct` (whole-book share) differ only by a
    // constant factor; renormalising within the class must cancel it, or the two payload fields
    // would give the class two different returns.
    const a = classWeightedReturn([row(45, 10), row(5, -10)]);
    const b = classWeightedReturn([row(4.5, 10), row(0.5, -10)]);
    expect(b.pct).toBeCloseTo(a.pct!, 10);
  });

  it('does NOT reproduce the now-weighted figure — the whole reason it exists', () => {
    // The +100% name opened at 10% of the class and is 20% of it today. Start-weighted the class
    // made +18.0%; weighting by today's share reads +26.7% — plausible, and inflated by the
    // winner's own return.
    const start = classWeightedReturn([row(10, 100), row(90, 9)]);
    expect(start.pct).toBeCloseTo(18.1, 10);
    const nowWeighted = 0.2 * 100 + 0.8 * 9;
    expect(nowWeighted).toBeGreaterThan(start.pct! + 5);
  });

  it('drops an unpriceable leg from BOTH sides and reports the weight it took with it', () => {
    const r = classWeightedReturn([row(60, 10), row(40, null)]);
    expect(r.pct).toBeCloseTo(10, 10);     // renormalised over the 60 that could be priced
    expect(r.legs).toBe(1);
    expect(r.weighed).toBe(2);
    expect(r.coveredPct).toBeCloseTo(60, 10);
  });

  it('counts a row with no opening weight as not-held-at-open, never as covered', () => {
    // Cash (no Beginwaarde) and a name bought mid-window. Both have real exposure today and no
    // share of the opening book — they are out of the average by construction, and saying so is
    // different from reporting a pricing gap.
    const r = classWeightedReturn([row(100, 5), row(null, null), row(null, 3)]);
    expect(r.pct).toBeCloseTo(5, 10);
    expect(r.notHeldAtOpen).toBe(2);
    expect(r.coveredPct).toBeCloseTo(100, 10);   // NOT 33 — the two were never in the denominator
  });

  it('treats a zero or negative opening weight as a non-member', () => {
    const r = classWeightedReturn([row(100, 5), row(0, 999)]);
    expect(r.pct).toBeCloseTo(5, 10);
    expect(r.weighed).toBe(1);
    expect(r.notHeldAtOpen).toBe(1);
  });

  it('returns null — not 0 — when nothing in the class can be weighed and priced', () => {
    expect(classWeightedReturn([]).pct).toBeNull();
    expect(classWeightedReturn([row(null, null)]).pct).toBeNull();
    // Weighed but wholly unpriceable: there IS opening weight, and none of it has a return.
    const r = classWeightedReturn([row(100, null)]);
    expect(r.pct).toBeNull();
    expect(r.weighed).toBe(1);
    expect(r.coveredPct).toBe(0);
  });

  it('equals the sum of the per-holding contributions the sleeve view prints', () => {
    // The identity that keeps the class header and SleeveBreakdown's Contribution column from
    // disagreeing: both renormalise the same `weight_pct` within the class.
    const rows = [row(30, 12.5), row(45, -3.25), row(25, 7)];
    const total = rows.reduce((s, x) => s + x.weight_pct!, 0);
    const contribSum = rows.reduce((s, x) => s + (x.weight_pct! / total) * x.own_return_pct!, 0);
    expect(classWeightedReturn(rows).pct).toBeCloseTo(contribSum, 10);
  });
});
