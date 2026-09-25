import { describe, expect, it } from 'vitest';
import { classWeightedReturn, type ClassReturnRow } from './classReturn';

const row = (start_value_eur: number | null, own_return_pct: number | null): ClassReturnRow =>
  ({ start_value_eur, own_return_pct });

describe('classWeightedReturn', () => {
  it('opening-value weights the same AIRS returns printed on the child rows', () => {
    const r = classWeightedReturn([row(900, 10), row(100, -10)]);
    expect(r.pct).toBeCloseTo(8, 10);
    expect(r.returnEur).toBeCloseTo(80, 10);
    expect(r.startEur).toBe(1000);
    expect(r.legs).toBe(2);
  });

  it('reconciles the measured Consumer Defensive subtotal', () => {
    const r = classWeightedReturn([
      row(33_812.21, -1.2647816383951183),
      row(18_330.00, 5.235122412134907),
      row(17_563.10, 7.832648286325594),
    ]);
    expect(r.returnEur).toBeCloseTo(1_907.6031657044, 8);
    expect(r.startEur).toBeCloseTo(69_705.31, 8);
    expect(r.pct).toBeCloseTo(2.7366683624, 8);
  });

  it('is value weighted, never an equal-weighted average of row rates', () => {
    const r = classWeightedReturn([row(1000, 0), row(100, 100)]);
    expect(r.pct).toBeCloseTo(100 / 1100 * 100, 10);
  });

  it('leaves a row with no opening value out of both sides', () => {
    const r = classWeightedReturn([row(1000, 10), row(null, 50)]);
    expect(r.pct).toBeCloseTo(10, 10);
    expect(r.legs).toBe(1);
    expect(r.rows).toBe(2);
    expect(r.coveredPct).toBe(100);
  });

  it('reports coverage by eligible opening value when a return is missing', () => {
    const r = classWeightedReturn([row(1000, 10), row(500, null)]);
    expect(r.pct).toBeCloseTo(10, 10);
    expect(r.coveredPct).toBeCloseTo(1000 / 1500 * 100, 10);
    expect(r.missing).toBe(1);
  });

  it('treats a zero or negative opening value as a non-member', () => {
    const r = classWeightedReturn([row(1000, 10), row(0, 999), row(-10, 999)]);
    expect(r.pct).toBeCloseTo(10, 10);
    expect(r.legs).toBe(1);
  });

  it('returns null rather than 0 when nothing has both operands', () => {
    expect(classWeightedReturn([]).pct).toBeNull();
    expect(classWeightedReturn([row(null, null)]).pct).toBeNull();
    expect(classWeightedReturn([row(1000, null)]).pct).toBeNull();
  });

  describe('cash', () => {
    it('returns 0%, not a dash, when explicitly told cash has no opening value', () => {
      const r = classWeightedReturn([row(null, null)], true);
      expect(r.pct).toBe(0);
      expect(r.returnEur).toBe(0);
      expect(r.coveredPct).toBe(100);
    });

    it('does not override a cash-like row that has a real opening value and return', () => {
      expect(classWeightedReturn([row(1000, 5)], true).pct).toBeCloseTo(5, 10);
    });
  });
});
