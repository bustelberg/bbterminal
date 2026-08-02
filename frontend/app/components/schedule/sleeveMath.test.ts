import { describe, it, expect } from 'vitest';
import { fmtSleevePct, parsePct, stockSleevePct, validateSleeves } from './sleeveMath';

const etf = (benchmarkId: number | null, weightPct: string) => ({ benchmarkId, weightPct });

describe('parsePct', () => {
  it('treats a half-typed row as 0 rather than NaN', () => {
    // NaN would poison the total and disable Save with no visible reason.
    expect(parsePct('')).toBe(0);
    expect(parsePct('-')).toBe(0);
    expect(parsePct('abc')).toBe(0);
    expect(parsePct(' 12.5 ')).toBe(12.5);
  });
});

describe('stockSleevePct', () => {
  it('is whatever cash and the ETFs left behind', () => {
    expect(stockSleevePct(10, [etf(1, '20')])).toBe(70);
    expect(stockSleevePct(0, [])).toBe(100);
  });

  it('goes negative on an over-allocated book', () => {
    // Reported, not clamped — the caller has to refuse it.
    expect(stockSleevePct(50, [etf(1, '60')])).toBe(-10);
  });
});

describe('validateSleeves', () => {
  it('accepts a book that adds up', () => {
    expect(validateSleeves(10, [etf(1, '20'), etf(2, '5')])).toBeNull();
    expect(validateSleeves(0, [])).toBeNull();
    expect(validateSleeves(100, [])).toBeNull();
  });

  it('refuses more than 100% and says what the total is', () => {
    expect(validateSleeves(50, [etf(1, '60')])).toContain('110.00%');
  });

  it('refuses a duplicate ETF instead of silently summing it', () => {
    expect(validateSleeves(0, [etf(7, '10'), etf(7, '10')])).toContain('twice');
  });

  it('refuses a row with no ETF picked', () => {
    expect(validateSleeves(0, [etf(null, '10')])).toContain('Pick an ETF');
  });

  it('refuses negative weights', () => {
    expect(validateSleeves(-5, [])).toContain('Cash');
    expect(validateSleeves(0, [etf(1, '-5')])).toContain('negative');
  });

  it('refuses an ETF alongside 100% cash', () => {
    expect(validateSleeves(100, [etf(1, '0')])).toContain('nothing to hold');
  });

  it('lets a human-typed 100% through despite float noise', () => {
    // 10.1 + 20.2 + 69.7 does not land on exactly 100 in binary floating point.
    expect(validateSleeves(10.1, [etf(1, '20.2'), etf(2, '69.7')])).toBeNull();
  });
});

describe('fmtSleevePct', () => {
  it('keeps whole numbers whole and pins the rest at 2dp', () => {
    expect(fmtSleevePct(20)).toBe('20');
    expect(fmtSleevePct(22.2222)).toBe('22.22');
    expect(fmtSleevePct(69.99999999)).toBe('70');
  });
});
