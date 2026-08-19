import { describe, expect, it } from 'vitest';
import { benchmarkProvenance } from './benchmarkSourceNote';

/**
 * The Scorecard's benchmark and the Attribution panel's benchmark are no longer the same number.
 * This card is the only thing on screen that says which one the tile is showing, so what is
 * asserted here is that it CANNOT come out silent, generic, or attributed to the wrong vendor.
 */
describe('benchmarkProvenance', () => {
  const etf = {
    source: 'etf', ticker: 'ACWI', from: '2025-12-31', asOf: '2026-08-18', label: 'ACWI',
    openPrice: 141.49, closePrice: 160.08, openFx: 1.175, closeFx: 1.1593, eurPct: 14.6709,
  };

  it('states the rule, then the same rule with this window’s own numbers', () => {
    const c = benchmarkProvenance(etf);
    expect(c.sourceKey).toBe('benchmark_etf');
    const [rule, blank, worked] = c.how.split('\n');
    expect(rule).toContain('FX is USD per EUR');
    // ⚠ THE BLANK LINE. It is what makes the two read as one thing said twice rather than one long
    // sentence, and it survives only because the card renders `how` with `whitespace-pre-wrap`.
    expect(blank).toBe('');
    // ⚠ The OPENING MARK, not the 1-Jan anchor — they differ by a trading day, and a reader
    // checking this by hand against 1 January would pull the wrong bar.
    expect(worked).toBe('2025-12-31 → 2026-08-18');
    expect(c.how).toContain('(160.08 ÷ 1.1593) ÷ (141.49 ÷ 1.1750) − 1 = +14.67%');
  });

  it('reproduces the tile: the worked line actually evaluates to the figure it claims', () => {
    // ⚠⚠ THE ONE ASSERTION THAT MATTERS. A worked example the reader cannot reproduce is worse
    // than no worked example — it looks like proof. Four decimals on FX because the rate moves in
    // the fourth and rounding it to two changes the answer.
    const c = benchmarkProvenance(etf);
    const m = c.how.match(/\(([\d.]+) ÷ ([\d.]+)\) ÷ \(([\d.]+) ÷ ([\d.]+)\) − 1 = ([+-][\d.]+)%/);
    expect(m).not.toBeNull();
    const [, c1, f1, o0, f0, claimed] = m as RegExpMatchArray;
    const computed = ((Number(c1) / Number(f1)) / (Number(o0) / Number(f0)) - 1) * 100;
    expect(computed).toBeCloseTo(Number(claimed), 2);
    expect(computed).toBeCloseTo(etf.eurPct, 2);
  });

  it('falls back to the rule alone rather than printing NaN when a mark is missing', () => {
    // An older payload, from before the marks were carried, still renders a usable card.
    const c = benchmarkProvenance({ ...etf, closeFx: null });
    expect(c.how).toContain('FX is USD per EUR');
    expect(c.how).not.toContain('\n');
    expect(c.how).not.toMatch(/NaN|undefined|null/);
  });

  it('refuses a zero rate rather than dividing by it', () => {
    const c = benchmarkProvenance({ ...etf, openFx: 0 });
    expect(c.how).not.toContain('Infinity');
    expect(c.how).not.toContain('\n');
  });

  it('uses the rebuild source key — never the ETF one — when it falls back', () => {
    const c = benchmarkProvenance({ source: 'rebuild', label: 'AEX' });
    // ⚠⚠ THE KEY IS THE VENDOR NAME ON SCREEN. `benchmark_etf` renders "GuruFocus daily close";
    // printing that over a yfinance reconstruction is the one mislabel the badge exists to stop.
    expect(c.sourceKey).toBe('benchmark');
    expect(c.how).toMatch(/constituents/);
    expect(c.how).toMatch(/FULL market cap/);
  });

  it('treats an absent source as the rebuild rather than claiming the ETF', () => {
    // An older payload with no `benchmark_source` must not be described as an ETF price series
    // it may not be. The safe default is the one that claims less.
    expect(benchmarkProvenance({ label: 'ACWI' }).sourceKey).toBe('benchmark');
    expect(benchmarkProvenance({ source: 'something-new', label: 'ACWI' }).sourceKey)
      .toBe('benchmark');
  });

  it('always fills every field of the card', () => {
    for (const source of ['etf', 'rebuild', undefined, null]) {
      const c = benchmarkProvenance({ source, label: 'ACWI', ticker: 'ACWI' });
      expect(c.what.length).toBeGreaterThan(20);
      expect(c.note.length).toBeGreaterThan(5);
      expect(c.how.length).toBeGreaterThan(60);
    }
  });

  it('still reads sensibly when the ETF window dates are missing', () => {
    const c = benchmarkProvenance({ ...etf, from: null, asOf: null });
    expect(c.how).toContain('(160.08 ÷ 1.1593)');
    expect(c.how).not.toMatch(/null|undefined/);
  });

});
