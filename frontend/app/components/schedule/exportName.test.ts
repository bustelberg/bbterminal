import { describe, expect, it } from 'vitest';
import { holdingsExportName } from './exportName';

describe('holdingsExportName', () => {
  it('is the strategy name plus the month the portfolio is for', () => {
    expect(holdingsExportName('MomentumTopSelectie Neutraal', '2026-09-07'))
      .toBe('MomentumTopSelectie Neutraal September');
  });

  it('takes the month from as_of_date, NOT from today', () => {
    // ⚠ THE CASE THAT MOTIVATES THE WHOLE MODULE. A first-Monday-of-September rebalance is
    // decided on Friday 4 Sep and the tick fires from Saturday 5 Sep, so the file is
    // downloaded in a week whose `today` may still read August in some zones while the
    // portfolio it contains is unambiguously September's.
    expect(holdingsExportName('S', '2026-09-07')).toBe('S September');
    expect(holdingsExportName('S', '2026-08-03')).toBe('S August');
  });

  it('does not shift the month for viewers west of Greenwich', () => {
    // `new Date('2026-09-01')` is UTC midnight; rendered in a negative-offset zone that is
    // 31 August. Parsing the string avoids the whole class.
    expect(holdingsExportName('S', '2026-09-01')).toBe('S September');
    expect(holdingsExportName('S', '2026-01-01')).toBe('S January');
    expect(holdingsExportName('S', '2026-12-31')).toBe('S December');
  });

  it('accepts a full timestamp, not just a bare date', () => {
    expect(holdingsExportName('S', '2026-09-07T02:00:00+00:00')).toBe('S September');
  });

  it('falls back rather than failing when the date is missing or junk', () => {
    expect(holdingsExportName('S', null)).toBe('S');
    expect(holdingsExportName('S', '')).toBe('S');
    expect(holdingsExportName('S', 'not-a-date')).toBe('S');
  });

  it('falls back rather than failing when the name is missing', () => {
    expect(holdingsExportName(null, '2026-09-07')).toBe('current-portfolio September');
    expect(holdingsExportName('   ', '2026-09-07')).toBe('current-portfolio September');
    expect(holdingsExportName(null, null)).toBe('current-portfolio');
  });
});
