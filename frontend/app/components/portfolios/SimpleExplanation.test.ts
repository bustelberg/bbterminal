import { describe, expect, it } from 'vitest';
import { infoFacts } from './SimpleExplanation';

describe('compact explanation facts', () => {
  it('uses the observation date without shifting a timestamp to another time zone', () => {
    expect(infoFacts('en').date('2026-10-29T23:30:00-05:00')).toBe('29 Oct 2026');
    expect(infoFacts('nl').date('2026-10-29')).toBe('29 okt 2026');
  });
  it('never replaces missing source dates or counts with today or a made-up sample', () => {
    expect(infoFacts('en').sample(null, '2026-10-29', null, 'weekly'))
      .toBe('date unavailable – 29 Oct 2026 · — weekly returns');
    expect(infoFacts('nl').dated('AIRS', null)).toBe('AIRS · datum onbekend');
    expect(infoFacts('en').date('2026-02-31')).toBe('date unavailable');
  });
  it('reports actual pair coverage rather than implying every pair was measured', () => {
    expect(infoFacts('en').pairs(300, 378, 30)).toBe('300 of 378 pairs · at least 30 shared returns per pair');
  });
  it('shows the observed drawdown and dates rather than a generic example', () => {
    expect(infoFacts('en').worst('2021-12-29', '2022-10-14', -18.01))
      .toBe('Worst fall: 29 Dec 2021 – 14 Oct 2022 · -18.01%');
  });
});
