import { describe, it, expect } from 'vitest';
import { noteFor, reportingLine, whyNoLine, type BlendNote } from './blendNotes';

const note = (over: Partial<BlendNote> = {}): BlendNote => ({
  kind: 'level', reporting: 9, reporting_pct: 88, contributing: 2,
  dropped: {}, best_covered_pct: 20, floor_pct: 60,
  years: 10, years_below_floor: 10, years_no_value: 0, ...over,
});

describe('noteFor', () => {
  const codes = ['annuals__Per Share Data__Dividends per Share',
    'annuals__per_share_data_array__Dividends per Share'];

  it('finds the note under whichever section spelling the blend saw', () => {
    expect(noteFor({ [codes[1]]: note() }, codes)?.reporting).toBe(9);
  });
  it('is undefined when the metric simply is not there — that IS "not ingested"', () => {
    expect(noteFor({}, codes)).toBeUndefined();
    expect(noteFor(undefined, codes)).toBeUndefined();
  });
});

describe('whyNoLine', () => {
  it('names the rebase when that is what dropped the holdings', () => {
    expect(whyNoLine(note({ dropped: { non_positive_base: 7 } }))).toMatch(/^7 of them start at 0/);
  });

  it('prefers the rebase over the floor — the floor is its CONSEQUENCE, not a second cause', () => {
    // Dropping 7 of 9 holdings is exactly what takes every year under the floor. Reporting the
    // floor here sends the reader after coverage they cannot fix.
    const w = whyNoLine(note({ dropped: { non_positive_base: 7 }, best_covered_pct: 20 }));
    expect(w).not.toMatch(/floor/);
  });

  it('names the floor when every holding survived and there is just too little weight', () => {
    expect(whyNoLine(note({ contributing: 9, best_covered_pct: 31 })))
      .toMatch(/only 31% of weight, under the 60% floor/);
  });

  it('names the unusable value when the weight is there but the maths is not', () => {
    expect(whyNoLine(note({ kind: 'multiple', best_covered_pct: 100, years_no_value: 3 })))
      .toMatch(/no year has a usable value/);
  });
});

describe('reportingLine', () => {
  it('leads with the fact that contradicts "not ingested"', () => {
    expect(reportingLine(note(), 'dividend/share')).toBe('9 holdings (88% of weight) report dividend/share');
  });
  it('reads correctly for one holding', () => {
    expect(reportingLine(note({ reporting: 1, reporting_pct: 4 }), 'revenue'))
      .toBe('1 holding (4% of weight) reports revenue');
  });
});
