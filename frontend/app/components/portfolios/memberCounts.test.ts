import { describe, it, expect } from 'vitest';
import { countFor, memberCountHow, memberCountLine, type MemberCount } from './memberCounts';

const CODES = ['annuals__Per Share Data__EPS without NRI',
  'annuals__per_share_data__EPS without NRI'];

const c = (considered: number, total: number, rule?: string): MemberCount =>
  ({ considered, total, ...(rule ? { rule } : {}) });

describe('countFor', () => {
  it('finds the count under whichever section spelling the blend saw', () => {
    expect(countFor(CODES, { [CODES[1]]: c(9, 12) })?.considered).toBe(9);
  });
  it('is undefined when the metric is not in the payload at all', () => {
    expect(countFor(CODES, {})).toBeUndefined();
    expect(countFor(CODES, undefined)).toBeUndefined();
  });
});

describe('memberCountLine', () => {
  const base = { isAgg: true, ownLabel: 'Bustelberg Offensief', benchLabel: 'AEX' };

  it('says nothing when both lines used every holding they had', () => {
    // ⚠ THE DEFAULT, on twelve of thirteen cards. A line reading "42 of 42" is noise everywhere it
    // is true, which is what would make the one card that matters unreadable.
    expect(memberCountLine({ ...base, own: c(42, 42), bench: c(22, 22) })).toBeNull();
  });

  it('names both sides when both withheld, each with its own numbers', () => {
    const line = memberCountLine({ ...base, own: c(36, 42, 'positive_only'),
      bench: c(12, 22, 'positive_only') });
    expect(line?.text).toBe('Bustelberg Offensief: 36 of 42 companies · AEX: 12 of 22');
  });

  it('names only the side that withheld', () => {
    // ⚠ TWO BLENDS OVER TWO SETS OF COMPANIES. One count standing for both would be wrong on
    // whichever it was not.
    expect(memberCountLine({ ...base, own: c(42, 42), bench: c(12, 22, 'aggregate') })?.text)
      .toBe('AEX: 12 of 22');
    expect(memberCountLine({ ...base, own: c(36, 42, 'aggregate'), bench: c(22, 22) })?.text)
      .toBe('Bustelberg Offensief: 36 of 42 companies');
  });

  it('groups an index thousands separator, because 1514 is not a number to read raw', () => {
    expect(memberCountLine({ ...base, benchLabel: 'ACWI',
      own: c(42, 42), bench: c(1480, 1514, 'aggregate') })?.text)
      .toBe('ACWI: 1,480 of 1,514');
  });

  it('says nothing about a single company — one member of one is a tautology', () => {
    expect(memberCountLine({ ...base, isAgg: false, benchLabel: null,
      own: c(0, 1, 'positive_only') })).toBeNull();
  });

  it('still reports the index beside a single company', () => {
    // The book half is a tautology; the INDEX half is not, and it is the line being compared to.
    expect(memberCountLine({ ...base, isAgg: false, bench: c(12, 22, 'aggregate') })?.text)
      .toBe('AEX: 12 of 22');
  });

  it('carries the rule of the side it is showing', () => {
    expect(memberCountLine({ ...base, own: c(36, 42, 'positive_only'),
      bench: c(22, 22, 'positive_only') })?.rule).toBe('positive_only');
    expect(memberCountLine({ ...base, own: c(42, 42, 'aggregate'),
      bench: c(12, 22, 'aggregate') })?.rule).toBe('aggregate');
  });

  it('falls back to `all` when the payload carries no rule', () => {
    // ⚠ AN OLDER PAYLOAD. The count is still true; only the explanation is unknown, and
    // `memberCountHow` answers that with the generic sentence rather than inventing a cause.
    expect(memberCountLine({ ...base, own: c(36, 42) })?.rule).toBe('all');
  });
});

describe('memberCountHow', () => {
  it('explains a survivorship filter as a filter, and names its cost', () => {
    const how = memberCountHow('positive_only');
    expect(how).toMatch(/positive in every period/);
    expect(how).toMatch(/survivorship/);
  });

  it('explains the euro sum as a missing input, and does NOT call it survivorship', () => {
    // ⚠⚠ THE WHOLE POINT OF `rule`. The two constructions withhold members for reasons that have
    // nothing to do with each other, and the FCF sentence on an EPS card would tell the reader
    // their earnings line excludes loss-makers — a confident wrong explanation of a right number.
    const how = memberCountHow('aggregate');
    expect(how).toMatch(/market cap/);
    expect(how).not.toMatch(/survivorship/i);
    expect(how).not.toMatch(/positive in every period/);
  });

  it('⚠ names the MARKET CAP, not a share count — the only aggregated metric is a total', () => {
    /**
     * Measured on ACWI revenue 2026-08-31 (`scripts/diagnose_blend_members.py`): all 1,511
     * constituents carry euros and the line is still drawn from 1,509. The two missing — CSG NV
     * and Alpha Bank SA — have no market cap in any period they report, so they are in no
     * period's weighted average. The copy said "it needs a share count", which is the conversion
     * a PER-SHARE metric needs; revenue is a company total already, so that sentence could never
     * be the reason on the one card that can show this count.
     */
    expect(memberCountHow('aggregate')).not.toMatch(/share count/);
  });

  it('admits it does not know rather than guessing one of the two', () => {
    const how = memberCountHow('all');
    expect(how).not.toMatch(/survivorship/i);
    expect(how).not.toMatch(/share count/);
    expect(how).toMatch(/per-holding table/);
  });
});
