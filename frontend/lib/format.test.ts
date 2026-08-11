import { describe, it, expect } from 'vitest';
import { fmtTimestamp } from './format';

describe('fmtTimestamp', () => {
  it('renders an em-dash for null/undefined/empty', () => {
    expect(fmtTimestamp(null)).toBe('—');
    expect(fmtTimestamp(undefined)).toBe('—');
    expect(fmtTimestamp('')).toBe('—');
  });

  it('falls back to the raw string for an unparseable value', () => {
    expect(fmtTimestamp('not-a-timestamp')).toBe('not-a-timestamp');
  });

  it('formats a valid ISO timestamp to a non-placeholder string', () => {
    const out = fmtTimestamp('2026-04-03T14:30:00Z');
    expect(out).not.toBe('—');
    expect(out).toMatch(/2026/);
  });
});
