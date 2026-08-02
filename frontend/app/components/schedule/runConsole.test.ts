import { describe, it, expect } from 'vitest';
import { formatLogTime, formatRunLogLine, type RunLogEntry } from './runConsole';

const entry = (over: Partial<RunLogEntry> = {}): RunLogEntry => ({
  seq: 1,
  at: '2026-08-02T09:14:22.517+00:00',
  level: 'info',
  phase: 'prices',
  message: 'NAS:AAPL — prices +2, volumes +2 (source api, 2 API call(s))',
  ...over,
});

describe('formatLogTime', () => {
  it('takes the wall clock out of an ISO timestamp', () => {
    expect(formatLogTime('2026-08-02T09:14:22.517+00:00')).toBe('09:14:22');
  });

  it('falls back to the raw string rather than inventing a time', () => {
    // A malformed timestamp is a fact about the entry; a fabricated "00:00:00"
    // would read as a real one.
    expect(formatLogTime('not-a-date')).toBe('not-a-date');
    expect(formatLogTime('')).toBe('');
  });
});

describe('formatRunLogLine', () => {
  it('carries time, run label, phase and message', () => {
    expect(formatRunLogLine(entry(), 'rebalance #412')).toBe(
      '09:14:22 [rebalance #412] prices · NAS:AAPL — prices +2, volumes +2 (source api, 2 API call(s))',
    );
  });

  it('omits the phase separator when the entry has no phase', () => {
    expect(formatRunLogLine(entry({ phase: null, message: 'hi' }), 'x')).toBe('09:14:22 [x] hi');
  });
});
