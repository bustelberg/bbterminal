import { describe, expect, it } from 'vitest';

import { airsScheduleFreshness } from './snapshotAge';

describe('AIRS freshness follows its Amsterdam schedule', () => {
  it('keeps yesterday current before today’s run is due', () => {
    const result = airsScheduleFreshness(
      '2026-09-28T09:15:00Z', // 11:15 Amsterdam
      new Date('2026-09-29T08:59:00Z'), // 10:59 Amsterdam
    );
    expect(result.stale).toBe(false);
    expect(result.label).toContain('pending');
  });

  it('allows the sequential 11:00 job one hour to finish', () => {
    expect(airsScheduleFreshness(
      '2026-09-28T09:15:00Z',
      new Date('2026-09-29T09:45:00Z'), // 11:45 Amsterdam
    ).stale).toBe(false);
  });

  it('turns stale after the completion window when today did not update', () => {
    const result = airsScheduleFreshness(
      '2026-09-28T09:15:00Z',
      new Date('2026-09-29T10:01:00Z'), // 12:01 Amsterdam
    );
    expect(result.stale).toBe(true);
    expect(result.label).toContain('2026-09-29 11:00 Amsterdam');
  });

  it('accepts today’s completed run and handles Amsterdam winter time', () => {
    expect(airsScheduleFreshness(
      '2026-12-15T10:25:00Z', // 11:25 Amsterdam (CET)
      new Date('2026-12-15T12:00:00Z'),
    )).toEqual({ stale: false, label: 'current AIRS run' });
  });
});
