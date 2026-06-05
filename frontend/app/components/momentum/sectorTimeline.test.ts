import { describe, it, expect } from 'vitest';
import type { Holding, PeriodRecord } from '../../../lib/stores/momentum';
import {
  detectCadenceDays,
  formatRunDuration,
  bucketRecordsByMonth,
  buildTimelineData,
  TIMELINE_BUCKET_THRESHOLD,
} from './sectorTimeline';

// Minimal fixtures — only the fields the data layer reads.
const h = (company_id: number, sector: string, fr: number | null, side: 'long' | 'short' = 'long') =>
  ({ company_id, sector, forward_return_pct: fr, side } as unknown as Holding);
const rec = (date: string, holdings: Holding[]) => ({ date, holdings } as unknown as PeriodRecord);

describe('detectCadenceDays', () => {
  it('returns 30 for fewer than two months', () => {
    expect(detectCadenceDays([])).toBe(30);
    expect(detectCadenceDays(['2024-01'])).toBe(30);
  });
  it('detects ~monthly cadence from YYYY-MM strings', () => {
    const d = detectCadenceDays(['2024-01', '2024-02', '2024-03']);
    expect(d).toBeGreaterThanOrEqual(28);
    expect(d).toBeLessThanOrEqual(31);
  });
  it('detects ~daily cadence from consecutive business days', () => {
    expect(detectCadenceDays(['2024-01-02', '2024-01-03', '2024-01-04'])).toBe(1);
  });
});

describe('formatRunDuration', () => {
  it('formats days / weeks / months / years on the calendar thresholds', () => {
    expect(formatRunDuration(1, 1)).toBe('1 day');
    expect(formatRunDuration(3, 1)).toBe('3 days');
    expect(formatRunDuration(1, 7)).toBe('1 wk');   // 7 days
    expect(formatRunDuration(1, 30)).toBe('1 mo');  // 30 days
    expect(formatRunDuration(12, 30)).toBe('12 mos'); // 360 days
    expect(formatRunDuration(13, 30)).toBe('1.1 yrs'); // 390 days
  });
});

describe('bucketRecordsByMonth', () => {
  it('passes through unchanged at/under the threshold (same reference)', () => {
    const recs = [rec('2024-01-02', [h(1, 'Tech', 1)])];
    expect(bucketRecordsByMonth(recs)).toBe(recs);
  });

  it('collapses sub-monthly records into one per month, compounding the sector return', () => {
    // 251 daily records (> threshold), all in 2024-01, same holding +1%/day.
    const n = TIMELINE_BUCKET_THRESHOLD + 1;
    const recs: PeriodRecord[] = [];
    for (let i = 0; i < n; i++) recs.push(rec('2024-01-01', [h(1, 'Tech', 1)]));

    const out = bucketRecordsByMonth(recs);
    expect(out).toHaveLength(1);
    expect(out[0].date).toBe('2024-01');
    // Deduped by company_id → a single holding...
    expect(out[0].holdings).toHaveLength(1);
    // ...carrying the chain-linked monthly return, not the 1% daily mean.
    const expected = (Math.pow(1.01, n) - 1) * 100;
    expect(out[0].holdings[0].forward_return_pct).toBeCloseTo(expected, 4);
  });
});

describe('buildTimelineData', () => {
  const records = [
    rec('2024-01', [h(1, 'Tech', 10), h(2, 'Health', 5)]),
    rec('2024-02', [h(1, 'Tech', 20), h(2, 'Health', -5)]),
    rec('2024-03', [h(1, 'Tech', 0)]), // Health drops out
  ];

  it('builds alphabetically-sorted sectors with chain-linked run returns + weights', () => {
    const d = buildTimelineData(records, () => true);
    expect(d.sectors).toEqual(['Health', 'Tech']);
    expect(d.months).toEqual(['2024-01', '2024-02', '2024-03']);

    // Tech held all three months → one run, return chain-linked 1.10·1.20·1.00.
    const tech = d.runs.get('Tech')!;
    expect(tech).toHaveLength(1);
    expect(tech[0].cumulativeReturnPct).toBeCloseTo(32, 6);
    expect(tech[0].monthsHeld).toBe(3);
    expect(tech[0].startMonth).toBe('2024-01');
    expect(tech[0].endMonth).toBe('2024-03');

    // Health held two months then dropped → one closed run, 1.05·0.95 − 1.
    const health = d.runs.get('Health')!;
    expect(health).toHaveLength(1);
    expect(health[0].cumulativeReturnPct).toBeCloseTo(-0.25, 6);
    expect(health[0].monthsHeld).toBe(2);

    // Weights: equal-split when both held (50/50), 100% when only Tech remains.
    expect(Array.from(d.weightByMonth.get('Tech')!)).toEqual([50, 50, 100]);
    // Every Tech month points at run index 0.
    expect(Array.from(d.runByMonth.get('Tech')!)).toEqual([0, 0, 0]);
  });

  it('respects the holding filter (long-only excludes shorts)', () => {
    const mixed = [
      rec('2024-01', [h(1, 'Tech', 10, 'long'), h(3, 'Energy', 4, 'short')]),
    ];
    const d = buildTimelineData(mixed, (x) => x.side !== 'short');
    expect(d.sectors).toEqual(['Tech']);
    // Sole long holding → 100% weight of its own side's pie.
    expect(Array.from(d.weightByMonth.get('Tech')!)).toEqual([100]);
  });
});
