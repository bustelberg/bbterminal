import { describe, expect, it } from 'vitest';
import { TL, buildTimeline, shortDay } from './timingTimeline';

/**
 * The timeline places each decision on the year. Everything below is about the two ways a chart
 * like this lies: by drawing a point where no observation exists, and by dropping one silently.
 */

const ADOBE = {
  start: '2026-01-01', end: '2026-08-05', open: 244.20, now: 223.61,
  trades: [
    { datum: '2026-02-03', kind: 'sell', quantity: 78, price_eur: 246.14, effect_eur: 1758 },
    { datum: '2026-04-23', kind: 'buy', quantity: 66, price_eur: 204.36, effect_eur: 1271 },
  ],
};

describe('buildTimeline', () => {
  it('places the open, every trade and today, in date order', () => {
    const tl = buildTimeline(ADOBE.start, ADOBE.end, ADOBE.open, ADOBE.now, ADOBE.trades)!;
    expect(tl.points.map((p) => p.kind)).toEqual(['open', 'sell', 'buy', 'now']);
    expect(tl.points.map((p) => p.x)).toEqual([...tl.points.map((p) => p.x)].sort((a, b) => a - b));
  });

  it('sorts trades AIRS handed us out of order', () => {
    const tl = buildTimeline(ADOBE.start, ADOBE.end, ADOBE.open, ADOBE.now,
      [...ADOBE.trades].reverse())!;
    expect(tl.points.map((p) => p.date))
      .toEqual(['2026-01-01', '2026-02-03', '2026-04-23', '2026-08-05']);
  });

  it('anchors the ends to the window, not to the first and last trade', () => {
    // ⚠ THE CLAIM THE CHART MAKES. An axis inferred from the trades would put the February sale at
    // the very start of the year — exactly the thing a reader is meant to learn from the picture.
    const tl = buildTimeline(ADOBE.start, ADOBE.end, ADOBE.open, ADOBE.now, ADOBE.trades)!;
    expect(tl.points[0].x).toBeCloseTo(TL.padL, 5);
    expect(tl.points[3].x).toBeCloseTo(TL.width - TL.padR, 5);
    expect(tl.points[1].x).toBeGreaterThan(TL.padL + 10);
  });

  it('draws a higher price higher up', () => {
    const tl = buildTimeline(ADOBE.start, ADOBE.end, ADOBE.open, ADOBE.now, ADOBE.trades)!;
    const sell = tl.points[1];      // €246.14, the highest point
    const buy = tl.points[2];       // €204.36, the lowest
    expect(sell.y).toBeLessThan(buy.y);          // SVG y grows downward
    expect(sell.y).toBeGreaterThanOrEqual(TL.top);
    expect(buy.y).toBeLessThanOrEqual(TL.height - TL.bottom);
  });

  it('gives the endpoints no effect — doing nothing is not a decision', () => {
    const tl = buildTimeline(ADOBE.start, ADOBE.end, ADOBE.open, ADOBE.now, ADOBE.trades)!;
    expect(tl.points[0].effect).toBeNull();
    expect(tl.points[3].effect).toBeNull();
    expect(tl.points[1].effect).toBe(1758);
  });

  it('draws a segment per gap between observations and no more', () => {
    // The path is a polyline through what we KNOW, so it has exactly one fewer segment than points.
    const tl = buildTimeline(ADOBE.start, ADOBE.end, ADOBE.open, ADOBE.now, ADOBE.trades)!;
    expect(tl.path.startsWith('M')).toBe(true);
    expect((tl.path.match(/L/g) ?? []).length).toBe(tl.points.length - 1);
  });
});

describe('what it refuses to draw', () => {
  it('returns null without both window bounds', () => {
    expect(buildTimeline(null, ADOBE.end, 1, 2, [])).toBeNull();
    expect(buildTimeline(ADOBE.start, null, 1, 2, [])).toBeNull();
  });

  it('returns null without both prices', () => {
    expect(buildTimeline(ADOBE.start, ADOBE.end, null, 2, [])).toBeNull();
    expect(buildTimeline(ADOBE.start, ADOBE.end, 1, null, [])).toBeNull();
  });

  it('returns null on a window that does not run forwards', () => {
    expect(buildTimeline('2026-08-05', '2026-01-01', 1, 2, [])).toBeNull();
    expect(buildTimeline('2026-01-01', '2026-01-01', 1, 2, [])).toBeNull();
  });

  it('COUNTS an undated trade rather than dropping it', () => {
    // ⚠ Omitted silently, the markers stop matching the table beside them and nothing says why.
    const tl = buildTimeline(ADOBE.start, ADOBE.end, ADOBE.open, ADOBE.now,
      [{ datum: null, kind: 'buy', price_eur: 210, effect_eur: 5 }, ...ADOBE.trades])!;
    expect(tl.undated).toBe(1);
    expect(tl.points.filter((p) => p.kind === 'buy' || p.kind === 'sell')).toHaveLength(2);
  });

  it('counts a trade with a date but no price too — it cannot be placed either', () => {
    const tl = buildTimeline(ADOBE.start, ADOBE.end, ADOBE.open, ADOBE.now,
      [{ datum: '2026-03-01', kind: 'buy', price_eur: null, effect_eur: 5 }])!;
    expect(tl.undated).toBe(1);
  });
});

describe('edges that would otherwise render as nonsense', () => {
  it('a position that never moved gets a flat line, not a division by zero', () => {
    const tl = buildTimeline(ADOBE.start, ADOBE.end, 100, 100, [])!;
    expect(tl.points.every((p) => Number.isFinite(p.y))).toBe(true);
    expect(tl.points[0].y).toBeCloseTo(tl.points[1].y, 5);
  });

  it('clamps a trade dated outside the window instead of drawing it off-frame', () => {
    // Off-frame reads as absent; clamped at the edge it is visible and its date still prints.
    const tl = buildTimeline(ADOBE.start, ADOBE.end, 100, 120,
      [{ datum: '2025-11-02', kind: 'buy', price_eur: 90, effect_eur: 1 },
        { datum: '2026-12-30', kind: 'sell', price_eur: 130, effect_eur: 1 }])!;
    for (const p of tl.points) {
      expect(p.x).toBeGreaterThanOrEqual(TL.padL - 1e-6);
      expect(p.x).toBeLessThanOrEqual(TL.width - TL.padR + 1e-6);
    }
  });

  it('keeps every marker inside the plot area', () => {
    const tl = buildTimeline(ADOBE.start, ADOBE.end, 5, 900,
      [{ datum: '2026-03-01', kind: 'buy', price_eur: 1200, effect_eur: 1 }])!;
    for (const p of tl.points) {
      expect(p.y).toBeGreaterThanOrEqual(TL.top - 1e-6);
      expect(p.y).toBeLessThanOrEqual(TL.height - TL.bottom + 1e-6);
    }
  });
});

describe('label lanes', () => {
  it('keeps two far-apart decisions in the same lane', () => {
    const tl = buildTimeline(ADOBE.start, ADOBE.end, ADOBE.open, ADOBE.now, ADOBE.trades)!;
    expect(tl.points[1].lane).toBe(0);
    expect(tl.points[2].lane).toBe(0);
  });

  it('steps a decision down a lane when its label would overprint the previous one', () => {
    const tl = buildTimeline(ADOBE.start, ADOBE.end, 100, 110, [
      { datum: '2026-03-01', kind: 'buy', price_eur: 100, effect_eur: 1 },
      { datum: '2026-03-03', kind: 'sell', price_eur: 105, effect_eur: 1 },
    ])!;
    expect(tl.points[1].lane).toBe(0);
    expect(tl.points[2].lane).toBe(1);
  });

  it('never uses more lanes than it has room for', () => {
    const many = Array.from({ length: 12 }, (_, i) => ({
      datum: `2026-03-${String(i + 1).padStart(2, '0')}`,
      kind: i % 2 ? 'buy' : 'sell', price_eur: 100 + i, effect_eur: i,
    }));
    const tl = buildTimeline(ADOBE.start, ADOBE.end, 100, 120, many)!;
    expect(Math.max(...tl.points.map((p) => p.lane))).toBeLessThanOrEqual(1);
  });
});

describe('shortDay', () => {
  it('reads as a day, without a year the axis has no room for', () => {
    expect(shortDay('2026-02-03')).toBe('3 Feb');
    expect(shortDay('2026-12-30')).toBe('30 Dec');
  });

  it('does not shift the date across a timezone', () => {
    // ⚠ Parsed as UTC on purpose. `new Date('2026-02-03')` west of Greenwich renders "2 Feb",
    // which puts a decision on the wrong day for no visible reason.
    expect(shortDay('2026-01-01')).toBe('1 Jan');
  });

  it('hands back anything it cannot parse rather than printing "Invalid Date"', () => {
    expect(shortDay('not-a-date')).toBe('not-a-date');
  });
});
