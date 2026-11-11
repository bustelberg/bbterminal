import { expect, it } from 'vitest';
import { weeklyAxisTicks } from './weeklyAxisTicks';

function weeks(start: string, count: number) {
  return Array.from({ length: count }, (_, index) => {
    const date = new Date(`${start}T12:00:00Z`);
    date.setUTCDate(date.getUTCDate() + 7 * index);
    return date.toISOString().slice(0, 10);
  });
}

it('uses years for a multi-year history in a narrow chart', () => {
  const dates = weeks('2023-01-01', 200);
  const ticks = weeklyAxisTicks(dates, dates[0], dates.at(-1)!, 420, false);
  expect(ticks.length).toBeGreaterThan(2);
  expect(ticks.every(tick => /^\d{4}$/.test(tick.label))).toBe(true);
  expect(ticks.map(tick => tick.label)).toContain('2025');
});

it('keeps month labels when they fit', () => {
  const dates = weeks('2026-01-01', 25);
  const ticks = weeklyAxisTicks(dates, dates[0], dates.at(-1)!, 800, false);
  expect(ticks.some(tick => tick.label === 'Mar')).toBe(true);
  expect(ticks[0].label).toContain('2026');
});

it('does not overlap the estimated label bounds, including narrow layouts', () => {
  const dates = weeks('2023-01-01', 200);
  for (const width of [200, 420, 960]) {
    const ticks = weeklyAxisTicks(dates, dates[0], dates.at(-1)!, width, false);
    let previousEnd = -Infinity;
    for (const tick of ticks) {
      const size = tick.label.length * 6 * 960 / width;
      const start = tick.x - (tick.anchor === 'middle' ? size / 2 : tick.anchor === 'end' ? size : 0);
      expect(start).toBeGreaterThan(previousEnd);
      previousEnd = start + size;
    }
  }
});
