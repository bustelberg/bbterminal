import { describe, expect, it } from 'vitest';
import { lastPerMonth } from './monthEnds';

/**
 * ONE POINT PER MONTH — THE MOST RECENT ONE IN IT.
 *
 * ⚠⚠ THE PROPERTY THAT MAKES THIS SAFE IS THAT NOTHING IS INVENTED. Every point it returns is one
 * that went in, on its own date — no averaging, no interpolation, no moving an observation to a
 * month boundary. That is why the thinned series can be drawn in the same ink as the full one, and
 * it is the thing to check first if this ever grows a second rule.
 *
 * ⚠⚠ AND THE SECOND PROPERTY IS THAT IT ENDS ON THE NEWEST ROW. The `Return` chip beside the chart
 * reads that same newest row, so the last point of the thinned series IS the chip's figure — by
 * this rule, not by luck. A current-month special case was added and removed on 2026-09-03 for
 * exactly that reason; see `monthEnds.ts`.
 */

const p = (date: string, value: number) => ({ date, value });

describe('lastPerMonth', () => {
  it('keeps the last observation of each month', () => {
    const rows = [p('2026-06-23', 1), p('2026-06-30', 2), p('2026-07-31', 3)];
    expect(lastPerMonth(rows)).toEqual([p('2026-06-30', 2), p('2026-07-31', 3)]);
  });

  it('⚠ the partial month contributes its NEWEST point, and never a second one', () => {
    // The current month gets no special case, which is what keeps the series ending on the newest
    // row — the figure the `Return` chip states. A rule that also asked "is it today?" would end
    // the line at last month's close on any morning AIRS has not published yet, while the chip and
    // the chart's own header both stated yesterday's number. Tried 2026-09-03, removed same day.
    const rows = [p('2026-07-31', 1), p('2026-08-03', 2), p('2026-08-26', 3)];
    expect(lastPerMonth(rows).map((r) => r.date)).toEqual(['2026-07-31', '2026-08-26']);
  });

  it('returns them in date order whatever order they arrived in', () => {
    // ⚠ A chart drawn from an unsorted series is a scribble, and `Map` preserves INSERTION order —
    // so an unsorted input would come back unsorted without the explicit sort.
    const rows = [p('2026-08-26', 3), p('2026-06-30', 1), p('2026-07-31', 2)];
    expect(lastPerMonth(rows).map((r) => r.value)).toEqual([1, 2, 3]);
  });

  it('never invents a point: every result is one of the inputs, unchanged', () => {
    const rows = [p('2026-06-23', 1), p('2026-06-30', 2), p('2026-08-26', 3)];
    for (const kept of lastPerMonth(rows)) expect(rows).toContain(kept);
  });

  it('a month with one observation keeps it', () => {
    expect(lastPerMonth([p('2026-02-14', 9)])).toEqual([p('2026-02-14', 9)]);
  });

  it('spans a year boundary without folding two Januarys together', () => {
    // ⚠ THE KEY IS `YYYY-MM`, not the month number — which is the whole reason to slice the string
    // rather than parse it. A month-of-year key would merge 2026-01 into 2025-01 silently, and the
    // series would lose a year the first time one is longer than twelve months.
    const rows = [p('2025-12-31', 1), p('2026-01-30', 2), p('2026-01-31', 3)];
    expect(lastPerMonth(rows).map((r) => r.date)).toEqual(['2025-12-31', '2026-01-31']);
  });

  it('is empty for an empty series', () => {
    expect(lastPerMonth([])).toEqual([]);
  });
});
