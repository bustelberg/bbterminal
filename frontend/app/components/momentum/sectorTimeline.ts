/**
 * Pure data layer for the Sector Timeline chart — no React, no DOM.
 *
 * Turns a backtest's `monthly_records` into the per-sector "run" lists +
 * per-month weights the chart paints. Extracted from `SectorTimelineChart.tsx`
 * so the (subtle) bucketing + run-building logic is testable in isolation and
 * the component is left as a thin rendering/interaction shell.
 */
import type { Holding, PeriodRecord } from '../../../lib/stores/momentum';

export type Run = {
  sector: string;
  startIdx: number;
  endIdx: number;            // inclusive month index in monthly_records
  startMonth: string;        // YYYY-MM
  endMonth: string;          // YYYY-MM
  monthsHeld: number;
  cumulativeReturnPct: number | null;
};

export type TimelineData = {
  sectors: string[];
  runs: Map<string, Run[]>;
  runByMonth: Map<string, Int16Array>;
  weightByMonth: Map<string, Float32Array>;
  months: string[];
  /** Median calendar-day interval between consecutive records. Drives the
   * tooltip's run-duration label (so a single-cell hold reads "1 wk" on a
   * weekly cadence, "1 day" on daily, "3 mo" on quarterly, etc.) and the
   * CAGR annualization. ~30 for monthly + bucketed sub-monthly cadences. */
  cadenceDays: number;
};

/** Median interval (in calendar days) between consecutive records. Used
 * to interpret a run's "cells held" as a real time duration. */
export function detectCadenceDays(months: readonly string[]): number {
  if (months.length < 2) return 30;
  const toMs = (s: string) => new Date(s.length === 7 ? `${s}-01` : s).getTime();
  const intervals: number[] = [];
  for (let i = 1; i < months.length; i++) {
    const dt = (toMs(months[i]) - toMs(months[i - 1])) / 86400000;
    if (dt > 0) intervals.push(dt);
  }
  if (intervals.length === 0) return 30;
  intervals.sort((a, b) => a - b);
  return intervals[Math.floor(intervals.length / 2)];
}

/** Format `cells × cadenceDays` total as a human duration string for the
 * tooltip ("3 days" / "5 wks" / "8 mo" / "1.5 yrs"). Uses calendar
 * conventions (~7 days / ~30 days / ~365.25 days) to keep numbers round. */
export function formatRunDuration(cells: number, cadenceDays: number): string {
  const days = Math.max(1, Math.round(cells * cadenceDays));
  if (days < 7) return `${days} day${days === 1 ? '' : 's'}`;
  if (days < 30) {
    const wks = Math.round(days / 7);
    return `${wks} wk${wks === 1 ? '' : 's'}`;
  }
  if (days < 365) {
    const mos = Math.round(days / 30);
    return `${mos} mo${mos === 1 ? '' : 's'}`;
  }
  const yrs = days / 365.25;
  // 1.0 / 2.0 etc render as "1.0 yrs" — fine for visual consistency.
  return `${yrs.toFixed(1)} yrs`;
}

// Above this many records we bucket by calendar month before building
// the cells. A 24-year daily backtest produces ~6000 records — at that
// scale the chart was rendering ~60k tiny <div>s (each with a hover
// handler), which made page scroll janky. Bucketing collapses to ~290
// monthly cells per sector, which is also the sensible visual resolution
// (a sub-pixel-wide cell carries no information). The bucket key is the
// raw "YYYY-MM" prefix; the representative record per bucket is the LAST
// one (most recent within the month) so tooltips still surface a real
// date the user can recognize.
export const TIMELINE_BUCKET_THRESHOLD = 250;

export function bucketRecordsByMonth(records: readonly PeriodRecord[]): readonly PeriodRecord[] {
  if (records.length <= TIMELINE_BUCKET_THRESHOLD) return records;

  // Pass 1: per (month, sector), compound the daily per-sector mean
  // returns into a single monthly factor. We need this so the bucketed
  // holdings carry an aggregated forward_return that, when fed back into
  // the same run-building logic below, reproduces the correct cumulative
  // run return on the tooltip. Without it the bucketed run.rets would
  // hold a single sub-percent daily mean per month and the tooltip's
  // "Run return" would understate by orders of magnitude.
  const compoundedByMonthSector = new Map<string, Map<string, number>>();
  for (const rec of records) {
    const monthKey = rec.date.slice(0, 7);
    let monthFactors = compoundedByMonthSector.get(monthKey);
    if (!monthFactors) {
      monthFactors = new Map<string, number>();
      compoundedByMonthSector.set(monthKey, monthFactors);
    }
    const sectorRets = new Map<string, number[]>();
    for (const h of rec.holdings) {
      if (h.forward_return_pct == null) continue;
      const sec = h.sector || 'Unknown';
      const arr = sectorRets.get(sec) ?? [];
      arr.push(h.forward_return_pct);
      sectorRets.set(sec, arr);
    }
    for (const [sec, rets] of sectorRets) {
      const mean = rets.reduce((a, b) => a + b, 0) / rets.length;
      const factor = 1 + mean / 100;
      const prior = monthFactors.get(sec) ?? 1.0;
      monthFactors.set(sec, prior * factor);
    }
  }

  // Pass 2: union holdings within each calendar month (a sector held on
  // any day shows as held for the bucketed cell). Dedupe by company_id.
  const byMonth = new Map<string, PeriodRecord>();
  for (const rec of records) {
    const monthKey = rec.date.slice(0, 7);
    const prior = byMonth.get(monthKey);
    if (!prior) {
      byMonth.set(monthKey, { ...rec, date: monthKey, holdings: [...rec.holdings] });
      continue;
    }
    const seen = new Set(prior.holdings.map((h) => h.company_id));
    for (const h of rec.holdings) {
      if (!seen.has(h.company_id)) {
        prior.holdings.push(h);
        seen.add(h.company_id);
      }
    }
  }

  // Pass 3: rewrite each bucketed holding's forward_return_pct with the
  // sector's compounded monthly factor → percentage. Per-sector mean over
  // these holdings then equals the compounded value, and chain-linking
  // across months in a run reproduces the true period cumulative return.
  for (const [monthKey, rec] of byMonth) {
    const monthFactors = compoundedByMonthSector.get(monthKey);
    if (!monthFactors) continue;
    rec.holdings = rec.holdings.map((h) => {
      const sec = h.sector || 'Unknown';
      const factor = monthFactors.get(sec);
      if (factor == null) return h;
      return { ...h, forward_return_pct: (factor - 1) * 100 };
    });
  }

  return Array.from(byMonth.values()).sort((a, b) => a.date.localeCompare(b.date));
}

/** Build per-sector run lists + per-month weights for the given monthly
 * records. `holdingFilter` lets the caller restrict which holdings count
 * (e.g. "only long-side" for the long panel of a long-short backtest);
 * weights are computed against the count of *filtered* holdings per month
 * so a 50/50 long/short split shows each side at 100% of its own pie. */
export function buildTimelineData(
  records: readonly PeriodRecord[],
  holdingFilter: (h: Holding) => boolean,
): TimelineData {
  records = bucketRecordsByMonth(records);
  const months = records.map((r) => r.date);
  const runs = new Map<string, Run[]>();
  const runByMonth = new Map<string, Int16Array>();
  const weightByMonth = new Map<string, Float32Array>();
  const totalHeld = new Map<string, number>();

  // First pass: collect every sector ever held + per-month weights.
  for (let i = 0; i < records.length; i++) {
    const rec = records[i];
    const filtered = rec.holdings.filter(holdingFilter);
    if (filtered.length === 0) continue;
    for (const h of filtered) {
      const sec = h.sector || 'Unknown';
      totalHeld.set(sec, (totalHeld.get(sec) ?? 0) + 1);
      if (!weightByMonth.has(sec)) {
        weightByMonth.set(sec, new Float32Array(months.length));
      }
    }
  }

  const sectors = Array.from(totalHeld.keys()).sort((a, b) => a.localeCompare(b));

  for (const sec of sectors) {
    const arr = new Int16Array(months.length);
    arr.fill(-1);
    runByMonth.set(sec, arr);
    runs.set(sec, []);
  }

  type OpenRun = { startIdx: number; rets: number[] };
  const open = new Map<string, OpenRun>();

  for (let i = 0; i < records.length; i++) {
    const rec = records[i];
    const filtered = rec.holdings.filter(holdingFilter);
    const total = filtered.length;
    const sectorReturns = new Map<string, number[]>();
    const sectorCount = new Map<string, number>();
    for (const h of filtered) {
      const sec = h.sector || 'Unknown';
      sectorCount.set(sec, (sectorCount.get(sec) ?? 0) + 1);
      if (h.forward_return_pct != null) {
        const arr = sectorReturns.get(sec) ?? [];
        arr.push(h.forward_return_pct);
        sectorReturns.set(sec, arr);
      }
    }
    for (const sec of sectors) {
      const w = weightByMonth.get(sec)!;
      const cnt = sectorCount.get(sec) ?? 0;
      if (cnt === 0) continue;
      w[i] = total > 0 ? (cnt / total) * 100 : 0;
      let run = open.get(sec);
      if (!run) {
        run = { startIdx: i, rets: [] };
        open.set(sec, run);
      }
      const rets = sectorReturns.get(sec) ?? [];
      if (rets.length > 0) {
        const mean = rets.reduce((a, b) => a + b, 0) / rets.length;
        run.rets.push(mean);
      }
    }
    for (const sec of [...open.keys()]) {
      if ((sectorCount.get(sec) ?? 0) === 0) {
        const r = open.get(sec)!;
        closeRun(sec, r, i - 1);
        open.delete(sec);
      }
    }
  }
  for (const [sec, r] of open) {
    closeRun(sec, r, records.length - 1);
  }

  function closeRun(sec: string, openR: OpenRun, endIdx: number) {
    let factor = 1.0;
    for (const r of openR.rets) factor *= 1 + r / 100;
    const cumulative = openR.rets.length > 0 ? (factor - 1) * 100 : null;
    // The run is held FROM the entry rebalance THROUGH the exit boundary.
    // For a single-period hold (endIdx == startIdx) the previous code set
    // startMonth == endMonth, which produced "2025-05-05 → 2025-05-05" on
    // daily cadences. Use months[endIdx + 1] (the next rebalance, where
    // the position was exited) when it exists; fall back to the last
    // record's date for runs that ran to the end of the backtest.
    const exitIdx = endIdx + 1 < months.length ? endIdx + 1 : endIdx;
    const newRun: Run = {
      sector: sec,
      startIdx: openR.startIdx,
      endIdx,
      startMonth: months[openR.startIdx],
      endMonth: months[exitIdx],
      monthsHeld: endIdx - openR.startIdx + 1,
      cumulativeReturnPct: cumulative,
    };
    const list = runs.get(sec)!;
    const newRunIdx = list.length;
    list.push(newRun);
    const map = runByMonth.get(sec)!;
    for (let j = openR.startIdx; j <= endIdx; j++) map[j] = newRunIdx;
  }

  return { sectors, runs, runByMonth, weightByMonth, months, cadenceDays: detectCadenceDays(months) };
}
