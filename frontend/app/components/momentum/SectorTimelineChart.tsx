'use client';

import { useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import { colorForSector } from '../../../lib/sectorColors';
import type { BacktestResult, Holding, PeriodRecord } from '../../../lib/stores/momentum';
import CollapsibleCard from './CollapsibleCard';
import { annualize, fmtPct } from './utils';

// Minimum on-screen width per cell. Cells stretch beyond this to fill the
// panel when there are few enough records that everything fits; otherwise
// each cell stays at this width and the panel scrolls horizontally. Picked
// so a single cell — whether it represents a day, week, month, or quarter
// — is wide enough to hover/click reliably and to read the year ticks
// above it. Drives:
//   - daily/weekly backtests post-bucketing (~290 monthly cells over
//     24 years): cellWidth = MIN, panel shows ~50 cells at a time.
//   - 3-month rebalance over 5 years (20 cells): cellWidth ~32px
//     stretched, panel shows everything, no scroll.
const MIN_CELL_WIDTH = 12;
const SECTOR_LABEL_WIDTH = 140;
const SECTOR_LABEL_GAP = 12; // matches `pr-3` on the label div

type Props = {
  result: BacktestResult;
};

// Sector color palette lives in `lib/sectorColors.ts` so /schedule's
// per-row sector chips share the exact same hue per sector.

type Run = {
  sector: string;
  startIdx: number;
  endIdx: number;            // inclusive month index in monthly_records
  startMonth: string;        // YYYY-MM
  endMonth: string;          // YYYY-MM
  monthsHeld: number;
  cumulativeReturnPct: number | null;
};

type TimelineData = {
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
function detectCadenceDays(months: readonly string[]): number {
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
function formatRunDuration(cells: number, cadenceDays: number): string {
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

/** Build per-sector run lists + per-month weights for the given monthly
 * records. `holdingFilter` lets the caller restrict which holdings count
 * (e.g. "only long-side" for the long panel of a long-short backtest);
 * weights are computed against the count of *filtered* holdings per month
 * so a 50/50 long/short split shows each side at 100% of its own pie. */
// Above this many records we bucket by calendar month before building
// the cells. A 24-year daily backtest produces ~6000 records — at that
// scale the chart was rendering ~60k tiny <div>s (each with a hover
// handler), which made page scroll janky. Bucketing collapses to ~290
// monthly cells per sector, which is also the sensible visual resolution
// (a sub-pixel-wide cell carries no information). The bucket key is the
// raw "YYYY-MM" prefix; the representative record per bucket is the LAST
// one (most recent within the month) so tooltips still surface a real
// date the user can recognize.
const TIMELINE_BUCKET_THRESHOLD = 250;

function bucketRecordsByMonth(records: readonly PeriodRecord[]): readonly PeriodRecord[] {
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

function buildTimelineData(
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

export default function SectorTimelineChart({ result }: Props) {
  // A long-short backtest has at least one holding with side === 'short'.
  // Long-only results omit `side` entirely (or always have 'long'), so the
  // single-panel branch covers the original behavior unchanged.
  const isLongShort = useMemo(
    () => result.monthly_records.some((r) => r.holdings.some((h) => h.side === 'short')),
    [result],
  );

  // Date-range slicing — empty strings = no filter (default: full range).
  // The chart re-builds against a sliced subset of monthly_records; the
  // input controls' min/max attributes pin them to the data extent so a
  // user can't accidentally reach into nothing. Monthly cadence records
  // are stored as "YYYY-MM"; we treat those as the first-of-month for
  // range comparison so a "From 2024-01-15" pick doesn't drop the whole
  // January row that the user can still see in their calendar.
  const normalize = (d: string): string => (d.length === 7 ? `${d}-01` : d.slice(0, 10));
  const allDates = result.monthly_records.map((r) => normalize(r.date));
  const dataFirst = allDates[0] ?? '';
  const dataLast = allDates[allDates.length - 1] ?? '';
  const [fromDate, setFromDate] = useState<string>('');
  const [toDate, setToDate] = useState<string>('');

  const filteredRecords = useMemo(() => {
    if (!fromDate && !toDate) return result.monthly_records;
    return result.monthly_records.filter((r) => {
      const d = normalize(r.date);
      if (fromDate && d < fromDate) return false;
      if (toDate && d > toDate) return false;
      return true;
    });
  }, [result, fromDate, toDate]);

  const longData = useMemo(
    () => buildTimelineData(filteredRecords, (h) => h.side !== 'short'),
    [filteredRecords],
  );
  const shortData = useMemo(
    () => isLongShort
      ? buildTimelineData(filteredRecords, (h) => h.side === 'short')
      : null,
    [filteredRecords, isLongShort],
  );

  const sliceControls = (
    <SliceControls
      fromDate={fromDate}
      toDate={toDate}
      dataFirst={dataFirst}
      dataLast={dataLast}
      onFromChange={setFromDate}
      onToChange={setToDate}
      onReset={() => { setFromDate(''); setToDate(''); }}
    />
  );

  if (!isLongShort) {
    return (
      <div className="flex flex-col gap-2">
        {sliceControls}
        <TimelinePanel
          title="Sector Timeline"
          data={longData}
          defaultCollapsed={false}
        />
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-4">
      {sliceControls}
      <TimelinePanel
        title="Sector Timeline · Long"
        data={longData}
        defaultCollapsed={false}
      />
      <TimelinePanel
        title="Sector Timeline · Short"
        data={shortData!}
        defaultCollapsed={false}
      />
    </div>
  );
}

function SliceControls({
  fromDate,
  toDate,
  dataFirst,
  dataLast,
  onFromChange,
  onToChange,
  onReset,
}: {
  fromDate: string;
  toDate: string;
  dataFirst: string;
  dataLast: string;
  onFromChange: (v: string) => void;
  onToChange: (v: string) => void;
  onReset: () => void;
}) {
  const isSliced = !!fromDate || !!toDate;
  return (
    <div className="bg-[#151821] rounded-xl border border-gray-800/40 px-4 py-2.5 flex items-center gap-3 flex-wrap">
      <span className="text-[11px] text-gray-500">Zoom:</span>
      <label className="flex items-center gap-1.5 text-[11px] text-gray-400">
        <span>From</span>
        <input
          type="date"
          value={fromDate}
          min={dataFirst.length === 10 ? dataFirst : undefined}
          max={dataLast.length === 10 ? dataLast : undefined}
          onChange={(e) => onFromChange(e.target.value)}
          className="bg-[#0f1117] border border-gray-700 rounded px-2 py-1 text-xs text-gray-200 focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
        />
      </label>
      <label className="flex items-center gap-1.5 text-[11px] text-gray-400">
        <span>To</span>
        <input
          type="date"
          value={toDate}
          min={dataFirst.length === 10 ? dataFirst : undefined}
          max={dataLast.length === 10 ? dataLast : undefined}
          onChange={(e) => onToChange(e.target.value)}
          className="bg-[#0f1117] border border-gray-700 rounded px-2 py-1 text-xs text-gray-200 focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
        />
      </label>
      {isSliced && (
        <button
          type="button"
          onClick={onReset}
          className="text-[11px] text-gray-500 hover:text-gray-300 underline-offset-2 hover:underline"
        >
          reset
        </button>
      )}
      {!isSliced && (
        <span className="text-[10px] text-gray-600 ml-auto">
          showing full range {dataFirst} → {dataLast}
        </span>
      )}
    </div>
  );
}

function TimelinePanel({
  title,
  data,
  defaultCollapsed,
}: {
  title: string;
  data: TimelineData;
  defaultCollapsed: boolean;
}) {
  const { sectors, runs, runByMonth, weightByMonth, months, cadenceDays } = data;
  const [hoveredRun, setHoveredRun] = useState<{ sector: string; runIdx: number } | null>(null);
  const [hoveredCell, setHoveredCell] = useState<{ sector: string; monthIdx: number } | null>(null);
  const [tooltipPos, setTooltipPos] = useState<{ x: number; y: number } | null>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const scrollAreaRef = useRef<HTMLDivElement>(null);
  // Wraps the two-column panel (labels + scroll area). Wheel and drag
  // handlers attach here so panning works no matter where in the panel
  // the cursor sits — including over the label column on the left.
  const panelRef = useRef<HTMLDivElement>(null);
  // The label column is rendered as a sibling of the scroll area (not
  // inside it via `position: sticky`), so cells physically can't render
  // over the label background no matter what the stacking context does.
  // The scroll area's clientWidth is therefore the cell-area width
  // directly — no need to subtract a label-column width.
  const [cellAreaWidth, setCellAreaWidth] = useState(0);
  const [isDragging, setIsDragging] = useState(false);
  const dragState = useRef<{ startX: number; startScroll: number; moved: boolean } | null>(null);

  // Resize observer keeps cell width in sync with the panel width.
  useLayoutEffect(() => {
    const el = scrollAreaRef.current;
    if (!el) return;
    const ro = new ResizeObserver(() => {
      setCellAreaWidth(el.clientWidth);
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  // cellWidth = max(MIN_CELL_WIDTH, area/N). When records fit at the
  // minimum cell width, they expand to fill the available space (no
  // scroll). When records would be narrower than the minimum, cells stay
  // at MIN_CELL_WIDTH and the panel scrolls horizontally — the user
  // wheels or drags to pan.
  const cellWidth = cellAreaWidth > 0 && months.length > 0
    ? Math.max(MIN_CELL_WIDTH, cellAreaWidth / months.length)
    : 0;
  const cellsTotalWidth = cellWidth * months.length;
  // The label column is a separate sibling now, so the scroll area's
  // inner content is just the cells (year axis + sector rows). Initial
  // scrollLeft = scrollWidth pins us to the right (most recent periods).
  const innerWidth = cellsTotalWidth;
  const hasOverflow = cellsTotalWidth > cellAreaWidth + 0.5;

  // On mount + when content/cell-width changes, jump to the right edge so
  // the most recent ~5 years are visible. The user pans backward in time
  // by scrolling left.
  useEffect(() => {
    const el = scrollAreaRef.current;
    if (!el || cellWidth === 0) return;
    el.scrollLeft = el.scrollWidth;
  }, [cellWidth, months.length]);

  // Mouse-wheel converts vertical wheel input into horizontal scroll, but
  // only when the panel actually overflows — otherwise normal page scroll
  // behavior is preserved. Trackpad horizontal gestures (deltaX != 0) are
  // added on top so two-finger swipes feel native. Listener lives on the
  // whole panel (panelRef) so wheeling over the label column on the
  // left also pans the cells — otherwise scrolling there would fall
  // through to the page.
  useEffect(() => {
    const panel = panelRef.current;
    if (!panel) return;
    const onWheel = (e: WheelEvent) => {
      const scroller = scrollAreaRef.current;
      if (!scroller) return;
      if (scroller.scrollWidth <= scroller.clientWidth) return;
      e.preventDefault();
      scroller.scrollLeft += e.deltaY + e.deltaX;
    };
    panel.addEventListener('wheel', onWheel, { passive: false });
    return () => panel.removeEventListener('wheel', onWheel);
  }, []);

  // Drag-to-pan. Mouse-down captures the start; document-level mousemove +
  // mouseup keep the drag active even if the cursor leaves the panel.
  // `moved` is checked on click so a small unintentional drag still lets
  // the cell hover/tooltip work.
  useEffect(() => {
    if (!isDragging) return;
    const onMove = (e: MouseEvent) => {
      const el = scrollAreaRef.current;
      const s = dragState.current;
      if (!el || !s) return;
      const dx = e.clientX - s.startX;
      if (Math.abs(dx) > 3) s.moved = true;
      el.scrollLeft = s.startScroll - dx;
    };
    const onUp = () => {
      setIsDragging(false);
    };
    document.addEventListener('mousemove', onMove);
    document.addEventListener('mouseup', onUp);
    return () => {
      document.removeEventListener('mousemove', onMove);
      document.removeEventListener('mouseup', onUp);
    };
  }, [isDragging]);

  if (sectors.length === 0) {
    return (
      <CollapsibleCard
        title={title}
        rightSlot="no sectors held"
        defaultCollapsed={defaultCollapsed}
      >
        {null}
      </CollapsibleCard>
    );
  }

  const handleMouseDown = (e: React.MouseEvent) => {
    const el = scrollAreaRef.current;
    if (!el) return;
    if (el.scrollWidth <= el.clientWidth) return; // nothing to pan
    dragState.current = { startX: e.clientX, startScroll: el.scrollLeft, moved: false };
    setIsDragging(true);
    // Hide any open tooltip while panning.
    setHoveredRun(null);
    setHoveredCell(null);
    setTooltipPos(null);
  };

  const handleEnter = (sector: string, monthIdx: number, e: React.MouseEvent) => {
    if (isDragging) return;
    const map = runByMonth.get(sector);
    if (!map) return;
    const runIdx = map[monthIdx];
    if (runIdx < 0) return;
    setHoveredRun({ sector, runIdx });
    setHoveredCell({ sector, monthIdx });
    setTooltipPos({ x: e.clientX, y: e.clientY });
  };
  const handleMove = (e: React.MouseEvent) => {
    if (isDragging) return;
    if (hoveredRun) setTooltipPos({ x: e.clientX, y: e.clientY });
  };
  const handleLeave = () => {
    setHoveredRun(null);
    setHoveredCell(null);
    setTooltipPos(null);
  };

  const runForTooltip = hoveredRun
    ? runs.get(hoveredRun.sector)?.[hoveredRun.runIdx] ?? null
    : null;

  return (
    <div ref={containerRef} onMouseLeave={handleLeave}>
      <CollapsibleCard
        title={title}
        rightSlot={`${sectors.length} sectors over ${months.length} months · ${hasOverflow ? 'scroll the timeline (wheel or drag) to pan through earlier periods' : 'full range fits the panel'}`}
        defaultCollapsed={defaultCollapsed}
        bodyClassName="px-5 pb-5"
      >
        {/* Two-column layout: a fixed-width label column (NOT inside the
            scroll area, so no z-index / sticky shenanigans can ever let
            cells render over labels) and a scrolling cells panel beside
            it. Heights line up because both columns produce the same row
            sequence with identical margins / heights. Drag + wheel
            handlers attach to this outer flex container so panning works
            from anywhere on the panel — including the label column. */}
        <div
          ref={panelRef}
          className="flex select-none"
          style={{ cursor: isDragging ? 'grabbing' : hasOverflow ? 'grab' : 'default' }}
          onMouseDown={handleMouseDown}
          onMouseMove={handleMove}
        >
          <div
            className="shrink-0"
            style={{
              width: SECTOR_LABEL_WIDTH + SECTOR_LABEL_GAP,
              backgroundColor: '#151821',
              borderRight: '1px solid rgba(75, 85, 99, 0.6)',
              boxShadow: '6px 0 8px -6px rgba(0, 0, 0, 0.6)',
              // High zIndex on the column itself isn't strictly needed
              // (it's a sibling of the scroll area, not an overlay), but
              // it covers any future overlay that might try to creep in.
              position: 'relative',
              zIndex: 1,
            }}
          >
            {/* Spacer for the year-axis row in the scroll panel. Same
                height as the year-axis cells so the first sector label
                lines up with the first sector row. */}
            <div className="text-[9px] font-mono" aria-hidden="true">&nbsp;</div>
            {sectors.map((sec, sIdx) => {
              const color = colorForSector(sec, sIdx);
              return (
                <div
                  key={`label-${sec}`}
                  className="flex items-center mt-1.5 h-5 pr-3 text-[11px] text-gray-300 truncate gap-1.5"
                >
                  <span className="inline-block w-2 h-2 rounded-sm shrink-0" style={{ background: color }} />
                  <span className="truncate">{sec}</span>
                </div>
              );
            })}
          </div>
          <div
            ref={scrollAreaRef}
            className="overflow-x-auto flex-1 min-w-0"
          >
            <div style={{ width: innerWidth, position: 'relative' }}>
              {/* Year axis above the cell rows. */}
              <div className="flex">
                {months.map((m, i) => {
                  const thisYear = m.slice(0, 4);
                  const prevYear = i > 0 ? months[i - 1].slice(0, 4) : '';
                  const isYear = i === 0 || thisYear !== prevYear;
                  return (
                    <div
                      key={`yax-${m}`}
                      className="text-[9px] text-gray-600 font-mono shrink-0"
                      style={{
                        width: cellWidth,
                        borderLeft: isYear ? '1px solid rgba(75,85,99,0.35)' : undefined,
                      }}
                    >
                      {isYear ? <span className="pl-0.5">{thisYear}</span> : ''}
                    </div>
                  );
                })}
              </div>

              {/* One row of cells per sector — labels render in the
                  sibling column on the left. */}
              {sectors.map((sec, sIdx) => {
                const color = colorForSector(sec, sIdx);
                const map = runByMonth.get(sec);
                const wts = weightByMonth.get(sec);
                return (
                  <div key={sec} className="flex items-stretch mt-1.5 h-5">
                    {months.map((m, mIdx) => {
                      const runIdx = map?.[mIdx] ?? -1;
                      const held = runIdx >= 0;
                      const w = wts?.[mIdx] ?? 0;
                      const inHoveredRun = hoveredRun
                        && hoveredRun.sector === sec
                        && hoveredRun.runIdx === runIdx;
                      const isThisHovered = hoveredCell
                        && hoveredCell.sector === sec
                        && hoveredCell.monthIdx === mIdx;
                      const baseAlpha = 0.25 + (w / 100) * 0.75;
                      return (
                        <div
                          key={`${sec}-${m}`}
                          className="shrink-0"
                          style={{
                            width: cellWidth,
                            background: held ? color : 'transparent',
                            opacity: held ? baseAlpha : 1,
                            // Visible separator between cells. Background
                            // matches the panel so empty cells stay empty
                            // while held cells appear to have a 1-px gap
                            // between them — same trick as `gap`, but
                            // box-sizing keeps each cell exactly cellWidth
                            // so the layout math (innerWidth, scroll-to-
                            // right) doesn't drift.
                            borderRight: '1px solid #151821',
                            boxSizing: 'border-box',
                            // Hover highlight is borders-only — no
                            // brightness/opacity changes on the cell
                            // itself, since those looked muddy at small
                            // cell widths. The hovered cell gets a white
                            // 2-px outline; other cells in the same run
                            // get a 1-px outline in the sector color so
                            // the run extent stays readable.
                            outline: isThisHovered
                              ? '2px solid rgba(255,255,255,0.95)'
                              : inHoveredRun
                                ? `1px solid ${color}`
                                : undefined,
                            outlineOffset: isThisHovered ? '0px' : inHoveredRun ? '1px' : undefined,
                            zIndex: isThisHovered ? 5 : undefined,
                            position: isThisHovered ? 'relative' : undefined,
                            cursor: isDragging ? 'grabbing' : 'pointer',
                          }}
                          onMouseEnter={(e) => handleEnter(sec, mIdx, e)}
                        />
                      );
                    })}
                  </div>
                );
              })}
            </div>
          </div>
        </div>
      </CollapsibleCard>

      {runForTooltip && tooltipPos && (
        <div
          className="fixed z-[300] pointer-events-none bg-[#1a1d27] border border-gray-700 rounded-lg px-3 py-2 shadow-xl text-xs"
          style={{
            top: tooltipPos.y + 14,
            left: Math.min(window.innerWidth - 280, tooltipPos.x + 14),
            minWidth: 220,
          }}
        >
          <div className="flex items-center gap-2 mb-1">
            <span
              className="inline-block w-2.5 h-2.5 rounded-sm"
              style={{ background: colorForSector(runForTooltip.sector, sectors.indexOf(runForTooltip.sector)) }}
            />
            <span className="text-gray-100 font-medium">{runForTooltip.sector}</span>
          </div>
          <div className="text-gray-400 font-mono">
            {runForTooltip.startMonth} → {runForTooltip.endMonth}
            <span className="text-gray-500"> ({formatRunDuration(runForTooltip.monthsHeld, cadenceDays)})</span>
          </div>
          {(() => {
            // CAGR needs duration in months. monthsHeld is the cell count;
            // for non-monthly cadences (weekly, daily, every_2_months,
            // every_3_months) multiply through cadenceDays/30 so the
            // exponent is right. Otherwise a single-week run would get
            // 12× annualization rather than 52×.
            const monthsEquiv = (runForTooltip.monthsHeld * cadenceDays) / 30;
            const cagr = annualize(runForTooltip.cumulativeReturnPct, monthsEquiv);
            const runColor = (v: number | null | undefined) =>
              v == null ? 'text-gray-500' : v >= 0 ? 'text-emerald-400 font-mono' : 'text-rose-400 font-mono';
            return (
              <>
                <div className="mt-1 text-gray-400">
                  Run return:{' '}
                  <span className={runColor(runForTooltip.cumulativeReturnPct)}>
                    {fmtPct(runForTooltip.cumulativeReturnPct)}
                  </span>
                </div>
                <div className="text-gray-400">
                  CAGR:{' '}
                  <span className={runColor(cagr)}>
                    {fmtPct(cagr)}
                  </span>
                </div>
                <div className="text-gray-600 text-[10px] mt-1">
                  equal-weighted across this sector&apos;s holdings
                </div>
              </>
            );
          })()}
        </div>
      )}
    </div>
  );
}
