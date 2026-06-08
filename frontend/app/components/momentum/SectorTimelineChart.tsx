'use client';

import { memo, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { colorForSector } from '../../../lib/sectorColors';
import { chartTheme } from '../../../lib/chartTheme';
import type { BacktestResult } from '../../../lib/stores/momentum';
import CollapsibleCard from './CollapsibleCard';
import { annualize, fmtPct } from './utils';
import {
  buildTimelineData,
  formatRunDuration,
  type TimelineData,
} from './sectorTimeline';

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
  /** Optional "go-live" date (YYYY-MM-DD). Drawn as a red dashed vertical
   * line at the month cell containing it, matching the other charts. */
  markerDate?: string;
  /** Start collapsed (e.g. the /schedule strategy detail renders every
   * card collapsed). Defaults to expanded. */
  defaultCollapsed?: boolean;
};

// Sector color palette lives in `lib/sectorColors.ts` so /schedule's
// per-row sector chips share the exact same hue per sector.
// The pure data layer (run-building + monthly bucketing) lives in
// `./sectorTimeline.ts`; this file is the rendering/interaction shell.

function SectorTimelineChartInner({ result, markerDate, defaultCollapsed = false }: Props) {
  // A long-short backtest has at least one holding with side === 'short'.
  // Long-only results omit `side` entirely (or always have 'long'), so the
  // single-panel branch covers the original behavior unchanged.
  const isLongShort = useMemo(
    () => result.monthly_records.some((r) => r.holdings.some((h) => h.side === 'short')),
    [result],
  );

  const longData = useMemo(
    () => buildTimelineData(result.monthly_records, (h) => h.side !== 'short'),
    [result],
  );
  const shortData = useMemo(
    () => isLongShort
      ? buildTimelineData(result.monthly_records, (h) => h.side === 'short')
      : null,
    [result, isLongShort],
  );

  if (!isLongShort) {
    return (
      <TimelinePanel
        title="Sector Timeline"
        data={longData}
        defaultCollapsed={defaultCollapsed}
        markerDate={markerDate}
      />
    );
  }

  return (
    <div className="flex flex-col gap-4">
      <TimelinePanel
        title="Sector Timeline · Long"
        data={longData}
        defaultCollapsed={defaultCollapsed}
        markerDate={markerDate}
      />
      <TimelinePanel
        title="Sector Timeline · Short"
        data={shortData!}
        defaultCollapsed={defaultCollapsed}
        markerDate={markerDate}
      />
    </div>
  );
}

/** React.memo barrier — see MonthlyHoldingsTable / EquityCurveCard for
 * rationale. Only `result` is a prop; default shallow compare on the
 * object reference is exactly what we want. */
const SectorTimelineChart = memo(SectorTimelineChartInner);
export default SectorTimelineChart;

function TimelinePanel({
  title,
  data,
  defaultCollapsed,
  markerDate,
}: {
  title: string;
  data: TimelineData;
  defaultCollapsed: boolean;
  markerDate?: string;
}) {
  const { sectors, runs, runByMonth, weightByMonth, months, cadenceDays } = data;
  const [hoveredRun, setHoveredRun] = useState<{ sector: string; runIdx: number } | null>(null);
  const [hoveredCell, setHoveredCell] = useState<{ sector: string; monthIdx: number } | null>(null);
  const [tooltipPos, setTooltipPos] = useState<{ x: number; y: number } | null>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  // Scroll area + panel wrapper are attached via CALLBACK refs (setScrollArea
  // / setPanel below) rather than plain useRefs. CollapsibleCard UNMOUNTS its
  // body on collapse and remounts it on expand, so a one-shot effect bound to
  // the original node would be left stranded on a detached node after a
  // collapse→expand cycle (ResizeObserver stuck reporting 0 → zero-width
  // cells → blank timeline; wheel listener dead). Callback refs re-attach the
  // observer + wheel listener to the fresh node on every (re)mount.
  const scrollAreaRef = useRef<HTMLDivElement | null>(null);
  const panelRef = useRef<HTMLDivElement | null>(null);
  const roRef = useRef<ResizeObserver | null>(null);
  // The label column is rendered as a sibling of the scroll area (not
  // inside it via `position: sticky`), so cells physically can't render
  // over the label background no matter what the stacking context does.
  // The scroll area's clientWidth is therefore the cell-area width
  // directly — no need to subtract a label-column width.
  const [cellAreaWidth, setCellAreaWidth] = useState(0);
  // Bumped each time the scroll area (re)mounts, so the scroll-to-right
  // effect re-pins to the right edge after a collapse→expand even when
  // cellWidth happens to be unchanged.
  const [scrollAreaTick, setScrollAreaTick] = useState(0);
  const [isDragging, setIsDragging] = useState(false);
  const dragState = useRef<{ startX: number; startScroll: number; moved: boolean } | null>(null);

  // Wheel → horizontal scroll. Stable handler so the panel callback ref can
  // add/remove it across remounts. Non-passive (calls preventDefault), so it
  // can't be a React onWheel prop. Only pans when the panel overflows.
  const handleWheel = useCallback((e: WheelEvent) => {
    const scroller = scrollAreaRef.current;
    if (!scroller) return;
    if (scroller.scrollWidth <= scroller.clientWidth) return;
    e.preventDefault();
    scroller.scrollLeft += e.deltaY + e.deltaX;
  }, []);

  // Callback ref — (re)attach a ResizeObserver every time the scroll area
  // mounts; disconnect when it unmounts. Sets the initial width immediately
  // so cells size correctly on the first frame after an expand.
  const setScrollArea = useCallback((node: HTMLDivElement | null) => {
    if (roRef.current) { roRef.current.disconnect(); roRef.current = null; }
    scrollAreaRef.current = node;
    if (node) {
      const ro = new ResizeObserver(() => setCellAreaWidth(node.clientWidth));
      ro.observe(node);
      roRef.current = ro;
      setCellAreaWidth(node.clientWidth);
      setScrollAreaTick((t) => t + 1);
    }
  }, []);

  // Callback ref — bind/unbind the non-passive wheel listener on the panel
  // wrapper as it mounts/unmounts (panning works from anywhere on the panel,
  // including the label column).
  const setPanel = useCallback((node: HTMLDivElement | null) => {
    if (panelRef.current) panelRef.current.removeEventListener('wheel', handleWheel);
    panelRef.current = node;
    if (node) node.addEventListener('wheel', handleWheel, { passive: false });
  }, [handleWheel]);

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

  // Go-live marker: px offset from the left of the cell strip, placed
  // proportionally (by calendar day) within the month cell that contains
  // the date. Null when there's no marker or it falls before the visible
  // range. Cheap plain const — recomputes each render with cellWidth.
  let markerLeft: number | null = null;
  if (markerDate && cellWidth > 0 && months.length > 0) {
    const norm = (s: string) => (s.length === 7 ? `${s}-01` : s.slice(0, 10));
    let mi = -1;
    for (let i = 0; i < months.length; i++) {
      if (norm(months[i]) <= markerDate) mi = i;
      else break;
    }
    if (mi >= 0) {
      const cellStart = new Date(norm(months[mi])).getTime();
      const go = new Date(markerDate).getTime();
      const frac = Math.max(0, Math.min(1, (go - cellStart) / (cadenceDays * 86400000)));
      markerLeft = (mi + frac) * cellWidth;
    }
  }

  // On mount + when content/cell-width changes, jump to the right edge so
  // the most recent ~5 years are visible. The user pans backward in time
  // by scrolling left.
  useEffect(() => {
    const el = scrollAreaRef.current;
    if (!el || cellWidth === 0) return;
    el.scrollLeft = el.scrollWidth;
  }, [cellWidth, months.length, scrollAreaTick]);

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
          ref={setPanel}
          className="flex"
          style={{ cursor: isDragging ? 'grabbing' : hasOverflow ? 'grab' : 'default' }}
          onMouseDown={handleMouseDown}
          onMouseMove={handleMove}
        >
          <div
            className="shrink-0"
            style={{
              width: SECTOR_LABEL_WIDTH + SECTOR_LABEL_GAP,
              // Transparent so the labels sit on the card surface (no dark
              // "frozen column" block). The column is a sibling of the
              // scroll area — not an overlay — so cells can never render
              // over it regardless of background.
              backgroundColor: 'transparent',
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
                  className="flex items-center mt-1.5 h-5 pr-3 text-[11px] text-fg-soft truncate gap-1.5"
                >
                  <span className="inline-block w-2 h-2 rounded-sm shrink-0" style={{ background: color }} />
                  <span className="truncate">{sec}</span>
                </div>
              );
            })}
          </div>
          <div
            ref={setScrollArea}
            className="overflow-x-auto flex-1 min-w-0 select-none"
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
                      className="text-[9px] text-fg-faint font-mono shrink-0"
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

              {/* Go-live marker — a red dashed vertical line spanning the
                  cell rows at the month containing the date. pointer-events
                  off so it never blocks a cell hover. */}
              {markerLeft != null && (
                <div
                  className="absolute top-0 bottom-0 pointer-events-none"
                  style={{ left: markerLeft, borderLeft: `1.5px dashed ${chartTheme.goLiveLine}`, zIndex: 6 }}
                  title={`Go-live ${markerDate}`}
                />
              )}
            </div>
          </div>
        </div>
      </CollapsibleCard>

      {runForTooltip && tooltipPos && (
        <div
          className="fixed z-[300] pointer-events-none bg-elevated border border-neutral-700 rounded-lg px-3 py-2 shadow-xl text-xs"
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
            <span className="text-fg-bright font-medium">{runForTooltip.sector}</span>
          </div>
          <div className="text-fg-muted font-mono">
            {runForTooltip.startMonth} → {runForTooltip.endMonth}
            <span className="text-fg-subtle"> ({formatRunDuration(runForTooltip.monthsHeld, cadenceDays)})</span>
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
              v == null ? 'text-fg-subtle' : v >= 0 ? 'text-pos-400 font-mono' : 'text-neg-400 font-mono';
            return (
              <>
                <div className="mt-1 text-fg-muted">
                  Run return:{' '}
                  <span className={runColor(runForTooltip.cumulativeReturnPct)}>
                    {fmtPct(runForTooltip.cumulativeReturnPct)}
                  </span>
                </div>
                <div className="text-fg-muted">
                  CAGR:{' '}
                  <span className={runColor(cagr)}>
                    {fmtPct(cagr)}
                  </span>
                </div>
                <div className="text-fg-faint text-[10px] mt-1">
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
