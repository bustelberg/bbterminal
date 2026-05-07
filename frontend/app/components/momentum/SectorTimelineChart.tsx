'use client';

import { useMemo, useRef, useState } from 'react';
import type { BacktestResult, Holding, PeriodRecord } from '../../../lib/stores/momentum';
import CollapsibleCard from './CollapsibleCard';
import { annualize, fmtPct } from './utils';

type Props = {
  result: BacktestResult;
};

// Same hue-spread palette as before — each GICS sector gets a distinct hue
// so two adjacent rows in the Gantt are easy to tell apart.
const SECTOR_COLORS: Record<string, string> = {
  'Information Technology': '#3b82f6',
  'Communication Services': '#ec4899',
  'Health Care': '#10b981',
  'Financials': '#06b6d4',
  'Consumer Discretionary': '#f97316',
  'Consumer Staples': '#92400e',
  'Industrials': '#a855f7',
  'Energy': '#ef4444',
  'Utilities': '#fbbf24',
  'Materials': '#84cc16',
  'Real Estate': '#14b8a6',
};
const FALLBACK_PALETTE = [
  '#0891b2', '#7c3aed', '#16a34a', '#dc2626', '#d97706',
  '#0284c7', '#c026d3', '#65a30d',
];
function colorForSector(sector: string, idx: number): string {
  return SECTOR_COLORS[sector] ?? FALLBACK_PALETTE[idx % FALLBACK_PALETTE.length];
}

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
};

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
    const newRun: Run = {
      sector: sec,
      startIdx: openR.startIdx,
      endIdx,
      startMonth: months[openR.startIdx],
      endMonth: months[endIdx],
      monthsHeld: endIdx - openR.startIdx + 1,
      cumulativeReturnPct: cumulative,
    };
    const list = runs.get(sec)!;
    const newRunIdx = list.length;
    list.push(newRun);
    const map = runByMonth.get(sec)!;
    for (let j = openR.startIdx; j <= endIdx; j++) map[j] = newRunIdx;
  }

  return { sectors, runs, runByMonth, weightByMonth, months };
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
  const { sectors, runs, runByMonth, weightByMonth, months } = data;
  const [hoveredRun, setHoveredRun] = useState<{ sector: string; runIdx: number } | null>(null);
  const [tooltipPos, setTooltipPos] = useState<{ x: number; y: number } | null>(null);
  const containerRef = useRef<HTMLDivElement>(null);

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

  const handleEnter = (sector: string, monthIdx: number, e: React.MouseEvent) => {
    const map = runByMonth.get(sector);
    if (!map) return;
    const runIdx = map[monthIdx];
    if (runIdx < 0) return;
    setHoveredRun({ sector, runIdx });
    setTooltipPos({ x: e.clientX, y: e.clientY });
  };
  const handleMove = (e: React.MouseEvent) => {
    if (hoveredRun) setTooltipPos({ x: e.clientX, y: e.clientY });
  };
  const handleLeave = () => {
    setHoveredRun(null);
    setTooltipPos(null);
  };

  const runForTooltip = hoveredRun
    ? runs.get(hoveredRun.sector)?.[hoveredRun.runIdx] ?? null
    : null;

  return (
    <div ref={containerRef} onMouseLeave={handleLeave}>
      <CollapsibleCard
        title={title}
        rightSlot={`${sectors.length} sectors over ${months.length} months · hover any cell to see when the sector was held and the return for that run`}
        defaultCollapsed={defaultCollapsed}
        bodyClassName="px-5 pb-5"
      >
        <div className="overflow-x-auto" onMouseMove={handleMove}>
          <div className="min-w-[800px]">
            {/* Year axis above the rows. Mark only the first record of each
                new calendar year (works for both monthly "YYYY-MM" rows and
                daily "YYYY-MM-DD" rows — the daily case used to mark every
                January day, ~21 ticks per year). */}
            <div className="flex" style={{ paddingLeft: 152 /* sector label col + gap */ }}>
              {months.map((m, i) => {
                const thisYear = m.slice(0, 4);
                const prevYear = i > 0 ? months[i - 1].slice(0, 4) : '';
                const isYear = i === 0 || thisYear !== prevYear;
                return (
                  <div
                    key={`yax-${m}`}
                    className="flex-1 min-w-[6px] text-[9px] text-gray-600 font-mono shrink-0"
                    style={{ borderLeft: isYear ? '1px solid rgba(75,85,99,0.35)' : undefined }}
                  >
                    {isYear ? <span className="pl-0.5">{thisYear}</span> : ''}
                  </div>
                );
              })}
            </div>

            {/* One row per sector */}
            {sectors.map((sec, sIdx) => {
              const color = colorForSector(sec, sIdx);
              const map = runByMonth.get(sec);
              const wts = weightByMonth.get(sec);
              return (
                <div key={sec} className="flex items-center mt-1.5">
                  <div className="w-[140px] shrink-0 pr-3 text-[11px] text-gray-300 truncate flex items-center gap-1.5">
                    <span className="inline-block w-2 h-2 rounded-sm shrink-0" style={{ background: color }} />
                    <span className="truncate">{sec}</span>
                  </div>
                  <div className="flex flex-1 gap-[1px] items-stretch h-5">
                    {months.map((m, mIdx) => {
                      const runIdx = map?.[mIdx] ?? -1;
                      const held = runIdx >= 0;
                      const w = wts?.[mIdx] ?? 0;
                      const inHoveredRun = hoveredRun
                        && hoveredRun.sector === sec
                        && hoveredRun.runIdx === runIdx;
                      const baseAlpha = 0.25 + (w / 100) * 0.75;
                      const alpha = inHoveredRun ? 1.0 : baseAlpha;
                      return (
                        <div
                          key={`${sec}-${m}`}
                          className="flex-1 min-w-[3px] cursor-pointer transition-opacity"
                          style={{
                            background: held ? color : 'transparent',
                            opacity: held ? alpha : 1,
                            outline: inHoveredRun ? `1px solid ${color}` : undefined,
                            outlineOffset: inHoveredRun ? '1px' : undefined,
                          }}
                          onMouseEnter={(e) => handleEnter(sec, mIdx, e)}
                        />
                      );
                    })}
                  </div>
                </div>
              );
            })}
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
            <span className="text-gray-500"> ({runForTooltip.monthsHeld} mo)</span>
          </div>
          {(() => {
            const cagr = annualize(runForTooltip.cumulativeReturnPct, runForTooltip.monthsHeld);
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
