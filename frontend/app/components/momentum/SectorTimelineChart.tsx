'use client';

import { useMemo, useRef, useState } from 'react';
import type { BacktestResult } from '../../../lib/stores/momentum';
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
  endIdx: number;            // inclusive month index in result.monthly_records
  startMonth: string;        // YYYY-MM
  endMonth: string;          // YYYY-MM
  monthsHeld: number;
  cumulativeReturnPct: number | null;
};

export default function SectorTimelineChart({ result }: Props) {
  // Build everything in one pass: per-month sector counts, per-(sector,month)
  // weight, and the list of runs (consecutive held months) per sector with
  // a chain-linked equal-weighted return for each run.
  const { sectors, runs, runByMonth, weightByMonth, months } = useMemo(() => {
    const months = result.monthly_records.map((r) => r.date);
    // sector → array of runs (in chronological order)
    const runs = new Map<string, Run[]>();
    // (sector, monthIdx) → run index within runs.get(sector); -1 if not held
    const runByMonth = new Map<string, Int16Array>();
    // (sector, monthIdx) → weight (% of portfolio)
    const weightByMonth = new Map<string, Float32Array>();
    // sector → total months held (for ordering)
    const totalHeld = new Map<string, number>();

    // First pass: collect every sector ever held + per-month weights.
    for (let i = 0; i < result.monthly_records.length; i++) {
      const rec = result.monthly_records[i];
      if (rec.holdings.length === 0) continue;
      for (const h of rec.holdings) {
        const sec = h.sector || 'Unknown';
        totalHeld.set(sec, (totalHeld.get(sec) ?? 0) + 1);
        if (!weightByMonth.has(sec)) {
          weightByMonth.set(sec, new Float32Array(months.length));
        }
      }
    }

    // Sort sectors by total months held desc — most persistent on top.
    const sectors = Array.from(totalHeld.keys()).sort(
      (a, b) => (totalHeld.get(b)! - totalHeld.get(a)!) || a.localeCompare(b),
    );

    // Initialize runByMonth with -1.
    for (const sec of sectors) {
      const arr = new Int16Array(months.length);
      arr.fill(-1);
      runByMonth.set(sec, arr);
      runs.set(sec, []);
    }

    // Second pass: build runs + weights + per-month sector returns.
    // For each sector, track an open run; close it when the sector is absent
    // for a month, and stash the chain-linked return.
    type OpenRun = { startIdx: number; rets: number[] };
    const open = new Map<string, OpenRun>();

    for (let i = 0; i < result.monthly_records.length; i++) {
      const rec = result.monthly_records[i];
      const total = rec.holdings.length;
      // Group this month's holdings by sector → mean forward return.
      const sectorReturns = new Map<string, number[]>();
      const sectorCount = new Map<string, number>();
      for (const h of rec.holdings) {
        const sec = h.sector || 'Unknown';
        sectorCount.set(sec, (sectorCount.get(sec) ?? 0) + 1);
        if (h.forward_return_pct != null) {
          const arr = sectorReturns.get(sec) ?? [];
          arr.push(h.forward_return_pct);
          sectorReturns.set(sec, arr);
        }
      }
      // Update weights, extend open runs.
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
      // Close any open runs whose sector wasn't held this month.
      for (const sec of [...open.keys()]) {
        if ((sectorCount.get(sec) ?? 0) === 0) {
          const r = open.get(sec)!;
          closeRun(sec, r, i - 1);
          open.delete(sec);
        }
      }
    }
    // Close any still-open runs at the end of the backtest.
    for (const [sec, r] of open) {
      closeRun(sec, r, result.monthly_records.length - 1);
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
  }, [result]);

  const [hoveredRun, setHoveredRun] = useState<{ sector: string; runIdx: number } | null>(null);
  const [tooltipPos, setTooltipPos] = useState<{ x: number; y: number } | null>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  if (sectors.length === 0) return null;

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

  // Resolved hovered run for the tooltip body.
  const runForTooltip = hoveredRun
    ? runs.get(hoveredRun.sector)?.[hoveredRun.runIdx] ?? null
    : null;

  return (
    <div
      ref={containerRef}
      className="bg-[#151821] rounded-xl border border-gray-800/40 p-5"
      onMouseLeave={handleLeave}
    >
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-white text-sm font-medium">Sector Timeline</h3>
        <span className="text-[11px] text-gray-500">
          {sectors.length} sectors over {months.length} months · hover any cell to see when the sector was held and the return for that run
        </span>
      </div>

      <div className="overflow-x-auto" onMouseMove={handleMove}>
        <div className="min-w-[800px]">
          {/* Year axis above the rows */}
          <div className="flex" style={{ paddingLeft: 152 /* sector label col + gap */ }}>
            {months.map((m, i) => {
              const month = m.slice(5, 7);
              const isYear = month === '01' || i === 0;
              return (
                <div
                  key={`yax-${m}`}
                  className="flex-1 min-w-[6px] text-[9px] text-gray-600 font-mono shrink-0"
                  style={{ borderLeft: isYear ? '1px solid rgba(75,85,99,0.35)' : undefined }}
                >
                  {isYear ? <span className="pl-0.5">{m.slice(0, 4)}</span> : ''}
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
                {/* Sector label */}
                <div className="w-[140px] shrink-0 pr-3 text-[11px] text-gray-300 truncate flex items-center gap-1.5">
                  <span className="inline-block w-2 h-2 rounded-sm shrink-0" style={{ background: color }} />
                  <span className="truncate">{sec}</span>
                </div>
                {/* Cells: one per month */}
                <div className="flex flex-1 gap-[1px] items-stretch h-5">
                  {months.map((m, mIdx) => {
                    const runIdx = map?.[mIdx] ?? -1;
                    const held = runIdx >= 0;
                    const w = wts?.[mIdx] ?? 0;
                    const inHoveredRun = hoveredRun
                      && hoveredRun.sector === sec
                      && hoveredRun.runIdx === runIdx;
                    // Opacity scales with weight: 0%→0.25, 100%→1.0.
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

      {/* Floating tooltip */}
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
