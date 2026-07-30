'use client';

import { useMemo, useState } from 'react';
import { sectorLabel } from '../../../lib/assetLabels';
import LwLineChart from '../LwLineChart';
import PerfTables from './PerfTables';
import { pct, ratio, ratioColor, retColor } from './format';
import { overallPerf } from './perfStats';
import { buildRegimeBands } from './regimeBands';

export type SectorIndex = {
  sector: string; size: number; dates: string[]; index: number[];
  // Present only when the sector has ≥220 days — enables the regime overlay.
  ma200?: number[]; bull?: boolean[]; turb?: boolean[];
};

function SectorCard({ s, overlay }: { s: SectorIndex; overlay?: boolean }) {
  const [open, setOpen] = useState(false);
  const line = useMemo(() => {
    const base = s.index[0] || 1;
    return s.dates.map((d, i) => ({ date: d, value: (s.index[i] / base) * 100 }));
  }, [s]);
  const summary = useMemo(() => overallPerf(s.dates, s.index), [s]);
  const canOverlay = overlay && !!s.bull?.length;
  const bands = useMemo(
    () => (canOverlay ? buildRegimeBands(s.dates, s.bull!, s.turb!) : undefined),
    [canOverlay, s],
  );

  return (
    <div className="rounded-lg border border-neutral-800/40 bg-elevated overflow-hidden">
      <button type="button" onClick={() => setOpen((v) => !v)}
        className="w-full flex items-center gap-3 px-3 py-2 text-left hover:bg-overlay/[0.02] transition-colors">
        <span className="text-fg-faint text-[10px] w-3 shrink-0">{open ? '▾' : '▸'}</span>
        <span className="text-sm text-fg-strong font-medium truncate flex-1">{sectorLabel(s.sector)}</span>
        <span className="text-[10px] text-fg-faint shrink-0">{s.size} name{s.size === 1 ? '' : 's'}</span>
        {summary && (
          <span className="flex items-center gap-3 shrink-0 font-mono text-xs">
            <span className={retColor(summary.ret)} title="Full-period annualized return (CAGR)">{pct(summary.ret)}</span>
            <span className={ratioColor(summary.sharpe)} title="Full-period Sharpe">Sharpe {ratio(summary.sharpe)}</span>
          </span>
        )}
      </button>
      {open && (
        <div className="px-3 pb-3 pt-1 space-y-3 border-t border-neutral-800/40">
          <LwLineChart data={line} scale="log" unit="=100" bands={bands} />
          <PerfTables dates={s.dates} level={s.index} />
        </div>
      )}
    </div>
  );
}

/** Per-sector equal-weight index chart + risk/return-by-period tables. Each
 * sector is a collapsible card (chart engines mount only when expanded). Cards
 * stream in progressively (largest sector first); `loading` shows the tail
 * indicator while more are still arriving. */
export default function SectorBreakdown({ sectors, loading, overlay }: { sectors: SectorIndex[]; loading?: boolean; overlay?: boolean }) {
  const [open, setOpen] = useState(false);  // whole section collapsed by default
  if (!sectors.length && !loading) return null;
  return (
    <div className="space-y-2">
      <button type="button" onClick={() => setOpen((v) => !v)}
        className="flex items-center gap-2 w-full text-left group">
        <span className="text-fg-faint text-[10px] w-3 shrink-0">{open ? '▾' : '▸'}</span>
        <span className="text-sm font-semibold text-fg-strong">By sector</span>
        <span className="text-[11px] text-fg-faint">({sectors.length}{loading ? '…' : ''})</span>
      </button>
      {open && (
        <>
          <p className="text-[10px] text-fg-faint leading-tight pl-5">
            Equal-weight index of each sector in the universe · expand a sector for its chart + risk/return by period.
          </p>
          <div className="space-y-1.5">
            {sectors.map((s) => <SectorCard key={s.sector} s={s} overlay={overlay} />)}
          </div>
          {loading && (
            <div className="flex items-center gap-2 text-[11px] text-fg-faint pt-0.5">
              <span className="loading-bar h-0.5 w-16 rounded-full" aria-hidden />
              Loading sectors… {sectors.length > 0 && `(${sectors.length} so far)`}
            </div>
          )}
        </>
      )}
    </div>
  );
}
