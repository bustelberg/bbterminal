'use client';

import type { SectorCount } from './types';

/** Sector distribution bars for an expanded universe card. Right half. */
export default function SectorBreakdown({ sectors, totalRows }: { sectors: SectorCount[]; totalRows: number }) {
  if (!sectors.length) {
    return <div className="p-5 text-xs text-fg-subtle border-t lg:border-t-0 lg:border-l border-neutral-800/40">No sector data.</div>;
  }
  const maxCount = sectors[0]?.count || 1;
  return (
    <div className="p-5 border-t lg:border-t-0 lg:border-l border-neutral-800/40">
      <div className="text-fg-muted text-xs font-medium mb-2">Sector breakdown ({sectors.length})</div>
      <div className="max-h-48 overflow-auto space-y-1">
        {sectors.map(s => {
          const pct = totalRows ? (s.count / totalRows) * 100 : 0;
          const barPct = (s.count / maxCount) * 100;
          return (
            <div key={s.sector} className="text-xs">
              <div className="flex items-center justify-between text-fg-muted">
                <span className="truncate pr-2">{s.sector}</span>
                <span className="font-mono text-fg-subtle shrink-0">{s.count} · {pct.toFixed(1)}%</span>
              </div>
              <div className="mt-0.5 h-1 bg-neutral-800 rounded-full overflow-hidden">
                <div className="h-full bg-accent-500/60" style={{ width: `${barPct}%` }} />
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
