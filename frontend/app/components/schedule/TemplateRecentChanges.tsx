'use client';

import { useEffect, useState } from 'react';
import LoadingDots from '../LoadingDots';
import { fmtTimestamp } from '../../../lib/format';
import { API_URL } from '../../../lib/apiUrl';

/** Recent additions/removals for one template. Fetched lazily on
 * expand from `GET /api/universe-templates/{key}/recent-changes`. */
export default function TemplateRecentChanges({ templateKey }: { templateKey: string }) {
  type ChangeEntry = {
    run_id: number;
    started_at: string;
    status: string;
    this_month: string | null;
    prev_month: string | null;
    additions_count: number;
    removals_count: number;
    renames_count: number;
    additions: Array<{ company_id: number; ticker?: string; name?: string | null; sector?: string | null }>;
    removals: Array<{ company_id: number; ticker?: string; name?: string | null; sector?: string | null }>;
    renames: Array<{ company_id: number; old_ticker?: string; new_ticker?: string; name?: string | null }>;
  };
  const [data, setData] = useState<ChangeEntry[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    let cancelled = false;
    fetch(`${API_URL}/api/universe-templates/${encodeURIComponent(templateKey)}/recent-changes?limit=5`)
      .then(async (r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((rows: ChangeEntry[]) => { if (!cancelled) setData(rows); })
      .catch((e: unknown) => { if (!cancelled) setError(e instanceof Error ? e.message : String(e)); });
    return () => { cancelled = true; };
  }, [templateKey]);

  if (error) {
    return (
      <div className="px-12 pb-3 text-xs text-neg-300">Failed to load recent changes: {error}</div>
    );
  }
  if (data == null) {
    return (
      <div className="px-12 pb-3 text-xs text-fg-subtle"><LoadingDots label="Loading recent changes" /></div>
    );
  }
  if (data.length === 0) {
    return (
      <div className="px-12 pb-3 text-xs text-fg-subtle">No pipeline runs have refreshed this template yet — recent additions/removals will appear here after the next tick.</div>
    );
  }
  return (
    <div className="px-12 pb-4 space-y-3">
      {data.map((entry) => {
        const noChanges = entry.additions_count === 0 && entry.removals_count === 0 && entry.renames_count === 0;
        return (
          <div key={`${entry.run_id}-${entry.this_month}`} className="bg-page border border-neutral-800/40 rounded-lg overflow-hidden">
            <div className="px-3 py-2 border-b border-neutral-800/40 flex items-baseline gap-3 flex-wrap text-xs">
              <span className="text-fg-soft font-mono">{fmtTimestamp(entry.started_at)}</span>
              <span className="text-fg-subtle font-mono">
                {entry.this_month ?? '—'}
                {entry.prev_month && <span className="text-fg-faint"> vs {entry.prev_month}</span>}
              </span>
              <span className="text-[10px] text-fg-subtle font-mono ml-auto">run #{entry.run_id}</span>
              <span className="text-xs text-fg-muted font-mono">
                +{entry.additions_count} / −{entry.removals_count}
                {entry.renames_count > 0 && <span> / r{entry.renames_count}</span>}
              </span>
            </div>
            {noChanges ? (
              <div className="px-3 py-2 text-[11px] text-fg-subtle italic">No constituent changes vs the prior month.</div>
            ) : (
              <div className="grid gap-2 md:grid-cols-3 p-3">
                {entry.additions_count > 0 && (
                  <DiffPanel
                    color="emerald" label="Additions" count={entry.additions_count}
                    items={entry.additions.map((a) => ({ key: a.company_id, primary: a.ticker ?? '?', secondary: a.name ?? null }))}
                  />
                )}
                {entry.removals_count > 0 && (
                  <DiffPanel
                    color="rose" label="Removals" count={entry.removals_count}
                    items={entry.removals.map((a) => ({ key: a.company_id, primary: a.ticker ?? '?', secondary: a.name ?? null }))}
                  />
                )}
                {entry.renames_count > 0 && (
                  <DiffPanel
                    color="amber" label="Renames" count={entry.renames_count}
                    items={entry.renames.map((r) => ({ key: r.company_id, primary: `${r.old_ticker} → ${r.new_ticker}`, secondary: r.name ?? null }))}
                  />
                )}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

function DiffPanel({
  color, label, count, items,
}: {
  color: 'emerald' | 'rose' | 'amber';
  label: string;
  count: number;
  items: Array<{ key: number; primary: string; secondary: string | null }>;
}) {
  const colorCls = color === 'emerald'
    ? 'text-pos-300 bg-pos-500/10 border-pos-500/30'
    : color === 'rose'
      ? 'text-neg-300 bg-neg-500/10 border-neg-500/30'
      : 'text-warn-300 bg-warn-500/10 border-warn-500/30';
  return (
    <div className="bg-card rounded border border-neutral-800/40">
      <div className="px-2.5 py-1 flex items-center gap-2 border-b border-neutral-800/40">
        <span className={`text-[10px] uppercase tracking-wider px-1.5 py-0.5 rounded border ${colorCls}`}>{label}</span>
        <span className="text-xs text-fg-soft font-mono">{count}</span>
      </div>
      <div className="max-h-48 overflow-auto divide-y divide-neutral-800/30">
        {items.slice(0, 50).map((it) => (
          <div key={it.key} className="px-2.5 py-1">
            <div className="text-xs font-mono text-fg">{it.primary}</div>
            {it.secondary && <div className="text-[10px] text-fg-subtle truncate">{it.secondary}</div>}
          </div>
        ))}
        {items.length > 50 && (
          <div className="px-2.5 py-1 text-[10px] text-fg-faint italic">+ {items.length - 50} more (open the run on /schedule for the full list)</div>
        )}
      </div>
    </div>
  );
}
