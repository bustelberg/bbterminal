'use client';

import { WEEKDAY_LABELS } from '../momentum/utils';

/** Read-only breakdown of a backtest's config blob. Re-used by the add
 * picker and the per-strategy detail view. */
export function StrategyConfigDetail({ cfg }: { cfg: Record<string, unknown> }) {
  const selection = (cfg.selection_mode as string | undefined) ?? 'momentum';
  const strategy = (cfg.strategy_type as string | undefined) ?? 'long_only';
  const indexUniverse = (cfg.index_universe as string | null | undefined) ?? null;
  const universeLabel = (cfg.universe_label as string | null | undefined) ?? null;
  const startDate = (cfg.start_date as string | undefined) ?? '—';
  const endDate = (cfg.end_date as string | undefined) ?? '—';
  const topNSectors = cfg.top_n_sectors as number | undefined;
  const topNPerSector = cfg.top_n_per_sector as number | undefined;
  const maxCompanies = cfg.max_companies as number | null | undefined;
  const minPriceScore = cfg.min_price_score as number | null | undefined;
  const rebalanceFrequency = (cfg.rebalance_frequency as string | undefined) ?? 'monthly';
  const rebalanceWeekday = (cfg.rebalance_weekday as number | undefined) ?? 0;
  const signalWeights = (cfg.signal_weights as Record<string, number> | undefined) ?? {};
  const categoryWeights = (cfg.category_weights as Record<string, number> | undefined) ?? {};

  const universe = indexUniverse ?? universeLabel ?? 'All companies (no universe filter)';
  const sortedSignals = Object.entries(signalWeights).sort((a, b) => b[1] - a[1]);
  const sortedCategories = Object.entries(categoryWeights).sort((a, b) => b[1] - a[1]);

  return (
    <div className="bg-[#151821] border border-gray-800/40 rounded-lg p-3">
      <div className="grid gap-x-6 gap-y-2 grid-cols-1 sm:grid-cols-2 md:grid-cols-3 text-xs">
        <ConfigRow label="Selection mode" value={selection} />
        <ConfigRow label="Strategy type" value={strategy} />
        <ConfigRow label="Universe" value={universe} />
        <ConfigRow label="Date range" value={`${startDate} → ${endDate}`} />
        <ConfigRow label="Rebalance" value={rebalanceFrequency} />
        <ConfigRow label="Rebalance day" value={`first ${WEEKDAY_LABELS[rebalanceWeekday] ?? 'Monday'}`} />
        <ConfigRow label="Top N sectors" value={topNSectors != null ? String(topNSectors) : '—'} />
        <ConfigRow label="Top N per sector" value={topNPerSector != null ? String(topNPerSector) : '—'} />
        <ConfigRow label="Max companies" value={maxCompanies != null ? String(maxCompanies) : 'unlimited'} />
        <ConfigRow label="Min price score" value={minPriceScore != null ? `> ${minPriceScore}` : 'off'} />
      </div>

      {sortedCategories.length > 0 && (
        <div className="mt-3">
          <div className="text-[10px] uppercase tracking-wider text-gray-500 mb-1">Category weights</div>
          <div className="flex flex-wrap gap-2 text-xs">
            {sortedCategories.map(([cat, w]) => (
              <span key={cat} className="bg-[#0f1117] border border-gray-800/60 rounded px-2 py-0.5">
                <span className="text-gray-400 capitalize">{cat}</span>
                <span className="text-gray-500 mx-1.5">·</span>
                <span className="font-mono text-gray-200">{w.toFixed(2)}</span>
              </span>
            ))}
          </div>
        </div>
      )}

      {sortedSignals.length > 0 && (
        <div className="mt-3">
          <div className="text-[10px] uppercase tracking-wider text-gray-500 mb-1">
            Signal weights ({sortedSignals.filter(([, w]) => w > 0).length} active)
          </div>
          <div className="grid gap-x-4 gap-y-1 grid-cols-1 sm:grid-cols-2 md:grid-cols-3 text-xs">
            {sortedSignals.map(([sig, w]) => (
              <div
                key={sig}
                className={`flex items-baseline justify-between gap-2 ${w === 0 ? 'opacity-50' : ''}`}
              >
                <span className="text-gray-400 truncate">{sig}</span>
                <span className="font-mono text-gray-200 shrink-0">{w.toFixed(2)}</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

function ConfigRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-baseline justify-between gap-2 border-b border-gray-800/20 pb-1">
      <span className="text-gray-500">{label}</span>
      <span className="font-mono text-gray-200 text-right truncate">{value}</span>
    </div>
  );
}
