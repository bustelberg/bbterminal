import type { Dispatch, SetStateAction } from 'react';

import type { SelectionMode } from './useBacktestConfig';

/**
 * `StrategyModeSelect` — the Strategy selection-mode dropdown at the top
 * of `/backtest`'s config panel, plus the sector→ETF mapping status line
 * shown when "Sector ETF" mode is active. Presentational; state owned by
 * `useBacktestConfig` (mode) and `useSectorEtfs` (the mapping).
 */
export default function StrategyModeSelect({
  selectionMode,
  setSelectionMode,
  sectorEtfs,
  sectorEtfsLoading,
  sectorEtfsError,
}: {
  selectionMode: SelectionMode;
  setSelectionMode: Dispatch<SetStateAction<SelectionMode>>;
  sectorEtfs: Record<string, number>;
  sectorEtfsLoading: boolean;
  sectorEtfsError: string | null;
}) {
  return (
    <div>
      <label className="text-fg-subtle text-xs block mb-1">Strategy</label>
      <select
        value={selectionMode}
        onChange={(e) => setSelectionMode(e.target.value as SelectionMode)}
        className="bg-page border border-neutral-700 rounded-lg px-3 py-2 text-fg-strong text-sm focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none"
        title="Momentum ranks the universe by signal score (price and volume signal groups). MomentumExtra adds a third trend-quality group (trend continuity, up-day consistency, RSI overbought guard). Random picks sectors/stocks at random, as a baseline to beat. All holds every eligible name in the universe equal-weighted (index-proxy benchmark). Sector ETF ranks sectors via stock-aggregate momentum then holds the mapped sector ETF for each picked sector — set the mapping on /benchmarks."
      >
        <option value="momentum">Momentum</option>
        <option value="momentum_extra">MomentumExtra (+ trend pillar)</option>
        <option value="random">Random (baseline)</option>
        <option value="all">All universe (index proxy)</option>
        <option value="sector_etf">Sector ETF (per-sector benchmark)</option>
      </select>
      {selectionMode === 'sector_etf' && (
        <div className="text-[10px] mt-1 max-w-xs">
          {sectorEtfsLoading ? (
            <span className="text-fg-subtle">loading sector mapping…</span>
          ) : sectorEtfsError ? (
            <span className="text-neg-400">{sectorEtfsError}</span>
          ) : Object.keys(sectorEtfs).length === 0 ? (
            <span className="text-warn-400">
              No sector→ETF mappings yet. Open <a href="/benchmarks" className="underline">/benchmarks</a>{' '}and tag at least one benchmark with a sector.
            </span>
          ) : (
            <span className="text-fg-subtle">
              {Object.keys(sectorEtfs).length} sector{Object.keys(sectorEtfs).length === 1 ? '' : 's'} mapped:{' '}
              <span className="text-fg-muted">{Object.keys(sectorEtfs).sort().join(', ')}</span>
            </span>
          )}
        </div>
      )}
    </div>
  );
}
