import {
  type CurrentPicksSnapshotMeta,
  type VariantsRunState,
} from '../../../lib/stores/momentum';

import type { SelectionMode } from './useBacktestConfig';

/**
 * `RunControls` — the two primary actions in `/backtest`'s config panel:
 * "Run variants" (kick off the sweep) and "Current Picks" (load/compute
 * the live portfolio). Presentational: the handlers come from
 * `useBacktestRun`; everything else is the disabled-state + tooltip logic.
 * Returns a Fragment so the buttons stay direct children of the parent's
 * flex row.
 */
export default function RunControls({
  runVariantsBacktest,
  showCurrentPicks,
  running,
  variantsRunning,
  eligibleCount,
  variantsBlockReason,
  longShortBlocked,
  selectionMode,
  variantsRun,
  currentPicksSnapshots,
}: {
  runVariantsBacktest: () => void;
  showCurrentPicks: () => void;
  running: boolean;
  variantsRunning: boolean;
  eligibleCount: number;
  variantsBlockReason: string | null;
  longShortBlocked: boolean;
  selectionMode: SelectionMode;
  variantsRun: VariantsRunState | null;
  currentPicksSnapshots: CurrentPicksSnapshotMeta[];
}) {
  return (
    <>
      <button
        onClick={runVariantsBacktest}
        disabled={running || variantsRunning || eligibleCount === 0 || variantsBlockReason != null}
        className="px-5 py-2 rounded-lg text-sm font-medium bg-indigo-600 hover:bg-indigo-500 text-white transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
        title={
          variantsBlockReason
            ? variantsBlockReason
            : eligibleCount === 0
              ? longShortBlocked
                ? `${selectionMode === 'all' ? 'All-universe' : selectionMode === 'sector_etf' ? 'Sector-ETF' : 'Random'} mode supports long-only variants only — adjust the Strategy axis below`
                : 'Pick at least one permutation in the Variants panel below'
              : `Run ${eligibleCount} permutation${eligibleCount === 1 ? '' : 's'} and compare them in one table`
        }
      >
        {variantsRunning
          ? `Running variants ${variantsRun?.completed ?? 0}/${variantsRun?.total ?? 0}…`
          : `Run variants (${eligibleCount})`}
      </button>
      <button
        onClick={showCurrentPicks}
        disabled={running || selectionMode === 'random'}
        className="px-4 py-2 rounded-lg text-sm font-medium border border-gray-700 text-gray-300 hover:bg-white/5 hover:text-white transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
        title={
          selectionMode === 'random'
            ? 'Current Picks is unavailable for random selection mode'
            : currentPicksSnapshots.length > 0
              ? `Load most recent snapshot (${currentPicksSnapshots[0].as_of_date}, ${currentPicksSnapshots[0].triggered_by})`
              : 'No saved snapshot yet — first click will run a full compute and save it'
        }
      >
        Current Picks
      </button>
    </>
  );
}
