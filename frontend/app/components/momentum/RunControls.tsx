import { type VariantsRunState } from '../../../lib/stores/momentum';

import type { SelectionMode } from './useBacktestConfig';

/**
 * `RunControls` — the primary action in `/backtest`'s config panel:
 * "Run variants" (kick off the sweep). Presentational: the handler comes
 * from `useBacktestRun`; everything else is the disabled-state + tooltip
 * logic. Returns a Fragment so the button stays a direct child of the
 * parent's flex row.
 */
export default function RunControls({
  runVariantsBacktest,
  running,
  variantsRunning,
  eligibleCount,
  variantsBlockReason,
  longShortBlocked,
  selectionMode,
  variantsRun,
}: {
  runVariantsBacktest: () => void;
  running: boolean;
  variantsRunning: boolean;
  eligibleCount: number;
  variantsBlockReason: string | null;
  longShortBlocked: boolean;
  selectionMode: SelectionMode;
  variantsRun: VariantsRunState | null;
}) {
  return (
    <button
      onClick={runVariantsBacktest}
      disabled={running || variantsRunning || eligibleCount === 0 || variantsBlockReason != null}
      className="px-5 py-2 rounded-lg text-sm font-medium bg-accent-600 hover:bg-accent-500 text-fg-strong transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
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
  );
}
