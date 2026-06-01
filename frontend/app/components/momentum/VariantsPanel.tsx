import {
  makeVariantKey,
  variantLabel,
} from '../../../lib/stores/momentum';

import AxisColumn from './AxisColumn';
import type { SelectionMode } from './useBacktestConfig';
import type { UseVariantSelectionResult } from './useVariantSelection';
import { toggleInSet } from './variantHelpers';

// Amber-chip threshold on the permutations count — purely a UI wall-time
// warning; the backend has no hard cap.
const LARGE_VARIANTS_THRESHOLD = 30;

/**
 * `VariantsPanel` — the cross-product sweep picker on `/backtest`. The
 * Run-variants button (in `RunControls`) fans the base config out across
 * every permutation enabled here. Each axis is a dimension of the
 * cross-product: Frequency (rebalance cadence) · Strategy (long-only /
 * long-short) · Universe · Grouping (sector / industry), plus three
 * comma-list numeric sweeps (top-N, per-N, min-price-score) and min/max
 * portfolio-size skips. Empty numeric axes inherit the base values.
 *
 * Presentational: all state + cross-product math lives in
 * `useVariantSelection`, passed in whole as `variantSel`. `eligibleCount`
 * / `totalPerms` are derived by the parent (eligibleCount also drives the
 * Run button), and `indexUniverses` supplies the Universe axis options.
 */
export default function VariantsPanel({
  variantSel,
  selectionMode,
  longShortBlocked,
  variantsBlockReason,
  topSectors,
  topPerSector,
  minPriceScore,
  indexUniverses,
  eligibleCount,
  totalPerms,
}: {
  variantSel: UseVariantSelectionResult;
  selectionMode: SelectionMode;
  longShortBlocked: boolean;
  variantsBlockReason: string | null;
  topSectors: number;
  topPerSector: number;
  minPriceScore: string;
  indexUniverses: { index_name: string; display_label: string; total_unique_tickers: number }[];
  eligibleCount: number;
  totalPerms: number;
}) {
  const {
    selectedFreqs, setSelectedFreqs,
    selectedStrategies, setSelectedStrategies,
    selectedUniverses, setSelectedUniverses,
    selectedGroupings, setSelectedGroupings,
    topSectorsSweep, setTopSectorsSweep,
    perSectorSweep, setPerSectorSweep,
    minScoreSweep, setMinScoreSweep,
    minPortfolioSizeRaw, setMinPortfolioSizeRaw,
    maxPortfolioSizeRaw, setMaxPortfolioSizeRaw,
    disabledPerms, setDisabledPerms,
    ALL_FREQS, ALL_STRATEGIES,
    minPortfolioSize, maxPortfolioSize,
    allPermutations, variantSize, belowMinSize, aboveMaxSize,
    togglePermDisabled,
  } = variantSel;

  return (
    <div className="border-t border-gray-800/60 pt-4 mb-4">
      <div className="flex items-baseline gap-3 mb-3">
        <h2 className="text-gray-300 text-xs font-semibold uppercase tracking-wider">
          Variants
        </h2>
        <span className="text-[10px] text-gray-500">
          Cross-product of the axes below — each permutation runs once and shows up in the variants table
        </span>
      </div>

      {variantsBlockReason && (
        <div className="mb-3 px-3 py-2 text-xs text-rose-300 bg-rose-500/10 border border-rose-500/30 rounded-lg">
          {variantsBlockReason}
        </div>
      )}

      {/* Sweep-axes row: three comma-separated text inputs. Empty means
          "inherit base, don't sweep". For min_price_score the literal
          `none` / `off` becomes a null token. */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-3 mb-3">
        <div>
          <label className="text-gray-500 text-xs block mb-1">
            Top {Array.from(selectedGroupings)[0] === 'industry' ? 'industries' : 'sectors'}{' '}
            <span className="text-gray-600 text-[10px]">(comma list, blank = inherit)</span>
          </label>
          <input
            type="text"
            value={topSectorsSweep}
            onChange={(e) => setTopSectorsSweep(e.target.value)}
            placeholder={`e.g. 3,4,5  (blank = ${topSectors})`}
            className="w-full bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-white text-sm font-mono focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
          />
        </div>
        <div>
          <label className="text-gray-500 text-xs block mb-1">
            Per {Array.from(selectedGroupings)[0] === 'industry' ? 'industry' : 'sector'}{' '}
            <span className="text-gray-600 text-[10px]">(comma list, blank = inherit)</span>
          </label>
          <input
            type="text"
            value={perSectorSweep}
            onChange={(e) => setPerSectorSweep(e.target.value)}
            placeholder={`e.g. 4,6,8  (blank = ${topPerSector})`}
            className="w-full bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-white text-sm font-mono focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
          />
        </div>
        <div>
          <label className="text-gray-500 text-xs block mb-1">
            Min price score{' '}
            <span className="text-gray-600 text-[10px]">(comma list; &quot;none&quot;/&quot;off&quot; = disabled)</span>
          </label>
          <input
            type="text"
            value={minScoreSweep}
            onChange={(e) => setMinScoreSweep(e.target.value)}
            placeholder={`e.g. 20,30,off  (blank = ${minPriceScore.trim() || 'off'})`}
            className="w-full bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-white text-sm font-mono focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
          />
        </div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-3 mb-3">
        <div>
          <label className="text-gray-500 text-xs block mb-1">
            Skip variants smaller than{' '}
            <span className="text-gray-600 text-[10px]">(blank = 12, 0 disables)</span>
          </label>
          <input
            type="number"
            min={0}
            value={minPortfolioSizeRaw}
            onChange={(e) => setMinPortfolioSizeRaw(e.target.value)}
            placeholder="blank = 12"
            className="w-full bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-white text-sm font-mono focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
          />
          {minPortfolioSize > 0 && belowMinSize.size > 0 && (
            <p className="text-[10px] text-amber-400/70 mt-1.5">
              Skipping {belowMinSize.size} of {allPermutations.length} (portfolio &lt; {minPortfolioSize}).
            </p>
          )}
        </div>
        <div>
          <label className="text-gray-500 text-xs block mb-1">
            Skip variants larger than{' '}
            <span className="text-gray-600 text-[10px]">(blank = 50, 0 disables)</span>
          </label>
          <input
            type="number"
            min={0}
            value={maxPortfolioSizeRaw}
            onChange={(e) => setMaxPortfolioSizeRaw(e.target.value)}
            placeholder="blank = 50"
            className="w-full bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-white text-sm font-mono focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
          />
          {maxPortfolioSize > 0 && aboveMaxSize.size > 0 && (
            <p className="text-[10px] text-amber-400/70 mt-1.5">
              Skipping {aboveMaxSize.size} of {allPermutations.length} (portfolio &gt; {maxPortfolioSize}).
            </p>
          )}
        </div>
      </div>

      {longShortBlocked && (
        <div className="mb-3 px-3 py-2 text-[11px] text-amber-300/80 bg-amber-500/5 border border-amber-500/20 rounded-lg">
          Long-short is disabled in {selectionMode === 'all' ? 'all-universe' : selectionMode === 'sector_etf' ? 'sector-ETF' : 'random'} mode (no top/bottom split to short on). Long-short rows below are greyed out.
        </div>
      )}

      <div className="grid grid-cols-1 md:grid-cols-2 gap-3 mb-3">
        <AxisColumn
          label="Frequency"
          options={ALL_FREQS}
          selected={selectedFreqs}
          onAll={() => setSelectedFreqs(new Set(ALL_FREQS))}
          onNone={() => setSelectedFreqs(new Set())}
          renderItem={(freq) => {
            const checked = selectedFreqs.has(freq);
            return (
              <label key={freq} className="flex items-center gap-2 px-2 py-1 rounded text-xs text-gray-300 hover:bg-white/5 cursor-pointer">
                <input
                  type="checkbox"
                  checked={checked}
                  onChange={() => toggleInSet(setSelectedFreqs, freq)}
                  className="accent-indigo-500 w-3.5 h-3.5 cursor-pointer"
                />
                <span>{freq}</span>
              </label>
            );
          }}
          maxHClass="max-h-56"
        />
        <AxisColumn
          label="Strategy"
          options={ALL_STRATEGIES}
          selected={selectedStrategies}
          onAll={() => setSelectedStrategies(new Set(ALL_STRATEGIES))}
          onNone={() => setSelectedStrategies(new Set())}
          renderItem={(strat) => {
            const checked = selectedStrategies.has(strat);
            const blocked = strat === 'long_short' && longShortBlocked;
            return (
              <label
                key={strat}
                className={`flex items-center gap-2 px-2 py-1 rounded text-xs ${
                  blocked ? 'text-gray-600 cursor-not-allowed' : 'text-gray-300 hover:bg-white/5 cursor-pointer'
                }`}
              >
                <input
                  type="checkbox"
                  checked={checked && !blocked}
                  disabled={blocked}
                  onChange={() => toggleInSet(setSelectedStrategies, strat)}
                  className="accent-indigo-500 w-3.5 h-3.5 cursor-pointer disabled:cursor-not-allowed"
                />
                <span>{strat}</span>
              </label>
            );
          }}
          maxHClass="max-h-32"
        />
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-3 mb-3">
        <AxisColumn
          label="Universe"
          options={indexUniverses.map((i) => i.index_name)}
          selected={selectedUniverses}
          onAll={() => setSelectedUniverses(new Set(indexUniverses.map((i) => i.index_name)))}
          onNone={() => setSelectedUniverses(new Set())}
          renderItem={(uni) => {
            const checked = selectedUniverses.has(uni);
            const entry = indexUniverses.find((i) => i.index_name === uni);
            return (
              <label key={uni} className="flex items-center gap-2 px-2 py-1 rounded text-xs text-gray-300 hover:bg-white/5 cursor-pointer">
                <input
                  type="checkbox"
                  checked={checked}
                  onChange={() => toggleInSet(setSelectedUniverses, uni)}
                  className="accent-indigo-500 w-3.5 h-3.5 cursor-pointer"
                />
                <span className="truncate">
                  {entry?.display_label ?? uni}
                  {entry?.total_unique_tickers != null && (
                    <span className="text-gray-600 ml-1">({entry.total_unique_tickers})</span>
                  )}
                </span>
              </label>
            );
          }}
          maxHClass="max-h-56"
        />
        <AxisColumn
          label="Grouping"
          options={['sector', 'industry'] as const}
          selected={selectedGroupings}
          onAll={() => setSelectedGroupings(new Set<'sector' | 'industry'>(['sector', 'industry']))}
          onNone={() => setSelectedGroupings(new Set<'sector' | 'industry'>())}
          renderItem={(grp) => {
            const checked = selectedGroupings.has(grp);
            // Industry only has data on LEONTEQ / ACWI_LEONTEQ — let the
            // user pick it anyway, but warn so they don't get a silently
            // empty result when paired with another universe.
            const hint = grp === 'industry'
              ? 'Only LEONTEQ / ACWI_LEONTEQ carry industry data — pairing with another universe will produce a backend error.'
              : 'Group picks by universe sector tag (every universe has this).';
            return (
              <label key={grp} className="flex items-center gap-2 px-2 py-1 rounded text-xs text-gray-300 hover:bg-white/5 cursor-pointer" title={hint}>
                <input
                  type="checkbox"
                  checked={checked}
                  onChange={() => toggleInSet(setSelectedGroupings, grp)}
                  className="accent-indigo-500 w-3.5 h-3.5 cursor-pointer"
                />
                <span>{grp}</span>
              </label>
            );
          }}
          maxHClass="max-h-32"
        />
      </div>

      {/* Permutations preview — every cross-product permutation with a
          per-row enable checkbox. The count chip turns amber once
          `eligibleCount` crosses LARGE_VARIANTS_THRESHOLD so wall-time
          costs are visible before the user hits run. */}
      <div className="border border-gray-800/60 rounded-lg overflow-hidden">
        <div className="flex items-center justify-between px-3 py-2 bg-[#0f1117] border-b border-gray-800/40">
          <div className="flex items-center gap-2">
            <span className="text-xs text-gray-400">Permutations</span>
            <span
              className={`text-[10px] font-mono px-1.5 py-0.5 rounded ${
                eligibleCount > LARGE_VARIANTS_THRESHOLD
                  ? 'bg-amber-500/15 text-amber-300 border border-amber-500/30'
                  : 'bg-gray-700/40 text-gray-300 border border-gray-700/40'
              }`}
              title={
                eligibleCount > LARGE_VARIANTS_THRESHOLD
                  ? `${eligibleCount} permutations will run sequentially — this may take a while`
                  : ''
              }
            >
              {eligibleCount} eligible / {totalPerms} total
            </span>
          </div>
          {totalPerms > eligibleCount && (
            <button
              type="button"
              onClick={() => setDisabledPerms(new Set())}
              className="text-[11px] text-indigo-400 hover:text-indigo-300"
            >
              Enable all
            </button>
          )}
        </div>
        <ul className="max-h-72 overflow-auto p-1">
          {allPermutations.length === 0 ? (
            <li className="px-3 py-2 text-xs text-gray-600">
              Pick at least one frequency, strategy, universe, and grouping above to generate permutations.
            </li>
          ) : (
            allPermutations.map((p) => {
              const key = makeVariantKey(p);
              const userDisabled = disabledPerms.has(key);
              const modeDisabled = longShortBlocked && p.strategy === 'long_short';
              const sizeBelowMin = belowMinSize.has(key);
              const sizeAboveMax = aboveMaxSize.has(key);
              const enabled = !userDisabled && !modeDisabled && !sizeBelowMin && !sizeAboveMax;
              const autoDisabled = modeDisabled || sizeBelowMin || sizeAboveMax;
              return (
                <li key={key}>
                  <label
                    className={`flex items-center gap-2 px-2 py-1.5 rounded text-xs ${
                      autoDisabled ? 'text-gray-600 cursor-not-allowed' : 'text-gray-300 hover:bg-white/5 cursor-pointer'
                    }`}
                  >
                    <input
                      type="checkbox"
                      checked={enabled}
                      disabled={autoDisabled}
                      onChange={() => togglePermDisabled(key)}
                      className="accent-indigo-500 w-3.5 h-3.5 cursor-pointer disabled:cursor-not-allowed"
                    />
                    <span className="truncate">
                      {variantLabel(p)}
                      {sizeBelowMin && (
                        <span className="ml-1.5 text-[9px] uppercase tracking-wider text-amber-400/70">
                          skipped · {variantSize(p)} &lt; {minPortfolioSize}
                        </span>
                      )}
                      {sizeAboveMax && (
                        <span className="ml-1.5 text-[9px] uppercase tracking-wider text-amber-400/70">
                          skipped · {variantSize(p)} &gt; {maxPortfolioSize}
                        </span>
                      )}
                    </span>
                  </label>
                </li>
              );
            })
          )}
        </ul>
      </div>
    </div>
  );
}
