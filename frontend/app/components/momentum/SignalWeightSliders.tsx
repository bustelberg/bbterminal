import type { Dispatch, SetStateAction } from 'react';

import type { SignalDef } from './types';

/**
 * `SignalWeightSliders` — the momentum-mode signal-weight + category-weight
 * sliders from `/backtest`'s Strategy parameters section. Purely
 * presentational: all state is owned by `useBacktestConfig` and threaded
 * in. The parent guards on `selectionMode === 'momentum'` before
 * rendering this.
 */
export default function SignalWeightSliders({
  signalDefs,
  weights,
  setWeights,
  categories,
  categoryWeights,
  setCategoryWeights,
}: {
  signalDefs: SignalDef[];
  weights: Record<string, number>;
  setWeights: Dispatch<SetStateAction<Record<string, number>>>;
  categories: string[];
  categoryWeights: Record<string, number>;
  setCategoryWeights: Dispatch<SetStateAction<Record<string, number>>>;
}) {
  return (
    <div className="space-y-4">
      {['price', 'volume'].map((group) => {
        const groupSignals = signalDefs.filter((s) => (s.group ?? 'price') === group);
        if (groupSignals.length === 0) return null;
        return (
          <div key={group}>
            <h3 className="text-gray-400 text-xs font-medium mb-2.5 uppercase tracking-wider">
              {group === 'price' ? 'Price Momentum' : 'Volume Confirmation'}
            </h3>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-x-8 gap-y-2.5">
              {groupSignals.map((s) => (
                <div key={s.key} className="flex items-center gap-3">
                  <div className="w-36 shrink-0 flex items-center gap-1.5">
                    <span className="text-gray-300 text-xs font-medium">{s.label}</span>
                    <span className="relative group/tip">
                      <span className="text-gray-600 hover:text-gray-400 cursor-help text-xs">&#9432;</span>
                      <span className="absolute bottom-full left-1/2 -translate-x-1/2 mb-1.5 hidden group-hover/tip:block w-64 px-3 py-2 rounded-lg bg-gray-800 border border-gray-700 text-gray-300 text-xs leading-relaxed shadow-xl z-50 pointer-events-none">
                        {s.description}
                      </span>
                    </span>
                  </div>
                  <input
                    type="range"
                    min={0}
                    max={10}
                    step={1}
                    value={weights[s.key] ?? 0}
                    onChange={(e) => setWeights((prev) => ({ ...prev, [s.key]: Number(e.target.value) }))}
                    className="flex-1 h-1 accent-indigo-500 cursor-pointer"
                  />
                  <span className="text-gray-500 text-xs w-5 text-right font-mono shrink-0">{weights[s.key] ?? 0}</span>
                </div>
              ))}
            </div>
          </div>
        );
      })}
      {/* Category Weights */}
      {categories.length > 1 && (
        <div>
          <h3 className="text-gray-400 text-xs font-medium mb-2.5 uppercase tracking-wider">Category Weights</h3>
          <div className="flex items-center gap-6">
            {categories.map((cat) => (
              <div key={cat} className="flex items-center gap-2">
                <span className="text-gray-300 text-xs font-medium w-28">
                  {cat === 'price' ? 'Price Momentum' : cat === 'volume' ? 'Volume Confirmation' : cat}
                </span>
                <input
                  type="range"
                  min={0}
                  max={100}
                  step={5}
                  value={categoryWeights[cat] ?? 50}
                  onChange={(e) => setCategoryWeights((prev) => ({ ...prev, [cat]: Number(e.target.value) }))}
                  className="w-32 h-1 accent-indigo-500 cursor-pointer"
                />
                <span className="text-gray-500 text-xs w-8 text-right font-mono">{categoryWeights[cat] ?? 50}%</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
