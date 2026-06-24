import type { Dispatch, SetStateAction } from 'react';

import type { SignalDef } from './types';

/** Friendly heading per signal pillar (group / category). */
const GROUP_LABELS: Record<string, string> = {
  price: 'Price Momentum',
  volume: 'Volume Confirmation',
  trend: 'Trend Quality',
};
const groupLabel = (g: string) => GROUP_LABELS[g] ?? g;

/**
 * `SignalWeightSliders` — the momentum-mode signal-weight + category-weight
 * sliders from `/backtest`'s Strategy parameters section. Purely
 * presentational: all state is owned by `useBacktestConfig` and threaded
 * in. The parent renders this for `momentum` / `momentum_extra` and passes
 * the ACTIVE categories (price+volume, or +trend for MomentumExtra) as
 * `categories` — so the trend pillar's sliders appear only for MomentumExtra.
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
  // Category sliders are RELATIVE: the backend normalizes them by their sum,
  // so the displayed % is each pillar's normalized share, not the raw slider
  // value. With 3 equal sliders that's 33% each (not 50%).
  const catTotal = categories.reduce((s, c) => s + (categoryWeights[c] ?? 50), 0);
  return (
    <div className="space-y-4">
      {categories.map((group) => {
        const groupSignals = signalDefs.filter((s) => (s.group ?? 'price') === group);
        if (groupSignals.length === 0) return null;
        return (
          <div key={group}>
            <h3 className="text-fg-muted text-xs font-medium mb-2.5 uppercase tracking-wider">
              {groupLabel(group)}
            </h3>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-x-8 gap-y-2.5">
              {groupSignals.map((s) => (
                <div key={s.key} className="flex items-center gap-3">
                  <div className="w-36 shrink-0 flex items-center gap-1.5">
                    <span className="text-fg-soft text-xs font-medium">{s.label}</span>
                    <span className="relative group/tip">
                      <span className="text-fg-faint hover:text-fg-muted cursor-help text-xs">&#9432;</span>
                      <span className="absolute bottom-full left-1/2 -translate-x-1/2 mb-1.5 hidden group-hover/tip:block w-64 px-3 py-2 rounded-lg bg-neutral-800 border border-neutral-700 text-fg-soft text-xs leading-relaxed shadow-xl z-50 pointer-events-none">
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
                    className="flex-1 h-1 accent-accent-500 cursor-pointer"
                  />
                  <span className="text-fg-subtle text-xs w-5 text-right font-mono shrink-0">{weights[s.key] ?? 0}</span>
                </div>
              ))}
            </div>
          </div>
        );
      })}
      {/* Category Weights */}
      {categories.length > 1 && (
        <div>
          <h3 className="text-fg-muted text-xs font-medium mb-2.5 uppercase tracking-wider">
            Category Weights{' '}
            <span className="text-fg-faint normal-case font-normal tracking-normal">(relative — shown as normalized share)</span>
          </h3>
          <div className="flex items-center gap-6">
            {categories.map((cat) => {
              const raw = categoryWeights[cat] ?? 50;
              const share = catTotal > 0 ? Math.round((raw / catTotal) * 100) : 0;
              return (
                <div key={cat} className="flex items-center gap-2">
                  <span className="text-fg-soft text-xs font-medium w-28">
                    {groupLabel(cat)}
                  </span>
                  <input
                    type="range"
                    min={0}
                    max={100}
                    step={5}
                    value={raw}
                    onChange={(e) => setCategoryWeights((prev) => ({ ...prev, [cat]: Number(e.target.value) }))}
                    className="w-32 h-1 accent-accent-500 cursor-pointer"
                  />
                  <span className="text-fg-subtle text-xs w-8 text-right font-mono">{share}%</span>
                </div>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}
