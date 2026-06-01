import type { Dispatch, SetStateAction } from 'react';

/**
 * `RandomParamsInputs` — the Trials + Base-seed inputs shown when the
 * random-baseline selection mode is active. Presentational; the parent
 * guards on `selectionMode === 'random'` before rendering this.
 */
export default function RandomParamsInputs({
  nTrials,
  setNTrials,
  randomSeed,
  setRandomSeed,
}: {
  nTrials: number;
  setNTrials: Dispatch<SetStateAction<number>>;
  randomSeed: number;
  setRandomSeed: Dispatch<SetStateAction<number>>;
}) {
  return (
    <div className="flex flex-wrap items-end gap-6">
      <div>
        <label className="text-gray-500 text-xs block mb-1">Trials (parallel seeds)</label>
        <input
          type="number"
          min={1}
          max={100}
          value={nTrials}
          onChange={(e) => setNTrials(Number(e.target.value))}
          className="w-24 bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-white text-sm font-mono text-center focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
          title="Independent random-selection runs. Summary headline becomes mean ± std across trials."
        />
        <div className="text-[10px] text-gray-600 mt-1 max-w-[260px]">
          More trials → tighter confidence on the noise-floor return. 5–25 is a sensible range.
        </div>
      </div>
      <div>
        <label className="text-gray-500 text-xs block mb-1">Base seed</label>
        <input
          type="number"
          value={randomSeed}
          onChange={(e) => setRandomSeed(Number(e.target.value))}
          className="w-24 bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-white text-sm font-mono text-center focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
          title="Same base seed reproduces the same set of random picks. Trials use seed, seed+1, ..., seed+N-1."
        />
        <div className="text-[10px] text-gray-600 mt-1 max-w-[260px]">
          Reproducibility anchor; trials use seed, seed+1, …, seed+N−1.
        </div>
      </div>
    </div>
  );
}
