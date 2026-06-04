import type { Dispatch, SetStateAction } from 'react';

/**
 * `DateRangeRow` — the Start / End / Max-Companies fields at the top of
 * `/backtest`'s config panel. (The universe picker moved into the Variants
 * panel; the rebalance-day knob moved there too — it's now a per-variant
 * sweep axis, so the base config no longer carries it.) Presentational;
 * state is owned by `useBacktestConfig`. Returns a Fragment so the fields
 * stay direct children of the parent's flex row and keep their `gap-5`
 * spacing.
 */
export default function DateRangeRow({
  startDate,
  setStartDate,
  endDate,
  setEndDate,
  maxCompanies,
  setMaxCompanies,
}: {
  startDate: string;
  setStartDate: Dispatch<SetStateAction<string>>;
  endDate: string;
  setEndDate: Dispatch<SetStateAction<string>>;
  maxCompanies: number;
  setMaxCompanies: Dispatch<SetStateAction<number>>;
}) {
  // min/max bound the typeable year to 4 digits — browsers otherwise
  // accept "202604" / "999999" in a <input type="month">, which breaks
  // the backend's YYYY-MM-01 parse. 1998 matches the price-data cutoff in
  // backend/ingest/prices.py (_PRICE_CUTOFF); the upper bound is one year
  // past today so the picker still allows scheduling forward.
  const currentYear = new Date().getFullYear();
  return (
    <>
      <div>
        <label className="text-fg-subtle text-xs block mb-1">Start</label>
        <input
          type="month"
          value={startDate}
          onChange={(e) => setStartDate(e.target.value)}
          min="1998-01"
          max={`${currentYear + 1}-12`}
          className="bg-page border border-neutral-700 rounded-lg px-3 py-2 text-fg-strong text-sm font-mono focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none"
        />
      </div>
      <div>
        <label className="text-fg-subtle text-xs block mb-1">End</label>
        <input
          type="month"
          value={endDate}
          onChange={(e) => setEndDate(e.target.value)}
          min="1998-01"
          max={`${currentYear + 1}-12`}
          className="bg-page border border-neutral-700 rounded-lg px-3 py-2 text-fg-strong text-sm font-mono focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none"
        />
      </div>
      <div>
        <label className="text-fg-subtle text-xs block mb-1">Max Companies</label>
        <input
          type="number"
          min={0}
          max={500}
          value={maxCompanies}
          onChange={(e) => setMaxCompanies(Number(e.target.value))}
          className="w-20 bg-page border border-neutral-700 rounded-lg px-3 py-2 text-fg-strong text-sm font-mono text-center focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none"
          title="0 = all companies, otherwise limit alphabetically"
        />
        <span className="text-fg-faint text-xs ml-1">0 = all</span>
      </div>
    </>
  );
}
