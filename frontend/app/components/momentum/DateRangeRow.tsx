import type { Dispatch, SetStateAction } from 'react';

import { WEEKDAY_LABELS } from './utils';

/**
 * `DateRangeRow` — the Start / End / Max-Companies / Rebalance-day fields
 * at the top of `/backtest`'s config panel. (The universe picker that used
 * to live here moved into the Variants panel, so this row is purely date +
 * sizing + the rebalance-weekday knob.) Presentational; state is owned by
 * `useBacktestConfig`. Returns a Fragment so the fields stay direct
 * children of the parent's flex row and keep their `gap-5` spacing.
 */
export default function DateRangeRow({
  startDate,
  setStartDate,
  endDate,
  setEndDate,
  maxCompanies,
  setMaxCompanies,
  rebalanceWeekday,
  setRebalanceWeekday,
}: {
  startDate: string;
  setStartDate: Dispatch<SetStateAction<string>>;
  endDate: string;
  setEndDate: Dispatch<SetStateAction<string>>;
  maxCompanies: number;
  setMaxCompanies: Dispatch<SetStateAction<number>>;
  rebalanceWeekday: number;
  setRebalanceWeekday: Dispatch<SetStateAction<number>>;
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
        <label className="text-gray-500 text-xs block mb-1">Start</label>
        <input
          type="month"
          value={startDate}
          onChange={(e) => setStartDate(e.target.value)}
          min="1998-01"
          max={`${currentYear + 1}-12`}
          className="bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-white text-sm font-mono focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
        />
      </div>
      <div>
        <label className="text-gray-500 text-xs block mb-1">End</label>
        <input
          type="month"
          value={endDate}
          onChange={(e) => setEndDate(e.target.value)}
          min="1998-01"
          max={`${currentYear + 1}-12`}
          className="bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-white text-sm font-mono focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
        />
      </div>
      <div>
        <label className="text-gray-500 text-xs block mb-1">Max Companies</label>
        <input
          type="number"
          min={0}
          max={500}
          value={maxCompanies}
          onChange={(e) => setMaxCompanies(Number(e.target.value))}
          className="w-20 bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-white text-sm font-mono text-center focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
          title="0 = all companies, otherwise limit alphabetically"
        />
        <span className="text-gray-600 text-xs ml-1">0 = all</span>
      </div>
      <div>
        <label className="text-gray-500 text-xs block mb-1">Rebalance day</label>
        <select
          value={rebalanceWeekday}
          onChange={(e) => setRebalanceWeekday(Number(e.target.value))}
          className="bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-white text-sm focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
          title="Which weekday each rebalance period enters on. e.g. 'Wednesday' rebalances on the first Wednesday of the month, using signals through the prior (Tuesday) close."
        >
          {WEEKDAY_LABELS.map((label, i) => (
            <option key={i} value={i}>{`First ${label}`}</option>
          ))}
        </select>
      </div>
    </>
  );
}
