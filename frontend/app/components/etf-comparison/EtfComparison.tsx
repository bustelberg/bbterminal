'use client';

import { useState } from 'react';
import ComparisonRow from './ComparisonRow';
import { ETF_STARTERS } from './starters';

const inputClass = 'rounded-lg border border-neutral-700 bg-page px-3 py-2 text-sm text-fg focus:border-accent-500 focus:outline-none focus:ring-1 focus:ring-accent-500/30';

export default function EtfComparison() {
  const [benchmarkInput, setBenchmarkInput] = useState('ACWI');
  const [benchmark, setBenchmark] = useState('ACWI');
  const [revision, setRevision] = useState(0);

  return <main className="mx-auto max-w-7xl space-y-5 px-6 py-8">
    <header className="flex flex-wrap items-end justify-between gap-4">
      <div>
        <h1 className="text-2xl font-semibold tracking-tight text-fg-strong">ETF comparison</h1>
        <p className="mt-2 text-sm text-fg-muted">Five portfolio ETFs, each compared with the benchmark from its own starting date.</p>
      </div>
      <form className="flex items-end gap-2" onSubmit={(event) => {
        event.preventDefault();
        setBenchmark(benchmarkInput.trim().toUpperCase());
        setRevision((value) => value + 1);
      }}>
        <label className="flex flex-col gap-1.5 text-xs font-medium text-fg-soft">Benchmark · Yahoo ticker
          <input required pattern="[A-Za-z0-9^.=_-]+" maxLength={40} value={benchmarkInput} onChange={(event) => setBenchmarkInput(event.target.value)} className={inputClass + ' w-32'} />
        </label>
        <button type="submit" className="rounded-lg bg-accent-600 px-4 py-2 text-sm font-semibold text-white hover:bg-accent-500">Update charts</button>
      </form>
    </header>
    <p className="text-xs leading-relaxed text-fg-muted">All returns are in EUR. Dividend-adjusted Yahoo closes are converted using historical FX rates for each observation before calculating returns. Each ETF and benchmark start at 0% on their first shared trading close on or after your date, through the latest shared completed close. Start dates default to 1 January, not purchase dates.</p>
    <div className="space-y-4">
      {ETF_STARTERS.map((etf) => <ComparisonRow key={etf.isin} etf={etf} benchmark={benchmark} revision={revision} />)}
    </div>
  </main>;
}
