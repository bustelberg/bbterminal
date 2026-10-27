'use client';

import { useState } from 'react';
import ComparisonRow from './ComparisonRow';
import { ETF_STARTERS } from './starters';

const inputClass = 'rounded-lg border border-neutral-700 bg-page px-3 py-2 text-sm text-fg focus:border-accent-500 focus:outline-none focus:ring-1 focus:ring-accent-500/30';

export default function EtfComparison() {
  const [benchmarkInput, setBenchmarkInput] = useState('ACWI');
  const [benchmark, setBenchmark] = useState('ACWI');
  const [revision, setRevision] = useState(0);
  const [tickerInput, setTickerInput] = useState('');
  const [etfs, setEtfs] = useState<Array<{ name: string; ticker: string; isin: string }>>([...ETF_STARTERS]);
  const [addError, setAddError] = useState('');

  return <main className="mx-auto max-w-7xl space-y-5 px-6 py-8">
    <header className="flex flex-wrap items-end justify-between gap-4">
      <div>
        <h1 className="text-2xl font-semibold tracking-tight text-fg-strong">ETF comparison</h1>
        <p className="mt-2 text-sm text-fg-muted">Add Yahoo Finance ETFs and compare them with another ETF from a date of your choice.</p>
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
    <form className="flex flex-wrap items-end gap-3 rounded-xl border border-neutral-800/40 bg-card p-4" onSubmit={(event) => {
      event.preventDefault();
      const ticker = tickerInput.trim().toUpperCase();
      if (etfs.some((etf) => etf.ticker === ticker)) {
        setAddError(`${ticker} is already in your comparisons.`);
        return;
      }
      setEtfs((rows) => [...rows, { ticker, name: ticker, isin: '' }]);
      setTickerInput('');
      setAddError('');
    }}>
      <label className="flex flex-col gap-1.5 text-xs font-medium text-fg-soft">ETF · Yahoo ticker
        <input required pattern="[A-Za-z0-9^.=_-]+" maxLength={40} placeholder="e.g. VWCE.DE" value={tickerInput} onChange={(event) => setTickerInput(event.target.value)} className={inputClass} />
      </label>
      <button type="submit" className="rounded-lg bg-accent-600 px-4 py-2 text-sm font-semibold text-white hover:bg-accent-500">Add ETF</button>
      <p className="basis-full text-xs text-fg-muted">Use the exact Yahoo Finance ticker, including its exchange suffix. Set each comparison’s starting date below.</p>
      {addError && <p role="alert" className="text-sm text-neg-400">{addError}</p>}
    </form>
    <p className="text-xs leading-relaxed text-fg-muted">All returns are in EUR. Dividend-adjusted Yahoo closes are converted using historical FX rates for each observation before calculating returns. Each ETF and benchmark start at 0% on their first shared trading close on or after your date, through the latest shared completed close. Start dates default to 1 January, not purchase dates.</p>
    <div className="space-y-4">
      {etfs.map((etf) => <ComparisonRow key={etf.ticker} etf={etf} benchmark={benchmark} revision={revision} onRemove={() => setEtfs((rows) => rows.filter((row) => row.ticker !== etf.ticker))} />)}
    </div>
  </main>;
}
