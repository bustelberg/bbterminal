'use client';

import { useState } from 'react';
import LoadingDots from '../LoadingDots';
import { SCHEDULE_FREQUENCIES, type Etf, type ScheduleFrequency } from './useDiversifier';

type Weights = Record<string, { weight: number; band: number }>;

/** Backtest a HAND-SPECIFIED portfolio: set a target weight + rebalance band
 * for the strategy and each selected fund, then compare the drift-rebalanced
 * result against vanilla momentum. Sits below the optimizer; the
 * "Use optimizer weights" button prefills from the optimizer's result. */
export default function ManualPortfolioSection({
  funds, weights, defaults, onChange, onReset, onRun, running, canRun, onSave, saving,
  scheduleFreq, onScheduleFreqChange, onScheduleAsNew,
  availableFunds, onAddFund, onAddByTicker, adding,
}: {
  funds: Etf[];                         // the selected funds (strategy is implicit)
  weights: Weights;                     // user overrides
  defaults: Weights;                    // auto-computed defaults (strategy %, rounded sleeve)
  onChange: (key: string, field: 'weight' | 'band', val: number) => void;
  onReset: () => void;                  // clear overrides → revert to defaults
  onRun: () => void;
  running: boolean;
  canRun: boolean;
  onSave: (name: string) => void;       // save this portfolio under a name
  saving: boolean;
  scheduleFreq: ScheduleFrequency;
  onScheduleFreqChange: (f: ScheduleFrequency) => void;
  onScheduleAsNew: (name: string) => void;   // create a NEW standalone scheduled strategy (vanilla or blended)
  availableFunds: Etf[];                // benchmarks not yet in the mix (pick to add a row)
  onAddFund: (benchmarkId: number) => void;     // add an existing benchmark to the mix
  onAddByTicker: (ticker: string) => void;      // fetch a new benchmark by ticker + add it to the mix
  adding: boolean;
}) {
  const [saveName, setSaveName] = useState('');
  const [addTicker, setAddTicker] = useState('');
  const rows: { key: string; label: string; name?: string }[] = [
    { key: 'strategy', label: 'Strategy' },
    ...funds.map((f) => ({ key: String(f.benchmark_id), label: f.ticker, name: f.name })),
  ];
  // Effective value = user override ?? auto-default.
  const eff = (key: string, field: 'weight' | 'band') =>
    weights[key]?.[field] ?? defaults[key]?.[field] ?? (field === 'band' ? 10 : 0);
  const sum = rows.reduce((s, r) => s + eff(r.key, 'weight'), 0);
  const sumOk = Math.abs(sum - 100) < 0.05;
  const hasOverrides = Object.keys(weights).length > 0;

  return (
    <div className="bg-card rounded-xl border border-neutral-800/40 p-5">
      <div className="flex items-center justify-between mb-1 gap-3 flex-wrap">
        <h3 className="text-fg-muted text-xs font-medium uppercase tracking-wider">Portfolio backtest (manual weights)</h3>
        {hasOverrides && (
          <button onClick={onReset} className="text-xs font-medium text-accent-400 hover:text-accent-500 transition-colors">
            Reset to defaults
          </button>
        )}
      </div>
      <p className="text-xs text-fg-subtle mb-3">
        Fix each holding&apos;s target weight + rebalance band; the portfolio is reset to target whenever <em>any</em> holding
        drifts outside its band. Compared against vanilla momentum over the common window.
      </p>

      <div className="overflow-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="text-fg-subtle text-xs border-b border-neutral-800/60">
              <th className="text-left font-medium py-2 px-2">Holding</th>
              <th className="text-right font-medium py-2 px-2 w-28">Target %</th>
              <th className="text-right font-medium py-2 px-2 w-28">Band ±%</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr key={r.key} className="border-b border-neutral-800/40">
                <td className="py-2 px-2">
                  <span className={`font-mono ${r.key === 'strategy' ? 'text-accent-400 font-medium' : 'text-fg-strong'}`}>{r.label}</span>
                  {r.name && <span className="text-fg-subtle text-xs ml-2">{r.name}</span>}
                </td>
                <td className="py-1 px-2 text-right">
                  <input
                    type="number"
                    step="5"
                    min={0}
                    max={100}
                    value={eff(r.key, 'weight')}
                    onChange={(e) => onChange(r.key, 'weight', Number(e.target.value))}
                    className="w-20 bg-page border border-neutral-700 rounded px-2 py-1 text-fg-strong text-sm font-mono text-right focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none"
                  />
                </td>
                <td className="py-1 px-2 text-right">
                  <input
                    type="number"
                    step="5"
                    min={0}
                    max={50}
                    value={eff(r.key, 'band')}
                    onChange={(e) => onChange(r.key, 'band', Number(e.target.value))}
                    className="w-20 bg-page border border-neutral-700 rounded px-2 py-1 text-fg-strong text-sm font-mono text-right focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none"
                  />
                </td>
              </tr>
            ))}
          </tbody>
          <tfoot>
            <tr>
              <td className="py-2 px-2 text-xs text-fg-subtle">Total</td>
              <td className={`py-2 px-2 text-right font-mono text-sm ${sumOk ? 'text-pos-400' : 'text-warn-400'}`}>{sum.toFixed(0)}%</td>
              <td></td>
            </tr>
          </tfoot>
        </table>
      </div>

      {/* Add any ETF/bond to the mix — pick an existing benchmark or fetch a
          new one by ticker (it's added + dropped straight into the table). */}
      <div className="flex items-center gap-2 mt-3 flex-wrap">
        <span className="text-xs text-fg-subtle">Add to mix:</span>
        <select
          value=""
          onChange={(e) => { if (e.target.value) onAddFund(Number(e.target.value)); }}
          className="bg-page border border-neutral-700 rounded-lg px-3 py-2 text-fg-strong text-sm max-w-xs focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none"
        >
          <option value="">Select an ETF / bond…</option>
          {availableFunds.map((f) => (
            <option key={f.benchmark_id} value={f.benchmark_id}>{f.ticker} — {f.name}</option>
          ))}
        </select>
        <span className="text-xs text-fg-subtle">or</span>
        <input
          value={addTicker}
          onChange={(e) => setAddTicker(e.target.value)}
          placeholder="add by ticker (e.g. SHY)"
          className="bg-page border border-neutral-700 rounded-lg px-3 py-2 text-fg-strong text-sm w-44 focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none placeholder-fg-faint"
          onKeyDown={(e) => { if (e.key === 'Enter' && addTicker.trim()) { onAddByTicker(addTicker); setAddTicker(''); } }}
        />
        <button
          onClick={() => { if (addTicker.trim()) { onAddByTicker(addTicker); setAddTicker(''); } }}
          disabled={adding || !addTicker.trim()}
          className="px-3 py-2 rounded-lg text-sm font-medium border border-accent-500 text-accent-400 hover:bg-accent-600/10 transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
        >
          {adding ? <LoadingDots /> : 'Add'}
        </button>
      </div>

      <div className="flex items-center gap-3 mt-3 flex-wrap">
        <button
          onClick={onRun}
          disabled={!canRun}
          className="px-4 py-2 rounded-lg text-sm font-medium bg-accent-600 hover:bg-accent-500 text-fg-strong transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
        >
          {running ? <LoadingDots /> : 'Run backtest'}
        </button>
        <div className="flex items-center gap-2 ml-auto">
          <input
            value={saveName}
            onChange={(e) => setSaveName(e.target.value)}
            placeholder="Name this portfolio…"
            className="bg-page border border-neutral-700 rounded-lg px-3 py-2 text-fg-strong text-sm w-56 focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none placeholder-fg-faint"
            onKeyDown={(e) => { if (e.key === 'Enter' && saveName.trim()) { onSave(saveName); setSaveName(''); } }}
          />
          <button
            onClick={() => { onSave(saveName); setSaveName(''); }}
            disabled={saving || !saveName.trim()}
            className="px-4 py-2 rounded-lg text-sm font-medium border border-accent-500 text-accent-400 hover:bg-accent-600/10 transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
          >
            {saving ? <LoadingDots /> : 'Save portfolio'}
          </button>
        </div>
      </div>
      {!sumOk && <p className="text-xs text-warn-400 mt-2">Weights sum to {sum.toFixed(0)}% — they&apos;ll be normalized to 100%.</p>}

      {/* Schedule as a NEW standalone strategy — appears on /schedule and is
          rebalanced by the pipeline. Vanilla (no ETFs) or blended (with them). */}
      <div className="flex items-center gap-2 mt-3 pt-3 border-t border-neutral-800/40 flex-wrap">
        <span className="text-xs text-fg-subtle">Schedule as a new strategy, rebalanced</span>
        <select
          value={scheduleFreq}
          onChange={(e) => onScheduleFreqChange(e.target.value as ScheduleFrequency)}
          className="bg-page border border-neutral-700 rounded-lg px-3 py-2 text-fg-strong text-sm focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none"
        >
          {SCHEDULE_FREQUENCIES.map((f) => (
            <option key={f} value={f}>{f}</option>
          ))}
        </select>
        <button
          onClick={() => { if (saveName.trim()) { onScheduleAsNew(saveName); setSaveName(''); } }}
          disabled={saving || !saveName.trim()}
          title={
            funds.length > 0
              ? 'Creates a new scheduled strategy blending the momentum sleeve with the selected ETFs (uses the name field above)'
              : 'Creates a new vanilla momentum scheduled strategy (uses the name field above)'
          }
          className="px-4 py-2 rounded-lg text-sm font-medium bg-accent-600 hover:bg-accent-500 text-fg-strong transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
        >
          {saving ? <LoadingDots /> : funds.length > 0 ? 'Schedule as variant (+ ETFs)' : 'Schedule as variant'}
        </button>
        <span className="text-xs text-fg-subtle">(uses the name field above)</span>
      </div>
    </div>
  );
}
