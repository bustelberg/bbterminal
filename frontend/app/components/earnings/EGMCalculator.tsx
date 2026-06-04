'use client';

import { useCallback, useEffect, useState } from 'react';
import InfoTip from '../InfoTip';
import { MC, type MetricRow } from './types';
import { earliestFutureValue, fmtPct, latestValue } from './utils';

/** Earnings Growth Multiple calculator. Reads current EPS + next-FY EPS
 * estimate from metrics, lets the user override either input, and
 * displays the implied growth rate vs the most recent historical YoY
 * EPS growth as a benchmark (green when EGM ≥ historic, red otherwise). */
export default function EGMCalculator({ metrics }: { metrics: MetricRow[] }) {
  const epsRaw = latestValue(metrics, MC.EPS_DILUTED);
  const fy1Raw = epsRaw
    ? earliestFutureValue(metrics, MC.EPS_FY1_EST, epsRaw.date)
    : latestValue(metrics, MC.EPS_FY1_EST);
  const yoyEpsGrowth = latestValue(metrics, MC.YOY_EPS_GROWTH);

  const [eps, setEps] = useState<string>('');
  const [fy1, setFy1] = useState<string>('');
  const [initialized, setInitialized] = useState(false);

  const resetDefaults = useCallback(() => {
    setEps(epsRaw ? epsRaw.value.toFixed(2) : '');
    setFy1(fy1Raw ? fy1Raw.value.toFixed(2) : '');
  }, [epsRaw, fy1Raw]);

  // Initialize defaults exactly once after raw data first arrives. The
  // setState-in-effect lint is correct that this is a discouraged
  // pattern, but the canonical alternative ("derive everything during
  // render") doesn't fit here — `resetDefaults` writes to several
  // form-state setters the user can then edit, so it must be a one-shot
  // side effect, not a derived value.
  useEffect(() => {
    if (!initialized && (epsRaw || fy1Raw)) {
      // eslint-disable-next-line react-hooks/set-state-in-effect
      resetDefaults();
      setInitialized(true);
    }
  }, [initialized, epsRaw, fy1Raw, resetDefaults]);

  const epsNum = parseFloat(eps);
  const fy1Num = parseFloat(fy1);
  const egm = !isNaN(epsNum) && !isNaN(fy1Num) && epsNum !== 0
    ? (fy1Num - epsNum) / epsNum
    : null;

  const inputClass = "w-28 bg-page border border-neutral-700 rounded-lg px-3 py-1.5 text-fg-strong font-mono text-sm focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none";

  return (
    <div className="flex items-center gap-6">
      <div>
        <div className="text-fg-subtle text-xs mb-1 flex items-center gap-1">
          Current EPS {epsRaw && <span className="text-fg-faint">({epsRaw.date})</span>}
          <InfoTip text="Diluted EPS (excluding non-recurring items) for the most recent fiscal year. Used as the base for calculating expected growth." />
        </div>
        <input type="number" step="0.01" value={eps} onChange={(e) => setEps(e.target.value)} className={inputClass} />
      </div>
      <div>
        <div className="text-fg-subtle text-xs mb-1 flex items-center gap-1">
          FY1 EPS Est {fy1Raw && <span className="text-fg-faint">({fy1Raw.date})</span>}
          <InfoTip text="Analyst consensus EPS estimate for the next fiscal year. The growth from Current EPS to this value determines the EGM." />
        </div>
        <input type="number" step="0.01" value={fy1} onChange={(e) => setFy1(e.target.value)} className={inputClass} />
      </div>
      <div>
        <div className="text-fg-subtle text-xs mb-1 flex items-center gap-1">
          EGM
          <InfoTip text="Earnings Growth Multiple = (FY1 EPS Est - Current EPS) / Current EPS. Shows expected year-over-year earnings growth. Green if above historic YoY EPS, red if below." />
        </div>
        <div className={`font-mono text-2xl font-semibold ${egm != null && yoyEpsGrowth ? (egm >= yoyEpsGrowth.value / 100 ? 'text-pos-400' : 'text-neg-400') : 'text-fg-strong'}`}>
          {egm != null ? fmtPct(egm) : '—'}
        </div>
      </div>
      <div>
        <div className="text-fg-subtle text-xs mb-1 flex items-center gap-1">
          Historic YoY EPS
          <InfoTip text={`Year-over-year EPS growth for the most recent fiscal year (${yoyEpsGrowth?.date ?? '—'}). Single year, not a multi-year average. Used as a benchmark for the EGM.`} />
        </div>
        <div className="font-mono text-2xl font-semibold text-fg-muted">
          {yoyEpsGrowth ? fmtPct(yoyEpsGrowth.value / 100) : '—'}
        </div>
      </div>
      <div>
        <div className="text-fg-subtle text-xs mb-1">&nbsp;</div>
        <button onClick={resetDefaults} className="px-3 py-1.5 rounded-lg text-sm text-fg-muted hover:text-fg-strong hover:bg-overlay/5 border border-neutral-700 transition-colors">
          Reset
        </button>
      </div>
    </div>
  );
}
