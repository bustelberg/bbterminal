'use client';

import { Fragment } from 'react';
import { Provenance } from '../../../lib/provenance';

/** One trailing-window slice of returns+risk (`GET …/asset-pipeline/risk/isin/{isin}`, the basket
 *  endpoint, and the portfolio risk-windows endpoint all return this shape). */
export type PerfWindow = {
  years: number; available: boolean;
  from_date?: string | null; to_date?: string | null; trading_days?: number;
  cagr_pct?: number | null; r2?: number | null; ann_vol_pct?: number | null;
  sharpe?: number | null; sortino?: number | null;
  max_drawdown_pct?: number | null; up_days_pct?: number | null; pos_12m_pct?: number | null;
  coverage_pct?: number | null;
};

const spct = (v: number) => `${v >= 0 ? '+' : ''}${v.toFixed(1)}%`;
const upct = (v: number) => `${v.toFixed(1)}%`;
const num2 = (v: number) => v.toFixed(2);
const num3 = (v: number) => v.toFixed(3);

const signTone = (v: number | null) => (v == null ? '' : v >= 0 ? 'text-pos-400' : 'text-neg-400');
const negTone = () => 'text-neg-400';
const ratioTone = (v: number | null) =>
  v == null ? '' : v >= 1 ? 'text-pos-400' : v >= 0.5 ? 'text-warn-300' : 'text-fg';
// Steadiness of the CAGR: >0.95 steady · 0.8–0.95 lumpy · <0.8 erratic (same scale as Fundamental).
const r2Tone = (v: number | null) =>
  v == null ? '' : v >= 0.95 ? 'text-pos-400' : v >= 0.8 ? 'text-warn-300' : 'text-neg-400';
// Rolling 1-year win rate: ≥80% reliably up · 60–80% mixed · <60% coin-flip-ish.
const winTone = (v: number | null) =>
  v == null ? '' : v >= 80 ? 'text-pos-400' : v >= 60 ? 'text-warn-300' : 'text-neg-400';

type Metric = {
  label: string;
  /** WHAT the reader is looking at, in one plain sentence — answered before where it came
   *  from, because Source/When/How are all questions ABOUT a number already identified. */
  what: string;
  note: string;
  how: string;
  get: (w: PerfWindow) => number | null | undefined;
  fmt: (v: number) => string;
  tone: (v: number | null) => string;
};

const METRICS: Metric[] = [
  { label: 'Price increase (ann.)', what: 'How fast the price grew, per year on average.', note: 'geometric annualized price gain',
    how: 'From the daily EUR close: (end ÷ start)^(1/years) − 1. Annualized so the windows compare directly. Price only — dividends excluded.',
    get: (w) => w.cagr_pct, fmt: spct, tone: signTone },
  { label: 'Price R²', what: 'How steadily it grew — a straight line up, or a jagged one.', note: 'log-price trend fit',
    how: 'R² of log(price) regressed on time — 1 = a straight line on a log axis (steady compounding). Fit on every daily close in the window, not a few points. ⚠ Strict: a crash-then-recovery (a deep V) scores near 0 even though it ended up, and R² also RISES with window length by construction — so read the "Positive 1-yr holds" row as the more forgiving consistency measure.',
    get: (w) => w.r2, fmt: num3, tone: r2Tone },
  { label: 'Positive 1-yr holds', what: 'How often simply holding for a year would have paid.', note: 'rolling 12-month win rate',
    how: 'Of the window’s trading days, the share from which a 1-year hold (this price ÷ the price ~12 months earlier) ended positive. Uses every daily close, and — unlike R² — a dip that recovered still scores high, so it answers "how often did simply holding for a year actually pay?"',
    get: (w) => w.pos_12m_pct, fmt: upct, tone: winTone },
  { label: 'Volatility (ann.)', what: 'How bumpy the ride was, per year.', note: 'annualized σ of daily returns',
    how: 'Standard deviation of daily EUR returns × √252 — how bumpy the ride was.',
    get: (w) => w.ann_vol_pct, fmt: upct, tone: () => 'text-fg' },
  { label: 'Sharpe', what: 'How much return was earned per unit of the risk taken.', note: 'return per unit of total risk',
    how: 'Annualized return ÷ annualized volatility (risk-free = 0). ≥1 is good.',
    get: (w) => w.sharpe, fmt: num2, tone: ratioTone },
  { label: 'Sortino', what: 'The same, counting only the falls — upside swings are not risk.', note: 'return per unit of downside risk',
    how: 'Annualized return ÷ downside deviation (only sub-zero daily returns), risk-free = 0.',
    get: (w) => w.sortino, fmt: num2, tone: ratioTone },
  { label: 'Max drawdown', what: 'The worst fall from a high point to the low that followed.', note: 'worst peak-to-trough',
    how: 'Largest fall from a running peak in the EUR price over the window. ⚠ Tends to deepen with a longer window — more chances for a big fall.',
    get: (w) => w.max_drawdown_pct, fmt: spct, tone: negTone },
  { label: 'Up days', what: 'How often the price finished a day higher than it started.', note: 'share of positive days',
    how: 'Fraction of trading days with a positive EUR return.',
    get: (w) => w.up_days_pct, fmt: upct, tone: () => 'text-fg' },
];

/**
 * The 2/4/8-year returns+risk table — metrics down the rows, trailing windows across the columns,
 * each metric carrying a WHERE/WHEN/HOW ⓘ. Shared by the single-instrument / basket Performance
 * modal AND the Analyse modal's Risk section, so all three read one definition of every metric.
 *
 * `coverage_pct` (present for a basket / a whole portfolio) is footnoted — a metric computed over a
 * fraction of the sleeve must say so.
 */
export default function PerformanceTable({ windows, asOf, benchWindows, benchLabel, subjectLabel = 'This' }: {
  windows: PerfWindow[]; asOf: string | null;
  // Optional benchmark values, shown as a second (grey) column within each window.
  benchWindows?: PerfWindow[]; benchLabel?: string; subjectLabel?: string;
}) {
  const coverages = windows.filter((w) => w.available && w.coverage_pct != null);
  const partialCoverage = coverages.some((w) => (w.coverage_pct ?? 100) < 99.5);
  const benchByYear = new Map((benchWindows ?? []).map((w) => [w.years, w]));
  const hasBench = !!benchWindows && benchWindows.length > 0;
  return (
    <div className="space-y-2">
      <div className="overflow-auto rounded-lg border border-neutral-800/40">
        <table className="w-full text-xs">
          <thead className="bg-card">
            <tr className="text-fg-faint text-[10px] uppercase tracking-wide border-b border-neutral-800/40">
              <th rowSpan={hasBench ? 2 : 1} className="px-3 py-2 text-left font-medium align-bottom">Metric</th>
              {windows.map((w) => (
                <th key={w.years} colSpan={hasBench ? 2 : 1}
                  className="px-3 py-2 text-right font-medium border-l border-neutral-800/30">
                  {w.years}Y
                  {w.available && w.from_date && (
                    <div className="text-[9px] font-normal normal-case text-fg-faint">
                      {w.from_date.slice(0, 7)} → {w.to_date?.slice(0, 7)}
                    </div>
                  )}
                  {!w.available && <div className="text-[9px] font-normal normal-case text-fg-faint">n/a</div>}
                </th>
              ))}
            </tr>
            {hasBench && (
              <tr className="text-fg-faint text-[9px] uppercase tracking-wide border-b border-neutral-800/40">
                {windows.map((w) => (
                  <Fragment key={w.years}>
                    <th className="px-3 py-1 text-right font-medium border-l border-neutral-800/30 max-w-[7rem] truncate">{subjectLabel}</th>
                    <th className="px-3 py-1 text-right font-medium text-fg-faint">{benchLabel}</th>
                  </Fragment>
                ))}
              </tr>
            )}
          </thead>
          <tbody className="divide-y divide-neutral-800/20">
            {METRICS.map((m) => (
              <tr key={m.label} className="hover:bg-overlay/[0.02]">
                <td className="px-3 py-1.5 text-fg-soft whitespace-nowrap">
                  {m.label}
                  <Provenance source="yfinance" asOf={asOf} what={m.what} note={m.note} how={m.how} />
                </td>
                {windows.map((w) => {
                  const val = m.get(w) ?? null;
                  const bw = benchByYear.get(w.years);
                  const bval = bw ? (m.get(bw) ?? null) : null;
                  return hasBench ? (
                    <Fragment key={w.years}>
                      <td className={`px-3 py-1.5 text-right font-mono border-l border-neutral-800/30 ${val == null ? 'text-fg-faint' : m.tone(val)}`}>
                        {val == null ? '—' : m.fmt(val)}
                      </td>
                      <td className="px-3 py-1.5 text-right font-mono text-fg-faint">
                        {bval == null ? '—' : m.fmt(bval)}
                      </td>
                    </Fragment>
                  ) : (
                    <td key={w.years} className={`px-3 py-1.5 text-right font-mono ${val == null ? 'text-fg-faint' : m.tone(val)}`}>
                      {val == null ? '—' : m.fmt(val)}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {hasBench && (
        <p className="text-[10px] text-fg-faint">
          Each window shows two columns: the subject and, in grey,{' '}
          <strong>{benchLabel ?? 'the benchmark'}</strong> (its investable ETF), on the same daily-EUR basis.
        </p>
      )}
      {coverages.length > 0 && (
        <p className={`text-[10px] ${partialCoverage ? 'text-warn-300' : 'text-fg-faint'}`}>
          Coverage: {coverages.map((w) => `${w.years}Y ${w.coverage_pct}%`).join(' · ')}
          {partialCoverage && ' — the rest was unpriceable or not yet listed at the window start, and is renormalized out.'}
        </p>
      )}
      <p className="text-[10px] text-fg-faint">
        2/4/8-year trailing windows on daily EUR returns, price only (dividends excluded). Risk-free
        0; annualized with 252 trading days. Hover any metric&apos;s ⓘ for its source and formula. R²
        rises with window length by construction, so read its 2Y↔8Y gap with that in mind (see its
        ⓘ). A window reads n/a when the series does not reach back that far.
      </p>
    </div>
  );
}
