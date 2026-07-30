// Pure per-period performance stats over the AlphaLab equal-weight index level
// series (aligned `dates` + cumulative `level`). Buckets by calendar year /
// 5-year / 10-year blocks. Per bucket: Return = geometric CAGR (annualized);
// Vol / Sharpe / Sortino are annualized (252 trading days, rf = 0). The ratios
// use the annualized ARITHMETIC-mean daily return as their numerator (textbook
// Sharpe/Sortino), while the Return column is the geometric CAGR — they answer
// different questions, so a bucket can show a positive Sharpe with a flat CAGR
// and vice-versa in choppy years.

export type Grouping = 'year' | '5y' | '10y';

export type PerfRow = {
  label: string;           // "2019" or "2015–2019"
  n: number;               // daily observations in the bucket
  ret: number;             // CAGR (annualized), fraction (0.12 = 12%)
  vol: number;             // annualized volatility, fraction
  sharpe: number | null;   // null when < MIN_OBS or zero vol
  sortino: number | null;  // null when < MIN_OBS or zero downside dev
};

const TD = 252;      // trading days per year
const MIN_OBS = 20;  // below this a bucket's ratios are too noisy to report

function mean(xs: number[]): number {
  return xs.reduce((a, b) => a + b, 0) / xs.length;
}

function sampleStd(xs: number[], mu: number): number {
  if (xs.length < 2) return 0;
  const v = xs.reduce((a, b) => a + (b - mu) ** 2, 0) / (xs.length - 1);
  return Math.sqrt(v);
}

function bucketKey(dateISO: string, g: Grouping): { key: number; label: string } {
  const y = parseInt(dateISO.slice(0, 4), 10);
  if (g === 'year') return { key: y, label: String(y) };
  const span = g === '5y' ? 5 : 10;
  const lo = Math.floor(y / span) * span;
  return { key: lo, label: `${lo}–${lo + span - 1}` };
}

function statsFor(rets: number[], label: string): PerfRow {
  const n = rets.length;
  const total = rets.reduce((acc, r) => acc * (1 + r), 1) - 1;
  const years = n / TD;
  const ret = years > 0 ? (1 + total) ** (1 / years) - 1 : 0;
  const mu = n ? mean(rets) : 0;
  const vol = sampleStd(rets, mu) * Math.sqrt(TD);
  const annArith = mu * TD;
  // Downside deviation vs a 0 target (only negative days contribute).
  const downside = n ? Math.sqrt(mean(rets.map((r) => (r < 0 ? r * r : 0)))) : 0;
  const annDown = downside * Math.sqrt(TD);
  const enough = n >= MIN_OBS;
  return {
    label,
    n,
    ret,
    vol,
    sharpe: enough && vol > 0 ? annArith / vol : null,
    sortino: enough && annDown > 0 ? annArith / annDown : null,
  };
}

/** Single stats row over the WHOLE series (label "Full period") — for a compact
 * summary/teaser. Null when there aren't ≥ 2 usable points. */
export function overallPerf(dates: string[], level: number[]): PerfRow | null {
  if (dates.length < 2) return null;
  const rets: number[] = [];
  for (let i = 1; i < dates.length; i++) {
    const prev = level[i - 1];
    if (!(prev > 0)) continue;
    const r = level[i] / prev - 1;
    if (Number.isFinite(r)) rets.push(r);
  }
  return rets.length ? statsFor(rets, 'Full period') : null;
}

/** Per-period rows from an index-level series. Daily simple returns are bucketed
 * by the calendar year of the day they're realized on, so each block's stats use
 * only the days that belong to it. Buckets are returned oldest-first. */
export function perfByPeriod(dates: string[], level: number[], g: Grouping): PerfRow[] {
  if (dates.length < 2) return [];
  const buckets = new Map<number, { label: string; rets: number[] }>();
  for (let i = 1; i < dates.length; i++) {
    const prev = level[i - 1];
    if (!(prev > 0)) continue;
    const r = level[i] / prev - 1;
    if (!Number.isFinite(r)) continue;
    const { key, label } = bucketKey(dates[i], g);
    let b = buckets.get(key);
    if (!b) { b = { label, rets: [] }; buckets.set(key, b); }
    b.rets.push(r);
  }
  return [...buckets.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([, b]) => statsFor(b.rets, b.label));
}
