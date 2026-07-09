import type { ChartBand } from '../LwLineChart';

// Regime timeline of the equal-weight universe index.
export type RegimeData = {
  dates: string[]; index: number[]; ma200: number[];
  bull: boolean[]; turb: boolean[];
  current?: { bull: boolean; turb: boolean; date: string };
  universe?: { name?: string; size?: number };
};

// bull? + turbulent? → the four regimes: hue encodes bull/bear (green/red),
// depth of tint encodes calm/turbulent (light/dark).
export type RegKey = 'bc' | 'bt' | 'rc' | 'rt';
export const REG: Record<RegKey, { label: string; fill: string; dot: string; desc: string }> = {
  bc: { label: 'Bull · Calm', fill: 'rgba(34,197,94,0.13)', dot: '#22c55e',       // light green
    desc: 'Index ≥ its 200-day average, and 63-day volatility ≤ its 2-year median.' },
  bt: { label: 'Bull · Turbulent', fill: 'rgba(21,128,61,0.30)', dot: '#15803d',  // dark green
    desc: 'Index ≥ its 200-day average, but 63-day volatility > its 2-year median.' },
  rc: { label: 'Bear · Calm', fill: 'rgba(239,68,68,0.13)', dot: '#ef4444',       // light red
    desc: 'Index < its 200-day average, but 63-day volatility ≤ its 2-year median.' },
  rt: { label: 'Bear · Turbulent', fill: 'rgba(153,27,27,0.32)', dot: '#b91c1c',  // dark red
    desc: 'Index < its 200-day average, and 63-day volatility > its 2-year median.' },
};
export const regKey = (b: boolean, t: boolean): RegKey => (b ? (t ? 'bt' : 'bc') : (t ? 'rt' : 'rc'));

// Methodology blurb for the legend's info tooltip (kept next to the colours so
// it stays in sync with the backend `_score_regime` definition).
export const REGIME_METHOD =
  'How regimes are classified (computed causally — each day uses only prior data):\n\n' +
  '• Index: the universe’s equal-weight price index — daily returns clipped ±50% (so one blow-up can’t dominate), averaged across the names trading that day, then compounded.\n\n' +
  '• Bull vs Bear: index at/above vs below its trailing 200-day average.\n\n' +
  '• Calm vs Turbulent: the index’s 63-day realized volatility below vs above the median of its own volatility over the trailing ~2 years (504 days).\n\n' +
  'Colour: hue = bull/bear (green/red), tint depth = calm/turbulent (light/dark).';

/** Collapse the per-day bull/turb flags into contiguous coloured spans for the
 * chart overlay. Each band runs from the first day of a regime run to the first
 * day of the next (so adjacent bands are seamless). */
export function buildRegimeBands(dates: string[], bull: boolean[], turb: boolean[]): ChartBand[] {
  const n = dates.length;
  if (!n || bull.length !== n || turb.length !== n) return [];
  const bands: ChartBand[] = [];
  let s = 0;
  for (let i = 1; i <= n; i++) {
    const prev = regKey(bull[s], turb[s]);
    if (i === n || regKey(bull[i], turb[i]) !== prev) {
      const to = i >= n ? n - 1 : i;  // extend to the next run's first bar → seamless
      bands.push({
        from: dates[s], to: dates[to], fromIndex: s, toIndex: to,
        fill: REG[prev].fill, label: REG[prev].label,
      });
      s = i;
    }
  }
  return bands;
}
