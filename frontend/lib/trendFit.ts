/**
 * Log-linear (exponential) trend fit — the "how steady is the growth?" measure behind the
 * LongEquity R². Revenue that compounds at a constant rate is a straight line in log space, so we
 * regress ln(value) on the year: the slope is the growth rate, and R² is how tightly the points
 * hug that line (1.0 = perfectly steady compounding, low = lumpy/cyclical).
 *
 * ⚠ FIT IN LOG SPACE, NOT ON THE RAW VALUES. A linear fit to revenue would be dominated by the
 * largest (latest) years and would call any exponential "not linear" — the opposite of the point.
 * Non-positive values have no log and are dropped (a loss year can't sit on an exponential trend);
 * the caller is told how many.
 */
export type TrendFit = {
  /** The fitted trend value at each used year — for overlaying on the bars. */
  trend: { year: number; value: number }[];
  /** Compound annual growth rate implied by the slope, as a fraction (0.12 = +12%/yr). */
  cagr: number | null;
  /** R² of the log-linear fit (0..1). Null when fewer than 2 usable points. */
  r2: number | null;
  /** Points actually used (positive, finite). */
  n: number;
  /** Non-positive / non-finite points dropped (no log). */
  dropped: number;
  /** The fitted line in LOG space: ln(value) = intercept + slope · year. Null when unfittable.
   *  Exposed so a caller can evaluate the trend at a year the data does not cover — `trend` only
   *  spans the observed ones. */
  slope: number | null;
  intercept: number | null;
};

/**
 * The trend's value at any year, including years beyond the data.
 *
 * ⚠ EXTRAPOLATION IS NOT A FORECAST AND MUST NOT BE DRAWN AS ONE. This continues the fitted
 * exponential; whether the business does is a different question entirely, and the caller is
 * responsible for making the projected stretch look different from the fitted one.
 */
export function trendValueAt(fit: TrendFit, year: number): number | null {
  if (fit.slope == null || fit.intercept == null) return null;
  const v = Math.exp(fit.intercept + fit.slope * year);
  return Number.isFinite(v) ? v : null;
}

export function logLinearFit(points: { year: number; value: number }[]): TrendFit {
  const used = points.filter((p) => Number.isFinite(p.value) && p.value > 0);
  const dropped = points.length - used.length;
  const n = used.length;
  if (n < 2) return { trend: [], cagr: null, r2: null, n, dropped, slope: null, intercept: null };

  const xs = used.map((p) => p.year);
  const ys = used.map((p) => Math.log(p.value));
  const mean = (a: number[]) => a.reduce((s, v) => s + v, 0) / a.length;
  const mx = mean(xs);
  const my = mean(ys);

  let sxx = 0;
  let sxy = 0;
  for (let i = 0; i < n; i++) {
    sxx += (xs[i] - mx) ** 2;
    sxy += (xs[i] - mx) * (ys[i] - my);
  }
  // All points in the same year (or one distinct x) — a slope is undefined.
  if (sxx === 0) return { trend: [], cagr: null, r2: null, n, dropped, slope: null, intercept: null };

  const slope = sxy / sxx;          // growth rate in log space
  const intercept = my - slope * mx;

  let ssRes = 0;
  let ssTot = 0;
  for (let i = 0; i < n; i++) {
    const yhat = intercept + slope * xs[i];
    ssRes += (ys[i] - yhat) ** 2;
    ssTot += (ys[i] - my) ** 2;
  }
  // A perfectly flat series has zero total variance — it IS its own trend, so R² = 1.
  const r2 = ssTot === 0 ? 1 : Math.max(0, 1 - ssRes / ssTot);
  const cagr = Math.exp(slope) - 1;
  const trend = used.map((p) => ({ year: p.year, value: Math.exp(intercept + slope * p.year) }));

  return { trend, cagr, r2, n, dropped, slope, intercept };
}
