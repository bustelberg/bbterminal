/** Layered fee model for a backtest — the "fee waterfall".
 *
 * Turns a strategy's GROSS return into the two net layers the user cares
 * about, plus the money Bustelberg accrues:
 *
 *   Gross
 *     ── Leonteq costs ──▶  after-Leonteq
 *         · transaction_bps per buy/sell (flat, every exchange)
 *         · leonteq_annual_bps per year, deducted at year-end
 *     ── Bustelberg fees ──▶  after-Bustelberg  (net to the client)
 *         · bustelberg_mgmt_bps per year (management)
 *         · bustelberg_perf_pct high-water-mark performance fee, charged
 *           each year-end on gains above the running peak
 *
 * Everything is computed CLIENT-SIDE from the gross daily equity curve
 * (`daily_records`) + the period chain (`monthly_records`, for trade
 * counting), so adjusting fees on /fees updates every backtest's
 * waterfall on the next render without re-running the backtest. Mirrors
 * how the old per-exchange net stats worked.
 *
 * Crystallization is annual: the annual fees + the performance fee are
 * applied at each Dec 31 inside the backtest window, with the final
 * partial year pro-rated by days. The high-water mark resets to the
 * post-management NAV at each crystallization peak so the client never
 * pays a performance fee twice on the same gains.
 *
 * Transaction cost is a turnover-drag model: at each rebalance the
 * fraction of the portfolio bought (new entrants) pays the buy fee and
 * the fraction sold (departures) pays the sell fee, both at
 * `transaction_bps`. The opening purchase (period 0 = all names) and the
 * final liquidation (last closed period = all names) are included.
 */
import type { DailyRecord, PeriodRecord } from '../../../lib/stores/momentum';
import type { NetStats } from './feeStats';

export type FeeConfig = {
  leonteq_annual_bps: number;
  transaction_bps: number;
  bustelberg_mgmt_bps: number;
  bustelberg_perf_pct: number;
};

export const DEFAULT_FEE_CONFIG: FeeConfig = {
  leonteq_annual_bps: 35,
  transaction_bps: 10,
  bustelberg_mgmt_bps: 100,
  bustelberg_perf_pct: 10,
};

/** One crystallization interval (calendar year, final one possibly
 * partial) in the fee breakdown — each year IN ISOLATION. `gross_return_pct`
 * is that year's own gross return; the four fee columns are what was
 * deducted from THAT year's return; `net_return_pct` is what's left. They
 * reconcile exactly per row: gross − transaction − annual − mgmt − perf =
 * net. (Year returns don't sum to the cumulative total — they compound.) */
export type FeeBreakdownRow = {
  label: string; // "2025", "2026 (partial)", …
  year_fraction: number;
  gross_return_pct: number; // this year's gross return
  transaction_pct: number; // pp deducted this year for per-trade costs
  leonteq_annual_pct: number; // pp deducted this year for the Leonteq annual fee
  mgmt_pct: number; // pp deducted this year for Bustelberg management
  perf_pct: number; // pp deducted this year for the performance fee
  net_return_pct: number; // this year's return, net of all fees
  hwm_pct: number; // running high-water-mark level (cumulative context)
};

export type FeeWaterfall = {
  gross_return_pct: number;
  after_leonteq_pct: number;
  after_bustelberg_pct: number; // net to client
  /** Percentage points lost to Leonteq (gross − after-Leonteq). */
  leonteq_drag_pp: number;
  /** Split of the Leonteq drag: per-trade transaction cost vs the annual fee. */
  transaction_drag_pp: number;
  leonteq_annual_drag_pp: number;
  /** Percentage points lost to Bustelberg (after-Leonteq − after-Bustelberg). */
  bustelberg_drag_pp: number;
  /** Money accrued by Bustelberg, as % of starting capital. */
  bustelberg_mgmt_pct: number;
  bustelberg_perf_pct: number;
  bustelberg_accrued_pct: number;
  /** Total Leonteq annual fee accrued, % of starting capital. */
  leonteq_annual_total_pct: number;
  years: number;
  /** Per-year crystallization breakdown for the detailed view. */
  breakdown: FeeBreakdownRow[];
  /** After-Bustelberg (net-to-client) headline stats, NetStats-shaped so
   * the existing `(net)` parenthetical consumers work unchanged. */
  net: NetStats;
};

type CurvePoint = { t: number; date: string; g: number };

/** ms timestamp for a YYYY-MM or YYYY-MM-DD string (UTC, day defaults to
 * the 1st / month-end fallback handled by callers). */
function parseDate(d: string): number {
  const s = d.length === 7 ? `${d}-01` : d;
  return Date.parse(s);
}

const YEAR_MS = 365.25 * 86400 * 1000;
const round2 = (v: number) => Math.round(v * 100) / 100;

/** Last point in `curve` whose date is ≤ targetMs (step sample). */
function sampleAt(curve: CurvePoint[], targetMs: number): number {
  let val = curve.length > 0 ? curve[0].g : 1;
  for (const p of curve) {
    if (p.t <= targetMs) val = p.g;
    else break;
  }
  return val;
}

/** The effective exit date (cash-out date) of a closed period: the latest
 * per-holding exit_date, falling back to the period label. */
function periodExitDate(r: PeriodRecord): string {
  let exit: string | null = null;
  for (const h of r.holdings) {
    if (h.exit_date && (exit == null || h.exit_date > exit)) exit = h.exit_date;
  }
  if (exit) return exit;
  return r.date.length === 7 ? `${r.date}-28` : r.date;
}

/** Build the gross closed-window daily curve (factor normalized to 1.0 at
 * the start), truncated to the last closed period's exit date. Falls back
 * to the period chain when no daily records are shipped. */
function buildGrossCurve(
  monthlyRecords: PeriodRecord[],
  dailyRecords: DailyRecord[] | undefined,
): CurvePoint[] {
  const closed = monthlyRecords.filter((r) => !r.is_open && r.holdings.length > 0);
  if (closed.length === 0) return [];
  const lastExit = periodExitDate(closed[closed.length - 1]);
  const lastExitMs = parseDate(lastExit);

  if (dailyRecords && dailyRecords.length > 1) {
    const pts: CurvePoint[] = [];
    for (const d of dailyRecords) {
      const t = parseDate(d.date);
      if (t > lastExitMs) break;
      pts.push({ t, date: d.date, g: 1 + d.cumulative_return_pct / 100 });
    }
    if (pts.length > 1) {
      const first = pts[0].g || 1;
      return pts.map((p) => ({ ...p, g: p.g / first }));
    }
  }

  // Fallback: period-chain curve. Anchor at the first entry date = 1.0,
  // then one point per closed period at its exit date.
  let firstEntry: string | null = null;
  for (const h of closed[0].holdings) {
    if (h.entry_date && (firstEntry == null || h.entry_date < firstEntry)) firstEntry = h.entry_date;
  }
  const startDate = firstEntry ?? (closed[0].date.length === 7 ? `${closed[0].date}-01` : closed[0].date);
  const pts: CurvePoint[] = [{ t: parseDate(startDate), date: startDate, g: 1 }];
  for (const r of closed) {
    const ex = periodExitDate(r);
    pts.push({ t: parseDate(ex), date: ex, g: 1 + (r.cumulative_return_pct ?? 0) / 100 });
  }
  return pts;
}

/** Per-closed-period transaction fee factor, keyed by exit date. */
function transactionDrags(
  monthlyRecords: PeriodRecord[],
  transactionBps: number,
): Array<{ t: number; factor: number }> {
  const fee = transactionBps / 10000;
  if (!(fee > 0)) return [];
  const closed = monthlyRecords.filter((r) => !r.is_open && r.holdings.length > 0);
  const sets = closed.map((r) => new Set(r.holdings.map((h) => h.company_id)));
  const out: Array<{ t: number; factor: number }> = [];
  for (let i = 0; i < closed.length; i++) {
    const cur = sets[i];
    const prev = i > 0 ? sets[i - 1] : new Set<number>();
    const next = i + 1 < sets.length ? sets[i + 1] : null; // null → last closed: all depart
    let entrants = 0;
    let departures = 0;
    for (const cid of cur) if (!prev.has(cid)) entrants++;
    for (const cid of cur) if (next == null || !next.has(cid)) departures++;
    const n = Math.max(cur.size, 1);
    const legFrac = (entrants + departures) / n;
    out.push({ t: parseDate(periodExitDate(closed[i])), factor: Math.max(0, 1 - legFrac * fee) });
  }
  return out;
}

/** Year-end crystallization dates inside (start, end], plus `end` itself
 * for the final partial year. `firstTradeMs` (the first rebalance date)
 * suppresses any year-end that precedes the first actual trade — otherwise
 * a January rebalance whose entry is priced at the prior trading day (which
 * can land on the previous Dec 31) would carve out a spurious, near-empty
 * prior-year bucket. */
function crystallizationDates(startMs: number, endMs: number, firstTradeMs: number): number[] {
  const out: number[] = [];
  const startYear = new Date(startMs).getUTCFullYear();
  const endYear = new Date(endMs).getUTCFullYear();
  for (let y = startYear; y <= endYear; y++) {
    const dec31 = Date.parse(`${y}-12-31`);
    if (dec31 > startMs && dec31 < endMs && dec31 >= firstTradeMs) out.push(dec31);
  }
  out.push(endMs); // final (possibly partial) year
  return out;
}

/** Multiply a base curve by a step-function drag (drops at each keyed
 * date). Returns a new curve. */
function applyStepDrag(
  base: CurvePoint[],
  drags: Array<{ t: number; factor: number }>,
): CurvePoint[] {
  const sorted = drags.slice().sort((a, b) => a.t - b.t);
  return base.map((p) => {
    let cum = 1;
    for (const d of sorted) {
      if (d.t <= p.t) cum *= d.factor;
      else break;
    }
    return { ...p, g: p.g * cum };
  });
}

/** Max drawdown (%, ≤ 0) over a curve's factors. */
function maxDrawdown(curve: CurvePoint[]): number {
  let peak = -Infinity;
  let maxDd = 0;
  for (const p of curve) {
    if (p.g > peak) peak = p.g;
    if (peak > 0) {
      const dd = (p.g / peak - 1) * 100;
      if (dd < maxDd) maxDd = dd;
    }
  }
  return maxDd;
}

/** Daily Sharpe from a curve (mean/std × √252), or null when too few
 * daily steps to be meaningful (< 21). */
function dailySharpe(curve: CurvePoint[]): number | null {
  const rets: number[] = [];
  for (let i = 1; i < curve.length; i++) {
    const prev = curve[i - 1].g;
    if (prev > 0) rets.push(curve[i].g / prev - 1);
  }
  if (rets.length < 21) return null;
  const mean = rets.reduce((a, b) => a + b, 0) / rets.length;
  const variance = rets.reduce((a, b) => a + (b - mean) ** 2, 0) / rets.length;
  const std = Math.sqrt(variance);
  return std > 0 ? (mean / std) * Math.sqrt(252) : null;
}

/** Calendar-year compound returns from a curve (Jan 1 → Dec 31 ratios).
 * Only full calendar years are reported. */
function yearlyFromCurve(curve: CurvePoint[]): Record<string, number> {
  const out: Record<string, number> = {};
  if (curve.length < 2) return out;
  const startYear = new Date(curve[0].t).getUTCFullYear();
  const endYear = new Date(curve[curve.length - 1].t).getUTCFullYear();
  for (let y = startYear; y <= endYear; y++) {
    const janMs = Date.parse(`${y}-01-01`);
    const decMs = Date.parse(`${y}-12-31`);
    // Need a point at/after Jan 1 of this year and the curve must extend
    // through year-end (otherwise it's a partial year — skip).
    if (curve[0].t > janMs) continue; // backtest started mid-year
    if (curve[curve.length - 1].t < decMs) continue; // incomplete final year
    const startFactor = sampleAt(curve, janMs);
    const endFactor = sampleAt(curve, decMs);
    if (startFactor > 0) out[String(y)] = (endFactor / startFactor - 1) * 100;
  }
  return out;
}

/** NetStats-shaped projection of a net curve onto the closed-period grid
 * (so the existing parenthetical + custom-range consumers keep working). */
function netStatsFromCurve(
  netCurve: CurvePoint[],
  grossCurve: CurvePoint[],
  closed: PeriodRecord[],
  years: number,
): NetStats {
  const dates: string[] = [];
  const periodReturns: number[] = [];
  const cumFactors: number[] = [];
  const periodDragFactors: Array<{ exit_date: string; fee_factor: number }> = [];
  let prevNet = 1;
  let prevGross = 1;
  for (const r of closed) {
    const exit = periodExitDate(r);
    const tMs = parseDate(exit);
    const netF = sampleAt(netCurve, tMs);
    const grossF = sampleAt(grossCurve, tMs);
    const netRet = prevNet > 0 ? (netF / prevNet - 1) * 100 : 0;
    const grossRet = prevGross > 0 ? (grossF / prevGross - 1) * 100 : 0;
    dates.push(r.date);
    periodReturns.push(round2(netRet));
    cumFactors.push(netF / (netCurve[0]?.g ?? 1));
    const feeFactor = (1 + netRet / 100) / (1 + grossRet / 100);
    periodDragFactors.push({ exit_date: exit, fee_factor: Math.min(1, Number.isFinite(feeFactor) ? feeFactor : 1) });
    prevNet = netF;
    prevGross = grossF;
  }
  const finalFactor = (netCurve[netCurve.length - 1]?.g ?? 1) / (netCurve[0]?.g ?? 1);
  const total = (finalFactor - 1) * 100;
  const annualized = years > 0 && finalFactor > 0 ? (Math.pow(finalFactor, 1 / years) - 1) * 100 : 0;
  return {
    total_return_pct: total,
    annualized_return_pct: annualized,
    max_drawdown_pct: maxDrawdown(netCurve),
    sharpe_ratio: dailySharpe(netCurve),
    dates,
    period_returns: periodReturns,
    cum_factors: cumFactors,
    yearly: yearlyFromCurve(netCurve),
    period_drag_factors: periodDragFactors,
  };
}

/** The headline entry point. Returns the fee waterfall + a net-to-client
 * NetStats, or null when there's no closed-period data to model. */
export function computeFeeWaterfall(
  monthlyRecords: PeriodRecord[] | undefined,
  dailyRecords: DailyRecord[] | undefined,
  config: FeeConfig,
  opts?: { grossTotalReturnPct?: number },
): FeeWaterfall | null {
  if (!monthlyRecords || monthlyRecords.length === 0) return null;
  const closed = monthlyRecords.filter((r) => !r.is_open && r.holdings.length > 0);
  if (closed.length === 0) return null;

  let gross = buildGrossCurve(monthlyRecords, dailyRecords);
  if (gross.length < 2) return null;

  // Anchor the curve's final factor to the authoritative gross total so
  // the panel's "Gross" row equals the headline Total Return exactly.
  if (opts?.grossTotalReturnPct != null) {
    const target = 1 + opts.grossTotalReturnPct / 100;
    const curFinal = gross[gross.length - 1].g;
    if (curFinal > 0) {
      const scale = target / curFinal;
      gross = gross.map((p) => ({ ...p, g: p.g * scale }));
    }
  }

  const startMs = gross[0].t;
  const endMs = gross[gross.length - 1].t;
  const years = Math.max((endMs - startMs) / YEAR_MS, 1e-9);

  // ── Leonteq layer ───────────────────────────────────────────────
  // (a) transaction drag at each rebalance.
  const txnDrags = transactionDrags(monthlyRecords, config.transaction_bps);
  const afterTxn = applyStepDrag(gross, txnDrags);

  // (b) annual deduction at each crystallization point. The first trade's
  // rebalance date anchors year bucketing so a prior-day entry priced into
  // the previous calendar year doesn't spawn a spurious leading year.
  const firstTradeMs = parseDate(closed[0].date);
  const crysts = crystallizationDates(startMs, endMs, firstTradeMs);
  const leonteqRate = config.leonteq_annual_bps / 10000;
  const leonteqDrags: Array<{ t: number; factor: number }> = [];
  let prevC = startMs;
  for (const c of crysts) {
    const yf = (c - prevC) / YEAR_MS;
    leonteqDrags.push({ t: c, factor: Math.max(0, 1 - leonteqRate * yf) });
    prevC = c;
  }
  const afterLeonteq = applyStepDrag(afterTxn, leonteqDrags);

  // ── Bustelberg layer (management + high-water-mark performance) ──
  // Walk the crystallization points once, applying the management +
  // performance fees on the after-Leonteq NAV and emitting INCREMENTAL
  // drag factors for the client curve. The drag pushed at each point is
  // R_i / R_{i-1} (where R = clientNAV / afterLeonteqNAV); applyStepDrag
  // compounds them, so the cumulative product telescopes to R_i — i.e.
  // each point's stored factor must be the *step*, not the running total.
  // (Pushing the running ratio R_i directly double-counts: a year-end +
  // final point would apply R_1·R_2 instead of R_2.)
  const mgmtRate = config.bustelberg_mgmt_bps / 10000;
  const perfRate = config.bustelberg_perf_pct / 100;
  let bPrev = 1; // client NAV (post all Bustelberg fees)
  let hwm = 1;
  let lPrev = sampleAt(afterLeonteq, startMs); // after-Leonteq NAV at start (≈1)
  let cumLeonteqAnnual = 1; // running product of annual factors, for the breakdown
  let prevR = 1; // previous client/afterLeonteq ratio (telescoping base)
  let sumMgmt = 0;
  let sumPerf = 0;
  let sumLeonteqAnnual = 0;
  const bustelbergDrags: Array<{ t: number; factor: number }> = [];
  const breakdown: FeeBreakdownRow[] = [];
  prevC = startMs;
  for (const c of crysts) {
    const yf = (c - prevC) / YEAR_MS;
    // Year-start / year-end NAVs at each layer, so we can express THIS
    // year's return and per-layer drag in isolation.
    const gPrev = sampleAt(gross, prevC);
    const gNow = sampleAt(gross, c);
    const atPrev = sampleAt(afterTxn, prevC);
    const atNow = sampleAt(afterTxn, c);
    const lNow = sampleAt(afterLeonteq, c);

    const gYear = gPrev > 0 ? gNow / gPrev - 1 : 0; // gross return this year
    const atYear = atPrev > 0 ? atNow / atPrev - 1 : 0; // after per-trade costs
    const lYear = lPrev > 0 ? lNow / lPrev - 1 : 0; // after txn + annual fee

    // Leonteq annual fee accrued (nominal, for the total) — on the
    // after-txn NAV carried with prior annual factors.
    const lNavBeforeAnnual = atNow * cumLeonteqAnnual;
    sumLeonteqAnnual += lNavBeforeAnnual * leonteqRate * yf;
    cumLeonteqAnnual *= Math.max(0, 1 - leonteqRate * yf);

    // Bustelberg fees on the after-Leonteq NAV. bPrev is this year's
    // STARTING client capital; bPre is its pre-Bustelberg-fee year-end
    // value (grows at exactly lYear), so net_year = lYear − mgmt − perf.
    const ratio = lPrev > 0 ? lNow / lPrev : 1;
    const bPre = bPrev * ratio;
    const mgmtFee = bPre * mgmtRate * yf;
    const bAfterMgmt = bPre - mgmtFee;
    let perfFee = 0;
    if (bAfterMgmt > hwm) {
      perfFee = perfRate * (bAfterMgmt - hwm);
      hwm = bAfterMgmt; // peak resets to the pre-perf-fee high
    }
    const bPost = bAfterMgmt - perfFee;
    sumMgmt += mgmtFee;
    sumPerf += perfFee;

    const R = lNow > 0 ? bPost / lNow : 1;
    bustelbergDrags.push({ t: c, factor: prevR > 0 ? R / prevR : R });
    prevR = R;

    const netYear = bPrev > 0 ? bPost / bPrev - 1 : 0;
    const cDate = new Date(c);
    const isDec31 = cDate.getUTCMonth() === 11 && cDate.getUTCDate() === 31;
    breakdown.push({
      label: `${cDate.getUTCFullYear()}${isDec31 ? '' : ' (partial)'}`,
      year_fraction: yf,
      gross_return_pct: gYear * 100,
      transaction_pct: (gYear - atYear) * 100,
      leonteq_annual_pct: (atYear - lYear) * 100,
      mgmt_pct: bPrev > 0 ? (mgmtFee / bPrev) * 100 : 0,
      perf_pct: bPrev > 0 ? (perfFee / bPrev) * 100 : 0,
      net_return_pct: netYear * 100,
      hwm_pct: (hwm - 1) * 100,
    });

    bPrev = bPost;
    lPrev = lNow;
    prevC = c;
  }
  const afterBustelberg = applyStepDrag(afterLeonteq, bustelbergDrags);

  const grossFinal = gross[gross.length - 1].g;
  const txnFinal = afterTxn[afterTxn.length - 1].g;
  const leonteqFinal = afterLeonteq[afterLeonteq.length - 1].g;
  const bustelbergFinal = afterBustelberg[afterBustelberg.length - 1].g;

  const grossPct = (grossFinal - 1) * 100;
  const afterTxnPct = (txnFinal - 1) * 100;
  const afterLeonteqPct = (leonteqFinal - 1) * 100;
  const afterBustelbergPct = (bustelbergFinal - 1) * 100;

  return {
    gross_return_pct: grossPct,
    after_leonteq_pct: afterLeonteqPct,
    after_bustelberg_pct: afterBustelbergPct,
    leonteq_drag_pp: grossPct - afterLeonteqPct,
    transaction_drag_pp: grossPct - afterTxnPct,
    leonteq_annual_drag_pp: afterTxnPct - afterLeonteqPct,
    bustelberg_drag_pp: afterLeonteqPct - afterBustelbergPct,
    bustelberg_mgmt_pct: sumMgmt * 100,
    bustelberg_perf_pct: sumPerf * 100,
    bustelberg_accrued_pct: (sumMgmt + sumPerf) * 100,
    leonteq_annual_total_pct: sumLeonteqAnnual * 100,
    years,
    breakdown,
    net: netStatsFromCurve(afterBustelberg, gross, closed, years),
  };
}
