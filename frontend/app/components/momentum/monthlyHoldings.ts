/**
 * Pure data layer for the Monthly Portfolios table — no React, no DOM.
 *
 * Extracted from `MonthlyHoldingsTable.tsx` so the subtle bits (one-way
 * turnover, open-period re-pricing, and the go-live period split with its
 * relative-return math) are testable in isolation and the component is left
 * as a rendering shell over these.
 */
import type { Holding, PeriodRecord } from '../../../lib/stores/momentum';

export type HeldCompany = { company_id: number; ticker: string; company_name: string };

/** Per-company close (local + optional EUR) at some date, keyed by the
 * stringified company_id (the shape the `/prices-at` endpoint returns). */
export type PriceMap = Record<string, { price_local: number; price_eur?: number; target_date: string }>;

/** One rendered row: a period record (possibly a go-live sub-slice) plus the
 * metadata the table needs (split label, which date to read turnover for,
 * and whether the net-of-fees parenthetical applies). */
export type DisplayRow = {
  row: PeriodRecord;
  key: string;
  label: string | null;
  turnoverDate: string | null;
  net: boolean;
  /** Window this row's return spans (YYYY-MM[-DD]). Drives the partial
   * net-of-fees calc for open periods + go-live sub-slices. `windowEnd` is
   * null for a normal closed period (its net comes from the closed-period
   * fee curve, not the partial calc). */
  windowStart: string;
  windowEnd: string | null;
};

/** Minimal daily-equity-curve point (strategy or universe). */
type DailyCurvePoint = { date: string; cumulative_return_pct: number };

/** Every distinct company ever held during the backtest — one entry per
 * company_id — sorted by ticker. Drives the header search box. */
export function collectHeldCompanies(records: readonly PeriodRecord[]): HeldCompany[] {
  const seen = new Map<number, HeldCompany>();
  for (const r of records) {
    for (const h of r.holdings) {
      if (!seen.has(h.company_id)) {
        seen.set(h.company_id, {
          company_id: h.company_id,
          ticker: h.ticker ?? '',
          company_name: h.company_name ?? '',
        });
      }
    }
  }
  return Array.from(seen.values()).sort((a, b) => a.ticker.localeCompare(b.ticker));
}

/** One-way turnover per period: % of the period's holdings that weren't held
 * in the previous period. The first period (no prior portfolio) and any
 * empty period are null. */
export function computeTurnoverByDate(records: readonly PeriodRecord[]): Record<string, number | null> {
  const map: Record<string, number | null> = {};
  let prevIds: Set<number> | null = null;
  for (const r of records) {
    const currIds = new Set(r.holdings.map((h) => h.company_id));
    if (prevIds === null || currIds.size === 0) {
      map[r.date] = null;
    } else {
      let added = 0;
      for (const id of currIds) if (!prevIds.has(id)) added += 1;
      map[r.date] = (added / currIds.size) * 100;
    }
    prevIds = currIds;
  }
  return map;
}

/** Weighted-mean forward return over a holdings list (skips null returns;
 * defaults each weight to 1). Null when nothing is priced. */
function weightedMeanReturn(arr: readonly Holding[]): number | null {
  let w = 0, r = 0;
  for (const h of arr) {
    if (h.forward_return_pct == null) continue;
    const wt = h.weight ?? 1;
    w += wt; r += wt * h.forward_return_pct;
  }
  return w > 0 ? r / w : null;
}

/** Re-price the trailing OPEN period to a fresher set of closes (when newer
 * data than the backtest's as-of exists), recomputing that period's return
 * (long weight-mean − short weight-mean, matching the engine) + the chained
 * cumulative. Returns the records unchanged when there's nothing to re-price. */
export function repriceOpenPeriod(
  records: readonly PeriodRecord[],
  openReprice: { date: string; prices: PriceMap } | null,
): PeriodRecord[] {
  const recs = records as PeriodRecord[];
  if (!openReprice || recs.length === 0) return recs;
  const lastIdx = recs.length - 1;
  const last = recs[lastIdx];
  if (!last.is_open) return recs;
  const newHoldings = last.holdings.map((h) => {
    const p = openReprice.prices[String(h.company_id)];
    if (!p) return h;
    const exitEur = p.price_eur ?? h.exit_price_eur;
    const entryEur = h.entry_price_eur;
    return {
      ...h,
      exit_price_local: p.price_local,
      exit_price_eur: exitEur,
      exit_date: p.target_date.slice(0, 10),
      forward_return_pct: exitEur != null && entryEur ? (exitEur / entryEur - 1) * 100 : h.forward_return_pct,
    };
  });
  const longRet = weightedMeanReturn(newHoldings.filter((h) => h.side !== 'short'));
  const shorts = newHoldings.filter((h) => h.side === 'short');
  const shortRet = shorts.length ? weightedMeanReturn(shorts) : null;
  const portRet = longRet != null ? (shortRet != null ? longRet - shortRet : longRet) : last.portfolio_return_pct;
  const prevCum = lastIdx >= 1 ? (recs[lastIdx - 1].cumulative_return_pct ?? 0) : 0;
  const cum = portRet != null ? ((1 + prevCum / 100) * (1 + portRet / 100) - 1) * 100 : last.cumulative_return_pct;
  const reLast: PeriodRecord = {
    ...last,
    holdings: newHoldings,
    portfolio_return_pct: portRet,
    cumulative_return_pct: cum,
    as_of_date: openReprice.date,
  };
  return [...recs.slice(0, lastIdx), reLast];
}

/** Cumulative value of a (sorted) daily curve at-or-before `date`. */
function cumAt(series: readonly DailyCurvePoint[], date: string): number | null {
  let v: number | null = null;
  for (const p of series) { if (p.date <= date) v = p.cumulative_return_pct; else break; }
  return v;
}

/** Relative period return between two cumulative levels: (1+a)/(1+b) − 1. */
function relReturn(a: number | null, b: number | null): number | null {
  return a != null && b != null ? ((1 + a / 100) / (1 + b / 100) - 1) * 100 : null;
}

/** Build the table's display rows: the period records, except the ONE period
 * whose window contains `markerDate` is split into two slices (pre-go-live and
 * go-live→end), with per-period + cumulative returns (strategy AND universe)
 * recomputed from the daily curves so each slice reads like a standalone
 * period. Holdings are re-priced at the go-live boundary when `goLivePrices`
 * is available (falls back to the full-period prices otherwise). With no
 * marker (or no daily curve) it's a straight passthrough. */
export function splitAtGoLive(args: {
  records: readonly PeriodRecord[];
  dailyRecords: readonly DailyCurvePoint[] | undefined;
  universeDailyRecords: readonly DailyCurvePoint[] | undefined;
  markerDate: string | undefined;
  goLivePrices: PriceMap | null;
}): DisplayRow[] {
  const { records, markerDate, goLivePrices } = args;
  const passthrough = (): DisplayRow[] =>
    records.map((r) => ({
      row: r, key: r.date, label: null, turnoverDate: r.date, net: true,
      windowStart: r.date.slice(0, 10),
      windowEnd: r.as_of_date ? r.as_of_date.slice(0, 10) : null,
    }));
  if (!markerDate) return passthrough();

  const stratSeries = (args.dailyRecords ?? []).map((d) => ({ date: d.date.slice(0, 10), cumulative_return_pct: d.cumulative_return_pct }));
  const uniSeries = (args.universeDailyRecords ?? []).map((d) => ({ date: d.date.slice(0, 10), cumulative_return_pct: d.cumulative_return_pct }));
  if (stratSeries.length === 0) return passthrough();

  const out: DisplayRow[] = [];
  for (let i = 0; i < records.length; i++) {
    const r = records[i];
    const start = r.date.slice(0, 10);
    const end = i + 1 < records.length ? records[i + 1].date.slice(0, 10) : (r.as_of_date ?? null);
    const inside = markerDate > start && (end == null || markerDate < end);
    if (!inside) {
      out.push({
        row: r, key: r.date, label: null, turnoverDate: r.date, net: true,
        windowStart: start, windowEnd: r.as_of_date ? r.as_of_date.slice(0, 10) : null,
      });
      continue;
    }

    const cStart = cumAt(stratSeries, start);
    const cGo = cumAt(stratSeries, markerDate);
    const cEnd = end ? cumAt(stratSeries, end) : stratSeries[stratSeries.length - 1].cumulative_return_pct;
    const uStart = cumAt(uniSeries, start);
    const uGo = cumAt(uniSeries, markerDate);
    const uEnd = end ? cumAt(uniSeries, end) : (uniSeries.length ? uniSeries[uniSeries.length - 1].cumulative_return_pct : null);

    // Re-price each holding at the go-live boundary so the expanded detail
    // shows entry/exit prices for the sub-period's own dates. `pre` ends at
    // go-live (exit = go-live price); `post` starts at go-live (entry =
    // go-live price). Falls back to the original holding when the go-live
    // price isn't (yet) available for a cid.
    const repricePre = (h: Holding): Holding => {
      const p = goLivePrices?.[String(h.company_id)];
      if (!p) return h;
      const exitEur = p.price_eur ?? h.exit_price_eur;
      const entryEur = h.entry_price_eur;
      return {
        ...h,
        exit_price_local: p.price_local,
        exit_price_eur: exitEur,
        exit_date: markerDate,
        forward_return_pct: exitEur != null && entryEur ? (exitEur / entryEur - 1) * 100 : h.forward_return_pct,
      };
    };
    const repricePost = (h: Holding): Holding => {
      const p = goLivePrices?.[String(h.company_id)];
      if (!p) return h;
      const entryEur = p.price_eur ?? h.entry_price_eur;
      const exitEur = h.exit_price_eur;
      return {
        ...h,
        entry_price_local: p.price_local,
        entry_price_eur: entryEur,
        entry_date: markerDate,
        forward_return_pct: entryEur != null && exitEur ? (exitEur / entryEur - 1) * 100 : h.forward_return_pct,
      };
    };

    const pre: PeriodRecord = {
      ...r,
      holdings: r.holdings.map(repricePre),
      portfolio_return_pct: relReturn(cGo, cStart),
      universe_return_pct: relReturn(uGo, uStart),
      cumulative_return_pct: cGo ?? r.cumulative_return_pct,
      universe_cumulative_return_pct: uGo,
      is_open: false,
      as_of_date: undefined,
    };
    const post: PeriodRecord = {
      ...r,
      holdings: r.holdings.map(repricePost),
      portfolio_return_pct: relReturn(cEnd, cGo),
      universe_return_pct: relReturn(uEnd, uGo),
      cumulative_return_pct: cEnd ?? r.cumulative_return_pct,
      universe_cumulative_return_pct: uEnd,
      is_open: r.is_open,
      as_of_date: r.as_of_date,
    };
    out.push({
      row: pre, key: `${r.date}__pre`, label: `${start} → ${markerDate} · pre go-live`,
      turnoverDate: r.date, net: false, windowStart: start, windowEnd: markerDate,
    });
    out.push({
      row: post, key: `${r.date}__post`, label: `${markerDate} → ${end ?? 'now'} · go-live →`,
      turnoverDate: null, net: false, windowStart: markerDate,
      windowEnd: end ?? (r.as_of_date ? r.as_of_date.slice(0, 10) : null),
    });
  }
  return out;
}
