'use client';

import { useMemo, useState } from 'react';
import {
  CartesianGrid, ComposedChart, Line, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis,
} from 'recharts';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { chartTheme } from '../../../lib/chartTheme';
import { logLinearFit } from '../../../lib/trendFit';
import { AspectCard } from '../../../lib/tipCard';
import InfoTip from '../InfoTip';
import HoldingsRevenueModal, { type Target } from './HoldingsRevenueModal';
import HoldingsIngestPanel from './HoldingsIngestPanel';
import { LegendItem } from './ChartLegend';
import { noteFor, reportingLine, whyNoLine, type BlendNote } from './blendNotes';
import { paddedLogDomain, periodTick, stepChanges, type Step } from './marginData';
import { endpointCagr } from './lineCagr';
import { periodAxis } from '../../../lib/chartAxis';
import { benchNote, benchmarkFirst, rebaseSeries, seriesCrossesZero } from './benchSeries';

/**
 * One "Long Equity" growth card: a metric per fiscal year on a LOG axis with an exponential-trend
 * overlay, its R²/CAGR, and a click-through to the per-holding table. A repeatable unit — Revenue
 * and FCF/share are two instances of it; more slot into the grid the same way.
 *
 * ⚠ R² IS COMPUTED FROM THE POINTS ON SCREEN (`logLinearFit`), so the headline can't disagree with
 * the plotted line. The company series is EXTRACTED from `data.metrics` (fetched once by the tab),
 * matching either section spelling.
 */

export type MetricCfg = {
  title: string;                 // 'Revenue' | 'FCF / share' | 'ROIC'
  noun: string;                  // 'revenue' | 'FCF/share' | 'ROIC' — for labels/messages
  codes: string[];               // company-metric codes (both section spellings)
  benchmarkMetric: string;       // the `metric` param for the holdings drill-down endpoint
  unit: 'millions' | 'per_share' | 'percent' | 'shares';   // 'shares' = a count, no currency
  // 'growth' = a compounding series on a LOG axis with an exponential trend + R²/CAGR;
  // 'ratio'  = a % on a LINEAR axis with an average line (a ratio doesn't compound).
  kind?: 'growth' | 'ratio';
  /**
   * The ANALYSTS' forecast of this same line — a striped continuation leaving the newest actual
   * (the LTM when there is one), with a dot on every forecast year exactly as the solid line has
   * one on every reported year. Omit and the card has no forecast leg, which is the right answer
   * for every metric nobody publishes a consensus for.
   *
   * ⚠⚠ IT IS NOT A MEASUREMENT AND MUST NEVER BE TREATED AS ONE. It is out of the trend fit, out of
   * the CAGR, out of `crossesZero` and out of the Latest/Avg tiles — every one of those is a claim
   * about what the business DID, and a consensus is a claim about what people expect. The fit in
   * particular would be corrupted twice over: it would extend the regression across five years
   * nobody has lived, and the R² above the chart would then describe how tightly the past hugs a
   * line drawn partly through the future.
   *
   * ⚠ ONLY ON THE ANNUAL BASIS. Analysts publish a figure per forward FISCAL YEAR; there is no
   * trailing-twelve-month reading of a forecast, and rolling one would invent quarters nobody
   * published. The backend refuses the metric on the quarterly basis for the same reason.
   */
  forecastCodes?: string[];
  /** The `metric` key for the same forecast in a NARROWED (benchmark) blend request. */
  forecastMetric?: string;
};

type MetricRow = { metric_code: string; target_date: string; numeric_value: number | null };

/**
 * One metric's points out of a metrics blob — the company's, or an index's.
 *
 * ⚠ THE X UNIT IS ALWAYS A YEAR — WHOLE ON ANNUAL, FRACTIONAL ON QUARTERLY — AND THAT IS WHAT
 * KEEPS THE CAGR A **C-A-GR**. `logLinearFit` regresses ln(value) on this axis, so its slope is
 * "per x unit". Bucketing quarterly points 0,1,2,3… would make the slope per QUARTER and the card
 * would print a quarterly growth rate under a label that says annual — a number ~4x too small,
 * entirely plausible, and wrong on every one of the three growth cards at once.
 *
 * A TTM point dated 2026-03-31 sits at 2026.25, so four of them span exactly 1.0 on the axis and
 * the fitted slope is per year by construction. R² is unaffected (it is scale-free).
 *
 * ⚠ ONE EXTRACTION, BOTH LINES. The benchmark overlay runs through this same function, so the two
 * series on a chart cannot have been built from different rules about which row wins a period.
 */
function extractPoints(rows: MetricRow[], codes: string[], cadence: 'annual' | 'quarterly') {
  const want = new Set(codes);
  const byX = new Map<number, { date: string; value: number }>();
  for (const m of rows) {
    if (!want.has(m.metric_code) || m.numeric_value == null) continue;
    const d = String(m.target_date);
    const y = parseInt(d.slice(0, 4), 10);
    if (y < 2015) continue;   // charts start from 2015, like the holdings/margin views
    // Annual rows keep one point per calendar year (the latest); TTM rows are already one per
    // quarter, so each gets its own x and nothing collapses.
    const x = cadence === 'quarterly'
      ? y + (Math.ceil(parseInt(d.slice(5, 7), 10) / 3) - 1) / 4
      : y;
    const cur = byX.get(x);
    if (!cur || d > cur.date) byX.set(x, { date: d, value: m.numeric_value });
  }
  return [...byX.entries()].map(([year, v]) => ({ year, value: v.value, date: v.date }))
    .sort((a, b) => a.year - b.year);
}

/**
 * Where an LTM point belongs on an ANNUAL axis: a fraction of a year past the last reported one.
 *
 * ⚠⚠ THE QUARTER BUCKET PUTS IT ON TOP OF A FISCAL YEAR, AND WITH A FORECAST ON THE CHART THAT IS
 * NO LONGER INVISIBLE. A trailing year ending 2026-03-31 buckets to `2026 + (1−1)/4` = **2026.0** —
 * the same x as FY2026, which is where the analysts' first estimate sits. So the newest ACTUAL and
 * the first FORECAST landed on one tick, the LTM appeared to be a year further along than it is,
 * and the dotted leg started underneath it instead of after it.
 *
 * The fix cannot be "use the calendar fraction" either, because the annual axis is not a calendar:
 * `extractPoints` places a fiscal year at the YEAR IT ENDS IN, so an off-calendar filer's FY2026
 * (ending 2026-03-31) already sits at 2026 while occupying the same months this LTM does. Measuring
 * from the entity's OWN last fiscal year end sidesteps the whole question — three months past it is
 * three months past it, whatever calendar that year was labelled with.
 *
 * ⚠ CLAMPED INSIDE THE YEAR, never onto its neighbours. At 0 it would sit on the last actual and
 * hide it; at 1 it would sit on the first forecast, which is the bug this exists to fix.
 */
const YEAR_MS = 365.25 * 24 * 3600 * 1000;
function ltmYearX(ltmDate: string | undefined, last: { year: number; date?: string } | null) {
  if (!last) return null;
  if (!ltmDate || !last.date) return last.year + 0.25;   // no date to measure from: a quarter on
  const gap = (Date.parse(`${ltmDate}T00:00:00Z`) - Date.parse(`${last.date}T00:00:00Z`)) / YEAR_MS;
  return last.year + Math.min(0.95, Math.max(0.05, gap));
}

/**
 * ⚠⚠ THE LTM POINT LIVES IN THE DRILL-DOWN TABLE, NOT HERE — AND THE ATTEMPT TO DERIVE IT ON THE
 * CLIENT WAS WRONG IN A WAY THAT LOOKED RIGHT (removed 2026-08-12, same day it was added).
 *
 * The reasoning was: the payload already carries every metric code, so the quarterly twin's newest
 * point IS the trailing twelve months and needs no request. It is not. `/by-isin/{isin}/metrics`
 * returns RAW rows, and the TTM roll-up (`_ttm_metric_rows`, cadence-aware since it has to sum four
 * quarters for a quarterly filer and TWO half-years for a semi-annual one) only runs when the
 * REQUEST asks for `cadence=quarterly`. So `quarterly__…__Revenue` here is one quarter: ASML plotted
 * ~8.8bn as its LTM against a true 35,327.5 — a quarter of the real figure, on a log axis, next to
 * eleven years of full-year points.
 *
 * Rebuilding the roll-up here would mean a second copy of the cadence rules (sum for flows, mean for
 * share counts, and `filings_per_year` so a semi-annual filer is not summed into 24 months) — the
 * duplication this tab keeps removing. The value has to come from the server, which already computes
 * it correctly for the table: see `_ltm_by_company` in `routers/earnings.py`.
 *
 * ⚠ RESOLVED: the annual payload now carries it as `ltm__…` rows (`_ltm_rows`) — one trailing
 * twelve months per metric, at the newest quarter-end, present only when that reaches PAST the last
 * full fiscal year. This reads those; it does not compute one.
 */
type LtmPoint = { year: number; value: number; date?: string };

function ltmPoint(rows: MetricRow[], codes: string[],
                  last: { year: number; date?: string } | null): LtmPoint | null {
  // ⚠ EXTRACTED ON THE **QUARTERLY** AXIS. The row is dated to a quarter-end, and that is where it
  // belongs on the x: a June LTM sits at 2026.25, a quarter past the last full year, so the gap on
  // screen is the real interval. Bucketed as annual it would land on 2026 and claim a whole year.
  //
  // ⚠ THE FILINGS BEHIND IT ARE NOT READ HERE. They are per HOLDING, and a chart point is a blend
  // of many — so they belong in the drill-down table, one ⓘ per row of its LTM column, where there
  // is a company to attribute them to. See `HoldingsRevenueModal`.
  const ltm = extractPoints(rows, codes.map(
    (c) => `ltm__${c.slice(c.indexOf('__') + 2)}`), 'quarterly');
  const p = ltm.length ? ltm[ltm.length - 1] : null;
  if (!p) return null;
  return { ...p, year: ltmYearX(p.date, last) ?? p.year };
}

/**
 * A point's move from the period before it, as the hover reads it: `"+11.4% vs 2024"`, or `''`.
 *
 * ⚠⚠ AGAINST THE PREVIOUS PERIOD, NOT THE ANCHOR. Cumulative-since-2015 is what the two lines
 * already SHOW — their separation on a log axis is exactly that comparison, so putting it in the
 * hover restates the picture instead of adding to it. The per-period step is the part the chart
 * cannot be read for: two lines both rising steeply say nothing about which one grew faster in the
 * year under the cursor.
 *
 * ⚠ IT NAMES THE PERIOD IT IS MEASURED FROM RATHER THAN SAYING "YoY", because on this tab it
 * frequently is not a year — the LTM point is a quarter or two past the last fiscal year (the same
 * stub that is excluded from the CAGR fit, because regressing it as a full period overstates the
 * rate), the quarterly basis steps one QUARTER at a time, and a period the coverage floor withheld
 * leaves a two-year interval drawn as one segment. "+4.2% YoY" over any of those is a confident
 * mislabel of the interval, which is worse than the extra word.
 *
 * ⚠ EMPTY, NOT ZERO, WHEN THERE IS NOTHING TO MEASURE FROM — the first point of a series, and a
 * non-positive base (`pct: null`; see `stepChanges`). "0.0%" there would assert a flat period that
 * was never observed. The caller renders the emptiness as a dash where the row would otherwise be
 * blank, and simply omits it where a real value is already printed.
 *
 * ⚠ EXPORTED, SO THE LEVEL CARDS SHARE ONE HOVER RATHER THAN RESEMBLING EACH OTHER. Invested
 * capital is the same kind of chart as Revenue — a currency level, indexed, on a log axis — and a
 * second copy of this formatting is how the two come to phrase the same fact differently. `ltmXs`
 * is optional because only the `MetricGrowthCard` charts carry an LTM point; the derived `*-inputs`
 * cards drop the `LTM` period (`plottable`) and pass nothing.
 */
export function pctSince(step: Step | null | undefined, ltmXs?: ReadonlySet<number>): string {
  if (!step || step.pct == null) return '';
  return `${step.pct >= 0 ? '+' : ''}${step.pct.toFixed(1)}% vs ${periodTick(step.from, ltmXs)}`;
}

export function Stat({ label, value, tone, color, info }: {
  label: string; value: string; tone?: string; color?: string; info?: React.ReactNode;
}) {
  // `color` (a chart hex) ties the tile to its line — a coloured left bar + matching value ink.
  return (
    <div className="rounded-lg border border-neutral-800/40 bg-inset px-3 py-2 min-w-[6.5rem]"
      style={color ? { borderLeft: `3px solid ${color}` } : undefined}>
      <div className="flex items-center gap-1 text-[12px] uppercase tracking-wide text-fg-muted">{label}{info}</div>
      <div className={`font-mono text-xl font-semibold leading-tight ${color ? '' : (tone ?? 'text-fg-strong')}`}
        style={color ? { color } : undefined}>{value}</div>
    </div>
  );
}

export default function MetricGrowthCard({
  cfg, metrics, isAgg, currency, holdingsTarget, holdingsName, ingestIsin, onIngested,
  blendNotes, onReloadMetrics, cadence = 'annual', benchMetrics, benchLabel, benchErr,
  benchNotes,
}: {
  cfg: MetricCfg;
  /** 'annual' = one point per fiscal year. 'quarterly' = one TRAILING-TWELVE-MONTH point per
   *  quarter — quarterly frequency, annual scope, so it is comparable with the annual line and
   *  free of the seasonality raw quarters would carry. */
  cadence?: 'annual' | 'quarterly';
  metrics: MetricRow[] | null;   // null = still loading (the tab's company fetch)
  isAgg: boolean;
  currency?: string | null;
  holdingsTarget: Target;
  holdingsName?: string | null;  // the portfolio/company the drill-down is for
  // Portfolio only: why a metric the holdings DO carry produced no blended line. Absent for a
  // metric nobody reports — that one really is "not ingested". See `blendNotes`.
  blendNotes?: Record<string, BlendNote>;
  // Reload just the metrics fetch (the four growth cards share it), NOT the whole tab — the
  // derived cards each refetch their own inputs on a re-key and none of that is needed here.
  onReloadMetrics?: () => void;
  // Single-company only: its ISIN + a callback to reload the tab's metrics after an ingest, so an
  // empty card can fetch this company's financials from GuruFocus (which brings every growth line
  // at once — revenue and shares are the same 192-company set). A portfolio ingests per-row in the
  // drill-down instead, so this is left undefined there.
  ingestIsin?: string;
  onIngested?: () => void;
  /**
   * The selected index blended the SAME way this card's own series is — one
   * `fundamental-blend-metrics` call in the tab, every code in it, so all three growth cards read
   * one fetch exactly as they read the company's own. Null = no benchmark selected.
   */
  benchMetrics?: MetricRow[] | null;
  benchLabel?: string | null;
  /** The INDEX's `blend_notes` — why a code its constituents carry drew no line. Distinct
   *  from `blendNotes`, which is the book's: the two answer the same question about two
   *  different sets of companies and must not be read for each other. */
  benchNotes?: Record<string, BlendNote>;
  /** Why the blend failed, if it did — so a missing overlay states its reason instead of looking
   *  like an index that happens to track this book exactly. See `benchNote`. */
  benchErr?: string | null;
}) {
  const [showHoldings, setShowHoldings] = useState(false);
  const [busy, setBusy] = useState(false);
  const [outcome, setOutcome] = useState<string | null>(null);
  const isRatio = cfg.kind === 'ratio';
  /** What this card calls ITS OWN line — read by the hover and by the legend, defined once so the
   *  two cannot drift. ⚠ `ownLabel`, not `own`: a Map called `own` already lives inside the
   *  `chartData` memo, and a component-scope `own` would be silently shadowed there. */
  const ownLabel = holdingsName ?? cfg.title;

  // A GuruFocus ingest outcome → a plain sentence. Every non-success is a real answer (see the
  // backend's `classify_fetch_outcome`), so it's stated, never swallowed.
  const explain = (status: string | undefined, detail: string | undefined, http: number) => {
    switch (status) {
      case 'unsubscribed': return 'Unsubscribed — this listing’s exchange is outside our GuruFocus subscription.';
      case 'no_data': return 'No data — GuruFocus has no fundamentals for this listing.';
      case 'not_equity': return 'Not an equity — fundamentals don’t apply (bond / fund / derivative).';
      case 'not_found': return 'Not found — couldn’t resolve this ISIN to a GuruFocus listing.';
      case 'error': return detail || 'Fetch failed.';
      default: return detail || status || `HTTP ${http}`;
    }
  };

  const ingest = async () => {
    if (!ingestIsin) return;
    setBusy(true); setOutcome(null);
    try {
      const r = await apiFetch(`${API_URL}/api/earnings/fundamental-coverage/ingest`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ isin: ingestIsin, name: holdingsName ?? undefined }),
      });
      const j = (await r.json().catch(() => null)) as { status?: string; detail?: string } | null;
      if (r.ok && j?.status === 'ingested') {
        // Financials landed (revenue AND shares come together) → reload the tab; the chart appears.
        onIngested?.();
        return;
      }
      // Anything else is a stated reason, and we do NOT reload — nothing changed, so keep it visible.
      setOutcome(explain(j?.status, j?.detail, r.status));
    } catch (e) {
      setOutcome(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  /** See `extractPoints` — the x unit, and why it has to be a year. */
  const reported = useMemo(
    () => extractPoints(metrics ?? [], cfg.codes, cadence), [metrics, cfg, cadence]);
  /**
   * The LTM point that extends an ANNUAL chart past its last full year — see `ltmPoint`.
   *
   * ⚠ ANNUAL ONLY. In quarterly view every point already IS a trailing twelve months, so the newest
   * one needs no special name and appending it would duplicate the last column.
   *
   * ⚠ AND ONLY WHEN IT IS NEWER THAN THE LAST REPORTED YEAR. The server already refuses to emit one
   * that coincides with a fiscal year-end, but a stale payload could still carry a row the annual
   * series has since caught up with — two points on one x, and the reader cannot tell which is which.
   */
  const ltm = useMemo(() => {
    if (cadence !== 'annual') return null;
    const last = reported.length ? reported[reported.length - 1] : null;
    const p = ltmPoint(metrics ?? [], cfg.codes, last);
    // ⚠ `ltmYearX` already clamps it INSIDE the year after the last reported one, so this can
    // no longer be a same-x collision — it stays as the guard against a stale payload whose
    // LTM the annual series has since caught up with.
    return p && (last == null || p.year > last.year) ? p : null;
  }, [metrics, cfg, cadence, reported]);
  const points = useMemo(() => (ltm ? [...reported, ltm] : reported), [reported, ltm]);

  /** ⚠⚠ THE INDEX GETS ITS LTM THROUGH THE SAME HELPER AS THE COMPANY, which is the only reason
   *  the two land on the same x. The blend stamps its LTM point with the newest constituent filing
   *  behind it (not with today), so for ASML both sit on 2026-06-30 → 2026.25. Without it the
   *  company line ran a quarter past an index line that simply stopped, and the gap read as
   *  outperformance in a period the index did not cover. */
  const benchLtm = useMemo(
    () => {
      if (cadence !== 'annual' || !benchMetrics) return null;
      // ⚠ MEASURED FROM THE INDEX'S OWN LAST FISCAL YEAR. Placing it a fraction past OUR last
      // year would put the two lines' LTM points at x positions that differ by whatever the
      // two books' fiscal calendars differ by — a gap that is pure bookkeeping, drawn as if it
      // were time.
      const bench = extractPoints(benchMetrics, cfg.codes, cadence);
      return ltmPoint(benchMetrics, cfg.codes, bench.length ? bench[bench.length - 1] : null);
    },
    [benchMetrics, cfg, cadence]);

  /** The index's own series, through the IDENTICAL extraction, and left RAW.
   *
   *  ⚠ NO LONGER SCALED ONTO OURS. It used to go through `rebaseOnto`, which stretched the index
   *  to meet this company's absolute level; now `rebaseSeries` indexes BOTH lines to 100 on their
   *  shared anchor, so pre-scaling here would transform the benchmark twice and the second scale
   *  would silently cancel the first. `rebaseOnto` still exists for callers that plot an absolute
   *  axis. A ratio card needs neither: both lines are already the same unit (%). */
  const benchByX = useMemo(() => {
    if (!benchMetrics) return null;
    const raw = new Map<number, number | null>(
      extractPoints(benchMetrics, cfg.codes, cadence).map((p) => [p.year, p.value]));
    if (benchLtm) raw.set(benchLtm.year, benchLtm.value);
    return raw.size ? raw : null;
  }, [benchMetrics, cfg, cadence, benchLtm]);

  /**
   * Every x on this chart that carries a trailing-twelve-month point — OURS **AND** THE INDEX'S.
   *
   * ⚠⚠ KEYED ON THE COMPANY'S LTM ALONE, THE TICK LIED ABOUT THE INDEX'S. An LTM point sits on a
   * QUARTER-END x (2026-06-30 → 2026.25) while every other point on an annual chart sits on a whole
   * year, so an LTM the tick formatter does not recognise falls through to `xToPeriod` and renders
   * as **"2026 Q2"** — a fiscal quarter, on an axis that has none, in the one place a reader is
   * looking for the newest figure. Measured 2026-08-14 on the ACWI overlay of `EPS (excl.
   * non-recurring)`: the portfolio blend emitted no LTM row at all (`_ltm_blend_rows`, fixed
   * server-side), so `ltm` was null, nothing could ever match, and the index's own LTM was labelled
   * a quarter nobody reported.
   *
   * ⚠ A SET, NOT THE COMPANY'S VALUE OR-ELSE THE INDEX'S. The two are stamped with the newest
   * filing behind each blend, so a book whose holdings have all reported Q2 while the index has not
   * (or the reverse) genuinely has two LTM windows — and both are LTMs. Labelling only one of them
   * would put the fake quarter back on the other.
   */
  const ltmXs = useMemo(() => {
    const xs = new Set<number>();
    if (ltm) xs.add(ltm.year);
    if (benchLtm) xs.add(benchLtm.year);
    return xs;
  }, [ltm, benchLtm]);
  /** ⚠ SAID OUT LOUD, because two ticks both reading "LTM" otherwise looks like a rendering bug
   *  rather than the fact it is: the two lines' trailing years end on different quarters, so they
   *  are not measured over the same twelve months and the gap between them is partly calendar. */
  const ltmSplit = ltmXs.size > 1;

  /**
   * ⚠⚠ THE AXIS IS INDEXED, THE HOVER IS ACTUAL. A level card plots BOTH lines rebased to 100 at
   * the first year they share, so a company and an index are compared on their growth — the only
   * thing they have in common — while the tooltip still reads out the real number, so the level is
   * never lost. That is what lets `Shares outstanding` be both "is this company diluting?" and
   * "15,004.7M shares", which the absolute-only axis could not do against a benchmark.
   *
   * ⚠ A RATIO IS NEVER REBASED. Margins and ROIC are already the same unit on both lines; indexing
   * a percentage to 100 would destroy the one axis that is directly readable.
   *
   * ⚠ AND IT FALLS BACK TO ABSOLUTE RATHER THAN GUESSING. `rebaseSeries` refuses when there is no
   * shared year with both values positive; the raw series is still true, just not comparable, so
   * that is what gets drawn (and `indexed` says so, for the axis label and the tooltip).
   */
  /**
   * ⚠⚠ THE SERIES CHANGES SIGN — SO IT CANNOT BE AN INDEX ON A LOG AXIS, AND SAYING SO IS THE FIX.
   *
   * EPS and FCF/share go negative; revenue does not. Before this, a loss year was dropped TWICE
   * over: `rebaseSeries` refuses to index (100 × v/−2 inverts the curve), the card fell back to
   * absolute values and said so in the legend — and then plotted them on a LOG axis, where
   * `chartData` nulls everything ≤ 0. The fallback promised the real numbers and then hid exactly
   * the ones that triggered it. Measured: AMD's 2015–16 losses and Intel's 2024 were invisible on
   * both paths, so a reader saw a line that simply began late with no indication why.
   *
   * A sign change is a fact about the business, not a gap. On a LINEAR absolute axis it draws — a
   * negative point below a zero line — which is the truth and needs no ratio invented for it.
   */
  /**
   * The analysts' forecast for a line, as a series that CONTINUES it rather than a second one.
   *
   * ⚠⚠ IT IS SEEDED WITH THE LAST REPORTED POINT, and that is what makes the join honest rather
   * than cosmetic. Two separate recharts `<Line>`s never connect, so without a shared point the
   * dotted leg would float in mid-air, starting at FY2026 with a visible gap after FY2025 — which
   * reads as a break in the data. The seed is not a forecast value: it is the ACTUAL, drawn once
   * more, so the dotted line leaves the solid one exactly where the solid one ends.
   *
   * ⚠ FROM THE LAST **REPORTED** YEAR, NOT THE LTM. The estimates are per forward FISCAL year, so
   * FY2026's consensus continues FY2025 — not a trailing year that happens to end in March 2026.
   * Seeding from the LTM would draw the consensus as growth from a different, overlapping window.
   */
  const forecastOf = (rows: MetricRow[] | null | undefined,
                      actual: { year: number; value: number }[],
                      ltmPt: { year: number; value: number } | null) => {
    if (!cfg.forecastCodes?.length || cadence !== 'annual' || !rows) return null;
    const pts = extractPoints(rows, cfg.forecastCodes, 'annual');
    // ⚠⚠ THE SEED IS THE NEWEST ACTUAL, WHICH IS THE **LTM** WHENEVER THERE IS ONE. Leaving from
    // the last fiscal year instead would draw the dotted leg straight through the LTM point,
    // as though the trailing year were not on the way from one to the other — and the LTM is
    // precisely the most recent thing we know. Chronologically it sits between the two.
    const last = ltmPt ?? (actual.length ? actual[actual.length - 1] : null);
    // Only points strictly PAST the last reported year — a "forecast" of a year already filed is
    // a stale estimate, and drawing it over the actual invites reading the gap as a surprise.
    const fwd = pts.filter((p) => last == null || p.year > last.year);
    if (!fwd.length) return null;
    return new Map<number, number | null>(
      [...(last ? [[last.year, last.value] as const] : []), ...fwd.map((p) => [p.year, p.value] as const)]);
  };
  const forecastByX = useMemo(
    () => forecastOf(metrics, reported, ltm),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [metrics, reported, ltm, cfg, cadence]);
  const benchForecastByX = useMemo(
    // ⚠ THE BENCHMARK'S OWN ACTUALS ARE ITS SEED, not ours — a dotted index leaving OUR last point
    // would draw the two lines as continuous when they are two different series.
    () => forecastOf(benchMetrics, extractPoints(benchMetrics ?? [], cfg.codes, cadence), benchLtm),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [benchMetrics, benchLtm, cfg, cadence]);

  /**
   * Where each forecast leg is SEEDED — its lowest x, which is the newest actual drawn a second
   * time so the striped line leaves the solid one instead of floating.
   *
   * ⚠⚠ THAT SEED IS A DUPLICATE, AND THE HOVER MUST NOT REPEAT IT. It is the LTM's own value, so
   * hovering the LTM listed it twice — once as the book and again as "the book — analyst est." at
   * an identical number, which reads as analysts forecasting the past, or worse, as two sources
   * agreeing. The point has to exist for the geometry and has to be silent in the tooltip; only the
   * FORECAST years are things anyone estimated.
   */
  const forecastSeedX = useMemo(
    () => (forecastByX ? Math.min(...forecastByX.keys()) : null), [forecastByX]);
  const benchForecastSeedX = useMemo(
    () => (benchForecastByX ? Math.min(...benchForecastByX.keys()) : null), [benchForecastByX]);

  // ⚠ ON `points`, NOT ON THE FORECAST. A consensus that dips negative is an expectation, not a
  // loss the company made, and letting it flip this card to a linear absolute axis would restate
  // the whole history because of something nobody has reported.
  const crossesZero = !isRatio && seriesCrossesZero(points.map((p) => p.value));
  /** Linear axis + absolute values: a ratio (already comparable, can be negative) or a level series
   *  that changes sign. Everything else keeps the indexed log axis. */
  const linear = isRatio || crossesZero;
  const { indexed, ownByX, benchRawByX } = useMemo(() => {
    const own = new Map(points.map((p) => [p.year, p.value as number | null]));
    // ⚠ NO REBASE WHEN IT CROSSES ZERO, even though `rebaseSeries` would refuse anyway on its own:
    // deciding it here keeps "which axis" and "indexed or not" one decision instead of two that
    // could disagree — which is precisely how the log-axis-under-absolute-values bug survived.
    if (linear) return { indexed: null, ownByX: own, benchRawByX: benchByX };
    return { indexed: rebaseSeries(own, benchByX), ownByX: own, benchRawByX: benchByX };
  }, [points, linear, benchByX]);

  /** ⚠ THE FOURTH ABSENCE, WHICH ONLY THE LEVEL CARDS HAVE: the two series may share no year where
   *  both values are positive, and `rebaseSeries` then refuses rather than inventing a base. The
   *  card still draws — in absolute units, which is the honest fallback — so this says which basis
   *  is on screen instead of reporting an empty series. */
  const note = benchLabel
    // ⚠ `false` — THIS CARD APPLIES NO FLOOR. `benchByX` is the blended rows as they arrived; the
    // coverage decision was made on the server. Claiming the floor here is a diagnosis this
    // component cannot make — see `benchNote`.
    ? benchNote({ universe: benchLabel, cadence }, benchMetrics, benchErr ?? null, benchByX, false)
      ?? (benchByX && !isRatio && !indexed
        ? `${benchLabel}: no year in common with a positive value — showing absolute, not indexed`
        : null)
    : null;

  // Present only when the blend saw this metric and still drew nothing — the one case where
  // "not ingested" would be false.
  const blendNote = noteFor(blendNotes, cfg.codes);
  /**
   * Why the INDEX has no forecast leg, in one short clause.
   *
   * ⚠ THE SERVER'S OWN REASON WHERE THERE IS ONE. `explain_empty` already measured which cause
   * dominated — a coverage floor, a non-positive rebase base — and restating it here from what the
   * client can see would be a second, guessing implementation of a diagnosis the blend already made.
   * The fallback is deliberately vague ("no consensus for enough of its members"), because when the
   * server recorded nothing the honest answer is that we do not know which it was.
   */
  const benchForecastWhy = useMemo(() => {
    const n = cfg.forecastCodes ? noteFor(benchNotes, cfg.forecastCodes) : undefined;
    return n ? whyNoLine(n) : 'no consensus for enough of its members to blend.';
  }, [benchNotes, cfg]);

  // ⚠⚠ FITTED ON THE REPORTED YEARS, NOT ON `points` — the LTM point is deliberately out. The
  // interval into it is a quarter or two, not a year; a log-linear regression that treats it as a
  // full period reads that stub as a year of growth, and both the trend line and the CAGR headline
  // (which IS this slope — see the file header) come out overstated. It is drawn, not fitted.
  // ⚠⚠ FITTED ON THE REPORTED YEARS, NOT ON `points` — the LTM point is deliberately out. The
  // interval into it is a quarter or two, not a year, and `logLinearFit` treats every x-step as one
  // unit; including it reads that stub as a year of growth. The CAGR headline IS this slope (see the
  // file header), so both the trend line and the number above it would come out overstated.
  const fit = useMemo(() => logLinearFit(reported), [reported]);        // growth only
  /**
   * The headline CAGR — POINT TO POINT, the same `endpointCagr` the Tables tab measures with.
   *
   * ⚠⚠ IT IS NOT `fit.cagr`, AND THAT IS THE WHOLE CHANGE. This tile used to report the fitted
   * exponential's slope, so the same book's FCF/share read 29.7% here and 30.1% in the Tables tab
   * — a 0.4pp gap that is a MODELLING difference, not a data one, and nothing on either screen
   * said so. Worse, it is not a fixed offset: it is however far the endpoint years sit off the
   * trend, so it moves per company and per metric.
   *
   * ⚠ `fit` IS STILL COMPUTED AND STILL DRAWN — R² and the trend overlay are the fit, and they are
   * the right thing to keep it for: "how steady" is a question about a model, "what was the rate"
   * is a question about two reported numbers. So the trend line on the chart may sit slightly off
   * the two points this number connects, which is not a discrepancy — it is what R² measures.
   *
   * ⚠ FED `reported`, NOT `points` — the LTM stub is out, exactly as it is out of the fit. The
   * interval into it is a quarter or two, so ending a "per annum" rate there overstates it by the
   * fraction of the year that has not happened.
   */
  const ptp = useMemo(() => endpointCagr(reported, (x) => periodTick(x, ltmXs)),
                      [reported, ltmXs]);
  const avg = points.length ? points.reduce((a, p) => a + p.value, 0) / points.length : null;  // ratio only
  const latest = points.length ? points[points.length - 1].value : null;

  const chartData = useMemo(() => {
    const trendByYear = new Map(fit.trend.map((t) => [t.year, t.value]));
    // What the LINES use: the indexed maps when we could anchor, the raw ones when we could not.
    const plotOwn = indexed?.own ?? ownByX;
    const plotBench = indexed ? indexed.bench : benchRawByX;
    // The trend is fitted on the RAW series, so it has to ride the same multiplier as the line it
    // belongs to — otherwise the dashed fit floats off its own data.
    const trendScale = indexed ? 100 / ((ownByX.get(indexed.anchor) as number)) : 1;
    const benchBase = indexed ? benchRawByX?.get(indexed.anchor) ?? null : null;
    const benchScale = benchBase && benchBase > 0 ? 100 / benchBase : 1;
    // ⚠ The x UNION, not our own periods: an index reaches back further than most books, and
    // clipping it to ours would redraw the benchmark's history whenever a holding changed.
    const xs = new Set<number>(points.map((p) => p.year));
    if (plotBench) for (const x of plotBench.keys()) xs.add(x);
    // The forecast reaches PAST every reported year, so it extends the axis rather than
    // filling it. Its seed x already exists (it is the last actual).
    for (const m of [forecastByX, benchForecastByX]) if (m) for (const x of m.keys()) xs.add(x);
    // ⚠ OFF THE **RAW** SERIES, NOT THE PLOTTED ONE. A rebase is one constant per series and
    // divides out of `v / prev`, so the step is the same number either way — but computing it on
    // the raw values means it cannot change when the axis flips to absolute on a sign change, and
    // it keeps the non-positive-base refusal looking at the real figures. See `stepChanges`.
    const ownStep = stepChanges(ownByX);
    const benchStep = benchRawByX ? stepChanges(benchRawByX) : null;
    // ⚠⚠ A FORECAST YEAR STEPS WITHIN ITS OWN SERIES. Read off the actual line's steps it has
    // none — that line stops at the last filing — so every estimate hovered as a bare dash,
    // which is the one thing a consensus is never short of: an expected growth rate. Because
    // the seed IS the newest actual, the first estimate's step is measured from it: FY2026e
    // against the LTM is "how much growth is priced in from where we actually are", which is
    // the number worth reading. FY2027e is then against FY2026e — expectation on expectation,
    // and the row says "analyst est." so it cannot be mistaken for a realised step.
    const fcStep = forecastByX ? stepChanges(forecastByX) : null;
    const bfcStep = benchForecastByX ? stepChanges(benchForecastByX) : null;
    return [...xs].sort((a, b) => a - b).map((year) => {
      const v = plotOwn.get(year) ?? null;
      const b = plotBench ? plotBench.get(year) ?? null : null;
      const t = trendByYear.get(year);
      // ⚠ THE SAME MULTIPLIER AS THE LINE IT CONTINUES. On a blend the server has already
      // rebased the forecast onto the ACTUAL it extends (`_FORECAST_BASE`), so it arrives on
      // the actual's scale; on a single company it is raw EPS, the same units as the actual.
      // Either way it takes this card's own rebase, never one of its own — a forecast
      // rebased independently restarts at 100 beside an actual at 1,800.
      // ⚠⚠ EACH FORECAST TAKES **ITS OWN** LINE'S MULTIPLIER. `rebaseSeries` divides each series by
      // its own value at the shared anchor, so ours and the index's are two different constants —
      // scaling the index's forecast by ours would leave it floating off the index it continues, at
      // whatever ratio the two happened to be at the anchor. That is a wrong number that still
      // draws a plausible line, which is the failure mode this card keeps removing.
      const fc = forecastByX?.get(year) ?? null;
      const bfc = benchForecastByX?.get(year) ?? null;
      const scaleFc = (x: number | null, k: number) => (x == null ? null
        : linear ? x : (x > 0 ? x * k : null));
      return {
        year,
        // Tooltip only, never plotted — the per-period move. The index ON the axis is already
        // cumulative growth since the anchor; this is the part it hides.
        step: ownStep.get(year) ?? null,
        benchStep: benchStep?.get(year) ?? null,
        forecastStep: fcStep?.get(year) ?? null,
        benchForecastStep: bfcStep?.get(year) ?? null,
        // The estimate in its own units — a single company only, same rule as `rawValue`: a
        // blend has no currency for it to be in.
        rawForecast: isAgg ? null : (forecastByX?.get(year) ?? null),
        // A log axis can't plot ≤ 0; a LINEAR one can, which is the whole point of `crossesZero`.
        value: linear ? v : (v != null && v > 0 ? v : null),
        // ⚠ NO TREND ON A SIGN-CHANGING SERIES. `logLinearFit` regresses ln(value) and silently
        // DROPS every non-positive point (it reports `dropped`, which nothing was reading) — so on
        // AMD it would fit a constant-growth exponential to 2017-2025 and draw it across 2015-16 as
        // though those years were on it. A dashed line through two losses, at full confidence.
        trend: linear ? null : (t != null ? t * trendScale : null),
        bench: linear ? b : (b != null && b > 0 ? b : null),
        // Carried for the tooltip only — never plotted.
        //
        // ⚠ ONLY A SINGLE COMPANY HAS ONE. A portfolio's series is ALREADY a blended index from
        // the backend (`currency: null` — there is no portfolio revenue), so its "raw" is just a
        // differently-anchored index; printing it beside ours would show two index numbers and
        // call one of them actual. Null here means the tooltip shows the index alone, which is
        // the whole truth available. The benchmark is always a blend, so it never has one.
        rawValue: isAgg ? null : (ownByX.get(year) ?? null),
        forecast: scaleFc(fc, trendScale),
        benchForecast: scaleFc(bfc, benchScale),
      };
    });
  }, [points, fit, linear, indexed, ownByX, benchRawByX, isAgg, forecastByX, benchForecastByX]);

  // Log axis: pad the domain (multiplicatively) so the min/max points + trend endpoints don't clip.
  const logDomain = useMemo(() =>
    paddedLogDomain(chartData.flatMap((d) => [d.value, d.trend, d.bench, d.forecast, d.benchForecast]).filter((v): v is number => v != null)),
  [chartData]);

  /**
   * ⚠⚠ AN INDEX IS A BARE NUMBER — 100, NEVER "100M" AND NEVER "EUR 100". `fmt` below is
   * UNIT-AWARE, and once the axis became an index it started dressing index values as the
   * quantity they were derived from: revenue and shares (unit `millions`/`shares`) fell through
   * to the B/T/M scaler and rendered an index of 100 as "100M", while `per_share` escaped only by
   * accident of having its own branch. A reader has no way to tell that apart from a real amount.
   * So anything ON the indexed axis goes through here instead, and the currency prefix is dropped
   * with it — the actual amounts live in the hover, which is the point of the split.
   */
  const fmtIndex = (v: number | null | undefined, dp = 0) => (v == null ? '—' : v.toFixed(dp));

  const fmt = (v: number | null | undefined) => {
    if (v == null) return '—';
    if (cfg.unit === 'percent') return `${v.toFixed(1)}%`;
    if (isAgg) return v.toFixed(1);                      // blended growth index
    if (cfg.unit === 'per_share') return v.toFixed(2);
    // 'millions' (currency) and 'shares' (a count) both scale B/T/M; only the prefix differs.
    const a = Math.abs(v);
    if (a >= 1e6) return `${(v / 1e6).toFixed(2)}T`;
    if (a >= 1e3) return `${(v / 1e3).toFixed(1)}B`;
    return `${v.toFixed(0)}M`;
  };
  // A share count carries no currency, so no ccy prefix on 'shares' (nor on a % ratio).
  const ccy = !isAgg && currency && cfg.unit !== 'percent' && cfg.unit !== 'shares' ? `${currency} ` : '';
  // ⚠ THE `cagr` FORMATTER IS GONE WITH THE FIT IT FORMATTED. It took a FRACTION (`fit.cagr`,
  // 0.297) while `endpointCagr` returns PERCENT (29.7) — leaving it here is a ×100 waiting for the
  // next caller that reaches for the obvious-looking helper.

  return (
    <div className="rounded-xl border border-neutral-800/40 bg-card p-4 space-y-3 min-w-0">
      <h4 className="text-base font-semibold text-fg-strong">{cfg.title}</h4>

      {metrics == null ? (
        <p className="text-xs text-fg-subtle py-16 text-center">Loading…</p>
      ) : points.length === 0 && isAgg && blendNote ? (
        // ⚠ THE HOLDINGS HAVE IT AND THE BLEND COULD NOT DRAW IT. Offering "fetch financials" here
        // would send the reader to spend GuruFocus quota on data that is already in the database.
        // State the fact and the reason, and open the per-holding table instead.
        <div className="py-16 flex flex-col items-center gap-2 text-center px-2">
          <p className="text-[12px] text-fg-soft">{reportingLine(blendNote, cfg.noun)}.</p>
          <p className="text-[12px] text-fg-faint">No portfolio line: {whyNoLine(blendNote)}</p>
          <button type="button" onClick={() => setShowHoldings(true)}
            className="text-xs px-3 py-1 rounded-lg border border-neutral-700 text-fg-soft hover:bg-overlay/5">
            See per holding
          </button>
        </div>
      ) : points.length === 0 && isAgg ? (
        // A portfolio has no single ISIN to fetch — it has N of them, each with its own outcome.
        // The panel runs them one at a time and REPORTS per holding; it stays on screen until the
        // reader chooses "Reload", so a partial success can't hide the holdings that came back
        // empty. Reload is metrics-only: the derived cards' inputs are untouched by this run.
        <HoldingsIngestPanel target={holdingsTarget} metric={cfg.benchmarkMetric}
          noun={cfg.noun} onIngested={onReloadMetrics ?? onIngested} />
      ) : points.length === 0 ? (
        <div className="py-16 flex flex-col items-center gap-3 text-center px-4">
          <p className="text-[12px] text-fg-faint">No {cfg.noun} ingested for this company.</p>
          {ingestIsin && (busy ? (
            <span className="text-xs text-fg-subtle">Fetching from GuruFocus…</span>
          ) : outcome ? (
            <>
              <p className="text-xs text-warn-300 max-w-[28ch]">{outcome}</p>
              <button type="button" onClick={ingest}
                className="text-[12px] px-2 py-0.5 rounded-lg border border-neutral-700 text-fg-soft hover:bg-overlay/5">
                Try again
              </button>
            </>
          ) : (
            <button type="button" onClick={ingest}
              title="Fetch this company's financials from GuruFocus."
              className="text-xs px-3 py-1 rounded-lg border border-accent-600/40 text-accent-400 hover:bg-overlay/5">
              Fetch financials from GuruFocus
            </button>
          ))}
        </div>
      ) : (
        <>
          <div className="flex flex-wrap gap-2">
            {isRatio ? (
              <>
                <Stat label="Avg" value={fmt(avg)} color={chartTheme.accent}
                  info={<InfoTip content={<AspectCard
                    what={`Average ${cfg.noun} over the years shown.`}
                    where="Computed here from the points below." when={`${points.length} year(s).`}
                    how="A simple mean — a ratio doesn't compound, so there's no growth rate." />} />} />
                {/* ⚠ THE TILE IS RENAMED WHEN IT IS SHOWING THE LTM POINT. `latest` reads the last
                    plotted value, so on an annual chart that has one it is a trailing-twelve-month
                    figure, not a fiscal year — and "Latest" over a number nobody filed is the kind
                    of quiet mislabel this tab keeps removing. */}
                {/* ⚠ RENAMED WHEN IT IS SHOWING THE LTM POINT. `latest` reads the last plotted
                    value, so on a chart that has one it is a trailing-twelve-month figure, not a
                    fiscal year — and "Latest" over a number nobody filed is the quiet mislabel this
                    tab keeps removing. */}
                <Stat label={ltm ? 'LTM' : 'Latest'} value={fmt(latest)} color={chartTheme.accent}
                  info={ltm ? <InfoTip text={'The trailing twelve months to the newest quarterly '
                    + 'filing — past the last full fiscal year, so it is drawn as an extra point at '
                    + 'its real position on the axis. It is NOT in the trend fit or the CAGR: the '
                    + 'interval into it is a quarter or two, and regressing it as a full year would '
                    + 'overstate the growth rate.'} /> : undefined} />
              </>
            ) : (
              <>
                {/* ⚠⚠ WITHHELD WHEN THE SERIES CROSSES ZERO, NOT COMPUTED FROM THE GOOD YEARS.
                    `logLinearFit` regresses ln(value) and DROPS every non-positive point — it even
                    returns `dropped`, which nothing was reading. So on AMD it would have printed a
                    CAGR fitted to 2017-2025 while the tile said "over 9 years", with the two loss
                    years silently excluded from a number describing the whole chart. A dash that
                    explains itself beats a plausible figure measured over a period nobody chose. */}
                <Stat label="R²" value={linear ? '—' : (fit.r2 == null ? '—' : fit.r2.toFixed(2))}
                  color={chartTheme.accent}
                  info={<InfoTip content={<AspectCard
                    what={`How tightly ${cfg.noun} hugs a constant-growth line (0–1).`}
                    where={linear
                      ? 'Not computed — this series changes sign.'
                      : 'Computed here — a log-linear regression on the points below.'}
                    when={linear
                      ? `${fit.dropped} of ${reported.length} year(s) are zero or negative.`
                      : `Over the ${fit.n} year(s) shown.`}
                    how={linear
                      ? 'A constant-growth exponential has no logarithm at or below zero, so it '
                        + 'cannot describe a series that crosses it. Fitting the positive years '
                        + 'alone would measure a period nobody chose and label it as the whole.'
                      : `R² of ln(${cfg.noun}) vs year. 1.0 = perfectly steady compounding; low = lumpy or cyclical.`} />} />} />
                {/* ⚠⚠ POINT TO POINT, THE SAME `endpointCagr` THE TABLES TAB USES — see `ptp`. The
                    tile next to it (R²) is still the fit, so the two now answer different
                    questions on purpose: what the rate WAS, and how steadily it got there. */}
                <Stat label="CAGR"
                  value={linear || ptp.pct == null ? '—' : `${ptp.pct >= 0 ? '+' : ''}${ptp.pct.toFixed(1)}%`}
                  color={chartTheme.accent}
                  info={<InfoTip content={<AspectCard
                    what={`The compound annual growth of ${cfg.noun}, first reported period to last.`}
                    where={linear ? 'Not computed — see R².'
                      : ptp.pct == null ? 'Not computed.'
                      : `Computed here from the two endpoints — the same measure, and the same `
                        + `function, the Tables tab reports. The two agree by construction.`}
                    when={linear ? 'Undefined across a sign change.'
                      : ptp.pct == null ? ptp.reason
                      : `${ptp.from} → ${ptp.to}, ${ptp.years} year(s).`}
                    how={linear
                      ? 'Growth from a negative base is not a percentage: −1 → +2 is not "+300%" '
                        + 'in any sense that compounds, and −2 → −1 would read as +50% growth for '
                        + 'a company still making a loss.'
                      : '(end ÷ start) ^ (1 ÷ years) − 1. ⚠ NOT the slope of the fitted trend beside '
                        + 'it: that smooths the endpoints, and how far it differs from this IS what '
                        + 'the R² is telling you. Only these two periods matter here, so one '
                        + 'unrepresentative year at either end moves it.'} />} />} />
              </>
            )}
          </div>

          <div>
            <ResponsiveContainer width="100%" height={320}>
              <ComposedChart data={chartData} margin={{ top: 5, right: 12, bottom: 5, left: 4 }}
                style={{ cursor: 'pointer' }}
                onClick={() => setShowHoldings(true)}>
                <CartesianGrid strokeDasharray="3 3" stroke={chartTheme.gridEarnings} />
                <XAxis {...periodAxis((x: number) => periodTick(x, ltmXs))} />
                {linear ? (
                  // ⚠ LINEAR AND ABSOLUTE — a ratio, or a level series that changes sign. The
                  // units differ: a ratio ticks in %, a sign-changing level in its own amounts.
                  <YAxis tick={{ fontSize: 12, fill: chartTheme.axisTick }} width={isRatio ? 48 : 60}
                    tickFormatter={(v: number) => (isRatio ? `${v.toFixed(0)}%` : fmt(v))} />
                ) : (
                  // Log scale: an exponential (constant-%) growth trend draws as a straight line.
                  <YAxis scale="log" domain={logDomain ?? ['dataMin', 'dataMax']} allowDataOverflow
                    tick={{ fontSize: 12, fill: chartTheme.axisTick }}
                    tickFormatter={(v: number) => (indexed ? fmtIndex(v) : fmt(v))} width={60} />
                )}
                {/* ⚠⚠ THE HOVER NEVER PRINTS A BARE INDEX, AND WHAT IT PRINTS INSTEAD IS THE STEP —
                    the move from the period before, not the growth since the anchor. Since-anchor
                    is what the chart ALREADY SHOWS: both lines start at 100 together, so how far
                    apart they have drawn is that comparison, and restating it in the hover adds
                    nothing. Which line grew faster in the year under the cursor is invisible up
                    there — two lines can both be climbing steeply — and that is what the hover is
                    for. A single company keeps its ACTUAL value in front of the step; a blended
                    portfolio and a benchmark have none to keep (`currency: null` — a blend of
                    members each rebased to 100 has no currency it could be in), so they show the
                    step alone, which is real for a blend because the units divide out of a ratio.
                    When the rebase refused (a ratio card, or a level that changes sign), plotted IS
                    the real number and prints exactly as it did before indexing existed.
                    ⚠ `labelFormatter` IS NOT COSMETIC EITHER: without it the header is the raw x —
                    "2026.25", a year that does not exist — on the hover of the very point the
                    reader opened the chart to check. It is the SAME formatter as the axis tick, so
                    a point cannot be named one thing below the chart and another inside it.
                    ⚠ AND THE LTM HEADER CARRIES ITS WINDOW AND ITS AFFORDANCE. "LTM" alone names a
                    period the reader cannot look up — it is not on the axis anywhere else, it is
                    not a fiscal year, and the filings under it appear in no other view in the app.
                    Naming the quarter it ends in makes it locatable; the LTM ⓘ under the chart
                    carries the arithmetic. */}
                <Tooltip contentStyle={chartTheme.tooltipCard.contentStyle} labelStyle={{ color: chartTheme.axisLabel }} itemSorter={benchmarkFirst}
                  labelFormatter={(x) => (typeof x !== 'number' ? x
                    : !ltmXs.has(x) ? periodTick(x, ltmXs)
                      // ⚠ THE REAL QUARTER-END, NOT THE x. The LTM now sits a MEASURED fraction of a
                      // year past the last reported one, so its x is a position on the axis and no
                      // longer decodes to a date — `periodTick` would round 2025.24 to "2025 Q2",
                      // naming a quarter the window does not end in.
                      : `LTM · 12 months to ${(x === ltm?.year ? ltm?.date : benchLtm?.date)
                        ?? periodTick(x)}`)}
                  formatter={(v, name, item) => {
                    const row = item?.payload as {
                      year?: number;
                      rawValue?: number | null; rawForecast?: number | null;
                      step?: Step | null; benchStep?: Step | null;
                      forecastStep?: Step | null; benchForecastStep?: Step | null;
                    } | undefined;
                    const plotted = typeof v === 'number' ? v : null;
                    // ⚠ WHOSE LINE, NOT WHICH METRIC. The card's own heading already says
                    // "EPS (excl. non-recurring)"; repeating it here spent the row on something the
                    // reader can see and left the two lines distinguished only by colour. The
                    // benchmark row was always named this way — this is the other side of it.
                    // ⚠⚠ THE SEED POINT IS DROPPED FROM THE HOVER — see `forecastSeedX`. It is the
                    // newest ACTUAL, carried into the forecast series only so the striped line
                    // leaves the solid one; listed, it printed the LTM twice, the second time
                    // labelled as an estimate — which reads either as analysts forecasting the past
                    // or as two sources agreeing. Recharts drops a row when the formatter returns a
                    // non-array nullish value, and that is the only way to OMIT one: a row with an
                    // empty value still takes a line and still reads as a series with nothing to say.
                    const rowYear = row?.year;
                    if ((name === 'forecast' && rowYear === forecastSeedX)
                      || (name === 'benchForecast' && rowYear === benchForecastSeedX)) return null;
                    // ⚠ THE FORECAST ROWS SAY SO IN THE HOVER TOO. On screen they are striped; in a
                    // list of four labelled values, ink is not available and the word has to be.
                    const label = name === 'bench' ? (benchLabel ?? 'Benchmark')
                      : name === 'benchForecast' ? `${benchLabel ?? 'Benchmark'} — analyst est.`
                        : name === 'forecast' ? `${ownLabel} — analyst est.`
                          : ownLabel;
                    const since = pctSince(
                      name === 'bench' ? row?.benchStep
                        : name === 'forecast' ? row?.forecastStep
                          : name === 'benchForecast' ? row?.benchForecastStep
                            : row?.step, ltmXs);
                    const tail = since ? `  ·  ${since}` : '';
                    // A ratio is already in real units; so is the absolute fallback when the rebase
                    // refused. Both keep the number they always printed, with the step beside it.
                    if (isRatio || !indexed) return [`${ccy}${fmt(plotted)}${tail}`, label];
                    // Everything else on this chart is an INDEX — a bare "142.6" is unreadable
                    // without hunting the anchor year out of the legend, and it wears the shape of a
                    // real quantity. Only our own line, and only for a single company, has an actual
                    // value behind it; the benchmark and a blended portfolio do not (`currency:
                    // null` — a blend of members each rebased to 100 has no currency it could be
                    // in), and inventing one for them is how an index is read as an amount. So they
                    // show the step alone, which IS real for a blend: a ratio of two of the line's
                    // own points, units divided out.
                    // ⚠ A DASH ONLY WHERE THE ROW WOULD OTHERWISE BE EMPTY — the first point of a
                    // series has no previous period, and a blend has no value to fall back on.
                    // ⚠ A SINGLE COMPANY'S ESTIMATE HAS UNITS TOO — EUR 23.23 a share is the figure
                    // analysts actually published, and showing only its growth rate would hide the
                    // level on the one line that has one. A blend has no currency for it to be in,
                    // exactly as with `rawValue`.
                    const raw = name === 'value' ? row?.rawValue ?? null
                      : name === 'forecast' ? row?.rawForecast ?? null : null;
                    if (raw == null) return [since || '—', label];
                    return [`${ccy}${fmt(raw)}${tail}`, label];
                  }} />
                {/* ⚠ ON EVERY LINEAR AXIS, NOT JUST A RATIO'S. Zero is where a sign-changing level
                    changes meaning — profit above it, loss below — and without the line a small
                    negative reads as a small positive at a glance. */}
                {linear && <ReferenceLine y={0} stroke={chartTheme.zeroLine} />}
                {isRatio && avg != null && <ReferenceLine y={avg} stroke={chartTheme.accent} strokeDasharray="5 3" strokeOpacity={0.6} />}
                <Line dataKey="value" name="value" type="monotone" stroke={chartTheme.accent} strokeWidth={2} dot={{ r: 2.5 }} connectNulls />
                {/* ⚠ `tooltipType="none"` — OUT OF THE HOVER, ON PURPOSE. It is a fitted line, not
                    a measurement: its value at a point is what a constant-growth exponential says
                    should have happened, printed in the same ink and the same list as two figures
                    that did. The fit is already described where it belongs — the R² and CAGR tiles
                    above the chart, and the dashed stroke + legend below it. Recharts' default
                    tooltip drops any series typed `none`, so this is declared on the series rather
                    than filtered in the formatter (which can blank a row but not remove it). */}
                {!linear && <Line dataKey="trend" name="trend" tooltipType="none" type="monotone" stroke={chartTheme.warn} strokeWidth={2} strokeDasharray="5 3" dot={false} connectNulls />}
                {/* ⚠ ONE COLOUR FOR THE BENCHMARK ON ALL FOURTEEN CHARTS — green (`chartTheme.pos`).
                    It has to be the same everywhere or the eye re-learns which line is the index on
                    every card. Validated, not eyeballed (`dataviz/scripts/validate_palette.js`):
                    green↔the accent blue is ΔE 19.1 deutan / 20.7 normal.
                    ⚠ ON THIS CARD IT ALSO SITS BESIDE THE AMBER TREND, and green↔amber is ΔE 7.9
                    under protanopia — the 6–8 floor band, legal only with a second encoding. It has
                    two: the trend is DASHED where the benchmark is solid, and both are named. */}
                {/* ⚠ A STRIPED LINE WITH A DOT PER FORECAST YEAR, matching the solid line it
                    continues — the estimates ARE points (FY2026, FY2027, …), one consensus each,
                    and a fine dotted stroke with smaller markers read as an annotation rather than
                    as a series carrying values. The stripe is what says "expected"; the dot is what
                    says "this is a figure". `r` matches its own line, not the other one's.
                    ⚠⚠ `strokeDasharray: '0'` ON THE DOT IS LOAD-BEARING, NOT TIDYING. Recharts builds
                    each marker as `{r: 3, ...lineProps, ...dotProps}` — so the LINE's dash pattern
                    lands on the little circle's own outline and chops it into arcs. The markers came
                    out as broken rings while the solid line's were whole, which reads as a rendering
                    fault rather than as a forecast. The dot object is spread last, so overriding it
                    here is the fix. */}
                {forecastByX && <Line dataKey="forecast" name="forecast" type="monotone"
                  stroke={chartTheme.accent} strokeWidth={2} strokeDasharray="4 3"
                  dot={{ r: 2.5, strokeDasharray: '0' }} connectNulls />}
                {benchForecastByX && <Line dataKey="benchForecast" name="benchForecast" type="monotone"
                  stroke={chartTheme.pos} strokeWidth={2} strokeDasharray="4 3"
                  dot={{ r: 2, strokeDasharray: '0' }} connectNulls />}
                {benchByX && <Line dataKey="bench" name="bench" type="monotone" stroke={chartTheme.pos} strokeWidth={2} dot={{ r: 2 }} connectNulls />}
              </ComposedChart>
            </ResponsiveContainer>
            <div className="flex justify-center flex-wrap gap-x-4 gap-y-1 text-xs mt-1">
              <LegendItem color={chartTheme.accent} label={ownLabel} />
              {/* ⚠ ONE ENTRY PER LINE, EACH WEARING ITS OWN STROKE. The average used to be the words
                  "(avg dashed)" appended to the series entry, beside a SOLID swatch — a sentence
                  asking the reader to work out which mark on the chart it meant. */}
              {isRatio && avg != null && (
                <LegendItem color={chartTheme.accent} stroke="dashed" label={`${ownLabel} average`} />
              )}
              {!linear && (
                <LegendItem color={chartTheme.warn} stroke="dashed"
                  label={`Trend (R² ${fit.r2 == null ? '—' : fit.r2.toFixed(2)})`} />
              )}
              {/* ⚠⚠ DOTTED, AND NAMED "analyst est." — NOT A SECOND MEASUREMENT. Everything else on
                  this chart is something that happened; this is what people expect to happen, and
                  the two must not be able to be confused at a glance. It is dotted where the trend
                  is dashed and the series solid, it carries its own legend entry rather than a
                  parenthetical, and it is out of the fit, the CAGR and the tiles. ⚠ ITS COVERAGE IS
                  THIN — ~1,850 estimate rows against 39,327 actual ones — so on a book it is drawn
                  from whichever holdings analysts cover, gated by the blend's own coverage floor. */}
              {forecastByX && (
                <LegendItem color={chartTheme.accent} stroke="striped"
                  label={`${ownLabel} — analyst est.`} />
              )}
              {benchForecastByX && (
                <LegendItem color={chartTheme.pos} stroke="striped"
                  label={`${benchLabel} — analyst est.`} />
              )}
              {/* ⚠⚠ A REFUSED FORECAST MUST NAME ITSELF, OR A CORRECT ANSWER READS AS A BUG. Measured
                  2026-08-14: switching the benchmark from AEX to ACWI dropped the index's expectation
                  line — 22 of 22 AEX names carry a consensus against 351 of 1,715 ACWI names (20%),
                  far under the blend's coverage floor, so every forecast period is rightly withheld.
                  On screen that was a striped line on the book, none on the index, and no way to
                  tell "the index has no expectations" from "too few of its members are covered".
                  ⚠ ONLY WHEN WE ASKED AND IT DREW ITS ACTUAL — otherwise this would fire on a card
                  with no forecast configured, or on an index that failed to load at all. */}
              {cfg.forecastCodes?.length && benchByX && !benchForecastByX && (
                <span className="text-fg-faint"
                  title={'Analysts do not cover enough of this index for a blended consensus. The '
                    + 'floor is the same one every other line on this tab clears, and a forecast '
                    + 'drawn from a fifth of the constituents would be a confident line over a '
                    + 'fraction of the index it names.'}>
                  {benchLabel}: {benchForecastWhy}
                </span>
              )}
              {/* ⚠ THE AXIS CHANGED, SO IT SAYS SO. A reader who knows this card as an indexed log
                  chart would otherwise read absolute euros as an index. It also names WHY, because
                  "this company made a loss" is the finding, not a rendering detail. */}
              {crossesZero && (
                <span className="text-fg-faint"
                  title={'A level series that changes sign cannot be indexed (100 × v/−2 inverts '
                    + 'the curve) and cannot sit on a log axis. Shown as actual amounts on a linear '
                    + 'axis instead, with a zero line — so the loss years are visible rather than '
                    + 'missing. No trend or CAGR: a constant-growth exponential cannot describe a '
                    + 'series that crosses zero, and fitting one to the positive years only would '
                    + 'draw a confident line straight through the losses.'}>
                  absolute — the series crosses zero
                </span>
              )}
              {/* ⚠ TWO "LTM" TICKS ARE A FINDING, NOT A GLITCH — and unexplained they read as one.
                  Each blend stamps its trailing year with the newest filing behind it, so when the
                  book and the index end on different quarters the last stretch of the two lines is
                  measured over different twelve months. Silence there would let a reader take the
                  gap for performance. */}
              {ltmSplit && (
                <span className="text-fg-faint"
                  title={'The two lines’ trailing twelve months end on different quarters — '
                    + 'each is stamped with the newest filing behind it. The last point of one '
                    + 'therefore covers a slightly later year than the other, so part of the gap '
                    + 'between them at the right-hand edge is calendar, not performance.'}>
                  two LTM windows — different quarter-ends
                </span>
              )}
              {benchByX && (
                <LegendItem color={chartTheme.pos} label={benchLabel}
                  title={isRatio ? undefined
                    : indexed
                      ? `Both lines are indexed to 100 at ${indexed.anchor}, the first year they share. Only the growth is being compared — hover any point for the actual value.`
                      : 'Absolute values: the two series share no year with a positive value, so there is no honest base to index them on.'} />
              )}
              {note && (
                <span className="text-fg-faint" title="An overlay that simply does not appear is indistinguishable from an index that matches this book exactly. Full detail is in the console.">
                  {note}
                </span>
              )}
            </div>
          </div>
        </>
      )}

      {showHoldings && (
        <HoldingsRevenueModal target={holdingsTarget} metric={cfg.benchmarkMetric} unit={cfg.unit}
          noun={cfg.noun} portfolioName={holdingsName} onClose={() => setShowHoldings(false)}
          seriesLabel={cfg.title}
          benchLabel={benchByX ? benchLabel : null}
          benchTarget={benchLabel ? { universe: benchLabel, cadence } : null} />
      )}
    </div>
  );
}
