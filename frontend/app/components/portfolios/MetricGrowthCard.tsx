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
import { noteFor, reportingLine, whyNoLine, type BlendNote } from './blendNotes';
import { paddedLogDomain , xToPeriod } from './marginData';
import { benchNote, rebaseSeries, seriesCrossesZero } from './benchSeries';

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
  return [...byX.entries()].map(([year, v]) => ({ year, value: v.value })).sort((a, b) => a.year - b.year);
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
function ltmPoint(rows: MetricRow[], codes: string[]): { year: number; value: number } | null {
  // ⚠ EXTRACTED ON THE **QUARTERLY** AXIS. The row is dated to a quarter-end, and that is where it
  // belongs on the x: a June LTM sits at 2026.25, a quarter past the last full year, so the gap on
  // screen is the real interval. Bucketed as annual it would land on 2026 and claim a whole year.
  const ltm = extractPoints(rows, codes.map(
    (c) => `ltm__${c.slice(c.indexOf('__') + 2)}`), 'quarterly');
  return ltm.length ? ltm[ltm.length - 1] : null;
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
  /** Why the blend failed, if it did — so a missing overlay states its reason instead of looking
   *  like an index that happens to track this book exactly. See `benchNote`. */
  benchErr?: string | null;
}) {
  const [showHoldings, setShowHoldings] = useState(false);
  const [busy, setBusy] = useState(false);
  const [outcome, setOutcome] = useState<string | null>(null);
  const isRatio = cfg.kind === 'ratio';

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
    const p = ltmPoint(metrics ?? [], cfg.codes);
    const lastYear = reported.length ? reported[reported.length - 1].year : null;
    return p && (lastYear == null || p.year > lastYear) ? p : null;
  }, [metrics, cfg, cadence, reported]);
  const points = useMemo(() => (ltm ? [...reported, ltm] : reported), [reported, ltm]);

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
    // ⚠⚠ THE INDEX GETS ITS LTM THROUGH THE SAME HELPER AS THE COMPANY, which is the only reason
    // the two land on the same x. The blend stamps its LTM point with the newest constituent
    // filing behind it (not with today), so for ASML both sit on 2026-06-30 → 2026.25. Without it
    // the company line ran a quarter past an index line that simply stopped, and the gap read as
    // outperformance in a period the index did not cover.
    if (cadence === 'annual') {
      const bl = ltmPoint(benchMetrics, cfg.codes);
      if (bl) raw.set(bl.year, bl.value);
    }
    return raw.size ? raw : null;
  }, [benchMetrics, cfg, cadence]);

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

  // ⚠⚠ FITTED ON THE REPORTED YEARS, NOT ON `points` — the LTM point is deliberately out. The
  // interval into it is a quarter or two, not a year; a log-linear regression that treats it as a
  // full period reads that stub as a year of growth, and both the trend line and the CAGR headline
  // (which IS this slope — see the file header) come out overstated. It is drawn, not fitted.
  // ⚠⚠ FITTED ON THE REPORTED YEARS, NOT ON `points` — the LTM point is deliberately out. The
  // interval into it is a quarter or two, not a year, and `logLinearFit` treats every x-step as one
  // unit; including it reads that stub as a year of growth. The CAGR headline IS this slope (see the
  // file header), so both the trend line and the number above it would come out overstated.
  const fit = useMemo(() => logLinearFit(reported), [reported]);        // growth only
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
    // ⚠ The x UNION, not our own periods: an index reaches back further than most books, and
    // clipping it to ours would redraw the benchmark's history whenever a holding changed.
    const xs = new Set<number>(points.map((p) => p.year));
    if (plotBench) for (const x of plotBench.keys()) xs.add(x);
    return [...xs].sort((a, b) => a - b).map((year) => {
      const v = plotOwn.get(year) ?? null;
      const b = plotBench ? plotBench.get(year) ?? null : null;
      const t = trendByYear.get(year);
      return {
        year,
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
      };
    });
  }, [points, fit, linear, indexed, ownByX, benchRawByX, isAgg]);

  // Log axis: pad the domain (multiplicatively) so the min/max points + trend endpoints don't clip.
  const logDomain = useMemo(() =>
    paddedLogDomain(chartData.flatMap((d) => [d.value, d.trend, d.bench]).filter((v): v is number => v != null)),
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
  const cagr = (v: number | null) => (v == null ? '—' : `${v >= 0 ? '+' : ''}${(v * 100).toFixed(1)}%`);

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
                <Stat label="CAGR" value={linear ? '—' : cagr(fit.cagr)} color={chartTheme.accent}
                  info={<InfoTip content={<AspectCard
                    what="The compound annual growth rate of the fitted trend."
                    where={linear ? 'Not computed — see R².' : 'Computed here from the same fit.'}
                    when={linear ? 'Undefined across a sign change.' : `Over the ${fit.n} year(s) shown.`}
                    how={linear
                      ? 'Growth from a negative base is not a percentage: −1 → +2 is not "+300%" '
                        + 'in any sense that compounds, and −2 → −1 would read as +50% growth for '
                        + 'a company still making a loss.'
                      : 'e^(slope) − 1 of the log-linear regression.'} />} />} />
              </>
            )}
          </div>

          <div>
            <ResponsiveContainer width="100%" height={320}>
              <ComposedChart data={chartData} margin={{ top: 5, right: 12, bottom: 5, left: 4 }}
                style={{ cursor: 'pointer' }} onClick={() => setShowHoldings(true)}>
                <CartesianGrid strokeDasharray="3 3" stroke={chartTheme.gridEarnings} />
                <XAxis dataKey="year" tickFormatter={(x: number) => (ltm && x === ltm.year ? "LTM" : xToPeriod(x))}
                  tick={{ fontSize: 12, fill: chartTheme.axisTick }} />
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
                {/* ⚠ THE HOVER READS THE ACTUAL NUMBER, NOT THE AXIS. With the lines indexed, the
                    plotted value is "112.4" — true but not a fact about the company. The raw value
                    rides along on the row (`rawValue`/`rawBench`) and is what gets shown, with the
                    index in parentheses so the point on screen is still identifiable. When the
                    rebase refused, plotted IS raw and the parenthetical is dropped rather than
                    printed twice. A blended index has no raw value at all — `currency: null`,
                    there is no portfolio share count — so it correctly shows the index alone. */}
                <Tooltip contentStyle={chartTheme.tooltipCard.contentStyle} labelStyle={{ color: chartTheme.axisLabel }}
                  formatter={(v, name, item) => {
                    const row = item?.payload as { rawValue?: number | null } | undefined;
                    const plotted = typeof v === 'number' ? v : null;
                    const label = name === 'trend' ? 'Trend'
                      : name === 'bench' ? (benchLabel ?? 'Benchmark')
                        : cfg.title;
                    // A ratio is already in real units; so is the absolute fallback when the
                    // rebase refused. Both print exactly as they did before indexing existed.
                    if (isRatio || !indexed) return [`${ccy}${fmt(plotted)}`, label];
                    // Everything else on this chart is an INDEX. Only our own line, and only for a
                    // single company, has an actual value behind it — the trend, the benchmark and
                    // a blended portfolio do not, and inventing one for them is how an index comes
                    // to be read as an amount.
                    const raw = name === 'value' ? row?.rawValue ?? null : null;
                    if (raw == null) return [fmtIndex(plotted, 1), label];
                    return [`${ccy}${fmt(raw)}  (index ${fmtIndex(plotted, 1)})`, label];
                  }} />
                {/* ⚠ ON EVERY LINEAR AXIS, NOT JUST A RATIO'S. Zero is where a sign-changing level
                    changes meaning — profit above it, loss below — and without the line a small
                    negative reads as a small positive at a glance. */}
                {linear && <ReferenceLine y={0} stroke={chartTheme.zeroLine} />}
                {isRatio && avg != null && <ReferenceLine y={avg} stroke={chartTheme.accent} strokeDasharray="5 3" strokeOpacity={0.6} />}
                <Line dataKey="value" name="value" type="monotone" stroke={chartTheme.accent} strokeWidth={2} dot={{ r: 2.5 }} connectNulls />
                {!linear && <Line dataKey="trend" name="trend" type="monotone" stroke={chartTheme.warn} strokeWidth={2} strokeDasharray="5 3" dot={false} connectNulls />}
                {/* ⚠ ONE COLOUR FOR THE BENCHMARK ON ALL FOURTEEN CHARTS — green (`chartTheme.pos`).
                    It has to be the same everywhere or the eye re-learns which line is the index on
                    every card. Validated, not eyeballed (`dataviz/scripts/validate_palette.js`):
                    green↔the accent blue is ΔE 19.1 deutan / 20.7 normal.
                    ⚠ ON THIS CARD IT ALSO SITS BESIDE THE AMBER TREND, and green↔amber is ΔE 7.9
                    under protanopia — the 6–8 floor band, legal only with a second encoding. It has
                    two: the trend is DASHED where the benchmark is solid, and both are named. */}
                {benchByX && <Line dataKey="bench" name="bench" type="monotone" stroke={chartTheme.pos} strokeWidth={2} dot={{ r: 2 }} connectNulls />}
              </ComposedChart>
            </ResponsiveContainer>
            <div className="flex justify-center flex-wrap gap-x-4 gap-y-1 text-xs mt-1">
              <span className="flex items-center gap-1.5"><span className="w-3 h-0.5 inline-block rounded" style={{ background: chartTheme.accent }} />{cfg.title}{isRatio ? ' (avg dashed)' : ''}</span>
              {!linear && (
                <span className="flex items-center gap-1.5"><span className="w-3 h-0.5 inline-block rounded" style={{ background: chartTheme.warn }} />Trend (R² {fit.r2 == null ? '—' : fit.r2.toFixed(2)})</span>
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
              {benchByX && (
                <span className="flex items-center gap-1.5"
                  title={isRatio ? undefined
                    : indexed
                      ? `Both lines are indexed to 100 at ${indexed.anchor}, the first year they share. Only the growth is being compared — hover any point for the actual value.`
                      : 'Absolute values: the two series share no year with a positive value, so there is no honest base to index them on.'}>
                  <span className="w-3 h-0.5 inline-block rounded" style={{ background: chartTheme.pos }} />
                  {benchLabel}
                </span>
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
