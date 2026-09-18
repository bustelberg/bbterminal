'use client';

import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  CartesianGrid, ComposedChart, Line, ReferenceDot, ReferenceLine, ResponsiveContainer, Tooltip,
  XAxis, YAxis,
} from 'recharts';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { chartTheme } from '../../../lib/chartTheme';
import { useQuickValuationCopy } from './quickValuationCopy';
import { tiltedAxis } from '../../../lib/chartAxis';
import { AspectCard } from '../../../lib/tipCard';
import { workedMean, workedPriceCagr, workedRatio } from './workedFormula';
import InfoTip from '../InfoTip';
import { Stat } from './MetricGrowthCard';
import QuickValuationInputsModal from './QuickValuationInputsModal';
import PriceTargetCalculator from './PriceTargetCalculator';
import { meanOf, paddedDomain, paddedLogDomain } from './marginData';
import { logLinearFit, trendValueAt } from '../../../lib/trendFit';
import MultipleHistoryChart from './MultipleHistoryChart';
import {
  forwardSeries, since,
} from './multiplesSeries';
import {
  addYears, BASIS, cagrBetween, cagrOf, compoundFrom, dailyYieldHistory, latestDateOf, priceTarget, priceVsMetric,
  PRICE_CODES, rebase, yearsBetween, yieldOf, type Basis, type MetricRow,
} from './quickValuation';
import { runSSE } from '../../../lib/stream';
import { invalidateReadCache } from '../../../lib/readCache';
import { cancelJob, jobsStore, startLocalJob } from '../../../lib/stores/jobs';
// `2026-07-24` reads as a database key; a toast is read by a person. Shared with the Deep
// Valuation tab, whose forward-P/E button reports the same date the same way.
import { onDate } from './asOfLine';

/**
 * The "Quick Valuation" tab: a company's SHARE PRICE against its FREE CASH FLOW PER SHARE over the
 * last ten fiscal years, as one indexed chart with the two growth rates above it.
 *
 * The question: has the price followed the cash the business throws off per share, or has the
 * MULTIPLE done the work? Both give the same total return and they are not the same investment —
 * which is why the two CAGRs sit beside each other and nothing else competes with them.
 *
 *  Single company only — there is no portfolio share and no portfolio FCF per share (the amounts
 * are in different currencies and cannot be summed). The tab is not offered for a basket; the
 * portfolio-level version of this question is the FCF-SBC yield card, currency-free by design.
 *
 *  Every observed point is a fiscal year-end close, not today's. Both series come from the same
 * fiscal rows, which is what makes the comparison internally consistent — but the newest point can
 * be up to a year old, so the PICTURE is a read on the last reported year. The one exception is
 * where the dotted price-target line STARTS, which is today's price at today's position on the
 * axis; it is drawn dotted, in its own series, and named in the legend for exactly that reason.
 *
 *  The calculator's "CURRENT SHARE PRICE" IS THE LIVE ONE, AND IT IS A DIFFERENT NUMBER. It is
 * the newest yfinance close (`asset_price` — what /portfolios prices every model with), converted
 * into the reporting currency the fiscal rows are filed in. Before 2026-07-29 the row printed the
 * fiscal year-end close under the label "Current share price": up to a year old, with the FCF
 * yield and the CAGR hanging off it, both labelled "at today's price" and neither measured there.
 * When there is no priced Yahoo listing it falls back to the fiscal close AND SAYS SO — a stale
 * price shown as live is the failure this exists to prevent, so a silent fallback would recreate it.
 *
 *  Type on this tab is `rem`-SCALED (`text-xs` / `text-base` / `text-lg`), NEVER ABSOLUTE px, AND
 * That was the whole of why it looked wrong. This tab and its three children were authored in
 * `text-[12px]` / `text-[11px]` while `html { font-size: 19px }` (`globals.css`) makes every rem
 * step in the app land elsewhere: `text-xs` is 0.75rem = **14.25px**. So every caption, label,
 * input and annotation here rendered ~16–23% smaller than the identical thing one tab over, and
 * the tab mixed BOTH scales inside itself — `text-xs` values sat next to `text-[12px]` labels in
 * the same row of the price-target card, a 2.25px difference that reads as sloppy without ever
 * announcing what it is. Reported exactly that way: "kinda ugly right now for some reason."
 *
 *  It also broke the responsive knob. `globals.css` steps the base 19 → 17.5 → 16 → 15px so the
 * whole UI compacts proportionally on a narrower screen; an absolute px size opts out, so these
 * four files got relatively LARGER on a phone and smaller on a desktop — out of step in both
 * directions, from one decision. The comment on that knob already says it: "a per-component font
 * bump would scale that component out of step with the padding and borders around it."
 *
 *  Chart ticks are the exception and stay at 12. They are SVG `fontSize`, not CSS, and 12 is what
 * `lib/chartAxis.ts::tiltedAxis` defaults to for every chart in the Long Equity grid. Bumping them
 * here would put this tab's axes out of step with every other chart in the app — the opposite of
 * the problem being fixed.
 */

/**
 * How far the fitted trend is carried past the last reported year — and therefore the horizon
 * of EVERY forecast on this tab: the dotted projection on the chart, the forecast per-share
 * figure, the forecast share price, and the CAGR quoted against them.
 *
 *  RAISED 2 → 10 (2026-08-04). Ten years is the horizon the question is actually asked over,
 * and it makes the CAGR mean something: over two years the answer was dominated by the rerating
 * (today's yield to the assumed one) rather than by the business compounding.
 *
 *  It used to be the history window too — a matching `YEARS = 10` — so half the chart was
 * extrapolation. That is why the projected stretch is drawn as a separate, thinner, dotted series
 * and the panel's info card says in as many words that it is an extrapolation nobody forecast: a
 * decade of compounding an exponential fit is a big claim, and the chart must not let it read as
 * data.  THE HISTORY CAP IS GONE (see `priceVsMetric`) and this constant did NOT follow it: the
 * history is now every fiscal year the company reports, as on the Graphs tab, while the forecast
 * stays ten years. So the extrapolated share of the chart SHRINKS on a long history — which is
 * the direction this note was worried about — and never grows.
 */
const PROJECT_YEARS = 10;
/** All three charts share it, so the grid cells match without any card padding out the gap. */
const CHART_HEIGHT = 320;
/** Where the multiple-history chart opens. GuruFocus's forward-P/E indicator starts 2015-11-30 —
 *  earlier years would draw a trailing line with no forward beside it, which is the one comparison
 *  that chart exists to make. */
const MULTIPLE_FROM_YEAR = 2017;

/** `GET /api/asset-pipeline/latest-close/isin/{isin}` — the fields this tab reads. */
type LatestClose = {
  date: string; stale_days?: number; symbol?: string | null;
  close: number; currency: string;
  close_in?: number | null; in_currency?: string | null;
};

export default function QuickValuationTab({ isin, name }: { isin: string; name?: string | null }) {
  const [metrics, setMetrics] = useState<MetricRow[] | null>(null);
  const [basis, setBasis] = useState<Basis>('fcf');
  const [currency, setCurrency] = useState<string | null>(null);
  /**  THE ANSWER CARRIES THE QUESTION IT ANSWERED. "Have we looked yet?" is derived from whether
   *  the stored result belongs to THIS (isin, currency) — a separate `pending` boolean has to be
   *  flipped in four places (mount, company change, success, failure) and the row makes a claim
   *  about provenance in whichever one gets missed. */
  const [liveRes, setLiveRes] = useState<
    { isin: string; currency: string; data: LatestClose | null } | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [showInputs, setShowInputs] = useState(false);
  /**
   * The GuruFocus company behind this ISIN — already on the wire, previously discarded.
   *
   *  `/by-isin/{isin}/metrics` answers `{company_id, company_name, currency, metrics}` and this
   * tab kept only two of the four. Everything GuruFocus files is keyed by `company_id`, so
   * refreshing any of it looked like it needed a second lookup. It does not.
   */
  const [companyId, setCompanyId] = useState<number | null>(null);
  /**  ITS OWN HANDLE, so this button's spinner and Cancel cannot be driven by another job. */
  const [peJobId, setPeJobId] = useState<string | null>(null);
  const [refreshJobId, setRefreshJobId] = useState<string | null>(null);
  /**
   * The newest forward-P/E observation date the last `load` saw — what the toast reports.
   *
   *  A ref, not state, because the refresh reads it immediately after awaiting `load`. State set
   * inside that call is not visible in the same tick, so a `useState` here would always hold the
   * PREVIOUS date and the toast would say "unchanged" on the one run that actually moved it.
   */
  const fwdDateRef = useRef<string | null>(null);
  const jobs = jobsStore.use((st) => st.jobs);
  const peJob = peJobId == null ? null : jobs.find((jb) => jb.id === peJobId) ?? null;
  const refreshJob = refreshJobId == null ? null : jobs.find((jb) => jb.id === refreshJobId) ?? null;
  const peRefreshing = peJob?.status === 'running';
  const refreshingQuickValuation = refreshJob?.status === 'running';
  const peCancelling = peRefreshing && peJob.cancelRequested;

  /**
   * Read this company's metrics.  EXTRACTED SO THE ↻ CAN RE-RUN IT — the chart's forward series is
   * a `useMemo` over `metrics`, so replacing that array is what redraws the line. Nothing else in
   * the tab needs to know a refresh happened.
   *
   *  `blank` IS FALSE ON A REFRESH. Clearing the metrics first would blank the whole tab for the
   * length of the round trip, on a press whose entire purpose is to move ONE line.
   */
  const load = useCallback(async (blank: boolean, signal?: AbortSignal) => {
    //  Currency is cleared with the metrics. It gates the live-price fetch below; left behind
    // from the previous company it would convert this one's close at that one's currency.
    //  And so is `companyId`, FOR THE SAME REASON AND A SHARPER ONE: it is what the forward-P/E
    // ↻ POSTs to. Held over from the previous company, that button refreshes THAT company's
    // indicators, labels its toast with THIS one's name, then re-reads this one's metrics and
    // truthfully reports "nothing newer" — a write against the wrong company that looks like a
    // vendor with no update. Latent until the ISIN could change inside one mount, which is what
    // the modal's A/B valuation switch now does.
    if (blank) { setMetrics(null); setCurrency(null); setLiveRes(null); setCompanyId(null); }
    setErr(null);
    const r = await apiFetch(
      `${API_URL}/api/earnings/by-isin/${encodeURIComponent(isin)}/metrics?cadence=annual`,
      { signal });
    if (r.status === 404) { setMetrics([]); return; }
    const b = await r.json().catch(() => null);
    if (!r.ok) throw new Error(b?.detail ?? `HTTP ${r.status}`);
    //  A cancelled run must not write. A cacheable read is SHARED, so aborting one caller does not
    // stop the request; without this the tab repaints from a refresh the reader already stopped.
    if (signal?.aborted) return;
    const rows = (b?.metrics ?? []) as MetricRow[];
    setMetrics(rows);
    setCurrency(b?.currency ?? null);
    setCompanyId(typeof b?.company_id === 'number' ? b.company_id : null);
    //  The same builder the chart uses, on the same rows, so the toast's date and the As-of tile
    // can never name different points. Read here rather than off `forwardHistory`, which is a memo
    // computed during the NEXT render and therefore still the old series at this line.
    const fwd = forwardSeries(rows);
    fwdDateRef.current = fwd.length
      ? new Date(fwd[fwd.length - 1].t).toISOString().slice(0, 10) : null;
  }, [isin]);

  /**
   * Ask GuruFocus for this company's forward-P/E series again, then redraw the chart.
   *
   *  The stream returns a log, not a series, so the cache must be dropped between the write and
   * The re-read. The metrics payload is on `readCache`'s allowlist with a ten-minute TTL — without
   * `invalidateReadCache` the re-request is answered from memory with the series we just replaced,
   * the toast goes green and the line does not move, which is indistinguishable from a failed
   * fetch. The share-price button one tab over carries the same warning for the same reason.
   *
   *  `indicators` ALONE, AND `force=true`. One vendor call for the one series this button is
   * under; `refresh-all` is five and would move three other things nobody asked about. Unforced
   * skips a source GuruFocus already answered today — exactly the case somebody presses this in.
   */
  const refreshForwardPE = useCallback(() => {
    if (companyId == null) return;
    setPeJobId(startLocalJob(
      `${name ?? isin} — forward P/E`, 'quickval.forwardpe',
      async (signal) => {
        //  The stream's lines are discarded on purpose — they are the fetcher's own log
        // ("forward_pe_ratio: calling …"), noise in a one-line toast, and they say nothing about
        // whether the SERIES moved, which is the only question this button is pressed to answer.
        await runSSE(`${API_URL}/api/earnings/${companyId}/refresh/indicators?force=true`,
          { method: 'POST' }, () => {}, signal);
        if (signal.aborted) return 'cancelled';
        const before = fwdDateRef.current;
        invalidateReadCache('refreshed the forward P/E on Quick Valuation');
        await load(false, signal);
        const after = fwdDateRef.current;
        //  The toast reports the date, not "done". The twin of this button on the Deep Valuation
        // tab shipped saying "re-read" whatever happened, and the first thing it produced was a bug
        // report — "I refreshed it but it's still old" — because a green toast over an unmoved date
        // is indistinguishable from a fetch that failed. GuruFocus publishes this with a multi-week
        // lag, so "nothing newer" is the COMMON and CORRECT outcome and has to be sayable.
        if (after == null) return 'GuruFocus returned no forward P/E for this company';
        return after !== before
          ? `forward P/E now ${onDate(after)}`
          : `still ${onDate(after)} — GuruFocus has nothing newer`;
      }));
  }, [companyId, name, isin, load]);

  const refreshQuickValuation = useCallback(() => {
    if (companyId == null) return;
    setRefreshJobId(startLocalJob(
      `${name ?? isin} — Quick Valuation`, 'quickval.all',
      async (signal, report) => {
        const total = 5;
        let done = 0;
        report({ done, total, message: 'starting refresh' });
        await runSSE(`${API_URL}/api/earnings/${companyId}/refresh-all?force=true`,
          { method: 'POST' }, (raw) => {
            const e = raw as { type?: string; message?: string };
            if (e.type === 'info' && e.message?.startsWith('  Result:')) {
              done += 1;
              report({ done, total, message: `${done} of ${total} sources refreshed` });
            }
          }, signal);
        if (signal.aborted) return 'cancelled';
        invalidateReadCache('refreshed all Quick Valuation inputs');
        await load(false, signal);
        return 'Quick Valuation data refreshed';
      }));
  }, [companyId, name, isin, load]);

  useEffect(() => {
    let alive = true;
    void (async () => {
      try {
        //  `?cadence=annual` IS SPELT OUT ONLY SO THIS SHARES THE LONG EQUITY TAB'S PAYLOAD. It is
        // the server's default (`cadence != "quarterly"` runs the identical loader), so the URL is
        // a no-op on the wire — but the read cache keys on the URL, and the tab a reader lands on
        // asks for it explicitly. Same request, same key, no second 12,000-row download.
        await load(true);
      } catch (e) {
        if (alive) setErr(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { alive = false; };
  }, [isin, load]);

  /**
   * Today's close, in the currency the fiscal rows are filed in.
   *
   *  Not fetched at all without a known reporting currency, and not used unless the backend could
   * actually convert into it. A price in the wrong currency divided into a cash flow is a yield
   * wrong by the exchange rate — a number with no tell. A 404 (no priced Yahoo listing, no stored
   * bars) is not an error here: it is the fallback path, and the panel labels it.
   */
  useEffect(() => {
    if (!currency) return;
    let alive = true;
    void (async () => {
      let data: LatestClose | null = null;
      try {
        const r = await apiFetch(`${API_URL}/api/asset-pipeline/latest-close/isin/`
          + `${encodeURIComponent(isin)}?currency=${encodeURIComponent(currency)}`);
        if (r.ok) data = await r.json() as LatestClose;
      } catch { /* a network failure is the same answer as a 404 here: fall back, and say so */ }
      if (alive) setLiveRes({ isin, currency, data });
    })();
    return () => { alive = false; };
  }, [isin, currency]);

  const b = BASIS[basis];
  //  Two label sets, and mixing them inside one string is the bug to avoid. `t` / `bl` are the
  // TRANSLATED words for everything DRAWN on this tab; `b` keeps its ENGLISH ones and is used only
  // by the ⓘ cards, whose prose is not translated yet. An English sentence with a Dutch metric name
  // spliced into it — "the cash the business threw off per share" under `FCF per aandeel` — reads
  // as a rendering fault. See `quickValuationCopy`.
  const t = useQuickValuationCopy();
  const bl = t.basis[basis];
  //  No year cap — every fiscal year the company reports, which is what the Graphs tab draws
  // from this same payload. See the  on `priceVsMetric` for the year the old 10-year slice ate.
  const points = useMemo(() => priceVsMetric(metrics ?? [], b.codes), [metrics, b.codes]);
  const idx = useMemo(() => rebase(points), [points]);

  /**
   * The exponential trend through the per-share series, and its R².
   *
   *  Fitted on the index, which changes nothing. A log-linear fit is invariant to scaling —
   * ln(k·v) = ln k + ln v shifts the intercept and leaves the slope and R² alone — so this R² is a
   * property of the series itself, not of the base year the chart happens to use.
   *
   * A cash-burn (or loss) year has no logarithm and is dropped by the fit (`fit.dropped`), so the
   * trend spans fewer years than the chart when one occurs.
   */
  const fit = useMemo(
    () => logLinearFit(idx.rows
      .filter((r): r is { year: number; price: number | null; value: number } => r.value != null)
      .map((r) => ({ year: r.year, value: r.value }))),
    [idx]);

  /**
   *  The forecast assumption is derived **HERE**, ABOVE THE CHART DATA, AND THAT ORDER IS THE
   * POINT. It used to sit beside the calculator three hundred lines down, which was fine while the
   * only editable input was an end VALUE the chart does not draw. A growth RATE is different: it
   * is precisely what the dotted projection already claims, so leaving it below would state one
   * assumption in two places and let them disagree the moment anyone typed. `trendPsAt`'s own note
   * names that failure — "a calculator quoting a forecast the chart does not draw is exactly the
   * kind of drift the rest of this folder is built to prevent".
   *
   *  The calculator's inputs live here, not in it. The chart needs the same answers, and a
   * callback from a child during render is the cascading-update pattern React (and the lint rule)
   * rightly refuses. State up, values down.
   */
  const [fcfStr, setFcfStr] = useState<string | null>(null);
  const [yieldStr, setYieldStr] = useState<string | null>(null);
  /**
   * The growth rate the forecast per-share figure is reached BY — the same assumption as `fcfStr`,
   * entered the way people actually hold it.
   *
   *  One of the two is authoritative at a time, and setting either clears the other. They are two
   * views of one number, so both being live at once has no meaning — something would have to pick,
   * and whichever it picked the other box would sit on screen showing a figure the target was not
   * computed from. Mutual clearing is also what keeps each box's TEXT the user's own: derive one
   * from the other on every keystroke instead and typing "12.55" into the rate rounds through the
   * end value and comes back as "12.6" under the caret.
   *
   *  The derived side is still shown, live. Type a rate and the forecast figure moves with it (and
   * the reverse) — that is the whole point of the pair. Only the AUTHORITY moves, not the display.
   */
  const [cagrStr, setCagrStr] = useState<string | null>(null);
  /**
   * The three **CURRENT** FIGURES, NOW EDITABLE TOO (on request: "every one of these should be
   * adjustable by the user, except Forecast share price").
   *
   *  They are three quantities bound by one equation — `yield = per-share ÷ price` — SO ONLY TWO
   * Can ever be free. Three independently-typed boxes would let a reader state a per-share figure,
   * a price and a yield that do not satisfy their own identity, and the panel would then have to
   * silently ignore one of them: the yield row would print a number the two rows above it do not
   * produce. The forecast side of this panel already solved exactly this (`cagrStr`/`fcfStr`) and
   * this follows it rather than inventing a second convention.
   *
   *  The price is the free one; the per-share figure and the yield are one assumption two ways.
   * Typing a yield asks "what would the per-share figure have to be for the shares to yield that,
   * at this price" — which is a question. The alternative binding, deriving the PRICE from a typed
   * yield, is not: the price is the market's, it is the one live number on the panel, and the
   * Est. CAGR at the foot is measured FROM it, so a demanded yield would silently rewrite the
   * thing every return on the card is quoted against.
   *
   *  SO `curPsStr` AND `curYieldStr` CLEAR EACH OTHER, exactly as `cagrStr`/`fcfStr` do, and for
   * the same two reasons: only one can be authoritative, and mutual clearing is what keeps each
   * box's TEXT the user's own instead of round-tripping "1.85" back as "1.8" under the caret.
   *
   *  `curPriceStr` IS INDEPENDENT OF BOTH and survives a basis switch — a share price is not
   * denominated in FCF or EPS. The other two are, and `switchBasis` clears them.
   */
  const [curPsStr, setCurPsStr] = useState<string | null>(null);
  const [curPriceStr, setCurPriceStr] = useState<string | null>(null);
  const [curYieldStr, setCurYieldStr] = useState<string | null>(null);
  const asNum = (s: string | null) => {
    if (s == null || s.trim() === '') return null;
    const v = parseFloat(s);
    return Number.isFinite(v) ? v : null;
  };

  /**
   * The fitted trend at any year, converted back to per-share currency.
   *
   *  Converted from the plotted index, not re-fitted. A second `logLinearFit` on the raw series
   * would give the same slope (the fit is scale-invariant) but would be a second computation of
   * the same line — and a calculator quoting a forecast the chart does not draw is exactly the
   * kind of drift the rest of this folder is built to prevent. index/100 × the anchor's raw value.
   *
   *  One function, two callers. The calculator's forecast and the forward-multiple ladder both
   * read it; two copies of "index back to currency" is two places for the anchor to go missing.
   */
  const lastFitted = fit.trend.length ? fit.trend[fit.trend.length - 1].year : null;
  const anchorValue = idx.anchor == null ? null
    : points.find((p) => p.year === idx.anchor)?.value ?? null;
  const trendPsAt = (year: number) => {
    if (anchorValue == null) return null;
    const projected = trendValueAt(fit, year);
    return projected == null ? null : projected / 100 * anchorValue;
  };
  const forecastPs = lastFitted == null ? null : trendPsAt(lastFitted + PROJECT_YEARS);

  /**
   * Where the growth rate compounds FROM: the fitted trend at the last reported year.
   *
   *  The fitted value, not the last actual, and that is what makes the pair consistent. The
   * default forecast is `trendPsAt(lastFitted + PROJECT_YEARS)`, so measuring the default rate from
   * the fit's own starting point reproduces that forecast EXACTLY — the panel opens showing a rate
   * and an end value that agree, and the rate it shows is literally the slope of the dotted line on
   * the chart (a log-linear fit is a straight line on a log axis, which IS a constant CAGR).
   *
   * Measured from `latestPs` instead, the default rate would be "the rate from this company's last
   * filed year to a point on a line fitted through ten of them" — a number that moves with one
   * year's noise, does not describe the drawn projection, and disagrees with the end value beside
   * it whenever the last actual sits off the trend, which is almost always.
   */
  const trendBasePs = lastFitted == null ? null : trendPsAt(lastFitted);
  /** The trend's own annual growth — the default the rate box shows and reverts to. */
  const defaultCagrPct = useMemo(() => {
    const c = cagrBetween(trendBasePs, forecastPs, PROJECT_YEARS);
    return c == null ? null : c * 100;
  }, [trendBasePs, forecastPs]);

  /**
   * The forecast per-share figure everything downstream is built on, from whichever of the pair is
   * live — and the rate that figure implies, which is what the projected line is drawn at.
   *
   *  The rate compounds over `PROJECT_YEARS`, NEVER `horizonYears`. They are 10 and ~9.2 and both
   * get called "the horizon" within a few lines of each other. `PROJECT_YEARS` is how far the
   * FUNDAMENTAL is carried past the last REPORTED year — the axis the trend is drawn on and the
   * only window this rate describes. `horizonYears` runs from the live PRICE's date, is shorter by
   * the reporting lag, and exists solely to annualise the price return. Compounding the fundamental
   * over it would quietly shrink the forecast as the price got fresher, a relationship that does
   * not exist.
   */
  const cagrForecastPs = compoundFrom(trendBasePs, asNum(cagrStr), PROJECT_YEARS);
  const effectiveForecastPs = asNum(fcfStr) ?? cagrForecastPs ?? forecastPs;
  /**
   * The rate the projection is ACTUALLY drawn at — the user's, or the one the live end value
   * implies, or the fit's own. Falls back to the fit when there is no positive base to measure
   * from, which is the case the chart must still draw: `logLinearFit` dropped the negative years,
   * so a trend can exist where a growth RATE cannot be quoted.
   */
  const effectiveCagrPct = useMemo(() => {
    const c = cagrBetween(trendBasePs, effectiveForecastPs, PROJECT_YEARS);
    return c == null ? null : c * 100;
  }, [trendBasePs, effectiveForecastPs]);
  /** What the rate box reads — the user's own text, or the rate the live end value implies. */
  const shownCagrPct = cagrStr != null ? null : effectiveCagrPct;

  const indexData = useMemo(() => {
    const trendByYear = new Map(fit.trend.map((t) => [t.year, t.value]));
    const rows = idx.rows.map((r) => ({
      year: r.year, price: r.price, value: r.value,
      trend: trendByYear.get(r.year) ?? null,
      // The projection starts AT the last fitted year so the two segments meet rather than leaving
      // a gap; that shared point carries both keys.
      future: null as number | null,
    }));
    const lastFitted = fit.trend.length ? fit.trend[fit.trend.length - 1].year : null;
    if (lastFitted == null) return rows;
    for (const r of rows) if (r.year === lastFitted) r.future = trendByYear.get(lastFitted) ?? null;
    //  Price and value are null in these rows. Extending them would draw observed data into years
    // nobody has reported; only the trend continues.
    //  Compounded at the **EFFECTIVE** RATE, NOT RE-READ OFF THE FIT. With nothing typed the two
    // are identical to floating point — extending a log-linear fit IS compounding at exp(slope)−1 —
    // so this changes no pixel by default. What it buys is that a rate typed into the calculator
    // Moves the dotted line: without it the panel would say "12%" beside a projection still drawn
    // at the fitted 8%, and the price target would hang off a forecast the chart contradicts.
    //  `future` IS AN INDEX, and a CAGR is scale-free, so the rate applies unconverted.
    const base = trendByYear.get(lastFitted) ?? null;
    const step = effectiveCagrPct == null ? null : 1 + effectiveCagrPct / 100;
    for (let k = 1; k <= PROJECT_YEARS; k++) {
      rows.push({
        year: lastFitted + k, price: null, value: null, trend: null,
        future: base != null && step != null && step > 0
          ? base * Math.pow(step, k)
          : trendValueAt(fit, lastFitted + k),
      });
    }
    return rows;
  }, [idx, fit, effectiveCagrPct]);

  /**
   *  A log axis cannot plot zero or less, and the index goes negative. A cash-burn or loss year is
   * a real observation — `rebase` deliberately keeps it, and on a linear axis it drew below zero.
   * Here it has nowhere to go, so it is nulled for the chart and COUNTED, because a year silently
   * missing from an earnings line is the one a reader most needs to know about.
   */
  const posOnly = (v: number | null) => (v != null && v > 0 ? v : null);
  /**
   *  The axis is indexed, the hover is actual — so every row carries BOTH.
   *
   * Indexing is what makes the comparison possible at all (€700 of price and €20 of FCF/share share
   * no axis), and it is exactly what makes the tooltip useless: hovering FY2021 and reading
   * "Price 214 / FCF/share 186" tells you the two are 100-based and nothing else. The numbers a
   * reader wants at a point are the ones the company actually reported.
   *
   *  The raw price and value are read from `points`, NOT DIVIDED BACK OUT OF THE INDEX. The index
   * is `100 × v / anchor`, so recovering `v` is exact in algebra and a round trip through two
   * floating-point operations in practice — and it would print a figure that differs in the last
   * digit from the same company's number everywhere else in the app. The source is still there;
   * there is no reason to reconstruct it.
   *
   *  The trend and its projection have no raw source — they are fitted, so they only ever existed
   * as an index and are converted through the ANCHOR VALUE (`index/100 × anchorValue`), the same
   * conversion `trendPsAt` uses. Not `anchorPrice`: both are fitted through the per-share series.
   */
  const rawByYear = useMemo(() => new Map(points.map((p) => [p.year, p])), [points]);
  const chartRows = useMemo(() => indexData.map((r) => {
    const raw = rawByYear.get(r.year);
    const toPs = (v: number | null) =>
      (v == null || anchorValue == null ? null : v / 100 * anchorValue);
    return {
      year: r.year,
      price: posOnly(r.price), value: posOnly(r.value),
      trend: posOnly(r.trend), future: posOnly(r.future),
      rawPrice: raw?.price ?? null,
      rawValue: raw?.value ?? null,
      rawTrend: toPs(r.trend),
      rawFuture: toPs(r.future),
    };
  }), [indexData, rawByYear, anchorValue]);
  const hiddenByLog = indexData.filter((r) => r.value != null && r.value <= 0).length;


  const priceCagr = cagrOf(points, (p) => p.price);
  const valueCagr = cagrOf(points, (p) => p.value);

  // The latest ACTUALS, in per-share currency — the chart is indexed, the calculator is not.
  const latestPs = [...points].reverse().find((p) => p.value != null)?.value ?? null;
  const latestPrice = [...points].reverse().find((p) => p.price != null)?.price ?? null;
  const lastPriceYear = [...points].reverse().find((p) => p.price != null)?.year ?? null;

  /**
   * The multiple THROUGH TIME — weekly, back to `MULTIPLE_FROM_YEAR`.
   *
   *  Computed from rows this tab already has. `/api/earnings/by-isin/{isin}/metrics` returns
   * 12,375 rows for ASML — 6,933 daily closes, 513 forward-P/E points, 113 quarterly FCF rows —
   * so a second request would only be a second chance to disagree with the charts beside it.
   *
   *  Two kinds of line. `forwardHistory` is GuruFocus's OWN published forward-P/E indicator, read
   * straight through; `trailingHistory` is ours (price ÷ last reported figure, lagged so nothing
   * uses a number the market did not have). The FCF basis gets no forward at all — nobody
   * forecasts capex, so no such series exists at any date.
   *
   *  Ttm on the FCF side, annual would be unreadable. A single fiscal year makes the multiple
   * step once a year and swing violently (ASML: 21.8x → 116.4x → 28.9x). The quarterly rows are
   * PER-QUARTER — verified against the annual row before this was written — so a rolling four-
   * quarter sum is the honest trailing-twelve-month figure and updates four times a year.
   */
  //  The trailing series was removed here, 2026-08-21, ON REQUEST — with the quarterly TTM roll
  // and the annual fallback that fed it. The multiple-through-time chart now draws the vendor's
  // forward indicator alone; see `MultipleHistoryChart` for what the removed line was for and why
  // the FCF basis is consequently empty until a forward FCF series exists.
  //
  //  `trailingMultiples` / `ttm` / `thin` STAY IN `multiplesSeries`, unused for now. They are pure,
  // tested, and the natural home for the plumbing a forward FCF line will need; deleting tested
  // arithmetic to satisfy an import list would be the wrong direction on a change described as
  // "for now". They are named here so the next reader knows the module is deliberately wider than
  // its callers rather than half-cleaned.
  const forwardHistory = useMemo(
    () => (basis === 'eps' ? since(forwardSeries(metrics ?? []), MULTIPLE_FROM_YEAR) : []),
    [metrics, basis]);

  //  Derived from the same two lines the chart above plots, not from GuruFocus's own
  // `Valuation Ratios__FCF Yield %` (or its P/E) — whose denominator convention (year-end price?
  // average market cap?) we do not control. One source, so the two charts cannot disagree.
  const yields = useMemo(
    () => dailyYieldHistory(metrics ?? [], b.codes), [metrics, b.codes]);
  const yieldValues = yields.map((y) => y.yld).filter((v): v is number => v != null);
  const avgYield = meanOf(yieldValues);
  /**
   *  The point the latest yield came from, not just the yield — because its ⓘ works the division
   * out and the two operands have to be the SAME year's. `latestPs` and `latestPrice` are found
   * INDEPENDENTLY (each is the newest point carrying that field), so on a company whose newest
   * price arrived before its newest filing they are different years, and dividing them would print
   * a plausible expression that does not equal the figure above it.
   */
  const latestYieldPoint = [...yields].reverse()
    .find((p) => p.yld != null) ?? null;
  const latestYield = latestYieldPoint
    ? latestYieldPoint.yld : null;

  /**
   *  Switching basis clears both overrides, and that is not tidiness. A hand-typed "forecast 12.40"
   * is 12.40 of FREE CASH FLOW per share; carried onto EPS it silently becomes a forecast of
   * earnings — a number the user never entered, feeding a price target and a CAGR that look
   * entirely reasonable. Same for a demanded yield: 4% of FCF and 4% of earnings are different
   * demands. The defaults for the new basis are recomputed from its own series.
   */
  const switchBasis = (next: Basis) => {
    if (next === basis) return;
    //  The growth rate goes with them. "12% a year" of FREE CASH FLOW carried onto EPS is a
    // forecast of earnings the user never made — the same trap as the per-share figure, and harder
    // to spot, because a plausible growth rate is plausible on either basis.
    //  The current per-share figure and the current yield go too — both are denominated in the
    // basis, so an FCF/share the reader typed would sit under an EPS label as a plausible earnings
    // figure they never entered, which is the same trap this function already guards for the
    // forecast side.  `curPriceStr` DELIBERATELY SURVIVES: a share price is not denominated in
    // either basis, and clearing it would throw away an unrelated correction on every switch.
    setBasis(next); setFcfStr(null); setYieldStr(null); setCagrStr(null);
    setCurPsStr(null); setCurYieldStr(null);
  };
  /**
   * The price everything in the calculator is measured FROM, and the date it belongs to.
   *
   *  One pair, never two. The price and its date travel together — a live figure annualised over
   * the fiscal horizon, or a fiscal figure stamped with today, are the same bug in two directions
   * and both look right. `livePrice` is used only when the backend returned a positive number in
   * OUR currency; anything else (no Yahoo listing, no FX to the reporting currency, a non-positive
   * close) falls back to the fiscal year-end close, and `priceLive` tells the panel to say so.
   */
  const live = liveRes?.isin === isin && liveRes.currency === currency ? liveRes.data : null;
  // Not "no live price" — "we have not asked yet". The row shows neither badge while it is true.
  const livePending = currency != null
    && !(liveRes?.isin === isin && liveRes.currency === currency);
  const livePrice = live?.close_in != null && live.close_in > 0 ? live.close_in : null;
  const priceLive = livePrice != null;
  const lastFiscalPriceDate = useMemo(
    () => latestDateOf(metrics ?? [], PRICE_CODES), [metrics]);
  const currentPrice = priceLive ? livePrice : latestPrice;
  const priceDate = priceLive ? live!.date : lastFiscalPriceDate;

  /**
   * How long the CAGR actually has to run.
   *
   *  It is not `PROJECT_YEARS`. The forecast sits `PROJECT_YEARS` past the last REPORTED year;
   * from a live price that is up to a year nearer, depending how stale the accounts are. Holding
   * the divisor at the full horizon while moving the start to today understates the return by
   * exactly the reporting lag — the CAGR would quietly get worse the fresher the price got.
   * Falls back to `PROJECT_YEARS`, which is what the distance IS when the price is the fiscal one.
   */
  const targetDate = addYears(lastFiscalPriceDate, PROJECT_YEARS);
  const horizonYears = yearsBetween(priceDate, targetDate) ?? PROJECT_YEARS;

  /**
   * The CURRENT trio, resolved. See the  on `curPsStr` for why the price is free and the
   * per-share figure and the yield are one assumption stated two ways.
   *
   *  Order matters: the price resolves first, because the yield→per-share derivation divides by
   * it. A typed yield against a typed price is a coherent question ("what per-share figure does
   * that yield imply on the price I just entered"); the reverse order would answer it against the
   * market's price and then redraw everything at the user's.
   *
   *  A non-positive price yields nothing rather than Infinity — the same guard `yieldOf` makes.
   */
  const effectiveCurrentPrice = asNum(curPriceStr) ?? currentPrice;
  const psFromYield = (() => {
    const y = asNum(curYieldStr);
    if (y == null || effectiveCurrentPrice == null || !(effectiveCurrentPrice > 0)) return null;
    return y * effectiveCurrentPrice / 100;
  })();
  const effectiveCurrentPs = asNum(curPsStr) ?? psFromYield ?? latestPs;

  const target = priceTarget(
    effectiveCurrentPs, effectiveCurrentPrice,
    effectiveForecastPs, asNum(yieldStr) ?? avgYield, horizonYears);

  /**
   * What the yield box SHOWS while it is not the authority — the yield the other two rows imply,
   * which is exactly `target.currentYield` (`priceTarget` computes it from the same two figures).
   * `null` while the box IS the authority, so it keeps the user's own unrounded text.
   *
   *  Read off `target`, NOT RECOMPUTED. One computation, several readers, is the rule the whole
   * panel is built on — a second `ps ÷ price` here could disagree with the row above it.
   */
  const shownCurrentYieldPct = curYieldStr != null ? null : target.currentYield;

  /** The horizon the calculator's target is quoted over — still shown in the panel and named in
   *  the trend legend; nothing is plotted at it since the price-target line was removed. */
  const targetYear = lastPriceYear == null ? null : lastPriceYear + PROJECT_YEARS;
  //  Not wrapped in `useMemo`. The React Compiler could not preserve a manual memo here and so
  // skipped optimising the whole component — worse than the memo was worth. Left plain, the
  // compiler memoizes it itself.
  //  The chart is the reported rows, nothing spliced in. It used to carry a dashed blue "price
  // target" line — where the price would have to go to hit the demanded yield — which needed two
  // synthetic rows (one at today's fractional x, one at the target year) grafted onto the fiscal
  // series and the whole thing re-sorted. The line is gone (2026-08-04) and so are they: every row
  // here is now a period the company actually reported. The calculator's target itself is
  // unaffected — it is a panel figure, not a series.
  const chartData = chartRows;

  /** Integer years only. A numeric axis would otherwise tick at 2025.8 — and the fractional x is
   *  a position for today, not a period anyone reports in. */
  const yearTicks = useMemo(() => chartRows.map((r) => r.year), [chartRows]);

  /**
   * The price target, as an INDEX, so it can sit on this chart's one axis.
   *
   *  A `ReferenceDot`, NOT A ROW AND NOT A LINE — which is the whole reason it can come back. The
   * dashed price-target LINE removed on 2026-08-04 needed two synthetic rows grafted into the
   * fiscal series and the array re-sorted, and it cost the invariant the comment above still
   * states: every row in `chartData` is a period the company actually reported. A reference dot is
   * an annotation drawn over the plot — it touches neither the data nor `yearTicks`, so the
   * invariant survives and the endpoint is still marked.
   *
   *  Indexed off the **ANCHOR'S** PRICE, the same divisor `rebase` used for the plotted price
   * line. Dividing by the latest price instead would put the dot on a second, invisible scale — it
   * would look like a point on the blue line and be measured from somewhere else.
   *
   *  And it moves with the calculator. `target.forecastPrice` is forecast per-share ÷ forecast
   * yield, both of which the reader can type over — so editing an assumption moves this dot, which
   * is the point of having it on the chart rather than only in the panel.
   */
  const anchorPrice = idx.anchor == null ? null
    : points.find((p) => p.year === idx.anchor)?.price ?? null;
  const targetIdx = target.forecastPrice != null && anchorPrice != null && anchorPrice > 0
    ? target.forecastPrice / anchorPrice * 100 : null;
  //  A log axis cannot plot zero or less — the same rule the series obey (`posOnly`). A negative
  // forecast yield would produce one, and a dot silently absent is worse than no dot at all.
  const showTarget = targetIdx != null && targetIdx > 0 && targetYear != null;

  const logDomain = useMemo(() => paddedLogDomain(
    chartData.flatMap((r) => [r.price, r.value, r.trend, r.future])
      //  The dot is in the domain, or it is off the top of the chart. A price target is normally
      // ABOVE everything plotted — that is what makes it worth drawing — and `allowDataOverflow`
      // on the axis means anything outside the computed domain is simply not rendered. Left out,
      // the mark would vanish exactly on the companies with the most upside.
      .concat(showTarget ? [targetIdx] : [])
      .filter((v): v is number => v != null)),
  [chartData, showTarget, targetIdx]);

  /**
   *  The x domain is extended too, for the same reason. The projection runs to
   * `lastFitted + PROJECT_YEARS` while the target is quoted at `lastPriceYear + PROJECT_YEARS`; the
   * two differ whenever the newest price year is not the newest FITTED year, which is the ordinary
   * state between a filing and the next one. `dataMax` alone would clip the dot off the right edge.
   */
  const xMax = Math.max(
    chartRows.length ? chartRows[chartRows.length - 1].year : 0, showTarget ? targetYear : 0);

  const pct = (v: number | null | undefined) => (v == null ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(1)}%`);
  const ccy = currency ? `${currency} ` : '';
  /**  PRECISION BY MAGNITUDE, NOT A FIXED `toFixed`. A €1,240 target does not want two decimals
   *  and a €3.40 one is destroyed without them; a single setting is wrong for one of the two. */
  const fmtPrice = (v: number | null) => (v == null ? '—'
    : v.toLocaleString(undefined, { maximumFractionDigits: Math.abs(v) < 10 ? 2 : 0 }));
  const yld = (v: number | null | undefined) => (v == null ? '—' : `${v.toFixed(2)}%`);

  /**
   * The FCF | EPS switch.
   *
   *  It is rendered even on the empty state below, which is why it lives in a variable. A company
   * with no FCF/share history still usually has EPS (and the reverse for a few) — an empty tab with
   * no way out reads as "no data for this company", when the answer is one click away on the other
   * basis.
   */
  const basisSwitch = (
    <div className="inline-flex rounded-lg border border-neutral-700 overflow-hidden shrink-0"
      role="group" aria-label="Valuation basis">
      {(Object.keys(BASIS) as Basis[]).map((k) => (
        <button key={k} type="button" onClick={() => switchBasis(k)}
          aria-pressed={basis === k}
          title={`${BASIS[k].perShare} — ${BASIS[k].what}.`}
          className={`px-2.5 py-1 text-xs font-medium transition-colors ${
            basis === k ? 'bg-accent-600 text-white' : 'text-fg-muted hover:bg-overlay/5'}`}>
          {t.basis[k].tab}
        </button>
      ))}
    </div>
  );

  if (err) return <p className="text-xs text-neg-300 py-16 text-center">{err}</p>;
  if (metrics == null) return <p className="text-xs text-fg-subtle py-16 text-center">{t.loading}</p>;
  if (!points.some((p) => p.price != null) || !points.some((p) => p.value != null)) {
    return (
      <div className="py-16 flex flex-col items-center gap-3">
        <p className="text-xs text-fg-faint text-center">
          {t.noHistory(bl.perShare, name ?? isin)}
        </p>
        {basisSwitch}
      </div>
    );
  }

  return (
    // A 2×2 grid of EQUAL cells:
    //
    //     price vs figure  |  price target
    //     yield            |  forward multiple
    //
    //  Only the calculator is placed, and it is placed so it can stay last in the dom. Getting
    // this layout by reordering the JSX would work on a wide screen and put the conclusion between
    // two charts on a phone, where the grid collapses to one column and DOM order IS the reading
    // order. Pinning it to (row 1, col 2) lets the three charts auto-flow around it — chart, then
    // the skipped cell, then yield, then forward — while a narrow screen still reads
    // charts-then-conclusion.
    //
    // `auto-rows-fr` equalises the row heights to the tallest and the default `items-stretch`
    // makes each card fill its cell; without either, four cards of different heights read as four
    // unrelated panels rather than one page.
    <div className="grid grid-cols-1 lg:grid-cols-2 auto-rows-fr gap-4">
    {/* The Long Equity card shape: header, then the stat tiles, then the chart — all inside the one
        card, so the numbers travel with the picture they describe. */}
    <div className="rounded-xl border border-neutral-800/40 bg-card p-4 space-y-3 min-w-0">
      <div className="flex items-baseline gap-2 flex-wrap">
        <h4 className="text-base font-semibold text-fg-strong">{t.priceVs(bl.perShare)}</h4>
        {hiddenByLog > 0 && (
          // Named, not dropped: on a linear axis these plotted below zero, and a cash-burn or loss
          // year vanishing without a word is exactly the observation a reader must not lose.
          <span className="text-xs text-warn-300"
            title="A log axis has no room for zero or a negative value. Those years are also excluded from the trend fit, for the same reason — a loss has no logarithm.">
            {t.notPlottable(String(hiddenByLog), bl.negativeYear)}
          </span>
        )}
        {/*  ONE SWITCH, THREE PANELS. It sits on the primary chart, and the yield card and the
            calculator beside it re-label themselves with it — which is how a reader learns they
            are all reading the same basis. */}
        <div className="ml-auto self-center">{basisSwitch}</div>
      </div>

      {/**
       *  A grid, not the shared flex row, and the reason is that `Stat` IS `flex-1 basis-0`.
       * A flex row of `basis-0` children NEVER WRAPS — it shrinks them to `min-w-0` instead — so
       * `flex-wrap` here was inert and these FIVE tiles simply divided the card between them.
       * `CardStats`' own sizing note measures that card at ~444px (1920px viewport) and ~316px
       * (1440px): five ways, that is **63–89px of inner width per tile**. At that width the value
       * truncates at about five monospace glyphs, so `USD 3,593` and `+34.3%` were being cut, and
       * the two-line label clamp was eating `EST. CAGR TO FY2036` — which is the tile a reader
       * opens this card for.
       *
       *  Five columns — one row, on request ("lets align these tiles horizontally"). It was
       * three, which put them 3 + 2 across two rows and gave each ~142px; one row divides the same
       * card five ways instead. Shrinking is still available inside a cell (`min-w-0` survives),
       * which is what lets that happen at all rather than overflowing the card.
       *
       *  The cost is truncation, and it is real at narrow widths. `CardStats`' sizing note
       * measures this card at ~444px (1920px viewport) and ~316px (1440px); five ways that is
       * ~82px and ~55px per tile. The value truncates (with the full figure on `title`, as it
       * always has) and the two-line label clamp bites on the longest labels — `EST. CAGR TO
       * FY2036` is the one to watch. If it reads badly on a smaller screen the lever is the tile
       * COUNT, not the font: four fit comfortably, five do not.
       *
       *  The tracks are `minmax(0, 8rem)`, NOT `1fr`, AND `grid-cols-3` WAS VISIBLY WRONG. An
       * `1fr` track takes an equal share of the CARD, but `Stat` caps itself at `max-w-[8rem]` —
       * so on any card wider than ~470px each tile sat left-aligned in a much wider cell and the
       * row read as three tiles flung apart with holes between them. Reported as "the tiles are
       * now very far apart which is odd, they should be next to each other". Capping the TRACK at
       * the same 8rem the tile caps itself at is what keeps them adjacent: the row is then
       * `gap-2` apart and left-aligned, and the leftover width stays leftover instead of being
       * dealt out into the gaps. The `0` minimum is what still lets them shrink on a narrow card.
       *
       *  THE 3+2 BREAK SPLITS THE HISTORY/FORECAST GROUPS, AND THAT IS ALREADY ACCEPTED. The note
       * on the forecast tiles below records that a divider rule between the two groups was tried
       * and removed precisely because the row breaks at a width nobody controls — "the distinction
       * it was drawing is carried by the LABELS instead, which travel with the tile however the row
       * breaks". A fixed 3-column break is strictly more predictable than what it replaced.
       *
       *  Not applied to the yield card's row below: two tiles in a flex row already get half the
       * card each, and forcing them into three columns would leave a third of it empty.
       */}
      <div className="grid grid-cols-[repeat(5,minmax(0,8rem))] gap-2">
        <Stat label={t.priceCagr} value={pct(priceCagr?.pct)} color={chartTheme.accentStrong}
          info={<InfoTip content={<AspectCard
            what="Compound annual growth of the fiscal year-end share price."
            where="GuruFocus `Month End Stock Price`, the close at each fiscal year end."
            when={priceCagr ? `${priceCagr.from} to ${priceCagr.to} (${priceCagr.years} years).` : 'Not available.'}
            how="First to last positive observation. The year-end price, not today's quote." />} />} />
        <Stat label={t.perShareCagr(bl.perShare)} value={pct(valueCagr?.pct)} color={chartTheme.warn}
          info={<InfoTip content={<AspectCard
            what={`Compound annual growth of ${b.what}.`}
            where={b.source}
            when={valueCagr ? `${valueCagr.from} to ${valueCagr.to} (${valueCagr.years} years).` : 'Not available.'}
            how={`Per share, so buybacks and new shares affect it. ${b.caveat}`} />} />} />
        {/*  THE CONCLUSION, ON THE CHART THAT ARGUES FOR IT. The two CAGRs to the left are what
            HAPPENED; these three are what the assumptions in the panel IMPLY, and the reader was
            having to hold a number from one card in their head while looking at the other. They are
            deliberately the LAST tiles in the row: history first, forecast after it.

             READ OFF `target` — THE SAME OBJECT THE PANEL PRINTS AND THE CHART'S DOT IS PLACED
            FROM. Not recomputed here from the same inputs, which is the version of this that goes
            wrong: `priceTarget` is one computation with several readers precisely so a tile cannot
            quote a figure the panel beside it disagrees with. They move together when the forecast
            rate is edited because there is only one of them.

             NO DIVIDER RULE BETWEEN THE TWO GROUPS, AND IT WAS TRIED. Five tiles at `min-w-6.5rem`
            overflow a half-width card, so the row wraps — and a `w-px self-stretch` separator wraps
            with them, landing at the START of a line as a stray vertical rule pointing at nothing.
            A separator that is in the right place only at some widths is worse than none. The
            distinction it was drawing is carried by the LABELS instead, which travel with the tile
            however the row breaks: "Price CAGR" is observed, "Est. CAGR to FY2035" is not, and the
            word doing that work is `Est.` */}
        <Stat label={t.currentSharePrice} value={`${ccy}${fmtPrice(target.currentPrice)}`}
          info={<InfoTip content={<AspectCard
            what="Current share price."
            where={priceLive
              ? `yfinance asset_price${live?.symbol ? ` (${live.symbol})` : ''}.`
              : 'GuruFocus Month End Stock Price.'}
            when={priceLive
              ? `Its close of ${priceDate ?? 'an unknown date'}.`
              : 'Fiscal year end.'}
            how="Used with the latest reported figure to calculate the current yield." />} />} />
        {/*  THE SAME COLOUR AS THE DOT IT DESCRIBES (`chartTheme.accentStrong`, the price line's,
            which is what the `ReferenceDot` below is stroked with) — the tile and the mark on the
            plot are one fact, and a tile whose bar matches nothing on the chart is a tile the eye
            has to look up.  Deliberately NOT `pos` green: on this tab green means nothing, and in
            the Graphs charts next door it means "the benchmark" on every single card.
             `Current share price` above stays UNCOLOURED on purpose — it is the live quote, and
            every point plotted here is a fiscal year-end close, so tying it to the price line would
            claim it is on a line it is not on. */}
        <Stat label={targetYear == null ? t.priceTarget : t.priceTargetFy(String(targetYear))}
          value={`${ccy}${fmtPrice(target.forecastPrice)}`} color={chartTheme.accentStrong}
          info={<InfoTip content={<AspectCard
            what={`Target share price from forecast ${b.perShare} and ${b.yieldInline}.`}
            where={`${b.perShare} divided by ${b.yieldInline}.`}
            //  This tile already carried its numbers, inline in `where`, WHERE THEY READ AS PROSE.
            // Moving them into the block gives them the same monospaced, selectable shape every
            // other worked formula now has — and puts them directly under the symbols they fill in
            // rather than trailing an em dash at the end of a sentence.
            worked={workedRatio(target.forecastPs, target.forecastYield,
              target.forecastPrice == null ? '' : fmtPrice(target.forecastPrice),
              '', '%')}
            when={targetYear == null ? 'At the end of the forecast window.' : `FY${targetYear}, ${PROJECT_YEARS} years past the last reported year.`}
            how="This is based on your forecast inputs." />} />} />
        <Stat label={targetYear == null ? t.estCagr : t.estCagrTo(String(targetYear))}
          value={pct(target.cagr == null ? null : target.cagr * 100)}
          tone={target.cagr == null ? undefined
            : target.cagr >= 0 ? 'text-pos-500' : 'text-neg-500'}
          info={<InfoTip content={<AspectCard
            what="Annualised return from the current price to the target price."
            where="(target price / current price)^(1 / years) - 1."
            when={`${horizonYears.toFixed(1)} years to FY${targetYear ?? 'target'}.`}
            worked={workedPriceCagr(target.forecastPrice, target.currentPrice,
              horizonYears, target.cagr)}
            how="Price return only. Dividends are excluded." />} />} />
      </div>

      {/*  INDEXED, NOT DUAL-AXIS. €700 of price and €20 of earnings share no axis, and two
          independently-scaled axes let any pair of series be made to look correlated — the
          rescaling is invisible and the reader has no way to check it. One axis, one base year. */}
      <div>
        {idx.anchor == null ? (
          // Rebasing off a cash-burn or loss year divides by a negative and flips every later
          // point, so a company with no positive year gets no index at all — see `rebase`.
          <p className="text-xs text-fg-faint py-16 text-center">
            {t.noPositiveBase(bl.perShare)}
          </p>
        ) : (
          <>
            <ResponsiveContainer width="100%" height={CHART_HEIGHT}>
              {/*  ROOM ON THE TOP AND RIGHT FOR THE TARGET'S LABEL. Recharts draws a reference
                  label into the margin, so at the default 5/12 the price would have been half a
                  label outside the SVG — the one number this chart was asked to show most clearly. */}
              <ComposedChart data={chartData} margin={{ top: 22, right: 44, bottom: 5, left: 4 }}
                style={{ cursor: 'pointer' }} onClick={() => setShowInputs(true)}>
                <CartesianGrid strokeDasharray="3 3" stroke={chartTheme.gridEarnings} />
                {/*  NUMERIC, NOT CATEGORICAL — a fiscal year has to sit at its real distance from
                    the next one, and the projected stretch runs a decade past the last of them.
                    Ticks are pinned to the reported years; the default numeric ticks would invent
                    2025.5 as though something were reported there.
                     The note here used to say "so today can sit between two fiscal years (see
                    `nowX`)" — `nowX` was deleted with the dashed price-target line on 2026-08-04
                    and no row has had a fractional x since. Every row in `chartData` is a reported
                    year or a projected one, which is the invariant the removal was FOR. */}
                {/*  THE TILT IS WHAT LETS `ticks` MEAN WHAT IT SAYS. `yearTicks` names every
                    reported year plus the horizon, but recharts still DROPS the ones it thinks
                    collide — an explicit `ticks` array is a candidate list, not an instruction. Flat
                    at 12px a `2025` needs 27px of pitch and half the horizon fell out; tilted it
                    needs 17px. */}
                <XAxis dataKey="year" type="number" domain={['dataMin', xMax]}
                  ticks={yearTicks} allowDecimals={false} interval="preserveStartEnd"
                  {...tiltedAxis()} />
                {/*  LOG SCALE, WHICH IS THE POINT: the fit is log-linear, so a constant-growth
                    series is a STRAIGHT line here and the R² above it becomes something the reader
                    can check by eye rather than take on trust. On a linear axis a 0.4 and a 0.95
                    both look like curves. */}
                <YAxis scale="log" domain={logDomain ?? ['dataMin', 'dataMax']} allowDataOverflow
                  tick={{ fontSize: 12, fill: chartTheme.axisTick }} width={52}
                  tickFormatter={(v: number) => v.toFixed(0)} />
                <Tooltip contentStyle={chartTheme.tooltipCard.contentStyle} labelStyle={{ color: chartTheme.axisLabel }}
                  // A fractional x is today, not a fiscal year — printing "2025.8" as the heading
                  // would read like a period the company reported.
                  labelFormatter={(v) => (typeof v === 'number' && !Number.isInteger(v)
                    ? 'Today' : `FY${v}`)}
                  /**
                   *  The reported figures, not the index. See `chartRows`: the axis has to be
                   * indexed for the two series to share it, and an indexed tooltip is the part of
                   * that trade nobody has to accept — "Price 214" at FY2021 says only that the
                   * base year is 100.
                   *
                   *  Each series is formatted the way its quantity is formatted elsewhere on this
                   * CARD. A price goes through `fmtPrice` (precision by magnitude — the same rule
                   * the target's label and the panel's price rows use); a PER-SHARE figure gets two
                   * decimals like the panel's forecast row, because `fmtPrice` would round FCF/share
                   * of 12.40 to "12" and that is the number this card is about.
                   *
                   *  Four series, four names. `trend` and `future` used to fall through to the
                   * `${b.perShare}` label with the observed line, so hovering a projected year
                   * showed two identically-named rows and no way to tell the fit from the data.
                   */
                  formatter={(v, n, item) => {
                    const row = item?.payload as {
                      rawPrice?: number | null; rawValue?: number | null;
                      rawTrend?: number | null; rawFuture?: number | null;
                    } | undefined;
                    const ps = (x: number | null | undefined) =>
                      (x == null ? '—' : `${ccy}${x.toFixed(2)}`);
                    if (n === 'price') return [`${ccy}${fmtPrice(row?.rawPrice ?? null)}`, 'Price'];
                    if (n === 'value') return [ps(row?.rawValue), b.perShare];
                    if (n === 'trend') return [ps(row?.rawTrend), `${b.perShare} · trend`];
                    if (n === 'future') return [ps(row?.rawFuture), `${b.perShare} · projected`];
                    return [typeof v === 'number' ? v.toFixed(0) : '—', String(n)];
                  }} />
                <ReferenceLine y={100} stroke={chartTheme.zeroLine} />
                <Line dataKey="price" name="price" type="monotone" stroke={chartTheme.accentStrong}
                  strokeWidth={2} dot={{ r: 2.5 }} connectNulls />
                <Line dataKey="value" name="value" type="monotone" stroke={chartTheme.warn}
                  strokeWidth={2} dot={{ r: 2.5 }} connectNulls />
                {/* The fitted exponential through the per-share series, and its continuation.  TWO SERIES,
                    NOT ONE: the projected stretch is drawn thinner and more transparent because it
                    is arithmetic carried past the data, not a fit to anything. One line would make
                    the last two years look as observed as the first eight. */}
                <Line dataKey="trend" name="trend" type="monotone" stroke={chartTheme.warn}
                  strokeWidth={1.5} strokeDasharray="5 3" strokeOpacity={0.75} dot={false} connectNulls />
                <Line dataKey="future" name="future" type="monotone" stroke={chartTheme.warn}
                  strokeWidth={1.5} strokeDasharray="2 4" strokeOpacity={0.5} dot={false} connectNulls />
                {/*  THE PRICE TARGET — THE ONE POINT ON THIS CHART THAT HAS NOT HAPPENED, so it is
                    drawn as the one mark that is not filled. A solid dot in the price colour would
                    read as an observed close; HOLLOW (white core, 2px accent ring) is the ordinary
                    encoding for a projected point, and it cannot be confused with the round dots on
                    the blue line even at a glance.

                     THE WHITE CORE IS ALSO THE SURFACE RING the dot needs to stay legible where it
                    lands on a gridline — one shape doing both jobs rather than a stroke drawn
                    around a mark.

                     THE LABEL WEARS TEXT INK, NOT THE SERIES COLOUR. `accentStrong` is a mark
                    colour; identity comes from the ring beside the number. Positioned `top`, which
                    is what the extra top margin above is for. */}
                {showTarget && (
                  <ReferenceDot x={targetYear} y={targetIdx} r={5} ifOverflow="extendDomain"
                    fill={chartTheme.tooltipCard.contentStyle.backgroundColor}
                    stroke={chartTheme.accentStrong} strokeWidth={2}
                    label={{
                      value: `${ccy}${fmtPrice(target.forecastPrice)}`,
                      position: 'top', fontSize: 12, fontWeight: 600,
                      fill: chartTheme.axisLabel,
                    }} />
                )}
              </ComposedChart>
            </ResponsiveContainer>
            <div className="flex justify-center flex-wrap gap-x-4 gap-y-1 text-xs mt-1">
              <span className="flex items-center gap-1.5"><span className="w-3 h-0.5 inline-block rounded" style={{ background: chartTheme.accentStrong }} />{t.legendSharePrice}</span>
              <span className="flex items-center gap-1.5"><span className="w-3 h-0.5 inline-block rounded" style={{ background: chartTheme.warn }} />{b.perShare}</span>
              {fit.r2 != null && (
                <span className="flex items-center gap-1.5 text-fg-muted"
                  title={`Exponential fit over ${fit.n} year(s)${fit.dropped ? `, ${fit.dropped} dropped (${b.negativeYear} years have no logarithm)` : ''}. R² is how tightly ${b.perShare} hugs a constant-growth line: 1.0 = perfectly steady compounding.`}>
                  <span className="w-3 h-0.5 inline-block rounded"
                    style={{ background: chartTheme.warn, opacity: 0.75 }} />
                  {t.legendTrend(fit.r2.toFixed(2), String(PROJECT_YEARS))}
                </span>
              )}
              {/*  THE DOT IS A SERIES, SO IT IS IN THE LEGEND. A lone unexplained mark on a chart
                  is read as an outlier in whichever line it sits nearest — here, the blue price
                  line, which is exactly the wrong reading: it is not a price that happened. The
                  key repeats the hollow ring so the shape, not just the colour, carries it. */}
              {showTarget && (
                <span className="flex items-center gap-1.5 text-fg-muted"
                  title={`Where the share price lands if ${b.perShare} reaches the forecast in the `
                    + `panel and the market pays the forecast ${b.yieldInline} for it — `
                    + `${ccy}${fmtPrice(target.forecastPs)} ÷ ${target.forecastYield?.toFixed(1)}%. `
                    + ' Not an observation and not anyone’s target: it is the arithmetic of the two '
                    + 'assumptions beside the chart, and it moves the moment you edit either.'}>
                  <span className="w-2.5 h-2.5 inline-block rounded-full border-2 box-border"
                    style={{ borderColor: chartTheme.accentStrong,
                      background: chartTheme.tooltipCard.contentStyle.backgroundColor }} />
                  Price target FY{targetYear}
                </span>
              )}
            </div>
          </>
        )}
      </div>

      {showInputs && (
        //  Handed the series, not the ISIN — one computation, so the table cannot disagree with
        // the line that opened it.
        <QuickValuationInputsModal points={points} index={idx} currency={currency} basis={b}
          name={name} isin={isin} onClose={() => setShowInputs(false)} />
      )}
    </div>

    <div className="rounded-xl border border-neutral-800/40 bg-card p-4 space-y-3 min-w-0">
      <div className="flex items-baseline gap-2 flex-wrap">
        <h4 className="text-base font-semibold text-fg-strong">{bl.yieldTitle}</h4>
        <span className="text-xs text-fg-faint">{t.yieldCaption(bl.perShare)}</span>
        <button type="button"
          onClick={() => (refreshingQuickValuation
            ? (refreshJobId ? void cancelJob(refreshJobId) : undefined)
            : refreshQuickValuation())}
          disabled={!!refreshJob?.cancelRequested || companyId == null}
          title={refreshJob?.cancelRequested ? 'Cancelling…'
            : refreshingQuickValuation ? 'Re-reading — press to cancel'
              : 'Refresh every Quick Valuation chart'}
          className={`ml-auto inline-block text-xs leading-none ${
            refreshJob?.cancelRequested ? 'cursor-wait text-fg-faint'
              : refreshingQuickValuation ? 'text-warn-400 hover:text-neg-400'
                : companyId == null ? 'cursor-default text-fg-faint/40'
                  : 'text-fg-faint hover:text-accent-400'}`}>
          {refreshJob?.cancelRequested ? 'Cancelling…' : refreshingQuickValuation ? 'Cancel' : 'Refresh'}
        </button>
      </div>

      <div className="flex flex-wrap gap-2">
        <Stat label={t.avg} value={yld(avgYield)} color={chartTheme.accent}
          info={<InfoTip content={<AspectCard
            what={`Average ${b.yieldInline}.`}
            where="Calculated from the charted daily closes."
            //  The span is named, not counted against a constant. This read "n of the last 10
            // fiscal years" off the history cap; with the cap gone there is no fixed denominator
            // to be `of`, and quoting one that no longer exists is worse than quoting none.
            when={yields.length
              ? `${yieldValues.length} daily closes shown (${yields[0]?.date} to ${yields[yields.length - 1]?.date}).`
              : 'No daily closes to average.'}
            //  `yieldValues` IS WHAT THE MEAN WAS TAKEN OVER — the same array `avgYield` divides,
            // so the addends listed here provably sum to the figure on the tile. It is also the
            // dashed line on the chart below, which is the third place this one number appears.
            worked={workedMean(yieldValues)}
            how="Simple average of daily yields." />} />} />
        <Stat label={t.latest} value={yld(latestYield)} color={chartTheme.accent}
          info={<InfoTip content={<AspectCard
            what={`Latest daily ${b.yieldInline}.`}
            where={`${b.perShare} divided by that day's closing price.`}
            when={latestYieldPoint?.date ? (
              <span className="inline-flex rounded-full border border-neutral-700 bg-overlay/10 px-1.5 py-0.5 font-mono text-[11px] text-fg-soft">
                as of {latestYieldPoint.date}
              </span>
            ) : 'No daily close with a reported per-share figure.'}
            //  Both operands off the same point — see `latestYieldPoint`. The FY label is in the
            // expression because that year is not necessarily the newest one on either line.
            worked={workedRatio(latestYieldPoint?.value, latestYieldPoint?.price,
              latestYield == null ? '' : yld(latestYield),
              '', ` ${ccy}`)}
            how="A higher yield means a lower price relative to this measure." />} />} />
        <Stat label={t.asOf} value={latestYieldPoint?.date ?? '—'} color={chartTheme.accent}
          info={<InfoTip content={<AspectCard
            what={`As-of date for the latest daily ${b.yieldInline}.`}
            where="The daily close series used as the yield denominator."
            when={latestYieldPoint?.date ?? 'No daily close with a reported per-share figure.'}
            how="The Latest yield and this date always come from the same daily observation." />} />} />
      </div>

      {/*  A YIELD, NOT A MULTIPLE — SO NEGATIVES STAY. A cash-burn or loss year is −5% here, which
          reads as what it is; the same year as P/FCF (or P/E) would be −20x and sort below every
          cheap year on the axis, as though it were the bargain of the decade. */}
      <div>
        <ResponsiveContainer width="100%" height={CHART_HEIGHT}>
          <ComposedChart data={yields} margin={{ top: 5, right: 12, bottom: 5, left: 4 }}
            style={{ cursor: 'pointer' }} onClick={() => setShowInputs(true)}>
            <CartesianGrid strokeDasharray="3 3" stroke={chartTheme.gridEarnings} />
            <XAxis dataKey="date" {...tiltedAxis()} tickFormatter={(date: string) => date.slice(0, 7)} />
            <YAxis domain={paddedDomain(yieldValues)} tick={{ fontSize: 12, fill: chartTheme.axisTick }}
              width={52} tickFormatter={(v: number) => `${v.toFixed(0)}%`} />
            <Tooltip contentStyle={chartTheme.tooltipCard.contentStyle} labelStyle={{ color: chartTheme.axisLabel }}
              formatter={(v) => [typeof v === 'number' ? `${v.toFixed(2)}%` : '—', b.yieldTitle]} />
            <ReferenceLine y={0} stroke={chartTheme.zeroLine} />
            {avgYield != null && (
              <ReferenceLine y={avgYield} stroke={chartTheme.accent} strokeDasharray="5 3" strokeOpacity={0.6} />
            )}
            <Line dataKey="yld" name="yld" type="monotone" stroke={chartTheme.accent}
              strokeWidth={2} dot={false} connectNulls />
          </ComposedChart>
        </ResponsiveContainer>
        <div className="flex justify-center flex-wrap gap-x-4 gap-y-1 text-xs mt-1">
          <span className="flex items-center gap-1.5"><span className="w-3 h-0.5 inline-block rounded" style={{ background: chartTheme.accent }} />{t.yieldLegend(bl.yieldTitle)}</span>
        </div>
      </div>
    </div>

    {/* Bottom-right, by auto-flow. Handed the computed series, never the ISIN — same rule as the
        drill-down modal, so it cannot disagree with the charts above about what the company earned. */}
    <MultipleHistoryChart height={CHART_HEIGHT} basis={b} basisKey={basis} currency={currency}
      forward={forwardHistory} fromYear={MULTIPLE_FROM_YEAR}
      name={name} isin={isin}
      onRefresh={refreshQuickValuation} canRefresh={companyId != null}
      refreshing={refreshingQuickValuation} cancelling={refreshingQuickValuation && !!refreshJob?.cancelRequested}
      onCancel={() => { if (refreshJobId) void cancelJob(refreshJobId); }} />

    {/* Top-right, but LAST IN THE DOM — see the grid note above. The only non-chart card, so it
        fills a cell sized by the chart beside it: a flex column with its CAGR footer pinned to the
        bottom rather than left floating halfway up a stretched card. */}
    <PriceTargetCalculator
      className="lg:col-start-2 lg:row-start-1"
      target={target} years={PROJECT_YEARS} currency={currency} basis={b} basisKey={basis}
      horizonYears={horizonYears} targetYear={targetYear}
      price={{ date: priceDate, live: priceLive, pending: livePending,
        symbol: live?.symbol ?? null, staleDays: live?.stale_days ?? null }}
      fcfStr={fcfStr}
      //  THE **EFFECTIVE** FORECAST, NOT THE TREND'S. With the rate box live this row must show the
      // figure that rate produces — it is what the target is actually built from, and a box still
      // showing the fitted forecast would sit one line above a price target that does not divide
      // from it.
      defaultForecastFcfPs={cagrForecastPs ?? forecastPs}
      //  Each setter clears the other — the rate and the end value are two views of ONE
      // assumption (see `cagrStr`), so only one can be the authority. Typing in either box makes it
      // the authority and hands the other back its derived role.
      onFcf={(v) => { setFcfStr(v); setCagrStr(null); }}
      cagrStr={cagrStr} shownCagrPct={shownCagrPct} defaultCagrPct={defaultCagrPct}
      onCagr={(v) => { setCagrStr(v); setFcfStr(null); }}
      onResetCagr={() => setCagrStr(null)}
      cagrDisabled={trendBasePs == null}
      yieldStr={yieldStr} onYield={setYieldStr} defaultForecastYield={avgYield}
      onReset={() => {
        setFcfStr(null); setYieldStr(null); setCagrStr(null);
        //  The header reset clears the current rows too. It is the card's "put everything back",
        // and a reset that left three of six typed figures in place would be the more confusing
        // half-measure — the panel would still be showing a target built on edits it just claimed
        // to have discarded.
        setCurPsStr(null); setCurPriceStr(null); setCurYieldStr(null);
      }}
      //  `null`, NOT the default's current value — see the  on `Input`'s `onRevert`. Null means
      // "never typed", which is what keeps the box TRACKING the computed figure as it moves.
      onResetFcf={() => { setFcfStr(null); setCagrStr(null); }}
      /**
       *  One object, not twelve flat props. These belong to one identity (`yield = per-share ÷
       * price`) and are read together by three adjacent rows; passed flat they would be twelve
       * more names in a list that is already long, with nothing in the signature saying they
       * interact. See the  on `curPsStr` for the binding.
       *
       *  `onPs` AND `onYield` CLEAR EACH OTHER, `onPrice` CLEARS NEITHER — that IS the binding,
       * expressed at the only place that knows it. The panel below never has to decide which of
       * two live values to believe, exactly as with the forecast pair.
       */
      current={{
        psStr: curPsStr, onPs: (v) => { setCurPsStr(v); setCurYieldStr(null); },
        defaultPs: latestPs, onResetPs: () => { setCurPsStr(null); setCurYieldStr(null); },
        priceStr: curPriceStr, onPrice: setCurPriceStr,
        defaultPrice: currentPrice, onResetPrice: () => setCurPriceStr(null),
        yieldStr: curYieldStr, onYield: (v) => { setCurYieldStr(v); setCurPsStr(null); },
        shownYieldPct: shownCurrentYieldPct,
        onResetYield: () => { setCurYieldStr(null); setCurPsStr(null); },
      }} />
    </div>
  );
}
