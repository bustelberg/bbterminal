'use client';

import { useEffect, useMemo, useState } from 'react';
import {
  CartesianGrid, ComposedChart, Line, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis,
} from 'recharts';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { chartTheme } from '../../../lib/chartTheme';
import { AspectCard } from '../../../lib/tipCard';
import InfoTip from '../InfoTip';
import { Stat } from './MetricGrowthCard';
import QuickValuationInputsModal from './QuickValuationInputsModal';
import PriceTargetCalculator from './PriceTargetCalculator';
import { meanOf, paddedDomain, paddedLogDomain } from './marginData';
import { logLinearFit, trendValueAt } from '../../../lib/trendFit';
import ForwardMultipleChart from './ForwardMultipleChart';
import {
  addYears, BASIS, cagrOf, forwardFigures, latestDateOf, priceTarget, priceVsMetric, PRICE_CODES,
  rebase, yearsBetween, yieldOf, type Basis, type MetricRow,
} from './quickValuation';

/**
 * The "Quick Valuation" tab: a company's SHARE PRICE against its FREE CASH FLOW PER SHARE over the
 * last ten fiscal years, as one indexed chart with the two growth rates above it.
 *
 * The question: has the price followed the cash the business throws off per share, or has the
 * MULTIPLE done the work? Both give the same total return and they are not the same investment —
 * which is why the two CAGRs sit beside each other and nothing else competes with them.
 *
 * ⚠ SINGLE COMPANY ONLY — there is no portfolio share and no portfolio FCF per share (the amounts
 * are in different currencies and cannot be summed). The tab is not offered for a basket; the
 * portfolio-level version of this question is the FCF-SBC yield card, currency-free by design.
 *
 * ⚠ EVERY OBSERVED POINT IS A FISCAL YEAR-END CLOSE, NOT TODAY'S. Both series come from the same
 * fiscal rows, which is what makes the comparison internally consistent — but the newest point can
 * be up to a year old, so the PICTURE is a read on the last reported year. The one exception is
 * where the dotted price-target line STARTS, which is today's price at today's position on the
 * axis; it is drawn dotted, in its own series, and named in the legend for exactly that reason.
 *
 * ⚠ THE CALCULATOR'S "CURRENT SHARE PRICE" IS THE LIVE ONE, AND IT IS A DIFFERENT NUMBER. It is
 * the newest yfinance close (`asset_price` — what /portfolios prices every model with), converted
 * into the reporting currency the fiscal rows are filed in. Before 2026-07-29 the row printed the
 * fiscal year-end close under the label "Current share price": up to a year old, with the FCF
 * yield and the CAGR hanging off it, both labelled "at today's price" and neither measured there.
 * When there is no priced Yahoo listing it falls back to the fiscal close AND SAYS SO — a stale
 * price shown as live is the failure this exists to prevent, so a silent fallback would recreate it.
 */

const YEARS = 10;
/** How far the fitted trend is carried past the last reported year. */
const PROJECT_YEARS = 2;
/** Both charts share it, so the two grid cells match without either card padding out the gap. */
const CHART_HEIGHT = 320;

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
  /** ⚠ THE ANSWER CARRIES THE QUESTION IT ANSWERED. "Have we looked yet?" is derived from whether
   *  the stored result belongs to THIS (isin, currency) — a separate `pending` boolean has to be
   *  flipped in four places (mount, company change, success, failure) and the row makes a claim
   *  about provenance in whichever one gets missed. */
  const [liveRes, setLiveRes] = useState<
    { isin: string; currency: string; data: LatestClose | null } | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [showInputs, setShowInputs] = useState(false);

  useEffect(() => {
    let alive = true;
    void (async () => {
      // ⚠ CURRENCY IS CLEARED WITH THE METRICS. It gates the live-price fetch below; left behind
      // from the previous company it would convert this one's close at that one's currency.
      setMetrics(null); setCurrency(null); setLiveRes(null); setErr(null);
      try {
        const r = await apiFetch(`${API_URL}/api/earnings/by-isin/${encodeURIComponent(isin)}/metrics`);
        if (r.status === 404) { if (alive) setMetrics([]); return; }
        const b = await r.json().catch(() => null);
        if (!alive) return;
        if (!r.ok) { setErr(b?.detail ?? `HTTP ${r.status}`); return; }
        setMetrics((b?.metrics ?? []) as MetricRow[]);
        setCurrency(b?.currency ?? null);
      } catch (e) {
        if (alive) setErr(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { alive = false; };
  }, [isin]);

  /**
   * Today's close, in the currency the fiscal rows are filed in.
   *
   * ⚠ NOT FETCHED AT ALL WITHOUT A KNOWN REPORTING CURRENCY, and not used unless the backend could
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
  const points = useMemo(() => priceVsMetric(metrics ?? [], b.codes, YEARS), [metrics, b.codes]);
  const idx = useMemo(() => rebase(points), [points]);

  /**
   * The exponential trend through the per-share series, and its R².
   *
   * ⚠ FITTED ON THE INDEX, WHICH CHANGES NOTHING. A log-linear fit is invariant to scaling —
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
    // ⚠ PRICE AND VALUE ARE NULL IN THESE ROWS. Extending them would draw observed data into years
    // nobody has reported; only the trend continues.
    for (let k = 1; k <= PROJECT_YEARS; k++) {
      rows.push({
        year: lastFitted + k, price: null, value: null, trend: null,
        future: trendValueAt(fit, lastFitted + k),
      });
    }
    return rows;
  }, [idx, fit]);

  /**
   * ⚠ A LOG AXIS CANNOT PLOT ZERO OR LESS, AND THE INDEX GOES NEGATIVE. A cash-burn or loss year is
   * a real observation — `rebase` deliberately keeps it, and on a linear axis it drew below zero.
   * Here it has nowhere to go, so it is nulled for the chart and COUNTED, because a year silently
   * missing from an earnings line is the one a reader most needs to know about.
   */
  const posOnly = (v: number | null) => (v != null && v > 0 ? v : null);
  const chartRows = useMemo(() => indexData.map((r) => ({
    year: r.year,
    price: posOnly(r.price), value: posOnly(r.value),
    trend: posOnly(r.trend), future: posOnly(r.future),
    priceTo: null as number | null,
  })), [indexData]);
  const hiddenByLog = indexData.filter((r) => r.value != null && r.value <= 0).length;


  const priceCagr = cagrOf(points, (p) => p.price);
  const valueCagr = cagrOf(points, (p) => p.value);

  // The latest ACTUALS, in per-share currency — the chart is indexed, the calculator is not.
  const latestPs = [...points].reverse().find((p) => p.value != null)?.value ?? null;
  const latestPrice = [...points].reverse().find((p) => p.price != null)?.price ?? null;
  const lastPriceYear = [...points].reverse().find((p) => p.price != null)?.year ?? null;
  /** ⚠ THE LAST YEAR WITH A FIGURE, NOT WITH A PRICE, and the two do come apart: a company that
   *  has closed a year but not yet filed it has the price and no EPS. Cutting the forecast ladder
   *  at the price year would then drop the one estimate that matters most — the current year's,
   *  the only one still genuinely unreported. */
  const lastValueYear = [...points].reverse().find((p) => p.value != null)?.year ?? null;

  /**
   * The fitted trend at any year, converted back to per-share currency.
   *
   * ⚠ CONVERTED FROM THE PLOTTED INDEX, NOT RE-FITTED. A second `logLinearFit` on the raw series
   * would give the same slope (the fit is scale-invariant) but would be a second computation of
   * the same line — and a calculator quoting a forecast the chart does not draw is exactly the
   * kind of drift the rest of this folder is built to prevent. index/100 × the anchor's raw value.
   *
   * ⚠ ONE FUNCTION, TWO CALLERS. The calculator's forecast and the forward-multiple ladder both
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
   * The forecast per-share figures the forward multiple divides today's price by — analyst
   * consensus where one is published, and NOTHING where one is not.
   *
   * ⚠ NOT THE TREND, EVEN THOUGH THE TREND IS RIGHT THERE. `trendPsAt` is used two lines up for
   * the price-target calculator, where an extrapolation is the declared purpose and the user can
   * type over it. On the multiple chart it would be a dotted line the reader has every reason to
   * take for the market's own expectation — so the FCF basis simply has no forward half, and the
   * chart says why. See `forwardFigures`.
   */
  const forward = forwardFigures(metrics ?? [], b, lastValueYear);

  // ⚠ DERIVED FROM THE SAME TWO LINES THE CHART ABOVE PLOTS, not from GuruFocus's own
  // `Valuation Ratios__FCF Yield %` (or its P/E) — whose denominator convention (year-end price?
  // average market cap?) we do not control. One source, so the two charts cannot disagree.
  const yields = useMemo(
    () => points.map((p) => ({ year: p.year, yld: yieldOf(p.value, p.price) })), [points]);
  const yieldValues = yields.map((y) => y.yld).filter((v): v is number => v != null);
  const avgYield = meanOf(yieldValues);
  const latestYield = [...yields].reverse().find((y) => y.yld != null)?.yld ?? null;

  /**
   * ⚠ THE CALCULATOR'S TWO INPUTS LIVE HERE, NOT IN IT. The chart draws the price line out to the
   * target, so both need the same answer — and a callback from a child during render is the
   * cascading-update pattern React (and the lint rule) rightly refuses. State up, values down.
   */
  const [fcfStr, setFcfStr] = useState<string | null>(null);
  const [yieldStr, setYieldStr] = useState<string | null>(null);
  /**
   * ⚠ SWITCHING BASIS CLEARS BOTH OVERRIDES, and that is not tidiness. A hand-typed "forecast 12.40"
   * is 12.40 of FREE CASH FLOW per share; carried onto EPS it silently becomes a forecast of
   * earnings — a number the user never entered, feeding a price target and a CAGR that look
   * entirely reasonable. Same for a demanded yield: 4% of FCF and 4% of earnings are different
   * demands. The defaults for the new basis are recomputed from its own series.
   */
  const switchBasis = (next: Basis) => {
    if (next === basis) return;
    setBasis(next); setFcfStr(null); setYieldStr(null);
  };
  const asNum = (s: string | null) => {
    if (s == null || s.trim() === '') return null;
    const v = parseFloat(s);
    return Number.isFinite(v) ? v : null;
  };
  /**
   * The price everything in the calculator is measured FROM, and the date it belongs to.
   *
   * ⚠ ONE PAIR, NEVER TWO. The price and its date travel together — a live figure annualised over
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
   * ⚠ IT IS NOT `PROJECT_YEARS` ANY MORE. The forecast sits two years past the last REPORTED year;
   * from a live price that is between one and two years away, depending how stale the accounts
   * are. Holding the divisor at 2 while moving the start to today understates the return by
   * exactly the reporting lag — the CAGR would quietly get worse the fresher the price got.
   * Falls back to `PROJECT_YEARS`, which is what the distance IS when the price is the fiscal one.
   */
  const targetDate = addYears(lastFiscalPriceDate, PROJECT_YEARS);
  const horizonYears = yearsBetween(priceDate, targetDate) ?? PROJECT_YEARS;

  const target = priceTarget(
    latestPs, currentPrice,
    asNum(fcfStr) ?? forecastPs, asNum(yieldStr) ?? avgYield, horizonYears);

  /**
   * The price line carried out to the target, in index units.
   *
   * ⚠ THIS IS THE TARGET, NOT A FORECAST OF THE PRICE. It goes where the demanded yield says it
   * goes — draw it like an observation and the chart would claim to predict the market. Hence the
   * separate series, drawn like the FCF projection: thin and dotted.
   *
   * ⚠ IT STARTS AT TODAY'S PRICE, WHICH IS WHY THE AXIS IS NUMERIC. The line's two endpoints are
   * the two numbers the calculator prints — current price and forecast price — so on a LOG axis
   * its slope IS the CAGR beside it. That only holds if "today" sits at its true distance along
   * the axis: on the category axis this used to be, a mid-year date had nowhere to go, the line
   * started at the year-old fiscal close instead, and it drew a different (gentler) rate than the
   * panel stated. `type="number"` gives today a fractional year (`nowX`), and `targetX − nowX` is
   * `horizonYears` by construction — the same quantity the CAGR is divided by, not a second one.
   *
   * With no live price `nowX` collapses onto `lastPriceYear` and the line is exactly what it was.
   */
  const anchorPrice = idx.anchor == null ? null
    : points.find((p) => p.year === idx.anchor)?.price ?? null;
  const targetYear = lastPriceYear == null ? null : lastPriceYear + PROJECT_YEARS;
  /** Where today falls on the fiscal-year axis: the last reported year plus the fraction of a
   *  year since that year end. Zero when the price IS the fiscal close. */
  const nowX = lastPriceYear == null ? null
    : lastPriceYear + (yearsBetween(lastFiscalPriceDate, priceDate) ?? 0);
  // ⚠ NOT WRAPPED IN `useMemo`. The React Compiler could not preserve a manual memo here and so
  // skipped optimising the whole component — worse than the memo was worth. Left plain, the
  // compiler memoizes it itself.
  const forecastPrice = target.forecastPrice;
  const currentIndex = anchorPrice != null && anchorPrice > 0 && currentPrice != null
    ? currentPrice / anchorPrice * 100 : null;
  const chartData = anchorPrice == null || anchorPrice <= 0 || lastPriceYear == null
    || forecastPrice == null || nowX == null || currentIndex == null
    ? chartRows
    : [
      ...chartRows.map((r) => (
        // The start point rides an EXISTING row only when today and the fiscal year end coincide
        // (the fallback price). Otherwise it gets its own row below, at its own x.
        r.year === nowX ? { ...r, priceTo: currentIndex }
          : r.year === targetYear ? { ...r, priceTo: forecastPrice / anchorPrice * 100 }
            : r)),
      ...(nowX === lastPriceYear ? [] : [{
        year: nowX, price: null, value: null, trend: null, future: null, priceTo: currentIndex,
      }]),
      // The target year is normally already on the axis (the FCF projection put it there). It is
      // NOT when the company stopped reporting FCF before its last priced year — and a target
      // endpoint with no row is a line with one point: a lone dot, and a legend quoting a CAGR
      // for a line nobody can see.
      ...(chartRows.some((r) => r.year === targetYear) ? [] : [{
        year: targetYear as number, price: null, value: null, trend: null, future: null,
        priceTo: forecastPrice / anchorPrice * 100,
      }]),
      // Sorted, because a numeric axis draws a line in DATA order, not x order — an out-of-place
      // row makes the line double back on itself.
    ].sort((a, b) => a.year - b.year);

  /** Integer years only. A numeric axis would otherwise tick at 2025.8 — and the fractional x is
   *  a position for today, not a period anyone reports in. */
  const yearTicks = useMemo(() => chartRows.map((r) => r.year), [chartRows]);

  const logDomain = useMemo(() => paddedLogDomain(
    chartData.flatMap((r) => [r.price, r.value, r.trend, r.future, r.priceTo])
      .filter((v): v is number => v != null)),
  [chartData]);

  const pct = (v: number | null | undefined) => (v == null ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(1)}%`);
  const yld = (v: number | null | undefined) => (v == null ? '—' : `${v.toFixed(2)}%`);

  /**
   * The FCF | EPS switch.
   *
   * ⚠ IT IS RENDERED EVEN ON THE EMPTY STATE BELOW, which is why it lives in a variable. A company
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
          className={`px-2.5 py-1 text-[11px] font-medium transition-colors ${
            basis === k ? 'bg-accent-600 text-white' : 'text-fg-muted hover:bg-overlay/5'}`}>
          {BASIS[k].tab}
        </button>
      ))}
    </div>
  );

  if (err) return <p className="text-xs text-neg-300 py-16 text-center">{err}</p>;
  if (metrics == null) return <p className="text-xs text-fg-subtle py-16 text-center">Loading…</p>;
  if (!points.some((p) => p.price != null) || !points.some((p) => p.value != null)) {
    return (
      <div className="py-16 flex flex-col items-center gap-3">
        <p className="text-[11px] text-fg-faint text-center">
          No share price / {b.perShare} history ingested for {name ?? isin}.
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
    // ⚠ ONLY THE CALCULATOR IS PLACED, AND IT IS PLACED SO IT CAN STAY LAST IN THE DOM. Getting
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
        <h4 className="text-base font-semibold text-fg-strong">Price vs {b.perShare}</h4>
        {idx.anchor != null && (
          <span className="text-[11px] text-fg-faint">
            indexed to 100 at FY{idx.anchor} · log scale
          </span>
        )}
        {hiddenByLog > 0 && (
          // Named, not dropped: on a linear axis these plotted below zero, and a cash-burn or loss
          // year vanishing without a word is exactly the observation a reader must not lose.
          <span className="text-[11px] text-warn-300"
            title="A log axis has no room for zero or a negative value. Those years are also excluded from the trend fit, for the same reason — a loss has no logarithm.">
            ⚠ {hiddenByLog} {b.negativeYear} year{hiddenByLog > 1 ? 's' : ''} not plottable on a log axis
          </span>
        )}
        {/* ⚠ ONE SWITCH, THREE PANELS. It sits on the primary chart, and the yield card and the
            calculator beside it re-label themselves with it — which is how a reader learns they
            are all reading the same basis. */}
        <div className="ml-auto self-center">{basisSwitch}</div>
      </div>

      <div className="flex flex-wrap gap-2">
        <Stat label="Price CAGR" value={pct(priceCagr?.pct)} color={chartTheme.accentStrong}
          info={<InfoTip content={<AspectCard
            what="Compound annual growth of the fiscal year-end share price."
            where="GuruFocus `Month End Stock Price`, the close at each fiscal year end."
            when={priceCagr ? `${priceCagr.from} → ${priceCagr.to} (${priceCagr.years} years).` : 'Not computable.'}
            how="First to last positive observation. The year-end price, not today's quote." />} />} />
        <Stat label={`${b.perShare} CAGR`} value={pct(valueCagr?.pct)} color={chartTheme.warn}
          info={<InfoTip content={<AspectCard
            what={`Compound annual growth of ${b.what}.`}
            where={b.source}
            when={valueCagr ? `${valueCagr.from} → ${valueCagr.to} (${valueCagr.years} years).` : 'Not computable.'}
            how={`Per SHARE, so buybacks flatter it and issuance dilutes it — which is the point: it is what accrues to one share you own. A gap to the price CAGR is the rerating, and a rerating is not repeatable. ${b.caveat}`} />} />} />
      </div>

      {/* ⚠ INDEXED, NOT DUAL-AXIS. €700 of price and €20 of earnings share no axis, and two
          independently-scaled axes let any pair of series be made to look correlated — the
          rescaling is invisible and the reader has no way to check it. One axis, one base year. */}
      <div>
        {idx.anchor == null ? (
          // Rebasing off a cash-burn or loss year divides by a negative and flips every later
          // point, so a company with no positive year gets no index at all — see `rebase`.
          <p className="text-[11px] text-fg-faint py-16 text-center">
            No fiscal year has both a positive price and positive {b.perShare}, so there is no base to index from.
          </p>
        ) : (
          <>
            <ResponsiveContainer width="100%" height={CHART_HEIGHT}>
              <ComposedChart data={chartData} margin={{ top: 5, right: 12, bottom: 5, left: 4 }}
                style={{ cursor: 'pointer' }} onClick={() => setShowInputs(true)}>
                <CartesianGrid strokeDasharray="3 3" stroke={chartTheme.gridEarnings} />
                {/* ⚠ NUMERIC, NOT CATEGORICAL — so today can sit at its real distance between two
                    fiscal years (see `nowX`). Ticks are pinned to the reported years; the default
                    numeric ticks would invent 2025.5 as though something were reported there. */}
                <XAxis dataKey="year" type="number" domain={['dataMin', 'dataMax']}
                  ticks={yearTicks} allowDecimals={false} interval="preserveStartEnd"
                  tick={{ fontSize: 11, fill: chartTheme.axisTick }} />
                {/* ⚠ LOG SCALE, WHICH IS THE POINT: the fit is log-linear, so a constant-growth
                    series is a STRAIGHT line here and the R² above it becomes something the reader
                    can check by eye rather than take on trust. On a linear axis a 0.4 and a 0.95
                    both look like curves. */}
                <YAxis scale="log" domain={logDomain ?? ['dataMin', 'dataMax']} allowDataOverflow
                  tick={{ fontSize: 11, fill: chartTheme.axisTick }} width={52}
                  tickFormatter={(v: number) => v.toFixed(0)} />
                <Tooltip contentStyle={chartTheme.tooltipCard.contentStyle} labelStyle={{ color: chartTheme.axisLabel }}
                  // A fractional x is today, not a fiscal year — printing "2025.8" as the heading
                  // would read like a period the company reported.
                  labelFormatter={(v) => (typeof v === 'number' && !Number.isInteger(v)
                    ? 'Today' : `FY${v}`)}
                  formatter={(v, n) => [typeof v === 'number' ? v.toFixed(0) : '—',
                    n === 'price' ? 'Price (index)'
                      : n === 'priceTo' ? 'Price target (index)' : `${b.perShare} (index)`]} />
                <ReferenceLine y={100} stroke={chartTheme.zeroLine} />
                <Line dataKey="price" name="price" type="monotone" stroke={chartTheme.accentStrong}
                  strokeWidth={2} dot={{ r: 2.5 }} connectNulls />
                <Line dataKey="value" name="value" type="monotone" stroke={chartTheme.warn}
                  strokeWidth={2} dot={{ r: 2.5 }} connectNulls />
                {/* The fitted exponential through the per-share series, and its continuation. ⚠ TWO SERIES,
                    NOT ONE: the projected stretch is drawn thinner and more transparent because it
                    is arithmetic carried past the data, not a fit to anything. One line would make
                    the last two years look as observed as the first eight. */}
                <Line dataKey="trend" name="trend" type="monotone" stroke={chartTheme.warn}
                  strokeWidth={1.5} strokeDasharray="5 3" strokeOpacity={0.75} dot={false} connectNulls />
                <Line dataKey="future" name="future" type="monotone" stroke={chartTheme.warn}
                  strokeWidth={1.5} strokeDasharray="2 4" strokeOpacity={0.5} dot={false} connectNulls />
                {/* Where the price has to go to hit the demanded yield. ⚠ NOT A PRICE FORECAST —
                    it is the calculator's target, drawn in the price colour but dotted like the
                    FCF projection so it cannot be mistaken for an observation.
                    Its first dot is TODAY'S price, sitting between two fiscal years; its slope on
                    this log axis is the panel's "Est. CAGR to FY{targetYear}", not a second rate. */}
                <Line dataKey="priceTo" name="priceTo" type="linear" stroke={chartTheme.accentStrong}
                  strokeWidth={1.5} strokeDasharray="2 4" strokeOpacity={0.5}
                  dot={{ r: 2.5, strokeWidth: 0, fill: chartTheme.accentStrong }} connectNulls />
              </ComposedChart>
            </ResponsiveContainer>
            <div className="flex justify-center flex-wrap gap-x-4 gap-y-1 text-xs mt-1">
              <span className="flex items-center gap-1.5"><span className="w-3 h-0.5 inline-block rounded" style={{ background: chartTheme.accentStrong }} />Share price</span>
              <span className="flex items-center gap-1.5"><span className="w-3 h-0.5 inline-block rounded" style={{ background: chartTheme.warn }} />{b.perShare}</span>
              {/* The target line names its own rate. ⚠ It DISPLAYS `target.cagr` — the panel's
                  figure, not a second one computed for the legend. */}
              {target.cagr != null && targetYear != null && (
                <span className="flex items-center gap-1.5 text-fg-muted"
                  title={`From ${priceLive ? "today's" : "the last fiscal year-end"} price to the target, over ${horizonYears.toFixed(1)} years. The same figure as “Est. CAGR to FY${targetYear}” in the panel.`}>
                  <span className="w-3 h-0.5 inline-block rounded"
                    style={{ background: chartTheme.accentStrong, opacity: 0.5 }} />
                  Price target — {pct(target.cagr * 100)}/yr to FY{targetYear}
                </span>
              )}
              {fit.r2 != null && (
                <span className="flex items-center gap-1.5 text-fg-muted"
                  title={`Exponential fit over ${fit.n} year(s)${fit.dropped ? `, ${fit.dropped} dropped (${b.negativeYear} years have no logarithm)` : ''}. R² is how tightly ${b.perShare} hugs a constant-growth line: 1.0 = perfectly steady compounding.`}>
                  <span className="w-3 h-0.5 inline-block rounded"
                    style={{ background: chartTheme.warn, opacity: 0.75 }} />
                  Trend (R² {fit.r2.toFixed(2)}), dotted = {PROJECT_YEARS}y projection
                </span>
              )}
            </div>
          </>
        )}
      </div>

      {showInputs && (
        // ⚠ HANDED THE SERIES, NOT THE ISIN — one computation, so the table cannot disagree with
        // the line that opened it.
        <QuickValuationInputsModal points={points} index={idx} currency={currency} basis={b}
          name={name} isin={isin} onClose={() => setShowInputs(false)} />
      )}
    </div>

    <div className="rounded-xl border border-neutral-800/40 bg-card p-4 space-y-3 min-w-0">
      <div className="flex items-baseline gap-2 flex-wrap">
        <h4 className="text-base font-semibold text-fg-strong">{b.yieldTitle}</h4>
        <span className="text-[11px] text-fg-faint">{b.perShare} ÷ year-end price · average dashed</span>
      </div>

      <div className="flex flex-wrap gap-2">
        <Stat label="Avg" value={yld(avgYield)} color={chartTheme.accent}
          info={<InfoTip content={<AspectCard
            what={`The average ${b.yieldInline} over the years shown — the dashed line.`}
            where="Computed here from the same two lines the chart above plots, not from GuruFocus's own ratio (whose denominator convention we don't control)."
            when={`${yieldValues.length} of the last ${YEARS} fiscal years.`}
            how="A simple mean of the yearly yields. A yield doesn't compound, so there is no growth rate to quote." />} />} />
        <Stat label="Latest" value={yld(latestYield)} color={chartTheme.accent}
          info={<InfoTip content={<AspectCard
            what={`The most recent fiscal year's ${b.yieldInline}.`}
            where={`That year's ${b.perShare} ÷ that year's closing price.`}
            when="The last fiscal year with a price — up to a year ago, not today."
            how="Above the average = the shares are cheaper on this measure than they usually have been, on the year-end price." />} />} />
      </div>

      {/* ⚠ A YIELD, NOT A MULTIPLE — SO NEGATIVES STAY. A cash-burn or loss year is −5% here, which
          reads as what it is; the same year as P/FCF (or P/E) would be −20x and sort below every
          cheap year on the axis, as though it were the bargain of the decade. */}
      <div>
        <ResponsiveContainer width="100%" height={CHART_HEIGHT}>
          <ComposedChart data={yields} margin={{ top: 5, right: 12, bottom: 5, left: 4 }}
            style={{ cursor: 'pointer' }} onClick={() => setShowInputs(true)}>
            <CartesianGrid strokeDasharray="3 3" stroke={chartTheme.gridEarnings} />
            <XAxis dataKey="year" tick={{ fontSize: 11, fill: chartTheme.axisTick }} />
            <YAxis domain={paddedDomain(yieldValues)} tick={{ fontSize: 11, fill: chartTheme.axisTick }}
              width={52} tickFormatter={(v: number) => `${v.toFixed(0)}%`} />
            <Tooltip contentStyle={chartTheme.tooltipCard.contentStyle} labelStyle={{ color: chartTheme.axisLabel }}
              formatter={(v) => [typeof v === 'number' ? `${v.toFixed(2)}%` : '—', b.yieldTitle]} />
            <ReferenceLine y={0} stroke={chartTheme.zeroLine} />
            {avgYield != null && (
              <ReferenceLine y={avgYield} stroke={chartTheme.accent} strokeDasharray="5 3" strokeOpacity={0.6} />
            )}
            <Line dataKey="yld" name="yld" type="monotone" stroke={chartTheme.accent}
              strokeWidth={2} dot={{ r: 2.5 }} connectNulls />
          </ComposedChart>
        </ResponsiveContainer>
        <div className="flex justify-center flex-wrap gap-x-4 gap-y-1 text-xs mt-1">
          <span className="flex items-center gap-1.5"><span className="w-3 h-0.5 inline-block rounded" style={{ background: chartTheme.accent }} />{b.yieldTitle} (avg dashed)</span>
        </div>
      </div>
    </div>

    {/* Bottom-right, by auto-flow. Handed the series and the price, never the ISIN — same rule as
        the drill-down modal, so it cannot disagree with the charts about what the company earned. */}
    <ForwardMultipleChart height={CHART_HEIGHT}
      points={points} basis={b} currency={currency}
      currentPrice={currentPrice} priceLive={priceLive} nowX={nowX}
      forward={forward} yearTicks={yearTicks} />

    {/* Top-right, but LAST IN THE DOM — see the grid note above. The only non-chart card, so it
        fills a cell sized by the chart beside it: a flex column with its CAGR footer pinned to the
        bottom rather than left floating halfway up a stretched card. */}
    <PriceTargetCalculator
      className="lg:col-start-2 lg:row-start-1"
      target={target} years={PROJECT_YEARS} currency={currency} basis={b}
      horizonYears={horizonYears} targetYear={targetYear}
      price={{ date: priceDate, live: priceLive, pending: livePending,
        symbol: live?.symbol ?? null, staleDays: live?.stale_days ?? null }}
      fcfStr={fcfStr} onFcf={setFcfStr} defaultForecastFcfPs={forecastPs}
      yieldStr={yieldStr} onYield={setYieldStr} defaultForecastYield={avgYield}
      onReset={() => { setFcfStr(null); setYieldStr(null); }} />
    </div>
  );
}
