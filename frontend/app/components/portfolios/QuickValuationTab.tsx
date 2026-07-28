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
import {
  cagrOf, fcfYieldOf, priceTarget, priceVsFcf, rebase, type MetricRow,
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
 * ⚠ THE PRICE IS THE FISCAL YEAR-END CLOSE, NOT TODAY'S. Both series come from the same fiscal
 * rows, which is what makes the comparison internally consistent — but the newest point can be up
 * to a year old, so this is a read on the last reported year, not a live quote.
 */

const YEARS = 10;
/** How far the fitted FCF trend is carried past the last reported year. */
const PROJECT_YEARS = 2;
/** Both charts share it, so the two grid cells match without either card padding out the gap. */
const CHART_HEIGHT = 320;

export default function QuickValuationTab({ isin, name }: { isin: string; name?: string | null }) {
  const [metrics, setMetrics] = useState<MetricRow[] | null>(null);
  const [currency, setCurrency] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [showInputs, setShowInputs] = useState(false);

  useEffect(() => {
    let alive = true;
    void (async () => {
      setMetrics(null); setErr(null);
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

  const points = useMemo(() => priceVsFcf(metrics ?? [], YEARS), [metrics]);
  const idx = useMemo(() => rebase(points), [points]);

  /**
   * The exponential trend through FCF/share, and its R².
   *
   * ⚠ FITTED ON THE INDEX, WHICH CHANGES NOTHING. A log-linear fit is invariant to scaling —
   * ln(k·v) = ln k + ln v shifts the intercept and leaves the slope and R² alone — so this R² is a
   * property of the cash-flow series itself, not of the base year the chart happens to use.
   *
   * A cash-burn year has no logarithm and is dropped by the fit (`fit.dropped`), so the trend
   * spans fewer years than the chart when one occurs.
   */
  const fit = useMemo(
    () => logLinearFit(idx.rows
      .filter((r): r is { year: number; price: number | null; fcf: number } => r.fcf != null)
      .map((r) => ({ year: r.year, value: r.fcf }))),
    [idx]);

  const indexData = useMemo(() => {
    const trendByYear = new Map(fit.trend.map((t) => [t.year, t.value]));
    const rows = idx.rows.map((r) => ({
      year: r.year, price: r.price, fcf: r.fcf,
      trend: trendByYear.get(r.year) ?? null,
      // The projection starts AT the last fitted year so the two segments meet rather than leaving
      // a gap; that shared point carries both keys.
      future: null as number | null,
    }));
    const lastFitted = fit.trend.length ? fit.trend[fit.trend.length - 1].year : null;
    if (lastFitted == null) return rows;
    for (const r of rows) if (r.year === lastFitted) r.future = trendByYear.get(lastFitted) ?? null;
    // ⚠ PRICE AND FCF ARE NULL IN THESE ROWS. Extending them would draw observed data into years
    // nobody has reported; only the trend continues.
    for (let k = 1; k <= PROJECT_YEARS; k++) {
      rows.push({
        year: lastFitted + k, price: null, fcf: null, trend: null,
        future: trendValueAt(fit, lastFitted + k),
      });
    }
    return rows;
  }, [idx, fit]);

  /**
   * ⚠ A LOG AXIS CANNOT PLOT ZERO OR LESS, AND THE FCF INDEX GOES NEGATIVE. A cash-burn year is a
   * real observation — `rebase` deliberately keeps it, and on a linear axis it drew below zero.
   * Here it has nowhere to go, so it is nulled for the chart and COUNTED, because a year silently
   * missing from a cash-flow line is the one a reader most needs to know about.
   */
  const posOnly = (v: number | null) => (v != null && v > 0 ? v : null);
  const chartRows = useMemo(() => indexData.map((r) => ({
    year: r.year,
    price: posOnly(r.price), fcf: posOnly(r.fcf),
    trend: posOnly(r.trend), future: posOnly(r.future),
    priceTo: null as number | null,
  })), [indexData]);
  const hiddenByLog = indexData.filter((r) => r.fcf != null && r.fcf <= 0).length;


  const priceCagr = cagrOf(points, (p) => p.price);
  const fcfCagr = cagrOf(points, (p) => p.fcf);

  // The latest ACTUALS, in per-share currency — the chart is indexed, the calculator is not.
  const latestFcfPs = [...points].reverse().find((p) => p.fcf != null)?.fcf ?? null;
  const latestPrice = [...points].reverse().find((p) => p.price != null)?.price ?? null;

  /**
   * The trend's value `PROJECT_YEARS` out, converted back to per-share currency.
   *
   * ⚠ CONVERTED FROM THE PLOTTED INDEX, NOT RE-FITTED. A second `logLinearFit` on the raw series
   * would give the same slope (the fit is scale-invariant) but would be a second computation of
   * the same line — and a calculator quoting a forecast the chart does not draw is exactly the
   * kind of drift the rest of this folder is built to prevent. index/100 × the anchor's raw FCF.
   */
  const forecastFcfPs = useMemo(() => {
    if (idx.anchor == null) return null;
    const anchorFcf = points.find((p) => p.year === idx.anchor)?.fcf ?? null;
    const lastFitted = fit.trend.length ? fit.trend[fit.trend.length - 1].year : null;
    if (anchorFcf == null || lastFitted == null) return null;
    const projected = trendValueAt(fit, lastFitted + PROJECT_YEARS);
    return projected == null ? null : projected / 100 * anchorFcf;
  }, [idx, points, fit]);

  // ⚠ DERIVED FROM THE SAME TWO LINES THE CHART ABOVE PLOTS, not from GuruFocus's own
  // `Valuation Ratios__FCF Yield %` — whose denominator convention (year-end price? average market
  // cap?) we do not control. One source, so the two charts cannot disagree.
  const yields = useMemo(
    () => points.map((p) => ({ year: p.year, yld: fcfYieldOf(p.fcf, p.price) })), [points]);
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
  const asNum = (s: string | null) => {
    if (s == null || s.trim() === '') return null;
    const v = parseFloat(s);
    return Number.isFinite(v) ? v : null;
  };
  const target = priceTarget(
    latestFcfPs, latestPrice,
    asNum(fcfStr) ?? forecastFcfPs, asNum(yieldStr) ?? avgYield, PROJECT_YEARS);

  /**
   * The price line carried out to the target, in index units.
   *
   * ⚠ THIS IS THE TARGET, NOT A FORECAST OF THE PRICE. It goes where the demanded yield says it
   * goes — draw it like an observation and the chart would claim to predict the market. Hence the
   * separate series, drawn like the FCF projection: thin and dotted.
   */
  const anchorPrice = idx.anchor == null ? null
    : points.find((p) => p.year === idx.anchor)?.price ?? null;
  const lastPriceYear = [...points].reverse().find((p) => p.price != null)?.year ?? null;
  const targetYear = lastPriceYear == null ? null : lastPriceYear + PROJECT_YEARS;
  // ⚠ NOT WRAPPED IN `useMemo`. The React Compiler could not preserve a manual memo here and so
  // skipped optimising the whole component — worse than the memo was worth. Left plain, the
  // compiler memoizes it itself.
  const forecastPrice = target.forecastPrice;
  const chartData = anchorPrice == null || anchorPrice <= 0 || lastPriceYear == null
    || forecastPrice == null
    ? chartRows
    : chartRows.map((r) => {
      if (r.year === lastPriceYear && r.price != null) return { ...r, priceTo: r.price };
      if (r.year === targetYear) return { ...r, priceTo: forecastPrice / anchorPrice * 100 };
      return r;
    });

  const logDomain = useMemo(() => paddedLogDomain(
    chartData.flatMap((r) => [r.price, r.fcf, r.trend, r.future, r.priceTo])
      .filter((v): v is number => v != null)),
  [chartData]);

  const pct = (v: number | null | undefined) => (v == null ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(1)}%`);
  const yld = (v: number | null | undefined) => (v == null ? '—' : `${v.toFixed(2)}%`);

  if (err) return <p className="text-xs text-neg-300 py-16 text-center">{err}</p>;
  if (metrics == null) return <p className="text-xs text-fg-subtle py-16 text-center">Loading…</p>;
  if (!points.some((p) => p.price != null) || !points.some((p) => p.fcf != null)) {
    return (
      <p className="text-[11px] text-fg-faint py-16 text-center">
        No share price / FCF-per-share history ingested for {name ?? isin}.
      </p>
    );
  }

  return (
    // A 2×2 grid of equal cells: the two charts stack down the left, the calculator spans both rows
    // on the right. `auto-rows-fr` equalises the row heights to the taller one and the default
    // `items-stretch` makes each card fill its cell — without it the rows size to their own content
    // and the two charts sit at different heights.
    <div className="grid grid-cols-1 lg:grid-cols-2 auto-rows-fr gap-4">
    {/* The Long Equity card shape: header, then the stat tiles, then the chart — all inside the one
        card, so the numbers travel with the picture they describe. */}
    <div className="rounded-xl border border-neutral-800/40 bg-card p-4 space-y-3 min-w-0">
      <div className="flex items-baseline gap-2 flex-wrap">
        <h4 className="text-base font-semibold text-fg-strong">Price vs FCF / share</h4>
        {idx.anchor != null && (
          <span className="text-[11px] text-fg-faint">
            indexed to 100 at FY{idx.anchor} · log scale
          </span>
        )}
        {hiddenByLog > 0 && (
          // Named, not dropped: on a linear axis these plotted below zero, and a cash-burn year
          // vanishing without a word is exactly the observation a reader must not lose.
          <span className="text-[11px] text-warn-300"
            title="A log axis has no room for zero or a negative value. Those years are also excluded from the trend fit, for the same reason — a loss has no logarithm.">
            ⚠ {hiddenByLog} cash-burn year{hiddenByLog > 1 ? 's' : ''} not plottable on a log axis
          </span>
        )}
      </div>

      <div className="flex flex-wrap gap-2">
        <Stat label="Price CAGR" value={pct(priceCagr?.pct)} color={chartTheme.accentStrong}
          info={<InfoTip content={<AspectCard
            what="Compound annual growth of the fiscal year-end share price."
            where="GuruFocus `Month End Stock Price`, the close at each fiscal year end."
            when={priceCagr ? `${priceCagr.from} → ${priceCagr.to} (${priceCagr.years} years).` : 'Not computable.'}
            how="First to last positive observation. The year-end price, not today's quote." />} />} />
        <Stat label="FCF/share CAGR" value={pct(fcfCagr?.pct)} color={chartTheme.warn}
          info={<InfoTip content={<AspectCard
            what="Compound annual growth of free cash flow per share."
            where="GuruFocus `Free Cash Flow per Share`, as reported."
            when={fcfCagr ? `${fcfCagr.from} → ${fcfCagr.to} (${fcfCagr.years} years).` : 'Not computable.'}
            how="Per SHARE, so buybacks flatter it and issuance dilutes it — which is the point: it is the cash accruing to one share you own. A gap to the price CAGR is the rerating, and a rerating is not repeatable." />} />} />
      </div>

      {/* ⚠ INDEXED, NOT DUAL-AXIS. €700 of price and €20 of cash flow share no axis, and two
          independently-scaled axes let any pair of series be made to look correlated — the
          rescaling is invisible and the reader has no way to check it. One axis, one base year. */}
      <div>
        {idx.anchor == null ? (
          // Rebasing off a cash-burn year divides by a negative and flips every later point, so a
          // company with no positive-FCF year gets no index at all — see `rebase`.
          <p className="text-[11px] text-fg-faint py-16 text-center">
            No fiscal year has both a positive price and positive FCF per share, so there is no base to index from.
          </p>
        ) : (
          <>
            <ResponsiveContainer width="100%" height={CHART_HEIGHT}>
              <ComposedChart data={chartData} margin={{ top: 5, right: 12, bottom: 5, left: 4 }}
                style={{ cursor: 'pointer' }} onClick={() => setShowInputs(true)}>
                <CartesianGrid strokeDasharray="3 3" stroke={chartTheme.gridEarnings} />
                <XAxis dataKey="year" tick={{ fontSize: 11, fill: chartTheme.axisTick }} />
                {/* ⚠ LOG SCALE, WHICH IS THE POINT: the fit is log-linear, so a constant-growth
                    series is a STRAIGHT line here and the R² above it becomes something the reader
                    can check by eye rather than take on trust. On a linear axis a 0.4 and a 0.95
                    both look like curves. */}
                <YAxis scale="log" domain={logDomain ?? ['dataMin', 'dataMax']} allowDataOverflow
                  tick={{ fontSize: 11, fill: chartTheme.axisTick }} width={52}
                  tickFormatter={(v: number) => v.toFixed(0)} />
                <Tooltip contentStyle={chartTheme.tooltipCard.contentStyle} labelStyle={{ color: chartTheme.axisLabel }}
                  formatter={(v, n) => [typeof v === 'number' ? v.toFixed(0) : '—',
                    n === 'price' ? 'Price (index)' : 'FCF/share (index)']} />
                <ReferenceLine y={100} stroke={chartTheme.zeroLine} />
                <Line dataKey="price" name="price" type="monotone" stroke={chartTheme.accentStrong}
                  strokeWidth={2} dot={{ r: 2.5 }} connectNulls />
                <Line dataKey="fcf" name="fcf" type="monotone" stroke={chartTheme.warn}
                  strokeWidth={2} dot={{ r: 2.5 }} connectNulls />
                {/* The fitted exponential through FCF/share, and its continuation. ⚠ TWO SERIES,
                    NOT ONE: the projected stretch is drawn thinner and more transparent because it
                    is arithmetic carried past the data, not a fit to anything. One line would make
                    the last two years look as observed as the first eight. */}
                <Line dataKey="trend" name="trend" type="monotone" stroke={chartTheme.warn}
                  strokeWidth={1.5} strokeDasharray="5 3" strokeOpacity={0.75} dot={false} connectNulls />
                <Line dataKey="future" name="future" type="monotone" stroke={chartTheme.warn}
                  strokeWidth={1.5} strokeDasharray="2 4" strokeOpacity={0.5} dot={false} connectNulls />
                {/* Where the price has to go to hit the demanded yield. ⚠ NOT A PRICE FORECAST —
                    it is the calculator's target, drawn in the price colour but dotted like the
                    FCF projection so it cannot be mistaken for an observation. */}
                <Line dataKey="priceTo" name="priceTo" type="linear" stroke={chartTheme.accentStrong}
                  strokeWidth={1.5} strokeDasharray="2 4" strokeOpacity={0.5}
                  dot={{ r: 2.5, strokeWidth: 0, fill: chartTheme.accentStrong }} connectNulls />
              </ComposedChart>
            </ResponsiveContainer>
            <div className="flex justify-center flex-wrap gap-x-4 gap-y-1 text-xs mt-1">
              <span className="flex items-center gap-1.5"><span className="w-3 h-0.5 inline-block rounded" style={{ background: chartTheme.accentStrong }} />Share price</span>
              <span className="flex items-center gap-1.5"><span className="w-3 h-0.5 inline-block rounded" style={{ background: chartTheme.warn }} />FCF / share</span>
              {fit.r2 != null && (
                <span className="flex items-center gap-1.5 text-fg-muted"
                  title={`Exponential fit over ${fit.n} year(s)${fit.dropped ? `, ${fit.dropped} dropped (no positive FCF)` : ''}. R² is how tightly FCF/share hugs a constant-growth line: 1.0 = perfectly steady compounding.`}>
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
        <QuickValuationInputsModal points={points} index={idx} currency={currency}
          name={name} isin={isin} onClose={() => setShowInputs(false)} />
      )}
    </div>

    <div className="rounded-xl border border-neutral-800/40 bg-card p-4 space-y-3 min-w-0">
      <div className="flex items-baseline gap-2 flex-wrap">
        <h4 className="text-base font-semibold text-fg-strong">FCF yield</h4>
        <span className="text-[11px] text-fg-faint">FCF / share ÷ year-end price · average dashed</span>
      </div>

      <div className="flex flex-wrap gap-2">
        <Stat label="Avg" value={yld(avgYield)} color={chartTheme.accent}
          info={<InfoTip content={<AspectCard
            what="The average FCF yield over the years shown — the dashed line."
            where="Computed here from the same two lines the chart above plots, not from GuruFocus's own FCF Yield % (whose denominator convention we don't control)."
            when={`${yieldValues.length} of the last ${YEARS} fiscal years.`}
            how="A simple mean of the yearly yields. A yield doesn't compound, so there is no growth rate to quote." />} />} />
        <Stat label="Latest" value={yld(latestYield)} color={chartTheme.accent}
          info={<InfoTip content={<AspectCard
            what="The most recent fiscal year's FCF yield."
            where="That year's FCF per share ÷ that year's closing price."
            when="The last fiscal year with a price — up to a year ago, not today."
            how="Above the average = the cash is cheaper than it usually has been, on the year-end price." />} />} />
      </div>

      {/* ⚠ A YIELD, NOT A MULTIPLE — SO NEGATIVES STAY. A cash-burn year is −5% here, which reads
          as what it is; the same year as P/FCF would be −20x and sort below every cheap year on
          the axis, as though it were the bargain of the decade. */}
      <div>
        <ResponsiveContainer width="100%" height={CHART_HEIGHT}>
          <ComposedChart data={yields} margin={{ top: 5, right: 12, bottom: 5, left: 4 }}
            style={{ cursor: 'pointer' }} onClick={() => setShowInputs(true)}>
            <CartesianGrid strokeDasharray="3 3" stroke={chartTheme.gridEarnings} />
            <XAxis dataKey="year" tick={{ fontSize: 11, fill: chartTheme.axisTick }} />
            <YAxis domain={paddedDomain(yieldValues)} tick={{ fontSize: 11, fill: chartTheme.axisTick }}
              width={52} tickFormatter={(v: number) => `${v.toFixed(0)}%`} />
            <Tooltip contentStyle={chartTheme.tooltipCard.contentStyle} labelStyle={{ color: chartTheme.axisLabel }}
              formatter={(v) => [typeof v === 'number' ? `${v.toFixed(2)}%` : '—', 'FCF yield']} />
            <ReferenceLine y={0} stroke={chartTheme.zeroLine} />
            {avgYield != null && (
              <ReferenceLine y={avgYield} stroke={chartTheme.accent} strokeDasharray="5 3" strokeOpacity={0.6} />
            )}
            <Line dataKey="yld" name="yld" type="monotone" stroke={chartTheme.accent}
              strokeWidth={2} dot={{ r: 2.5 }} connectNulls />
          </ComposedChart>
        </ResponsiveContainer>
        <div className="flex justify-center flex-wrap gap-x-4 gap-y-1 text-xs mt-1">
          <span className="flex items-center gap-1.5"><span className="w-3 h-0.5 inline-block rounded" style={{ background: chartTheme.accent }} />FCF yield (avg dashed)</span>
        </div>
      </div>
    </div>

    {/* ⚠ ORDERED AFTER BOTH CHARTS, PLACED BESIDE THEM. `lg:col-start-2 lg:row-start-1` puts it in
        the right column from the first row while leaving it LAST in the DOM — so a narrow screen
        stacks charts-then-calculator (the reading order) instead of wedging it between them. */}
    <PriceTargetCalculator
      className="lg:col-start-2 lg:row-start-1 lg:row-span-2"
      target={target} years={PROJECT_YEARS} currency={currency}
      fcfStr={fcfStr} onFcf={setFcfStr} defaultForecastFcfPs={forecastFcfPs}
      yieldStr={yieldStr} onYield={setYieldStr} defaultForecastYield={avgYield}
      onReset={() => { setFcfStr(null); setYieldStr(null); }} />
    </div>
  );
}
