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
import { useLang } from '../../../lib/i18n';
import { chartTitle } from './longEquityCopy';
import { Stat } from './MetricGrowthCard';
import { pairedSpan, RatioStats } from './CardStats';
import { LegendItem } from './ChartLegend';
import { type Target } from './HoldingsRevenueModal';
import DividendYieldInputsModal from './DividendYieldInputsModal';
import DailyToggle from './DailyToggle';
import { coverageByYear, dividendYieldByYear, type DividendYieldInputs } from './dividendYieldData';
import { paddedDomain, xToMonth, xToPeriod } from './marginData';
import { periodAxis } from '../../../lib/chartAxis';
import { benchNote, benchmarkFirst, mergeSeries, useBenchInputs, withBench, type BenchTarget } from './benchSeries';

/**
 * Dividend yield card: Dividends per Share ÷ the fiscal year-end share price, per fiscal year, on
 * a LINEAR % axis (a ratio, not a compounding series — no log / exponential trend). Click through
 * to the two base lines per company.
 *
 * ⚠ THIS REPLACED A "DIVIDEND / SHARE" CARD, AND THE UNIT IS THE WHOLE REASON. A per-share amount
 * has no portfolio-level meaning — there is no portfolio share, the amounts sit in different
 * currencies, and the level rule rebases each holding to 100 at its first year, which a dividend
 * series starting at 0.00 cannot survive. The portfolio card was therefore permanently empty while
 * every holding carried the line. A yield is currency-free, so the weight-weighted average IS the
 * book's yield (these are value weights), and a non-payer contributes a true 0 instead of being
 * dropped. Mirrors {@link ./FcfSbcYieldCard}.
 */

export default function DividendYieldCard({ holdingsTarget, holdingsName, benchTarget }: {
  holdingsTarget: Target; holdingsName?: string | null;
  /** The index drawn beside the book — same endpoint, same helper. See `benchSeries`. */
  benchTarget?: BenchTarget | null;
}) {
  // ⚠ READ FROM THE STORE, NOT DRILLED THROUGH `LongEquityTab` AS A PROP. Fourteen sibling
  // cards would mean fourteen chances to forget one, and a card left on English would look
  // like a missing translation rather than a missing prop. `useLang` is an external store
  // (see `lib/i18n.ts`), so every card reads the one value directly.
  const [lang] = useLang();
  const [data, setData] = useState<DividendYieldInputs | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [showInputs, setShowInputs] = useState(false);
  /**
   * Daily is a PER-CARD override of the tab's cadence, and only these two yield cards offer it.
   *
   * ⚠ IT IS NOT A THIRD SETTING ON THE TAB TOGGLE. A yield is the only shape here with a daily
   * input: the denominator is a price, which moves every trading day. The other ten cards are pure
   * accounting — revenue, margins, debt ratios — and a tab-wide "Daily" would blank all of them.
   */
  const [daily, setDaily] = useState(false);
  // ⚠ Memoised, and `daily` is a dep: it is the effect key below AND the modal's target, so a
  // fresh object each render would refetch forever while a stale one would leave the drill-down
  // showing a different cadence from the chart it opened from.
  const target = useMemo(
    () => (daily ? { ...holdingsTarget, cadence: 'daily' } : holdingsTarget),
    [holdingsTarget, daily]);

  useEffect(() => {
    let alive = true;
    void (async () => {
      setData(null); setErr(null);
      try {
        const r = await apiFetch(`${API_URL}/api/earnings/dividend-yield-inputs`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(target),
        });
        const b = await r.json().catch(() => null);
        if (!alive) return;
        if (!r.ok) { setErr(b?.detail ?? `HTTP ${r.status}`); return; }
        setData(b as DividendYieldInputs);
      } catch (e) {
        if (alive) setErr(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { alive = false; };
  }, [target]);

  const yieldByYr = useMemo(() => dividendYieldByYear(data?.rows ?? []), [data]);
  const covByYr = useMemo(() => coverageByYear(data?.rows ?? []), [data]);

  const [benchData, benchErr] = useBenchInputs<DividendYieldInputs>('dividend-yield-inputs', benchTarget);
  const benchByYr = useMemo(
    () => (benchData ? dividendYieldByYear(benchData.rows) : null), [benchData]);

  const note = benchNote(benchTarget, benchData, benchErr, benchByYr);

  const chartData = useMemo(
    () => mergeSeries(yieldByYr, benchByYr, 'yld'), [yieldByYr, benchByYr]);

  const own = holdingsName ?? 'Dividend yield';
  /**
   * The book's figures and the benchmark's, over the ONE window both lines cover — see
   * `CardStats`/`sharedSpan`. ⚠ COMPUTED ONCE: `own.avg` is BOTH the tile and the dashed average
   * line on the chart below, so the card cannot plot a mean it does not print.
   */
  const stats = useMemo(() => pairedSpan(yieldByYr, benchByYr), [yieldByYr, benchByYr]);
  const avg = stats.own.avg;
  // The latest year's coverage — a yield averaged over 40% of the book is not the book's yield.
  // ⚠ KEYED ON THE PERIOD THE `Latest` TILE ACTUALLY PRINTS (`own.latestX`), not on this line's own
  // newest. With a benchmark on screen the tiles are pinned to the shared window, so reading
  // coverage off a later year would report the share of the book behind a figure that is not shown.
  const latestCov = stats.own.latestX != null ? covByYr.get(stats.own.latestX) ?? null : null;
  const pct = (v: number | null) => (v == null ? '—' : `${v.toFixed(2)}%`);

  return (
    <div className="rounded-xl border border-neutral-800/40 bg-card p-4 space-y-3 min-w-0">
      <div className="flex items-baseline justify-between gap-2">
        <h4 className="text-base font-semibold text-fg-strong">{chartTitle(lang, 'dividendYield')}</h4>
        <DailyToggle on={daily} onChange={setDaily}
          note={'Daily: the dividend per share stays flat between fiscal periods while the PRICE '
            + 'moves every trading day — a trailing yield. Off, it follows the tab’s cadence.'} />
      </div>

      {data == null && !err ? (
        <p className="text-xs text-fg-subtle py-16 text-center">Loading…</p>
      ) : err ? (
        <p className="text-xs text-neg-300 py-16 text-center">{err}</p>
      ) : yieldByYr.size === 0 ? (
        <p className="text-[12px] text-fg-faint py-16 text-center">No dividend / price figures ingested to compute a yield.</p>
      ) : (
        <>
          <RatioStats stats={stats} benchLabel={benchTarget?.label} fmt={pct}
            avgInfo={<InfoTip content={<AspectCard
              what="Average dividend yield over the years shown."
              where="Computed here — dividends per share ÷ that fiscal year's end price, per holding, then weight-averaged."
              when="The years on the chart."
              how="A yield is currency-free, so the weighted average IS the book's yield (the weights are value weights). A company that pays nothing counts as 0%; one we have no dividend line for is left out and the year renormalises over the rest." />} />}>
            {/* ⚠ THE BOOK'S COVERAGE, AND ONLY THE BOOK'S — passed as a child so it lands after
                both pairs. The index has its own (very different) coverage; showing one figure
                under a row that carries two lines would read as if it described both. */}
            {latestCov != null && latestCov < 99.5 && (
              <Stat label="Coverage" value={`${latestCov.toFixed(0)}%`}
                tone={latestCov < 60 ? 'text-warn-300' : undefined}
                info={<InfoTip content={<AspectCard
                  what="The share of the book the latest year's yield is computed over."
                  where="Holdings with both a dividend line and a price that year."
                  when="The latest year on the chart."
                  how="Cash, funds and holdings with nothing ingested are not in the average — a yield over part of a book is not the book's yield, so the share is stated rather than assumed." />} />} />
            )}
          </RatioStats>

          <div>
            <ResponsiveContainer width="100%" height={320}>
              <ComposedChart data={chartData} margin={{ top: 5, right: 12, bottom: 5, left: 4 }}
                style={{ cursor: 'pointer' }} onClick={() => setShowInputs(true)}>
                <CartesianGrid strokeDasharray="3 3" stroke={chartTheme.gridEarnings} />
                <XAxis {...periodAxis(daily ? xToMonth : xToPeriod)} />
                <YAxis domain={paddedDomain(withBench(yieldByYr.values(), benchByYr))} tick={{ fontSize: 12, fill: chartTheme.axisTick }} width={48}
                  tickFormatter={(v: number) => `${v.toFixed(0)}%`} />
                <Tooltip contentStyle={chartTheme.tooltipCard.contentStyle} labelStyle={{ color: chartTheme.axisLabel }} itemSorter={benchmarkFirst}
                  formatter={(v, n) => [`${typeof v === 'number' ? v.toFixed(2) : '—'}%`, n === 'bench' ? (benchTarget?.label ?? 'Benchmark') : own]} />
                <ReferenceLine y={0} stroke={chartTheme.zeroLine} />
                {avg != null && <ReferenceLine y={avg} stroke={chartTheme.accent} strokeDasharray="5 3" strokeOpacity={0.6} />}
                {/* ⚠ NO DOTS ON A DAILY SERIES — 2,700 markers is a solid band, not a line. */}
                <Line dataKey="yld" name="yld" type="monotone" stroke={chartTheme.accent} strokeWidth={2} dot={daily ? false : { r: 2.5 }} connectNulls />
                {benchByYr && <Line dataKey="bench" name="bench" type="monotone" stroke={chartTheme.pos} strokeWidth={2} dot={{ r: 2 }} connectNulls />}
              </ComposedChart>
            </ResponsiveContainer>
            <div className="flex justify-center flex-wrap gap-x-4 gap-y-1 text-xs mt-1">
              <LegendItem color={chartTheme.accent} label={own} />
              {avg != null && <LegendItem color={chartTheme.accent} stroke="dashed"
                label={`${own} average`} />}
              {benchByYr && <LegendItem color={chartTheme.pos} label={benchTarget?.label} />}
              {note && (
                <span className="text-fg-faint" title="An overlay that simply does not appear is indistinguishable from an index that matches this book exactly. Full detail is in the console.">
                  {note}
                </span>
              )}
            </div>
          </div>
        </>
      )}

      {showInputs && (
        <DividendYieldInputsModal target={target} portfolioName={holdingsName}
          benchTarget={benchTarget} benchLabel={benchTarget?.label ?? null}
          onClose={() => setShowInputs(false)} />
      )}
    </div>
  );
}
