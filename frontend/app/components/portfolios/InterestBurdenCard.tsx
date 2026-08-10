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
import { type Target } from './HoldingsRevenueModal';
import InterestBurdenInputsModal from './InterestBurdenInputsModal';
import { interestBurdenByYear, type InterestBurdenInputs } from './interestBurdenData';
import { meanOf, paddedDomain , xToPeriod } from './marginData';
import { benchNote, mergeSeries, useBenchInputs, withBench, type BenchTarget } from './benchSeries';

/**
 * Interest-burden card: the share of operating profit spent on interest = |Interest expense| ÷
 * Operating income per fiscal year, on a LINEAR % axis (a ratio, not a compounding series — no log
 * / exponential trend). Lower = less of profit lost to servicing debt. Click through to the two
 * base lines per company.
 *
 * ⚠ THE RATIO IS DERIVED HERE from the raw lines (`interestBurdenByYear`), so the line, the tiles
 * and the drill-down are one computation. Aggregation is a weight-weighted average of per-company
 * ratios — currency-safe, unlike summing mixed-currency amounts. Mirrors {@link ./DebtRatioCard}.
 *
 * ⚠ THIS IS THE ONE CARD WHOSE BENCHMARK LINE IS USUALLY ABSENT, AND IT IS NOT A DATA GAP. The
 * ratio needs a POSITIVE operating income, and a BANK reports no operating income line at all
 * (GuruFocus template 'B') — measured 2026-08-04, ING, ABN AMRO, JPMorgan, Bank of America, Morgan
 * Stanley and Goldman all carry interest expense with none, and insurers (NN, ASR, Aegon) carry
 * neither. Their index weight stays in the coverage denominator, so AEX lands at 72–80% and clears
 * the 80% floor in ONE year of twelve while the S&P clears it in none. A book of industrials
 * clears it every year, which is why the portfolio line is there and the index's is not. Lowering
 * the floor for the benchmark alone would mean the two lines on this chart were computed under
 * different rules — the one thing this whole overlay refuses to do; see `benchSeries`.
 */

export default function InterestBurdenCard({ holdingsTarget, holdingsName, benchTarget }: {
  holdingsTarget: Target; holdingsName?: string | null;
  /** The index drawn beside the book — same endpoint, same helper. See `benchSeries`. */
  benchTarget?: BenchTarget | null;
}) {
  const [data, setData] = useState<InterestBurdenInputs | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [showInputs, setShowInputs] = useState(false);

  useEffect(() => {
    let alive = true;
    void (async () => {
      setData(null); setErr(null);
      try {
        const r = await apiFetch(`${API_URL}/api/earnings/interest-burden-inputs`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(holdingsTarget),
        });
        const b = await r.json().catch(() => null);
        if (!alive) return;
        if (!r.ok) { setErr(b?.detail ?? `HTTP ${r.status}`); return; }
        setData(b as InterestBurdenInputs);
      } catch (e) {
        if (alive) setErr(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { alive = false; };
  }, [holdingsTarget]);

  const ratioByYr = useMemo(() => interestBurdenByYear(data?.rows ?? []), [data]);

  const [benchData, benchErr] = useBenchInputs<InterestBurdenInputs>('interest-burden-inputs', benchTarget);
  const benchByYr = useMemo(
    () => (benchData ? interestBurdenByYear(benchData.rows) : null), [benchData]);

  const note = benchNote(benchTarget, benchData, benchErr, benchByYr);

  const chartData = useMemo(
    () => mergeSeries(ratioByYr, benchByYr, 'ratio'), [ratioByYr, benchByYr]);

  const avg = meanOf([...ratioByYr.values()]);
  const latestYear = Math.max(-Infinity, ...ratioByYr.keys());
  const latest = Number.isFinite(latestYear) ? ratioByYr.get(latestYear) ?? null : null;
  const pct = (v: number | null) => (v == null ? '—' : `${v.toFixed(1)}%`);

  return (
    <div className="rounded-xl border border-neutral-800/40 bg-card p-4 space-y-3 min-w-0">
      <h4 className="text-base font-semibold text-fg-strong">Interest / op. profit</h4>

      {data == null && !err ? (
        <p className="text-xs text-fg-subtle py-16 text-center">Loading…</p>
      ) : err ? (
        <p className="text-xs text-neg-300 py-16 text-center">{err}</p>
      ) : ratioByYr.size === 0 ? (
        <p className="text-[11px] text-fg-faint py-16 text-center">No interest / operating-income figures ingested to compute a ratio.</p>
      ) : (
        <>
          <div className="flex flex-wrap gap-2">
            <Stat label="Avg" value={pct(avg)} color={chartTheme.accent}
              info={<InfoTip content={<AspectCard
                what="Average share of operating profit spent on interest, over the years shown."
                where="Computed here — |Interest expense| ÷ Operating income per year, weight-averaged across holdings."
                when="The years on the chart."
                how="Operating profit is GuruFocus's Operating Income line. Lower = less of profit going to service debt; a heavily-levered company reads high." />} />} />
            <Stat label="Latest" value={pct(latest)} color={chartTheme.accent} />
          </div>

          <div>
            <ResponsiveContainer width="100%" height={320}>
              <ComposedChart data={chartData} margin={{ top: 5, right: 12, bottom: 5, left: 4 }}
                style={{ cursor: 'pointer' }} onClick={() => setShowInputs(true)}>
                <CartesianGrid strokeDasharray="3 3" stroke={chartTheme.gridEarnings} />
                <XAxis dataKey="year" tickFormatter={xToPeriod} tick={{ fontSize: 11, fill: chartTheme.axisTick }} />
                <YAxis domain={paddedDomain(withBench(ratioByYr.values(), benchByYr))} tick={{ fontSize: 11, fill: chartTheme.axisTick }} width={48}
                  tickFormatter={(v: number) => `${v.toFixed(0)}%`} />
                <Tooltip contentStyle={chartTheme.tooltipCard.contentStyle} labelStyle={{ color: chartTheme.axisLabel }}
                  formatter={(v, n) => [`${typeof v === 'number' ? v.toFixed(1) : '—'}%`, n === 'bench' ? (benchTarget?.universe ?? 'Benchmark') : 'Interest / op. profit']} />
                <ReferenceLine y={0} stroke={chartTheme.zeroLine} />
                {avg != null && <ReferenceLine y={avg} stroke={chartTheme.accent} strokeDasharray="5 3" strokeOpacity={0.6} />}
                <Line dataKey="ratio" name="ratio" type="monotone" stroke={chartTheme.accent} strokeWidth={2} dot={{ r: 2.5 }} connectNulls />
                {benchByYr && <Line dataKey="bench" name="bench" type="monotone" stroke={chartTheme.pos} strokeWidth={2} dot={{ r: 2 }} connectNulls />}
              </ComposedChart>
            </ResponsiveContainer>
            <div className="flex justify-center flex-wrap gap-x-4 gap-y-1 text-xs mt-1">
              <span className="flex items-center gap-1.5"><span className="w-3 h-0.5 inline-block rounded" style={{ background: chartTheme.accent }} />Interest / op. profit (avg dashed)</span>
              {benchByYr && <span className="flex items-center gap-1.5"><span className="w-3 h-0.5 inline-block rounded" style={{ background: chartTheme.pos }} />{benchTarget?.universe}</span>}
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
        <InterestBurdenInputsModal target={holdingsTarget} portfolioName={holdingsName}
          benchTarget={benchTarget} benchLabel={benchTarget?.universe ?? null}
          onClose={() => setShowInputs(false)} />
      )}
    </div>
  );
}
