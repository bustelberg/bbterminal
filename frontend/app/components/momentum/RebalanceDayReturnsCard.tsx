'use client';

import { useMemo } from 'react';
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer,
  CartesianGrid, ReferenceLine,
} from 'recharts';
import CollapsibleCard from './CollapsibleCard';
import InfoTip from '../universe/InfoTip';
import { tooltipStyle, annualize, fmtPct } from './utils';
import { chartTheme } from '../../../lib/chartTheme';
import type { BacktestResult } from '../../../lib/stores/momentum';

/**
 * Rebalance-day returns — the strategy's 1-day return on each month's
 * rebalance day (close of the rebalance day vs the prior trading day),
 * compounded into a random-walk line. Answers "how much of performance is
 * concentrated on the day we rebalance?" — i.e. what you'd forgo by sitting
 * out the market on those specific days.
 *
 * Rebalance dates come from `monthly_records[i].date` (the exact rebalance
 * day the backtest used); the per-day move is read off the daily equity
 * curve so it's independent of the entry-pricing convention. Charted in the
 * same recharts style as the Equity Curve card.
 */
const WEEKDAYS = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];

type WalkPoint = { date: string; ret: number; cum: number };

/** Deduped daily cumulative-return curve as `{date, cum}[]` (the curve repeats
 * period-boundary dates — prior exit == next entry — with equal values; keep
 * the last). */
function dedupeCurve(recs: { date: string; cumulative_return_pct: number }[]): { date: string; cum: number }[] {
  const seen = new Map<string, number>();
  const out: { date: string; cum: number }[] = [];
  for (const r of recs) {
    const d = r.date.slice(0, 10);
    const idx = seen.get(d);
    if (idx === undefined) { seen.set(d, out.length); out.push({ date: d, cum: r.cumulative_return_pct }); }
    else { out[idx].cum = r.cumulative_return_pct; }
  }
  return out;
}

export default function RebalanceDayReturnsCard({
  result,
  defaultCollapsed = false,
}: {
  result: BacktestResult;
  defaultCollapsed?: boolean;
}) {
  const { walk, weekdayLabel, stratTotalPct, withCAGR, withoutCAGR, impactPP } = useMemo(() => {
    const curve = dedupeCurve(result.daily_records ?? []);
    const idxByDate = new Map(curve.map((p, i) => [p.date, i]));

    // The FIRST rebalance day of each calendar month. For a monthly strategy
    // that's every rebalance; for weekly/daily cadences we keep only the first
    // rebalance in each month, matching "the first rebalance day of each month".
    const firstByMonth = new Map<string, string>();
    for (const d of (result.monthly_records ?? []).map((m) => m.date.slice(0, 10)).sort()) {
      const ym = d.slice(0, 7);
      if (!firstByMonth.has(ym)) firstByMonth.set(ym, d);
    }
    const rebalDates = Array.from(firstByMonth.values()).sort();

    const walk: WalkPoint[] = [];
    let factor = 1;
    const wkCount: Record<number, number> = {};
    for (const r of rebalDates) {
      // The trading day the rebalance lands on: the exact date if it traded,
      // else the first curve day on/after it (covers a holiday rebalance day).
      let idx = idxByDate.get(r);
      if (idx === undefined) {
        idx = curve.findIndex((p) => p.date >= r);
        if (idx < 0) continue;
      }
      if (idx <= 0) continue; // need a prior trading day to measure a 1-day move
      const f0 = 1 + curve[idx - 1].cum / 100;
      const f1 = 1 + curve[idx].cum / 100;
      if (f0 <= 0) continue;
      const ret = (f1 / f0 - 1) * 100;
      factor *= 1 + ret / 100;
      const day = curve[idx].date;
      const wd = new Date(`${day}T00:00:00Z`).getUTCDay();
      wkCount[wd] = (wkCount[wd] ?? 0) + 1;
      walk.push({ date: day, ret, cum: (factor - 1) * 100 });
    }

    const modalWeekday = Object.entries(wkCount).sort((a, b) => b[1] - a[1])[0]?.[0];
    const weekdayLabel = modalWeekday != null ? WEEKDAYS[Number(modalWeekday)] : null;
    const stratTotalPct = curve.length ? curve[curve.length - 1].cum : null;

    // CAGR with vs without the rebalance-day moves, annualized over the full
    // backtest span. Returns compound multiplicatively — each day's factor
    // appears once in the full product — so removing the rebalance days gives
    // other-days cumulative = (1 + full) / (1 + rebalance-only) − 1. The gap
    // between the two CAGRs is the EXACT percentage-point hit to the headline
    // annualized return (a touch larger than the raw rebalance-day CAGR when
    // the rest of the year is also positive).
    let withCAGR: number | null = null;
    let withoutCAGR: number | null = null;
    let impactPP: number | null = null;
    if (walk.length >= 2 && curve.length >= 2 && stratTotalPct != null) {
      const spanDays = (Date.parse(curve[curve.length - 1].date) - Date.parse(curve[0].date)) / 86_400_000;
      const spanMonths = spanDays > 0 ? spanDays / 30.4375 : 0;
      const rebalCum = walk[walk.length - 1].cum;
      const rebalFactor = 1 + rebalCum / 100;
      if (rebalFactor > 0) {
        const otherCum = ((1 + stratTotalPct / 100) / rebalFactor - 1) * 100;
        withCAGR = annualize(stratTotalPct, spanMonths);
        withoutCAGR = annualize(otherCum, spanMonths);
        if (withCAGR != null && withoutCAGR != null) impactPP = withCAGR - withoutCAGR;
      }
    }
    return { walk, weekdayLabel, stratTotalPct, withCAGR, withoutCAGR, impactPP };
  }, [result]);

  const stats = useMemo(() => {
    if (walk.length === 0) return null;
    const rets = walk.map((p) => p.ret);
    const n = rets.length;
    const total = walk[walk.length - 1].cum;
    const avg = rets.reduce((a, b) => a + b, 0) / n;
    const pos = rets.filter((r) => r > 0).length;
    const best = Math.max(...rets);
    const worst = Math.min(...rets);
    return { n, total, avg, pos, best, worst };
  }, [walk]);

  if (walk.length < 2) {
    return (
      <CollapsibleCard title="Rebalance-day returns" defaultCollapsed={defaultCollapsed} bodyClassName="px-5 py-4">
        <div className="text-xs text-fg-subtle">No daily equity curve available for this run.</div>
      </CollapsibleCard>
    );
  }

  const interval = Math.max(0, Math.floor(walk.length / 12) - 1);

  return (
    <CollapsibleCard
      title="Rebalance-day returns (Cumulative %)"
      defaultCollapsed={defaultCollapsed}
      rightSlot={stats && (
        <span className="font-mono flex items-center gap-2">
          <span className={stats.total >= 0 ? 'text-pos-400' : 'text-neg-400'}>
            {stats.total >= 0 ? '+' : ''}{stats.total.toFixed(1)}% over {stats.n} days
          </span>
          {impactPP != null && (
            <span className="text-fg-faint">
              · skipping{' '}
              <span className={impactPP >= 0 ? 'text-neg-400' : 'text-pos-400'}>
                {impactPP >= 0 ? '−' : '+'}{Math.abs(impactPP).toFixed(2)} pp/yr
              </span>
            </span>
          )}
        </span>
      )}
      bodyClassName="px-5 pb-5"
    >
      <ResponsiveContainer width="100%" height={380}>
        <LineChart data={walk}>
          <CartesianGrid strokeDasharray="3 3" stroke={chartTheme.grid} />
          <XAxis
            dataKey="date"
            tick={{ fill: chartTheme.axisTick, fontSize: 12 }}
            tickLine={false}
            interval={interval}
          />
          <YAxis
            tick={{ fill: chartTheme.axisTick, fontSize: 12 }}
            tickLine={false}
            tickFormatter={(v: number) => `${v}%`}
            domain={[(min: number) => Math.min(0, min), (max: number) => Math.max(0, max)]}
          />
          <Tooltip
            {...tooltipStyle}
            labelFormatter={(d) => String(d)}
            formatter={(value, _name, item) => {
              const cum = Number(value);
              const ret = Number((item?.payload as { ret?: number } | undefined)?.ret ?? NaN);
              const cumStr = `${cum >= 0 ? '+' : ''}${cum.toFixed(2)}%`;
              const retStr = Number.isFinite(ret) ? ` (day ${ret >= 0 ? '+' : ''}${ret.toFixed(2)}%)` : '';
              return [`${cumStr}${retStr}`, 'Cumulative'];
            }}
          />
          <ReferenceLine y={0} stroke={chartTheme.zeroLine} strokeWidth={1} />
          <Line
            type="monotone"
            dataKey="cum"
            stroke={chartTheme.accent}
            strokeWidth={2}
            dot={false}
            name="cum"
          />
        </LineChart>
      </ResponsiveContainer>

      {withCAGR != null && withoutCAGR != null && (
        <div className="mt-3 space-y-1.5">
          <div className="flex items-baseline gap-2 flex-wrap">
            <span className="text-xs text-fg-muted">Annualized return (CAGR)</span>
            <span className="text-[12px] text-fg-subtle">with rebalance days</span>
            <span className={`font-mono font-semibold ${withCAGR >= 0 ? 'text-pos-400' : 'text-neg-400'}`}>{fmtPct(withCAGR)}</span>
            <span className="text-fg-faint">→ without</span>
            <span className={`font-mono font-semibold ${withoutCAGR >= 0 ? 'text-pos-400' : 'text-neg-400'}`}>{fmtPct(withoutCAGR)}</span>
          </div>
          {impactPP != null && (
            <div className="flex items-baseline gap-1.5 flex-wrap">
              <span className="text-xs text-fg-muted">Skipping the {weekdayLabel ?? 'rebalance'}-day moves costs</span>
              <span className={`font-mono font-semibold text-base ${impactPP >= 0 ? 'text-neg-400' : 'text-pos-400'}`}>
                {impactPP >= 0 ? '−' : '+'}{Math.abs(impactPP).toFixed(2)} pp/yr off CAGR
              </span>
              <InfoTip text="Returns compound multiplicatively: (1 + total CAGR) = (1 + rebalance-day CAGR) × (1 + other-days CAGR). 'Without' divides the annual growth factor by the rebalance-day contribution, so this gap is the EXACT percentage-point hit to your annualized return — a touch larger than the raw rebalance-day CAGR when the other days are also positive. pp = percentage points." />
            </div>
          )}
        </div>
      )}

      {stats && (
        <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-[12px] text-fg-subtle mt-2">
          <span className="text-[11px] uppercase tracking-wide text-fg-faint">
            {weekdayLabel ? `${weekdayLabel} rebalance days` : 'Rebalance days'}
          </span>
          <span>
            compounded{' '}
            <span className={`font-mono ${stats.total >= 0 ? 'text-pos-400' : 'text-neg-400'}`}>
              {stats.total >= 0 ? '+' : ''}{stats.total.toFixed(2)}%
            </span>
          </span>
          {stratTotalPct != null && (
            <span title="The strategy's full cumulative return over the whole backtest — for scale against the rebalance-day contribution above.">
              of strategy total{' '}
              <span className={`font-mono ${stratTotalPct >= 0 ? 'text-pos-400' : 'text-neg-400'}`}>
                {stratTotalPct >= 0 ? '+' : ''}{stratTotalPct.toFixed(1)}%
              </span>
            </span>
          )}
          <span>avg/day <span className={`font-mono ${stats.avg >= 0 ? 'text-pos-400' : 'text-neg-400'}`}>{stats.avg >= 0 ? '+' : ''}{stats.avg.toFixed(2)}%</span></span>
          <span>up <span className="font-mono text-fg-soft">{stats.pos}/{stats.n}</span></span>
          <span>best <span className="font-mono text-pos-400">+{stats.best.toFixed(2)}%</span></span>
          <span>worst <span className="font-mono text-neg-400">{stats.worst.toFixed(2)}%</span></span>
        </div>
      )}

      <p className="text-[11px] text-fg-faint mt-2">
        Each step is the strategy&apos;s 1-day return on a month&apos;s rebalance day — the close of that day vs the prior
        trading day&apos;s close — compounded into a random walk. It isolates how much of the strategy&apos;s performance lands
        on rebalance days: the line is what you&apos;d earn (or forgo) by being in the market <em>only</em>{' '}on those days.
      </p>
    </CollapsibleCard>
  );
}
