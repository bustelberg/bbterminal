'use client';

import { useMemo } from 'react';
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer,
  CartesianGrid,
} from 'recharts';
import InfoTip from '../InfoTip';
import Spinner from '../Spinner';
import { MC, type MetricRow } from './types';
import { annualSeries, computeCAGR, fmtPct, timeSeries } from './utils';

// Tooltip style — small local copy so the tooltip can opt out of the
// strokeWidth bump that the imported tooltipStyle was bundling.
const TOOLTIP_STYLE = {
  backgroundColor: '#151821',
  border: '1px solid #374151',
  borderRadius: '8px',
};

type Indexed = {
  date: string;
  ts: number;
  price?: number;
  oe_actual?: number;
  oe_est?: number;
};

type IndexedResult = {
  data: Indexed[];
  cagrs: { price: number | null; oe_act: number | null; oe_est: number | null };
  startDate: string | null;
};

/** Build the indexed-to-100 series + CAGRs for one company. The
 * `anchorDate` lets the comparison path force both companies onto a
 * shared start date so their indexed lines are directly slope-comparable
 * on the log axis. When null, uses the natural "first date where price
 * AND OE actual are both positive" anchor. Returns startDate=null when
 * there's not enough data to plot. */
function buildIndexed(metrics: MetricRow[], anchorDate: string | null = null): IndexedResult {
  const dailyPrice = timeSeries(metrics, 'close_price');
  const annualPrice = annualSeries(metrics, MC.PRICE);
  const priceSeries = dailyPrice.length > 0 ? dailyPrice : annualPrice;

  const epsActual = annualSeries(metrics, MC.EPS_WO_NRI);
  const divActual = annualSeries(metrics, MC.DIV_PS);
  const epsEst = annualSeries(metrics, MC.EPS_EST);
  const divEst = annualSeries(metrics, MC.DIV_EST);

  // OE actual = EPS_WO_NRI + DIV_PS per year
  const divMap: Record<string, number> = {};
  for (const d of divActual) divMap[d.date.slice(0, 4)] = d.value;
  const oeActual = epsActual.map((e) => {
    const yr = e.date.slice(0, 4);
    const div = divMap[yr] ?? 0;
    return { date: e.date, value: e.value + div };
  });
  // OE estimate = EPS_EST + DIV_EST per year
  const divEstMap: Record<string, number> = {};
  for (const d of divEst) divEstMap[d.date.slice(0, 4)] = d.value;
  const oeEst = epsEst.map((e) => {
    const yr = e.date.slice(0, 4);
    const div = divEstMap[yr] ?? 0;
    return { date: e.date, value: e.value + div };
  });

  if (priceSeries.length === 0 || oeActual.length === 0) {
    return { data: [], cagrs: { price: null, oe_act: null, oe_est: null }, startDate: null };
  }

  // Anchor selection: when called with no anchor, pick the natural first
  // overlapping positive point. When called with a forced anchor (e.g.
  // commonStart in the comparison path), find the first positive points
  // at or after that date.
  const minDate = anchorDate ?? '0000-00-00';
  const firstOE = oeActual.find((o) => o.date >= minDate && o.value > 0);
  if (!firstOE) {
    return { data: [], cagrs: { price: null, oe_act: null, oe_est: null }, startDate: null };
  }
  const firstPrice = priceSeries.find((p) => p.date >= firstOE.date && p.value > 0);
  if (!firstPrice) {
    return { data: [], cagrs: { price: null, oe_act: null, oe_est: null }, startDate: null };
  }
  const startDate = firstPrice.date;
  const priceBase = firstPrice.value;
  const oeBase = firstOE.value;

  // Cap chart end so OE estimates don't stretch the x-axis past actuals.
  const lastPriceDate = priceSeries[priceSeries.length - 1].date;
  const endCutoff = `${parseInt(lastPriceDate.slice(0, 4)) + 2}-12-31`;

  const oeActMap: Record<string, number> = {};
  for (const o of oeActual) oeActMap[o.date] = o.value;
  const oeEstMap: Record<string, number> = {};
  for (const o of oeEst) if (o.date <= endCutoff) oeEstMap[o.date] = o.value;
  const priceMap: Record<string, number> = {};
  for (const p of priceSeries) priceMap[p.date] = p.value;

  const lastActualDate = [...oeActual].filter((o) => o.date >= startDate && o.value > 0).pop()?.date;
  const lastActualIndexed = lastActualDate && oeActMap[lastActualDate] > 0
    ? (oeActMap[lastActualDate] / oeBase) * 100
    : undefined;

  // Union of all dates from all three series within window.
  const allDates = new Set<string>();
  for (const p of priceSeries) if (p.date >= startDate) allDates.add(p.date);
  for (const o of oeActual) if (o.date >= startDate && o.date <= endCutoff) allDates.add(o.date);
  for (const d of Object.keys(oeEstMap)) if (d >= startDate) allDates.add(d);
  const sortedDates = [...allDates].sort();

  const data: Indexed[] = sortedDates.map((d) => {
    const oeEstVal = oeEstMap[d] != null && oeEstMap[d] > 0 ? (oeEstMap[d] / oeBase) * 100 : undefined;
    return {
      date: d,
      ts: new Date(d).getTime(),
      price: priceMap[d] != null ? (priceMap[d] / priceBase) * 100 : undefined,
      oe_actual: oeActMap[d] != null && oeActMap[d] > 0 ? (oeActMap[d] / oeBase) * 100 : undefined,
      // Bridge: at the last actual date, also set oe_est so the rose
      // line picks up from where the green line ends.
      oe_est: d === lastActualDate ? (oeEstVal ?? lastActualIndexed) : oeEstVal,
    };
  });

  // CAGRs — annual price for CAGR to match OE annual intervals.
  const annualPriceForCagr = annualSeries(metrics, MC.PRICE);
  const priceFiltered = annualPriceForCagr.filter((p) => p.date >= startDate);
  const oeActFiltered = oeActual.filter((o) => o.date >= startDate && o.value > 0);
  const oeEstFiltered = oeEst.filter((o) => o.date >= startDate && o.value > 0);

  return {
    data,
    cagrs: {
      price: computeCAGR(priceFiltered),
      oe_act: computeCAGR(oeActFiltered),
      oe_est: computeCAGR(oeEstFiltered),
    },
    startDate,
  };
}

type Props = {
  metrics: MetricRow[];
  metricsB?: MetricRow[];
  labelA?: string;
  labelB?: string;
  /** True during B's initial metrics fetch. Shows a spinner in the
   * CAGR-pill row for B so the comparison side reads as "loading"
   * rather than silently missing. */
  loadingB?: boolean;
};

/** Log-scale chart comparing share price growth to Owner Earnings (EPS +
 * Dividends), both indexed to 100 at the first overlapping year. Three
 * series: price (indigo), OE actual (emerald), OE estimate (rose). With
 * `metricsB` supplied, three additional dashed lines render in the same
 * colors so the user can pair "solid A vs dashed B" by hue. Both
 * companies share a common anchor (max of their natural start dates)
 * so the indexed slopes are directly comparable. */
export default function RelativeGrowthChart({ metrics, metricsB, labelA, labelB, loadingB }: Props) {
  const hasB = !!metricsB;
  const combined = useMemo(() => {
    // First pass — find each company's natural start date.
    const naturalA = buildIndexed(metrics);
    const naturalB = metricsB ? buildIndexed(metricsB) : null;
    // Force both companies onto the LATER of the two natural starts so
    // they share an x-axis origin (both lines start at 100). This is
    // what makes "is A appreciating faster than B?" answerable from
    // slope alone on a log axis.
    const commonStart = naturalB?.startDate
      ? (naturalA.startDate && naturalA.startDate > naturalB.startDate ? naturalA.startDate : naturalB.startDate)
      : naturalA.startDate;
    const A = commonStart ? buildIndexed(metrics, commonStart) : naturalA;
    const B = hasB && commonStart ? buildIndexed(metricsB!, commonStart) : null;

    // Merge A and B by `ts` so the chart treats them as one dataset.
    const byTs = new Map<number, Record<string, number | string | undefined>>();
    for (const r of A.data) {
      const cur = byTs.get(r.ts) ?? { date: r.date, ts: r.ts };
      cur.price_a = r.price;
      cur.oe_actual_a = r.oe_actual;
      cur.oe_est_a = r.oe_est;
      byTs.set(r.ts, cur);
    }
    if (B) {
      for (const r of B.data) {
        const cur = byTs.get(r.ts) ?? { date: r.date, ts: r.ts };
        cur.price_b = r.price;
        cur.oe_actual_b = r.oe_actual;
        cur.oe_est_b = r.oe_est;
        byTs.set(r.ts, cur);
      }
    }
    const merged = Array.from(byTs.values()).sort((x, y) => Number(x.ts) - Number(y.ts));
    return { merged, cagrsA: A.cagrs, cagrsB: B?.cagrs ?? null, commonStart };
  }, [metrics, metricsB, hasB]);

  if (combined.merged.length === 0) {
    return <div className="text-gray-500 text-sm py-8 text-center">Not enough data for Relative Growth chart. Refresh to load.</div>;
  }

  const aLab = labelA ?? 'A';
  const bLab = labelB ?? 'B';

  return (
    <>
      <div className="text-gray-500 text-xs mb-2 flex items-center gap-1 flex-wrap">
        Price vs OE, indexed to 100
        {hasB && combined.commonStart && (
          <span className="text-gray-600 font-mono">(common start {combined.commonStart})</span>
        )}
        <InfoTip text="Compares share price growth to Owner Earnings (EPS + Dividends) growth on a log scale. If price grows faster than OE, the stock is getting more expensive (multiple expansion). If OE outpaces price, it's getting cheaper. In comparison mode, both companies are rebased to 100 at the same start date so their indexed slopes are directly comparable." />
      </div>
      <div className="flex flex-wrap gap-x-5 gap-y-1 mb-2">
        <div className="flex items-center gap-1.5">
          <div className="text-indigo-400 text-xs">{aLab} Price</div>
          <div className="text-indigo-400 font-mono text-sm font-semibold">{fmtPct(combined.cagrsA.price ?? null)}</div>
        </div>
        <div className="flex items-center gap-1.5">
          <div className="text-emerald-400 text-xs">{aLab} OE Act</div>
          <div className="text-emerald-400 font-mono text-sm font-semibold">{fmtPct(combined.cagrsA.oe_act ?? null)}</div>
        </div>
        <div className="flex items-center gap-1.5">
          <div className="text-rose-400 text-xs">{aLab} OE Est</div>
          <div className="text-rose-400 font-mono text-sm font-semibold">{fmtPct(combined.cagrsA.oe_est ?? null)}</div>
        </div>
        {hasB && combined.cagrsB && (
          <>
            <div className="w-full" />
            <div className="flex items-center gap-1.5">
              <div className="text-indigo-400 text-xs">{bLab} Price</div>
              <div className="text-indigo-400 font-mono text-sm font-semibold">{fmtPct(combined.cagrsB.price ?? null)}</div>
            </div>
            <div className="flex items-center gap-1.5">
              <div className="text-emerald-400 text-xs">{bLab} OE Act</div>
              <div className="text-emerald-400 font-mono text-sm font-semibold">{fmtPct(combined.cagrsB.oe_act ?? null)}</div>
            </div>
            <div className="flex items-center gap-1.5">
              <div className="text-rose-400 text-xs">{bLab} OE Est</div>
              <div className="text-rose-400 font-mono text-sm font-semibold">{fmtPct(combined.cagrsB.oe_est ?? null)}</div>
            </div>
          </>
        )}
        {hasB && !combined.cagrsB && loadingB && (
          <>
            <div className="w-full" />
            <div className="flex items-center gap-1.5">
              <div className="text-gray-500 text-xs">Loading {bLab}</div>
              <Spinner size={10} />
            </div>
          </>
        )}
      </div>
      <ResponsiveContainer width="100%" height={280}>
        <LineChart data={combined.merged} margin={{ top: 5, right: 10, bottom: 5, left: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#1e2330" />
          <XAxis
            dataKey="ts"
            type="number"
            scale="time"
            domain={['dataMin', 'dataMax']}
            tick={{ fontSize: 11, fill: '#6b7280' }}
            tickFormatter={(v: number) => new Date(v).getFullYear().toString()}
          />
          <YAxis
            scale="log"
            domain={['auto', 'auto']}
            tick={{ fontSize: 11, fill: '#6b7280' }}
            tickFormatter={(v: number) => v.toFixed(0)}
          />
          <Tooltip
            contentStyle={TOOLTIP_STYLE}
            labelStyle={{ color: '#9ca3af' }}
            labelFormatter={(v) => new Date(Number(v)).toISOString().slice(0, 10)}
            formatter={(v, name) => {
              const isB = String(name).endsWith('_b');
              const company = isB ? bLab : aLab;
              const which = String(name).replace(/_[ab]$/, '');
              const lab = which === 'price_a' || which === 'price' ? `${company} Price`
                : which === 'oe_actual_a' || which === 'oe_actual' ? `${company} OE Actual`
                : which === 'oe_est_a' || which === 'oe_est' ? `${company} OE Estimate`
                : `${company} ${which}`;
              return [Number(v).toFixed(1), lab];
            }}
          />
          <Line type="monotone" dataKey="price_a" stroke="#6366f1" strokeWidth={2} dot={false} connectNulls />
          <Line type="monotone" dataKey="oe_actual_a" stroke="#34d399" strokeWidth={2} dot={false} connectNulls />
          <Line type="monotone" dataKey="oe_est_a" stroke="#f87171" strokeWidth={2} dot={false} connectNulls />
          {hasB && (
            <>
              <Line type="monotone" dataKey="price_b" stroke="#6366f1" strokeWidth={2} strokeDasharray="5 3" dot={false} connectNulls />
              <Line type="monotone" dataKey="oe_actual_b" stroke="#34d399" strokeWidth={2} strokeDasharray="5 3" dot={false} connectNulls />
              <Line type="monotone" dataKey="oe_est_b" stroke="#f87171" strokeWidth={2} strokeDasharray="5 3" dot={false} connectNulls />
            </>
          )}
        </LineChart>
      </ResponsiveContainer>
      <div className="flex flex-wrap justify-center gap-x-5 gap-y-1 text-xs mt-1">
        <span className="flex items-center gap-1.5"><span className="w-3 h-0.5 bg-indigo-400 inline-block rounded" />Price</span>
        <span className="flex items-center gap-1.5"><span className="w-3 h-0.5 bg-emerald-400 inline-block rounded" />OE Actual</span>
        <span className="flex items-center gap-1.5"><span className="w-3 h-0.5 bg-rose-400 inline-block rounded" />OE Estimate</span>
        {hasB && (
          <span className="text-gray-500 ml-3">— solid = {aLab} · dashed = {bLab}</span>
        )}
      </div>
    </>
  );
}
