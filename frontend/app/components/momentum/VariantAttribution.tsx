'use client';

import { Fragment, memo, useMemo } from 'react';
import {
  momentumStore,
  parseVariantKey,
  VARIANT_DEFS,
  type VariantKey,
  type VariantOutcome,
  type VariantParams,
  type Summary,
} from '../../../lib/stores/momentum';
import CellInfoTip from './CellInfoTip';
import CollapsibleCard from './CollapsibleCard';
import { fmtPct } from './utils';

// Per-axis attribution: for each axis the user swept (i.e. that took >=2
// distinct values across the loaded variants), group OK variants by axis
// value and average every metric within the group. The value whose mean
// is best for a given metric wins a gold star in that metric's column.
//
// What this catches: "monthly Sharpe averages 1.41 across the 8 monthly
// variants vs 0.97 for the 8 every-6-months ones — cadence is helping
// you." What this MISSES: 2-way interactions ("monthly works on ACWI but
// kills LEONTEQ"). The marginal averages collapse those into the row
// means; for interaction detection see a future 2D-heatmap addition.

// ─── Types + lookups ────────────────────────────────────────────────────────

type AxisKey =
  | 'frequency'
  | 'strategy'
  | 'universe'
  | 'grouping'
  | 'top_n_sectors'
  | 'top_n_per_sector'
  | 'min_price_score';

const AXIS_LABEL: Record<AxisKey, string> = {
  frequency: 'Frequency',
  strategy: 'Strategy',
  universe: 'Universe',
  grouping: 'Grouping',
  top_n_sectors: 'Top sectors',
  top_n_per_sector: 'Per sector',
  min_price_score: 'Min price score',
};

const ALL_AXES: readonly AxisKey[] = [
  'frequency', 'strategy', 'universe', 'grouping',
  'top_n_sectors', 'top_n_per_sector', 'min_price_score',
];

type MetricKey =
  | 'annualized'
  | 'universe_annualized'
  | 'sharpe'
  | 'sortino'
  | 'win_rate'
  | 'median'
  | 'total'
  | 'max_dd';

const METRIC_LABEL: Record<MetricKey, string> = {
  annualized: 'Annualized',
  universe_annualized: 'Universe ann.',
  sharpe: 'Sharpe',
  sortino: 'Sortino',
  win_rate: 'Win rate',
  median: 'Median month',
  total: 'Total return',
  max_dd: 'Max DD',
};

const ALL_METRICS: readonly MetricKey[] = [
  'annualized', 'universe_annualized', 'sharpe', 'sortino',
  'win_rate', 'median', 'total', 'max_dd',
];

/** "Higher value is better" holds for every metric in this table —
 * including `max_dd`, where -5 > -20 (closer to zero = better). One
 * winner-selection rule for all columns. */
function readMetric(summary: Summary | undefined, m: MetricKey): number | null {
  if (!summary) return null;
  const candidate = (() => {
    switch (m) {
      case 'annualized':          return summary.annualized_return_pct;
      case 'universe_annualized': return summary.universe_annualized_return_pct;
      case 'sharpe':              return summary.sharpe_ratio;
      case 'sortino':             return summary.sortino_ratio;
      case 'win_rate':            return summary.win_rate_pct;
      case 'median':              return summary.median_period_return_pct;
      case 'total':               return summary.total_return_pct;
      case 'max_dd':              return summary.max_drawdown_pct;
    }
  })();
  if (candidate == null || !Number.isFinite(candidate)) return null;
  return candidate;
}

/** Returns the "axis value" for a variant. Undefined when the variant
 * didn't override that axis (inherited the base config); those
 * variants don't participate in the axis's per-value breakdown
 * because they don't represent a deliberate choice for that axis. */
function readAxis(p: VariantParams, a: AxisKey): string | undefined {
  let raw: string | number | null | undefined;
  switch (a) {
    case 'frequency':         raw = p.frequency; break;
    case 'strategy':          raw = p.strategy; break;
    case 'universe':          raw = p.universe; break;
    case 'grouping':          raw = p.grouping; break;
    case 'top_n_sectors':     raw = p.top_n_sectors; break;
    case 'top_n_per_sector':  raw = p.top_n_per_sector; break;
    case 'min_price_score':   raw = p.min_price_score; break;
  }
  if (raw === undefined) return undefined;
  // `min_price_score: null` is the "explicitly off" axis token —
  // preserve it as a distinct group rather than collapsing into the
  // inherit case.
  if (raw === null) return 'off';
  return String(raw);
}

function fmtMetric(m: MetricKey, value: number | null): string {
  if (value == null) return '—';
  if (m === 'sharpe' || m === 'sortino') return value.toFixed(2);
  if (m === 'win_rate') return `${value.toFixed(0)}%`;
  return fmtPct(value);
}

/** Same units as `fmtMetric` but unsigned — used for std deviations
 * shown in parentheses next to a mean. Std is always positive so the
 * `+`/`-` prefix `fmtPct` adds reads wrong here. */
function fmtMetricStd(m: MetricKey, std: number): string {
  if (m === 'sharpe' || m === 'sortino') return std.toFixed(2);
  if (m === 'win_rate') return `${std.toFixed(0)}%`;
  return `${std.toFixed(2)}%`;
}

/** Sample standard deviation (Bessel's correction — divides by N-1
 * rather than N). Returns null when fewer than 2 values exist, which
 * matches what np.std behaves as for the multi-trial aggregator. */
function sampleStd(values: number[]): number | null {
  if (values.length < 2) return null;
  const mean = values.reduce((a, b) => a + b, 0) / values.length;
  const sqDiffSum = values.reduce((s, v) => s + (v - mean) ** 2, 0);
  return Math.sqrt(sqDiffSum / (values.length - 1));
}

// ─── Group ordering ─────────────────────────────────────────────────────────

/** Stable index of each frequency in VARIANT_DEFS — sorts monthly /
 * every_3_months / every_6_months in their natural cadence order
 * rather than alphabetical, which would put `every_12_months` first
 * and `monthly` near the end. */
const FREQ_ORDER: Map<string, number> = (() => {
  const m = new Map<string, number>();
  VARIANT_DEFS.forEach((v, i) => {
    if (!m.has(v.frequency)) m.set(v.frequency, i);
  });
  return m;
})();

function sortGroupKeys(axis: AxisKey, keys: string[]): string[] {
  switch (axis) {
    case 'frequency':
      return keys.slice().sort(
        (a, b) => (FREQ_ORDER.get(a) ?? 999) - (FREQ_ORDER.get(b) ?? 999),
      );
    case 'strategy':
      // long_only first, then long_short.
      return keys.slice().sort((a, b) => {
        const order = { long_only: 0, long_short: 1 } as Record<string, number>;
        return (order[a] ?? 99) - (order[b] ?? 99);
      });
    case 'grouping':
      // sector first, then industry.
      return keys.slice().sort((a, b) => {
        if (a === 'sector') return -1;
        if (b === 'sector') return 1;
        return a.localeCompare(b);
      });
    case 'top_n_sectors':
    case 'top_n_per_sector':
    case 'min_price_score': {
      // Numeric ascending. `off` (min_price_score null) sinks to the
      // bottom so the comparison reads "1, 5, 10, off".
      return keys.slice().sort((a, b) => {
        if (a === 'off') return 1;
        if (b === 'off') return -1;
        return Number(a) - Number(b);
      });
    }
    case 'universe':
      return keys.slice().sort((a, b) => a.localeCompare(b));
  }
}

// ─── Component ─────────────────────────────────────────────────────────────

function VariantAttributionInner() {
  const variants = momentumStore.use((s) => s.variants);

  const { totalOk, axisBreakdowns } = useMemo(() => {
    // Collect every variant whose status is `ok` (we can't analyze
    // pending/running/error rows — they have no result to read metrics from).
    type AnalyzedVariant = {
      key: VariantKey;
      params: VariantParams;
      summary: Summary | undefined;
    };
    const okList: AnalyzedVariant[] = [];
    for (const [k, o] of Object.entries(variants)) {
      const out = o as VariantOutcome | undefined;
      if (!out || out.status !== 'ok') continue;
      const p = parseVariantKey(k as VariantKey);
      if (!p) continue;
      okList.push({ key: k as VariantKey, params: p, summary: out.result.summary });
    }

    type MetricStat = { mean: number; std: number | null } | null;
    type AxisRow = {
      value: string;
      n: number;
      stats: Partial<Record<MetricKey, MetricStat>>;
    };
    type AxisBreakdown = {
      axis: AxisKey;
      label: string;
      groups: AxisRow[];
      winnerPerMetric: Partial<Record<MetricKey, string | null>>;
    };
    const breakdowns: AxisBreakdown[] = [];

    for (const axis of ALL_AXES) {
      // Bucket OK variants by their value for this axis. Variants that
      // didn't override this axis (`readAxis` → undefined) are dropped
      // from this axis's breakdown so we're only comparing variants
      // that made deliberate choices on it.
      const buckets = new Map<string, AnalyzedVariant[]>();
      for (const v of okList) {
        const value = readAxis(v.params, axis);
        if (value === undefined) continue;
        if (!buckets.has(value)) buckets.set(value, []);
        buckets.get(value)!.push(v);
      }
      // No comparison if only one value (or zero) showed up.
      if (buckets.size < 2) continue;

      const groupsRaw: AxisRow[] = [];
      for (const [value, vs] of buckets.entries()) {
        const stats: Partial<Record<MetricKey, MetricStat>> = {};
        for (const m of ALL_METRICS) {
          const nums: number[] = [];
          for (const v of vs) {
            const x = readMetric(v.summary, m);
            if (x != null) nums.push(x);
          }
          stats[m] = nums.length === 0
            ? null
            : {
                mean: nums.reduce((a, b) => a + b, 0) / nums.length,
                std: sampleStd(nums),
              };
        }
        groupsRaw.push({ value, n: vs.length, stats });
      }

      // Sort group rows by the axis-appropriate order so they read
      // naturally (cadence-ascending for frequencies, numeric for
      // top-N, etc.) rather than insertion order.
      const sortedKeys = sortGroupKeys(axis, groupsRaw.map((g) => g.value));
      const groupByValue = new Map(groupsRaw.map((g) => [g.value, g] as const));
      const groups = sortedKeys
        .map((k) => groupByValue.get(k))
        .filter((g): g is AxisRow => g != null);

      // Winner per metric — the value whose mean tops the column.
      // Ties don't get a winner (rare; would need extra logic to mark
      // both, and "tied" winners would dilute the visual signal anyway).
      const winnerPerMetric: Partial<Record<MetricKey, string | null>> = {};
      for (const m of ALL_METRICS) {
        let bestVal = -Infinity;
        let bestKey: string | null = null;
        let tie = false;
        for (const g of groups) {
          const s = g.stats[m];
          if (s == null) continue;
          if (s.mean > bestVal) {
            bestVal = s.mean;
            bestKey = g.value;
            tie = false;
          } else if (s.mean === bestVal) {
            tie = true;
          }
        }
        winnerPerMetric[m] = tie ? null : bestKey;
      }

      breakdowns.push({ axis, label: AXIS_LABEL[axis], groups, winnerPerMetric });
    }

    return { totalOk: okList.length, axisBreakdowns: breakdowns };
  }, [variants]);

  if (totalOk < 2 || axisBreakdowns.length === 0) return null;

  return (
    <CollapsibleCard
      title="Variant attribution"
      rightSlot={
        <span className="flex items-center gap-1.5">
          <span>
            Mean (±sample std) across {totalOk} successful variants — ★ marks the value whose mean tops each metric column.
          </span>
          <CellInfoTip>
            For each axis the sweep varied, this groups variants by axis value and shows the mean of every metric within the group. The parenthetical is the sample standard deviation (Bessel-corrected, N−1) — a tight std means the axis-value alone explains a lot of the metric&apos;s variance; a wide std means other axes are pulling the metric around. A consistent ★ across columns on one row = a value that helps across the board; mixed ★s = different values shine on different metrics. Caveat: marginal averages collapse 2-way interactions — &quot;monthly works on ACWI but kills LEONTEQ&quot; is invisible here.
          </CellInfoTip>
        </span>
      }
    >
      <div className="overflow-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="text-gray-500 text-xs border-b border-gray-800/40">
              <th className="text-left font-medium px-3 py-2">Axis value</th>
              <th className="text-right font-medium px-3 py-2">n</th>
              {ALL_METRICS.map((m) => (
                <th key={m} className="text-right font-medium px-3 py-2 whitespace-nowrap">
                  {METRIC_LABEL[m]}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {axisBreakdowns.map((axis) => (
              <Fragment key={axis.axis}>
                <tr className="bg-[#0f1117] border-y border-gray-800/40">
                  <td
                    colSpan={ALL_METRICS.length + 2}
                    className="px-3 py-1.5 text-[11px] uppercase tracking-wider text-indigo-300 font-medium"
                  >
                    {axis.label}
                    <span className="ml-2 text-gray-600 normal-case font-normal">
                      ({axis.groups.length} distinct values)
                    </span>
                  </td>
                </tr>
                {axis.groups.map((g) => (
                  <tr
                    key={`${axis.axis}-${g.value}`}
                    className="border-b border-gray-800/20 hover:bg-white/[0.02]"
                  >
                    <td className="px-3 py-2 text-gray-200 font-mono text-xs">{g.value}</td>
                    <td className="px-3 py-2 text-right font-mono text-gray-500">{g.n}</td>
                    {ALL_METRICS.map((m) => {
                      const stat = g.stats[m] ?? null;
                      const isWinner = axis.winnerPerMetric[m] === g.value;
                      return (
                        <td
                          key={m}
                          className={`px-3 py-2 text-right font-mono whitespace-nowrap ${
                            isWinner
                              ? 'text-amber-300 bg-amber-500/[0.04] font-medium'
                              : 'text-gray-300'
                          }`}
                        >
                          {fmtMetric(m, stat?.mean ?? null)}
                          {stat?.std != null && (
                            <span className="ml-1 text-gray-500 text-[11px]">
                              (±{fmtMetricStd(m, stat.std)})
                            </span>
                          )}
                          {isWinner && stat != null && (
                            <span className="ml-1 text-amber-400">★</span>
                          )}
                        </td>
                      );
                    })}
                  </tr>
                ))}
              </Fragment>
            ))}
          </tbody>
        </table>
      </div>
    </CollapsibleCard>
  );
}

/** React.memo barrier — same rationale as the other heavy momentum cards.
 * Subscribes to `variants` directly so no props are needed; memo is
 * still helpful because the parent's other store subscriptions
 * (`active`, `run`, etc.) shouldn't trigger this analysis to rebuild. */
const VariantAttribution = memo(VariantAttributionInner);
export default VariantAttribution;
