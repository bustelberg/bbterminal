'use client';

import { memo, useMemo } from 'react';
import { type ChartCadence, type MetricRow } from './types';
import { dropExtremeOutliers } from './utils';
import { bandOf, type Band } from './MetricBandChart';
import { buildMemberSeries, weightedAverageSeries, type PortfolioMemberMetrics } from './portfolioBreakdown';
import type { SnapshotChartConfig } from './snapshotBandCharts';
import { chartTheme } from '../../../lib/chartTheme';

const BAND_DOT: Record<Band, string> = {
  good: chartTheme.pos,
  neutral: chartTheme.warn,
  poor: chartTheme.neg,
};
const BAND_WORD: Record<Band, string> = { good: 'good', neutral: 'caution', poor: 'poor' };
const NO_DATA = '#cbd5e1'; // soft gray when a metric has no value

type Props = {
  /** The banded charts to score (each carries a buildSeries + scoring rubric). */
  charts: SnapshotChartConfig[];
  /** Metrics of the company this row scores. */
  metrics: MetricRow[];
  cadence: ChartCadence;
  hideOutliers: boolean;
  /** Optional company name shown as a row prefix (used in comparison mode to
   * label the A vs B rows). */
  label?: string;
  /** Shared min-width (in `ch`) for the label so both rows' circles align
   * regardless of name length — pass the longer of the two names' lengths. */
  labelMinCh?: number;
  /** Per-member metrics when this row scores a portfolio — the latest value is
   * then the weighted-member average, matching the band charts below. */
  breakdown?: PortfolioMemberMetrics[];
};

/** At-a-glance scorecard above the Charts grid: one green/amber/red circle per
 * banded chart, coloured by where the company's LATEST value lands in that
 * chart's scoring rubric. Reads from the same config the charts render from,
 * so a circle always agrees with its chart's latest-point colour. */
function BandScorecardInner({ charts, metrics, cadence, hideOutliers, label, labelMinCh, breakdown }: Props) {
  const items = useMemo(() => {
    return charts.map((c) => {
      const buildOne = (m: MetricRow[]) => (hideOutliers ? dropExtremeOutliers(c.buildSeries(m, cadence)) : c.buildSeries(m, cadence));
      // Portfolio → weighted-member average (matches the band chart's line);
      // single company → its own series.
      const series = breakdown ? weightedAverageSeries(buildMemberSeries(breakdown, buildOne)) : buildOne(metrics);
      const latest = series.length > 0 ? series[series.length - 1].value : null;
      const band = bandOf(latest, c.band);
      return {
        key: c.key,
        title: c.title,
        band,
        valueLabel: latest == null ? 'no data' : c.format(latest),
      };
    });
  }, [charts, metrics, cadence, hideOutliers, breakdown]);

  return (
    <div className="flex flex-wrap items-center gap-x-4 gap-y-2">
      {label && (
        <span
          className="text-xs font-medium text-fg-soft mr-0.5 inline-block whitespace-nowrap"
          style={labelMinCh ? { minWidth: `${labelMinCh}ch` } : undefined}
        >
          {label}
        </span>
      )}
      {items.map((it) => (
        <div
          key={it.key}
          className="flex items-center gap-1.5 text-xs text-fg-muted"
          title={`${it.title}: ${it.valueLabel}${it.band ? ` (${BAND_WORD[it.band]})` : ''}`}
        >
          <span
            className="inline-block w-2.5 h-2.5 rounded-full shrink-0"
            style={{ background: it.band ? BAND_DOT[it.band] : NO_DATA }}
          />
          <span className="truncate">{it.title}</span>
        </div>
      ))}
    </div>
  );
}

// Memoized — see MetricBandChart: avoids re-rendering on SSE-log churn during refresh.
const BandScorecard = memo(BandScorecardInner);
export default BandScorecard;
