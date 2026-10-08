'use client';

import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { track } from '../../../lib/loading';
import { type Basket } from './types';
import PanelDialog from './PanelDialog';
import {
  egmSource, estimateCagrWorking, medianPEWorking, reverseDcfSource, reverseDcfWorking,
  type MedianPeWorking, type SourceObs,
} from './egmInputs';
import { calculateEGM, egmStorageKey, EGM_DEFAULTS, type EgmOverrides } from './egm';
import { forwardLegs, normalisedFcf } from './normalisedFcf';
import {
  FORECAST_YEARS, impliedGrowth, marketCapOf, PERPETUITY_GROWTH,
} from './reverseDcf';
import { type MetricRow } from './quickValuation';
import InfoTip from '../InfoTip';
import OwnerEarningsModal from './OwnerEarningsModal';
import { AspectCard, type FormulaSymbol } from '../../../lib/tipCard';
import { Provenance } from '../../../lib/provenance';
import { onDate } from './asOfLine';
import PortfolioFundamentalsRefresh, {
  type FundamentalRefreshFeeds, type RefreshScope,
} from './PortfolioFundamentalsRefresh';
import {
  workedEgmReturn, workedEgmTotalReturn, workedFairValue, workedFairValueGap,
  workedImpliedPrice,
} from './valuationFormulas';
import { ANALYSE_COPY } from './analyseCopy';

type ApiMetric = MetricRow & { recorded_at?: string | null };

type BookWeight = {
  holding_name: string;
  current_value_eur: number;
  total_current_value_eur: number;
  as_of_date: string;
  fetched_at?: string | null;
};

type ApiRow = {
  company_id: number;
  isin: string;
  name: string;
  weight_pct: number;
  book_weight?: BookWeight | null;
  currency?: string | null;
  source_fetched_at: {
    financials?: string | null;
    estimates?: string | null;
    indicators?: string | null;
  };
  metrics: ApiMetric[];
};

type Payload = {
  coverage: { covered_pct?: number; holdings?: number; by_reason_pct?: Record<string, number> };
  rows: ApiRow[];
};

/** The lightweight AIRS-book payload used to calculate the displayed portfolio weights. */
type BookWeightsPayload = {
  as_of_date?: string | null;
  fetched_at?: string | null;
  total_current_value_eur?: number | null;
  rows?: {
    holding_name?: string | null;
    isin?: string | null;
    current_value_eur?: number | null;
  }[];
};

const RATES = Array.from({ length: 14 }, (_, i) => (7 + i) / 100);
const EPS_ESTIMATE_CODE = 'annual_per_share_eps_estimate';
const EPS_ACTUAL_CODES = new Set([
  'annuals__Per Share Data__EPS without NRI',
  'annuals__per_share_data__EPS without NRI',
  'annuals__per_share_data_array__EPS without NRI',
]);
const OCF_ESTIMATE_CODE = 'annual_operating_cash_flow_estimate';
const OCF_ACTUAL_CODES = new Set([
  'annuals__Cashflow Statement__Cash Flow from Operations',
  'annuals__cashflow_statement__Cash Flow from Operations',
]);
const HISTORICAL_PRICE_CODES = new Set([
  'annuals__Per Share Data__Month End Stock Price',
  'annuals__per_share_data__Month End Stock Price',
  'annuals__per_share_data_array__Month End Stock Price',
]);
const DILUTED_SHARE_CODES = new Set([
  'annuals__Income Statement__Shares Outstanding (Diluted Average)',
  'annuals__income_statement__Shares Outstanding (Diluted Average)',
]);
const EPS_YEARS = [2025, 2026, 2027] as const;
type EpsYear = typeof EPS_YEARS[number];
type EpsYearObservation = {
  metric: ApiMetric;
  kind: 'actual' | 'estimate';
  actual: ApiMetric | null;
  estimate: ApiMetric | null;
};
type OcfYearObservation = EpsYearObservation;
type HistoricalOcfRow = {
  year: number;
  price: ApiMetric | null;
  shares: ApiMetric | null;
  ocf: ApiMetric | null;
  multiple: number | null;
  used: boolean;
};
type HistoricalOcfWorking = { rows: HistoricalOcfRow[]; median: number | null };
type Model = 'dcf' | 'egm' | 'eps' | 'ocf';
const MODEL_LABEL: Record<Model, string> = {
  dcf: 'Reverse DCF',
  egm: 'Expected Growth Model',
  eps: 'EPS Estimates',
  ocf: 'OCF Estimates',
};
const COMPANY_REFRESH: Record<Model, {
  feeds: FundamentalRefreshFeeds;
  prices: boolean;
  keyRatios: boolean;
}> = {
  dcf: { feeds: 'statements_estimates', prices: true, keyRatios: true },
  egm: { feeds: 'all', prices: true, keyRatios: false },
  eps: { feeds: 'statements_estimates', prices: true, keyRatios: false },
  ocf: { feeds: 'statements_estimates', prices: true, keyRatios: false },
};
const LOADING_DOTS = ['...', '..', '.', '..'] as const;
type Sort = { key: string; direction: 'asc' | 'desc' };
type InputObservation = {
  label: string;
  value: string;
  retrieved: string | null;
  applies: string | (string | null | undefined)[] | null;
  /** Plain-language provenance when a date would be false or ambiguous (for example, a derived
   *  multiple or a house fallback). */
  retrievedText?: string;
  /** Plain-language period semantics for a range or for two operands with different dates. */
  appliesText?: string;
};

function uniqueDates(values: (string | null | undefined)[]): string[] {
  const byDay = new Map<string, string>();
  for (const value of values) {
    if (!value) continue;
    const day = value.slice(0, 10);
    if (/^\d{4}-\d{2}-\d{2}$/.test(day)) byDay.set(day, value);
  }
  return [...byDay.values()].sort();
}

function dateWindow(values: (string | null | undefined)[]): string[] {
  const dates = uniqueDates(values);
  return dates.length > 1 ? [dates[0], dates[dates.length - 1]] : dates;
}

function InputObservationRows({ inputs }: { inputs: InputObservation[] }) {
  return (
    <span className="block space-y-2">
      {inputs.map((input) => {
        const applies = uniqueDates(Array.isArray(input.applies) ? input.applies : [input.applies]);
        return (
        <span key={`${input.label}:${applies.join(',')}`} className="block border-b border-neutral-800/50 pb-2 last:border-0 last:pb-0">
          <span className="flex flex-wrap items-center gap-1.5">
            <span>{input.label}</span>
            <span className="inline-flex rounded-full border border-neutral-700 bg-overlay/10 px-1.5 py-0.5 font-mono text-[11px] text-fg-soft">
              {input.value}
            </span>
          </span>
          <span className="mt-1 grid gap-1 text-fg-faint">
            <span className="inline-flex items-center gap-1">
              <span>Retrieved</span>
              <span className="inline-flex rounded-full border border-neutral-700 bg-overlay/10 px-1.5 py-0.5 font-mono text-[11px] text-fg-soft">
                {input.retrievedText ?? (input.retrieved ? onDate(input.retrieved) : 'not recorded')}
              </span>
            </span>
            <span className="flex items-start gap-1">
              <span className="shrink-0 pt-0.5">Applies to</span>
              <span className="flex flex-col items-start gap-1">
                {input.appliesText ? (
                  <span className="inline-flex rounded-full border border-neutral-700 bg-overlay/10 px-1.5 py-0.5 font-mono text-[11px] text-fg-soft">
                    {input.appliesText}
                  </span>
                ) : (applies.length ? applies : [null]).map((date, index) => (
                  <span key={date ?? index} className="inline-flex rounded-full border border-neutral-700 bg-overlay/10 px-1.5 py-0.5 font-mono text-[11px] text-fg-soft">
                    {date ? onDate(date) : 'not recorded'}
                  </span>
                ))}
              </span>
            </span>
          </span>
        </span>
        );
      })}
    </span>
  );
}

function DateBadges({ retrieved, applies, inputs }: {
  retrieved: (string | null | undefined)[];
  applies: (string | null | undefined)[];
  inputs?: InputObservation[];
}) {
  if (inputs?.length) return <InputObservationRows inputs={inputs} />;
  const rows = [
    ['Retrieved', retrieved, uniqueDates(retrieved)],
    ['Applies to', applies, uniqueDates(applies)],
  ] as const;
  return (
    <span className="block space-y-1.5">
      {rows.map(([label, supplied, dates]) => (
        <span key={label} className="flex flex-wrap items-center gap-1.5">
          <span>{label}</span>
          {(dates.length ? dates : [null]).map((date, index) => (
            <span key={date ?? index}
              className="inline-flex rounded-full border border-neutral-700 bg-overlay/10 px-1.5 py-0.5 font-mono text-[11px] text-fg-soft">
              {date ? onDate(date) : supplied.length ? 'not recorded' : 'not applicable'}
            </span>
          ))}
        </span>
      ))}
    </span>
  );
}

function ValuationCell({ value, what, where, how, worked, legend, retrieved, applies, inputs, tone,
  emphasis = false }: {
  value: ReactNode;
  what: ReactNode;
  where: ReactNode;
  how: ReactNode;
  worked?: string;
  legend?: readonly FormulaSymbol[];
  retrieved: (string | null | undefined)[];
  applies: (string | null | undefined)[];
  inputs?: InputObservation[];
  tone: 'dcf' | 'egm' | 'eps' | 'ocf';
  emphasis?: boolean;
}) {
  return (
    <td className={`${tone === 'dcf' ? 'bg-accent-500/[0.035]'
      : tone === 'egm' ? 'bg-pos-500/[0.035]'
        : tone === 'eps' ? 'bg-warn-500/[0.035]' : 'bg-sky-500/[0.035]'} px-3 py-2 text-right font-mono tabular-nums ${emphasis ? 'font-semibold text-fg-strong' : ''}`}>
      <span className="flex items-center justify-end gap-1.5 whitespace-nowrap">
        <span>{value}</span>
        <InfoTip wide className="font-sans text-fg-faint" content={(
          <AspectCard what={what} where={where}
            when={<DateBadges retrieved={retrieved} applies={applies} inputs={inputs} />}
            how={how} worked={worked} legend={legend} />
        )} />
      </span>
    </td>
  );
}

const inputNumber = new Intl.NumberFormat('en-GB', { maximumFractionDigits: 2 });
const compactNumber = new Intl.NumberFormat('en-GB', {
  notation: 'compact', compactDisplay: 'short', maximumFractionDigits: 2,
});
const eurWhole = new Intl.NumberFormat('en-GB', {
  style: 'currency', currency: 'EUR', minimumFractionDigits: 0, maximumFractionDigits: 0,
});

/** GuruFocus company-level statement values are stored in millions; render their real scale. */
export function compactMillions(value: number, suffix = ''): string {
  return `${compactNumber.format(value * 1_000_000)}${suffix}`;
}

function bookWeightPct(bookWeight: BookWeight): number {
  return bookWeight.current_value_eur / bookWeight.total_current_value_eur * 100;
}

export function displayedWeightPct(
  modelWeightPct: number,
  bookWeight?: Pick<BookWeight, 'current_value_eur' | 'total_current_value_eur'> | null,
): number {
  return bookWeight
    ? bookWeight.current_value_eur / bookWeight.total_current_value_eur * 100
    : modelWeightPct;
}

/**
 * A refusal is an answer, not an empty cell.
 *
 * A reverse DCF can only compound a positive starting cash flow. Tesla exposed the otherwise
 * invisible branch: every source operand was present in the info card, but its FY1 FCF normalised
 * to a negative number, so `solveGrowth` correctly returned null and fourteen bare dashes made
 * that look like missing data. Name all three outcomes: missing operands, a non-positive FCF base,
 * and a complete input set for which the solver finds no rate in its supported range.
 */
export function dcfGrowthCellLabel(impliedGrowth: number | null,
  normalisedStartingFcf: number | null, targetMarketCap: number | null = null): string {
  if (impliedGrowth != null) return `${(impliedGrowth * 100).toFixed(1)}%`;
  if (normalisedStartingFcf != null && normalisedStartingFcf <= 0) return 'No +FCF';
  if (normalisedStartingFcf == null || targetMarketCap == null) return 'Missing inputs';
  return 'No solution';
}

/** The final line of the worked five-year median shown in the Exit P/E info card. */
export function medianPeCalculation(working: MedianPeWorking): string {
  const pes = working.rows
    .filter((row) => row.used && row.pe != null)
    .map((row) => row.pe as number)
    .sort((a, b) => a - b);
  if (!pes.length || working.median == null) return 'No positive fiscal-year P/E is available.';
  const shown = pes.map((pe) => `${inputNumber.format(pe)}×`).join(', ');
  const middle = Math.floor(pes.length / 2);
  if (pes.length % 2) {
    return `Sorted fiscal-year P/Es: ${shown}. Median = middle value ${inputNumber.format(pes[middle])}×.`;
  }
  const medianStep = `(${inputNumber.format(pes[middle - 1])} + ${inputNumber.format(pes[middle])}) ÷ 2`;
  return `Sorted fiscal-year P/Es: ${shown}. Median = ${medianStep} = ${inputNumber.format(working.median)}×.`;
}

function observationRetrievedAt(metrics: ApiMetric[], observation: SourceObs): string | null {
  if (!observation.code || !observation.date) return null;
  return metrics
    .filter((metric) => metric.metric_code === observation.code
      && metric.target_date === observation.date && metric.recorded_at)
    .map((metric) => metric.recorded_at as string)
    .sort()
    .at(-1) ?? null;
}

export function sourceInput(metrics: ApiMetric[], label: string, observation: SourceObs,
  suffix = ''): InputObservation {
  if (observation.raw == null) return {
    label,
    value: 'not available',
    retrieved: null,
    applies: null,
    retrievedText: 'not available',
    appliesText: 'not available',
  };
  return {
    label: observation.ttm ? `TTM ${label}` : label,
    value: `${inputNumber.format(observation.raw)}${suffix}`,
    retrieved: observationRetrievedAt(metrics, observation),
    applies: observation.date,
  };
}

function sourceMillionsInput(metrics: ApiMetric[], label: string, observation: SourceObs,
  suffix = ''): InputObservation {
  const input = sourceInput(metrics, label, observation);
  return observation.raw == null
    ? input
    : { ...input, value: compactMillions(observation.raw, suffix) };
}

function medianPeInputs(metrics: ApiMetric[], working: MedianPeWorking): InputObservation[] {
  return working.rows.map((point) => {
    const observations = metrics.filter((metric) =>
      metric.target_date.slice(0, 4) === String(point.year)
      && metric.numeric_value != null
      && (metric.metric_code.endsWith('__Month End Stock Price')
        || metric.metric_code.endsWith('__EPS without NRI')));
    const retrieved = observations.map((metric) => metric.recorded_at ?? null)
      .filter((value): value is string => value != null).sort().at(-1) ?? null;
    const applies = observations.map((metric) => metric.target_date).sort().at(-1) ?? null;
    const price = point.price == null ? 'missing price' : inputNumber.format(point.price);
    const eps = point.eps == null ? 'missing EPS' : inputNumber.format(point.eps);
    return {
      label: `FY${point.year}: price ÷ EPS`,
      value: point.used && point.pe != null
        ? `${price} ÷ ${eps} = ${inputNumber.format(point.pe)}×`
        : `${price} ÷ ${eps} = excluded (EPS is not positive)`,
      retrieved,
      applies,
    };
  });
}

export function epsEstimateForYear(metrics: ApiMetric[], year: number): ApiMetric | null {
  return metrics
    .filter((metric) => metric.metric_code === EPS_ESTIMATE_CODE
      && metric.numeric_value != null && metric.target_date.slice(0, 4) === String(year))
    .sort((a, b) => b.target_date.localeCompare(a.target_date)
      || (b.recorded_at ?? '').localeCompare(a.recorded_at ?? ''))[0] ?? null;
}

export function epsActualForYear(metrics: ApiMetric[], year: number): ApiMetric | null {
  return metrics
    .filter((metric) => EPS_ACTUAL_CODES.has(metric.metric_code)
      && metric.numeric_value != null && metric.target_date.slice(0, 4) === String(year))
    .sort((a, b) => b.target_date.localeCompare(a.target_date)
      || (b.recorded_at ?? '').localeCompare(a.recorded_at ?? ''))[0] ?? null;
}

/** Prefer the reported fiscal-year result; use consensus only while that actual is unavailable. */
export function epsObservationForYear(metrics: ApiMetric[], year: number): EpsYearObservation | null {
  const actual = epsActualForYear(metrics, year);
  const estimate = epsEstimateForYear(metrics, year);
  if (actual) return { metric: actual, kind: 'actual', actual, estimate };
  if (estimate) return { metric: estimate, kind: 'estimate', actual, estimate };
  return null;
}

export function epsYearWhat(name: string, year: number,
  kind: EpsYearObservation['kind'] | null): string {
  if (kind === 'actual') return `Reported FY${year} EPS without NRI for ${name}.`;
  if (kind === 'estimate') return `FY${year} consensus EPS estimate for ${name}; no actual is stored.`;
  return `No FY${year} actual or consensus EPS is stored for ${name}.`;
}

export function epsYearHow(year: number, kind: EpsYearObservation['kind'] | null,
  targetDate: string | null): string {
  if (kind === 'actual') return 'Use reported EPS without NRI; it takes priority over consensus.';
  if (kind === 'estimate') {
    return `No reported FY${year} EPS is stored; use consensus for ${onDate(targetDate)}.`;
  }
  return `Neither annual statements nor analyst estimates supplies FY${year} EPS.`;
}

export function epsActualToEstimateCagr2025To2027(metrics: ApiMetric[]): number | null {
  const start = epsObservationForYear(metrics, 2025)?.metric.numeric_value ?? null;
  const end = epsObservationForYear(metrics, 2027)?.metric.numeric_value ?? null;
  if (start == null || start <= 0 || end == null || end <= 0) return null;
  return Math.pow(end / start, 1 / 2) - 1;
}

export function priceToEpsMultiple(price: number | null, eps: number | null): number | null {
  if (price == null || price <= 0 || eps == null || eps <= 0) return null;
  return price / eps;
}

/** How far one fiscal-year P/E sits above or below the completed-ten-year median P/E. */
export function peDeltaFromHistoricalMedian(pe: number | null, median: number | null): number | null {
  if (pe == null || median == null || median <= 0) return null;
  return pe / median - 1;
}

export function epsInput(metric: ApiMetric | null, label: string,
  currency?: string | null, feedCheckedAt?: string | null): InputObservation[] {
  if (metric?.numeric_value == null) return [{
    label,
    value: 'not supplied by GuruFocus',
    retrieved: feedCheckedAt ?? null,
    applies: null,
    retrievedText: feedCheckedAt ? undefined : 'not recorded',
    appliesText: 'No matching fiscal period was returned',
  }];
  return [{
    label,
    value: `${inputNumber.format(metric.numeric_value)}${currency ? ` ${currency}/share` : ' per share'}`,
    retrieved: metric.recorded_at ?? null,
    applies: metric.target_date,
  }];
}

function epsYearInputs(observation: EpsYearObservation | null, year: EpsYear,
  currency: string | null | undefined, financialsCheckedAt: string | null | undefined,
  estimatesCheckedAt: string | null | undefined): InputObservation[] {
  return [
    ...epsInput(observation?.actual ?? null,
      `Reported EPS for FY${year}${observation?.kind === 'actual' ? ' (used)' : ''}`,
      currency, financialsCheckedAt),
    ...epsInput(observation?.estimate ?? null,
      `Consensus EPS estimate for FY${year}${observation?.kind === 'estimate' ? ' (used)' : ''}`,
      currency, estimatesCheckedAt),
  ];
}

function latestMetricForYear(metrics: ApiMetric[], codes: ReadonlySet<string>,
  year: number): ApiMetric | null {
  return metrics
    .filter((metric) => codes.has(metric.metric_code) && metric.numeric_value != null
      && metric.target_date.slice(0, 4) === String(year))
    .sort((a, b) => b.target_date.localeCompare(a.target_date)
      || (b.recorded_at ?? '').localeCompare(a.recorded_at ?? ''))[0] ?? null;
}

export function ocfEstimateForYear(metrics: ApiMetric[], year: number): ApiMetric | null {
  return latestMetricForYear(metrics, new Set([OCF_ESTIMATE_CODE]), year);
}

export function ocfActualForYear(metrics: ApiMetric[], year: number): ApiMetric | null {
  return latestMetricForYear(metrics, OCF_ACTUAL_CODES, year);
}

/** Prefer reported OCF; use the consensus estimate until the annual statement is available. */
export function ocfObservationForYear(metrics: ApiMetric[], year: number): OcfYearObservation | null {
  const actual = ocfActualForYear(metrics, year);
  const estimate = ocfEstimateForYear(metrics, year);
  if (actual) return { metric: actual, kind: 'actual', actual, estimate };
  if (estimate) return { metric: estimate, kind: 'estimate', actual, estimate };
  return null;
}

export function ocfActualToEstimateCagr2025To2027(metrics: ApiMetric[]): number | null {
  const start = ocfObservationForYear(metrics, 2025)?.metric.numeric_value ?? null;
  const end = ocfObservationForYear(metrics, 2027)?.metric.numeric_value ?? null;
  if (start == null || start <= 0 || end == null || end <= 0) return null;
  return Math.pow(end / start, 1 / 2) - 1;
}

/** Operating cash flow and diluted shares are both in millions, leaving cash flow per share. */
export function operatingCashFlowPerShare(ocf: number | null, dilutedShares: number | null): number | null {
  if (ocf == null || dilutedShares == null || dilutedShares <= 0) return null;
  return ocf / dilutedShares;
}

/** Current market capitalisation divided by the selected company-level OCF, all in millions. */
export function priceToOcfMultiple(price: number | null, dilutedShares: number | null,
  ocf: number | null): number | null {
  const ocfPerShare = operatingCashFlowPerShare(ocf, dilutedShares);
  if (price == null || price <= 0 || ocfPerShare == null || ocfPerShare <= 0) return null;
  return price / ocfPerShare;
}

export function historicalOcfMultipleWorking(metrics: ApiMetric[], years = 10): HistoricalOcfWorking {
  const candidates = new Set<number>();
  for (const metric of metrics) {
    if (OCF_ACTUAL_CODES.has(metric.metric_code) && metric.numeric_value != null) {
      candidates.add(Number(metric.target_date.slice(0, 4)));
    }
  }
  const completedYears = [...candidates].filter(Number.isFinite).sort((a, b) => a - b).slice(-years);
  const rows = completedYears.map((year): HistoricalOcfRow => {
    const price = latestMetricForYear(metrics, HISTORICAL_PRICE_CODES, year);
    const shares = latestMetricForYear(metrics, DILUTED_SHARE_CODES, year);
    const ocf = ocfActualForYear(metrics, year);
    const multiple = priceToOcfMultiple(
      price?.numeric_value ?? null,
      shares?.numeric_value ?? null,
      ocf?.numeric_value ?? null,
    );
    return { year, price, shares, ocf, multiple, used: multiple != null };
  });
  const multiples = rows.filter((row) => row.used).map((row) => row.multiple as number)
    .sort((a, b) => a - b);
  if (!multiples.length) return { rows, median: null };
  const middle = Math.floor(multiples.length / 2);
  const median = multiples.length % 2
    ? multiples[middle]
    : (multiples[middle - 1] + multiples[middle]) / 2;
  return { rows, median };
}

function ocfInput(metric: ApiMetric | null, label: string, currency: string | null | undefined,
  feedCheckedAt: string | null | undefined): InputObservation {
  if (metric?.numeric_value == null) return {
    label,
    value: 'not supplied by GuruFocus',
    retrieved: feedCheckedAt ?? null,
    applies: null,
    retrievedText: feedCheckedAt ? undefined : 'not recorded',
    appliesText: 'No matching fiscal period was returned',
  };
  return {
    label,
    value: compactMillions(metric.numeric_value, currency ? ` ${currency}` : ''),
    retrieved: metric.recorded_at ?? null,
    applies: metric.target_date,
  };
}

function ocfYearInputs(observation: OcfYearObservation | null, year: EpsYear,
  currency: string | null | undefined, financialsCheckedAt: string | null | undefined,
  estimatesCheckedAt: string | null | undefined): InputObservation[] {
  return [
    ocfInput(observation?.actual ?? null,
      `Reported OCF for FY${year}${observation?.kind === 'actual' ? ' (used)' : ''}`,
      currency, financialsCheckedAt),
    ocfInput(observation?.estimate ?? null,
      `Consensus OCF estimate for FY${year}${observation?.kind === 'estimate' ? ' (used)' : ''}`,
      currency, estimatesCheckedAt),
  ];
}

function ocfPerShareWhat(name: string, year: EpsYear, kind: OcfYearObservation['kind'] | null): string {
  if (kind === 'actual') return `FY${year} reported OCF/share for ${name}.`;
  if (kind === 'estimate') return `FY${year} consensus OCF/share for ${name}; no actual is stored.`;
  return `No FY${year} actual or consensus OCF is stored for ${name}.`;
}

function ocfPerShareHow(year: EpsYear, kind: OcfYearObservation['kind'] | null): string {
  if (kind === 'actual') return 'Divide reported OCF by current diluted shares.';
  if (kind === 'estimate') return `Divide consensus OCF by current diluted shares; no FY${year} actual is stored.`;
  return `Neither statements nor consensus supplies FY${year} OCF.`;
}

function historicalOcfInputs(working: HistoricalOcfWorking,
  currency: string | null | undefined): InputObservation[] {
  return working.rows.map((row) => ({
    label: `FY${row.year}: market value ÷ OCF`,
    value: row.multiple == null
      ? 'excluded; one or more positive inputs are missing'
      : `${inputNumber.format(row.price!.numeric_value!)} × ${compactMillions(row.shares!.numeric_value!, ' shares')}`
        + ` ÷ ${compactMillions(row.ocf!.numeric_value!, currency ? ` ${currency}` : '')}`
        + ` = ${inputNumber.format(row.multiple)}×`,
    retrieved: [row.price?.recorded_at, row.shares?.recorded_at, row.ocf?.recorded_at]
      .filter((value): value is string => value != null).sort().at(-1) ?? null,
    applies: [row.price?.target_date, row.shares?.target_date, row.ocf?.target_date],
    appliesText: currency ? `Price and cash flow reported in ${currency}` : undefined,
  }));
}

function dcfRow(row: ApiRow, today: string, egmOverrides?: EgmOverrides) {
  const src = reverseDcfSource(row.metrics, today);
  const working = reverseDcfWorking(row.metrics, today);
  const forward = forwardLegs({
    ocfEstimate: src.ocfEstimate, fcfEstimate: src.fcfEstimate,
    ebitdaEstimate: src.ebitdaEstimate, ebitEstimate: src.ebitEstimate,
    capex: src.capex, dep: src.dep, normalise: true,
  });
  const useForward = forward.fcf != null;
  const base = useForward ? forward.fcf : src.fcf;
  const adjusted = normalisedFcf({
    fcf: base,
    sbc: src.sbc,
    capex: useForward ? forward.capex : src.capex,
    dep: useForward ? forward.dep : src.dep,
  }).used;
  const target = marketCapOf(src);
  const growth = impliedGrowth(src, {
    years: FORECAST_YEARS,
    perpetuityGrowth: PERPETUITY_GROWTH,
    discountRates: RATES,
    fcfOverride: adjusted,
    targetOverride: target,
  });
  const egm = egmSource(row.metrics, today);
  const forwardPeDerived = egm.price != null && egm.epsNextFY != null && egm.epsNextFY > 0;
  const forwardPE = forwardPeDerived
    ? (egm.price as number) / (egm.epsNextFY as number) : egm.forwardPE;
  const cagrWorking = estimateCagrWorking(row.metrics, today);
  const estimateDates = dateWindow(cagrWorking.points.map((point) => point.date));
  const medianPeWorking = medianPEWorking(row.metrics);
  const historicalPe10yWorking = medianPEWorking(row.metrics, 10);
  const medianPeYears = new Set(medianPeWorking.rows.map((point) => String(point.year)));
  const medianPeDates = dateWindow(row.metrics
    .filter((metric) => medianPeYears.has(metric.target_date.slice(0, 4))
      && (metric.metric_code.endsWith('__Month End Stock Price')
        || metric.metric_code.endsWith('__EPS without NRI')))
    .map((metric) => metric.target_date));
  const medianPeObservations = medianPeInputs(row.metrics, medianPeWorking);
  const historicalPe10yObservations = medianPeInputs(row.metrics, historicalPe10yWorking);
  const medianPeWorked = medianPeCalculation(medianPeWorking);
  const historicalPe10yWorked = medianPeCalculation(historicalPe10yWorking);
  const egmAssumptions = {
    growthRate: egmOverrides?.growthRate ?? null,
    dividendYield: egm.dividendYield,
    exitPE: egmOverrides?.exitPE ?? null,
    hurdleRate: EGM_DEFAULTS.hurdleRate,
    years: EGM_DEFAULTS.years,
  };
  const forwardPeObservations: InputObservation[] = forwardPeDerived ? [
    {
      label: 'Current share price',
      value: egm.price == null ? 'not available'
        : `${inputNumber.format(egm.price)}${row.currency ? ` ${row.currency}/share` : ''}`,
      retrieved: row.source_fetched_at.financials ?? null,
      applies: egm.priceDate,
    },
    {
      label: 'FY1 consensus EPS',
      value: egm.epsNextFY == null ? 'not available'
        : `${inputNumber.format(egm.epsNextFY)}${row.currency ? ` ${row.currency}/share` : ''}`,
      retrieved: row.source_fetched_at.estimates ?? null,
      applies: egm.epsNextFYDate,
    },
    {
      label: 'Current forward P/E (price ÷ FY1 EPS)',
      value: forwardPE == null ? 'not available' : `${inputNumber.format(forwardPE)}×`,
      retrieved: null,
      retrievedText: 'Not retrieved; calculated here',
      applies: null,
      appliesText: 'The price and FY1 EPS observations above',
    },
  ] : [{
    label: 'Current forward P/E',
    value: forwardPE == null ? 'not available' : `${inputNumber.format(forwardPE)}×`,
    retrieved: row.source_fetched_at.indicators ?? null,
    applies: egm.forwardPEDate,
  }];
  const expectedReturnInputs: InputObservation[] = [
    ...forwardPeObservations,
    {
      label: 'Expected EPS growth',
      value: egmAssumptions.growthRate == null ? 'not set' : `${(egmAssumptions.growthRate * 100).toFixed(1)}%`,
      retrieved: null,
      applies: null,
      retrievedText: 'Enter this assumption in the EGM tab',
      appliesText: 'Every forecast year',
    },
    {
      label: 'Dividend yield',
      value: `${((egmAssumptions.dividendYield ?? 0) * 100).toFixed(2)}%`,
      retrieved: egm.dividendYield != null ? row.source_fetched_at.financials ?? null : null,
      applies: egm.dividendYield != null ? egm.dividendYieldDate ?? null : null,
      retrievedText: egm.dividendYield == null ? 'Not reported; model uses 0%' : undefined,
      appliesText: egm.dividendYield == null ? 'Every forecast year' : undefined,
    },
    {
      label: 'Exit P/E',
      value: egmAssumptions.exitPE == null ? 'not set' : `${inputNumber.format(egmAssumptions.exitPE)}×`,
      retrieved: null,
      applies: null,
      retrievedText: 'Enter this assumption in the EGM tab',
      appliesText: 'End of year 5',
    },
  ];
  const egmResult = calculateEGM({ ...egm, forwardPE }, egmAssumptions);
  const fairValueInputs: InputObservation[] = [
    {
      label: 'FY1 consensus EPS',
      value: egm.epsNextFY == null
        ? 'not available'
        : `${inputNumber.format(egm.epsNextFY)}${row.currency ? ` ${row.currency}` : ''}/share`,
      retrieved: egm.epsNextFY == null ? null : row.source_fetched_at.estimates ?? null,
      applies: egm.epsNextFYDate,
      retrievedText: egm.epsNextFY == null ? 'not available' : undefined,
      appliesText: egm.epsNextFYDate == null ? 'not available' : undefined,
    },
    {
      label: 'Maximum starting P/E',
      value: egmResult.maxPE == null ? 'not available' : `${inputNumber.format(egmResult.maxPE)}×`,
      retrieved: null,
      applies: null,
      retrievedText: egmResult.maxPE == null ? 'not available' : 'Not retrieved; calculated here',
      appliesText: egmResult.maxPE == null
        ? 'not available'
        : 'The current growth, dividend, exit P/E and 10-year hurdle assumptions',
    },
  ];
  const epsObservations = Object.fromEntries(EPS_YEARS.map((year) => [
    year, epsObservationForYear(row.metrics, year),
  ])) as Record<EpsYear, EpsYearObservation | null>;
  const epsEstimateCagr = epsActualToEstimateCagr2025To2027(row.metrics);
  const epsByYear = {
    2025: epsObservations[2025]?.metric.numeric_value ?? null,
    2026: epsObservations[2026]?.metric.numeric_value ?? null,
    2027: epsObservations[2027]?.metric.numeric_value ?? null,
  } as const;
  const peByYear = {
    2025: priceToEpsMultiple(src.price, epsByYear[2025]),
    2026: priceToEpsMultiple(src.price, epsByYear[2026]),
    2027: priceToEpsMultiple(src.price, epsByYear[2027]),
  } as const;
  const peDeltaByYear = {
    2025: peDeltaFromHistoricalMedian(peByYear[2025], historicalPe10yWorking.median),
    2026: peDeltaFromHistoricalMedian(peByYear[2026], historicalPe10yWorking.median),
    2027: peDeltaFromHistoricalMedian(peByYear[2027], historicalPe10yWorking.median),
  } as const;
  const ocfObservations = Object.fromEntries(EPS_YEARS.map((year) => [
    year, ocfObservationForYear(row.metrics, year),
  ])) as Record<EpsYear, OcfYearObservation | null>;
  const ocfByYear = {
    2025: ocfObservations[2025]?.metric.numeric_value ?? null,
    2026: ocfObservations[2026]?.metric.numeric_value ?? null,
    2027: ocfObservations[2027]?.metric.numeric_value ?? null,
  } as const;
  const ocfPerShareByYear = {
    2025: operatingCashFlowPerShare(ocfByYear[2025], src.sharesOutstanding),
    2026: operatingCashFlowPerShare(ocfByYear[2026], src.sharesOutstanding),
    2027: operatingCashFlowPerShare(ocfByYear[2027], src.sharesOutstanding),
  } as const;
  const ocfEstimateCagr = ocfPerShareByYear[2025] == null || ocfPerShareByYear[2025]! <= 0
    || ocfPerShareByYear[2027] == null || ocfPerShareByYear[2027]! <= 0
    ? null : Math.pow(ocfPerShareByYear[2027]! / ocfPerShareByYear[2025]!, 1 / 2) - 1;
  const historicalOcfWorking = historicalOcfMultipleWorking(row.metrics);
  const pOcfByYear = {
    2025: priceToOcfMultiple(src.price, src.sharesOutstanding, ocfByYear[2025]),
    2026: priceToOcfMultiple(src.price, src.sharesOutstanding, ocfByYear[2026]),
    2027: priceToOcfMultiple(src.price, src.sharesOutstanding, ocfByYear[2027]),
  } as const;
  const pOcfDeltaByYear = {
    2025: peDeltaFromHistoricalMedian(pOcfByYear[2025], historicalOcfWorking.median),
    2026: peDeltaFromHistoricalMedian(pOcfByYear[2026], historicalOcfWorking.median),
    2027: peDeltaFromHistoricalMedian(pOcfByYear[2027], historicalOcfWorking.median),
  } as const;
  const commonInputs = [
    sourceInput(row.metrics, 'Close price', working.price,
      row.currency ? ` ${row.currency}` : ''),
    sourceMillionsInput(row.metrics, 'Diluted shares outstanding', working.shares, ' shares'),
  ];
  const stockPriceInputs = commonInputs.slice(0, 1);
  const shareCountInputs = commonInputs.slice(1);
  const historicalOcfObservations = historicalOcfInputs(historicalOcfWorking, row.currency);
  const upsideInputs: InputObservation[] = [
    {
      label: 'Model fair value',
      value: egmResult.fairValue == null
        ? 'not available'
        : `${inputNumber.format(egmResult.fairValue)}${row.currency ? ` ${row.currency}` : ''}/share`,
      retrieved: null,
      applies: null,
      retrievedText: egmResult.fairValue == null ? 'not available' : 'Not retrieved; calculated here',
      appliesText: egmResult.fairValue == null
        ? 'not available'
        : 'Current Expected Growth Model valuation',
    },
    {
      ...stockPriceInputs[0],
      label: 'Current share price',
      value: stockPriceInputs[0].value === 'not available'
        ? stockPriceInputs[0].value
        : `${stockPriceInputs[0].value}/share`,
    },
  ];
  // Show the route the model attempted, even when one missing operand prevented that route from
  // becoming the solver's base. L'Oréal, for example, has an FY1 OCF estimate but no capex with
  // which to turn it into FCF; hiding the present OCF made the card show only a close price and
  // gave no account of why the otherwise-successful refresh still could not calculate.
  const attemptedForward = working.fcfEst.raw != null || working.ocfEst.raw != null;
  const cashFlowInputs = !attemptedForward
    ? [
      sourceMillionsInput(row.metrics, 'Free cash flow', working.fcf, row.currency ? ` ${row.currency}` : ''),
      sourceMillionsInput(row.metrics, 'Stock-based compensation', working.sbc, row.currency ? ` ${row.currency}` : ''),
      sourceMillionsInput(row.metrics, 'Capital expenditure', working.capex, row.currency ? ` ${row.currency}` : ''),
      sourceMillionsInput(row.metrics, 'Depreciation and amortisation', working.dep, row.currency ? ` ${row.currency}` : ''),
    ]
    : forward.vendor
      ? [
        sourceMillionsInput(row.metrics, 'FY1 free-cash-flow estimate', working.fcfEst, row.currency ? ` ${row.currency}` : ''),
        sourceMillionsInput(row.metrics, 'FY1 operating-cash-flow estimate', working.ocfEst, row.currency ? ` ${row.currency}` : ''),
        sourceMillionsInput(row.metrics, 'FY1 EBITDA estimate', working.ebitdaEst, row.currency ? ` ${row.currency}` : ''),
        sourceMillionsInput(row.metrics, 'FY1 EBIT estimate', working.ebitEst, row.currency ? ` ${row.currency}` : ''),
        sourceMillionsInput(row.metrics, 'Stock-based compensation', working.sbc, row.currency ? ` ${row.currency}` : ''),
      ]
      : [
        sourceMillionsInput(row.metrics, 'FY1 operating-cash-flow estimate', working.ocfEst, row.currency ? ` ${row.currency}` : ''),
        sourceMillionsInput(row.metrics, 'Capital expenditure', working.capex, row.currency ? ` ${row.currency}` : ''),
        sourceMillionsInput(row.metrics, 'Depreciation and amortisation', working.dep, row.currency ? ` ${row.currency}` : ''),
        sourceMillionsInput(row.metrics, 'Stock-based compensation', working.sbc, row.currency ? ` ${row.currency}` : ''),
      ];
  const dcfInputs = [...commonInputs, ...cashFlowInputs];
  return {
    ...row, src, growth,
    dcfForward: attemptedForward,
    // The base that actually reached the solver distinguishes missing inputs from a complete set
    // of inputs that says there is no positive cash flow to compound.
    dcfStartingFcf: adjusted,
    dcfInputs,
    stockPriceInputs,
    upsideInputs,
    egm, forwardPE, forwardPeDerived, egmAssumptions, egmResult,
    epsObservations, epsByYear, epsEstimateCagr, peByYear, peDeltaByYear,
    ocfObservations, ocfByYear, ocfPerShareByYear, ocfEstimateCagr, historicalOcfWorking,
    historicalOcfObservations, pOcfByYear, pOcfDeltaByYear, shareCountInputs,
    historicalPe10yWorking, historicalPe10yObservations, historicalPe10yWorked, fairValueInputs,
    estimateDates, medianPeDates, medianPeObservations, medianPeWorked, expectedReturnInputs,
  };
}

type ValuationRow = ReturnType<typeof dcfRow>;

function sortValue(row: ValuationRow, key: string): number | null {
  if (key.startsWith('rate:')) {
    const rate = Number(key.slice(5));
    return row.growth.find((cell) => cell.discountRate === rate)?.impliedGrowth ?? null;
  }
  if (key.startsWith('epsYear:')) {
    const year = Number(key.slice('epsYear:'.length)) as EpsYear;
    return row.epsByYear[year];
  }
  if (key.startsWith('epsPe:')) {
    const year = Number(key.slice('epsPe:'.length)) as 2025 | 2026 | 2027;
    return row.peByYear[year] ?? null;
  }
  if (key.startsWith('ocfYear:')) {
    const year = Number(key.slice('ocfYear:'.length)) as EpsYear;
    return row.ocfPerShareByYear[year];
  }
  if (key.startsWith('ocfMultiple:')) {
    const year = Number(key.slice('ocfMultiple:'.length)) as EpsYear;
    return row.pOcfByYear[year];
  }
  const values: Record<string, number | null> = {
    // Sort the number the reader sees. AIRS book weights can differ from the underlying
    // model-composition weight after certificate and TopSelectie look-through.
    weight: displayedWeightPct(row.weight_pct, row.book_weight),
    price: row.src.price,
    eps: row.egm.epsNextFY,
    forwardPE: row.forwardPE,
    epsGrowth: row.egmAssumptions.growthRate,
    dividend: row.egmAssumptions.dividendYield,
    exitPE: row.egmAssumptions.exitPE,
    fairValue: row.egmResult.fairValue,
    upside: row.egmResult.upside,
    expectedReturn: row.egmResult.expectedReturn,
    impliedPrice: row.egmResult.impliedPrice,
    totalReturn: row.egmResult.totalReturn,
    epsEstimateCagr: row.epsEstimateCagr,
    epsHistoricalPe10y: row.historicalPe10yWorking.median,
    ocfEstimateCagr: row.ocfEstimateCagr,
    ocfHistoricalMultiple10y: row.historicalOcfWorking.median,
  };
  return values[key] ?? null;
}

export default function PortfolioFundamentalModal({ name, portfolioId, basket, bookPortfolio, onClose }: {
  name: string;
  portfolioId?: number;
  basket?: Basket;
  bookPortfolio?: string;
  onClose: () => void;
}) {
  const [data, setData] = useState<Payload | null>(null);
  const [egmOverrides, setEgmOverrides] = useState<Record<string, EgmOverrides>>({});
  // `undefined` is loading; `null` means this view genuinely has no usable AIRS book.
  const [bookWeights, setBookWeights] = useState<BookWeightsPayload | null | undefined>(undefined);
  const [error, setError] = useState<string | null>(null);
  const [model, setModel] = useState<Model>('dcf');
  const [lookThroughCertificates, setLookThroughCertificates] = useState(false);
  // Direct holdings and certificate look-through are two views of the same book. Keep each
  // completed response for the lifetime of the modal so the checkbox can switch between them
  // immediately instead of repeating both API requests every time.
  const dataByCertificateScope = useRef(new Map<boolean, Payload>());
  const weightsByCertificateScope = useRef(new Map<boolean, BookWeightsPayload | null>());
  const selectedCertificateScope = useRef(false);
  const certificateScopeMessages = useRef(new Map<boolean, () => void>());
  const [companyFundamental, setCompanyFundamental] = useState<ApiRow | null>(null);
  const [fundamentalsRevision, setFundamentalsRevision] = useState(0);
  const [loadingDotIndex, setLoadingDotIndex] = useState(0);
  const partialRefreshTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const [sorts, setSorts] = useState<Record<Model, Sort>>({
    dcf: { key: 'weight', direction: 'desc' },
    egm: { key: 'weight', direction: 'desc' },
    eps: { key: 'weight', direction: 'desc' },
    ocf: { key: 'weight', direction: 'desc' },
  });
  const today = new Date().toISOString().slice(0, 10);

  const finishCertificateScopeMessage = useCallback((scope: boolean) => {
    certificateScopeMessages.current.get(scope)?.();
    certificateScopeMessages.current.delete(scope);
  }, []);
  const publishCertificateScope = useCallback((scope: boolean) => {
    const payload = dataByCertificateScope.current.get(scope);
    if (!payload || !weightsByCertificateScope.current.has(scope)) return false;
    if (selectedCertificateScope.current === scope) {
      setData(payload);
      setBookWeights(weightsByCertificateScope.current.get(scope) ?? null);
      setError(null);
    }
    finishCertificateScopeMessage(scope);
    return true;
  }, [finishCertificateScopeMessage]);

  useEffect(() => {
    if (data || error) return;
    const timer = window.setInterval(
      () => setLoadingDotIndex((index) => (index + 1) % LOADING_DOTS.length), 360,
    );
    return () => window.clearInterval(timer);
  }, [data, error]);

  useEffect(() => {
    const cached = dataByCertificateScope.current.get(lookThroughCertificates);
    if (cached) {
      if (!bookPortfolio) {
        setData(cached);
        setError(null);
      } else publishCertificateScope(lookThroughCertificates);
      return;
    }
    const controller = new AbortController();
    setError(null);
    void (async () => {
      try {
        const response = await apiFetch(`${API_URL}/api/earnings/portfolio-company-metrics`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(bookPortfolio
            ? { book_portfolio: bookPortfolio,
              look_through_certificates: lookThroughCertificates }
            : portfolioId != null
              ? { portfolio_id: portfolioId }
            : { holdings: basket?.holdings ?? [], basket_label: basket?.label ?? name }),
          signal: controller.signal,
        });
        const body = await response.json().catch(() => null) as Payload | { detail?: string } | null;
        if (!response.ok) throw new Error((body as { detail?: string } | null)?.detail ?? `HTTP ${response.status}`);
        const payload = body as Payload;
        dataByCertificateScope.current.set(lookThroughCertificates, payload);
        if (bookPortfolio) publishCertificateScope(lookThroughCertificates);
        else setData(payload);
      } catch (e) {
        if (!controller.signal.aborted) {
          finishCertificateScopeMessage(lookThroughCertificates);
          setError(e instanceof Error ? e.message : String(e));
        }
      }
    })();
    return () => controller.abort();
  }, [basket, bookPortfolio, finishCertificateScopeMessage, lookThroughCertificates, name,
    portfolioId, publishCertificateScope, fundamentalsRevision]);

  // This lightweight route uses the same AIRS source, complete-book denominator and linked-book
  // expansion as Analyse, without making Weight wait for returns, benchmarks and chart data.
  useEffect(() => {
    if (!bookPortfolio) {
      setBookWeights(null);
      return;
    }
    if (weightsByCertificateScope.current.has(lookThroughCertificates)) {
      publishCertificateScope(lookThroughCertificates);
      return;
    }
    const controller = new AbortController();
    void (async () => {
      try {
        const response = await apiFetch(
          `${API_URL}/api/airs/accounts/${encodeURIComponent(bookPortfolio)}/fundamental-weights${lookThroughCertificates ? '?look_through=true' : ''}`,
          { signal: controller.signal },
        );
        const body = await response.json().catch(() => null) as BookWeightsPayload | { detail?: string } | null;
        if (!response.ok) throw new Error((body as { detail?: string } | null)?.detail ?? `HTTP ${response.status}`);
        const payload = body as BookWeightsPayload;
        weightsByCertificateScope.current.set(lookThroughCertificates, payload);
        publishCertificateScope(lookThroughCertificates);
      } catch {
        if (!controller.signal.aborted) {
          weightsByCertificateScope.current.set(lookThroughCertificates, null);
          publishCertificateScope(lookThroughCertificates);
        }
      }
    })();
    return () => controller.abort();
  }, [bookPortfolio, lookThroughCertificates, publishCertificateScope]);

  // The fill is concurrent, so several companies can land within a few milliseconds. Coalesce
  // those stream events into one re-read, while still showing the first completed batch promptly.
  const reloadPartialFundamentals = useCallback(() => {
    if (partialRefreshTimer.current != null) return;
    partialRefreshTimer.current = setTimeout(() => {
      partialRefreshTimer.current = null;
      dataByCertificateScope.current.delete(lookThroughCertificates);
      setFundamentalsRevision((value) => value + 1);
    }, 300);
  }, [lookThroughCertificates]);
  const reloadFundamentals = useCallback(() => {
    dataByCertificateScope.current.delete(lookThroughCertificates);
    setFundamentalsRevision((value) => value + 1);
  }, [lookThroughCertificates]);
  useEffect(() => () => {
    if (partialRefreshTimer.current != null) clearTimeout(partialRefreshTimer.current);
    certificateScopeMessages.current.forEach((resolve) => resolve());
    certificateScopeMessages.current.clear();
  }, []);

  // The EGM tab owns these two assumptions. Until a reader sets them there, this portfolio view
  // deliberately leaves the columns and dependent valuation outputs empty.
  useEffect(() => {
    if (!data || typeof window === 'undefined') return;
    const next: Record<string, EgmOverrides> = {};
    for (const { isin } of data.rows) {
      try {
        const saved = JSON.parse(window.localStorage.getItem(egmStorageKey(isin)) ?? '{}') as EgmOverrides;
        const growthRate = saved.growthRate;
        const exitPE = saved.exitPE;
        if ((typeof growthRate === 'number' && Number.isFinite(growthRate))
          || (typeof exitPE === 'number' && Number.isFinite(exitPE))) {
          next[isin] = {
            ...(typeof growthRate === 'number' && Number.isFinite(growthRate) ? { growthRate } : {}),
            ...(typeof exitPE === 'number' && Number.isFinite(exitPE) ? { exitPE } : {}),
          };
        }
      } catch { /* malformed browser storage is equivalent to no assumptions */ }
    }
    setEgmOverrides(next);
  }, [data]);

  const updateEgmOverride = useCallback((isin: string, field: keyof EgmOverrides, raw: string) => {
    const number = raw.trim() === '' ? null : Number(raw);
    if (number != null && !Number.isFinite(number)) return;
    setEgmOverrides((current) => ({
      ...current,
      [isin]: { ...current[isin], ...(number == null ? { [field]: undefined } : { [field]: number }) },
    }));
    try {
      const stored = JSON.parse(window.localStorage.getItem(egmStorageKey(isin)) ?? '{}') as Record<string, unknown>;
      if (number == null) delete stored[field];
      else stored[field] = number;
      window.localStorage.setItem(egmStorageKey(isin), JSON.stringify(stored));
    } catch { /* browser storage being unavailable must not block editing */ }
  }, []);

  const rows = useMemo(() => {
    const holdings = bookWeights?.rows ?? [];
    const total = bookWeights?.total_current_value_eur ?? 0;
    const byIsin = new Map<string, { name: string; currentValue: number }>();
    for (const holding of holdings) {
      if (!holding.isin || holding.current_value_eur == null) continue;
      const present = byIsin.get(holding.isin);
      byIsin.set(holding.isin, {
        name: present?.name ?? holding.holding_name ?? holding.isin,
        currentValue: (present?.currentValue ?? 0) + holding.current_value_eur,
      });
    }
    return (data?.rows ?? []).map((row) => {
      const holding = byIsin.get(row.isin);
      const bookWeight = holding && total > 0 && bookWeights?.as_of_date
        ? {
          holding_name: holding.name,
          current_value_eur: holding.currentValue,
          total_current_value_eur: total,
          as_of_date: bookWeights.as_of_date,
          fetched_at: bookWeights.fetched_at,
        }
        : null;
      return dcfRow({ ...row, book_weight: bookWeight }, today, egmOverrides[row.isin]);
    });
  }, [bookWeights, data, egmOverrides, today]);
  const refreshScope = useMemo<RefreshScope | null>(() => {
    const isins = [...new Set((data?.rows ?? []).map((row) => row.isin).filter(Boolean))];
    if (!isins.length) return null;
    // Refresh the exact company universe this table resolved, including companies reached through
    // certificate/TopSelectie look-through. A portfolio-id refresh stops at wrapper instruments,
    // so it can be narrower than the rows visible here.
    return {
      kind: 'basket',
      holdings: isins.map((isin) => ({ isin })),
      name,
    };
  }, [data, name]);
  const activeSort = sorts[model];
  const activeControlClass = {
    dcf: 'border-accent-500 bg-accent-600 text-white shadow-sm hover:bg-accent-500',
    egm: 'border-pos-500 bg-pos-500 text-white shadow-sm hover:bg-pos-400',
    eps: 'border-warn-500 bg-warn-500 text-white shadow-sm hover:bg-warn-400',
    ocf: 'border-sky-500 bg-sky-600 text-white shadow-sm hover:bg-sky-500',
  }[model];
  const supportingControlClass = {
    dcf: 'border-accent-500/60 text-accent-300 hover:bg-accent-500/10',
    egm: 'border-pos-500/60 text-pos-300 hover:bg-pos-500/10',
    eps: 'border-warn-500/60 text-warn-300 hover:bg-warn-500/10',
    ocf: 'border-sky-500/60 text-sky-300 hover:bg-sky-500/10',
  }[model];
  const checkboxAccentClass = {
    dcf: 'accent-accent-600',
    egm: 'accent-pos-500',
    eps: 'accent-warn-500',
    ocf: 'accent-sky-500',
  }[model];
  const headerControlClass = 'inline-flex h-9 shrink-0 items-center justify-center whitespace-nowrap rounded-md border px-4 text-xs font-medium shadow-sm transition-colors';
  const sortedRows = useMemo(() => [...rows].sort((a, b) => {
    const av = sortValue(a, activeSort.key);
    const bv = sortValue(b, activeSort.key);
    // Missing is not zero and never outranks a real valuation, whichever direction is active.
    if (av == null) return bv == null ? 0 : 1;
    if (bv == null) return -1;
    return activeSort.direction === 'asc' ? av - bv : bv - av;
  }), [activeSort, rows]);
  const toggleSort = (key: string) => setSorts((current) => ({
    ...current,
    [model]: current[model].key === key
      ? { key, direction: current[model].direction === 'desc' ? 'asc' : 'desc' }
      : { key, direction: 'desc' },
  }));
  const NumericHeader = ({ sortKey, label, title, info, className = '' }: {
    sortKey: string; label: string; title?: string; info?: string; className?: string;
  }) => {
    const active = activeSort.key === sortKey;
    return (
      <th className={`px-3 py-2 text-right font-medium ${className}`} title={title}>
        <span className="ml-auto flex w-fit items-center gap-1">
          <button type="button" onClick={() => toggleSort(sortKey)}
            aria-label={`Sort by ${label} ${active && activeSort.direction === 'desc' ? 'ascending' : 'descending'}`}
            className={`inline-flex cursor-pointer items-center gap-1 whitespace-nowrap hover:text-fg-strong ${active
              ? model === 'dcf' ? 'text-accent-300'
                : model === 'egm' ? 'text-pos-300'
                  : model === 'eps' ? 'text-warn-400' : 'text-sky-400' : ''}`}>
            <span>{label}</span>
            <span aria-hidden className="w-2 text-center text-[9px]">
              {active ? (activeSort.direction === 'desc' ? '▼' : '▲') : '↕'}
            </span>
          </button>
          {info && <InfoTip text={info} className="normal-case tracking-normal text-fg-faint" />}
        </span>
      </th>
    );
  };
  return (
    <PanelDialog onClose={onClose} labelledBy="portfolio-fundamental-title">
      <div className="flex h-full min-h-0 flex-col rounded-xl border border-neutral-800/40 bg-card shadow-xl">
        <div className="flex shrink-0 items-center gap-4 border-b border-neutral-800/40 px-5 py-4">
          <div className="min-w-0">
            <h3 id="portfolio-fundamental-title" className="text-lg font-semibold text-fg-strong">
              Fundamental
            </h3>
            <p className="truncate text-sm text-fg-muted">
              {name} · {data ? `${rows.length} ${rows.length === 1 ? 'company' : 'companies'}` : 'companies'}
            </p>
          </div>
          <div className="ml-auto inline-flex shrink-0 gap-1"
            role="group" aria-label="Valuation model">
            <button type="button" onClick={() => setModel('dcf')} aria-pressed={model === 'dcf'}
              className={`${headerControlClass} ${model === 'dcf'
                ? activeControlClass : 'border-neutral-700 bg-page text-fg-muted hover:bg-overlay/5 hover:text-fg-strong'}`}>
              Reverse DCF
            </button>
            <button type="button" onClick={() => setModel('egm')} aria-pressed={model === 'egm'}
              className={`${headerControlClass} ${model === 'egm'
                ? activeControlClass : 'border-neutral-700 bg-page text-fg-muted hover:bg-overlay/5 hover:text-fg-strong'}`}>
              Expected Growth Model
            </button>
            <button type="button" onClick={() => setModel('eps')} aria-pressed={model === 'eps'}
              className={`${headerControlClass} ${model === 'eps'
                ? activeControlClass : 'border-neutral-700 bg-page text-fg-muted hover:bg-overlay/5 hover:text-fg-strong'}`}>
              EPS Estimates
            </button>
            <button type="button" onClick={() => setModel('ocf')} aria-pressed={model === 'ocf'}
              className={`${headerControlClass} ${model === 'ocf'
                ? activeControlClass : 'border-neutral-700 bg-page text-fg-muted hover:bg-overlay/5 hover:text-fg-strong'}`}>
              OCF Estimates
            </button>
          </div>
          {bookPortfolio && (
            <label
              title="Replace linked certificates with the companies held by their underlying strategies."
              className={`${headerControlClass} cursor-pointer gap-1.5 ${supportingControlClass} ${lookThroughCertificates ? 'bg-overlay/10' : 'bg-page'}`}>
              <input type="checkbox" checked={lookThroughCertificates}
                onChange={(event) => {
                  const checked = event.target.checked;
                  // End any superseded message immediately; its requests are aborted by the
                  // effect cleanup. The selected scope is published only after BOTH its company
                  // metrics and AIRS weights have landed, so the current table cannot become a
                  // hybrid of old rows and new weights while this message is visible.
                  certificateScopeMessages.current.forEach((resolve) => resolve());
                  certificateScopeMessages.current.clear();
                  selectedCertificateScope.current = checked;
                  setLookThroughCertificates(checked);
                  if (!publishCertificateScope(checked)) {
                    const pending = new Promise<void>((resolve) => {
                      certificateScopeMessages.current.set(checked, resolve);
                    });
                    void track('Updating certificate view…', pending);
                  }
                  setError(null);
                }}
                className={checkboxAccentClass} />
              Look through certificates
            </label>
          )}
          {refreshScope && (
            <PortfolioFundamentalsRefresh scope={refreshScope} everything allPeriods prominent
              prominentTone={model === 'dcf' ? 'accent' : model === 'egm'
                ? 'positive' : model === 'eps' ? 'warning' : 'info'} showNote={false}
              label="Refresh all companies"
              jobTitle={`${name}: ${MODEL_LABEL[model]}`}
              onProgress={reloadPartialFundamentals}
              onDone={reloadFundamentals} />
          )}
          {!refreshScope && (
            // Keep the header geometry fixed while an uncached certificate scope resolves. The
            // previous button used to disappear here, pulling Close and every control beside it
            // sideways for the duration of the request.
            <button type="button" disabled aria-busy={!data}
              title={!data ? 'The company list is loading.' : 'There are no companies to refresh.'}
              className={`${headerControlClass} ${activeControlClass} opacity-50 ${!data ? 'cursor-wait' : 'cursor-not-allowed'}`}>
              Refresh all companies
            </button>
          )}
          <button type="button" onClick={onClose}
            className={`${headerControlClass} ${activeControlClass}`}>
            Close
          </button>
        </div>

        <div className="min-h-0 flex-1 overflow-hidden px-6">
          <div className="h-full overflow-hidden">
          {!data && !error && (
            <div className="flex min-h-64 items-center justify-center">
              <p className="text-sm text-fg-muted">
                Loading company fundamentals{LOADING_DOTS[loadingDotIndex]}
              </p>
            </div>
          )}
          {error && <p className="m-5 rounded-lg border border-neg-500/30 bg-neg-500/10 p-3 text-sm text-neg-300">{error}</p>}
          {data && rows.length === 0 && (
            <p className="p-5 text-sm text-fg-muted">No operating companies with stored fundamentals were found.</p>
          )}
          {rows.length > 0 && (
            // This frame, rather than an ancestor outside the table, owns both scroll axes. Its
            // border therefore never travels away from the sticky header: frame, header and body
            // remain one object while only the rows/columns inside it move.
            <div className="my-4 h-[calc(100%-2rem)] overflow-auto border border-neutral-800/50">
            <table className="isolate min-w-max w-full border-separate border-spacing-0 text-xs">
              {/* The model-colour cells use translucent tints. Give the sticky header group its
                  own opaque surface so scrolled body rows cannot show through those tints. */}
              <thead className="sticky top-0 z-30 bg-page text-xs uppercase tracking-wide text-fg-faint">
                <tr className="border-b border-neutral-800/40">
                  {/* These identifying columns are the subject, not the selected valuation model. They
                      stay pinned and unchanged while the switch replaces only the coloured block
                      to their right. Fixed widths make the sticky offsets exact. */}
                  <th className="sticky left-0 z-40 w-12 min-w-12 max-w-12 bg-page px-2 py-2 text-right font-medium"
                    aria-label="Row number">
                    #
                  </th>
                  <th className="sticky left-12 z-40 w-80 min-w-80 max-w-80 bg-page px-3 py-2 text-left font-medium">
                    Company
                  </th>
                  <NumericHeader sortKey="weight" label="Weight"
                    info="Each row's current share of the paired AIRS book, calculated from VOLK Huidige waarde."
                    className="sticky left-[23rem] z-40 w-24 min-w-24 max-w-24 bg-page" />
                  <NumericHeader sortKey="price" label="Stock price"
                    info="The latest stored GuruFocus closing price. The currency code is the company's GuruFocus exchange currency; the value is not converted to EUR."
                    className="sticky left-[29rem] z-40 w-36 min-w-36 max-w-36 border-r-2 border-neutral-700 bg-page shadow-[6px_0_8px_-6px_var(--color-neutral-700)]" />
                  {model === 'dcf' ? RATES.map((rate) => (
                    <NumericHeader key={rate} sortKey={`rate:${rate}`}
                      label={`${(rate * 100).toFixed(0)}%`} className="bg-accent-500/10" />
                  )) : model === 'egm' ? (
                    <>
                      <NumericHeader sortKey="eps" label="FY1 EPS" className="bg-pos-500/10" />
                      <NumericHeader sortKey="forwardPE" label="Forward P/E" className="bg-pos-500/10" />
                      <NumericHeader sortKey="epsGrowth" label="EPS growth" className="bg-pos-500/10" />
                      <NumericHeader sortKey="dividend" label="Dividend" className="bg-pos-500/10" />
                      <NumericHeader sortKey="exitPE" label="Exit P/E" className="bg-pos-500/10" />
                      <NumericHeader sortKey="fairValue" label="Fair value" className="bg-pos-500/10" />
                      <NumericHeader sortKey="upside" label="Upside" className="bg-pos-500/10" />
                      <NumericHeader sortKey="expectedReturn" label="Expected annual return" className="bg-pos-500/10" />
                      <NumericHeader sortKey="impliedPrice" label="Price in 5y" className="bg-pos-500/10" />
                      <NumericHeader sortKey="totalReturn" label="Total return" className="bg-pos-500/10" />
                    </>
                  ) : model === 'eps' ? (
                    <>
                      {EPS_YEARS.map((year) => (
                        <NumericHeader key={year} sortKey={`epsYear:${year}`}
                          label={`EPS FY${year}`} className="bg-warn-500/10" />
                      ))}
                      <NumericHeader sortKey="epsEstimateCagr" label="EPS CAGR FY2025–FY2027"
                        className="bg-warn-500/10" />
                      <NumericHeader sortKey="epsHistoricalPe10y" label="10y Historical P/E"
                        className="bg-warn-500/10" />
                      <NumericHeader sortKey="epsPe:2025" label="P/E FY2025"
                        className="bg-warn-500/10" />
                      <NumericHeader sortKey="epsPe:2026" label="P/E FY2026"
                        className="bg-warn-500/10" />
                      <NumericHeader sortKey="epsPe:2027" label="P/E FY2027"
                        className="bg-warn-500/10" />
                    </>
                  ) : (
                    <>
                      {EPS_YEARS.map((year) => (
                        <NumericHeader key={year} sortKey={`ocfYear:${year}`}
                          label={`OCF/share FY${year}`} className="bg-sky-500/10" />
                      ))}
                      <NumericHeader sortKey="ocfEstimateCagr" label="OCF/share CAGR FY2025–FY2027"
                        className="bg-sky-500/10" />
                      <NumericHeader sortKey="ocfHistoricalMultiple10y" label="10y Historical P/OCF"
                        className="bg-sky-500/10" />
                      {EPS_YEARS.map((year) => (
                        <NumericHeader key={`ocfMultiple:${year}`} sortKey={`ocfMultiple:${year}`}
                          label={`P/OCF FY${year}`} className="bg-sky-500/10" />
                      ))}
                    </>
                  )}
                </tr>
              </thead>
              <tbody className="divide-y divide-neutral-800/20">
                {sortedRows.map((row, rowIndex) => (
                  <tr key={row.company_id} className="hover:bg-overlay/[0.03]">
                    <td className="sticky left-0 z-20 w-12 min-w-12 max-w-12 border-l border-neutral-800/50 bg-page px-2 py-2 text-right font-mono tabular-nums text-fg-faint">
                      {rowIndex + 1}
                    </td>
                    <td className="sticky left-12 z-20 w-80 min-w-80 max-w-80 bg-page px-3 py-2">
                      <div className="flex items-center gap-2">
                        <div className="min-w-0 flex-1">
                          <div className="truncate font-medium text-fg-strong" title={row.name}>{row.name}</div>
                          <div className="font-mono text-[10px] text-fg-faint">{row.isin}</div>
                        </div>
                        <span className="flex shrink-0 items-center gap-1.5">
                          <button type="button" onClick={() => setCompanyFundamental(row)}
                            title={`Open the full Fundamental view for ${row.name}`}
                            className="shrink-0 rounded-md border border-neutral-700 px-2 py-1 text-[10px] font-medium text-fg-muted transition-colors hover:border-accent-500/50 hover:bg-overlay/5 hover:text-accent-300">
                            Fundamental
                          </button>
                          <PortfolioFundamentalsRefresh
                            scope={{ kind: 'company', isin: row.isin, name: row.name }}
                            feeds={COMPANY_REFRESH[model].feeds}
                            prices={COMPANY_REFRESH[model].prices}
                            keyRatios={COMPANY_REFRESH[model].keyRatios}
                            allPeriods compact broadcast={false} showNote={false}
                            label="Refresh"
                            jobTitle={`${row.name}: ${MODEL_LABEL[model]}`}
                            onProgress={reloadPartialFundamentals}
                            onDone={reloadFundamentals} />
                        </span>
                      </div>
                    </td>
                    <td className="sticky left-[23rem] z-20 w-24 min-w-24 max-w-24 bg-page px-3 py-2 text-right font-mono tabular-nums">
                      <span className="flex items-center justify-end gap-1.5 whitespace-nowrap">
                        <span>{row.book_weight ? `${bookWeightPct(row.book_weight).toFixed(2)}%`
                          : bookPortfolio != null ? bookWeights === undefined ? '…' : '—'
                            : `${row.weight_pct.toFixed(2)}%`}</span>
                        {row.book_weight
                          ? <Provenance source="airs_volk" asOf={row.book_weight.as_of_date}
                            fetchedAt={row.book_weight.fetched_at} kind="formula"
                            what={ANALYSE_COPY.en.row.weightWhat(row.book_weight.holding_name)}
                            note={ANALYSE_COPY.en.row.weightNote}
                            how={ANALYSE_COPY.en.row.weightHow(
                              eurWhole.format(row.book_weight.current_value_eur),
                              eurWhole.format(row.book_weight.total_current_value_eur),
                              `${bookWeightPct(row.book_weight).toFixed(2)}%`,
                            )} />
                          : bookPortfolio != null ? <InfoTip wide className="font-sans text-fg-faint" content={<AspectCard
                            what={bookWeights === undefined ? 'AIRS book weight is loading.' : 'No AIRS book value is available for this company.'}
                            where="AIRS Vermogensoverzicht (VOLK)."
                            when={bookWeights === undefined ? 'Loading the current AIRS book values.' : 'No matching current AIRS book holding was returned.'}
                            how="A model-composition weight is not substituted for an AIRS book weight." />} />
                          : <InfoTip wide className="font-sans text-fg-faint" content={<AspectCard
                            what={`${row.name}'s share of the whole current portfolio.`}
                            where={`${name}'s current composition after linked certificates and TopSelecties are looked through.`}
                            when="The portfolio composition loaded for this Fundamental view."
                            how={`This company's portfolio weight is ${row.weight_pct.toFixed(2)}%; funds, cash, bonds and uncovered companies are not redistributed over the visible company rows.`} />} />}
                      </span>
                    </td>
                    <td className="sticky left-[29rem] z-20 w-36 min-w-36 max-w-36 border-r-2 border-neutral-700 bg-page px-3 py-2 font-mono tabular-nums shadow-[6px_0_8px_-6px_var(--color-neutral-700)]">
                      <span className="grid grid-cols-[2.25rem_1fr_auto] items-baseline gap-1.5">
                        <span className="text-left text-[10px] text-fg-faint">{row.currency ?? ''}</span>
                        <span className="text-right">{row.src.price == null ? '—' : inputNumber.format(row.src.price)}</span>
                        <InfoTip wide className="font-sans text-fg-faint" content={<AspectCard
                          what={`The latest stored closing price for ${row.name}.`}
                          where="GuruFocus closing-price history."
                          when={<InputObservationRows inputs={row.stockPriceInputs} />}
                          how="Select the newest stored close; the currency shown in the cell is the company's GuruFocus exchange currency." />} />
                      </span>
                    </td>
                    {model === 'dcf' ? row.growth.map((cell) => (
                      <ValuationCell key={cell.discountRate} tone="dcf"
                        value={dcfGrowthCellLabel(
                          cell.impliedGrowth, row.dcfStartingFcf, marketCapOf(row.src))}
                        what={row.dcfStartingFcf != null && row.dcfStartingFcf <= 0
                          ? `No growth rate can be solved at ${(cell.discountRate * 100).toFixed(0)}% because normalised starting FCF is zero or negative.`
                          : `Annual FCF growth implied by the share price at a ${(cell.discountRate * 100).toFixed(0)}% discount rate.`}
                        where={`GuruFocus close price, diluted shares and ${row.dcfForward ? 'FY1 consensus' : 'latest reported'} cash-flow inputs stored for ${row.name}.`}
                        retrieved={[row.source_fetched_at.financials,
                          row.dcfForward ? row.source_fetched_at.estimates : null]}
                        applies={[row.src.priceDate, row.src.sharesDate, row.src.flowBasis.date,
                          row.dcfForward ? row.src.ocfEstimateDate : null]}
                        inputs={row.dcfInputs}
                        how={row.dcfStartingFcf != null && row.dcfStartingFcf <= 0
                          ? `Base FCF is ${compactMillions(row.dcfStartingFcf, row.currency ? ` ${row.currency}` : '')}. Non-positive FCF has no growth solution.`
                          : `Discount FCF and terminal value at ${(cell.discountRate * 100).toFixed(0)}%, then solve growth against market value.`} />
                    )) : model === 'egm' ? (
                      <>
                        <ValuationCell tone="egm"
                          value={row.egm.epsNextFY == null ? '—' : inputNumber.format(row.egm.epsNextFY)}
                          what="Consensus earnings per share for the next fiscal year."
                          where={`GuruFocus analyst estimates for ${row.name}.`}
                          retrieved={[row.source_fetched_at.estimates]}
                          applies={[row.egm.epsNextFYDate]}
                          how="Select the earliest positive-period EPS estimate whose fiscal period ends after today." />
                        <ValuationCell tone="egm"
                          value={row.forwardPE == null ? '—' : `${inputNumber.format(row.forwardPE)}×`}
                          what="The forward price-to-earnings multiple used as the model's starting valuation."
                          where={row.forwardPeDerived
                            ? 'Latest stored GuruFocus close price divided by FY1 consensus EPS.'
                            : 'GuruFocus Forward PE Ratio indicator.'}
                          retrieved={row.forwardPeDerived
                            ? [row.source_fetched_at.estimates]
                            : [row.source_fetched_at.indicators]}
                          applies={row.forwardPeDerived
                            ? [row.egm.priceDate, row.egm.epsNextFYDate]
                            : [row.egm.forwardPEDate]}
                          how={row.forwardPeDerived
                            ? 'Divide the current stored close by the next-fiscal-year EPS estimate.'
                            : 'Use the newest positive Forward PE Ratio observation supplied by GuruFocus.'} />
                        <ValuationCell tone="egm"
                          value={<input type="number" step="0.1"
                            aria-label={`EPS growth for ${row.name}`}
                            value={row.egmAssumptions.growthRate == null ? '' : row.egmAssumptions.growthRate * 100}
                            placeholder="—"
                            onChange={(event) => updateEgmOverride(row.isin, 'growthRate', event.target.value === ''
                              ? '' : String(Number(event.target.value) / 100))}
                            className="w-16 rounded border border-neutral-700 bg-page px-1.5 py-0.5 text-right text-[12px] text-fg-strong focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30" />}
                          what="The annual EPS growth rate. Enter it here or in the EGM tab."
                          where="This assumption is saved per company and shared with the EGM tab."
                          retrieved={[]}
                          applies={['Every forecast year']}
                          how="This portfolio view uses only the EPS growth rate entered in the EGM tab." />
                        <ValuationCell tone="egm"
                          value={`${inputNumber.format((row.egmAssumptions.dividendYield ?? 0) * 100)}%`}
                          what="The annual dividend yield carried through the model."
                          where="The newest GuruFocus Dividend Yield % observation; an unavailable yield is treated as 0%."
                          retrieved={row.egm.dividendYield != null
                            ? [row.source_fetched_at.financials] : []}
                          applies={row.egm.dividendYield != null
                            ? [row.egm.dividendYieldDate] : []}
                          how="Convert the vendor percentage to a decimal and hold that yield constant for the projection." />
                        <ValuationCell tone="egm"
                          value={<input type="number" step="0.1"
                            aria-label={`Exit P/E for ${row.name}`}
                            value={row.egmAssumptions.exitPE == null ? '' : row.egmAssumptions.exitPE}
                            placeholder="—"
                            onChange={(event) => updateEgmOverride(row.isin, 'exitPE', event.target.value)}
                            className="w-16 rounded border border-neutral-700 bg-page px-1.5 py-0.5 text-right text-[12px] text-fg-strong focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30" />}
                          what="The P/E multiple assumed at the end of year five. Enter it here or in the EGM tab."
                          where="This assumption is saved per company and shared with the EGM tab."
                          retrieved={[]}
                          applies={['End of year five']}
                          how="This portfolio view uses only the exit P/E entered in the EGM tab." />
                        <ValuationCell tone="egm" emphasis
                          value={row.egmResult.fairValue == null ? '—' : inputNumber.format(row.egmResult.fairValue)}
                          what="The highest price today that still meets the model's 10% annual return hurdle."
                          where="FY1 consensus EPS, expected EPS growth, dividend yield and the historical or default exit P/E."
                          retrieved={[row.source_fetched_at.estimates, row.source_fetched_at.financials]}
                          applies={[row.egm.epsNextFYDate, row.egm.dividendYieldDate,
                            ...row.estimateDates, ...row.medianPeDates]}
                          inputs={row.fairValueInputs}
                          how="Multiply FY1 EPS by the maximum starting P/E compatible with ten years of EPS growth, dividends, the exit multiple and the 10% hurdle rate."
                          worked={workedFairValue(
                            row.egm.epsNextFY,
                            row.egmResult.maxPE,
                            row.egmResult.fairValue,
                          )}
                          legend={row.egmResult.fairValue == null ? undefined : [
                            { sym: String.raw`EPS_{\text{FY1}}`, is: 'the FY1 consensus EPS' },
                            { sym: String.raw`PE_{\max}`, is: 'the maximum starting P/E that meets the hurdle' },
                          ]} />
                        <ValuationCell tone="egm"
                          value={row.egmResult.upside == null ? '—' : `${row.egmResult.upside >= 0 ? '+' : ''}${(row.egmResult.upside * 100).toFixed(2)}%`}
                          what="The difference between model fair value and the latest stored share price."
                          where="The Expected Growth Model fair value and GuruFocus close price."
                          retrieved={[row.source_fetched_at.estimates, row.source_fetched_at.financials]}
                          applies={[row.egm.priceDate, row.egm.epsNextFYDate,
                            row.egm.dividendYieldDate, ...row.estimateDates, ...row.medianPeDates]}
                          inputs={row.upsideInputs}
                          how="Divide fair value by the latest stored share price and subtract one."
                          worked={workedFairValueGap(
                            row.egmResult.fairValue,
                            row.egm.price,
                            row.egmResult.upside == null
                              ? ''
                              : `${row.egmResult.upside >= 0 ? '+' : ''}${(row.egmResult.upside * 100).toFixed(2)}%`,
                          )}
                          legend={row.egmResult.upside == null ? undefined : [
                            { sym: 'FV', is: 'the Expected Growth Model fair value today' },
                            { sym: 'P_0', is: 'the latest stored share price' },
                          ]} />
                        <ValuationCell tone="egm" emphasis
                          value={row.egmResult.expectedReturn == null ? '—' : `${row.egmResult.expectedReturn >= 0 ? '+' : ''}${(row.egmResult.expectedReturn * 100).toFixed(2)}%`}
                          what="The modelled annualised shareholder return over ten years."
                          where="Current forward P/E, expected EPS growth, dividend yield and the historical or default exit P/E."
                          retrieved={[row.source_fetched_at.estimates, row.source_fetched_at.financials,
                            row.forwardPeDerived ? null : row.source_fetched_at.indicators]}
                          applies={[row.egm.priceDate, row.egm.epsNextFYDate, row.egm.forwardPEDate,
                            row.egm.dividendYieldDate, ...row.estimateDates, ...row.medianPeDates]}
                          inputs={row.expectedReturnInputs}
                          how="The three annual factors multiply; the change from the current to the exit P/E is spread across the ten-year horizon."
                          worked={row.egmResult.bridge == null ? '' : workedEgmReturn(
                            row.egmResult.bridge,
                            row.egmAssumptions.years,
                            `${row.egmResult.expectedReturn != null && row.egmResult.expectedReturn >= 0 ? '+' : ''}${((row.egmResult.expectedReturn ?? 0) * 100).toFixed(2)}%`,
                          )}
                          legend={row.egmResult.bridge == null ? undefined : [
                            { sym: 'g', is: 'the annual EPS growth rate' },
                            { sym: 'y', is: 'the annual dividend yield' },
                            { sym: String.raw`PE_{\text{exit}}`, is: 'the assumed P/E after ten years' },
                            { sym: String.raw`PE_{\text{fwd}}`, is: 'the current forward P/E' },
                            { sym: 'n', is: `the ${row.egmAssumptions.years}-year forecast horizon` },
                          ]} />
                        <ValuationCell tone="egm"
                          value={row.egmResult.impliedPrice == null ? '—' : inputNumber.format(row.egmResult.impliedPrice)}
                          what="The share price implied at the end of year ten."
                          where="Current price and forward P/E, expected EPS growth and the historical or default exit P/E."
                          retrieved={[row.source_fetched_at.estimates, row.source_fetched_at.financials,
                            row.forwardPeDerived ? null : row.source_fetched_at.indicators]}
                          applies={[row.egm.priceDate, row.egm.epsNextFYDate, row.egm.forwardPEDate,
                            ...row.estimateDates, ...row.medianPeDates]}
                          inputs={row.expectedReturnInputs.filter((input) => input.label !== 'Dividend yield')}
                          how="Grow earnings for five years and revalue them from today's forward P/E to the exit P/E; dividends are not part of this price."
                          worked={row.egmResult.bridge == null ? '' : workedImpliedPrice(
                            row.egm.price, row.egmResult.bridge, row.egmAssumptions.years,
                            row.egmResult.impliedPrice,
                          )}
                          legend={row.egmResult.bridge == null ? undefined : [
                            { sym: 'P_0', is: 'the current share price' },
                            { sym: 'g', is: 'the annual EPS growth rate' },
                            { sym: 'n', is: `the ${row.egmAssumptions.years}-year forecast horizon` },
                            { sym: String.raw`PE_{\text{exit}}`, is: 'the assumed P/E after ten years' },
                            { sym: String.raw`PE_{\text{fwd}}`, is: 'the current forward P/E' },
                          ]} />
                        <ValuationCell tone="egm"
                          value={row.egmResult.totalReturn == null ? '—' : `${row.egmResult.totalReturn >= 0 ? '+' : ''}${(row.egmResult.totalReturn * 100).toFixed(2)}%`}
                          what="The cumulative shareholder return modelled over ten years, including dividends."
                          where="The Expected Growth Model's annualised return and ten-year horizon."
                          retrieved={[row.source_fetched_at.estimates, row.source_fetched_at.financials,
                            row.forwardPeDerived ? null : row.source_fetched_at.indicators]}
                          applies={[row.egm.priceDate, row.egm.epsNextFYDate, row.egm.forwardPEDate,
                            row.egm.dividendYieldDate, ...row.estimateDates, ...row.medianPeDates]}
                          inputs={row.expectedReturnInputs}
                          how="Compound the expected annual shareholder return for ten years."
                          worked={workedEgmTotalReturn(
                            row.egmResult.expectedReturn,
                            row.egmAssumptions.years,
                            `${row.egmResult.totalReturn != null && row.egmResult.totalReturn >= 0 ? '+' : ''}${((row.egmResult.totalReturn ?? 0) * 100).toFixed(2)}%`,
                          )}
                          legend={row.egmResult.expectedReturn == null ? undefined : [
                            { sym: 'R', is: 'the expected annual shareholder return' },
                            { sym: 'n', is: `the ${row.egmAssumptions.years}-year forecast horizon` },
                          ]} />
                      </>
                    ) : model === 'eps' ? (
                      <>
                        {EPS_YEARS.map((year) => {
                          const observation = row.epsObservations[year];
                          const metric = observation?.metric ?? null;
                          const kind = observation?.kind ?? null;
                          return (
                            <ValuationCell key={year} tone="eps"
                              value={metric?.numeric_value == null ? '—' : inputNumber.format(metric.numeric_value)}
                              what={epsYearWhat(row.name, year, kind)}
                              where={`GuruFocus annual financial statements and analyst estimates stored for ${row.name}.`}
                              retrieved={[metric?.recorded_at]}
                              applies={[metric?.target_date]}
                              inputs={epsYearInputs(observation, year, row.currency,
                                row.source_fetched_at.financials, row.source_fetched_at.estimates)}
                              how={epsYearHow(year, kind, metric?.target_date ?? null)} />
                          );
                        })}
                        <ValuationCell tone="eps" emphasis
                          value={row.epsEstimateCagr == null
                            ? '—' : `${inputNumber.format(row.epsEstimateCagr * 100)}%`}
                          what="The annualised change from the selected FY2025 EPS to the selected FY2027 EPS."
                          where={`GuruFocus financial statements and analyst estimates stored for ${row.name}.`}
                          retrieved={[row.source_fetched_at.financials,
                            row.source_fetched_at.estimates]}
                          applies={[row.epsObservations[2025]?.metric.target_date,
                            row.epsObservations[2027]?.metric.target_date]}
                          inputs={[
                            ...epsYearInputs(row.epsObservations[2025], 2025, row.currency,
                              row.source_fetched_at.financials, row.source_fetched_at.estimates),
                            ...epsYearInputs(row.epsObservations[2027], 2027, row.currency,
                              row.source_fetched_at.financials, row.source_fetched_at.estimates),
                          ]}
                          how="For each endpoint, prefer reported EPS without NRI and otherwise use consensus EPS. Compound the change between the two positive selected values over two years." />
                        <ValuationCell tone="eps"
                          value={row.historicalPe10yWorking.median == null
                            ? '—' : `${inputNumber.format(row.historicalPe10yWorking.median)}×`}
                          what="The median P/E across the latest ten completed fiscal years."
                          where="GuruFocus fiscal year-end prices and EPS without NRI."
                          retrieved={[row.source_fetched_at.financials]}
                          applies={row.historicalPe10yWorking.rows.map((point) => `${point.year}-12-31`)}
                          inputs={row.historicalPe10yObservations}
                          how={row.historicalPe10yWorked} />
                        {([2025, 2026, 2027] as const).map((year) => {
                          const observation = row.epsObservations[year];
                          const epsMetric = observation?.metric ?? null;
                          const multiple = row.peByYear[year];
                          const period = `FY${year} ${observation?.kind ?? 'EPS'}`;
                          return (
                            <ValuationCell key={`pe:${year}`} tone="eps"
                              value={multiple == null ? '—' : `${inputNumber.format(multiple)}×`}
                              what={`The latest stock price expressed as a multiple of the ${period} EPS.`}
                              where={`GuruFocus close price, annual statements and analyst estimates stored for ${row.name}.`}
                              retrieved={[epsMetric?.recorded_at]}
                              applies={[row.src.priceDate, epsMetric?.target_date]}
                              inputs={[
                                ...row.stockPriceInputs,
                                ...epsYearInputs(observation, year, row.currency,
                                  row.source_fetched_at.financials, row.source_fetched_at.estimates),
                              ]}
                              how={`Divide the latest stored close price by positive ${period} EPS.`} />
                          );
                        })}
                      </>
                    ) : (
                      <>
                        {EPS_YEARS.map((year) => {
                          const observation = row.ocfObservations[year];
                          const metric = observation?.metric ?? null;
                          const kind = observation?.kind ?? null;
                          return (
                            <ValuationCell key={year} tone="ocf"
                              value={row.ocfPerShareByYear[year] == null
                                ? '—' : inputNumber.format(row.ocfPerShareByYear[year]!)}
                              what={ocfPerShareWhat(row.name, year, kind)}
                              where={`GuruFocus annual cash-flow statements and analyst estimates stored for ${row.name}.`}
                              retrieved={[metric?.recorded_at]}
                              applies={[metric?.target_date]}
                              inputs={ocfYearInputs(observation, year, row.currency,
                                row.source_fetched_at.financials, row.source_fetched_at.estimates)
                                .concat(row.shareCountInputs)}
                              how={ocfPerShareHow(year, kind)} />
                          );
                        })}
                        <ValuationCell tone="ocf" emphasis
                          value={row.ocfEstimateCagr == null
                            ? '—' : `${inputNumber.format(row.ocfEstimateCagr * 100)}%`}
                          what="The annualised change from selected FY2025 OCF per diluted share to selected FY2027 OCF per diluted share."
                          where={`GuruFocus cash-flow statements and analyst estimates stored for ${row.name}.`}
                          retrieved={[row.source_fetched_at.financials,
                            row.source_fetched_at.estimates]}
                          applies={[row.ocfObservations[2025]?.metric.target_date,
                            row.ocfObservations[2027]?.metric.target_date]}
                          inputs={[
                            ...ocfYearInputs(row.ocfObservations[2025], 2025, row.currency,
                              row.source_fetched_at.financials, row.source_fetched_at.estimates),
                            ...ocfYearInputs(row.ocfObservations[2027], 2027, row.currency,
                              row.source_fetched_at.financials, row.source_fetched_at.estimates),
                            ...row.shareCountInputs,
                          ]}
                          how="Divide each selected OCF by current diluted shares, then compound the change between the two positive per-share values over two years." />
                        <ValuationCell tone="ocf"
                          value={row.historicalOcfWorking.median == null
                            ? '—' : `${inputNumber.format(row.historicalOcfWorking.median)}×`}
                          what="The median price-to-operating-cash-flow-per-share multiple across the latest ten completed fiscal years."
                          where="GuruFocus fiscal year-end prices, diluted shares and reported operating cash flow."
                          retrieved={[row.source_fetched_at.financials]}
                          applies={row.historicalOcfWorking.rows.map((point) => `${point.year}-12-31`)}
                          inputs={row.historicalOcfObservations}
                          how="Divide each year-end share price by that year's positive OCF per diluted share, then take the median." />
                        {EPS_YEARS.map((year) => {
                          const observation = row.ocfObservations[year];
                          const metric = observation?.metric ?? null;
                          const multiple = row.pOcfByYear[year];
                          const period = `FY${year} ${observation?.kind ?? 'OCF'}`;
                          return (
                            <ValuationCell key={`pOcf:${year}`} tone="ocf"
                              value={multiple == null ? '—' : `${inputNumber.format(multiple)}×`}
                              what={`The latest stock price expressed as a multiple of ${period} per diluted share.`}
                              where={`GuruFocus close price, diluted shares, statements and estimates stored for ${row.name}.`}
                              retrieved={[metric?.recorded_at]}
                              applies={[row.src.priceDate, metric?.target_date]}
                              inputs={[
                                ...row.stockPriceInputs,
                                ...row.shareCountInputs,
                                ...ocfYearInputs(observation, year, row.currency,
                                  row.source_fetched_at.financials, row.source_fetched_at.estimates),
                              ]}
                              how={`Divide the latest stock price by positive ${period} divided by diluted shares.`} />
                          );
                        })}
                      </>
                    )}
                  </tr>
                ))}
              </tbody>
            </table>
            </div>
          )}
          </div>
        </div>
        <p className="shrink-0 border-t border-neutral-800/40 px-5 py-3 text-xs leading-relaxed text-fg-muted">
          {model === 'dcf'
            ? `Implied FCF growth over ${FORECAST_YEARS} years, with 3% perpetual growth. The 7–20% columns are discount rates.`
            : model === 'egm'
              ? 'Expected Growth Model over 5 years. Enter EPS growth and exit P/E for each company in its EGM tab; the hurdle rate is 10%.'
              : model === 'eps'
                ? 'Each fiscal year uses reported EPS without NRI when available and otherwise uses the GuruFocus consensus estimate. CAGR compounds FY2025 and FY2027 over two years; each P/E uses that year’s selected positive EPS.'
                : 'Each fiscal year divides reported operating cash flow, or otherwise the GuruFocus consensus estimate, by current diluted shares. CAGR compounds FY2025 and FY2027 OCF per share over two years; each P/OCF divides the latest stock price by that year’s selected positive OCF per share.'}
        </p>
        {companyFundamental && (
          <OwnerEarningsModal isin={companyFundamental.isin} name={companyFundamental.name}
            bookName={name} sharePct={companyFundamental.weight_pct}
            refreshScope={portfolioId != null
              ? { kind: 'portfolio', id: portfolioId, name }
              : basket
                ? { kind: 'basket', holdings: basket.holdings, name: basket.label || name }
                : undefined}
            onClose={() => setCompanyFundamental(null)} />
        )}
      </div>
    </PanelDialog>
  );
}
