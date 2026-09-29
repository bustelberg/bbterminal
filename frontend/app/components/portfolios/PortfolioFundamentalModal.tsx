'use client';

import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { type Basket } from './types';
import PanelDialog from './PanelDialog';
import {
  egmSource, estimateCagrWorking, medianPEWorking, reverseDcfSource, reverseDcfWorking,
  type MedianPeWorking, type SourceObs,
} from './egmInputs';
import { calculateEGM, EGM_DEFAULTS } from './egm';
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
import PortfolioFundamentalsRefresh, { type RefreshScope } from './PortfolioFundamentalsRefresh';
import { workedEgmReturn, workedEgmTotalReturn, workedImpliedPrice } from './valuationFormulas';
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

/** The AIRS-book leg of the exact analysis payload that powers the Analyse modal. */
type BookAnalysis = {
  holdings_as_of?: string | null;
  holdings_fetched_at?: string | null;
  book_holdings?: {
    name?: string | null;
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
const EPS_ESTIMATE_YEARS = [2026, 2027] as const;
type EpsEstimateYear = typeof EPS_ESTIMATE_YEARS[number];
type Model = 'dcf' | 'egm' | 'eps';
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
  tone: 'dcf' | 'egm' | 'eps';
  emphasis?: boolean;
}) {
  return (
    <td className={`${tone === 'dcf' ? 'bg-accent-500/[0.035]'
      : tone === 'egm' ? 'bg-pos-500/[0.035]' : 'bg-warn-500/[0.035]'} px-3 py-2 text-right font-mono tabular-nums ${emphasis ? 'font-semibold text-fg-strong' : ''}`}>
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
const eurWhole = new Intl.NumberFormat('en-GB', {
  style: 'currency', currency: 'EUR', minimumFractionDigits: 0, maximumFractionDigits: 0,
});

function bookWeightPct(bookWeight: BookWeight): number {
  return bookWeight.current_value_eur / bookWeight.total_current_value_eur * 100;
}

/**
 * A refusal is an answer, not an empty cell.
 *
 * A reverse DCF can only compound a positive starting cash flow. Tesla exposed the otherwise
 * invisible branch: every source operand was present in the info card, but its FY1 FCF normalised
 * to a negative number, so `solveGrowth` correctly returned null and fourteen bare dashes made
 * that look like missing data. Keep a genuinely missing/unsolvable case as a dash; name the one
 * refusal we can diagnose exactly from the row's own inputs.
 */
export function dcfGrowthCellLabel(impliedGrowth: number | null,
  normalisedStartingFcf: number | null): string {
  if (impliedGrowth != null) return `${(impliedGrowth * 100).toFixed(1)}%`;
  return normalisedStartingFcf != null && normalisedStartingFcf <= 0 ? 'No +FCF' : '—';
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

function sourceInput(metrics: ApiMetric[], label: string, observation: SourceObs,
  suffix = ''): InputObservation | null {
  if (observation.raw == null) return null;
  return {
    label: observation.ttm ? `TTM ${label}` : label,
    value: `${inputNumber.format(observation.raw)}${suffix}`,
    retrieved: observationRetrievedAt(metrics, observation),
    applies: observation.date,
  };
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
    .sort((a, b) => b.target_date.localeCompare(a.target_date))[0] ?? null;
}

export function epsActualForYear(metrics: ApiMetric[], year: number): ApiMetric | null {
  return metrics
    .filter((metric) => EPS_ACTUAL_CODES.has(metric.metric_code)
      && metric.numeric_value != null && metric.target_date.slice(0, 4) === String(year))
    .sort((a, b) => b.target_date.localeCompare(a.target_date))[0] ?? null;
}

export function epsActualToEstimateCagr2025To2027(metrics: ApiMetric[]): number | null {
  const start = epsActualForYear(metrics, 2025)?.numeric_value ?? null;
  const end = epsEstimateForYear(metrics, 2027)?.numeric_value ?? null;
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

function epsInput(metric: ApiMetric | null, label: string,
  currency?: string | null): InputObservation[] {
  if (metric?.numeric_value == null) return [];
  return [{
    label,
    value: `${inputNumber.format(metric.numeric_value)}${currency ? ` ${currency}/share` : ' per share'}`,
    retrieved: metric.recorded_at ?? null,
    applies: metric.target_date,
  }];
}

function dcfRow(row: ApiRow, today: string) {
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
    growthRate: egm.analystGrowth5Y ?? EGM_DEFAULTS.growthRate,
    dividendYield: egm.dividendYield,
    exitPE: egm.medianPE5Y ?? EGM_DEFAULTS.exitPE,
    hurdleRate: EGM_DEFAULTS.hurdleRate,
    years: EGM_DEFAULTS.years,
  };
  const estimateRange = estimateDates.length > 1
    ? `Forecast ${onDate(estimateDates[0])} → ${onDate(estimateDates[estimateDates.length - 1])}`
    : undefined;
  const medianPeRange = medianPeDates.length > 1
    ? `Completed FYs ${medianPeDates[0].slice(0, 4)}–${medianPeDates[medianPeDates.length - 1].slice(0, 4)}`
    : undefined;
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
      value: `${(egmAssumptions.growthRate * 100).toFixed(1)}%`,
      retrieved: egm.analystGrowth5Y != null ? row.source_fetched_at.estimates ?? null : null,
      applies: egm.analystGrowth5Y != null ? estimateDates : null,
      retrievedText: egm.analystGrowth5Y == null ? 'House assumption' : undefined,
      appliesText: egm.analystGrowth5Y != null ? estimateRange : 'Every forecast year',
    },
    {
      label: 'Dividend yield',
      value: `${((egmAssumptions.dividendYield ?? 0) * 100).toFixed(2)}%`,
      retrieved: egm.dividendYield != null ? row.source_fetched_at.financials ?? null : null,
      applies: egm.dividendYield != null ? egm.dividendYieldDate : null,
      retrievedText: egm.dividendYield == null ? 'Not reported; model uses 0%' : undefined,
      appliesText: egm.dividendYield == null ? 'Every forecast year' : undefined,
    },
    {
      label: egm.medianPE5Y != null ? 'Historical median Exit P/E' : 'Default Exit P/E',
      value: `${inputNumber.format(egmAssumptions.exitPE)}×`,
      retrieved: egm.medianPE5Y != null ? row.source_fetched_at.financials ?? null : null,
      applies: egm.medianPE5Y != null ? medianPeDates : null,
      retrievedText: egm.medianPE5Y == null ? 'House assumption' : undefined,
      appliesText: egm.medianPE5Y != null ? medianPeRange : 'End of year 10',
    },
  ];
  const egmResult = calculateEGM({ ...egm, forwardPE }, egmAssumptions);
  const eps2025Actual = epsActualForYear(row.metrics, 2025);
  const epsEstimates = Object.fromEntries(EPS_ESTIMATE_YEARS.map((year) => [
    year, epsEstimateForYear(row.metrics, year),
  ])) as Record<EpsEstimateYear, ApiMetric | null>;
  const epsEstimateCagr = epsActualToEstimateCagr2025To2027(row.metrics);
  const epsByYear = {
    2025: eps2025Actual?.numeric_value ?? null,
    2026: epsEstimates[2026]?.numeric_value ?? null,
    2027: epsEstimates[2027]?.numeric_value ?? null,
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
  const commonInputs = [
    sourceInput(row.metrics, 'Close price', working.price,
      row.currency ? ` ${row.currency}` : ''),
    sourceInput(row.metrics, 'Diluted shares outstanding', working.shares, 'm shares'),
  ];
  const stockPriceInputs = commonInputs.slice(0, 1)
    .filter((input): input is InputObservation => input != null);
  const cashFlowInputs = !useForward
    ? [
      sourceInput(row.metrics, 'Free cash flow', working.fcf, 'm'),
      sourceInput(row.metrics, 'Stock-based compensation', working.sbc, 'm'),
      sourceInput(row.metrics, 'Capital expenditure', working.capex, 'm'),
      sourceInput(row.metrics, 'Depreciation and amortisation', working.dep, 'm'),
    ]
    : forward.vendor
      ? [
        sourceInput(row.metrics, 'FY1 free-cash-flow estimate', working.fcfEst, 'm'),
        sourceInput(row.metrics, 'FY1 operating-cash-flow estimate', working.ocfEst, 'm'),
        sourceInput(row.metrics, 'FY1 EBITDA estimate', working.ebitdaEst, 'm'),
        sourceInput(row.metrics, 'FY1 EBIT estimate', working.ebitEst, 'm'),
        sourceInput(row.metrics, 'Stock-based compensation', working.sbc, 'm'),
      ]
      : [
        sourceInput(row.metrics, 'FY1 operating-cash-flow estimate', working.ocfEst, 'm'),
        sourceInput(row.metrics, 'Capital expenditure', working.capex, 'm'),
        sourceInput(row.metrics, 'Depreciation and amortisation', working.dep, 'm'),
        sourceInput(row.metrics, 'Stock-based compensation', working.sbc, 'm'),
      ];
  const dcfInputs = [...commonInputs, ...cashFlowInputs]
    .filter((input): input is InputObservation => input != null);
  return {
    ...row, src, growth,
    dcfForward: useForward,
    // The base that actually reached the solver distinguishes missing inputs from a complete set
    // of inputs that says there is no positive cash flow to compound.
    dcfStartingFcf: adjusted,
    dcfInputs,
    stockPriceInputs,
    egm, forwardPE, forwardPeDerived, egmAssumptions, egmResult,
    eps2025Actual, epsEstimates, epsEstimateCagr, peByYear, peDeltaByYear,
    historicalPe10yWorking, historicalPe10yObservations, historicalPe10yWorked,
    estimateDates, medianPeDates, medianPeObservations, medianPeWorked, expectedReturnInputs,
  };
}

type ValuationRow = ReturnType<typeof dcfRow>;

function sortValue(row: ValuationRow, key: string): number | null {
  if (key.startsWith('rate:')) {
    const rate = Number(key.slice(5));
    return row.growth.find((cell) => cell.discountRate === rate)?.impliedGrowth ?? null;
  }
  if (key.startsWith('epsEstimate:')) {
    const year = Number(key.slice('epsEstimate:'.length)) as EpsEstimateYear;
    return row.epsEstimates[year]?.numeric_value ?? null;
  }
  if (key.startsWith('epsPe:')) {
    const year = Number(key.slice('epsPe:'.length)) as 2025 | 2026 | 2027;
    return row.peByYear[year] ?? null;
  }
  if (key.startsWith('epsPeDelta:')) {
    const year = Number(key.slice('epsPeDelta:'.length)) as 2025 | 2026 | 2027;
    return row.peDeltaByYear[year] ?? null;
  }
  const values: Record<string, number | null> = {
    weight: row.weight_pct,
    price: row.src.price,
    epsActual2025: row.eps2025Actual?.numeric_value ?? null,
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
  };
  return values[key] ?? null;
}

export default function PortfolioFundamentalModal({ name, portfolioId, basket, onClose }: {
  name: string;
  portfolioId?: number;
  basket?: Basket;
  onClose: () => void;
}) {
  const [data, setData] = useState<Payload | null>(null);
  // `undefined` is loading; `null` means this model genuinely has no usable paired AIRS book.
  const [bookAnalysis, setBookAnalysis] = useState<BookAnalysis | null | undefined>(undefined);
  const [error, setError] = useState<string | null>(null);
  const [model, setModel] = useState<Model>('dcf');
  const [companyFundamental, setCompanyFundamental] = useState<ApiRow | null>(null);
  const [fundamentalsRevision, setFundamentalsRevision] = useState(0);
  const partialRefreshTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const [sorts, setSorts] = useState<Record<Model, Sort>>({
    dcf: { key: 'weight', direction: 'desc' },
    egm: { key: 'weight', direction: 'desc' },
    eps: { key: 'weight', direction: 'desc' },
  });
  const today = new Date().toISOString().slice(0, 10);

  useEffect(() => {
    const controller = new AbortController();
    void (async () => {
      try {
        const response = await apiFetch(`${API_URL}/api/earnings/portfolio-company-metrics`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(portfolioId != null
            ? { portfolio_id: portfolioId }
            : { holdings: basket?.holdings ?? [], basket_label: basket?.label ?? name }),
          signal: controller.signal,
        });
        const body = await response.json().catch(() => null) as Payload | { detail?: string } | null;
        if (!response.ok) throw new Error((body as { detail?: string } | null)?.detail ?? `HTTP ${response.status}`);
        setData(body as Payload);
      } catch (e) {
        if (!controller.signal.aborted) setError(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => controller.abort();
  }, [basket, name, portfolioId, fundamentalsRevision]);

  // We deliberately take the EUR numerator, total and source dates from the SAME book-analysis
  // payload as Analyse. The metrics endpoint owns fundamentals; it must not grow a second notion
  // of an AIRS weight that can drift from the Analyse table.
  useEffect(() => {
    if (portfolioId == null) {
      setBookAnalysis(null);
      return;
    }
    const controller = new AbortController();
    setBookAnalysis(undefined);
    void (async () => {
      try {
        const response = await apiFetch(
          `${API_URL}/api/airs/model-portfolios/${portfolioId}/analysis?benchmark=ACWI&weight_by=book&source=book`,
          { signal: controller.signal },
        );
        const body = await response.json().catch(() => null) as BookAnalysis | { detail?: string } | null;
        if (!response.ok) throw new Error((body as { detail?: string } | null)?.detail ?? `HTTP ${response.status}`);
        setBookAnalysis(body as BookAnalysis);
      } catch {
        // Fundamental remains useful for an unpaired book; its Weight cell then says exactly that
        // through the existing model-composition fallback.
        if (!controller.signal.aborted) setBookAnalysis(null);
      }
    })();
    return () => controller.abort();
  }, [portfolioId]);

  // The fill is concurrent, so several companies can land within a few milliseconds. Coalesce
  // those stream events into one re-read, while still showing the first completed batch promptly.
  const reloadPartialFundamentals = useCallback(() => {
    if (partialRefreshTimer.current != null) return;
    partialRefreshTimer.current = setTimeout(() => {
      partialRefreshTimer.current = null;
      setFundamentalsRevision((value) => value + 1);
    }, 300);
  }, []);
  useEffect(() => () => {
    if (partialRefreshTimer.current != null) clearTimeout(partialRefreshTimer.current);
  }, []);

  const rows = useMemo(() => {
    const holdings = bookAnalysis?.book_holdings ?? [];
    const total = holdings.reduce((sum, holding) => sum + (holding.current_value_eur ?? 0), 0);
    const byIsin = new Map<string, { name: string; currentValue: number }>();
    for (const holding of holdings) {
      if (!holding.isin || holding.current_value_eur == null) continue;
      const present = byIsin.get(holding.isin);
      byIsin.set(holding.isin, {
        name: present?.name ?? holding.name ?? holding.isin,
        currentValue: (present?.currentValue ?? 0) + holding.current_value_eur,
      });
    }
    return (data?.rows ?? []).map((row) => {
      const holding = byIsin.get(row.isin);
      const bookWeight = holding && total > 0 && bookAnalysis?.holdings_as_of
        ? {
          holding_name: holding.name,
          current_value_eur: holding.currentValue,
          total_current_value_eur: total,
          as_of_date: bookAnalysis.holdings_as_of,
          fetched_at: bookAnalysis.holdings_fetched_at,
        }
        : null;
      return dcfRow({ ...row, book_weight: bookWeight }, today);
    });
  }, [bookAnalysis, data, today]);
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
                : model === 'egm' ? 'text-pos-300' : 'text-warn-400' : ''}`}>
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
          <div className="ml-auto inline-flex shrink-0 rounded-lg border border-neutral-700 bg-page p-0.5"
            role="group" aria-label="Valuation model">
            <button type="button" onClick={() => setModel('dcf')} aria-pressed={model === 'dcf'}
              className={`rounded-md px-4 py-1.5 text-xs font-medium transition-colors ${model === 'dcf'
                ? 'bg-accent-600 text-white shadow-sm' : 'text-fg-muted hover:bg-overlay/5 hover:text-fg-strong'}`}>
              Reverse DCF
            </button>
            <button type="button" onClick={() => setModel('egm')} aria-pressed={model === 'egm'}
              className={`rounded-md px-4 py-1.5 text-xs font-medium transition-colors ${model === 'egm'
                ? 'bg-pos-500 text-white shadow-sm' : 'text-fg-muted hover:bg-overlay/5 hover:text-fg-strong'}`}>
              Expected Growth Model
            </button>
            <button type="button" onClick={() => setModel('eps')} aria-pressed={model === 'eps'}
              className={`rounded-md px-4 py-1.5 text-xs font-medium transition-colors ${model === 'eps'
                ? 'bg-warn-500 text-white shadow-sm' : 'text-fg-muted hover:bg-overlay/5 hover:text-fg-strong'}`}>
              EPS Estimates
            </button>
          </div>
          {refreshScope && (
            <PortfolioFundamentalsRefresh scope={refreshScope} everything prominent showNote={false}
              label="Refresh all companies"
              jobTitle={`${name}: ${model === 'dcf' ? 'Reverse DCF'
                : model === 'egm' ? 'Expected Growth Model' : 'EPS Estimates'}`}
              onProgress={reloadPartialFundamentals}
              onDone={() => setFundamentalsRevision((value) => value + 1)} />
          )}
          <button type="button" onClick={onClose}
            className="shrink-0 rounded-md border border-accent-500 bg-accent-600 px-4 py-1.5 text-sm font-medium text-white shadow-sm transition-colors hover:bg-accent-500">
            Close
          </button>
        </div>

        <div className="min-h-0 flex-1 overflow-auto px-6">
          {!data && !error && <p className="p-5 text-sm text-fg-muted">Loading company fundamentals…</p>}
          {error && <p className="m-5 rounded-lg border border-neg-500/30 bg-neg-500/10 p-3 text-sm text-neg-300">{error}</p>}
          {data && rows.length === 0 && (
            <p className="p-5 text-sm text-fg-muted">No operating companies with stored fundamentals were found.</p>
          )}
          {rows.length > 0 && (
            <div className="my-4 min-w-max rounded-xl border border-neutral-800/50">
            <table className="w-full text-xs">
              <thead className="sticky top-0 z-10 bg-card text-xs uppercase tracking-wide text-fg-faint">
                <tr className="border-b border-neutral-800/40">
                  {/* These three columns are the subject, not the selected valuation model. They
                      stay pinned and unchanged while the switch replaces only the coloured block
                      to their right. Fixed widths make the sticky offsets exact. */}
                  <th className="sticky left-0 z-20 w-80 min-w-80 max-w-80 bg-page px-3 py-2 text-left font-medium">
                    Company
                  </th>
                  <NumericHeader sortKey="weight" label="Weight"
                    info="Each row's current share of the paired AIRS book, calculated from VOLK Huidige waarde."
                    className="sticky left-80 z-20 w-24 min-w-24 max-w-24 bg-page" />
                  <NumericHeader sortKey="price" label="Stock price"
                    info="The latest stored GuruFocus closing price. The currency code is the company's GuruFocus exchange currency; the value is not converted to EUR."
                    className="sticky left-[26rem] z-20 w-36 min-w-36 max-w-36 border-r-2 border-neutral-700 bg-page" />
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
                      <NumericHeader sortKey="impliedPrice" label="Price in 10y" className="bg-pos-500/10" />
                      <NumericHeader sortKey="totalReturn" label="Total return" className="bg-pos-500/10" />
                    </>
                  ) : (
                    <>
                      <NumericHeader sortKey="epsActual2025" label="EPS 2025A"
                        className="bg-warn-500/10" />
                      {EPS_ESTIMATE_YEARS.map((year) => (
                        <NumericHeader key={year} sortKey={`epsEstimate:${year}`}
                          label={`EPS ${year}E`} className="bg-warn-500/10" />
                      ))}
                      <NumericHeader sortKey="epsEstimateCagr" label="CAGR 2025A–2027E"
                        className="bg-warn-500/10" />
                      <NumericHeader sortKey="epsHistoricalPe10y" label="10y Historical P/E"
                        className="bg-warn-500/10" />
                      <NumericHeader sortKey="epsPe:2025" label="P/E 2025A"
                        className="bg-warn-500/10" />
                      <NumericHeader sortKey="epsPe:2026" label="P/E 2026E"
                        className="bg-warn-500/10" />
                      <NumericHeader sortKey="epsPe:2027" label="P/E 2027E"
                        className="bg-warn-500/10" />
                      {([2025, 2026, 2027] as const).map((year) => (
                        <NumericHeader key={`peDelta:${year}`} sortKey={`epsPeDelta:${year}`}
                          label={`${year} % delta`} className="bg-warn-500/10" />
                      ))}
                    </>
                  )}
                </tr>
              </thead>
              <tbody className="divide-y divide-neutral-800/20">
                {sortedRows.map((row) => (
                  <tr key={row.company_id} className="hover:bg-overlay/[0.03]">
                    <td className="sticky left-0 z-[2] w-80 min-w-80 max-w-80 bg-page px-3 py-2">
                      <div className="flex items-center gap-2">
                        <div className="min-w-0 flex-1">
                          <div className="truncate font-medium text-fg-strong" title={row.name}>{row.name}</div>
                          <div className="font-mono text-[10px] text-fg-faint">{row.isin}</div>
                        </div>
                        <button type="button" onClick={() => setCompanyFundamental(row)}
                          title={`Open the full Fundamental view for ${row.name}`}
                          className="shrink-0 rounded-md border border-neutral-700 px-2 py-1 text-[10px] font-medium text-fg-muted transition-colors hover:border-accent-500/50 hover:bg-overlay/5 hover:text-accent-300">
                          Fundamental
                        </button>
                      </div>
                    </td>
                    <td className="sticky left-80 z-[2] w-24 min-w-24 max-w-24 bg-page px-3 py-2 text-right font-mono tabular-nums">
                      <span className="flex items-center justify-end gap-1.5 whitespace-nowrap">
                        <span>{row.book_weight ? `${bookWeightPct(row.book_weight).toFixed(2)}%`
                          : portfolioId != null ? bookAnalysis === undefined ? '…' : '—'
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
                          : portfolioId != null ? <InfoTip wide className="font-sans text-fg-faint" content={<AspectCard
                            what={bookAnalysis === undefined ? 'AIRS book weight is loading.' : 'No AIRS book value is available for this company.'}
                            where="AIRS Vermogensoverzicht (VOLK)."
                            when={bookAnalysis === undefined ? 'Loading the same AIRS book analysis used by Analyse.' : 'No matching current AIRS book holding was returned.'}
                            how="A model-composition weight is not substituted for an AIRS book weight." />} />
                          : <InfoTip wide className="font-sans text-fg-faint" content={<AspectCard
                            what={`${row.name}'s share of the whole current portfolio.`}
                            where={`${name}'s current composition after linked certificates and TopSelecties are looked through.`}
                            when="The portfolio composition loaded for this Fundamental view."
                            how={`This company's portfolio weight is ${row.weight_pct.toFixed(2)}%; funds, cash, bonds and uncovered companies are not redistributed over the visible company rows.`} />} />}
                      </span>
                    </td>
                    <td className="sticky left-[26rem] z-[2] w-36 min-w-36 max-w-36 border-r-2 border-neutral-700 bg-page px-3 py-2 font-mono tabular-nums">
                      <span className="grid grid-cols-[2.25rem_1fr_auto] items-baseline gap-1.5">
                        <span className="text-left text-[10px] text-fg-faint">{row.currency ?? ''}</span>
                        <span className="text-right">{row.src.price == null ? '—' : row.src.price.toFixed(2)}</span>
                        <InfoTip wide className="font-sans text-fg-faint" content={<AspectCard
                          what={`The latest stored closing price for ${row.name}.`}
                          where="GuruFocus closing-price history."
                          when={<InputObservationRows inputs={row.stockPriceInputs} />}
                          how="Select the newest stored close; the currency shown in the cell is the company's GuruFocus exchange currency." />} />
                      </span>
                    </td>
                    {model === 'dcf' ? row.growth.map((cell) => (
                      <ValuationCell key={cell.discountRate} tone="dcf"
                        value={dcfGrowthCellLabel(cell.impliedGrowth, row.dcfStartingFcf)}
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
                          ? `Base FCF is ${inputNumber.format(row.dcfStartingFcf)}${row.currency ? ` ${row.currency}m` : 'm'}. Non-positive FCF has no growth solution.`
                          : `Discount FCF and terminal value at ${(cell.discountRate * 100).toFixed(0)}%, then solve growth against market value.`} />
                    )) : model === 'egm' ? (
                      <>
                        <ValuationCell tone="egm"
                          value={row.egm.epsNextFY == null ? '—' : row.egm.epsNextFY.toFixed(2)}
                          what="Consensus earnings per share for the next fiscal year."
                          where={`GuruFocus analyst estimates for ${row.name}.`}
                          retrieved={[row.source_fetched_at.estimates]}
                          applies={[row.egm.epsNextFYDate]}
                          how="Select the earliest positive-period EPS estimate whose fiscal period ends after today." />
                        <ValuationCell tone="egm"
                          value={row.forwardPE == null ? '—' : `${row.forwardPE.toFixed(2)}×`}
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
                          value={`${(row.egmAssumptions.growthRate * 100).toFixed(2)}%`}
                          what="The annual EPS growth rate assumed for the next ten years."
                          where={row.egm.analystGrowth5Y != null
                            ? 'GuruFocus analyst EPS estimates.'
                            : '10% house assumption because a usable estimate series is unavailable.'}
                          retrieved={row.egm.analystGrowth5Y != null
                            ? [row.source_fetched_at.estimates] : []}
                          applies={row.estimateDates}
                          how={row.egm.analystGrowth5Y != null
                            ? 'Calculate the CAGR from the first to the last positive future EPS estimate.'
                            : 'Apply the 10% house default.'} />
                        <ValuationCell tone="egm"
                          value={`${((row.egmAssumptions.dividendYield ?? 0) * 100).toFixed(2)}%`}
                          what="The annual dividend yield carried through the model."
                          where="The newest GuruFocus Dividend Yield % observation; an unavailable yield is treated as 0%."
                          retrieved={row.egm.dividendYield != null
                            ? [row.source_fetched_at.financials] : []}
                          applies={row.egm.dividendYield != null
                            ? [row.egm.dividendYieldDate] : []}
                          how="Convert the vendor percentage to a decimal and hold that yield constant for the projection." />
                        <ValuationCell tone="egm"
                          value={`${row.egmAssumptions.exitPE.toFixed(2)}×`}
                          what="The price-to-earnings multiple assumed at the end of year ten."
                          where={row.egm.medianPE5Y != null
                            ? 'GuruFocus fiscal year-end prices and EPS without NRI.'
                            : 'House assumption of 20 times earnings because usable five-year history is unavailable.'}
                          retrieved={row.egm.medianPE5Y != null
                            ? [row.source_fetched_at.financials] : []}
                          applies={row.medianPeDates}
                          inputs={row.egm.medianPE5Y != null ? row.medianPeObservations : undefined}
                          how={row.egm.medianPE5Y != null
                            ? row.medianPeWorked
                            : 'Apply the house default of 20 times earnings.'} />
                        <ValuationCell tone="egm" emphasis
                          value={row.egmResult.fairValue == null ? '—' : row.egmResult.fairValue.toFixed(2)}
                          what="The highest price today that still meets the model's 10% annual return hurdle."
                          where="FY1 consensus EPS, expected EPS growth, dividend yield and the historical or default exit P/E."
                          retrieved={[row.source_fetched_at.estimates, row.source_fetched_at.financials]}
                          applies={[row.egm.epsNextFYDate, row.egm.dividendYieldDate,
                            ...row.estimateDates, ...row.medianPeDates]}
                          how="Multiply FY1 EPS by the maximum starting P/E compatible with ten years of EPS growth, dividends, the exit multiple and the 10% hurdle rate." />
                        <ValuationCell tone="egm"
                          value={row.egmResult.upside == null ? '—' : `${row.egmResult.upside >= 0 ? '+' : ''}${(row.egmResult.upside * 100).toFixed(2)}%`}
                          what="The difference between model fair value and the latest stored share price."
                          where="The Expected Growth Model fair value and GuruFocus close price."
                          retrieved={[row.source_fetched_at.estimates, row.source_fetched_at.financials]}
                          applies={[row.egm.priceDate, row.egm.epsNextFYDate,
                            row.egm.dividendYieldDate, ...row.estimateDates, ...row.medianPeDates]}
                          how="Divide fair value by the latest stored share price and subtract one." />
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
                          value={row.egmResult.impliedPrice == null ? '—' : row.egmResult.impliedPrice.toFixed(2)}
                          what="The share price implied at the end of year ten."
                          where="Current price and forward P/E, expected EPS growth and the historical or default exit P/E."
                          retrieved={[row.source_fetched_at.estimates, row.source_fetched_at.financials,
                            row.forwardPeDerived ? null : row.source_fetched_at.indicators]}
                          applies={[row.egm.priceDate, row.egm.epsNextFYDate, row.egm.forwardPEDate,
                            ...row.estimateDates, ...row.medianPeDates]}
                          inputs={row.expectedReturnInputs.filter((input) => input.label !== 'Dividend yield')}
                          how="Grow earnings for ten years and revalue them from today's forward P/E to the exit P/E; dividends are not part of this price."
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
                    ) : (
                      <>
                        <ValuationCell tone="eps"
                          value={row.eps2025Actual?.numeric_value == null
                            ? '—' : row.eps2025Actual.numeric_value.toFixed(2)}
                          what="The reported FY2025 earnings per share, excluding non-recurring items."
                          where={`GuruFocus annual financial statements stored for ${row.name}.`}
                          retrieved={[row.source_fetched_at.financials]}
                          applies={[row.eps2025Actual?.target_date]}
                          inputs={epsInput(row.eps2025Actual, 'Reported EPS for FY2025', row.currency)}
                          how="Select the latest annual EPS without NRI observation whose fiscal period ends in 2025." />
                        {EPS_ESTIMATE_YEARS.map((year) => {
                          const estimate = row.epsEstimates[year];
                          return (
                            <ValuationCell key={year} tone="eps"
                              value={estimate?.numeric_value == null
                                ? '—' : estimate.numeric_value.toFixed(2)}
                              what={`The GuruFocus consensus earnings-per-share estimate for fiscal year ${year}.`}
                              where={`GuruFocus analyst estimates stored for ${row.name}.`}
                              retrieved={[row.source_fetched_at.estimates]}
                              applies={[estimate?.target_date]}
                              inputs={epsInput(estimate, `EPS estimate for FY${year}`, row.currency)}
                              how={`Select the annual per-share EPS estimate whose fiscal period ends in ${year}.`} />
                          );
                        })}
                        <ValuationCell tone="eps" emphasis
                          value={row.epsEstimateCagr == null
                            ? '—' : `${(row.epsEstimateCagr * 100).toFixed(1)}%`}
                          what="The annualised change from reported FY2025 EPS to the FY2027 EPS estimate."
                          where={`GuruFocus financial statements and analyst estimates stored for ${row.name}.`}
                          retrieved={[row.source_fetched_at.financials,
                            row.source_fetched_at.estimates]}
                          applies={[row.eps2025Actual?.target_date,
                            row.epsEstimates[2027]?.target_date]}
                          inputs={[
                            ...epsInput(row.eps2025Actual, 'Reported EPS for FY2025', row.currency),
                            ...epsInput(row.epsEstimates[2027], 'EPS estimate for FY2027', row.currency),
                          ]}
                          how="Compound the change between positive FY2025 actual EPS and FY2027 estimated EPS over two years." />
                        <ValuationCell tone="eps"
                          value={row.historicalPe10yWorking.median == null
                            ? '—' : `${row.historicalPe10yWorking.median.toFixed(2)}×`}
                          what="The median P/E across the latest ten completed fiscal years."
                          where="GuruFocus fiscal year-end prices and EPS without NRI."
                          retrieved={[row.source_fetched_at.financials]}
                          applies={row.historicalPe10yWorking.rows.map((point) => `${point.year}-12-31`)}
                          inputs={row.historicalPe10yObservations}
                          how={row.historicalPe10yWorked} />
                        {([2025, 2026, 2027] as const).map((year) => {
                          const actual = year === 2025;
                          const epsMetric = actual ? row.eps2025Actual : row.epsEstimates[year];
                          const multiple = row.peByYear[year];
                          const period = `FY${year}${actual ? ' actual' : ' estimate'}`;
                          return (
                            <ValuationCell key={`pe:${year}`} tone="eps"
                              value={multiple == null ? '—' : `${multiple.toFixed(1)}×`}
                              what={`The latest stock price expressed as a multiple of the ${period} EPS.`}
                              where={`GuruFocus close price and ${actual ? 'reported EPS without NRI' : 'analyst EPS consensus'} stored for ${row.name}.`}
                              retrieved={[actual ? row.source_fetched_at.financials
                                : row.source_fetched_at.estimates]}
                              applies={[row.src.priceDate, epsMetric?.target_date]}
                              inputs={[
                                ...row.stockPriceInputs,
                                ...epsInput(epsMetric,
                                  `${actual ? 'Reported EPS' : 'EPS estimate'} for FY${year}`,
                                  row.currency),
                              ]}
                              how={`Divide the latest stored close price by positive ${period} EPS.`} />
                          );
                        })}
                        {([2025, 2026, 2027] as const).map((year) => {
                          const actual = year === 2025;
                          const epsMetric = actual ? row.eps2025Actual : row.epsEstimates[year];
                          const multiple = row.peByYear[year];
                          const delta = row.peDeltaByYear[year];
                          const period = `FY${year}${actual ? ' actual' : ' estimate'}`;
                          return (
                            <ValuationCell key={`peDelta:${year}`} tone="eps"
                              value={delta == null ? '—' : `${delta >= 0 ? '+' : ''}${(delta * 100).toFixed(2)}%`}
                              what={`How far the ${period} P/E differs from the ten-year historical median P/E.`}
                              where="The current share price, that fiscal year's EPS and the latest ten completed fiscal years of price and EPS history."
                              retrieved={[actual ? row.source_fetched_at.financials
                                : row.source_fetched_at.estimates, row.source_fetched_at.financials]}
                              applies={[row.src.priceDate, epsMetric?.target_date,
                                ...row.historicalPe10yWorking.rows.map((point) => `${point.year}-12-31`)]}
                              inputs={[
                                ...row.stockPriceInputs,
                                ...epsInput(epsMetric,
                                  `${actual ? 'Reported EPS' : 'EPS estimate'} for FY${year}`,
                                  row.currency),
                                {
                                  label: '10y historical P/E',
                                  value: row.historicalPe10yWorking.median == null
                                    ? 'not available' : `${row.historicalPe10yWorking.median.toFixed(2)}×`,
                                  retrieved: row.source_fetched_at.financials ?? null,
                                  applies: row.historicalPe10yWorking.rows.map((point) => `${point.year}-12-31`),
                                },
                              ]}
                              how={`${multiple == null || row.historicalPe10yWorking.median == null
                                ? 'A delta requires both the fiscal-year P/E and the ten-year historical median P/E.'
                                : `${multiple.toFixed(2)}× ÷ ${row.historicalPe10yWorking.median.toFixed(2)}× − 1 = ${(delta! * 100).toFixed(2)}%`}`} />
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
        <p className="shrink-0 border-t border-neutral-800/40 px-5 py-3 text-xs leading-relaxed text-fg-muted">
          {model === 'dcf'
            ? `Implied FCF growth over ${FORECAST_YEARS} years, with 3% perpetual growth. The 7–20% columns are discount rates.`
            : model === 'egm'
              ? 'Expected Growth Model over 10 years. EPS growth and exit P/E use company estimates/history where available, otherwise the 10% growth and 20× house defaults; hurdle rate is 10%.'
              : 'FY2025 is reported EPS without NRI; FY2026 and FY2027 are GuruFocus consensus estimates. CAGR compounds FY2025A to FY2027E over two years. Each P/E divides the latest stored stock price by that year’s positive EPS.'}
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
