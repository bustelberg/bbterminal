'use client';

import { useEffect, useMemo, useState, type ReactNode } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { type Basket } from './types';
import PanelDialog from './PanelDialog';
import {
  egmSource, estimateCagrWorking, medianPEWorking, reverseDcfSource, reverseDcfWorking,
  type SourceObs,
} from './egmInputs';
import { calculateEGM, EGM_DEFAULTS } from './egm';
import { forwardLegs, normalisedFcf } from './normalisedFcf';
import {
  FORECAST_YEARS, impliedGrowth, marketCapOf, PERPETUITY_GROWTH,
} from './reverseDcf';
import { type MetricRow } from './quickValuation';
import InfoTip from '../InfoTip';
import OwnerEarningsModal from './OwnerEarningsModal';
import { AspectCard } from '../../../lib/tipCard';
import { onDate } from './asOfLine';
import PortfolioFundamentalsRefresh, { type RefreshScope } from './PortfolioFundamentalsRefresh';

type ApiMetric = MetricRow & { recorded_at?: string | null };

type ApiRow = {
  company_id: number;
  isin: string;
  name: string;
  weight_pct: number;
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
  applies: string | null;
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
      {inputs.map((input) => (
        <span key={`${input.label}:${input.applies ?? ''}`} className="block border-b border-neutral-800/50 pb-2 last:border-0 last:pb-0">
          <span className="flex flex-wrap items-center gap-1.5">
            <span>{input.label}</span>
            <span className="inline-flex rounded-full border border-neutral-700 bg-overlay/10 px-1.5 py-0.5 font-mono text-[11px] text-fg-soft">
              {input.value}
            </span>
          </span>
          <span className="mt-1 flex flex-wrap items-center gap-x-2 gap-y-1 text-fg-faint">
            <span className="inline-flex items-center gap-1">
              <span>Retrieved</span>
              <span className="inline-flex rounded-full border border-neutral-700 bg-overlay/10 px-1.5 py-0.5 font-mono text-[11px] text-fg-soft">
                {input.retrieved ? onDate(input.retrieved) : 'not recorded'}
              </span>
            </span>
            <span className="inline-flex items-center gap-1">
              <span>Applies to</span>
              <span className="inline-flex rounded-full border border-neutral-700 bg-overlay/10 px-1.5 py-0.5 font-mono text-[11px] text-fg-soft">
                {input.applies ? onDate(input.applies) : 'not recorded'}
              </span>
            </span>
          </span>
        </span>
      ))}
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

function ValuationCell({ value, what, where, how, retrieved, applies, inputs, tone, emphasis = false }: {
  value: ReactNode;
  what: ReactNode;
  where: ReactNode;
  how: ReactNode;
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
            how={how} />
        )} />
      </span>
    </td>
  );
}

const inputNumber = new Intl.NumberFormat('en-GB', { maximumFractionDigits: 2 });

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
  const medianPeWorking = medianPEWorking(row.metrics);
  const medianPeYears = new Set(medianPeWorking.rows.map((point) => String(point.year)));
  const medianPeDates = dateWindow(row.metrics
    .filter((metric) => medianPeYears.has(metric.target_date.slice(0, 4))
      && (metric.metric_code.endsWith('__Month End Stock Price')
        || metric.metric_code.endsWith('__EPS without NRI')))
    .map((metric) => metric.target_date));
  const egmAssumptions = {
    growthRate: egm.analystGrowth5Y ?? EGM_DEFAULTS.growthRate,
    dividendYield: egm.dividendYield,
    exitPE: egm.medianPE5Y ?? EGM_DEFAULTS.exitPE,
    hurdleRate: EGM_DEFAULTS.hurdleRate,
    years: EGM_DEFAULTS.years,
  };
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
    dcfInputs,
    stockPriceInputs,
    egm, forwardPE, forwardPeDerived, egmAssumptions, egmResult,
    eps2025Actual, epsEstimates, epsEstimateCagr, peByYear,
    estimateDates: dateWindow(cagrWorking.points.map((point) => point.date)),
    medianPeDates,
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
  const [error, setError] = useState<string | null>(null);
  const [model, setModel] = useState<Model>('dcf');
  const [companyFundamental, setCompanyFundamental] = useState<ApiRow | null>(null);
  const [refreshRevision, setRefreshRevision] = useState(0);
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
  }, [basket, name, portfolioId, refreshRevision]);

  const rows = useMemo(() => (data?.rows ?? []).map((row) => dcfRow(row, today)), [data, today]);
  const refreshScope = useMemo<RefreshScope | null>(() => {
    const isins = [...new Set((data?.rows ?? []).map((row) => row.isin).filter(Boolean))];
    if (!isins.length) return null;
    // Refresh the exact company universe this table resolved, including companies reached through
    // certificate/TopSelectie look-through. A portfolio-id refresh stops at wrapper instruments,
    // so it can be narrower than the rows visible here.
    return {
      kind: 'basket',
      holdings: isins.map((isin) => ({ isin })),
      name: `${name} visible companies`,
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
            <PortfolioFundamentalsRefresh scope={refreshScope} everything allPeriods prominent showNote={false}
              label="Refresh all companies" onDone={() => setRefreshRevision((value) => value + 1)} />
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
            <div className="my-4 min-w-max overflow-hidden rounded-xl border border-neutral-800/50">
            <table className="w-full text-xs">
              <thead className="sticky top-0 z-10 bg-card text-[10px] uppercase tracking-wide text-fg-faint">
                <tr className="border-b border-neutral-800/40">
                  {/* These three columns are the subject, not the selected valuation model. They
                      stay pinned and unchanged while the switch replaces only the coloured block
                      to their right. Fixed widths make the sticky offsets exact. */}
                  <th className="sticky left-0 z-20 w-64 min-w-64 bg-page px-3 py-2 text-left font-medium">
                    Company
                  </th>
                  <NumericHeader sortKey="weight" label="Weight"
                    info="This operating company's share of the whole current portfolio after linked certificates and TopSelecties are looked through. Funds, cash, bonds and companies without stored fundamentals are not redistributed over these rows."
                    className="sticky left-64 z-20 w-24 min-w-24 bg-page" />
                  <NumericHeader sortKey="price" label="Stock price"
                    info="The latest stored GuruFocus closing price. The currency code is the company's GuruFocus exchange currency; the value is not converted to EUR."
                    className="sticky left-[22rem] z-20 w-28 min-w-28 border-r-2 border-neutral-700 bg-page" />
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
                      <NumericHeader sortKey="expectedReturn" label="Expected p.a." className="bg-pos-500/10" />
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
                      <NumericHeader sortKey="epsPe:2025" label="P/E 2025A"
                        className="bg-warn-500/10" />
                      <NumericHeader sortKey="epsPe:2026" label="P/E 2026E"
                        className="bg-warn-500/10" />
                      <NumericHeader sortKey="epsPe:2027" label="P/E 2027E"
                        className="bg-warn-500/10" />
                    </>
                  )}
                </tr>
              </thead>
              <tbody className="divide-y divide-neutral-800/20">
                {sortedRows.map((row) => (
                  <tr key={row.company_id} className="hover:bg-overlay/[0.03]">
                    <td className="sticky left-0 z-[2] w-64 min-w-64 bg-page px-3 py-2">
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
                    <td className="sticky left-64 z-[2] w-24 min-w-24 bg-page px-3 py-2 text-right font-mono tabular-nums">
                      {row.weight_pct.toFixed(2)}%
                    </td>
                    <td className="sticky left-[22rem] z-[2] w-28 min-w-28 border-r-2 border-neutral-700 bg-page px-3 py-2 font-mono tabular-nums">
                      <span className="grid grid-cols-[2.25rem_1fr] items-baseline">
                        <span className="text-left text-[10px] text-fg-faint">{row.currency ?? ''}</span>
                        <span className="text-right">{row.src.price == null ? '—' : row.src.price.toFixed(2)}</span>
                      </span>
                    </td>
                    {model === 'dcf' ? row.growth.map((cell) => (
                      <ValuationCell key={cell.discountRate} tone="dcf"
                        value={cell.impliedGrowth == null ? '—' : `${(cell.impliedGrowth * 100).toFixed(1)}%`}
                        what={`The annual free-cash-flow growth implied by the current share price at a ${(cell.discountRate * 100).toFixed(0)}% discount rate.`}
                        where={`GuruFocus close price, diluted shares and ${row.dcfForward ? 'FY1 consensus' : 'latest reported'} cash-flow inputs stored for ${row.name}.`}
                        retrieved={[row.source_fetched_at.financials,
                          row.dcfForward ? row.source_fetched_at.estimates : null]}
                        applies={[row.src.priceDate, row.src.sharesDate, row.src.flowBasis.date,
                          row.dcfForward ? row.src.ocfEstimateDate : null]}
                        inputs={row.dcfInputs}
                        how={`Project normalised FCF, add the terminal value, discount at ${(cell.discountRate * 100).toFixed(0)}%, then solve for the annual growth rate equalling today's market capitalisation.`} />
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
                          value={row.forwardPE == null ? '—' : `${row.forwardPE.toFixed(1)}×`}
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
                          value={`${(row.egmAssumptions.growthRate * 100).toFixed(1)}%`}
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
                          value={`${row.egmAssumptions.exitPE.toFixed(1)}×`}
                          what="The price-to-earnings multiple assumed at the end of year ten."
                          where={row.egm.medianPE5Y != null
                            ? 'GuruFocus fiscal year-end prices and EPS without NRI.'
                            : 'House assumption of 20 times earnings because usable five-year history is unavailable.'}
                          retrieved={row.egm.medianPE5Y != null
                            ? [row.source_fetched_at.financials] : []}
                          applies={row.medianPeDates}
                          how={row.egm.medianPE5Y != null
                            ? 'Calculate each positive fiscal-year P/E and take the median of the latest five years.'
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
                          value={row.egmResult.upside == null ? '—' : `${row.egmResult.upside >= 0 ? '+' : ''}${(row.egmResult.upside * 100).toFixed(1)}%`}
                          what="The difference between model fair value and the latest stored share price."
                          where="The Expected Growth Model fair value and GuruFocus close price."
                          retrieved={[row.source_fetched_at.estimates, row.source_fetched_at.financials]}
                          applies={[row.egm.priceDate, row.egm.epsNextFYDate,
                            row.egm.dividendYieldDate, ...row.estimateDates, ...row.medianPeDates]}
                          how="Divide fair value by the latest stored share price and subtract one." />
                        <ValuationCell tone="egm" emphasis
                          value={row.egmResult.expectedReturn == null ? '—' : `${row.egmResult.expectedReturn >= 0 ? '+' : ''}${(row.egmResult.expectedReturn * 100).toFixed(1)}%`}
                          what="The modelled annualised shareholder return over ten years."
                          where="Current forward P/E, expected EPS growth, dividend yield and the historical or default exit P/E."
                          retrieved={[row.source_fetched_at.estimates, row.source_fetched_at.financials,
                            row.forwardPeDerived ? null : row.source_fetched_at.indicators]}
                          applies={[row.egm.priceDate, row.egm.epsNextFYDate, row.egm.forwardPEDate,
                            row.egm.dividendYieldDate, ...row.estimateDates, ...row.medianPeDates]}
                          how="Compound EPS growth and dividends, include the ten-year change from the current forward P/E to the exit P/E, then annualise the result." />
                        <ValuationCell tone="egm"
                          value={row.egmResult.impliedPrice == null ? '—' : row.egmResult.impliedPrice.toFixed(2)}
                          what="The share price implied at the end of year ten."
                          where="Current price and forward P/E, expected EPS growth and the historical or default exit P/E."
                          retrieved={[row.source_fetched_at.estimates, row.source_fetched_at.financials,
                            row.forwardPeDerived ? null : row.source_fetched_at.indicators]}
                          applies={[row.egm.priceDate, row.egm.epsNextFYDate, row.egm.forwardPEDate,
                            ...row.estimateDates, ...row.medianPeDates]}
                          how="Grow earnings for ten years and revalue them from today's forward P/E to the exit P/E; dividends are not part of this price." />
                        <ValuationCell tone="egm"
                          value={row.egmResult.totalReturn == null ? '—' : `${row.egmResult.totalReturn >= 0 ? '+' : ''}${(row.egmResult.totalReturn * 100).toFixed(0)}%`}
                          what="The cumulative shareholder return modelled over ten years, including dividends."
                          where="The Expected Growth Model's annualised return and ten-year horizon."
                          retrieved={[row.source_fetched_at.estimates, row.source_fetched_at.financials,
                            row.forwardPeDerived ? null : row.source_fetched_at.indicators]}
                          applies={[row.egm.priceDate, row.egm.epsNextFYDate, row.egm.forwardPEDate,
                            row.egm.dividendYieldDate, ...row.estimateDates, ...row.medianPeDates]}
                          how="Compound the expected annual shareholder return for ten years." />
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
