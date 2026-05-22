'use client';

import { useCallback, useMemo } from 'react';
import InfoTip from '../InfoTip';
import Spinner from '../Spinner';
import { MC, type Cadence, type MetricRow } from './types';
import {
  cadenceHoverText,
  computeCAGR,
  flowSeriesPreferQuarterlyTTM,
  fmtNum,
  fmtPct,
  fmtPctPoints,
  latestObservation,
  logLinearR2,
  observationCadence,
  stdDev,
  timeSeries,
  trailingYearsWindow,
  ttmYoYGrowthRates,
  yoyGrowthRates,
} from './utils';

/** Two-column snapshot table (Balance Sheet / Capital Intensity / Capital
 * Allocation / Value Creation on the left; Profitability / Historical
 * Growth / Outlook / Valuation on the right). Each row shows the latest
 * value, its anchor date, an info tooltip, and a cadence tooltip on the
 * value indicating the publication frequency of the underlying source. */
type EarningsSource = 'prices' | 'indicators' | 'financials' | 'analyst_estimates';

export default function SnapshotStats({
  metrics,
  refreshingSources,
}: {
  metrics: MetricRow[];
  refreshingSources?: Set<string>;
}) {
  // For any `annuals__X` code, prefer the `quarterly__X` twin whenever it
  // exists — quarterly is the always-correct read for point-in-time / ratio
  // metrics (Debt-to-Equity, Interest Coverage, CAPEX/Revenue, ROE, …) since
  // GuruFocus's annual block is just a snapshot of the last fiscal year and
  // will lag for most of the year. Annual is only used when the quarterly
  // twin doesn't exist at all in the response (some metrics ship annual-only).
  //
  // We use `latestObservation` (null-aware) instead of `latestValue` so that
  // when the most recent period exists but GF reported "N/A" for it, the
  // dashboard shows the period date with a "—" value, rather than reaching
  // back to a stale numeric from years ago. The existing fmt* helpers
  // already render null as "—".
  const lv = useCallback(
    (code: string) => {
      if (!code.startsWith('annuals__')) return latestObservation(metrics, code);
      const quarterly = latestObservation(metrics, 'quarterly__' + code.slice('annuals__'.length));
      if (quarterly) return quarterly;
      return latestObservation(metrics, code);
    },
    [metrics],
  );

  // Resolve which underlying metric_code lv() actually picked, so the cadence
  // hover reflects the real source. For annuals codes we always prefer the
  // quarterly twin when it exists, matching lv() above.
  const resolvedCode = useCallback(
    (code: string): string => {
      if (!code.startsWith('annuals__')) return code;
      const qCode = 'quarterly__' + code.slice('annuals__'.length);
      const quarterly = latestObservation(metrics, qCode);
      return quarterly ? qCode : code;
    },
    [metrics],
  );

  const cadenceFor = useCallback(
    (code: string): Cadence | null => observationCadence(metrics, resolvedCode(code)),
    [metrics, resolvedCode],
  );

  // Derived: FCF / Net Income
  const fcfOverNi = useMemo(() => {
    const fcf = lv(MC.FCF);
    const ni = lv(MC.NET_INCOME);
    if (!fcf || !ni || fcf.value == null || ni.value == null || ni.value === 0) return null;
    return fcf.value / ni.value;
  }, [lv]);

  // Derived: Interest Coverage. GuruFocus's pre-computed
  // `Valuation and Quality > Interest Coverage` is sparse on cash-rich /
  // low-debt names (e.g. AAPL — GF returns "N/A" for the most recent quarters
  // because the ratio is huge or undefined). With null rows now stored, the
  // GF fallback at least dates correctly to the latest period; for a real
  // numeric we recompute from raw Operating Income / Interest Expense so the
  // row matches Debt-to-Equity's freshness whenever both components are
  // populated.
  const interestCoverage = useMemo<{ value: number | null; date: string; computed: boolean } | null>(() => {
    const op = lv(MC.OPERATING_INCOME);
    const ie = lv(MC.INTEREST_EXPENSE);
    // Compute only when both raw fields have numeric values for their period.
    if (op?.value != null && ie?.value != null && Math.abs(ie.value) > 0) {
      const value = op.value / Math.abs(ie.value);
      const date = op.date < ie.date ? op.date : ie.date;
      return { value, date, computed: true };
    }
    // Fall back to GF's pre-computed series. `value` may be null when GF
    // reported "N/A" for the most recent period — the row will render as
    // "—" with that period's date instead of a stale numeric.
    const fallback = lv(MC.INTEREST_COVERAGE);
    return fallback ? { value: fallback.value, date: fallback.date, computed: false } : null;
  }, [lv]);

  // Derived: Value Creation + Historical Growth metrics, computed from GuruFocus time series.
  //
  // Revenue and FCF flow series prefer the quarterly twin's TTM (so the
  // latest data point lands at the most recent reported quarter end, not
  // the latest fiscal-year close). Falls back to annuals when fewer than
  // 5 quarterly points are available. The TTM construction means each
  // point covers a full year of activity, so the CAGR / R² / SD math
  // stays apples-to-apples with the annual version — just shifted to a
  // freshly-anchored sliding window.
  const valueGrowth = useMemo(() => {
    const priceSeries = timeSeries(metrics, 'close_price');
    const revSeries = flowSeriesPreferQuarterlyTTM(metrics, MC.REVENUE);
    const fcfSeries = flowSeriesPreferQuarterlyTTM(metrics, MC.FCF);

    const price5Y = trailingYearsWindow(priceSeries, 5);
    const price10Y = trailingYearsWindow(priceSeries, 10);
    const rev5Y = trailingYearsWindow(revSeries, 5);
    const fcf5Y = trailingYearsWindow(fcfSeries, 5);

    const lastDate = (s: { date: string }[]) => (s.length ? s[s.length - 1].date : null);

    // For the FCF SD: if we ended up with quarterly TTM we have ~16+ data
    // points and use 4-quarter-lag YoY rates; if we fell back to annual we
    // use the annual YoY rates.
    const fcfYoY = fcf5Y.length >= 8 ? ttmYoYGrowthRates(fcf5Y) : yoyGrowthRates(fcf5Y);

    return {
      price5YCAGR: computeCAGR(price5Y),
      price5YR2: logLinearR2(price5Y),
      price10YCAGR: computeCAGR(price10Y),
      price10YR2: logLinearR2(price10Y),
      priceDate: lastDate(priceSeries),
      rev5YCAGR: computeCAGR(rev5Y),
      rev5YR2: logLinearR2(rev5Y),
      revDate: lastDate(revSeries),
      fcf5YCAGR: computeCAGR(fcf5Y),
      fcf5YR2: logLinearR2(fcf5Y),
      fcfGrowthSD: stdDev(fcfYoY),
      fcfDate: lastDate(fcfSeries),
    };
  }, [metrics]);

  type StatRow = { label: string; value: string; date?: string | null; info?: string; cadence?: Cadence | null; source: EarningsSource };

  // Cadence sources for the computed Value-Creation / Historical-Growth rows.
  // The displayed metric is derived (CAGR, R², SD), but the user wants to know
  // how often the underlying observations land — i.e. is this 5Y growth built
  // from 5 annual points or 20 quarterly ones?
  const priceCadence = useMemo(() => observationCadence(metrics, 'close_price'), [metrics]);
  const revenueCadence = cadenceFor(MC.REVENUE);
  const fcfCadence = cadenceFor(MC.FCF);

  // For the computed Interest Coverage row: report the slower of the two
  // anchor cadences (Operating Income vs Interest Expense). When neither
  // component is available we fall back to GuruFocus's pre-computed series
  // and report its cadence directly.
  const interestCoverageCadence = useMemo<Cadence | null>(() => {
    if (interestCoverage?.computed) {
      const opC = cadenceFor(MC.OPERATING_INCOME);
      const ieC = cadenceFor(MC.INTEREST_EXPENSE);
      if (!opC) return ieC ?? null;
      if (!ieC) return opC;
      return (opC.medianDays ?? 0) >= (ieC.medianDays ?? 0) ? opC : ieC;
    }
    return cadenceFor(MC.INTEREST_COVERAGE);
  }, [interestCoverage, cadenceFor]);

  const leftSections: { title: string; rows: StatRow[] }[] = [
    {
      title: 'Balance Sheet',
      rows: [
        { label: 'Interest Coverage', value: fmtNum(interestCoverage?.value ?? null), date: interestCoverage?.date, cadence: interestCoverageCadence, source: 'financials', info: `EBIT / Interest Expense. Higher = more capacity to service debt. Below 3 is a warning sign. ${interestCoverage?.computed ? 'Computed from quarterly Operating Income ÷ Interest Expense — uses GuruFocus\'s pre-computed series only when raw fields are missing.' : 'Sourced from GuruFocus\'s Valuation & Quality > Interest Coverage (raw Income Statement components weren\'t both populated).'}` },
        { label: 'Debt / Equity', value: fmtNum(lv(MC.DEBT_TO_EQUITY)?.value ?? null), date: lv(MC.DEBT_TO_EQUITY)?.date, cadence: cadenceFor(MC.DEBT_TO_EQUITY), source: 'financials', info: 'Total Debt / Total Equity. Lower = less leverage. Above 2 warrants scrutiny.' },
      ],
    },
    {
      title: 'Capital Intensity',
      rows: [
        { label: 'CAPEX / Revenue', value: fmtNum(lv(MC.CAPEX_TO_REV)?.value ?? null), date: lv(MC.CAPEX_TO_REV)?.date, cadence: cadenceFor(MC.CAPEX_TO_REV), source: 'financials', info: 'Capital expenditure as a share of revenue. Lower = more capital-light business model.' },
        { label: 'CAPEX / OCF', value: fmtNum(lv(MC.CAPEX_TO_OCF)?.value ?? null), date: lv(MC.CAPEX_TO_OCF)?.date, cadence: cadenceFor(MC.CAPEX_TO_OCF), source: 'financials', info: 'Capital expenditure as a share of operating cash flow. Lower = more cash left after reinvestment.' },
      ],
    },
    {
      title: 'Capital Allocation',
      rows: [
        { label: 'ROE', value: fmtPctPoints(lv(MC.ROE)?.value ?? null), date: lv(MC.ROE)?.date, cadence: cadenceFor(MC.ROE), source: 'financials', info: 'Return on Equity = Net Income / Shareholders\' Equity. Measures profit generated per dollar of equity.' },
        { label: 'ROIC', value: fmtPctPoints(lv(MC.ROIC)?.value ?? null), date: lv(MC.ROIC)?.date, cadence: cadenceFor(MC.ROIC), source: 'financials', info: 'Return on Invested Capital = NOPAT / Invested Capital. Measures efficiency of all capital deployed.' },
      ],
    },
    {
      title: 'Value Creation',
      rows: [
        { label: 'Price 5Y CAGR', value: fmtPct(valueGrowth.price5YCAGR), date: valueGrowth.priceDate, cadence: priceCadence, source: 'prices', info: 'Compound Annual Growth Rate of share price over the last 5 years, from GuruFocus daily close prices.' },
        { label: 'Price 5Y R²', value: fmtNum(valueGrowth.price5YR2), date: valueGrowth.priceDate, cadence: priceCadence, source: 'prices', info: 'R-squared of log-linear regression of share price vs time over the last 5 years. Higher = more consistent growth.' },
        { label: 'Price 10Y CAGR', value: fmtPct(valueGrowth.price10YCAGR), date: valueGrowth.priceDate, cadence: priceCadence, source: 'prices', info: 'Compound Annual Growth Rate of share price over the last 10 years, from GuruFocus daily close prices.' },
        { label: 'Price 10Y R²', value: fmtNum(valueGrowth.price10YR2), date: valueGrowth.priceDate, cadence: priceCadence, source: 'prices', info: 'R-squared of log-linear regression of share price vs time over the last 10 years. Higher = more consistent growth.' },
      ],
    },
  ];

  const rightSections: { title: string; rows: StatRow[] }[] = [
    {
      title: 'Profitability',
      rows: [
        { label: 'Gross Margin', value: fmtPctPoints(lv(MC.GROSS_MARGIN)?.value ?? null), date: lv(MC.GROSS_MARGIN)?.date, cadence: cadenceFor(MC.GROSS_MARGIN), source: 'financials', info: 'Gross Profit / Revenue. Indicates pricing power and cost of goods sold efficiency.' },
        { label: 'Net Margin', value: fmtPctPoints(lv(MC.NET_MARGIN)?.value ?? null), date: lv(MC.NET_MARGIN)?.date, cadence: cadenceFor(MC.NET_MARGIN), source: 'financials', info: 'Net Income / Revenue. Bottom-line profitability after all expenses.' },
        { label: 'FCF / Net Income', value: fmtPct(fcfOverNi), date: lv(MC.FCF)?.date, cadence: cadenceFor(MC.FCF), source: 'financials', info: 'Free Cash Flow / Net Income. Above 1 means cash earnings exceed accounting earnings (high quality).' },
      ],
    },
    {
      title: 'Historical Growth',
      rows: [
        { label: 'Revenue 5Y Growth', value: fmtPct(valueGrowth.rev5YCAGR), date: valueGrowth.revDate, cadence: revenueCadence, source: 'financials', info: '5-year revenue CAGR. Anchored to the latest quarter\'s trailing-twelve-months revenue when quarterly data is available, otherwise to annual revenue.' },
        { label: 'Revenue R²', value: fmtNum(valueGrowth.rev5YR2), date: valueGrowth.revDate, cadence: revenueCadence, source: 'financials', info: 'R-squared of log-linear regression of TTM revenue vs time over the last 5 years. Higher = more consistent growth.' },
        { label: 'FCF 5Y Growth', value: fmtPct(valueGrowth.fcf5YCAGR), date: valueGrowth.fcfDate, cadence: fcfCadence, source: 'financials', info: '5-year FCF CAGR. Same TTM-quarterly construction as Revenue. Null if FCF was negative at either endpoint.' },
        { label: 'FCF Growth R²', value: fmtNum(valueGrowth.fcf5YR2), date: valueGrowth.fcfDate, cadence: fcfCadence, source: 'financials', info: 'R-squared of log-linear regression of TTM FCF vs time over the last 5 years. Null if any FCF in the window was non-positive.' },
        { label: 'FCF Growth SD', value: fmtPct(valueGrowth.fcfGrowthSD), date: valueGrowth.fcfDate, cadence: fcfCadence, source: 'financials', info: 'Standard deviation of 4-quarter-lag TTM FCF growth rates over the last 5 years (or annual YoY rates when quarterly data is sparse). Lower = more predictable.' },
      ],
    },
    {
      title: 'Outlook',
      rows: [
        { label: 'EPS LT Growth EST', value: fmtPctPoints(lv(MC.EPS_EST)?.value ?? null), date: lv(MC.EPS_EST)?.date, cadence: cadenceFor(MC.EPS_EST), source: 'analyst_estimates', info: 'Analyst consensus long-term EPS growth rate estimate (3-5 years forward).' },
      ],
    },
    {
      title: 'Valuation',
      rows: [
        { label: 'Forward P/E', value: fmtNum(lv(MC.FWD_PE)?.value ?? null), date: lv(MC.FWD_PE)?.date, cadence: cadenceFor(MC.FWD_PE), source: 'indicators', info: 'Price / Forward EPS estimate. Lower = cheaper relative to expected earnings.' },
        { label: 'PEG', value: fmtNum(lv(MC.PEG)?.value ?? null), date: lv(MC.PEG)?.date, cadence: cadenceFor(MC.PEG), source: 'financials', info: 'P/E / EPS Growth Rate. Below 1 suggests undervalued relative to growth; above 2 may be expensive.' },
      ],
    },
  ];

  function renderColumn(sections: { title: string; rows: StatRow[] }[]) {
    return (
      <div className="space-y-4">
        {sections.map((sec) => (
          <div key={sec.title}>
            <div className="text-gray-400 text-xs font-semibold uppercase tracking-wider mb-2">{sec.title}</div>
            <div className="space-y-1">
              {sec.rows.map((r) => (
                <div key={r.label} className="flex justify-between items-center">
                  <span className="text-gray-400 text-sm flex items-center gap-1.5">
                    {r.label}
                    {r.info && <InfoTip text={r.info} />}
                  </span>
                  <span className="text-right">
                    <span className="inline-flex items-center justify-end gap-1.5">
                      {refreshingSources?.has(r.source) && <Spinner size={10} />}
                      <span className="text-white font-mono text-sm">{r.value}</span>
                      {r.cadence && <InfoTip text={cadenceHoverText(r.cadence)} />}
                    </span>
                    {r.date && (
                      <span className="block text-gray-500 text-[10px] font-mono">{r.date}</span>
                    )}
                  </span>
                </div>
              ))}
            </div>
          </div>
        ))}
      </div>
    );
  }

  return (
    <div className="grid grid-cols-2 gap-8">
      {renderColumn(leftSections)}
      {renderColumn(rightSections)}
    </div>
  );
}
