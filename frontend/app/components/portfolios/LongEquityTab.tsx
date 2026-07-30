'use client';

import { useEffect, useMemo, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { blendLoadingLabel, loadBlendMetrics, type BlendProgress } from './blendMetrics';
import MetricGrowthCard, { type MetricCfg } from './MetricGrowthCard';
import MarginCard from './MarginCard';
import CashReturnCard from './CashReturnCard';
import DebtRatioCard from './DebtRatioCard';
import InterestBurdenCard from './InterestBurdenCard';
import SbcOcfCard from './SbcOcfCard';
import InvestedCapitalCard from './InvestedCapitalCard';
import CapexMarginCard from './CapexMarginCard';
import GrossMarginCard from './GrossMarginCard';
import CashConversionCard from './CashConversionCard';
import FcfSbcYieldCard from './FcfSbcYieldCard';
import DividendYieldCard from './DividendYieldCard';
import { type BlendNote } from './blendNotes';

/**
 * The "Long Equity" tab: a grid of growth cards (Revenue, FCF/share, …), each a
 * {@link MetricGrowthCard} — the metric per fiscal year on a log axis with an exponential-trend
 * R²/CAGR, the benchmark's same-metric growth, and a per-holding drill-down. The company's metrics
 * are fetched ONCE here (they carry every code) and handed to each card, which extracts its own.
 */

type MetricRow = { metric_code: string; target_date: string; numeric_value: number | null };
type MetricsResponse = {
  currency?: string | null; company_name?: string | null; metrics: MetricRow[];
  // Portfolio only: per metric_code, why a code the holdings DO carry produced no blended line.
  // See `blendNotes` / the backend's `explain_empty`.
  blend_notes?: Record<string, BlendNote>;
};

// Each card is one metric. `codes` carries BOTH GuruFocus section spellings (see the backend's
// `_METRIC_CODES`); `benchmarkMetric` is the `metric` param for the benchmark + holdings endpoints.
const CARDS: MetricCfg[] = [
  {
    title: 'Revenue', noun: 'revenue', unit: 'millions', kind: 'growth', benchmarkMetric: 'revenue',
    codes: ['annuals__Income Statement__Revenue', 'annuals__income_statement__Revenue'],
  },
  {
    title: 'FCF / share', noun: 'FCF/share', unit: 'per_share', kind: 'growth', benchmarkMetric: 'fcf_ps',
    codes: ['annuals__Per Share Data__Free Cash Flow per Share',
      'annuals__per_share_data__Free Cash Flow per Share'],
  },
  {
    // A count, not currency (no ccy prefix). CAGR reads as the buyback (−) / dilution (+) rate.
    // Uses the INCOME STATEMENT spelling — the only one both cohorts share (see backend `shares`).
    title: 'Shares outstanding', noun: 'shares outstanding', unit: 'shares', kind: 'growth', benchmarkMetric: 'shares',
    codes: ['annuals__Income Statement__Shares Outstanding (Diluted Average)',
      'annuals__income_statement__Shares Outstanding (Diluted Average)'],
  },
  // ⚠ NO "DIVIDEND / SHARE" CARD — the dividend is reported as a YIELD, for a company and for a
  // portfolio alike (`DividendYieldCard`). A per-share amount has no portfolio-level meaning: there
  // is no portfolio share, the amounts sit in different currencies, and the level rule rebases each
  // holding to 100 at its first year, which a dividend series starting at 0.00 cannot survive — so
  // the portfolio card read "No dividend/share ingested" while every holding carried the line.
];

export default function LongEquityTab({ isin, name, basket, portfolioId, sbcCorrection = true }: {
  isin?: string;
  name?: string | null;
  basket?: { holdings: { isin: string; weight: number; name?: string }[] };
  portfolioId?: number;
  /** ⚠ OWNED BY THE MODAL, NOT HERE — its checkbox lives in the tab row, which is in the fixed
   *  head and therefore always visible. Governs the four charts whose numerator is FCF. */
  sbcCorrection?: boolean;
}) {
  const isAgg = !!basket || portfolioId != null;
  const [data, setData] = useState<MetricsResponse | null>(null);
  const [err, setErr] = useState<string | null>(null);
  // Bumped after an empty card ingests this company's financials — reloads the metrics (repopulates
  // every growth card) and re-keys the derived cards so they refetch their inputs too.
  const [reloadKey, setReloadKey] = useState(0);
  // ⚠ A SECOND, NARROWER KEY. The growth cards all read ONE metrics fetch, while each derived card
  // owns its own endpoint and refetches on a re-key — so bumping `reloadKey` to refresh one chart
  // reloads twelve. This one refetches the metrics only; nothing else moves.
  const [metricsKey, setMetricsKey] = useState(0);
  /** Portfolio only: how many holdings the blend has read. Null on a single company. */
  const [progress, setProgress] = useState<BlendProgress | null>(null);

  // ⚠ Memoised — it's a card/modal effect dep, so a fresh object each render would refetch forever.
  const holdingsTarget = useMemo(() => (isAgg
    ? (basket
      ? { holdings: basket.holdings.map((h) => ({ isin: h.isin, name: h.name, weight: h.weight })) }
      : { portfolio_id: portfolioId })
    : { holdings: [{ isin: isin ?? '', weight: 1 }] }), [isAgg, basket, portfolioId, isin]);

  useEffect(() => {
    let alive = true;
    void (async () => {
      setErr(null); setData(null); setProgress(null);
      try {
        // A single company is keyed by ISIN; a portfolio is the blended pseudo-company (each metric
        // blends as a LEVEL → a growth index). Both return every metric code. The portfolio path
        // streams per-company progress — it is a read per holding — via the SAME loader
        // `FundamentalCharts` uses, so the two tabs cannot come to load the blend differently.
        if (isAgg) {
          const out = await loadBlendMetrics<MetricsResponse>(
            { basket, portfolioId }, (p) => { if (alive) setProgress(p); },
          );
          if (!alive) return;
          if (out.kind === 'none') { setData({ metrics: [] }); return; }
          if (out.kind === 'error') { setErr(out.message); return; }
          setData(out.data);
          return;
        }
        const r = await apiFetch(`${API_URL}/api/earnings/by-isin/${encodeURIComponent(isin ?? '')}/metrics`);
        if (r.status === 404) { if (alive) setData({ metrics: [] }); return; }
        const b = await r.json().catch(() => null);
        if (!alive) return;
        if (!r.ok) { setErr(b?.detail ?? `HTTP ${r.status}`); return; }
        setData(b as MetricsResponse);
      } catch (e) {
        if (alive) setErr(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { alive = false; };
  }, [isin, isAgg, basket, portfolioId, reloadKey, metricsKey]);

  if (err) return <p className="text-xs text-neg-300 py-16 text-center">{err}</p>;

  // ⚠ ONE COUNT FOR THE WHOLE GRID, NOT TWELVE. Every card below reads this one metrics fetch (or
  // fires its own once it lands), so twelve boxes each saying "Loading…" showed a wait twelve times
  // over and none of them could say how far along it was. While the blend is running the grid is a
  // single line that can.
  if (isAgg && !data) {
    return <p className="text-xs text-fg-subtle py-16 text-center">{blendLoadingLabel(progress)}</p>;
  }

  // Fixed order across the grid: Revenue, FCF/share, FCF-SBC margin, Cash return on capital,
  // Debt / assets ex-GW, Interest / op. profit, Shares outstanding, SBC / OCF, Invested capital,
  // Capex margin, Dividend yield, FCF-SBC yield.
  const [revenue, fcfPs, shares] = CARDS;
  // Single-company only: an empty growth card can fetch this company's financials, then reload.
  const ingestIsin = isAgg ? undefined : isin;
  const onIngested = () => setReloadKey((k) => k + 1);
  const onReloadMetrics = () => setMetricsKey((k) => k + 1);
  const gName = name ?? data?.company_name;
  const growth = {
    metrics: data?.metrics ?? null, isAgg, currency: data?.currency,
    blendNotes: data?.blend_notes, holdingsTarget, holdingsName: gName,
    ingestIsin, onIngested, onReloadMetrics,
  };
  return (
    <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4">
      <MetricGrowthCard key={revenue.title} cfg={revenue}
        {...growth} />
      <MetricGrowthCard key={fcfPs.title} cfg={fcfPs}
        {...growth} />
      {/* Derived cards fetch their own inputs; re-key on reload so an ingest repopulates them too. */}
      <MarginCard key={`margin-${reloadKey}`} holdingsTarget={holdingsTarget} holdingsName={gName} sbcCorrection={sbcCorrection} />
      <CashReturnCard key={`cashret-${reloadKey}`} holdingsTarget={holdingsTarget} holdingsName={gName} sbcCorrection={sbcCorrection} />
      <DebtRatioCard key={`debt-${reloadKey}`} holdingsTarget={holdingsTarget} holdingsName={gName} />
      <InterestBurdenCard key={`intburden-${reloadKey}`} holdingsTarget={holdingsTarget} holdingsName={gName} />
      <MetricGrowthCard key={shares.title} cfg={shares}
        {...growth} />
      <SbcOcfCard key={`sbcocf-${reloadKey}`} holdingsTarget={holdingsTarget} holdingsName={gName} />
      <InvestedCapitalCard key={`invcap-${reloadKey}`} holdingsTarget={holdingsTarget} holdingsName={gName} isAgg={isAgg} />
      <CapexMarginCard key={`capex-${reloadKey}`} holdingsTarget={holdingsTarget} holdingsName={gName} />
      <DividendYieldCard key={`divyield-${reloadKey}`} holdingsTarget={holdingsTarget} holdingsName={gName} />
      <FcfSbcYieldCard key={`fcfsbcyield-${reloadKey}`} holdingsTarget={holdingsTarget} holdingsName={gName} sbcCorrection={sbcCorrection} />
      {/* Last on the tab, as asked. Gross margin is the cleanest read on pricing power, and it is
          the one card here a bank simply cannot have — see GrossMarginCard. */}
      <GrossMarginCard key={`grossmargin-${reloadKey}`} holdingsTarget={holdingsTarget} holdingsName={gName} />
      {/* Directly after gross margin: the two halves of "are these earnings real" — what the sale
          leaves after direct cost, then whether the resulting profit turns into money. */}
      <CashConversionCard key={`cashconv-${reloadKey}`} holdingsTarget={holdingsTarget} holdingsName={gName} sbcCorrection={sbcCorrection} />
    </div>
  );
}
