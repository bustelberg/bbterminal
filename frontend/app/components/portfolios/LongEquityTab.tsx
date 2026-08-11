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
/** The indices a chart can be measured against — the same three the /benchmarks panel rebuilds.
 *  ⚠ Their coverage differs a lot (SP500 is the best-ingested), and a thinly-covered index makes a
 *  confident-looking line over a fraction of itself; each card states the coverage it drew. */
const BENCHMARKS = ['SP500', 'ACWI', 'AEX'];

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
  /**
   * The cadence every chart on this tab is on.
   *
   * ⚠ "Quarterly" IS TRAILING TWELVE MONTHS, NOT RAW QUARTERS — quarterly frequency with annual
   * scope. Raw quarters would put a seasonal sawtooth through revenue, margins and cash
   * conversion, and the growth cards' trend line would fit the season rather than the business.
   * The roll-up is per metric and lives on the SERVER (`_TTM_RULE`): flows sum over four quarters,
   * balances take the latest, and an already-annualised rate takes the mean. Summing four
   * quarter-end balance sheets would report a company with 4x its assets, and nothing on the
   * resulting chart would look wrong.
   */
  const [cadence, setCadence] = useState<'annual' | 'quarterly'>('annual');
  /**
   * The benchmark drawn beside every chart, or null for none.
   *
   * ⚠ IT IS THE SAME REQUEST AS THE PORTFOLIO'S, WITH `universe` INSTEAD OF `holdings` — so each
   * card computes the benchmark line with the identical helper it runs over the book. There is no
   * second implementation of "FCF-SBC margin" anywhere, which is the only way the two lines on one
   * chart can be guaranteed to mean the same thing. The index arrives cap-weighted; the card's
   * existing weighted average does the rest.
   */
  const [benchmark, setBenchmark] = useState<string | null>(null);
  /** ⚠ Memoised for the same reason `holdingsTarget` is — it is an effect dep in twelve cards. */
  const benchTarget = useMemo(
    () => (benchmark ? { universe: benchmark, cadence } : null), [benchmark, cadence]);

  // ⚠ Memoised — it's a card/modal effect dep, so a fresh object each render would refetch forever.
  // ⚠ `cadence` RIDES IN THE BODY, which is what makes one toggle move nine cards: every derived
  // card POSTs this object verbatim to its own `*-inputs` endpoint, and they all read their lines
  // through one cadence-aware loader on the server. Nothing per-card to keep in step.
  const holdingsTarget = useMemo(() => (isAgg
    ? (basket
      ? { holdings: basket.holdings.map((h) => ({ isin: h.isin, name: h.name, weight: h.weight })), cadence }
      : { portfolio_id: portfolioId, cadence })
    : { holdings: [{ isin: isin ?? '', weight: 1 }], cadence }), [isAgg, basket, portfolioId, isin, cadence]);

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
            { basket, portfolioId, cadence }, (p) => { if (alive) setProgress(p); },
          );
          if (!alive) return;
          if (out.kind === 'none') { setData({ metrics: [] }); return; }
          if (out.kind === 'error') { setErr(out.message); return; }
          setData(out.data);
          return;
        }
        const r = await apiFetch(`${API_URL}/api/earnings/by-isin/${encodeURIComponent(isin ?? '')}`
          + `/metrics?cadence=${cadence}`);
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
  }, [isin, isAgg, basket, portfolioId, reloadKey, metricsKey, cadence]);

  /**
   * The benchmark's metrics for the three GROWTH cards — ONE blend for all three, exactly as the
   * book's own metrics are fetched once above.
   *
   * ⚠ The nine derived cards do NOT come through here: each owns its `*-inputs` endpoint and
   * fetches the index from it directly (`useBenchInputs`), because that is the only way each
   * benchmark line is computed by the same helper as the portfolio line beside it. This fetch is
   * the growth cards' equivalent, not a second source for anyone else.
   *
   * Silent on failure: the benchmark is an overlay on charts that already work.
   */
  const [benchMetrics, setBenchMetrics] = useState<MetricRow[] | null>(null);
  const [benchErr, setBenchErr] = useState<string | null>(null);
  useEffect(() => {
    let alive = true;
    void (async () => {
      setBenchMetrics(null); setBenchErr(null);
      if (!benchmark) return;
      try {
        const r = await apiFetch(`${API_URL}/api/earnings/fundamental-blend-metrics`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          // ⚠ NAME THE THREE METRICS. Unnamed, the blend reads every charted code per constituent —
          // three paged requests each, i.e. ~1,500 round trips for the S&P. Named, it is one
          // chunked query per metric. See the request model's `metrics` field.
          body: JSON.stringify({
            universe: benchmark, cadence, metrics: CARDS.map((c) => c.benchmarkMetric),
          }),
        });
        const b = await r.json().catch(() => null);
        if (!alive) return;
        if (!r.ok) {
          const detail = (b?.detail as string) ?? `HTTP ${r.status}`;
          console.warn(`[bb:bench] blend ${benchmark}: ${detail}`, b);
          setBenchErr(detail);
          return;
        }
        setBenchMetrics((b as MetricsResponse)?.metrics ?? []);
      } catch (e) {
        const detail = e instanceof Error ? e.message : String(e);
        console.warn(`[bb:bench] blend ${benchmark}: ${detail}`, e);
        if (alive) setBenchErr(detail);
      }
    })();
    return () => { alive = false; };
  }, [benchmark, cadence]);

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
    ingestIsin, onIngested, onReloadMetrics, cadence,
    benchMetrics, benchLabel: benchmark, benchErr,
  };
  // ⚠ ONE KEY SUFFIX FOR EVERY DERIVED CARD. They each own their fetch, so without the cadence in
  // the key a switch would leave twelve charts showing the previous basis until something else
  // re-keyed them — and the toggle would look broken on the cards that matter most.
  const ck = `${reloadKey}-${cadence}`;
  return (
    <>
    <div className="flex items-center gap-2 mb-3 text-[11px]">
      <span className="text-fg-faint">Periods</span>
      <div className="inline-flex rounded-lg border border-neutral-800/40 overflow-hidden">
        {([
          ['annual', 'Annual', 'One point per fiscal year.'],
          ['quarterly', 'Quarterly',
            'One point per quarter, each the TRAILING TWELVE MONTHS — quarterly frequency with '
            + 'annual scope. Flows sum the last four quarters, balances take the latest, and an '
            + 'already-annualised rate takes their mean. Raw quarters would put a seasonal '
            + 'sawtooth through revenue and every margin built on it.'],
        ] as const).map(([k, label, note]) => (
          <button key={k} type="button" onClick={() => setCadence(k)} title={note}
            aria-pressed={cadence === k}
            className={`cursor-pointer px-2 py-1 transition-colors ${cadence === k
              ? 'bg-accent-600 text-white'
              : 'text-fg-subtle hover:bg-overlay/5'}`}>
            {label}
          </button>
        ))}
      </div>
      {cadence === 'quarterly' && (
        <span className="text-fg-faint">
          trailing 12 months — a Q4 point equals that fiscal year
        </span>
      )}
      {/* ⚠ ONE CONTROL FOR TWELVE CHARTS. Per-card benchmark pickers would let two charts on one
          screen be measured against different indices — a comparison a reader cannot arbitrate,
          and the same failure the tab-wide cadence toggle avoids. */}
      <label className="flex items-center gap-1.5 ml-4 cursor-pointer text-fg-faint">
        <input type="checkbox" className="cursor-pointer"
          checked={benchmark != null}
          onChange={(e) => setBenchmark(e.target.checked ? BENCHMARKS[0] : null)} />
        Benchmark
      </label>
      {benchmark != null && (
        <select value={benchmark} onChange={(e) => setBenchmark(e.target.value)}
          aria-label="Benchmark"
          title={'The index each chart is measured against. Its constituents are cap-weighted and '
            + 'run through the SAME formula as the portfolio, so the two lines are comparable. '
            + 'Only constituents whose fundamentals are ingested contribute — the coverage floor '
            + 'applies to the index exactly as it does to the book.'}
          className="cursor-pointer bg-page border border-neutral-700 rounded-lg px-2 py-0.5 text-[11px] font-mono text-fg focus:border-accent-500">
          {BENCHMARKS.map((b) => <option key={b} value={b}>{b}</option>)}
        </select>
      )}
    </div>
    <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4">
      <MetricGrowthCard key={revenue.title} cfg={revenue}
        {...growth} />
      <MetricGrowthCard key={fcfPs.title} cfg={fcfPs}
        {...growth} />
      {/* Derived cards fetch their own inputs; re-key on reload so an ingest repopulates them too. */}
      <MarginCard key={`margin-${ck}`} benchTarget={benchTarget} holdingsTarget={holdingsTarget} holdingsName={gName} sbcCorrection={sbcCorrection} />
      <CashReturnCard key={`cashret-${ck}`} benchTarget={benchTarget} holdingsTarget={holdingsTarget} holdingsName={gName} sbcCorrection={sbcCorrection} />
      <DebtRatioCard key={`debt-${ck}`} benchTarget={benchTarget} holdingsTarget={holdingsTarget} holdingsName={gName} />
      <InterestBurdenCard key={`intburden-${ck}`} benchTarget={benchTarget} holdingsTarget={holdingsTarget} holdingsName={gName} />
      <MetricGrowthCard key={shares.title} cfg={shares}
        {...growth} />
      <SbcOcfCard key={`sbcocf-${ck}`} benchTarget={benchTarget} holdingsTarget={holdingsTarget} holdingsName={gName} />
      <InvestedCapitalCard key={`invcap-${ck}`} benchTarget={benchTarget} holdingsTarget={holdingsTarget} holdingsName={gName} isAgg={isAgg} />
      <CapexMarginCard key={`capex-${ck}`} benchTarget={benchTarget} holdingsTarget={holdingsTarget} holdingsName={gName} />
      <DividendYieldCard key={`divyield-${ck}`} benchTarget={benchTarget} holdingsTarget={holdingsTarget} holdingsName={gName} />
      <FcfSbcYieldCard key={`fcfsbcyield-${ck}`} benchTarget={benchTarget} holdingsTarget={holdingsTarget} holdingsName={gName} sbcCorrection={sbcCorrection} />
      {/* Last on the tab, as asked. Gross margin is the cleanest read on pricing power, and it is
          the one card here a bank simply cannot have — see GrossMarginCard. */}
      <GrossMarginCard key={`grossmargin-${ck}`} benchTarget={benchTarget} holdingsTarget={holdingsTarget} holdingsName={gName} />
      {/* Directly after gross margin: the two halves of "are these earnings real" — what the sale
          leaves after direct cost, then whether the resulting profit turns into money. */}
      <CashConversionCard key={`cashconv-${ck}`} benchTarget={benchTarget} holdingsTarget={holdingsTarget} holdingsName={gName} sbcCorrection={sbcCorrection} />
    </div>
    </>
  );
}
