'use client';

import { useEffect, useMemo, useState } from 'react';
import { createPortal } from 'react-dom';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { blendLoadingLabel } from './blendMetrics';
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
import { useFundamentalChromeCopy } from './fundamentalChromeCopy';
import { type MemberCount } from './memberCounts';
import { benchBody, type BenchTarget } from './benchSeries';
import type { ChartStatistics } from './chartStatistics';
import { type Basket } from './types';
import { GraphCoverageProvider } from './GraphCoverage';
import { useBenchmarkRepair } from './benchmarkRepair';

/**
 * The "Long Equity" tab: a grid of growth cards (Revenue, FCF/share, …), each a
 * {@link MetricGrowthCard} — the metric per fiscal year on a log axis with an exponential-trend
 * R²/CAGR, the benchmark's same-metric growth, and a per-holding drill-down. The company's metrics
 * are fetched ONCE here (they carry every code) and handed to each card, which extracts its own.
 */

type MetricRow = { metric_code: string; target_date: string; numeric_value: number | null };
type MetricsResponse = {
  chart_statistics?: Record<string, ChartStatistics>;
  currency?: string | null; company_name?: string | null; metrics: MetricRow[];
  // Portfolio only: per metric_code, why a code the holdings DO carry produced no blended line.
  // See `blendNotes` / the backend's `explain_empty`.
  blend_notes?: Record<string, BlendNote>;
  /** Per metric code, how many members the line was drawn from, how many were on offer, and the
   *  `rule` that explains the gap.
   *   Different from `coverage`, which answers "how many hold this metric at all" — a positives-
   *  only metric withholds members deliberately, and a euro-summed one leaves out any member whose
   *  euros could not be built. See `memberCounts`. */
  member_counts?: Record<string, MemberCount>;
  /** The common fundamentals baseline shown on every Graphs card. */
  coverage?: {
    holdings?: number; member_count_total?: number;
    rows?: { reason?: string | null }[];
  };
};

// Each card is one metric. `codes` carries BOTH GuruFocus section spellings (see the backend's
// `_METRIC_CODES`); `benchmarkMetric` is the `metric` param for the benchmark + holdings endpoints.
/** The indices a chart can be measured against — the same three the /benchmarks panel rebuilds.
 *   Their coverage differs a lot (SP500 is the best-ingested), and a thinly-covered index makes a
 *  confident-looking line over a fraction of itself; each card states the coverage it drew. */
const BENCHMARKS = ['SP500', 'ACWI', 'AEX'];

/** The select value that means "the company the caller handed me", not an index.
 *   A sentinel, not an ISIN. An ISIN in the option value would collide the day an index is ever
 *  labelled with one, and it would put an identifier in a control whose other values are names. */
const COMPARE_VALUE = '__compare__';

export const CARDS: MetricCfg[] = [
  {
    /**
     *  First on the tab — the weighted share-price increase of the book and of the index. Every
     * other card on this grid explains a return; this one IS it, so it leads and the explanation
     * follows.
     *
     *  It needed no new endpoint, no new blend rule and no new chart, and that is not a
     * COINCIDENCE — it is the tab's design paying out. A price is a LEVEL in each company's own
     * units, exactly like Revenue or FCF/share, so `_fundamental_blend` already aggregates it by
     * the rule this card wants: the line is CHAINED from weighted growth,
     *
     *     index[p] = index[anchor] x (1 + Σ w_i·g_i / Σ w_i),   g_i = P_i(p)/P_i(anchor) − 1
     *
     * which is the definition of "the weighted increase on the stocks in it". The benchmark line
     * is the identical computation over the index's cap-weighted constituents (`benchBody` swaps
     * `holdings` for `universe`), so the two lines on this chart cannot mean different things.
     *
     *  It is a price return in each company's reporting currency — not a EUR total return, and
     * It will not reconcile with the analyse modal. Three deliberate differences, every one of
     * which moves the number: dividends are excluded (a price index, not total return); the FX leg
     * is excluded (each g_i divides out its own currency, so a US holding's euro move is not in
     * it — see the EUR-basis rule in CLAUDE.md, which governs the RETURN surfaces, not this tab);
     * and the weights are the book's current weights held flat across history rather than a
     * traded position. So this answers "how did these businesses' shares do" on the same axis as
     * their revenue and earnings — it is deliberately NOT the portfolio's performance, which
     * `period_return_pct` owns and nothing here may restate.
     *
     *  The benchmark line here is the constituent rebuild, and it still reads high. Measured
     * 2026-08-21 after the anchor-weighting fix (`blend_series`, which took a decade of ACWI from a
     * fabricated +20.21%/yr to +11.14%/yr): ACWI +11.14, SP500 +14.09, AEX +10.29 per year over
     * 2015→2025, each roughly 1–2pp above what the index itself did. That residual is NOT a further
     * bug — it is the rebuild's three documented properties, all of which push the same way:
     * membership is a snapshot of TODAY's constituents projected backwards (survivorship — the
     * names that fell out are not in it), weights are FULL market cap where MSCI float-adjusts, and
     * the basis is each company's own reporting currency rather than one index currency. It is
     * exactly why the Analyse modal's benchmark tile reads the index ETF's own price series instead
     * (`routers/_benchmark_etf.py`, and the  on it in CLAUDE.md). There is no ETF series on this
     * tab, because every other card here is a FUNDAMENTAL the ETF cannot supply — so this line is
     * the same constituents as the fourteen charts around it, which is the property that makes the
     * comparison internally coherent, at the cost of a point or two against the published index.
     *
     *  The same `price_ps` LINE THE DIVIDEND-YIELD CARD DIVIDES BY. There is one "share price" on
     * this tab: GuruFocus's `Month End Stock Price` at each fiscal period end, in the reporting
     * currency, which is also `_RG_PRICE_CODE` — the price leg of /earnings'
     * Share-Price-vs-Owner-Earnings chart. Measured on ASML it IS a sample of the daily close
     * (681.7 / 678.7 / 921.4 at the last three year-ends, ratio 1.0000), so this changes the
     * frequency of that series and nothing else.
     *
     *  And it is the one price series here that self-heals through a split. CLAUDE.md records
     * that our stored `close_price` cannot: ingest only fetches dates NEWER than the stored max,
     * so a vendor's retroactive split rewrite is never re-read (KLA 1929→211). This line arrives
     * in the financials blob, which `ingest/earnings/financials.py` re-parses IN FULL on every
     * fetch and upserts by diff — a rewritten history therefore lands. That is a reason to prefer
     * the fiscal-period price here over the daily close, not a claim that the daily one is fixed.
     *
     *  THE `LTM` POINT ON THIS CARD IS THE LATEST QUARTER-END PRICE, not a trailing twelve months
     * of anything: `price_ps`'s `_TTM_RULE` is `last`, because a price is a level at an instant
     * (summing four of them would report a share at 4x its price). Same reading `market_cap` and
     * every balance-sheet line already carry under that label.
     *
     *  No forecast leg. `forecastCodes` is for a published analyst consensus of THIS line, and
     * nobody publishes one for a share price that we ingest — a dotted continuation here would be
     * an extrapolation wearing a forecast's clothes. See the EPS card's note.
     *
     *  THE `Tables` TAB'S `priceCagr` ROW IS THIS SAME LINE, ONE TAB AWAY IN THE SAME MODAL, AND
     * They must not drift. That row runs `buildBlend` over `portfolio-revenue-matrix?metric=
     * price_ps` — the client twin of `_fundamental_blend.blend_series`, same chained weighted
     * growth, same coverage floor, same carry-forward — so the two are one series computed on two
     * sides of the wire, not two definitions of "the basket's price". They differ only as every
     * card/row pair on this tab does: the card's CAGR is a log-linear FIT (hence the R² beside it)
     * and the row's is point-to-point.  At a high R² that gap is worth ~0.5pp; a large one means
     * the two sides are reading DIFFERENT SERIES, not that the fit disagrees — the trap that hid
     * the `fcf_per_share`/`fcf_ps` key bug for weeks. See `TablesTab`'s header.
     */
    title: 'Share price', titleKey: 'sharePrice',
    noun: 'share price', unit: 'per_share', kind: 'growth',
    benchmarkMetric: 'price_ps',
    //  Three per-share section spellings, same as `div_ps` — the capitalized cohort's
    // `Per Share Data`, and the lowercase cohort's `per_share_data` AND `per_share_data_array`.
    // These must stay identical to the backend's `_METRIC_CODES['price_ps']`, or a whole cohort's
    // holdings read as "no share price ingested" while carrying the line.
    codes: ['annuals__Per Share Data__Month End Stock Price',
      'annuals__per_share_data__Month End Stock Price',
      'annuals__per_share_data_array__Month End Stock Price'],
  },
  {
    /**
     *  First of the fundamental cards, deliberately — it is the line the rest of the page is
     * about, and it sits directly under the share price because it is the half of that move
     * anybody can underwrite. Revenue says how much a business sold; this says what reached a
     * share. The card indexes it to 100 at the first year it shares with the benchmark, so what
     * is compared is the GROWTH — the same idea as /earnings' Share-Price-vs-Owner-Earnings
     * chart, whose price leg is now the card above this one.
     *
     *  "EXCLUDING NON-RECURRING ITEMS" IS THE WHOLE POINT AND IT IS ONE OF THREE NEAR-IDENTICAL
     * LINES. GuruFocus also publishes `EPS (Diluted)` and `Earnings per Share (Diluted)`, both of
     * which INCLUDE one-offs — an impairment, a disposal, a tax settlement. They agree with this
     * one in most years, which is exactly what makes the wrong choice hard to catch: the series
     * looks right until a single distorted year bends the fitted trend, and the CAGR printed above
     * the chart IS that trend's slope. Same line `_RG_OE_CODE` uses on /earnings, so the two
     * surfaces cannot come to mean different things by "earnings".
     *
     *  EPS Goes negative, and the card already knows. A loss year cannot be plotted on a log axis
     * and cannot be a rebase base (100 × v/−2 inverts the curve), so `rebaseSeries` refuses when
     * there is no shared positive year and the card falls back to ABSOLUTE values, saying so in
     * the legend. That is the same treatment FCF/share gets and it is why neither is silently
     * wrong for a loss-making company — the gap is visible instead.
     */
    title: 'EPS (excl. non-recurring)', titleKey: 'epsNri',
    noun: 'EPS', unit: 'per_share', kind: 'growth',
    benchmarkMetric: 'eps_nri',
    codes: ['annuals__Per Share Data__EPS without NRI',
      'annuals__per_share_data__EPS without NRI',
      'annuals__per_share_data_array__EPS without NRI'],
    /**
     *  The forecast of **THIS** LINE, not of EPS generally. GuruFocus publishes
     * `annual_per_share_eps_estimate` beside it and the two agree to a cent on almost every company
     * (Apple 8.76 vs 8.77), which is exactly why the choice cannot be made by eye: this card's
     * actual is `EPS without NRI`, so continuing it with an including-NRI consensus would put a
     * one-off impairment on the wrong side of the join with nothing on screen to say so.
     *
     *  The only card with one, deliberately. Analysts forecast earnings; they do not publish a
     * consensus for FCF/share or a diluted share count, and `_FORECAST_BASE` knows of exactly two
     * forecast lines (this and dividends per share). A dotted leg on a card with no consensus
     * behind it would be an extrapolation wearing a forecast's clothes.
     */
    forecastCodes: ['annual_eps_nri_estimate'],
    forecastMetric: 'eps_nri_estimate',
  },
  {
    title: 'Revenue', titleKey: 'revenue',
    noun: 'revenue', unit: 'millions', kind: 'growth', benchmarkMetric: 'revenue',
    codes: ['annuals__Income Statement__Revenue', 'annuals__income_statement__Revenue'],
  },
  {
    title: 'FCF per share', titleKey: 'fcfPs',
    noun: 'FCF per share', unit: 'per_share', kind: 'growth', benchmarkMetric: 'fcf_ps',
    codes: ['annuals__Per Share Data__Free Cash Flow per Share',
      'annuals__per_share_data__Free Cash Flow per Share',
      'annuals__per_share_data_array__Free Cash Flow per Share'],
  },
  {
    // A count, not currency (no ccy prefix). CAGR reads as the buyback (−) / dilution (+) rate.
    // Uses the INCOME STATEMENT spelling — the only one both cohorts share (see backend `shares`).
    title: 'Shares outstanding', titleKey: 'shares',
    noun: 'shares outstanding', unit: 'shares', kind: 'growth', benchmarkMetric: 'shares',
    codes: ['annuals__Income Statement__Shares Outstanding (Diluted Average)',
      'annuals__income_statement__Shares Outstanding (Diluted Average)'],
  },
  //  NO "DIVIDEND / SHARE" CARD — the dividend is reported as a YIELD, for a company and for a
  // portfolio alike (`DividendYieldCard`). A per-share amount has no portfolio-level meaning: there
  // is no portfolio share, the amounts sit in different currencies, and the level rule rebases each
  // holding to 100 at its first year, which a dividend series starting at 0.00 cannot survive — so
  // the portfolio card read "No dividend/share ingested" while every holding carried the line.
];

/**
 * The metric list the blend is narrowed to — one declaration, because two tabs now send it.
 *
 *  `TablesTab` SENDS THE IDENTICAL ARRAY SO THE TWO SHARE ONE CACHE ENTRY. Both the server's
 * `cached_blend` and the browser's `readCache` key on the request BODY, so a different metric list
 * is a different entry — and an index blend is the most expensive read on this modal (~1,500
 * constituents on ACWI). Spelling the list twice would not be a wrong answer, it would be the
 * expensive answer computed twice.
 */
export const BLEND_METRICS: string[] = [...new Set(CARDS.flatMap(
  (c) => [c.benchmarkMetric, ...(c.forecastMetric ? [c.forecastMetric] : [])]))];

/**
 * metric KEY -> the GuruFocus codes its line is drawn from, for reading a blended payload back.
 *
 *  Derived from `CARDS`, NEVER LISTED SEPARATELY. These are the same code lists the charts
 * extract with, and this file has already paid for one key/code mismatch — `fcf_per_share` against
 * the real `fcf_ps`, which drew REVENUE under an FCF heading at +19.0% against +28.0%. A second
 * hand-written copy is that bug with a longer fuse.
 *
 *  Growth cards only. A ratio card's line is an average of a point-in-time ratio, not a level
 * chain, so it has no blended level to read and is not comparable to these.
 */
export const BLEND_CODES: Record<string, { codes: string[]; forecast: string[] }> =
  Object.fromEntries(CARDS
    .filter((c) => c.kind === 'growth')
    .map((c) => [c.benchmarkMetric, { codes: c.codes, forecast: c.forecastCodes ?? [] }]));

export default function LongEquityTab({
  isin, name, basket, portfolioId, sbcCorrection = true, compare = null, onBenchmarkChange, controlsHost,
}: {
  isin?: string;
  name?: string | null;
  basket?: Basket;
  portfolioId?: number;
  /**
   * A second company to draw beside this one, on every chart, instead of an index.
   *
   *  It reuses the benchmark slot rather than adding a third series, and that is the whole
   * TRICK. Every card here already draws a second line on a shared y-domain, with a legend, a
   * hover order and a coverage floor — all of it computed by running the card's OWN helper
   * (`marginByYear`, `debtRatioByYear`, …) over a second row set. A company is a one-holding book
   * to those same endpoints, so company-vs-company needs no new chart code, no new blend rule and
   * no new endpoint: fourteen charts become comparisons at once, and they cannot disagree with the
   * single-company view because they ARE it.
   *
   *  The cost is that you get one or the other. A chart carries the book and one comparison line;
   * choosing a company means not showing the index on that chart. Making it three lines would be a
   * third colour on fourteen charts, two of which already carry an amber trend line — see the
   * palette note in `benchSeries`.
   */
  compare?: { isin: string; name: string } | null;
  /** Reports the selector's actual target to the modal-level Refresh fundamentals control. */
  controlsHost?: HTMLElement | null;
  onBenchmarkChange?: (target: BenchTarget | null) => void;
  /**  OWNED BY THE MODAL, NOT HERE — its checkbox lives in the tab row, which is in the fixed
   *  head and therefore always visible. Governs the four charts whose numerator is FCF. */
  sbcCorrection?: boolean;
}) {
  // The period switch's own words. Its KEYS ('annual' | 'quarterly') are state and stay English —
  // they go into `ck`, which re-keys every card's fetch. See `fundamentalChromeCopy`.
  const chrome = useFundamentalChromeCopy();
  const isAgg = !!basket || portfolioId != null;
  const [bundle, setBundle] = useState<{ own: MetricsResponse; benchmark: MetricsResponse | null; requestKey: string } | null>(null);
  const [err, setErr] = useState<string | null>(null);
  // Bumped after an empty card ingests this company's financials — reloads the metrics (repopulates
  // every growth card) and re-keys the derived cards so they refetch their inputs too.
  const [reloadKey, setReloadKey] = useState(0);
  useEffect(() => {
    const reload = () => setReloadKey((k) => k + 1);
    window.addEventListener('bb:fundamentals-finished', reload);
    return () => window.removeEventListener('bb:fundamentals-finished', reload);
  }, []);
  //  A second, narrower key. The growth cards all read ONE metrics fetch, while each derived card
  // owns its own endpoint and refetches on a re-key — so bumping `reloadKey` to refresh one chart
  // reloads twelve. This one refetches the metrics only; nothing else moves.
  const [metricsKey, setMetricsKey] = useState(0);
  /** Portfolio only: how many holdings the blend has read. Null on a single company. */
  const progress = null;
  /**
   * The cadence every chart on this tab is on.
   *
   *  "Quarterly" IS TRAILING TWELVE MONTHS, NOT RAW QUARTERS — quarterly frequency with annual
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
   *  It is the same request as the portfolio's, with `universe` INSTEAD OF `holdings` — so each
   * card computes the benchmark line with the identical helper it runs over the book. There is no
   * second implementation of "FCF-SBC margin" anywhere, which is the only way the two lines on one
   * chart can be guaranteed to mean the same thing. The index arrives cap-weighted; the card's
   * existing weighted average does the rest.
   */
  //  Acwi, and spelt out rather than `BENCHMARKS[0]` (2026-09-03, on request; it was 'AEX').
  // The default is a choice about what a book is measured against, so it is written where the
  // choice is made: pinning it to the array's order would let a reorder of the list silently
  // re-benchmark every chart on the tab.
  //  Why acwi rather than the dutch index it was. These books are global — the Analyse modal's
  // own scorecard already defaults to ACWI, and the relative-momentum precompute ranks against it
  // — so AEX put the tab's fourteen charts on a 22-name national index while the tile two clicks
  // away used a 2,000-name global one. Two defaults for the same question, on one screen.
  //  The other two are still offered and the coverage note above still applies: SP500 is the
  // best-ingested, so a card drawn against ACWI states the coverage it actually reached.
  const [benchmarkChoice, setBenchmarkChoice] = useState<{ company?: string; value: string | null }>(
    { company: compare?.isin, value: compare ? COMPARE_VALUE : 'ACWI' });
  const benchmark = benchmarkChoice.company === compare?.isin
    ? benchmarkChoice.value : compare ? COMPARE_VALUE : 'ACWI';
  const setBenchmark = (value: string | null) => setBenchmarkChoice({ company: compare?.isin, value });

  /**
   *  A company picked elsewhere wins, and the select follows it. On /research-dashboard the second
   * company is chosen by the PAGE, not by this control; leaving the select on the default index
   * would draw an index while the page above it named a company. Derived at render from `compare` — an effect
   * that assigned it would render once with the wrong line and again with the right one.
   */
  const selected = compare && benchmark === COMPARE_VALUE ? COMPARE_VALUE : benchmark;

  /**  Memoised for the same reason `holdingsTarget` is — it is an effect dep in twelve cards. */
  const benchTarget = useMemo<BenchTarget | null>(() => {
    if (compare && selected === COMPARE_VALUE) {
      return { isin: compare.isin, label: compare.name, cadence };
    }
    return selected && selected !== COMPARE_VALUE
      ? { universe: selected, label: selected, cadence }
      : null;
  }, [compare, selected, cadence]);

  useEffect(() => onBenchmarkChange?.(benchTarget), [benchTarget, onBenchmarkChange]);
  const repairRevision = useBenchmarkRepair(benchTarget);

  //  Memoised — it's a card/modal effect dep, so a fresh object each render would refetch forever.
  //  `cadence` RIDES IN THE BODY, which is what makes one toggle move nine cards: every derived
  // card POSTs this object verbatim to its own `*-inputs` endpoint, and they all read their lines
  // through one cadence-aware loader on the server. Nothing per-card to keep in step.
  const holdingsTarget = useMemo(() => (isAgg
    ? (basket
      ? { holdings: basket.holdings.map((h) => ({ isin: h.isin, name: h.name, weight: h.weight })), cadence }
      : { portfolio_id: portfolioId, cadence })
    : { holdings: [{ isin: isin ?? '', weight: 1 }], cadence }), [isAgg, basket, portfolioId, isin, cadence]);

  const requestKey = JSON.stringify({ isin, isAgg, holdingsTarget, benchTarget, cadence });
  const currentBundle = bundle?.requestKey === requestKey ? bundle : null;
  const data = currentBundle?.own ?? null;
  const benchMetrics = currentBundle?.benchmark?.metrics ?? null;
  const benchNotes = currentBundle?.benchmark?.blend_notes;
  const benchCounts = currentBundle?.benchmark?.member_counts;
  const benchErr = null;
  useEffect(() => {
    let alive = true;
    const ctrl = new AbortController();
    setBundle(null);
    setErr(null);
    void (async () => {
      try {
        const response = await apiFetch(`${API_URL}/api/earnings/chart-bundle`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' }, signal: ctrl.signal,
          body: JSON.stringify({ isin: isAgg ? undefined : isin,
            own: isAgg ? holdingsTarget : undefined, cadence,
            benchmark: benchTarget ? JSON.parse(benchBody(benchTarget)) : null }),
        });
        const result = await response.json();
        if (!alive) return;
        if (!response.ok) throw new Error(result?.detail ?? `HTTP ${response.status}`);
        if (!result.ready) throw new Error('The complete chart is not ready yet.');
        setBundle({ ...result, requestKey });
      } catch (error) {
        if (alive) setErr(error instanceof Error ? error.message : String(error));
      }
    })();
    return () => { alive = false; ctrl.abort(); };
  }, [isin, isAgg, holdingsTarget, benchTarget, cadence, reloadKey, metricsKey, repairRevision, requestKey]);

  /**
   *  Neither an error nor a load may take the control row with it — these used to be early
   * `return`s, and the toggle that STARTED the load was the first thing to leave the screen.
   *
   * A book's blend runs for a minute, so pressing Quarterly replaced the whole tab — cadence
   * toggle, benchmark picker and all — with one "Loading… (3 of 41 companies)" line, and the
   * reader who wanted to go straight back to Annual had nothing to click until it finished. A
   * control that disappears exactly while its own work is in flight is the one moment it is most
   * needed: switching back now cancels that fetch (see the  on the effect above) and the cached
   * answer for the cadence you came from returns instantly.
   *
   *  One count for the whole grid, not twelve. Every card below reads this one metrics fetch (or
   * fires its own once it lands), so twelve boxes each saying "Loading…" showed a wait twelve
   * times over and none of them could say how far along it was.
   */
  const body = err
    ? <p className="text-xs text-neg-300 py-16 text-center">{err}</p>
    : isAgg && !data
      ? <p className="text-xs text-fg-subtle py-16 text-center">{blendLoadingLabel(progress)}</p>
      : null;

  // Fixed order across the grid: Share price, EPS (excl. non-recurring), Revenue, FCF/share,
  // FCF-SBC margin, Cash return on capital, Debt / assets ex-GW, Interest / op. profit, Shares
  // outstanding, SBC / OCF, Invested capital, Capex margin, Dividend yield, FCF-SBC yield.
  const [sharePrice, epsNri, revenue, fcfPs, shares] = CARDS;
  // Single-company only: an empty growth card can fetch this company's financials, then reload.
  const ingestIsin = isAgg ? undefined : isin;
  const onIngested = () => setReloadKey((k) => k + 1);
  const onReloadMetrics = () => setMetricsKey((k) => k + 1);
  const gName = name ?? data?.company_name;
  const growth = {
    metrics: data?.metrics ?? null,
    chartStatistics: data?.chart_statistics,
    benchStatistics: currentBundle?.benchmark?.chart_statistics,
    isAgg, currency: data?.currency,
    blendNotes: data?.blend_notes, memberCounts: data?.member_counts,
    holdingsTarget, holdingsName: gName,
    // A slice is blended from its own holdings, not from every position in the parent portfolio.
    // The coverage line must therefore name the slice while the rest of the modal retains its
    // parent-book context.
    coverageLabel: basket?.label ?? gName,
    ingestIsin, onIngested, onReloadMetrics, cadence,
    benchMetrics, benchLabel: benchTarget?.label ?? null, benchTarget, benchErr, benchNotes,
    benchCounts,
  };
  //  One key suffix for every derived card. They each own their fetch, so without the cadence in
  // the key a switch would leave twelve charts showing the previous basis until something else
  // re-keyed them — and the toggle would look broken on the cards that matter most.
  const ck = `${reloadKey}-${cadence}`;
  const controls = <div className="flex shrink-0 items-center gap-2 whitespace-nowrap text-xs">
      <span className="text-fg-faint">{chrome.periods}</span>
      <div className="flex shrink-0 items-center gap-0.5 rounded-lg border border-neutral-700 p-0.5 w-fit">
        {([
          ['annual', chrome.annual, chrome.annualNote],
          ['quarterly', chrome.quarterly, chrome.quarterlyNote],
        ] as const).map(([k, label, note]) => (
          <button key={k} type="button" onClick={() => setCadence(k)} title={note}
            aria-pressed={cadence === k}
            className={`px-3 py-1 rounded-md text-xs font-medium transition-colors ${cadence === k
              ? 'bg-accent-600 text-fg-strong'
              : 'text-fg-muted hover:text-fg-strong hover:bg-overlay/5'}`}>
            {label}
          </button>
        ))}
      </div>
      {!controlsHost && cadence === 'quarterly' && (
        <span className="text-fg-faint">
          trailing 12 months — a Q4 point equals that fiscal year
        </span>
      )}
      {/*  ONE CONTROL FOR TWELVE CHARTS. Per-card benchmark pickers would let two charts on one
          screen be measured against different indices — a comparison a reader cannot arbitrate,
          and the same failure the tab-wide cadence toggle avoids. */}
      {/*  ONE CONTROL, NOT A CHECKBOX PLUS A PICKER. The pair had the benchmark OFF by default and
          hid which index it would draw until you ticked it, so the common case — measure the book
          against something — cost two interactions and a guess. A single select shows the answer
          while it states the question, and `None` is an option rather than a second widget. */}
      <label className="flex items-center gap-1.5 ml-1 text-fg-faint">
        Benchmark
        <select value={selected ?? ''}
          onChange={(e) => setBenchmark(e.target.value || null)}
          aria-label="Compare against"
          title={'The index each chart is measured against. Its constituents are cap-weighted and '
            + 'run through the SAME formula as the portfolio, so the two lines are comparable. '
            + 'Only constituents whose fundamentals are ingested contribute — the coverage floor '
            + 'applies to the index exactly as it does to the book.'}
          className="cursor-pointer bg-page border border-neutral-700 rounded-lg px-2 py-0.5 text-[11px] font-mono text-fg focus:border-accent-500">
          <option value="">None</option>
          {/*  FIRST, AND ONLY WHEN THERE IS ONE. The comparison company is what the reader came
              for on /research-dashboard; the indices stay available underneath so a company can
              still be measured against its market without leaving the page. */}
          {compare && <option value={COMPARE_VALUE}>{compare.name}</option>}
          {BENCHMARKS.map((b) => <option key={b} value={b}>{b}</option>)}
        </select>
      </label>
    </div>;
  return (
    <>
    {controlsHost ? createPortal(controls, controlsHost) : <div className="mb-3">{controls}</div>}
    {/* The controls above stay put; only what they govern is replaced while it loads or fails. */}
    {body ?? (
    <GraphCoverageProvider value={null}>
    <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
      {/*  THREE ACROSS, NOT FOUR. Each card carries a 320px chart with up to five series, a legend
          that wraps, and stat tiles above it — at four columns the plot area was narrow enough that
          a twelve-year axis crowded its ticks and the legend took three lines. Only the grid
          changes; the card order below is fixed and reflows unaltered. */}
      {/*  FIRST IN THE GRID — the weighted share-price increase of the book and of the index, i.e.
          the move every card after it exists to explain. See its entry in `CARDS`, and in
          particular why it is NOT the portfolio's return. */}
      <MetricGrowthCard key={sharePrice.title} cfg={sharePrice}
        {...growth} />
      {/*  SECOND — the line the tab is about. See its entry in `CARDS`. */}
      <MetricGrowthCard key={epsNri.title} cfg={epsNri}
        {...growth} />
      <MetricGrowthCard key={revenue.title} cfg={revenue}
        {...growth} />
      <MetricGrowthCard key={fcfPs.title} cfg={fcfPs}
        {...growth} />
      {/* Derived cards fetch their own inputs; re-key on reload so an ingest repopulates them too. */}
      <MarginCard key={`margin-${ck}`} benchTarget={benchTarget} holdingsTarget={holdingsTarget} holdingsName={gName} sbcCorrection={sbcCorrection} onRefreshed={onIngested} />
      <CashReturnCard key={`cashret-${ck}`} benchTarget={benchTarget} holdingsTarget={holdingsTarget} holdingsName={gName} sbcCorrection={sbcCorrection} onRefreshed={onIngested} />
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
    </GraphCoverageProvider>
    )}
    </>
  );
}
