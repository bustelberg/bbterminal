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
    /**
     * ⚠ FIRST ON THE TAB, DELIBERATELY — it is the line the whole page is about. Revenue says how
     * much a business sold; this says what reached a share. The card indexes it to 100 at the
     * first year it shares with the benchmark, so what is compared is the GROWTH — the same idea
     * as /earnings' Share-Price-vs-Owner-Earnings chart, minus the price leg.
     *
     * ⚠⚠ "EXCLUDING NON-RECURRING ITEMS" IS THE WHOLE POINT AND IT IS ONE OF THREE NEAR-IDENTICAL
     * LINES. GuruFocus also publishes `EPS (Diluted)` and `Earnings per Share (Diluted)`, both of
     * which INCLUDE one-offs — an impairment, a disposal, a tax settlement. They agree with this
     * one in most years, which is exactly what makes the wrong choice hard to catch: the series
     * looks right until a single distorted year bends the fitted trend, and the CAGR printed above
     * the chart IS that trend's slope. Same line `_RG_OE_CODE` uses on /earnings, so the two
     * surfaces cannot come to mean different things by "earnings".
     *
     * ⚠ EPS GOES NEGATIVE, AND THE CARD ALREADY KNOWS. A loss year cannot be plotted on a log axis
     * and cannot be a rebase base (100 × v/−2 inverts the curve), so `rebaseSeries` refuses when
     * there is no shared positive year and the card falls back to ABSOLUTE values, saying so in
     * the legend. That is the same treatment FCF/share gets and it is why neither is silently
     * wrong for a loss-making company — the gap is visible instead.
     */
    title: 'EPS (excl. non-recurring)', noun: 'EPS', unit: 'per_share', kind: 'growth',
    benchmarkMetric: 'eps_nri',
    codes: ['annuals__Per Share Data__EPS without NRI',
      'annuals__per_share_data__EPS without NRI'],
    /**
     * ⚠ THE FORECAST OF **THIS** LINE, not of EPS generally. GuruFocus publishes
     * `annual_per_share_eps_estimate` beside it and the two agree to a cent on almost every company
     * (Apple 8.76 vs 8.77), which is exactly why the choice cannot be made by eye: this card's
     * actual is `EPS without NRI`, so continuing it with an including-NRI consensus would put a
     * one-off impairment on the wrong side of the join with nothing on screen to say so.
     *
     * ⚠ THE ONLY CARD WITH ONE, DELIBERATELY. Analysts forecast earnings; they do not publish a
     * consensus for FCF/share or a diluted share count, and `_FORECAST_BASE` knows of exactly two
     * forecast lines (this and dividends per share). A dotted leg on a card with no consensus
     * behind it would be an extrapolation wearing a forecast's clothes.
     */
    forecastCodes: ['annual_eps_nri_estimate'],
    forecastMetric: 'eps_nri_estimate',
  },
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
  // ⚠ AEX, NOT `BENCHMARKS[0]`. The default is a choice about what this book is measured against —
  // a Dutch book against the Dutch index — not "whichever we happen to list first", and pinning it
  // to the array's order means reordering the list silently re-benchmarks every chart in the tab.
  const [benchmark, setBenchmark] = useState<string | null>('AEX');
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
    /**
     * ⚠ ABORTED ON THE WAY OUT, NOT MERELY IGNORED. A book's blend is a read per holding and runs
     * for a minute; the `alive` flag alone dropped the RESULT but left the work running, so
     * flipping the cadence twice had two blends of forty companies in flight, competing for the
     * same connections while the reader waited on the second. Switching now cancels the first.
     *
     * ⚠ THE STREAM HONOURS IT PROPERLY — `runSSE` wires abort to `reader.cancel()`, so the server
     * stops mid-book rather than finishing into a dropped connection.
     */
    const ctrl = new AbortController();
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
            ctrl.signal,
          );
          if (!alive) return;
          if (out.kind === 'none') { setData({ metrics: [] }); return; }
          if (out.kind === 'error') { setErr(out.message); return; }
          setData(out.data);
          return;
        }
        const r = await apiFetch(`${API_URL}/api/earnings/by-isin/${encodeURIComponent(isin ?? '')}`
          + `/metrics?cadence=${cadence}`, { signal: ctrl.signal });
        if (r.status === 404) { if (alive) setData({ metrics: [] }); return; }
        const b = await r.json().catch(() => null);
        if (!alive) return;
        if (!r.ok) { setErr(b?.detail ?? `HTTP ${r.status}`); return; }
        setData(b as MetricsResponse);
      } catch (e) {
        // ⚠ AN ABORT IS NOT AN ERROR. It is this component cancelling its own request, and
        // rendering "AbortError" where a chart was would turn a deliberate switch into a failure.
        // `alive` is already false by then — this only guards a future caller that forgets.
        if (alive) setErr(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { alive = false; ctrl.abort(); };
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
  /**
   * The index's own `blend_notes` — why a code its constituents DO carry drew nothing.
   *
   * ⚠⚠ IT WAS THROWN AWAY, AND THAT MADE A CORRECT REFUSAL LOOK LIKE A BUG. Switching the
   * benchmark from AEX to ACWI drops the analyst-expectation line, because 22 of 22 AEX names
   * carry a consensus and only 351 of 1,715 ACWI names do — 20%, far under the blend's floor,
   * so every forecast period is refused. That is the right answer and it vanished in silence:
   * a striped line on the book, none on the index, and nothing on screen to say which of
   * "the index has no expectations" and "too few of its members are covered" was true.
   */
  const [benchNotes, setBenchNotes] = useState<Record<string, BlendNote> | undefined>();
  const [benchErr, setBenchErr] = useState<string | null>(null);
  useEffect(() => {
    let alive = true;
    // Same rule as the book's own load above: a switch cancels the index read it replaces. An
    // index blend is the most expensive read on the tab (~1,500 constituents on ACWI).
    const ctrl = new AbortController();
    void (async () => {
      setBenchMetrics(null); setBenchErr(null); setBenchNotes(undefined);
      if (!benchmark) return;
      try {
        const r = await apiFetch(`${API_URL}/api/earnings/fundamental-blend-metrics`, {
          signal: ctrl.signal,
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          // ⚠ NAME THE THREE METRICS. Unnamed, the blend reads every charted code per constituent —
          // three paged requests each, i.e. ~1,500 round trips for the S&P. Named, it is one
          // chunked query per metric. See the request model's `metrics` field.
          // ⚠⚠ THE FORECAST METRICS MUST BE NAMED HERE OR THE INDEX SILENTLY HAS NO FORECAST. The
          // book's own line comes from the UNNARROWED read, which already pages `annual_%estimate`;
          // a benchmark is a narrowed read and gets exactly what this list asks for. Omitting them
          // draws a dotted consensus on the book and none on the index — which reads as "the index
          // has no expectations" rather than "we did not ask for them".
          body: JSON.stringify({
            universe: benchmark, cadence,
            metrics: [...new Set(CARDS.flatMap(
              (c) => [c.benchmarkMetric, ...(c.forecastMetric ? [c.forecastMetric] : [])]))],
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
        setBenchNotes((b as MetricsResponse)?.blend_notes);
      } catch (e) {
        if (!alive) return;                     // aborted by the switch — not a failure to report
        const detail = e instanceof Error ? e.message : String(e);
        console.warn(`[bb:bench] blend ${benchmark}: ${detail}`, e);
        setBenchErr(detail);
      }
    })();
    return () => { alive = false; ctrl.abort(); };
  }, [benchmark, cadence]);

  /**
   * ⚠⚠ NEITHER AN ERROR NOR A LOAD MAY TAKE THE CONTROL ROW WITH IT — these used to be early
   * `return`s, and the toggle that STARTED the load was the first thing to leave the screen.
   *
   * A book's blend runs for a minute, so pressing Quarterly replaced the whole tab — cadence
   * toggle, benchmark picker and all — with one "Loading… (3 of 41 companies)" line, and the
   * reader who wanted to go straight back to Annual had nothing to click until it finished. A
   * control that disappears exactly while its own work is in flight is the one moment it is most
   * needed: switching back now cancels that fetch (see the ⚠ on the effect above) and the cached
   * answer for the cadence you came from returns instantly.
   *
   * ⚠ ONE COUNT FOR THE WHOLE GRID, NOT TWELVE. Every card below reads this one metrics fetch (or
   * fires its own once it lands), so twelve boxes each saying "Loading…" showed a wait twelve
   * times over and none of them could say how far along it was.
   */
  const body = err
    ? <p className="text-xs text-neg-300 py-16 text-center">{err}</p>
    : isAgg && !data
      ? <p className="text-xs text-fg-subtle py-16 text-center">{blendLoadingLabel(progress)}</p>
      : null;

  // Fixed order across the grid: EPS (excl. non-recurring), Revenue, FCF/share, FCF-SBC margin,
  // Cash return on capital, Debt / assets ex-GW, Interest / op. profit, Shares outstanding,
  // SBC / OCF, Invested capital, Capex margin, Dividend yield, FCF-SBC yield.
  const [epsNri, revenue, fcfPs, shares] = CARDS;
  // Single-company only: an empty growth card can fetch this company's financials, then reload.
  const ingestIsin = isAgg ? undefined : isin;
  const onIngested = () => setReloadKey((k) => k + 1);
  const onReloadMetrics = () => setMetricsKey((k) => k + 1);
  const gName = name ?? data?.company_name;
  const growth = {
    metrics: data?.metrics ?? null, isAgg, currency: data?.currency,
    blendNotes: data?.blend_notes, holdingsTarget, holdingsName: gName,
    ingestIsin, onIngested, onReloadMetrics, cadence,
    benchMetrics, benchLabel: benchmark, benchErr, benchNotes,
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
      {/* ⚠ ONE CONTROL, NOT A CHECKBOX PLUS A PICKER. The pair had the benchmark OFF by default and
          hid which index it would draw until you ticked it, so the common case — measure the book
          against something — cost two interactions and a guess. A single select shows the answer
          while it states the question, and `None` is an option rather than a second widget. */}
      <label className="flex items-center gap-1.5 ml-4 text-fg-faint">
        Benchmark
        <select value={benchmark ?? ''}
          onChange={(e) => setBenchmark(e.target.value || null)}
          aria-label="Benchmark"
          title={'The index each chart is measured against. Its constituents are cap-weighted and '
            + 'run through the SAME formula as the portfolio, so the two lines are comparable. '
            + 'Only constituents whose fundamentals are ingested contribute — the coverage floor '
            + 'applies to the index exactly as it does to the book.'}
          className="cursor-pointer bg-page border border-neutral-700 rounded-lg px-2 py-0.5 text-[11px] font-mono text-fg focus:border-accent-500">
          <option value="">None</option>
          {BENCHMARKS.map((b) => <option key={b} value={b}>{b}</option>)}
        </select>
      </label>
    </div>
    {/* The controls above stay put; only what they govern is replaced while it loads or fails. */}
    {body ?? (
    <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
      {/* ⚠ THREE ACROSS, NOT FOUR. Each card carries a 320px chart with up to five series, a legend
          that wraps, and stat tiles above it — at four columns the plot area was narrow enough that
          a twelve-year axis crowded its ticks and the legend took three lines. Only the grid
          changes; the card order below is fixed and reflows unaltered. */}
      {/* ⚠ FIRST IN THE GRID — the line the tab is about. See its entry in `CARDS`. */}
      <MetricGrowthCard key={epsNri.title} cfg={epsNri}
        {...growth} />
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
    )}
    </>
  );
}
