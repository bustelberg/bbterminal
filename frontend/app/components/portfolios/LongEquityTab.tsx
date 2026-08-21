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
import { benchBody, type BenchTarget } from './benchSeries';

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

/** The select value that means "the company the caller handed me", not an index.
 *  ⚠ A SENTINEL, NOT AN ISIN. An ISIN in the option value would collide the day an index is ever
 *  labelled with one, and it would put an identifier in a control whose other values are names. */
const COMPARE_VALUE = '__compare__';

const CARDS: MetricCfg[] = [
  {
    /**
     * ⚠ FIRST ON THE TAB — THE WEIGHTED SHARE-PRICE INCREASE OF THE BOOK AND OF THE INDEX. Every
     * other card on this grid explains a return; this one IS it, so it leads and the explanation
     * follows.
     *
     * ⚠⚠ IT NEEDED NO NEW ENDPOINT, NO NEW BLEND RULE AND NO NEW CHART, AND THAT IS NOT A
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
     * ⚠⚠ IT IS A PRICE RETURN IN EACH COMPANY'S REPORTING CURRENCY — NOT A EUR TOTAL RETURN, AND
     * IT WILL NOT RECONCILE WITH THE ANALYSE MODAL. Three deliberate differences, every one of
     * which moves the number: dividends are excluded (a price index, not total return); the FX leg
     * is excluded (each g_i divides out its own currency, so a US holding's euro move is not in
     * it — see the EUR-basis rule in CLAUDE.md, which governs the RETURN surfaces, not this tab);
     * and the weights are the book's current weights held flat across history rather than a
     * traded position. So this answers "how did these businesses' shares do" on the same axis as
     * their revenue and earnings — it is deliberately NOT the portfolio's performance, which
     * `period_return_pct` owns and nothing here may restate.
     *
     * ⚠⚠ THE BENCHMARK LINE HERE IS THE CONSTITUENT REBUILD, AND IT STILL READS HIGH. Measured
     * 2026-08-21 after the anchor-weighting fix (`blend_series`, which took a decade of ACWI from a
     * fabricated +20.21%/yr to +11.14%/yr): ACWI +11.14, SP500 +14.09, AEX +10.29 per year over
     * 2015→2025, each roughly 1–2pp above what the index itself did. That residual is NOT a further
     * bug — it is the rebuild's three documented properties, all of which push the same way:
     * membership is a snapshot of TODAY's constituents projected backwards (survivorship — the
     * names that fell out are not in it), weights are FULL market cap where MSCI float-adjusts, and
     * the basis is each company's own reporting currency rather than one index currency. It is
     * exactly why the Analyse modal's benchmark tile reads the index ETF's own price series instead
     * (`routers/_benchmark_etf.py`, and the ⚠⚠ on it in CLAUDE.md). There is no ETF series on this
     * tab, because every other card here is a FUNDAMENTAL the ETF cannot supply — so this line is
     * the same constituents as the fourteen charts around it, which is the property that makes the
     * comparison internally coherent, at the cost of a point or two against the published index.
     *
     * ⚠ THE SAME `price_ps` LINE THE DIVIDEND-YIELD CARD DIVIDES BY. There is one "share price" on
     * this tab: GuruFocus's `Month End Stock Price` at each fiscal period end, in the reporting
     * currency, which is also `_RG_PRICE_CODE` — the price leg of /earnings'
     * Share-Price-vs-Owner-Earnings chart. Measured on ASML it IS a sample of the daily close
     * (681.7 / 678.7 / 921.4 at the last three year-ends, ratio 1.0000), so this changes the
     * frequency of that series and nothing else.
     *
     * ⚠ AND IT IS THE ONE PRICE SERIES HERE THAT SELF-HEALS THROUGH A SPLIT. CLAUDE.md records
     * that our stored `close_price` cannot: ingest only fetches dates NEWER than the stored max,
     * so a vendor's retroactive split rewrite is never re-read (KLA 1929→211). This line arrives
     * in the financials blob, which `ingest/earnings/financials.py` re-parses IN FULL on every
     * fetch and upserts by diff — a rewritten history therefore lands. That is a reason to prefer
     * the fiscal-period price here over the daily close, not a claim that the daily one is fixed.
     *
     * ⚠ THE `LTM` POINT ON THIS CARD IS THE LATEST QUARTER-END PRICE, not a trailing twelve months
     * of anything: `price_ps`'s `_TTM_RULE` is `last`, because a price is a level at an instant
     * (summing four of them would report a share at 4x its price). Same reading `market_cap` and
     * every balance-sheet line already carry under that label.
     *
     * ⚠ NO FORECAST LEG. `forecastCodes` is for a published analyst consensus of THIS line, and
     * nobody publishes one for a share price that we ingest — a dotted continuation here would be
     * an extrapolation wearing a forecast's clothes. See the EPS card's note.
     *
     * ⚠⚠ THE `Tables` TAB'S `priceCagr` ROW IS THIS SAME LINE, ONE TAB AWAY IN THE SAME MODAL, AND
     * THEY MUST NOT DRIFT. That row runs `buildBlend` over `portfolio-revenue-matrix?metric=
     * price_ps` — the client twin of `_fundamental_blend.blend_series`, same chained weighted
     * growth, same coverage floor, same carry-forward — so the two are one series computed on two
     * sides of the wire, not two definitions of "the basket's price". They differ only as every
     * card/row pair on this tab does: the card's CAGR is a log-linear FIT (hence the R² beside it)
     * and the row's is point-to-point. ⚠ At a high R² that gap is worth ~0.5pp; a large one means
     * the two sides are reading DIFFERENT SERIES, not that the fit disagrees — the trap that hid
     * the `fcf_per_share`/`fcf_ps` key bug for weeks. See `TablesTab`'s header.
     */
    title: 'Share price', titleKey: 'sharePrice',
    noun: 'share price', unit: 'per_share', kind: 'growth',
    benchmarkMetric: 'price_ps',
    // ⚠ THREE PER-SHARE SECTION SPELLINGS, same as `div_ps` — the capitalized cohort's
    // `Per Share Data`, and the lowercase cohort's `per_share_data` AND `per_share_data_array`.
    // These must stay identical to the backend's `_METRIC_CODES['price_ps']`, or a whole cohort's
    // holdings read as "no share price ingested" while carrying the line.
    codes: ['annuals__Per Share Data__Month End Stock Price',
      'annuals__per_share_data__Month End Stock Price',
      'annuals__per_share_data_array__Month End Stock Price'],
  },
  {
    /**
     * ⚠ FIRST OF THE FUNDAMENTAL CARDS, DELIBERATELY — it is the line the rest of the page is
     * about, and it sits directly under the share price because it is the half of that move
     * anybody can underwrite. Revenue says how much a business sold; this says what reached a
     * share. The card indexes it to 100 at the first year it shares with the benchmark, so what
     * is compared is the GROWTH — the same idea as /earnings' Share-Price-vs-Owner-Earnings
     * chart, whose price leg is now the card above this one.
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
    title: 'EPS (excl. non-recurring)', titleKey: 'epsNri',
    noun: 'EPS', unit: 'per_share', kind: 'growth',
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
    title: 'Revenue', titleKey: 'revenue',
    noun: 'revenue', unit: 'millions', kind: 'growth', benchmarkMetric: 'revenue',
    codes: ['annuals__Income Statement__Revenue', 'annuals__income_statement__Revenue'],
  },
  {
    title: 'FCF per share', titleKey: 'fcfPs',
    noun: 'FCF per share', unit: 'per_share', kind: 'growth', benchmarkMetric: 'fcf_ps',
    codes: ['annuals__Per Share Data__Free Cash Flow per Share',
      'annuals__per_share_data__Free Cash Flow per Share'],
  },
  {
    // A count, not currency (no ccy prefix). CAGR reads as the buyback (−) / dilution (+) rate.
    // Uses the INCOME STATEMENT spelling — the only one both cohorts share (see backend `shares`).
    title: 'Shares outstanding', titleKey: 'shares',
    noun: 'shares outstanding', unit: 'shares', kind: 'growth', benchmarkMetric: 'shares',
    codes: ['annuals__Income Statement__Shares Outstanding (Diluted Average)',
      'annuals__income_statement__Shares Outstanding (Diluted Average)'],
  },
  // ⚠ NO "DIVIDEND / SHARE" CARD — the dividend is reported as a YIELD, for a company and for a
  // portfolio alike (`DividendYieldCard`). A per-share amount has no portfolio-level meaning: there
  // is no portfolio share, the amounts sit in different currencies, and the level rule rebases each
  // holding to 100 at its first year, which a dividend series starting at 0.00 cannot survive — so
  // the portfolio card read "No dividend/share ingested" while every holding carried the line.
];

export default function LongEquityTab({
  isin, name, basket, portfolioId, sbcCorrection = true, compare = null,
}: {
  isin?: string;
  name?: string | null;
  basket?: { holdings: { isin: string; weight: number; name?: string }[] };
  portfolioId?: number;
  /**
   * A SECOND COMPANY to draw beside this one, on every chart, instead of an index.
   *
   * ⚠⚠ IT REUSES THE BENCHMARK SLOT RATHER THAN ADDING A THIRD SERIES, AND THAT IS THE WHOLE
   * TRICK. Every card here already draws a second line on a shared y-domain, with a legend, a
   * hover order and a coverage floor — all of it computed by running the card's OWN helper
   * (`marginByYear`, `debtRatioByYear`, …) over a second row set. A company is a one-holding book
   * to those same endpoints, so company-vs-company needs no new chart code, no new blend rule and
   * no new endpoint: fourteen charts become comparisons at once, and they cannot disagree with the
   * single-company view because they ARE it.
   *
   * ⚠ THE COST IS THAT YOU GET ONE OR THE OTHER. A chart carries the book and one comparison line;
   * choosing a company means not showing the index on that chart. Making it three lines would be a
   * third colour on fourteen charts, two of which already carry an amber trend line — see the
   * palette note in `benchSeries`.
   */
  compare?: { isin: string; name: string } | null;
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
  const [benchmark, setBenchmark] = useState<string | null>(compare ? COMPARE_VALUE : 'AEX');

  /**
   * ⚠ A COMPANY PICKED ELSEWHERE WINS, AND THE SELECT FOLLOWS IT. On /research-dashboard the second
   * company is chosen by the PAGE, not by this control; leaving the select on 'AEX' would draw an
   * index while the page above it named a company. Derived at render from `compare` — an effect
   * that assigned it would render once with the wrong line and again with the right one.
   */
  const selected = compare && benchmark === COMPARE_VALUE ? COMPARE_VALUE : benchmark;

  /** ⚠ Memoised for the same reason `holdingsTarget` is — it is an effect dep in twelve cards. */
  const benchTarget = useMemo<BenchTarget | null>(() => {
    if (compare && selected === COMPARE_VALUE) {
      return { isin: compare.isin, label: compare.name, cadence };
    }
    return selected && selected !== COMPARE_VALUE
      ? { universe: selected, label: selected, cadence }
      : null;
  }, [compare, selected, cadence]);

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
        // streams per-company progress — it is a read per holding — via `blendMetrics`, which is
        // the one definition of "load this book's metrics" (it had a second caller until the
        // Old-charts tab was removed; the point of the module is the single definition).
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
      if (!benchTarget) return;
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
          // ⚠ THE SAME BODY BUILDER THE CARDS USE (`benchBody`), then the metric list merged in.
          // Hand-writing `{universe: …}` here is what made this the ONE fetch that could not
          // compare against a company: the ten cards went through `useBenchInputs` and switched,
          // and the three growth cards silently kept drawing the index. One builder, one shape.
          body: JSON.stringify({
            ...JSON.parse(benchBody(benchTarget)),
            metrics: [...new Set(CARDS.flatMap(
              (c) => [c.benchmarkMetric, ...(c.forecastMetric ? [c.forecastMetric] : [])]))],
          }),
        });
        const b = await r.json().catch(() => null);
        if (!alive) return;
        if (!r.ok) {
          const detail = (b?.detail as string) ?? `HTTP ${r.status}`;
          console.warn(`[bb:bench] blend ${benchTarget.label}: ${detail}`, b);
          setBenchErr(detail);
          return;
        }
        setBenchMetrics((b as MetricsResponse)?.metrics ?? []);
        setBenchNotes((b as MetricsResponse)?.blend_notes);
      } catch (e) {
        if (!alive) return;                     // aborted by the switch — not a failure to report
        const detail = e instanceof Error ? e.message : String(e);
        console.warn(`[bb:bench] blend ${benchTarget.label}: ${detail}`, e);
        setBenchErr(detail);
      }
    })();
    return () => { alive = false; ctrl.abort(); };
  }, [benchTarget, cadence]);

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
    metrics: data?.metrics ?? null, isAgg, currency: data?.currency,
    blendNotes: data?.blend_notes, holdingsTarget, holdingsName: gName,
    ingestIsin, onIngested, onReloadMetrics, cadence,
    benchMetrics, benchLabel: benchTarget?.label ?? null, benchTarget, benchErr, benchNotes,
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
        <select value={selected ?? ''}
          onChange={(e) => setBenchmark(e.target.value || null)}
          aria-label="Compare against"
          title={'The index each chart is measured against. Its constituents are cap-weighted and '
            + 'run through the SAME formula as the portfolio, so the two lines are comparable. '
            + 'Only constituents whose fundamentals are ingested contribute — the coverage floor '
            + 'applies to the index exactly as it does to the book.'}
          className="cursor-pointer bg-page border border-neutral-700 rounded-lg px-2 py-0.5 text-[11px] font-mono text-fg focus:border-accent-500">
          <option value="">None</option>
          {/* ⚠ FIRST, AND ONLY WHEN THERE IS ONE. The comparison company is what the reader came
              for on /research-dashboard; the indices stay available underneath so a company can
              still be measured against its market without leaving the page. */}
          {compare && <option value={COMPARE_VALUE}>{compare.name}</option>}
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
      {/* ⚠ FIRST IN THE GRID — the weighted share-price increase of the book and of the index, i.e.
          the move every card after it exists to explain. See its entry in `CARDS`, and in
          particular why it is NOT the portfolio's return. */}
      <MetricGrowthCard key={sharePrice.title} cfg={sharePrice}
        {...growth} />
      {/* ⚠ SECOND — the line the tab is about. See its entry in `CARDS`. */}
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
