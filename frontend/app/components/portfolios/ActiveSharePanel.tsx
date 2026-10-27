'use client';

/**
 * Active share — how much of the book's stock sleeve is not the benchmark.
 *
 *     AS = ½ · Σ |wᵢᵖ − wᵢᵇ|          Overlap = Σ min(wᵢᵖ, wᵢᵇ) = 1 − AS
 *
 *  It is a structural measure, not a return one, and the panel has to say so. Every other number
 * in this modal is about what happened; this one is about what the book IS, today, regardless of
 * how it has performed. A high active share is not good news and a low one is not bad — it is the
 * size of the bet, and the only thing it predicts is how far the return CAN diverge. Rendering it
 * beside the excess-return tiles without that framing invites reading it as a score.
 *
 *  The individual stocks are treated as 100% OF THE PORTFOLIO — funds, cash and bonds dropped and
 * the rest renormalised, which is what the user asked for and the standard convention. `stocks_pct`
 * is printed rather than assumed: a book that is 40% ETFs has an active share describing 60% of
 * itself, and a figure whose denominator is invisible is a figure nobody can compare.
 *
 *  The rows are companies, not holdings. Two share classes fold into one line with their weights
 * summed — see `_active_share._issuer_key` — so the count here will not always match the Holdings
 * table's, and that is correct rather than a discrepancy.
 *
 *  And the word on screen is "COMPANY", not "issuer" (2026-08-25). The code still says issuer,
 * which is right there: `_issuer_key` folds share classes and ADRs onto one entity, and "issuer"
 * is the term for that entity in general. But every row this panel can produce IS a company —
 * funds, cash and bonds are dropped before the fold — so on screen "issuer" was jargon buying no
 * precision, and the table's own column header already said Company while the counts beside it
 * said issuers. Same rename in Concentration and Effective positions, which fold identically.
 */
import { Fragment, useMemo, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { chartTheme } from '../../../lib/chartTheme';
import { AspectCard } from '../../../lib/tipCard';
import InfoTip from '../InfoTip';
import { withWorked, subNum, texWords } from './workedFormula';
import { dayOf, dayRange } from './asOfLine';
import {
  Provenance, sourceField, sourceLabel, sourceVendor, type SourceKey,
} from '../../../lib/provenance';
import type { ActiveShare, ActiveShareRow } from '../../../lib/types/api';
import { useRiskCopy } from './riskCopy';
import LoadingDots from './LoadingDots';
import dynamic from 'next/dynamic';
import AirsWeightCalculation, { type AirsWeightComponent } from './AirsWeightCalculation';
import { riskRequestKey, useRiskResult } from './useRiskResult';

const riskLoading = () => <p role="status" className="text-sm text-fg-muted">Loading risk…</p>;
const TrackingErrorView = dynamic(() => import('./TrackingErrorView'), { loading: riskLoading });
const CorrelationView = dynamic(() => import('./CorrelationView'), { loading: riskLoading });
const VolatilityView = dynamic(() => import('./VolatilityView'), { loading: riskLoading });
const DrawdownView = dynamic(() => import('./DrawdownView'), { loading: riskLoading });
const ConcentrationView = dynamic(() => import('./ConcentrationView'), { loading: riskLoading });

/**
 * One body for all six risk views.
 *
 *  They are scale-free and read only `weight_pct`; the extra AIRS fields ride on the SAME object
 * anyway, deliberately: views assembled from subtly different holdings lists are exactly the failure the shared
 * `build_issuer_weights` / `build_paired_series` exist to prevent, one level up.
 */
export type ActiveShareHolding = {
  isin?: string | null;
  name?: string | null;
  weight_pct: number;
  /** Raw AIRS sizing at the two dates offered by the Risk-wide basis control. */
  weight_now_pct?: number | null;
  weight_start_pct?: number | null;
  is_fund?: boolean;
  bucket?: string | null;
  /** AIRS's own `current_value_eur`.  Absent on an ad-hoc basket — weights without euros. */
  value_eur?: number | null;
  /**  The LISTING's currency — the FX exposure actually borne, not the company's reporting one. */
  currency?: string | null;
};

/** Apply one AIRS weight snapshot without changing membership or any other holding fact. */
export function holdingsOnRiskBasis(
  holdings: ActiveShareHolding[], basis: 'now' | 'start',
): ActiveShareHolding[] {
  return holdings.map((holding) => ({
    ...holding,
    weight_pct: basis === 'start'
      ? (holding.weight_start_pct ?? 0)
      : (holding.weight_now_pct ?? holding.weight_pct),
  }));
}

/** Keep fixed-income certificates out of every equity-risk request, even when AIRS labels one Equity. */
function isDirectEquityStock(holding: ActiveShareHolding): boolean {
  return !holding.is_fund
    && (holding.bucket == null || holding.bucket === '' || holding.bucket === 'Equity')
    && !/\b(etf|etp|etn|ucits|fund|fonds|certificate|certificaten|index|perp|bond|obligat|coupon|treasur|gilt|bund|govie|sovereign|floating\s*rate|frn|high\s*yield|fixed\s*income)\b|\d[.,]?\d*\s*%/i.test(holding.name ?? '');
}

/**  TWO DECIMALS ON EVERY NON-INTEGER, ACROSS ALL SIX VIEWS. One decimal read as false
 *  precision on a figure the reader is asked to check against a table that carries two: "79.5%"
 *  beside rows summing to 79.53 invites the arithmetic to be redone and found wrong. Counts
 *  (issuers, observations, lines, periods) stay integers — they ARE integers. */
const pct2 = (v: number | null | undefined) => (v == null ? '—' : `${v.toFixed(2)}%`);
const signed = (v: number) => `${v >= 0 ? '+' : ''}${v.toFixed(2)}`;

/**  ONE ROW'S BAR IS SCALED TO THE LARGEST BET ON SCREEN, not to 100%. The biggest active weight
 *  in a diversified book is a couple of points, so a 0-100 scale renders every row as an invisible
 *  sliver — a chart that cannot be read is worse than the number alone. */
function Bar({ v, max }: { v: number; max: number }) {
  const w = max > 0 ? Math.min(100, (Math.abs(v) / max) * 100) : 0;
  return (
    <span className="inline-flex items-center w-full h-3" aria-hidden>
      <span className="relative w-full h-1.5 rounded-sm bg-overlay/[0.06]">
        <span
          className="absolute top-0 h-1.5 rounded-sm"
          style={{
            left: v >= 0 ? '50%' : `${50 - w / 2}%`,
            width: `${w / 2}%`,
            background: v >= 0 ? chartTheme.pos : chartTheme.neg,
          }} />
        <span className="absolute top-[-2px] left-1/2 h-[10px] w-px bg-neutral-700/60" />
      </span>
    </span>
  );
}

function Tile({ label, value, tone, info, simpleInfo, suffix }: {
  label: string; value: string; tone?: string; info?: React.ReactNode;
  simpleInfo?: string;
  /** Equation operator/result immediately following this tile. */
  suffix?: React.ReactNode;
}) {
  return (
    <>
      <div className="rounded-lg border border-neutral-800/40 bg-elevated px-4 py-2.5 min-w-[8.5rem]">
        <div className="text-sm text-fg-faint flex items-center gap-1">
          {label}{simpleInfo ? <InfoTip className="ml-0.5" text={simpleInfo} /> : info}
        </div>
        <div className={`font-mono text-2xl tabular-nums ${tone ?? 'text-fg-strong'}`}>{value}</div>
      </div>
      {suffix}
    </>
  );
}

export default function ActiveSharePanel({
  holdings, benchmark, portfolioName, portfolioAsOf, portfolioFetchedAt,
  portfolioSource, weightComponentsNow, weightTotalNow, riskProfilePolicy,
  portefeuille, benchmarkProfile,
}: {
  holdings: ActiveShareHolding[];
  portefeuille?: string | null;
  benchmark: string;
  benchmarkProfile?: string | null;
  /** The book's own name, so its date line names the thing it dates rather than saying "the book".
   *   It is on screen anyway (the modal's heading), which is what makes it the shortest possible
   *  label here — the reader does not have to work out which of the two lines is theirs. */
  portfolioName: string;
  /**
   *  The book's own valuation date, passed in rather than assumed. AIRS values end-of-day on
   * its own cadence, so "the weights" are as of whenever it last valued this book — Friday on a
   * Monday morning, older after a failed scrape. The card used to say "Today's weights", which
   * was an assumption printed as a fact; the reader can only judge whether it is current if they
   * are told the date.  It cannot be derived here: a holdings array carries no date, which is
   * exactly why the claim went unchecked for as long as it did.
   */
  portfolioAsOf?: string | null;
  /** The AIRS Beginwaarde snapshot date (normally 1 January of the analysis year). */
  portfolioStartAsOf?: string | null;
  /** When WE last read that valuation — a different fact from when AIRS produced it. */
  portfolioFetchedAt?: string | null;
  /**
   * Which AIRS scan these weights came from — a model portfolio's composition or an account's own
   * Vermogensoverzicht.
   *
   *  It is the caller's fact, not a default. The modal already knows (`isBasket`), and the two
   * scans are different objects read by different jobs; picking one here would print a source that
   * happens to be right for whichever kind of book was opened first.
   */
  portfolioSource: SourceKey;
  /** Canonical reviewed policy from the analysis response. It fixes the Stocks/Bonds benchmark for every
   * return-based risk view; Active Share and Concentration remain stock-sleeve measures. */
  riskProfilePolicy?: Record<string, number> | null;
  weightComponentsNow?: AirsWeightComponent[];
  weightComponentsStart?: AirsWeightComponent[];
  weightTotalNow?: number | null;
  weightTotalStart?: number | null;
  onClose: () => void;
}) {
  const t = useRiskCopy();
  /**  ALL ROWS, OR ONLY WHAT WE HOLD. The underweights are the other half of the measure — a book
   *  can be 70% active almost entirely by NOT owning things — but there are ~1,700 of them on ACWI,
   *  so the default is the book and the index's biggest gaps are one click away. */
  /**
   * Which of the two risk views is on screen.
   *
   *  One panel with a switch, not two buttons in the toolbar, and that is the point of it.
   * Active share is what the book LOOKS like against the index; tracking error is what that
   * difference has actually DONE. They are the same question from opposite ends — a book can be
   * 80% active and track closely (different names, same sectors) or 30% active and wander (few
   * bets, enormous ones) — so reading either as a proxy for the other is the standard mistake,
   * and putting them one click apart under one heading is what makes the pair legible.
   *
   *  The tracking-error side is mounted lazily. It costs a five-year daily price load for every
   * holding plus the tracker; most opens of this panel never switch to it, and paying for it on
   * every open would make the cheap view as slow as the expensive one.
   */
  const [view, setView] =
    useState<'active' | 'te' | 'corr' | 'vol' | 'dd' | 'conc'>('active');
  const weightedHoldings = useMemo(
    () => holdingsOnRiskBasis(holdings, 'now').filter(isDirectEquityStock), [holdings],
  );
  const riskBenchmarkWeights: Record<string, number> | undefined = riskProfilePolicy ? { ...riskProfilePolicy, EquityOnly: 1 } : undefined;
  const profileLabel = benchmarkProfile ? ({
    Neutraal: 'Neutral', Offensief: 'Offensive',
    'Beperkt Offensief': 'Moderately offensive', Defensief: 'Defensive',
  }[benchmarkProfile] ?? benchmarkProfile) : null;
  const riskHoldings = useMemo(
    () => holdingsOnRiskBasis(holdings, 'now').filter(isDirectEquityStock), [holdings],
  );
  const selectedPortfolioAsOf = portfolioAsOf;
  const benchmarkStart = null;
  const selectedWeightComponents = weightComponentsNow;
  const selectedWeightTotal = weightTotalNow;

  //  The body is the dependency, not an object identity. `holdings` is rebuilt on every render of
  // the parent, so depending on the array itself would refetch forever.
  const activeUrl = `${API_URL}/api/airs/portfolio/active-share?benchmark=${encodeURIComponent(riskProfilePolicy ? 'ACWI' : benchmark)}&weight_method=workbook-identity-v7`
    + `${benchmarkStart ? `&benchmark_start=${encodeURIComponent(benchmarkStart)}` : ''}&held_only=true`;
  const activeBody = JSON.stringify({
    holdings: riskBenchmarkWeights ? riskHoldings : weightedHoldings,
    benchmark_weights: riskBenchmarkWeights,
  });
  const key = riskRequestKey(activeUrl, activeBody);
  const { data, error } = useRiskResult<ActiveShare>(key, async () => {
    const r = await apiFetch(activeUrl, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: activeBody,
    });
    const b = await r.json().catch(() => null);
    if (!r.ok) throw new Error(b?.detail ?? `HTTP ${r.status}`);
    return b as ActiveShare;
  }, 'active-share', 'the active share could not be computed', view === 'active');
  const benchmarkLabel = riskBenchmarkWeights?.Stocks != null && riskBenchmarkWeights?.Bonds != null
    ? `${riskBenchmarkWeights.Stocks}% ACWI`
    : (data?.benchmark ?? benchmark);

  // The active-share response names a policy benchmark for people ("60% ACWI + 40% AGGH.AS").
  // The four return endpoints and Concentration still need the canonical ACWI key to resolve
  // their benchmark constituents; their request body carries the policy separately.
  const riskBenchmark = riskBenchmarkWeights ? 'ACWI' : (data?.benchmark ?? benchmark);

  //  `?? []` ON BOTH — every list on this payload is optional in the generated types (the
  // Pydantic default makes it so), and `.filter` on undefined is a blank panel with no error.
  const all: ActiveShareRow[] = data?.rows ?? [];
  const rows = all.filter((r) => r.held && !r.residual);
  // A policy comparator has two real asset sleeves. Keeping its bond ETFs among the company rows
  // obscures the allocation decision the policy benchmark is meant to make visible.
  const rowGroups = (() => {
    const stocks = rows.filter((row) => row.bucket === 'Stocks');
    const stockEtfs = rows.filter((row) => row.bucket === 'Stock ETFs');
    const bonds = rows.filter((row) => row.bucket === 'Bonds');
    if (!stockEtfs.length && !bonds.length) return [{ label: '', rows }];
    return [
      { label: 'Stocks', rows: stocks },
      { label: 'Stock ETFs', rows: stockEtfs },
      { label: 'Bonds', rows: bonds },
    ].filter((group) => group.rows.length > 0);
  })();
  const maxBet = Math.max(0, ...all.map((r) => Math.abs(r.active_pct ?? 0)));
  /**
   * The footer row's sums — over the ROWS ON SCREEN, not over `all`.
   *
   *  What they mean changes with the filter, and the row has to say which. Over EVERY name both
   * weight columns sum to 100% and Active sums to exactly zero — that zero is the reason active
   * share is halved, since every overweight has a matching underweight by construction. Over the
   * HELD names only, Active sums to the book's whole overweight, which is carried by the index
   * constituents not on screen. Both are useful; a footer that printed either without naming the
   * set would be read as the other.
   *
   *  AND ½ Σ|Active| RECONCILES TO THE TILE ABOVE **ONLY OVER EVERY NAME**. On the held subset
   * half the sum is missing, so it is not shown there rather than shown and quietly wrong.
   */
  /**
   * The two-sided date line, built once and shared by every tile that compares the book WITH the
   * index — active share, overlap, off-benchmark.
   *
   *  One string for the three, because they are three readings of ONE pair of inputs. Three
   * separate calls would be three places for the operands to drift apart, and a panel where two
   * tiles date the same weights differently is a panel nobody can reconcile.  The Stocks tile is
   * deliberately NOT here — no index appears in it, so it takes `whenBook`.
   */
  const whenBoth = t.active.whenWeights(
    portfolioName, dayOf(selectedPortfolioAsOf), dayOf(portfolioFetchedAt),
    data?.benchmark ?? benchmark,
    dayRange(data?.benchmark_caps_from, data?.benchmark_caps_to),
    data?.benchmark_caps_unstamped ?? 0);

  const totals = rows.reduce((a, r) => ({
    book: a.book + (r.portfolio_pct ?? 0),
    bench: a.bench + (r.benchmark_pct ?? 0),
    active: a.active + (r.active_pct ?? 0),
  }), { book: 0, bench: 0, active: 0 });

  // Reserve space for the full labels, not just percentage values. Both tables share the same
  // columns and horizontal scroll surface so the fixed totals cannot drift away from the rows.
  const portfolioColumnWidth = `max(10rem, ${portfolioName.length + 6}ch)`;
  const benchmarkColumnWidth = `max(12rem, ${(data?.benchmark ?? benchmark).length + 6}ch)`;
  const tableMinWidth = `calc(14rem + ${portfolioColumnWidth} + ${benchmarkColumnWidth} + 6rem + 7rem)`;
  const tableColumns = <colgroup>
    <col />
    <col style={{ width: portfolioColumnWidth }} />
    <col style={{ width: benchmarkColumnWidth }} />
    <col className="w-24" />
    <col className="w-28" />
  </colgroup>;

  return (
    <div className="relative h-full min-h-0 flex flex-col rounded-xl border border-neutral-800/40
      bg-card p-4">
      {/*  THE HEADER IS `shrink-0` AND OUTSIDE THE VIEW BRANCH — which is the whole point
          of the switch living here. It used to sit above content whose height changed with
          the selected view, so choosing "Tracking error" moved the very control that had
          just been clicked. A toggle that jumps out from under the pointer reads as a
          misclick even when it worked. */}
      <div className="relative shrink-0 text-center">
        <div className="min-w-0">
          <h4 className="text-sm font-semibold text-fg-strong">
            {`"${portfolioName}" vs ${profileLabel ? `the "${profileLabel}" benchmark (${benchmarkLabel})` : benchmarkLabel}`}
          </h4>
          <p className="text-xs text-fg-faint mt-0.5">
            {t.subtitle(portfolioName, selectedPortfolioAsOf)}
          </p>
          {/*  A TWO-POSITION SEGMENTED CONTROL, JOINED — which is the shape that means "exactly
              one of these". The filter chips elsewhere in this modal are separate pills because
              they mean "any of these"; borrowing that look here would promise both views at once.
              See the  on `Chip` in `TablesTab` for the same distinction. */}
          {/*  `flex-wrap`, NOT `inline-flex` — at six positions the row was wider than a
              narrow dialog, and an overflowing segmented control silently hides its last option.
              Wrapping keeps every view reachable; the joined look survives via the shared border. */}
          {riskBenchmarkWeights && <p className="mt-2 text-center text-xs text-fg-muted">Individual stocks at actual portfolio weights vs {riskBenchmarkWeights.Stocks}% ACWI. Bonds excluded; equity weights are not scaled to 100%.</p>}
          <div className="flex w-fit flex-wrap mx-auto mt-2 rounded-lg border border-neutral-800/40
            overflow-hidden max-w-full text-xs">
            {/*  FIVE RISK MEASURES, AND ATTRIBUTION IS DELIBERATELY NOT THE SIXTH. Every one
                of these describes the SHAPE of the book — how far it sits from the index, how far
                that gap has moved, how much of the movement is shared, how much it varies, how
                deep it has fallen, and how few names it is. Attribution DECOMPOSES the active
                return into allocation + selection + interaction, terms that sum to it exactly;
                none of these appears in that decomposition and none of them sums to anything.
                Putting it on this switch would imply they reconcile. It keeps its own dialog. */}
            {/*  THE KEYS DRIVE THE ORDER, THE COPY TABLE DRIVES THE WORDS — a label typed
                here in one language is exactly the drift `riskCopy`'s compile-time guarantee
                exists to prevent. */}
            {(['active', 'te', 'corr', 'vol', 'dd', 'conc'] as const).map((k) => (
              <button key={k} type="button" onClick={() => setView(k)}
                aria-pressed={view === k}
                className={`cursor-pointer px-3 py-1.5 font-medium whitespace-nowrap
                  transition-colors ${
                  view === k ? 'bg-accent-600 text-white'
                    : 'text-fg-subtle hover:bg-overlay/5'}`}>
                {t.views[k]}
              </button>
            ))}
          </div>
        </div>
      </div>

      {/*  EVERYTHING BELOW THE HEADER SCROLLS AS ONE. `min-h-0` is what lets it: without it the
          flex child refuses to shrink under its content and the dialog's fixed height gives way
          — see `PanelDialog`. */}
      <div className={`relative flex-1 min-h-0 pt-3 ${view === 'active'
        ? 'overflow-hidden' : 'overflow-auto space-y-3'}`}>

      {/*  MOUNTED ONLY WHILE SELECTED — unmounting drops its fetch, which is what makes the
          lazy load real rather than merely hidden.  Return-based views receive the policy sleeve;
          Active Share itself remains the deliberately narrower stock-sleeve measure. */}
      {view === 'te' && (
        <TrackingErrorView holdings={riskHoldings} benchmarkWeights={riskBenchmarkWeights}
          portefeuille={portefeuille}
          benchmark={riskBenchmark}
          //  The same book identity the active-share cards carry. This view measures the same
          // sleeve, so its cards owe the reader the same answer to "whose weights, read when, from
          // where" — and a second copy of those facts would be a second thing to keep true.
          portfolioName={portfolioName} portfolioAsOf={selectedPortfolioAsOf}
          portfolioFetchedAt={portfolioFetchedAt} portfolioSource={portfolioSource} />
      )}
      {view === 'corr' && (
        <CorrelationView holdings={riskHoldings} benchmarkWeights={riskBenchmarkWeights}
          benchmark={riskBenchmark}
          portfolioName={portfolioName} portfolioAsOf={selectedPortfolioAsOf}
          portfolioFetchedAt={portfolioFetchedAt} portfolioSource={portfolioSource} />
      )}
      {view === 'vol' && (
        <VolatilityView holdings={riskHoldings} benchmarkWeights={riskBenchmarkWeights}
          benchmark={riskBenchmark}
          portfolioName={portfolioName} portfolioAsOf={selectedPortfolioAsOf}
          portfolioFetchedAt={portfolioFetchedAt} portfolioSource={portfolioSource} />
      )}
      {view === 'dd' && (
        <DrawdownView holdings={riskHoldings} benchmarkWeights={riskBenchmarkWeights}
          benchmark={riskBenchmark}
          portfolioName={portfolioName} portfolioAsOf={selectedPortfolioAsOf}
          portfolioFetchedAt={portfolioFetchedAt} portfolioSource={portfolioSource} />
      )}
      {view === 'conc' && (
        <ConcentrationView holdings={weightedHoldings}
          benchmarkWeights={riskBenchmarkWeights}
          benchmark={riskBenchmark}
          benchmarkStart={benchmarkStart}
          portfolioName={portfolioName} portfolioAsOf={selectedPortfolioAsOf}
          portfolioFetchedAt={portfolioFetchedAt} portfolioSource={portfolioSource} />
      )}

      {/*  CENTRED, BECAUSE THE BOX NO LONGER SHRINKS TO THEM. One line of text in the top
          corner of a fixed 76vh panel reads as a render that failed halfway. */}
      {view === 'active' && (error || !data || !data.available) && (
        <div className="h-full grid place-items-center text-center px-6">
          {error ? <p className="text-xs text-neg-300">{error}</p>
            : !data ? <p className="text-2xl font-medium text-fg-strong">{t.common.computing} <LoadingDots /></p>
              : <p className="text-xs text-fg-muted max-w-md">{data.reason}</p>}
        </div>
      )}

      {view === 'active' && data?.available && (
        <div className="h-full min-h-0 flex flex-col gap-3">
          <div className="shrink-0 flex flex-wrap justify-center gap-2">
            {/*  `where` STAYS AT THE CALL SITE where it interpolates live counts — the copy
                table carries the sentences, not the arithmetic. A `where` with two numbers in it
                would need a formatter per language for no gain; these are counts and a benchmark
                name, which read the same in both. */}
            <Tile label={t.active.activeShare} value={pct2(data.active_share_pct)}
              suffix={riskBenchmarkWeights ? undefined : <span className="self-center text-base leading-none font-mono text-fg-faint shrink-0" aria-hidden>+</span>}
              simpleInfo={`Half the sum of absolute differences between actual stock weights and the profile-scaled ACWI weights. Bonds are excluded; neither equity allocation is scaled to 100%.`}
              info={<InfoTip className="ml-0.5" content={<AspectCard
                what={t.active.cards.activeShare.what}
                where={t.active.heldVsIndex(data.n_holdings ?? 0, data.benchmark_members ?? 0,
                  sourceLabel(portfolioSource),
                  sourceField('benchmark_caps'), sourceVendor('benchmark_caps'))}
                when={whenBoth}
                worked={data.active_share_pct == null ? '' : withWorked(
                  String.raw`\tfrac{1}{2}\sum_i \left| w_i^{\,p} - w_i^{\,b} \right|`,
                  riskBenchmarkWeights ? String.raw`= ${subNum(data.active_share_pct, 2)}\%` : String.raw`\text{overlap } ${subNum(data.overlap_pct ?? 0, 2)}\%`
                  + (riskBenchmarkWeights ? '' : String.raw` + \text{active } ${subNum(data.active_share_pct, 2)}\% = 100\%`))}
                legend={[
                  { sym: 'i', is: t.active.legend.issuer },
                  { sym: String.raw`w_i^{\,p}`, is: t.active.legend.wp(portfolioName) },
                  { sym: String.raw`w_i^{\,b}`, is: t.active.legend.wb(data.benchmark ?? benchmark) },
                ]}
                />} />} />
            <Tile label={t.active.overlap} value={pct2(data.overlap_pct)}
              suffix={riskBenchmarkWeights ? undefined : <><span className="self-center text-base leading-none font-mono text-fg-faint shrink-0" aria-hidden>=</span><Tile label="Total" value="100.00%" /></>}
              info={<InfoTip className="ml-0.5" content={<AspectCard
                what={t.active.cards.overlap.what}
                where={t.active.heldVsIndex(data.n_holdings ?? 0, data.benchmark_members ?? 0,
                  sourceLabel(portfolioSource),
                  sourceField('benchmark_caps'), sourceVendor('benchmark_caps'))}
                when={whenBoth}
                worked={data.overlap_pct == null ? '' : withWorked(
                  String.raw`\sum_i \min\!\left( w_i^{\,p},\; w_i^{\,b} \right)`,
                  riskBenchmarkWeights ? String.raw`= ${subNum(data.overlap_pct, 2)}\%` : String.raw`${subNum(data.overlap_pct, 2)}\% = 100\%`
                + String.raw` - \text{active } ${subNum(data.active_share_pct ?? 0, 2)}\%`)}
                legend={[
                  { sym: 'i', is: t.active.legend.issuer },
                  { sym: String.raw`\min`, is: t.active.legend.min },
                  { sym: String.raw`w_i^{\,p}`, is: t.active.legend.wp(portfolioName) },
                  { sym: String.raw`w_i^{\,b}`, is: t.active.legend.wb(data.benchmark ?? benchmark) },
                ]}
                how={riskBenchmarkWeights ? 'Sum the smaller actual weight for each company held on both sides. Different equity allocation totals mean overlap and active share need not add to 100%.' : t.active.cards.overlap.how} />} />} />
          </div>

          {/*  THE TABLE SCROLLS IN ITS OWN BOX AGAIN (2026-08-25), REVERSING THE NOTE THAT USED
              TO SIT HERE. That note said a cap would "nest one scrollbar inside another — and the
              outer one would have nothing to scroll". The first half is true and the second is
              not: the outer box still scrolls the tiles, the coverage note and the filter chips.
              What the cap buys is the only thing that reliably fixes a sticky header — a
              SCROLLPORT THE HEADER BELONGS TO. Pinned to the shared panel body, the header
              depended on `position: sticky` being honoured on a table section across the whole
              ancestor chain, and it was not: rows rode straight over it, through two attempts at
              fixing the paint order. Sticking a cell to the top of the box it actually lives in is
              the most-tested table pattern there is, and it is what the mobile rule in the project
              docs already asks for ("keep dense tables inside their own overflow-auto container").
               `max-h`, NOT `h` — a short book must not get a half-empty box with a scrollbar. */}
          <div className="flex-1 min-h-0 flex flex-col rounded-lg border border-neutral-800/40
            overflow-x-auto">
            <div className="flex min-h-0 flex-1 flex-col text-xs" style={{ minWidth: tableMinWidth }}>
            <div className="flex-1 min-h-0 overflow-auto [scrollbar-gutter:stable]">
              <table className="w-full table-fixed text-xs">
              {tableColumns}
              {/*  STICKY AND THE BACKGROUND BOTH SIT ON THE `<th>` CELLS, NOT ON `<thead>`, AND
                  THAT IS THE WHOLE FIX. Two separate reasons, either of which alone produces the
                  reported symptom — rows travelling over a header that should absorb them:

                  1. `position: sticky` ON A TABLE SECTION IS NOT UNIVERSALLY IMPLEMENTED. Several
                     engines honour it only on `<th>`/`<td>`, and one that ignores it on `<thead>`
                     lets the header scroll away with the rows rather than pinning at all.
                  2. Tailwind's preflight sets `border-collapse: collapse`, and under collapse a
                     background painted on `<thead>` or `<tr>` sits BELOW the cells of the rows
                     passing it. `#eef3fa` is fully opaque, so this was never a transparency
                     problem and darkening the token would not have fixed it — a `<th>` background
                     paints in the cell's own layer and covers what goes underneath.

                   `z-10` BECAUSE STICKY ALONE ONLY WINS AGAINST NON-POSITIONED CONTENT, and the
                  rows are not that: `Bar` uses `relative`/`absolute` for its bars, so its spans
                  paint in the positioned phase and would sit over a z-auto header. */}
              <thead>
                <tr className="text-fg-faint [&>th]:px-2.5 [&>th]:py-1 [&>th]:font-medium
                  [&>th]:sticky [&>th]:top-0 [&>th]:z-10 [&>th]:bg-inset [&>th]:whitespace-nowrap">
                  <th className="text-left">{t.active.colCompany}</th>
                  <th className="text-right whitespace-nowrap">{portfolioName}</th>
                  <th className="text-right">{data.benchmark ?? benchmark}</th>
                  <th className="text-right">{t.active.colActive}</th>
                  <th className="w-28" />
                </tr>
              </thead>
              <tbody>
                {rowGroups.map((group) => (
                  <Fragment key={group.label || 'positions'}>
                    {group.label && <tr>
                      <td colSpan={5} className="border-t border-neutral-700/50 bg-overlay/[0.03] px-2.5 py-1.5
                        text-[10px] font-semibold uppercase tracking-wider text-fg-faint">
                        {group.label}
                      </td>
                    </tr>}
                    {group.rows.map((r) => (
                  <tr key={r.name}
                    className="[&>td]:px-2.5 [&>td]:py-1 [&>td]:border-t [&>td]:border-neutral-800/20">
                    <td className="text-fg-soft">
                      {r.residual ? t.active.otherPositions : r.yahoo_symbol ? (
                        <a href={`https://finance.yahoo.com/quote/${encodeURIComponent(r.yahoo_symbol)}/`}
                          target="_blank" rel="noreferrer" title={`Open ${r.yahoo_symbol} on Yahoo Finance`}
                          className="hover:text-accent-300 hover:underline underline-offset-2">
                          {r.name}
                        </a>
                      ) : r.name}
                      {!r.held && !r.residual && (
                        <span className="ml-1.5 text-[10px] text-fg-faint">
                          {t.active.notHeld}
                        </span>
                      )}
                    </td>
                    <td className="text-right font-mono tabular-nums text-fg-muted">
                      {/* Every row reserves one identical provenance slot. The digits therefore
                          align independently of whether this particular row has an info icon. */}
                      <span className="inline-grid grid-cols-[auto_1.375rem] items-center
                        whitespace-nowrap">
                        <span>
                          {(r.portfolio_pct ?? 0) > 0
                            ? `${(r.portfolio_pct ?? 0).toFixed(2)}%` : '—'}
                        </span>
                        <span className="flex justify-end">
                          {r.held && !r.residual && selectedWeightTotal != null
                            && selectedWeightTotal > 0 && selectedWeightComponents?.length ? (
                            <Provenance source={portfolioSource} asOf={selectedPortfolioAsOf}
                              what={t.active.weightWhat(r.name, portfolioName)}
                              note={t.active.currentWeight}
                              how={t.active.weightHow}
                              calculation={<AirsWeightCalculation
                                components={selectedWeightComponents}
                                numerator={(r.portfolio_pct ?? 0) / 100 * selectedWeightTotal}
                                denominator={selectedWeightTotal}
                                result={`${(r.portfolio_pct ?? 0).toFixed(2)}%`}
                                holdingName={r.name}
                                valueLabel="Huidige waarde" />} />
                            ) : null}
                        </span>
                      </span>
                    </td>
                    <td className="text-right font-mono tabular-nums text-fg-muted">
                      <span className="inline-flex flex-row-reverse items-center gap-1 whitespace-nowrap">
                      {!r.residual && <span className="font-sans">
                        <InfoTip content={r.weight_calculation ? <div className="space-y-2 text-xs">
                          <p>{r.name}: iShares ACWI workbook weight on {r.weight_calculation.as_of}: {r.weight_calculation.initial_weight_pct.toFixed(4)}%.</p>
                          {(r.weight_calculation.prices ?? []).map((price, index) => <p key={index}>
                            {price.ticker}: €{price.start_price_eur?.toFixed(4)} ({price.start_date}) → €{price.end_price_eur?.toFixed(4)} ({price.end_date}).
                          </p>)}
                          <p>Yfinance EUR return: {r.weight_calculation.return_eur_pct.toFixed(4)}%.</p>
                          <p className="font-mono">{r.weight_calculation.initial_weight_pct.toFixed(4)} × (1 + {r.weight_calculation.return_eur_pct.toFixed(4)} / 100) ÷ {r.weight_calculation.normalisation_total.toFixed(4)} × 100 = {r.weight_calculation.result_pct.toFixed(4)}%</p>
                          <p>The denominator sums the workbook weights grown by each constituent’s EUR return.</p>
                          {riskBenchmarkWeights && <>
                            <p>ACWI represents {riskBenchmarkWeights.Stocks ?? 0}% of the policy benchmark. Apply that allocation to get this position’s weight in the full benchmark:</p>
                            <p className="font-mono">{r.weight_calculation.result_pct.toFixed(4)}% × ({riskBenchmarkWeights.Stocks ?? 0} / 100) = {(r.benchmark_pct ?? 0).toFixed(4)}% (displayed as {(r.benchmark_pct ?? 0).toFixed(2)}%).</p>
                          </>}
                        </div> : undefined} text={`${data.benchmark ?? benchmark}'s benchmark weight in ${r.name}.`} />
                      </span>}
                      {(r.benchmark_pct ?? 0) > 0 ? `${(r.benchmark_pct ?? 0).toFixed(2)}%`
                        : r.in_benchmark ? <span title="Index member; price unavailable for the weight calculation">{data.benchmark ?? benchmark}</span>
                          : <span title="Not a constituent of the policy benchmark; this is not a price-return field">—</span>}
                      </span>
                    </td>
                    {/*  NO TONE ON THE NUMBER ITSELF beyond direction. An overweight is not a
                        good thing and an underweight is not a bad one; the colour says which way
                        the bet goes, never whether it was right. */}
                    <td className={`text-right font-mono tabular-nums ${
                      (r.active_pct ?? 0) >= 0 ? 'text-pos-300' : 'text-neg-300'}`}>
                      {signed(r.active_pct ?? 0)}
                    </td>
                    <td className="pr-2.5"><Bar v={r.active_pct ?? 0} max={maxBet} /></td>
                  </tr>
                    ))}
                  </Fragment>
                ))}
              </tbody>
              </table>
            </div>
              {/*  FIXED BELOW THE SCROLL BOX. A sticky footer cannot rise from row 1,575; it only
                  sticks after its natural position reaches the bottom edge. Keeping this outside
                  makes the portfolio and benchmark sums visible in both table views. The repeated
                  colgroup keeps all numeric columns aligned with the rows above. */}
            <div className="shrink-0 overflow-y-auto [scrollbar-gutter:stable]">
            <table className="w-full table-fixed text-xs" aria-label="Active share totals">
              {tableColumns}
              <tfoot>
                <tr className="[&>td]:px-2.5 [&>td]:py-1.5 [&>td]:border-t
                  [&>td]:border-neutral-700/60 [&>td]:bg-inset font-medium">
                  <td className="text-fg-soft">
                    {t.active.totalHeld(rows.length)}
                    <InfoTip className="ml-1" content={<AspectCard
                      {...t.active.totalCardHeld}
                      when={whenBoth}
                      // This is a totals-row formula, not a second derivation of active share.
                      // It mirrors the three numeric cells immediately to its right.
                      worked={String.raw`\begin{aligned}`
                        + `${texWords(portfolioName)} &= `
                        + String.raw`\sum_{i\in S} w_i^{\,p}`
                        + String.raw` = ${totals.book.toFixed(2)}\% \\ `
                        + `${texWords(data.benchmark ?? benchmark)} &= `
                        + String.raw`\sum_{i\in S} w_i^{\,b}`
                        + String.raw` = ${totals.bench.toFixed(2)}\% \\ `
                        + String.raw`\text{Active} &= \sum_{i\in S}`
                        + String.raw`\left(w_i^{\,p}-w_i^{\,b}\right)`
                        + String.raw` = ${totals.book.toFixed(2)}\% - ${totals.bench.toFixed(2)}\%`
                        + String.raw` = ${totals.active.toFixed(2)}\%`
                        + String.raw`\end{aligned}`}
                      legend={[
                        { sym: 'i', is: t.active.legend.issuer },
                        { sym: 'S', is: t.active.legend.heldSet },
                        { sym: String.raw`w_i^{\,p}`, is: t.active.legend.wp(portfolioName) },
                        { sym: String.raw`w_i^{\,b}`,
                          is: t.active.legend.wb(data.benchmark ?? benchmark) },
                      ]} />} />
                  </td>
                  <td className="text-right font-mono tabular-nums text-fg">
                    <span className="inline-grid grid-cols-[auto_1.375rem] items-center
                      whitespace-nowrap">
                      <span>{`${totals.book.toFixed(2)}%`}</span>
                      <span aria-hidden />
                    </span>
                  </td>
                  <td className="text-right font-mono tabular-nums text-fg">
                    {`${totals.bench.toFixed(2)}%`}
                  </td>
                  {/*  NO TONE ON THE TOTAL. Over every name it is zero and a colour would imply a
                      direction; over the held subset it is always positive and a green would read
                      as a verdict on a number that is positive by construction. */}
                  <td className="text-right font-mono tabular-nums text-fg">
                    {signed(totals.active)}
                  </td>
                  <td />
                </tr>
              </tfoot>
            </table>
            </div>
            </div>
          </div>
        </div>
      )}
      </div>
    </div>
  );
}
