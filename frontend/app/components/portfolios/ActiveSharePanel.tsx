'use client';

/**
 * ACTIVE SHARE — how much of the book's stock sleeve is not the benchmark.
 *
 *     AS = ½ · Σ |wᵢᵖ − wᵢᵇ|          Overlap = Σ min(wᵢᵖ, wᵢᵇ) = 1 − AS
 *
 * ⚠⚠ IT IS A STRUCTURAL MEASURE, NOT A RETURN ONE, AND THE PANEL HAS TO SAY SO. Every other number
 * in this modal is about what happened; this one is about what the book IS, today, regardless of
 * how it has performed. A high active share is not good news and a low one is not bad — it is the
 * size of the bet, and the only thing it predicts is how far the return CAN diverge. Rendering it
 * beside the excess-return tiles without that framing invites reading it as a score.
 *
 * ⚠ THE INDIVIDUAL STOCKS ARE TREATED AS 100% OF THE PORTFOLIO — funds, cash and bonds dropped and
 * the rest renormalised, which is what the user asked for and the standard convention. `stocks_pct`
 * is printed rather than assumed: a book that is 40% ETFs has an active share describing 60% of
 * itself, and a figure whose denominator is invisible is a figure nobody can compare.
 *
 * ⚠ THE ROWS ARE COMPANIES, NOT HOLDINGS. Two share classes fold into one line with their weights
 * summed — see `_active_share._issuer_key` — so the count here will not always match the Holdings
 * table's, and that is correct rather than a discrepancy.
 *
 * ⚠ AND THE WORD ON SCREEN IS "COMPANY", not "issuer" (2026-08-25). The code still says issuer,
 * which is right there: `_issuer_key` folds share classes and ADRs onto one entity, and "issuer"
 * is the term for that entity in general. But every row this panel can produce IS a company —
 * funds, cash and bonds are dropped before the fold — so on screen "issuer" was jargon buying no
 * precision, and the table's own column header already said Company while the counts beside it
 * said issuers. Same rename in Concentration and Effective positions, which fold identically.
 */
import { useEffect, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { chartTheme } from '../../../lib/chartTheme';
import { AspectCard } from '../../../lib/tipCard';
import InfoTip from '../InfoTip';
import { traceError } from '../../../lib/debugTrace';
import { withWorked, subNum, workedRatio } from './workedFormula';
import { dayOf, dayRange } from './asOfLine';
import { sourceField, sourceLabel, sourceVendor, type SourceKey } from '../../../lib/provenance';
import type { ActiveShare, ActiveShareRow } from '../../../lib/types/api';
import { useRiskCopy } from './riskCopy';
import TrackingErrorView from './TrackingErrorView';
import CorrelationView from './CorrelationView';
import VolatilityView from './VolatilityView';
import DrawdownView from './DrawdownView';
import ConcentrationView from './ConcentrationView';

/**
 * ONE BODY FOR ALL SEVEN RISK VIEWS.
 *
 * ⚠⚠ SIX OF THEM ARE SCALE-FREE and read only `weight_pct`; `value_eur` and `currency` exist for
 * the Effective-positions view alone. They ride on the SAME object anyway, deliberately: seven
 * views assembled from seven slightly different holdings lists is exactly the failure the shared
 * `build_issuer_weights` / `build_paired_series` exist to prevent, one level up.
 */
export type ActiveShareHolding = {
  isin?: string | null;
  name?: string | null;
  weight_pct: number;
  is_fund?: boolean;
  /** AIRS's own `current_value_eur`. ⚠ Absent on an ad-hoc basket — weights without euros. */
  value_eur?: number | null;
  /** ⚠ The LISTING's currency — the FX exposure actually borne, not the company's reporting one. */
  currency?: string | null;
};

/** ⚠ TWO DECIMALS ON EVERY NON-INTEGER, ACROSS ALL SEVEN VIEWS. One decimal read as false
 *  precision on a figure the reader is asked to check against a table that carries two: "79.5%"
 *  beside rows summing to 79.53 invites the arithmetic to be redone and found wrong. Counts
 *  (issuers, observations, lines, periods) stay integers — they ARE integers. */
const pct2 = (v: number | null | undefined) => (v == null ? '—' : `${v.toFixed(2)}%`);
const signed = (v: number) => `${v >= 0 ? '+' : ''}${v.toFixed(2)}`;

/** ⚠ ONE ROW'S BAR IS SCALED TO THE LARGEST BET ON SCREEN, not to 100%. The biggest active weight
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

function Tile({ label, value, tone, info }: {
  label: string; value: string; tone?: string; info?: React.ReactNode;
}) {
  return (
    <div className="rounded-lg border border-neutral-800/40 bg-elevated px-3 py-2 min-w-[7.5rem]">
      <div className="text-[9px] uppercase tracking-wider text-fg-faint flex items-center gap-1">
        {label}{info}
      </div>
      <div className={`font-mono text-xl tabular-nums ${tone ?? 'text-fg-strong'}`}>{value}</div>
    </div>
  );
}

export default function ActiveSharePanel({
  holdings, benchmark, portfolioName, portfolioAsOf, portfolioFetchedAt, portfolioSource, onClose,
}: {
  holdings: ActiveShareHolding[];
  benchmark: string;
  /** The book's own name, so its date line names the thing it dates rather than saying "the book".
   *  ⚠ It is on screen anyway (the modal's heading), which is what makes it the shortest possible
   *  label here — the reader does not have to work out which of the two lines is theirs. */
  portfolioName: string;
  /**
   * ⚠⚠ THE BOOK'S OWN VALUATION DATE, PASSED IN RATHER THAN ASSUMED. AIRS values end-of-day on
   * its own cadence, so "the weights" are as of whenever it last valued this book — Friday on a
   * Monday morning, older after a failed scrape. The card used to say "Today's weights", which
   * was an assumption printed as a fact; the reader can only judge whether it is current if they
   * are told the date. ⚠ It cannot be derived here: a holdings array carries no date, which is
   * exactly why the claim went unchecked for as long as it did.
   */
  portfolioAsOf?: string | null;
  /** When WE last read that valuation — a different fact from when AIRS produced it. */
  portfolioFetchedAt?: string | null;
  /**
   * WHICH AIRS scan these weights came from — a model portfolio's composition or an account's own
   * Vermogensoverzicht.
   *
   * ⚠ IT IS THE CALLER'S FACT, NOT A DEFAULT. The modal already knows (`isBasket`), and the two
   * scans are different objects read by different jobs; picking one here would print a source that
   * happens to be right for whichever kind of book was opened first.
   */
  portfolioSource: SourceKey;
  onClose: () => void;
}) {
  const t = useRiskCopy();
  const [data, setData] = useState<ActiveShare | null>(null);
  const [error, setError] = useState<string | null>(null);
  /** ⚠ ALL ROWS, OR ONLY WHAT WE HOLD. The underweights are the other half of the measure — a book
   *  can be 70% active almost entirely by NOT owning things — but there are ~1,700 of them on ACWI,
   *  so the default is the book and the index's biggest gaps are one click away. */
  const [showAll, setShowAll] = useState(false);
  /**
   * Which of the two risk views is on screen.
   *
   * ⚠⚠ ONE PANEL WITH A SWITCH, NOT TWO BUTTONS IN THE TOOLBAR, and that is the point of it.
   * Active share is what the book LOOKS like against the index; tracking error is what that
   * difference has actually DONE. They are the same question from opposite ends — a book can be
   * 80% active and track closely (different names, same sectors) or 30% active and wander (few
   * bets, enormous ones) — so reading either as a proxy for the other is the standard mistake,
   * and putting them one click apart under one heading is what makes the pair legible.
   *
   * ⚠ THE TRACKING-ERROR SIDE IS MOUNTED LAZILY. It costs a five-year daily price load for every
   * holding plus the tracker; most opens of this panel never switch to it, and paying for it on
   * every open would make the cheap view as slow as the expensive one.
   */
  const [view, setView] =
    useState<'active' | 'te' | 'corr' | 'vol' | 'dd' | 'conc'>('active');

  // ⚠ THE BODY IS THE DEPENDENCY, NOT AN OBJECT IDENTITY. `holdings` is rebuilt on every render of
  // the parent, so depending on the array itself would refetch forever.
  const key = `${benchmark}|${holdings.length}|${holdings.reduce((s, h) => s + h.weight_pct, 0).toFixed(4)}`;

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      try {
        const r = await apiFetch(
          `${API_URL}/api/airs/portfolio/active-share?benchmark=${encodeURIComponent(benchmark)}`,
          { method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ holdings }) });
        const b = await r.json().catch(() => null);
        if (cancelled) return;
        if (!r.ok) { setError(b?.detail ?? `HTTP ${r.status}`); return; }
        setData(b as ActiveShare);
      } catch (e) {
        traceError('active-share', 'the active share could not be computed', e);
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { cancelled = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key]);

  // ⚠ `?? []` ON BOTH — every list on this payload is optional in the generated types (the
  // Pydantic default makes it so), and `.filter` on undefined is a blank panel with no error.
  const all: ActiveShareRow[] = data?.rows ?? [];
  const rows = all.filter((r) => (showAll ? true : r.held));
  const maxBet = Math.max(0, ...all.map((r) => Math.abs(r.active_pct ?? 0)));
  /**
   * The footer row's sums — over the ROWS ON SCREEN, not over `all`.
   *
   * ⚠⚠ WHAT THEY MEAN CHANGES WITH THE FILTER, AND THE ROW HAS TO SAY WHICH. Over EVERY name both
   * weight columns sum to 100% and Active sums to exactly zero — that zero is the reason active
   * share is halved, since every overweight has a matching underweight by construction. Over the
   * HELD names only, Active sums to the book's whole overweight, which is carried by the index
   * constituents not on screen. Both are useful; a footer that printed either without naming the
   * set would be read as the other.
   *
   * ⚠ AND ½ Σ|Active| RECONCILES TO THE TILE ABOVE **ONLY OVER EVERY NAME**. On the held subset
   * half the sum is missing, so it is not shown there rather than shown and quietly wrong.
   */
  /**
   * The two-sided date line, built once and shared by every tile that compares the book WITH the
   * index — active share, overlap, off-benchmark.
   *
   * ⚠ ONE STRING FOR THE THREE, because they are three readings of ONE pair of inputs. Three
   * separate calls would be three places for the operands to drift apart, and a panel where two
   * tiles date the same weights differently is a panel nobody can reconcile. ⚠ The Stocks tile is
   * deliberately NOT here — no index appears in it, so it takes `whenBook`.
   */
  const whenBoth = t.active.whenWeights(
    portfolioName, dayOf(portfolioAsOf), dayOf(portfolioFetchedAt),
    data?.benchmark ?? benchmark,
    dayRange(data?.benchmark_caps_from, data?.benchmark_caps_to),
    data?.benchmark_caps_unstamped ?? 0);

  const totals = rows.reduce((a, r) => ({
    book: a.book + (r.portfolio_pct ?? 0),
    bench: a.bench + (r.benchmark_pct ?? 0),
    active: a.active + (r.active_pct ?? 0),
    abs: a.abs + Math.abs(r.active_pct ?? 0),
  }), { book: 0, bench: 0, active: 0, abs: 0 });

  return (
    <div className="h-full min-h-0 flex flex-col rounded-xl border border-neutral-800/40
      bg-card p-4">
      {/* ⚠⚠ THE HEADER IS `shrink-0` AND OUTSIDE THE VIEW BRANCH — which is the whole point
          of the switch living here. It used to sit above content whose height changed with
          the selected view, so choosing "Tracking error" moved the very control that had
          just been clicked. A toggle that jumps out from under the pointer reads as a
          misclick even when it worked. */}
      <div className="shrink-0 flex items-start justify-between gap-3">
        <div>
          <h4 className="text-sm font-semibold text-fg-strong">
            {t.titles[view]} vs {data?.benchmark ?? benchmark}
          </h4>
          <p className="text-[11px] text-fg-faint mt-0.5">
            {t.subtitle}
          </p>
          {/* ⚠ A TWO-POSITION SEGMENTED CONTROL, JOINED — which is the shape that means "exactly
              one of these". The filter chips elsewhere in this modal are separate pills because
              they mean "any of these"; borrowing that look here would promise both views at once.
              See the ⚠ on `Chip` in `TablesTab` for the same distinction. */}
          {/* ⚠ `flex-wrap`, NOT `inline-flex` — at six positions the row was wider than a
              narrow dialog, and an overflowing segmented control silently hides its last option.
              Wrapping keeps every view reachable; the joined look survives via the shared border. */}
          <div className="flex flex-wrap mt-2 rounded-lg border border-neutral-800/40
            overflow-hidden max-w-full">
            {/* ⚠⚠ FIVE RISK MEASURES, AND ATTRIBUTION IS DELIBERATELY NOT THE SIXTH. Every one
                of these describes the SHAPE of the book — how far it sits from the index, how far
                that gap has moved, how much of the movement is shared, how much it varies, how
                deep it has fallen, and how few names it is. Attribution DECOMPOSES the active
                return into allocation + selection + interaction, terms that sum to it exactly;
                none of these appears in that decomposition and none of them sums to anything.
                Putting it on this switch would imply they reconcile. It keeps its own dialog. */}
            {/* ⚠ THE KEYS DRIVE THE ORDER, THE COPY TABLE DRIVES THE WORDS — a label typed
                here in one language is exactly the drift `riskCopy`'s compile-time guarantee
                exists to prevent. */}
            {(['active', 'te', 'corr', 'vol', 'dd', 'conc'] as const).map((k) => (
              <button key={k} type="button" onClick={() => setView(k)}
                aria-pressed={view === k}
                className={`cursor-pointer px-3 py-1 text-[11px] transition-colors ${
                  view === k ? 'bg-accent-600 text-white'
                    : 'bg-elevated text-fg-muted hover:text-accent-300'}`}>
                {t.views[k]}
              </button>
            ))}
          </div>
        </div>
        <button type="button" onClick={onClose}
          className="cursor-pointer text-fg-faint hover:text-fg text-sm leading-none px-1"
          aria-label={t.close}>×</button>
      </div>

      {/* ⚠ EVERYTHING BELOW THE HEADER SCROLLS AS ONE. `min-h-0` is what lets it: without it the
          flex child refuses to shrink under its content and the dialog's fixed height gives way
          — see `PanelDialog`. */}
      <div className="flex-1 min-h-0 overflow-auto pt-3 space-y-3">

      {/* ⚠ MOUNTED ONLY WHILE SELECTED — unmounting drops its fetch, which is what makes the
          lazy load real rather than merely hidden. It takes the SAME holdings, so both views
          describe one portfolio; see `compute_tracking_error`. */}
      {view === 'te' && (
        <TrackingErrorView holdings={holdings} benchmark={data?.benchmark ?? benchmark}
          // ⚠ THE SAME BOOK IDENTITY THE ACTIVE-SHARE CARDS CARRY. This view measures the same
          // sleeve, so its cards owe the reader the same answer to "whose weights, read when, from
          // where" — and a second copy of those facts would be a second thing to keep true.
          portfolioName={portfolioName} portfolioAsOf={portfolioAsOf}
          portfolioFetchedAt={portfolioFetchedAt} portfolioSource={portfolioSource} />
      )}
      {view === 'corr' && (
        <CorrelationView holdings={holdings} benchmark={data?.benchmark ?? benchmark}
          portfolioName={portfolioName} portfolioAsOf={portfolioAsOf}
          portfolioFetchedAt={portfolioFetchedAt} portfolioSource={portfolioSource} />
      )}
      {view === 'vol' && (
        <VolatilityView holdings={holdings} benchmark={data?.benchmark ?? benchmark}
          portfolioName={portfolioName} portfolioAsOf={portfolioAsOf}
          portfolioFetchedAt={portfolioFetchedAt} portfolioSource={portfolioSource} />
      )}
      {view === 'dd' && (
        <DrawdownView holdings={holdings} benchmark={data?.benchmark ?? benchmark}
          portfolioName={portfolioName} portfolioAsOf={portfolioAsOf}
          portfolioFetchedAt={portfolioFetchedAt} portfolioSource={portfolioSource} />
      )}
      {view === 'conc' && (
        <ConcentrationView holdings={holdings} benchmark={data?.benchmark ?? benchmark}
          portfolioName={portfolioName} portfolioAsOf={portfolioAsOf}
          portfolioFetchedAt={portfolioFetchedAt} portfolioSource={portfolioSource} />
      )}

      {/* ⚠ CENTRED, BECAUSE THE BOX NO LONGER SHRINKS TO THEM. One line of text in the top
          corner of a fixed 76vh panel reads as a render that failed halfway. */}
      {view === 'active' && (error || !data || !data.available) && (
        <div className="h-full grid place-items-center text-center px-6">
          {error ? <p className="text-xs text-neg-300">{error}</p>
            : !data ? <p className="text-xs text-fg-subtle">{t.common.computing}</p>
              : <p className="text-xs text-fg-muted max-w-md">{data.reason}</p>}
        </div>
      )}

      {view === 'active' && data?.available && (
        <>
          <div className="flex flex-wrap gap-2">
            {/* ⚠ `where` STAYS AT THE CALL SITE where it interpolates live counts — the copy
                table carries the sentences, not the arithmetic. A `where` with two numbers in it
                would need a formatter per language for no gain; these are counts and a benchmark
                name, which read the same in both. */}
            <Tile label={t.active.activeShare} value={pct2(data.active_share_pct)}
              info={<InfoTip className="ml-0.5" content={<AspectCard
                what={t.active.cards.activeShare.what}
                where={t.active.heldVsIndex(data.n_holdings ?? 0, data.benchmark_members ?? 0,
                  sourceLabel(portfolioSource),
                  sourceField('benchmark_caps'), sourceVendor('benchmark_caps'))}
                when={whenBoth}
                worked={data.active_share_pct == null ? '' : withWorked(
                  String.raw`\tfrac{1}{2}\sum_i \left| w_i^{\,p} - w_i^{\,b} \right|`,
                  String.raw`\text{overlap } ${subNum(data.overlap_pct ?? 0, 2)}\%`
                  + String.raw` + \text{active } ${subNum(data.active_share_pct, 2)}\% = 100\%`)}
                legend={[
                  { sym: 'i', is: t.active.legend.issuer },
                  { sym: String.raw`w_i^{\,p}`, is: t.active.legend.wp(portfolioName) },
                  { sym: String.raw`w_i^{\,b}`, is: t.active.legend.wb(data.benchmark ?? benchmark) },
                ]}
                />} />} />
            <Tile label={t.active.overlap} value={pct2(data.overlap_pct)}
              info={<InfoTip className="ml-0.5" content={<AspectCard
                what={t.active.cards.overlap.what}
                where={t.active.heldVsIndex(data.n_holdings ?? 0, data.benchmark_members ?? 0,
                  sourceLabel(portfolioSource),
                  sourceField('benchmark_caps'), sourceVendor('benchmark_caps'))}
                when={whenBoth}
                worked={data.overlap_pct == null ? '' : withWorked(
                  String.raw`\sum_i \min\!\left( w_i^{\,p},\; w_i^{\,b} \right)`,
                  String.raw`${subNum(data.overlap_pct, 2)}\% = 100\%`
                  + String.raw` - \text{active } ${subNum(data.active_share_pct ?? 0, 2)}\%`)}
                legend={[
                  { sym: String.raw`\min`, is: t.active.legend.min },
                  { sym: String.raw`w_i^{\,p}`, is: t.active.legend.wp(portfolioName) },
                  { sym: String.raw`w_i^{\,b}`, is: t.active.legend.wb(data.benchmark ?? benchmark) },
                ]}
                how={t.active.cards.overlap.how} />} />} />
            <Tile label={t.active.offBenchmark} value={pct2(data.off_benchmark_pct)}
              info={<InfoTip className="ml-0.5" content={<AspectCard
                what={t.active.cards.offBenchmark.what}
                where={t.active.offBenchWhere(
                  (data.n_holdings ?? 0) - (data.n_in_benchmark ?? 0), data.n_holdings ?? 0)}
                when={whenBoth}
                worked={data.off_benchmark_pct == null ? '' : withWorked(
                  String.raw`\sum_{i \,:\; w_i^{\,b} = 0} w_i^{\,p}`,
                  String.raw`${subNum(data.off_benchmark_pct, 2)}\%`)}
                legend={[
                  { sym: String.raw`w_i^{\,b} = 0`,
                    is: t.active.legend.notInBench(data.benchmark ?? benchmark) },
                  { sym: String.raw`w_i^{\,p}`, is: t.active.legend.wp(portfolioName) },
                ]}
                how={t.active.cards.offBenchmark.how} />} />} />
            <Tile label={t.active.stocks} value={pct2(data.stocks_pct)}
              tone="text-fg-muted"
              info={<InfoTip className="ml-0.5" content={<AspectCard
                what={t.active.cards.stocks.what}
                where={t.active.cards.stocks.where}
                // ⚠ THE BOOK'S DATE ALONE. No index appears in this figure, so dating the caps
                // beside it would date a side the number does not contain.
                when={t.active.whenBook(
                  portfolioName, dayOf(portfolioAsOf), dayOf(portfolioFetchedAt))}
                worked={withWorked(
                  String.raw`\dfrac{W_{\text{stocks}}}{W_{\text{book}}}`,
                  workedRatio(data.stocks_weight, data.total_weight,
                    pct2(data.stocks_pct), '%', '%'))}
                legend={[
                  { sym: String.raw`W_{\text{stocks}}`, is: t.active.legend.stocksNum },
                  { sym: String.raw`W_{\text{book}}`, is: t.active.legend.stocksDen },
                ]}
                how={t.active.cards.stocks.how} />} />} />
          </div>

          {/* ⚠ THE INDEX'S OWN COVERAGE, STATED WHENEVER ITS WEIGHTS ARE ON SCREEN — the same rule
              the composition charts follow. A constituent we cannot price does not lose its weight,
              it redistributes it across the rest, which makes active share read slightly LOW. */}
          {data.benchmark_covered_pct != null && data.benchmark_covered_pct < 99.5 && (
            <p className="text-[11px] text-fg-faint">
              {t.active.coverage(`${data.benchmark_covered_pct.toFixed(2)}%`,
                data.benchmark ?? benchmark)}
            </p>
          )}

          {(data.unresolved ?? []).length > 0 && (
            <p className="text-[11px] text-warn-300">
              {t.active.unmatched(
                (data.unresolved ?? []).length,
                pct2((data.unresolved ?? []).reduce((acc, u) => acc + (u.weight_pct ?? 0), 0)),
                (data.unresolved ?? []).slice(0, 4).map((u) => u.name ?? u.isin).join(', ')
                  + ((data.unresolved ?? []).length > 4 ? '…' : ''))}
            </p>
          )}

          <div className="flex items-center gap-2 pt-1">
            <button type="button" onClick={() => setShowAll(false)}
              className={`cursor-pointer rounded-md border px-2 py-0.5 text-[11px] transition-colors ${
                !showAll ? 'bg-accent-600 text-white border-transparent'
                  : 'bg-elevated border-neutral-800/40 text-fg-muted hover:text-accent-300'}`}>
              {t.active.heldOnly(data.n_holdings ?? 0)}
            </button>
            <button type="button" onClick={() => setShowAll(true)}
              title="Include the index names the book does not hold — the underweights are the other half of the measure."
              className={`cursor-pointer rounded-md border px-2 py-0.5 text-[11px] transition-colors ${
                showAll ? 'bg-accent-600 text-white border-transparent'
                  : 'bg-elevated border-neutral-800/40 text-fg-muted hover:text-accent-300'}`}>
              {t.active.everyName(all.length)}
            </button>
          </div>

          {/* ⚠⚠ THE TABLE SCROLLS IN ITS OWN BOX AGAIN (2026-08-25), REVERSING THE NOTE THAT USED
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
              ⚠ `max-h`, NOT `h` — a short book must not get a half-empty box with a scrollbar. */}
          <div className="rounded-lg border border-neutral-800/40 max-h-[55vh] overflow-auto">
            <table className="w-full text-xs">
              {/* ⚠⚠ STICKY AND THE BACKGROUND BOTH SIT ON THE `<th>` CELLS, NOT ON `<thead>`, AND
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

                  ⚠ `z-10` BECAUSE STICKY ALONE ONLY WINS AGAINST NON-POSITIONED CONTENT, and the
                  rows are not that: `Bar` uses `relative`/`absolute` for its bars, so its spans
                  paint in the positioned phase and would sit over a z-auto header. */}
              <thead>
                <tr className="text-fg-faint [&>th]:px-2.5 [&>th]:py-1 [&>th]:font-medium
                  [&>th]:sticky [&>th]:top-0 [&>th]:z-10 [&>th]:bg-inset">
                  <th className="text-left">{t.active.colCompany}</th>
                  <th className="text-right">{t.active.colBook}</th>
                  <th className="text-right">{data.benchmark ?? benchmark}</th>
                  <th className="text-right">{t.active.colActive}</th>
                  <th className="w-28" />
                </tr>
              </thead>
              <tbody>
                {rows.map((r) => (
                  <tr key={r.name}
                    className="[&>td]:px-2.5 [&>td]:py-1 [&>td]:border-t [&>td]:border-neutral-800/20">
                    <td className="text-fg-soft">
                      {r.name}
                      {!r.held && (
                        <span className="ml-1.5 text-[10px] text-fg-faint">
                          {t.active.notHeld}
                        </span>
                      )}
                    </td>
                    <td className="text-right font-mono tabular-nums text-fg-muted">
                      {(r.portfolio_pct ?? 0) > 0 ? `${(r.portfolio_pct ?? 0).toFixed(2)}%` : '—'}
                    </td>
                    <td className="text-right font-mono tabular-nums text-fg-muted">
                      {(r.benchmark_pct ?? 0) > 0 ? `${(r.benchmark_pct ?? 0).toFixed(2)}%` : '—'}
                    </td>
                    {/* ⚠ NO TONE ON THE NUMBER ITSELF beyond direction. An overweight is not a
                        good thing and an underweight is not a bad one; the colour says which way
                        the bet goes, never whether it was right. */}
                    <td className={`text-right font-mono tabular-nums ${
                      (r.active_pct ?? 0) >= 0 ? 'text-pos-300' : 'text-neg-300'}`}>
                      {signed(r.active_pct ?? 0)}
                    </td>
                    <td className="pr-2.5"><Bar v={r.active_pct ?? 0} max={maxBet} /></td>
                  </tr>
                ))}
              </tbody>
              {/* ⚠ STICKY TO THE BOTTOM OF THE SCROLL BOX, like the header is to the top. With
                  1,678 rows a plain `<tfoot>` is a footer nobody reaches, and the total is the
                  one row that reconciles this table to the tiles above it.
                  ⚠ A SOLID BACKGROUND IS LOAD-BEARING: the rows scroll UNDER it, and at any
                  alpha the digits of two rows overlap. */}
              {/* ⚠ SAME TREATMENT AS THE HEADER AND FOR BOTH THE SAME REASONS — see above. Rows
                  pass under this end too, so it carried the identical bug. */}
              <tfoot>
                <tr className="[&>td]:px-2.5 [&>td]:py-1.5 [&>td]:border-t
                  [&>td]:border-neutral-700/60 [&>td]:sticky [&>td]:bottom-0 [&>td]:z-10
                  [&>td]:bg-inset font-medium">
                  <td className="text-fg-soft">
                    {showAll ? t.active.totalAll(rows.length) : t.active.totalHeld(rows.length)}
                    <InfoTip className="ml-1" content={<AspectCard
                      {...(showAll ? t.active.totalCard : t.active.totalCardHeld)}
                      when={whenBoth}
                      // ⚠ THE SYMBOLIC HALF ONLY OVER EVERY NAME. On the held subset half the sum
                      // is missing, so ½ Σ|Active| is NOT the active share — printing the formula
                      // there would invite exactly the reconciliation the copy warns against.
                      worked={showAll
                        ? withWorked(
                          String.raw`\tfrac{1}{2}\sum_i \left| w_i^{\,p} - w_i^{\,b} \right|`,
                          String.raw`\tfrac{1}{2} \times ${totals.abs.toFixed(2)}`
                          + String.raw` = ${(totals.abs / 2).toFixed(2)}\%`)
                        : ''}
                      legend={showAll
                        ? [{ sym: String.raw`\left| w_i^{\,p} - w_i^{\,b} \right|`,
                          is: t.active.legend.absActive }]
                        : undefined} />} />
                  </td>
                  <td className="text-right font-mono tabular-nums text-fg">
                    {`${totals.book.toFixed(2)}%`}
                  </td>
                  <td className="text-right font-mono tabular-nums text-fg">
                    {`${totals.bench.toFixed(2)}%`}
                  </td>
                  {/* ⚠ NO TONE ON THE TOTAL. Over every name it is zero and a colour would imply a
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
        </>
      )}
      </div>
    </div>
  );
}
