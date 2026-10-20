'use client';

import { Fragment, useEffect, useRef, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { startJob } from '../../../lib/stores/jobs';
import type { ModelPortfolioAttribution } from '../../../lib/types/api';
import { Provenance, type SourceKey } from '../../../lib/provenance';
import { Holdings, type HoldingsProvenance } from './BucketDetailPanel';
import LoadingDots from './LoadingDots';
import InfoTip from '../InfoTip';
import { useAttributionCopy } from './attributionCopy';
import {
  workedAllocation, workedContribution, workedInteraction, workedReturn,
  workedSelection, workedTotal, workedWeight,
} from './attributionFormulas';
import AirsWeightCalculation from './AirsWeightCalculation';
import ReturnCalculation from './ReturnCalculation';
import BenchmarkMembershipMark from './BenchmarkMembershipMark';

/**
 * WHY the model beat or lagged the index — Brinson-Fachler.
 *
 * An excess return is a fact, not an explanation. "-11.60% vs ACWI" says nothing about whether
 * the bet that failed was the SECTORS chosen or the STOCKS chosen inside them — different
 * mistakes, different fixes:
 *
 *   ALLOCATION  did you tilt toward sectors that beat the index? (not merely "that went up" —
 *               overweighting a sector that rose 5% while the index rose 10% is a bad call)
 *   SELECTION   inside a sector, did your companies beat the index's companies?
 *
 * The panel says "sector", "region" or "currency" depending on the axis chosen — never the
 * jargon "bucket", and never "sector" while showing regions.
 *
 *  The three effects SUM to the excess. That identity is the whole point, and it is checked,
 * not assumed — if it ever fails, the table is three columns of numbers sitting next to each
 * other, and the banner says so instead of letting them be read as a decomposition.
 */
type Axis = 'sector' | 'region' | 'currency';
// The endpoint's top-level model has optional defaults in generated OpenAPI types. This panel
// consumes the normalized sleeves emitted by the attribution builder, so replace both extended
// payload fields rather than intersecting their older and newer generated shapes.
type AttributionWithExclusions = Omit<ModelPortfolioAttribution, 'excluded' | 'sleeves'> & {
  excluded?: Array<{ name?: string | null; asset_class?: string | null; weight_pct?: number; return_pct?: number | null; reason?: string | null }>;
  sleeves?: Array<{ bucket: string; portfolio_weight_pct: number; priced_weight_pct?: number; policy_weight_pct: number | null;
    portfolio_return_pct: number | null; comparator: string | null; comparator_return_pct: number | null;
    allocation_pct?: number | null; selection_pct?: number | null; interaction_pct?: number | null; impact_pct: number | null;
    holdings?: Array<{ isin?: string | null; name?: string | null; airs_name?: string | null; weight_pct: number; return_pct: number | null; contribution_pct: number }> }>;
};

/**
 * Decimals on every figure in this panel — one constant, because "all of it" is the requirement.
 *
 *  It was three precisions, and that is why this exists (2026-08-13, on request). `pct`/`pp`
 * defaulted to 2 but were CALLED with an explicit `1` in six places, and the weights were bare
 * `.toFixed(1)` literals in nine more — so one row showed `34.4` against a tooltip quoting
 * `34.38%`, and the arithmetic printed in the ⓘ ("wt × return = contribution") could not be
 * reproduced from the digits beside it. A default that every call site overrides is not a default.
 *
 *  THE ⓘ CARDS QUOTE THE SAME FORMATTERS, not their own `toFixed`. The whole point of printing
 * `(4.30% − 6.10%) × (…)` is that a reader can check it against the row; a card rounded differently
 * from the cells it explains is worse than no card.
 */
const DP = 2;

const pct = (v: number | null | undefined, dp = DP) =>
  v == null ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(dp)}%`;

/** Pydantic-defaulted fields come back optional in the generated types. A missing effect is 0 —
 *  it contributed nothing — which is a fact, not an unknown, so it is safe to default here. */
const n = (v: number | null | undefined) => v ?? 0;

/** `pp` — an effect is percentage POINTS, never percent. The excess is a difference of two
 *  returns; a `%` here would claim a different quantity. */
const pp = (v: number | null | undefined, dp = DP) =>
  v == null ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(dp)}pp`;

/**
 * A WEIGHT — bare digits, no unit and no sign.
 *
 *  No leading `+`, unlike `pct`/`pp`. A weight is a share of a base and cannot be negative, so a
 * sign on it would imply a direction it does not have; a return and an effect both can go either
 * way, which is why those two carry one. The `%` stays at the call site because the main table puts
 * it in the column header and the tooltips put it inline.
 */
const wt = (v: number | null | undefined, dp = DP) => n(v).toFixed(dp);

/**
 * A number with its OWN ⓘ.
 *
 *  THE ⓘ IS THE ONLY HOVER TARGET IN THIS PANEL, AND EVERY ONE OF THEM OPENS THE SAME CARD —
 * What · Source · When · How. Nothing else reacts to a pointer: not the value, not the column
 * label. That single rule is what makes the gesture worth learning, and it was arrived at by
 * removing two earlier answers. Free prose under each chip made the affordance unpredictable
 * (some opened a definition, some an essay, some a source). A second tooltip on the column label
 * was worse: two targets in one header, no way to tell which held the answer, so a reader found
 * one, read it, and concluded that was all there was.
 *
 *  The icon, not the number, is the trigger. Hovering a bare value is invisible — a tooltip
 * nobody knows is there is a tooltip that does not exist.
 */
function Num({ children, prov }: { children: React.ReactNode; prov: React.ReactNode }) {
  // `justify-end` inside a right-aligned cell: the icon is a fixed width and sits OUTSIDE the
  // digits, so the numeric column still lines up on its own right edge rather than on the icon.
  return (
    <span className="inline-flex items-center justify-end gap-1 whitespace-nowrap">
      {children}
      {prov}
    </span>
  );
}

/** An effect cell — allocation / selection / interaction / contribution, all of them a
 *  weight × return decomposition of the excess, all of them in `pp`. */
function Eff({ v, prov }: { v?: number | null; prov?: React.ReactNode }) {
  if (v == null) return <span className="text-fg-faint">—</span>;
  //  The threshold is derived from `DP`, NOT A LITERAL 0.005 THAT HAPPENS TO MATCH IT. It means
  // "this would print as zero", so it has to move with the precision — a hardcoded one drifts the
  // moment DP changes and leaves some "0.00pp" cells shown and others dashed, with no rule a
  // reader can infer. Same lesson as `composition.DISPLAY_EPSILON`.
  const body = Math.abs(v) < 0.5 / 10 ** DP
    ? <span className="text-fg-faint">—</span>
    : <span className={v >= 0 ? 'text-pos-400' : 'text-neg-400'}>{pp(v)}</span>;
  return prov ? <Num prov={prov}>{body}</Num> : body;
}

/**
 *  The word follows the axis. It is "sector" only when the axis IS sector — switch to Region and
 * every "sector" in this panel becomes a lie. "Bucket" was correct but it is jargon; naming the
 * thing the reader actually chose is both correct AND plain.
 */
/**
 * A column header. The LABEL is inert; the ⓘ beside it is the only thing to hover.
 *
 *  One target per header. The label used to carry its own dotted-underline tooltip explaining
 * what the column means, beside an ⓘ carrying What/Source/When/How — two hover targets, two
 * different cards, one header. A reader cannot see which of them holds the answer they want, so
 * they find one, read it, and stop; whichever they land on, they conclude that is all there is.
 * The ⓘ is now the single place, and anything the label's card said that its card did not must
 * be folded INTO it — deleting the second tooltip is only safe if nothing was left behind in it.
 */
function Th({ label, prov, align = 'right', simpleInfo }: {
  label: React.ReactNode;
  /** The column's ⓘ — What / Source / When / How. One per column; every cell repeats it with its
   *  own row's figures. */
  prov?: React.ReactNode;
  align?: 'left' | 'right';
  /** A header needs a definition, not the full per-value provenance card. */
  simpleInfo?: string;
}) {
  return (
    <th className={`px-2 py-1.5 align-top font-medium whitespace-normal break-words leading-tight ${align === 'left' ? 'text-left' : 'text-right'}`}>
      {label}
      {simpleInfo ? <InfoTip text={simpleInfo} /> : prov}
    </th>
  );
}

export function Names({ title, rows, hint, weightSrc, returnSrc, weightAsOf, returnAsOf,
  weightHow, returnHow,
  owner = 'the model', held = true, benchmark = 'the benchmark' }: {
  title: string;
  rows: NonNullable<ModelPortfolioAttribution['top_contributors']>;
  hint?: string;
  weightSrc: SourceKey;
  returnSrc: SourceKey;
  weightAsOf?: string | null;
  returnAsOf?: string | null;
  /**  The numerator of the weight, named exactly — the AIRS book weights by `Beginwaarde`, the
   *  model by its own stated percentage, and the index by start-of-window cap. Passed in rather
   *  than guessed here: all three land in the same three columns. */
  weightHow: string;
  /** How one holding's return is computed, in the same terms. */
  returnHow: string;
  /** Whose basket these weights are a share of — "the model", or the benchmark's name. */
  owner?: string;
  /** False for the index's winners you do NOT own: same columns, different meaning. */
  held?: boolean;
  benchmark?: string;
}) {
  const copy = useAttributionCopy();
  if (!rows?.length) return null;
  return (
    <div>
      <h5 className="text-[12px] font-semibold text-fg-strong">{title}</h5>
      {hint && <p className="text-[11px] text-fg-faint mb-1">{hint}</p>}
      {/* `table-fixed` + this colgroup pin the numeric columns to a fixed width and let the Name
          column take the rest and TRUNCATE — without it the table sizes to its content and, in the
          narrower dock, spills past its grid cell and overlaps the neighbouring list. */}
      <table className="w-full text-[12px] table-fixed">
        {/*  Widened for the per-cell ⓘ. Every numeric column now carries a 14px chip plus its
            gap OUTSIDE the digits, and at the old `w-9` the weight column could not fit "4.7%"
            and an icon — under `table-fixed` that does not wrap, it spills over the neighbour.
             AND WIDENED AGAIN, x1.2 (2026-08-13, on request), because the digits grew: every
            figure in this panel moved to two decimals (see `DP`), so "4.7%" became "4.70%" and
            "+59.2%" became "+59.24%" — two more glyphs in a column that was already sized to the
            character. `table-fixed` does not resize to fit, it OVERLAPS, so a precision change is
            a width change. 3rem -> 3.6rem, 4.5rem -> 5.4rem, exactly x1.2 on all three. */}
        <colgroup>
          <col />
          <col className="w-[3.6rem]" />
          <col className="w-[5.4rem]" />
          <col className="w-[5.4rem]" />
        </colgroup>
        {/* Three bare percentages in a row (10.0% · +59.2% · +5.92%) are unreadable without
            labels — worse than an unexplained header, because there is nothing to hover. */}
        <thead>
          <tr className="text-fg-faint text-[10px] uppercase tracking-wide">
            <th className="py-0.5 pr-2 text-left font-medium">{copy.headers.name}</th>
            <th className="py-0.5 px-1 text-right font-medium whitespace-nowrap">
              {copy.headers.weight}
              <Provenance source={weightSrc} column kind="copied" note={copy.prov.weightIn(owner)}
                what={copy.prov.eachShare(owner)}
                how={copy.prov.weightHow(weightHow, owner)}
                worked={workedWeight(null)} />
            </th>
            <th className="py-0.5 px-1 text-right font-medium whitespace-nowrap">
              {copy.headers.ret}
              <Provenance source={returnSrc} column kind="formula" note={copy.prov.returnNote}
                what={copy.prov.eachReturn}
                how={returnHow}
                worked={workedReturn(null)} />
            </th>
            <th className="py-0.5 pl-1 text-right font-medium whitespace-nowrap">
              {copy.headers.contribution}
              <Provenance source="derived" column kind="formula" note={copy.prov.contribution}
                what={copy.prov.eachContribution(owner, held)}
                how={copy.prov.contributionHow}
                worked={workedContribution(null, null, null)} />
            </th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr key={`${r.isin ?? r.ticker ?? r.name}`} className="border-t border-neutral-800/20">
              <td className="py-1 pr-2 text-fg truncate" title={r.name ?? r.ticker ?? r.isin ?? ''}>
                <span className="flex min-w-0 items-center gap-1.5">
                  <BenchmarkMembershipMark member={held && r.in_both} label={copy.names.shared(benchmark)} />
                  <span className="truncate">{r.name ?? r.ticker ?? r.isin}</span>
                </span>
              </td>
              <td className="py-1 px-1 text-right font-mono text-fg-subtle">
                <Num prov={<Provenance source={weightSrc} asOf={weightAsOf}
                  historical={weightSrc === 'benchmark_caps'}
                  what={copy.prov.share(r.name ?? r.ticker ?? r.isin ?? '', owner)}
                  note={copy.prov.weightIn(owner)}
                  how={copy.prov.weightHow(weightHow, owner)}
                  kind={r.weight_denominator_components?.length ? undefined : 'copied'}
                  worked={r.weight_denominator_components?.length ? undefined
                    : workedWeight(`${wt(r.weight_pct)}%`, r.weight_value_eur, r.weight_total_eur)}
                  calculation={r.weight_denominator_components?.length
                    && r.weight_value_eur != null && r.weight_total_eur != null
                    ? <AirsWeightCalculation components={r.weight_denominator_components}
                      numerator={r.weight_value_eur} denominator={r.weight_total_eur}
                      result={`${wt(r.weight_pct)}%`}
                      holdingName={r.airs_name ?? r.name ?? r.ticker ?? r.isin ?? 'Holding'}
                      rolledTo={r.weight_denominator_components.some(
                        (component) => component.base_value_eur != null) ? weightAsOf : undefined} />
                    : undefined} />}>
                  {wt(r.weight_pct)}%
                </Num>
              </td>
              <td className="py-1 px-1 text-right font-mono text-fg-subtle">
                <Num prov={<Provenance source={returnSrc} asOf={returnAsOf} kind="formula"
                  what={copy.prov.holdingReturn(r.name ?? r.ticker ?? r.isin ?? '')}
                  note={copy.prov.returnNote}
                  how={returnHow}
                  worked={workedReturn(pct(r.return_pct))} />}>
                  {pct(r.return_pct)}
                </Num>
              </td>
              <td className="py-1 pl-1 text-right font-mono font-semibold">
                <Eff v={r.contribution_pct}
                  prov={<Provenance source="derived" kind="formula"
                    what={copy.prov.holdingContribution(r.name ?? r.ticker ?? r.isin ?? '', owner, held)}
                    note={copy.prov.contributionNote(owner, held)}
                    how={copy.prov.contributionHow}
                    worked={workedContribution(`${wt(r.weight_pct)}%`, pct(r.return_pct),
                      pp(r.contribution_pct))} />} />
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

/**
 * The names behind ONE row of the attribution table: what you hold in that bucket beside what the
 * index holds, with each side's weights and returns.
 *
 *  The same table the sector-bar drill-down uses, imported rather than rebuilt — same columns,
 * same rank, same sort, same overlap treatment (a shared name tinted and dotted, the rest faded),
 * off the same payload. Two tables for one question is two things to learn.
 *
 *  Both lists are on the same base as the row above them. Portfolio positions retain their real
 * AIRS opening-book shares; benchmark constituents retain their real index shares. Each list adds
 * to the corresponding bucket row without pretending the visible stocks fill 100% of the book.
 *
 *  An empty side is a finding, not a blank. A bucket the index holds and you do not is an
 * allocation bet with no picks to judge — exactly what the row's Selection column says by being
 * 0.00pp. Saying so beats an empty box.
 */
function BucketNames({ row, bucket, benchmark, startLabel, portfolioProvenance,
  benchmarkProvenance }: {
  row: Pick<NonNullable<ModelPortfolioAttribution['rows']>[number],
    'portfolio_holdings' | 'benchmark_holdings'>;
  bucket: string;
  benchmark: string;
  portfolioProvenance: HoldingsProvenance;
  benchmarkProvenance: HoldingsProvenance;
  /**  WHEN the weights were measured — passed down because THIS panel has a window toggle and the
   *  `/bucket` drill-down does not. A YTD window opens on 1 January; a since-inception one opens on
   *  the model's own effective date. See `Holdings`'s `startLabel`. */
  startLabel: string;
}) {
  const copy = useAttributionCopy();
  const mine = row.portfolio_holdings ?? [];
  const theirs = row.benchmark_holdings ?? [];
  const hasSharedHolding = mine.some((h) => h.in_both);
  const sum = (rows: typeof mine) => rows.reduce((s, h) => s + n(h.weight_pct), 0);
  return (
    <div className="space-y-2">
      {hasSharedHolding && (
        <p className="text-[13px] text-fg-muted flex items-center gap-2">
          <span className="w-2 h-2 rounded-full bg-accent-500 inline-block shrink-0 ring-2 ring-accent-500/25" />
          {copy.names.shared(benchmark)}
        </p>
      )}
      <div className="grid gap-4 lg:grid-cols-2 lg:items-start">
        <div>
          <p className="text-base font-medium text-fg-muted mb-1.5">
            {copy.names.yourHoldings} <span className="text-fg-faint">({mine.length})</span>
            {mine.length > 0 && (
              <span className="text-fg-faint"> &middot; {wt(sum(mine))}%</span>
            )}
          </p>
          {mine.length
            ? <Holdings rows={mine} startLabel={startLabel} provenance={portfolioProvenance}
                />
            : (
              <p className="text-[12px] text-fg-subtle py-1">
                {copy.names.noneMine(bucket)}
              </p>
            )}
        </div>
        <div>
          <p className="text-base font-medium text-fg-muted mb-1.5">
            {benchmark} {copy.names.constituents} <span className="text-fg-faint">({theirs.length})</span>
            {theirs.length > 0 && (
              <span className="text-fg-faint"> &middot; {wt(sum(theirs))}%</span>
            )}
          </p>
          {theirs.length
            ? <Holdings rows={theirs} startLabel={startLabel} provenance={benchmarkProvenance}
                />
            : (
              <p className="text-[12px] text-fg-subtle py-1">
                {copy.names.noneIndex(benchmark, bucket)}
              </p>
            )}
        </div>
      </div>
    </div>
  );
}

export default function AttributionPanel({ id, benchmark, window, source = 'model',
  lookThrough = false, portfolioName, portfolioAsOf, benchmarkAsOf, onClose }: {
  id: number; benchmark: string; window: 'ytd' | 'since';
  source?: 'model' | 'book';
  /** Reader-facing nickname passed by the row that opened Analyse. */
  portfolioName?: string;
  /** Match the parent modal's certificate-membership toggle. Off by default. */
  lookThrough?: boolean;
  portfolioAsOf?: string | null; benchmarkAsOf?: string | null;
  onClose: () => void;
}) {
  const copy = useAttributionCopy();
  const [axis, setAxis] = useState<Axis>('sector');
  const [showExtraEffects, setShowExtraEffects] = useState(false);
  // Keep the fully-shaped response for every period while this panel is open. The backend also
  // prewarms and fingerprints these exact requests, but retaining the responses here means a
  // Jan → Feb → Jan click is synchronous: no spinner, no HTTP trip, no chance to recompute.
  const periodCache = useRef(new Map<string, AttributionWithExclusions>());
  const [loaded, setLoaded] = useState<{ key: string; value: AttributionWithExclusions } | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [refreshVersion, setRefreshVersion] = useState(0);
  const [refreshingPrices, setRefreshingPrices] = useState(false);
  const [monthOffset, setMonthOffset] = useState(0);
  const attributionMonths = Array.from({ length: new Date().getUTCMonth() + 1 }, (_, index) => {
    const d = new Date(Date.UTC(new Date().getUTCFullYear(), index, 1));
    return { value: d.toISOString().slice(0, 10), label: d.toLocaleDateString(copy.lang === 'nl' ? 'nl-NL' : 'en-GB', { month: 'long', year: 'numeric', timeZone: 'UTC' }) };
  });
  const selectedMonth = monthOffset ? attributionMonths[monthOffset - 1]?.value : undefined;
  // Keep browser responses compatible with the payload version. The server uses the matching
  // cache version so a deployed calculation upgrade cannot leave a live modal showing old maths.
  const requestKey = JSON.stringify({ attributionPayload: 'workbook-membership-markers-v4', id, benchmark, window, axis, source, lookThrough,
    month: selectedMonth ?? 'ytd', refreshVersion });
  const data = periodCache.current.get(requestKey)
    ?? (loaded?.key === requestKey ? loaded.value : null);
  /**
   * The bucket whose names are open, or null.
   *
   *  One at a time, and cleared when the axis changes. Bucket names are not unique across axes
   * — "Technology" is a sector and "United States" a region — so a key left over from the previous
   * axis would either open nothing or, worse, open a same-named bucket on a table it does not
   * belong to.
   */
  const [openBucket, setOpenBucket] = useState<string | null>(null);
  const [openSleeve, setOpenSleeve] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    const cached = periodCache.current.get(requestKey);
    if (cached) {
      setError(null);
      setLoaded({ key: requestKey, value: cached });
      return () => { cancelled = true; };
    }
    void (async () => {
      try {
        const r = await apiFetch(
          `${API_URL}/api/airs/model-portfolios/${id}/attribution`
          + `?benchmark=${encodeURIComponent(benchmark)}&window=${window}&axis=${axis}`
          + `&source=${source}&look_through=${lookThrough}`
          + (selectedMonth ? `&month=${encodeURIComponent(selectedMonth)}` : ''), { noReadCache: true });
        const b = await r.json().catch(() => null);
        if (cancelled) return;
        if (!r.ok) { setError(b?.detail ?? `HTTP ${r.status}`); return; }
        const value = b as AttributionWithExclusions;
        periodCache.current.set(requestKey, value);
        setError(null);
        setLoaded({ key: requestKey, value });
      } catch (e) {
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { cancelled = true; };
  }, [id, benchmark, window, axis, source, lookThrough, refreshVersion, selectedMonth, requestKey]);

  const refreshBenchmarkPrices = async () => {
    setRefreshingPrices(true);
    try {
      const { done } = await startJob(
        `${API_URL}/api/benchmarks/index/${encodeURIComponent(benchmark)}/refresh/job`,
        `Refresh ${benchmark} prices`,
      );
      const job = await done;
      if (job.status === 'done') {
        setError(null);
        // Price/cap data changed. Do not let a response calculated before the refresh survive in
        // the panel-local cache; the backend's data fingerprint will likewise reject its copy.
        periodCache.current.clear();
        setLoaded(null);
        setRefreshVersion((version) => version + 1);
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setRefreshingPrices(false);
    }
  };

  /**
   * The window, spelt out — this is the panel's heading and nothing else reads it.
   *
   *  Written out, not "YTD" (2026-08-13, on request). The heading was `Why — YTD vs SP500`, which
   * is three abbreviations and a dash standing in for a sentence: "Why" names no quantity, "vs"
   * does not say what is being compared, and the panel underneath is a Brinson decomposition rather
   * than a difference of two numbers. `Year-to-date performance attribution compared to SP500` says
   * what the table is.
   *
   *  The second window is hyphenated to match, because both are compound adjectives in front of
   * "performance attribution" — `Since-inception performance attribution compared to SP500`. Left
   * as "Since inception" it reads as a sentence fragment where its twin reads as a title.
   */
  const selectedPeriodLabel = selectedMonth ? attributionMonths[monthOffset - 1]?.label : null;
  const label = selectedPeriodLabel ?? (window === 'ytd' ? copy.chrome.ytd : copy.chrome.since);
  /**
   * WHEN the drill-down's weights were measured — the header under "Weight" in each names table.
   *
   *  Derived from the same `window` AS THE HEADING, so the two cannot disagree. A YTD window opens
   * on 1 January; a since-inception one opens on the model's own effective date, which for 27 of
   * the 56 models is somewhere inside this year. "Start of year" on that second case would be a
   * confident wrong date on a column a reader uses to check the arithmetic.
   */
  // The weight is measured at the WINDOW'S OPEN, not sometime during the named month. Showing
  // “October 2026” made the header look like an imprecise monthly average although the payload's
  // opening weights are specifically from 2026-10-01.
  const startDate = data?.start ?? selectedMonth;
  const startLabel = startDate
    ? new Intl.DateTimeFormat(copy.lang === 'nl' ? 'nl-NL' : 'en-GB', {
      day: 'numeric', month: 'long', year: 'numeric', timeZone: 'UTC',
    }).format(new Date(`${startDate.slice(0, 10)}T00:00:00Z`))
    : (window === 'ytd' ? copy.chrome.startYear : copy.chrome.inception);
  // ONE source for the word: the axis the server actually computed, NOT the picker's state. If
  // the two disagree — a response still in flight, a server that normalises an unknown axis —
  // the labels must describe the numbers ON SCREEN, not the request that asked for them.
  const axisKey = data?.axis ?? axis;
  const w = axisKey === 'sector' ? copy.axis.sector : axisKey === 'region' ? copy.axis.region
    : axisKey === 'currency' ? copy.axis.currency : copy.axis.group;
  // Both portfolio paths price returns from the same yfinance series as the benchmark. AIRS is
  // the source of the opening weight; keeping the sources separate prevents a return cell from
  // quietly claiming it came from VOLK.
  const pSrc: SourceKey = 'yfinance';
  //  The portfolio weight is a different quantity in the two sources, so a How that names one
  // is wrong for the other. The BOOK weights by `Beginwaarde` — the position's value when the
  // window opened, which is why a holding bought mid-window has weight 0 and drops out. The
  // MODEL weights by the composition's own stated percentage. Neither is renormalised after
  // certificates, funds, cash or unpriced positions are removed: the weight stays on the real
  // whole-book/model basis AIRS supplied.
  const isBook = (data?.source ?? source) === 'book';
  const pWeightSrc = isBook ? 'Beginwaarde' : 'the model’s stated weight';
  const pWeightSource: SourceKey = isBook ? 'airs_volk' : 'airs_model';
  //  Plain words, not an expression. This reaches a card's `how`, where the maths is now
  // typeset through `worked` — so a second, prose copy of the same division would be the two
  // ways of setting one formula the house style exists to prevent.
  const pReturnHow = 'the EUR close at the end of the window against the close at the start';
  const portfolioHoldingProvenance: HoldingsProvenance = {
    owner: isBook ? 'the complete AIRS book' : 'the complete AIRS model',
    nameSource: isBook ? 'airs_volk' : 'airs_model',
    currentWeightSource: isBook ? 'airs_volk' : 'airs_model',
    startWeightSource: pWeightSource,
    returnSource: pSrc,
    currentWeightAsOf: portfolioAsOf,
    startWeightAsOf: data?.start,
    returnAsOf: benchmarkAsOf,
    currentBasis: isBook
      ? 'Current EUR position value divided by the current EUR value of the complete AIRS book.'
      : 'The model source has no current position values, so this value is unavailable.',
    startBasis: isBook
      ? 'Every raw AIRS Beginwaarde shown below is added together. That sum is the denominator for this holding.'
      : 'The stated AIRS model weight, kept on the complete model basis without renormalising the remaining stocks.',
    returnBasis: pReturnHow,
  };
  const benchmarkHoldingProvenance: HoldingsProvenance = {
    owner: benchmark,
    nameSource: 'benchmark_caps',
    currentWeightSource: 'benchmark_caps',
    startWeightSource: 'benchmark_caps',
    returnSource: 'benchmark',
    currentWeightAsOf: benchmarkAsOf,
    startWeightAsOf: data?.start,
    returnAsOf: benchmarkAsOf,
    currentBasis: `Current constituent market cap divided by the total current market cap represented by ${benchmark}.`,
    startBasis: `Constituent market cap at the start of the window divided by ${benchmark}'s total start market cap.`,
    returnBasis: 'The EUR close at the end of the window against the close at the start.',
  };
  const hasRows = (data?.rows?.length ?? 0) > 0;
  // Bonds are priced against their dedicated comparator (normally AGGH.AS), rather than
  // ACWI. They therefore belong in this view as one sleeve-level row, not as a made-up
  // stock-sector Brinson decomposition.
  const bondsSleeve = (data?.sleeves ?? []).find((sleeve) => sleeve.bucket === 'Bonds');
  const attributionYear = selectedPeriodLabel ?? data?.start?.slice(0, 4) ?? new Date().getUTCFullYear().toString();
  // The footer is the sum of the rows the table actually renders. Bonds is a separate sleeve
  // because its comparator is not ACWI, but it is still a displayed row and cannot disappear
  // from a column total just because its calculation is specialised.
  const portfolioWeightTotal = (data?.rows ?? [])
    .reduce((sum, row) => sum + n(row.portfolio_weight_pct), 0)
    + n(bondsSleeve?.portfolio_weight_pct);
  const benchmarkWeightTotal = (data?.rows ?? [])
    .reduce((sum, row) => sum + n(row.benchmark_weight_pct), 0)
    + n(bondsSleeve?.policy_weight_pct);
  const allocationColumnTotal = (data?.rows ?? [])
    .reduce((sum, row) => sum + n(row.allocation_pct), 0)
    + n(bondsSleeve?.allocation_pct);
  const selectionColumnTotal = (data?.rows ?? [])
    .reduce((sum, row) => sum + n(row.selection_pct), 0)
    + n(bondsSleeve?.selection_pct);
  const interactionColumnTotal = (data?.rows ?? [])
    .reduce((sum, row) => sum + n(row.interaction_pct), 0)
    + n(bondsSleeve?.interaction_pct);
  const totalColumnTotal = allocationColumnTotal + selectionColumnTotal + interactionColumnTotal
    + n(data?.fund_implementation_residual_pct);
  // One table, on one whole-portfolio basis. Index-only sectors stay in it because choosing not
  // to own one is a real allocation decision; non-sector sleeves complete the portfolio to 100%.
  const rankedRows = [...(data?.rows ?? [])]
    .sort((a, b) => Math.abs(n(b.total_pct)) - Math.abs(n(a.total_pct)));
  const excludedGroups = (data?.excluded ?? []).reduce<Record<string, {
    weight: number; returnContribution: number; hasMissingReturn: boolean;
  }>>((groups, item) => {
    const label = item.reason === 'cash' ? 'Cash'
      : item.asset_class === 'Bonds' ? 'Bonds'
        : item.asset_class === 'Alternatives' ? 'Alternatives'
          : item.reason === 'fund' ? 'Stock ETFs' : 'Other holdings';
    const group = groups[label] ?? { weight: 0, returnContribution: 0, hasMissingReturn: false };
    group.weight += n(item.weight_pct);
    if (item.return_pct == null) group.hasMissingReturn = true;
    else group.returnContribution += n(item.weight_pct) * item.return_pct;
    groups[label] = group;
    return groups;
  }, {});
  const excludedWeightTotal = Object.values(excludedGroups).reduce((sum, group) => sum + group.weight, 0);
  const excludedReturn = (group: { weight: number; returnContribution: number; hasMissingReturn: boolean }) =>
    group.hasMissingReturn || group.weight === 0 ? null : group.returnContribution / group.weight;

  return (
    <section className="h-full min-h-0 flex flex-col bg-card border border-accent-500/30
      rounded-xl p-4">
      {/*  `shrink-0`, AND IT CARRIES THE AXIS PICKER. Sized to its content, this dialog
          resized every time the axis changed — moving the select the reader had just used
          out from under the pointer. See `PanelDialog` for the fixed box. */}
      <div className="relative shrink-0 mb-2">
        <div className="w-full">
          <h4 className="text-sm font-semibold text-fg-strong">
            {copy.chrome.title(label, benchmark, portfolioName || data?.name || undefined)}
          </h4>
          <p className="mt-1 text-xs text-fg-subtle">{copy.chrome.subtitle(label)}</p>
          <div className="mt-4 flex justify-center py-2"><div className="inline-flex max-w-full overflow-x-auto rounded-lg border border-neutral-800/40 text-xs" aria-label="Attribution period">
            {[{ label: 'YTD', value: 0 }, ...attributionMonths.map((month, index) => ({ label: month.label.split(' ')[0].slice(0, 3), value: index + 1 }))].map((period) => <button
              key={period.value} type="button" onClick={() => { setError(null); setOpenBucket(null); setMonthOffset(period.value); }}
              className={`shrink-0 px-3 py-1.5 font-medium transition-colors ${monthOffset === period.value ? 'bg-accent-600 text-white' : 'text-fg-subtle hover:bg-overlay/5'}`}
              aria-pressed={monthOffset === period.value}>{period.label}</button>)}
          </div></div>
        </div>
        <div className="absolute right-0 top-0 flex items-center gap-2 shrink-0">
          <button type="button" aria-pressed={showExtraEffects} onClick={() => setShowExtraEffects((show) => !show)}
            className="rounded-lg border border-neutral-700 px-2 py-1 text-[12px] text-fg-subtle hover:bg-overlay/5">
            {showExtraEffects ? copy.chrome.hideExtraEffects : copy.chrome.showExtraEffects}
          </button>
          <select value={axis}
            onChange={(e) => { setError(null); setOpenBucket(null); setAxis(e.target.value as Axis); }}
            className="bg-page border border-neutral-700 rounded-lg px-2 py-1 text-[12px] text-fg focus:border-accent-500">
            <option value="sector">{copy.axis.bySector}</option>
            <option value="region">{copy.axis.byRegion}</option>
            <option value="currency">{copy.axis.byCurrency}</option>
          </select>
        </div>
      </div>

      {/*  `min-h-0` OR THE FIXED HEIGHT GIVES WAY. A flex item defaults to `min-height:auto`,
          which refuses to shrink below its content, so `overflow-auto` here would be ignored and
          the section would grow instead — silently, and only for the longest tables. */}
      <div className="flex-1 min-h-0 overflow-x-auto overflow-y-scroll [scrollbar-gutter:stable]">
      {error && (
        <div className="bg-neg-500/10 border border-neg-500/20 rounded-lg px-3 py-2 text-xs text-neg-300">
          {copy.lang === 'nl' ? copy.chrome.error : error}
        </div>
      )}
      {!data && !error && (
        <div className="h-full grid place-items-center">
          <p className="text-xs text-fg-subtle">{copy.chrome.loading} <LoadingDots /></p>
        </div>
      )}

      {data && (
        <>
          {data.note && (
            <div className="flex flex-wrap items-center gap-2 mb-2">
              <p className="text-[12px] text-fg-muted">{data.note}</p>
              {data.needs_benchmark_price_refresh && (
                <button type="button" onClick={() => void refreshBenchmarkPrices()}
                  disabled={refreshingPrices}
                  className="cursor-pointer rounded-lg border border-accent-500/50 px-2 py-1 text-[12px] text-accent-300 disabled:cursor-wait disabled:opacity-60">
                  {refreshingPrices ? 'Refreshing prices…' : `Refresh ${benchmark} prices`}
                </button>
              )}
            </div>
          )}
          {data && (data.sleeves?.length ?? 0) > 0 && false && (
            <div className="mb-4 overflow-x-auto rounded-xl border border-neutral-800/50 bg-card shadow-sm">
              <div className="min-w-[700px]">
                <div className="grid grid-cols-[minmax(150px,1fr)_84px_84px_88px_110px_98px_88px] gap-2 border-b border-accent-500/20 bg-accent-500/[0.06] px-4 py-3 text-[10px] font-medium uppercase tracking-wide text-fg-faint">
                  <span>Sleeve</span><span className="text-right">You</span><span className="text-right">Policy</span><span className="text-right">Return</span><span>Compared with</span><span className="text-right">Benchmark</span><span className="text-right">Impact</span>
                </div>
                <div className="divide-y divide-neutral-800/25">
                  {(data?.sleeves ?? []).map((sleeve) => {
                    const sleeveHoldings = sleeve.holdings ?? [];
                    const expandable = sleeveHoldings.length > 0;
                    const open = openSleeve === sleeve.bucket;
                    return <Fragment key={sleeve.bucket}>
                      <button type="button" disabled={!expandable}
                        onClick={() => expandable && setOpenSleeve(open ? null : sleeve.bucket)}
                        className={`grid w-full grid-cols-[minmax(150px,1fr)_84px_84px_88px_110px_98px_88px] items-center gap-2 px-4 py-3 text-left transition-colors ${expandable ? 'cursor-pointer hover:bg-accent-500/[0.05]' : 'cursor-default'}`}>
                        <span className="flex items-center gap-2 text-[13px] font-medium text-fg">{expandable && <span className="grid h-5 w-5 place-items-center rounded-full bg-inset text-[10px] text-fg-faint">{open ? '▾' : '▸'}</span>}{sleeve.bucket}</span>
                        <span className="text-right font-mono text-[12px] text-fg">{wt(sleeve.portfolio_weight_pct)}%</span>
                        <span className="text-right font-mono text-[12px] text-fg-soft">{sleeve.policy_weight_pct == null ? '—' : `${wt(sleeve.policy_weight_pct)}%`}</span>
                        <span className="text-right font-mono text-[12px] text-fg">{pct(sleeve.portfolio_return_pct)}</span>
                        <span>{sleeve.comparator ? <span className="inline-flex rounded-full border border-accent-500/25 bg-accent-500/[0.08] px-2 py-0.5 text-[11px] font-medium text-accent-200">{sleeve.comparator}</span> : <span className="text-[11px] text-fg-faint">Not benchmarked</span>}</span>
                        <span className="text-right font-mono text-[12px] text-fg-soft">{pct(sleeve.comparator_return_pct)}</span>
                        <span className={`text-right font-mono text-[12px] font-semibold ${sleeve.impact_pct == null ? 'text-fg-faint' : sleeve.impact_pct >= 0 ? 'text-pos-300' : 'text-neg-300'}`}>{sleeve.impact_pct == null ? '—' : pp(sleeve.impact_pct)}</span>
                      </button>
                      {open && <div className="border-t border-neutral-800/25 bg-inset/40 px-4 py-4">
                        <div className="mb-3 flex items-center justify-between"><p className="text-[12px] font-medium text-fg">Holdings in {sleeve.bucket}</p><span className="rounded-full bg-card px-2 py-0.5 text-[11px] text-fg-faint">{sleeveHoldings.length} instruments</span></div>
                        <div className="divide-y divide-neutral-800/25 overflow-hidden rounded-lg border border-neutral-800/35 bg-card">
                          <div className="grid grid-cols-[minmax(0,1fr)_80px_84px_84px] gap-2 bg-overlay/[0.02] px-3 py-2 text-[10px] uppercase tracking-wide text-fg-faint">
                            <span>Instrument</span><span className="text-right">Weight</span><span className="text-right">Return</span><span className="text-right">Contribution</span>
                          </div>
                          {sleeveHoldings.map((holding) => <div key={holding.isin ?? holding.name}
                            className="grid grid-cols-[minmax(0,1fr)_80px_84px_84px] gap-2 px-3 py-2 text-[12px]">
                            <span className="truncate text-fg">{holding.name ?? holding.airs_name ?? 'Unknown holding'}</span><span className="text-right font-mono text-fg-soft">{wt(holding.weight_pct)}%</span>
                            <span className="text-right font-mono text-fg-soft">{pct(holding.return_pct)}</span><span className={`text-right font-mono ${holding.return_pct == null ? 'text-fg-faint' : holding.contribution_pct >= 0 ? 'text-pos-300' : 'text-neg-300'}`}>{holding.return_pct == null ? '—' : pp(holding.contribution_pct)}</span>
                          </div>)}
                        </div>
                      </div>}
                    </Fragment>;
                  })}
                </div>
              </div>
            </div>
          )}
          {/*  The identity IS the decomposition. If it fails, these are just three columns. */}
          {hasRows && !data.reconciles && (
            <p className="text-[12px] text-neg-300 mb-2">
              {/*  THE ONE FIGURE IN THIS PANEL THAT IS NOT AT `DP`, AND DELIBERATELY SO. Every
                  other number here is a quantity a reader compares; this one is the PROOF that the
                  three columns are a decomposition, and it only ever appears when that proof has
                  failed. At two decimals a real 0.004pp break prints "+0.00%" — a banner announcing
                  a failure while showing zero, which reads as the banner being wrong rather than
                  the table. It stays at four. */}
              {copy.chrome.residual(pct(data.residual_pct, 4))}
            </p>
          )}

          {/*  NO COVERAGE NOTES HERE — ALL FOUR REMOVED ON REQUEST 2026-08-05, not overlooked.
              What they said is still true and the fields are still in the payload:
                • `unattributed_excess_pct` — this table decomposes the ATTRIBUTABLE SLEEVE, not
                  the account: cash, income on positions closed during the year and the account’s
                  own flows are real return with no bucket to attribute them to.
                • the SOLD share — a position sold out has no opening weight, so a sector traded
                  out of entirely reads here as one that was never owned. A FALSE finding, not a
                  missing one.
                • `unpriced_pct` — the same false finding from a different cause.
                • `excluded_pct` — funds and cash, correctly out (a fund has no sector).
              Every one of those is still computed and still on the wire; none of them is on
              screen. Read a row here as a statement about the sleeve that COULD be attributed,
              never about the account. */}

          {/*  NO FORMULA STRIP HERE. Each effect's arithmetic lives in its OWN column header's
              info icon (`Th prov` → `Provenance how`), stated in the SAME WORDS the headers use
              so the two cannot drift — a strip above the table restated all three permanently,
              so the panel carried every formula twice. */}
          {hasRows && <>
          <div className="hidden mb-3 overflow-x-auto rounded-lg border border-neutral-800/40 bg-inset/30">
            <div className="min-w-[660px]">
            <div className="grid grid-cols-[minmax(150px,1fr)_72px_72px_88px_88px_88px] gap-2 border-b border-neutral-800/40 px-3 py-2 text-[10px] uppercase tracking-wide text-fg-faint">
              <span>{w}</span><span className="text-right">Your weight</span><span className="text-right">{benchmark} weight</span><span className="text-right">Your return</span><span className="text-right">{benchmark} return</span><span className="text-right">Impact</span>
            </div>
            <div className="divide-y divide-neutral-800/25">
              {rankedRows.map((row) => {
                const open = openBucket === row.bucket;
                const impact = n(row.total_pct);
                return <Fragment key={`summary-${row.bucket}`}>
                  <button type="button" onClick={() => setOpenBucket(open ? null : row.bucket)}
                    className="grid w-full grid-cols-[minmax(150px,1fr)_72px_72px_88px_88px_88px] items-center gap-2 px-3 py-2 text-left hover:bg-overlay/[0.03]">
                    <span className="truncate text-[12px] text-fg">{row.bucket}</span>
                    <span className="text-right font-mono text-[12px] text-fg-soft">{wt(row.portfolio_weight_pct)}%</span>
                    <span className="text-right font-mono text-[12px] text-fg-soft">{wt(row.benchmark_weight_pct)}%</span>
                    <span className="text-right font-mono text-[12px] text-fg-soft">{pct(row.portfolio_return_pct)}</span>
                    <span className="text-right font-mono text-[12px] text-fg-soft">{pct(row.benchmark_return_pct)}</span>
                    <span className={`text-right font-mono text-[12px] font-semibold ${impact >= 0 ? 'text-pos-300' : 'text-neg-300'}`}>{pp(impact)}</span>
                  </button>
                  {open && <div className="border-t border-neutral-800/25 bg-card px-3 py-3">
                    <BucketNames row={row} bucket={row.bucket} benchmark={benchmark}
                      startLabel={startLabel} portfolioProvenance={portfolioHoldingProvenance}
                      benchmarkProvenance={benchmarkHoldingProvenance} />
                  </div>}
                </Fragment>;
              })}
              {Object.entries(excludedGroups).sort(([a], [b]) => a.localeCompare(b)).map(([label, group]) => (
                <div key={label} className="grid grid-cols-[minmax(150px,1fr)_72px_72px_88px_88px_88px] items-center gap-2 px-3 py-2 text-[12px]">
                  <span className="truncate text-fg-soft">{label}</span>
                  <span className="text-right font-mono text-fg-soft">{wt(group.weight)}%</span>
                  <span className="text-right font-mono text-fg-faint">0.00%</span>
                  <span className="text-right font-mono text-fg-soft">{pct(excludedReturn(group))}</span>
                  <span className="text-right font-mono text-fg-faint">{label === 'Stock ETFs' ? pct(data.benchmark_return_pct) : label === 'Cash' ? pct(0) : '—'}</span>
                  <span className="text-right text-fg-faint">—</span>
                </div>
              ))}
              <div className="grid grid-cols-[minmax(150px,1fr)_72px_72px_88px_88px_88px] items-center gap-2 border-t border-neutral-800/40 px-3 py-2 text-[12px] font-semibold">
                <span className="text-fg">Total</span>
                <span className="text-right font-mono text-fg">{wt(portfolioWeightTotal + excludedWeightTotal)}%</span>
                <span className="text-right font-mono text-fg">{wt(benchmarkWeightTotal)}%</span>
                <span />
                <span />
                <span className="text-right font-mono text-fg">{pp(data.attributed_pct)}</span>
              </div>
            </div>
            </div>
          </div>
          {data && <>
          <div className="overflow-x-auto rounded-lg border border-neutral-800/40 mb-3">
            {/* Fixed columns keep the table still while YTD/month/axis requests replace their
                values. Without an explicit layout the browser sized each response from its
                labels and numbers, so clicking Jan visibly shifted every column. */}
            <table className={`w-full table-fixed text-[12px] ${showExtraEffects ? 'min-w-[1600px]' : 'min-w-[1250px]'}`}>
              <colgroup>
                <col className="w-[12%]" />
                <col span={showExtraEffects ? 8 : 6} />
              </colgroup>
              <thead className="sticky top-0 z-[1] bg-card">
                <tr className="text-fg-faint text-[11px] uppercase tracking-wide border-b border-neutral-800/40">
                  <Th align="left" label={w}
                    simpleInfo={`The ${w}s used to group your holdings and ${benchmark}'s constituents.`}
                    prov={<Provenance source="derived" column kind="formula" note={`the ${w}s`}
                      what={`The ${w}s the excess is split across — including the ones ${benchmark} holds and you do NOT, because choosing not to own something is a decision the numbers can price.`}
                      how={copy.prov.bucketsHow(w)} />} />
                  {/*  WORDS, NOT `w_P` / `R_B`. The subscripted notation was readable only while
                      the formula strip below carried its key (w = weight, R = return, P = you,
                      B = index); with that strip gone the symbols arrive undecoded, and the
                      `uppercase` on this row was flattening the lowercase `w` that carries half
                      the convention. A header that needs a hover to be read at all is a header
                      that will be guessed at instead. The tooltip formulas use the same words, so
                      the two cannot drift apart the way symbols and a missing key did. */}
                  <Th label={copy.headers.yourWeight(attributionYear)}
                    prov={<InfoTip text="Your opening weight in each sector, measured at the start of the selected period." />} />
                  <Th label={`Benchmark weight at start of ${attributionYear}`}
                    simpleInfo={`${benchmark}'s opening weight in each ${w}.`}
                    prov={<Provenance source="derived" column kind="formula"
                      what={`${benchmark}'s share in each ${w}, at the START of the window — weighting by today’s cap would be look-ahead.`}
                      note={`policy benchmark weight in this ${w}`}
                      how={`${benchmark}'s sector weight, scaled to the policy Stocks allocation.`}
                      worked={workedWeight(null)} />} />
                  <Th label={copy.headers.yourReturn(attributionYear)}
                    simpleInfo={`The EUR return of your holdings in each ${w} since the start of the selected period.`}
                    prov={<Provenance source={pSrc} column kind="formula"
                      what={`What your directly held stocks in each ${w} returned, in EUR. ETF sector weights are included in allocation, but an ETF has no observable sector return.`}
                      note={`your direct-stock return in this ${w}`}
                      how={copy.prov.yourReturnHow(pReturnHow)}
                      worked={workedReturn(null)} legend={copy.prov.returnLegend(copy.prov.yours)} />} />
                  {/* The reference point. Allocation is scored against THIS number, so it has to
                      be on the screen — an over/underweight is judged by whether its sector beat
                      or lagged the index as a whole, not by whether it went up. */}
                  <Th label={copy.headers.indexReturn(attributionYear)}
                    simpleInfo={`The EUR return of ${benchmark}'s holdings in each ${w} since the start of the selected period.`}
                    prov={<Provenance source="benchmark" column kind="formula"
                      what={`What ${benchmark}'s holdings in each ${w} returned, in EUR.`}
                      note={`${benchmark} return in this ${w}`}
                      how={copy.prov.indexReturnHow(benchmark)}
                      worked={workedReturn(null)} legend={copy.prov.returnLegend(benchmark)} />} />
                  <Th label={copy.headers.allocation}
                    prov={<Provenance source="derived" column kind="formula" note="Brinson-Fachler allocation"
                      what={`What choosing where to put the money was worth — scored against the index total, so a ${w} that rose by LESS than the index counts against you.`}
                      how={copy.prov.allocationHow}
                      worked={workedAllocation(null, null, null,
                        pct(data.benchmark_return_pct), null)}
                      legend={copy.prov.effectLegend(benchmark, w)} />} />
                  <Th label={copy.headers.selection}
                    prov={<Provenance source="derived" column kind="formula" note="Brinson selection"
                      what="What choosing which companies to hold was worth, scored at the index’s weight so sizing is held constant."
                      how={copy.prov.selectionHow}
                      worked={workedSelection(null, null, null, null)}
                      legend={copy.prov.effectLegend(benchmark, w)} />} />
                  {showExtraEffects && <><Th label={copy.headers.interaction}
                    prov={<Provenance source="derived" column note="interaction (the cross term)"
                      what={copy.prov.interactionWhat}
                      where={copy.prov.interactionWhere}
                      when={copy.prov.interactionWhen}
                      how={copy.prov.interactionHow}
                      worked={workedInteraction(null, null, null, null, null)}
                      legend={copy.prov.effectLegend(benchmark, w)} />} />
                  <Th label={copy.headers.total}
                    prov={<Provenance source="derived" column kind="formula" note={`this ${w}'s share of the excess`}
                      what={`Each ${w}'s whole share of the excess. The column sums to the excess, and that identity is checked, not assumed.`}
                      how={copy.prov.totalHow}
                      worked={workedTotal(null, null, null, null)} />} /></>}
                </tr>
              </thead>
              <tbody className="divide-y divide-neutral-800/20">
                {(data.rows ?? []).map((r) => {
                  // The row's own figures, formatted once — every card below quotes them, so a
                  // reader can check the arithmetic against the digits in the row.
                  const wP = n(r.portfolio_weight_pct);
                  const wB = n(r.benchmark_weight_pct);
                  const acwiSectorWeight = n(r.benchmark_index_weight_pct);
                  const benchmarkStocksWeight = n(r.benchmark_stock_weight_pct);
                  const etfWeight = n(r.lookthrough_weight_pct);
                  const directWeight = Math.max(0, wP - etfWeight);
                  const weightHow = <>
                    <p>{copy.prov.yourWeightHow(pWeightSrc)}</p>
                    <p className="mt-1">Direct stocks: {wt(directWeight)}%; verified ETF look-through: {wt(etfWeight)}%; together: {wt(wP)}%.</p>
                    {etfWeight > 0 && <div className="mt-2 overflow-x-auto rounded border border-neutral-700/50">
                      <table className="w-full min-w-[420px] text-[11px]">
                        <thead className="bg-inset/50 text-fg-faint">
                          <tr><th className="px-2 py-1 text-left font-medium">ETF</th><th className="px-2 py-1 text-right font-medium">ETF weight</th><th className="px-2 py-1 text-right font-medium">{r.bucket} split</th><th className="px-2 py-1 text-right font-medium">In sector</th></tr>
                        </thead>
                        <tbody className="divide-y divide-neutral-800/40">
                          {(r.lookthrough_funds ?? []).map((fund, index) => <tr key={`${fund.isin ?? fund.name}-${index}`}>
                            <td className="px-2 py-1 text-fg-soft">{fund.name ?? fund.isin ?? 'ETF'}</td>
                            <td className="px-2 py-1 text-right font-mono">{wt(fund.fund_weight_pct)}%</td>
                            <td className="px-2 py-1 text-right font-mono">{wt(fund.sector_pct)}%</td>
                            <td className="px-2 py-1 text-right font-mono text-fg">{wt(fund.sector_weight_pct)}%</td>
                          </tr>)}
                        </tbody>
                      </table>
                    </div>}
                  </>;
                  const benchmarkWeightHow = <>
                    <p>{copy.prov.indexWeightHow(benchmark)}</p>
                    <div className="mt-2 overflow-x-auto rounded border border-neutral-700/50">
                      <table className="w-full min-w-[360px] text-[11px]">
                        <tbody className="divide-y divide-neutral-800/40">
                          <tr><th className="bg-inset/50 px-2 py-1 text-left font-medium text-fg-faint">ACWI sector weight</th><td className="px-2 py-1 text-right font-mono">{wt(acwiSectorWeight)}%</td></tr>
                          <tr><th className="bg-inset/50 px-2 py-1 text-left font-medium text-fg-faint">Benchmark allocation to stocks</th><td className="px-2 py-1 text-right font-mono">{wt(benchmarkStocksWeight)}%</td></tr>
                          <tr><th className="bg-inset/50 px-2 py-1 text-left font-medium text-fg-faint">Benchmark sector weight</th><td className="px-2 py-1 text-right font-mono text-fg">{wt(wB)}%</td></tr>
                        </tbody>
                      </table>
                    </div>
                    <p className="mt-1">{wt(acwiSectorWeight)}% × {wt(benchmarkStocksWeight)}% = {wt(wB)}%.</p>
                  </>;
                  const rP = pct(r.portfolio_return_pct);
                  const rB = pct(r.benchmark_return_pct);
                  const rBt = pct(data.benchmark_return_pct);
                  const open = openBucket === r.bucket;
                  return (
                    <Fragment key={r.bucket}>
                    {/*  THE WHOLE ROW IS THE HIT TARGET, not a chevron in the first cell. Every
                        figure on it belongs to the bucket the drill-down explains, so any of them
                        is a reasonable place to click and ask "which names is this?". */}
                    <tr onClick={() => setOpenBucket(open ? null : r.bucket)}
                      title={open ? copy.row.hide(r.bucket) : copy.row.show(r.bucket, benchmark)}
                      className={`cursor-pointer transition-colors ${
                        open ? 'bg-accent-500/[0.07]' : 'hover:bg-overlay/[0.02]'}`}>
                      <td className="px-2 py-1.5 text-fg whitespace-nowrap overflow-hidden text-ellipsis">
                        <span className={`inline-block w-3 text-[10px] ${open ? 'text-accent-400' : 'text-fg-faint'}`}>
                          {open ? '▾' : '▸'}
                        </span>
                        {r.bucket}
                      </td>
                      <td className="px-2 py-1.5 text-right font-mono text-fg-subtle">
                        <Num prov={<Provenance source={pWeightSource} asOf={data.start} kind="formula"
                          what={`Your real AIRS opening-book share held in ${r.bucket}.`}
                          how={weightHow}
                          worked={workedWeight(`${wt(wP)}%`)} />}>
                          {wt(wP)}%
                        </Num>
                      </td>
                      <td className="px-2 py-1.5 text-right font-mono text-fg-subtle">
                        <Num prov={<Provenance source="benchmark_caps" asOf={data.start} historical kind="formula"
                          what={`The policy benchmark's share held in ${r.bucket}.`}
                          note={`${benchmark} sector weight, scaled by the Stocks policy allocation`}
                          how={benchmarkWeightHow}
                          worked={workedWeight(`${wt(wB)}%`)} />}>
                          {wt(wB)}%
                        </Num>
                      </td>
                      <td className="px-2 py-1.5 text-right font-mono text-fg-subtle">
                        <Num prov={<Provenance source={pSrc} asOf={benchmarkAsOf} kind="formula"
                          what={`What your ${r.bucket} holdings returned, in EUR.`}
                          note={`your return in ${r.bucket}`}
                          how={copy.prov.yourReturnHow(pReturnHow)}
                          worked={workedReturn(rP)} legend={copy.prov.returnLegend(copy.prov.yours)}
                          calculation={<ReturnCalculation rows={r.portfolio_holdings ?? []}
                            result={rP} bucket={r.bucket} owner={copy.prov.yours} />} />}>
                          {rP}
                        </Num>
                      </td>
                      <td className="px-2 py-1.5 text-right font-mono text-fg-subtle">
                        <Num prov={<Provenance source="benchmark" asOf={benchmarkAsOf} kind="formula"
                          what={`What ${benchmark}'s ${r.bucket} holdings returned, in EUR.`}
                          note={`${benchmark} return in ${r.bucket}`}
                          how={copy.prov.indexReturnHow(benchmark)}
                          worked={workedReturn(rB)} legend={copy.prov.returnLegend(benchmark)}
                          calculation={<ReturnCalculation rows={r.benchmark_holdings ?? []}
                            result={rB} bucket={r.bucket} owner={benchmark} />} />}>
                          {rB}
                        </Num>
                      </td>
                      <td className="px-2 py-1.5 text-right font-mono">
                        <Eff v={n(r.allocation_pct)}
                          prov={<Provenance source="derived" kind="formula" note={`allocation — ${r.bucket}`}
                            what={`What your ${r.bucket} over/underweight was worth, scored against the index total (${rBt}).`}
                            how={copy.prov.allocationHow}
                            worked={workedAllocation(`${wt(wP)}%`, `${wt(wB)}%`, rB, rBt,
                              pp(r.allocation_pct))}
                            legend={copy.prov.effectLegend(benchmark, w)} />} />
                      </td>
                      <td className="px-2 py-1.5 text-right font-mono">
                        <Eff v={n(r.selection_pct)}
                          prov={<Provenance source="derived" kind="formula" note={`selection — ${r.bucket}`}
                            what={`What your ${r.bucket} company picks were worth, scored at the index’s weight.`}
                            how={copy.prov.selectionHow}
                            worked={workedSelection(`${wt(wB)}%`, rP, rB, pp(r.selection_pct))}
                            legend={copy.prov.effectLegend(benchmark, w)} />} />
                      </td>
                      {showExtraEffects && <><td className="px-2 py-1.5 text-right font-mono">
                        <Eff v={n(r.interaction_pct)}
                          prov={<Provenance source="derived" note={`interaction — ${r.bucket}`}
                            what={copy.prov.interactionWhatFor(r.bucket)}
                            where={copy.prov.interactionWhere}
                            when={copy.prov.interactionWhen}
                            how={copy.prov.interactionHow}
                            worked={workedInteraction(`${wt(wP)}%`, `${wt(wB)}%`, rP, rB,
                              pp(r.interaction_pct))}
                            legend={copy.prov.effectLegend(benchmark, w)} />} />
                      </td>
                      <td className="px-2 py-1.5 text-right font-mono font-semibold">
                        <Eff v={n(r.total_pct)}
                          prov={<Provenance source="derived" kind="formula" note={`${r.bucket}'s share of the excess`}
                            what={`${r.bucket}'s whole share of the excess.`}
                            how={`${pp(r.allocation_pct)} + ${pp(r.selection_pct)} + ${pp(r.interaction_pct)} = ${pp(r.total_pct)}.`} />} />
                      </td></>}
                    </tr>
                    {open && (
                      <tr className="bg-inset/60">
                        <td colSpan={showExtraEffects ? 9 : 7} className="px-3 py-3">
                          <BucketNames row={r} bucket={r.bucket} benchmark={benchmark}
                            startLabel={startLabel} portfolioProvenance={portfolioHoldingProvenance}
                            benchmarkProvenance={benchmarkHoldingProvenance} />
                        </td>
                      </tr>
                    )}
                    </Fragment>
                  );
                })}
                {bondsSleeve && (() => {
                  const bondKey = '__sleeve:Bonds';
                  const open = openBucket === bondKey;
                  const bondPricedWeight = bondsSleeve.priced_weight_pct ?? bondsSleeve.portfolio_weight_pct;
                  const bondCoverageNote = bondPricedWeight < bondsSleeve.portfolio_weight_pct - 1e-6
                    ? `Calculated over the priced ${wt(bondPricedWeight)}% of your ${wt(bondsSleeve.portfolio_weight_pct)}% bond sleeve; the policy weight is scaled to that same coverage.`
                    : 'Calculated over the complete bond sleeve.';
                  return <Fragment key={bondKey}>
                  <tr onClick={() => setOpenBucket(open ? null : bondKey)}
                    title={open ? copy.row.hide('Bonds') : copy.row.show('Bonds', bondsSleeve.comparator ?? 'its comparator')}
                    className={`border-t border-neutral-800/40 bg-inset/20 cursor-pointer transition-colors ${
                      open ? 'bg-accent-500/[0.07]' : 'hover:bg-overlay/[0.02]'}`}>
                    <td className="px-2 py-1.5 text-fg whitespace-nowrap">
                      <span className={`inline-block w-3 text-[10px] ${open ? 'text-accent-400' : 'text-fg-faint'}`}>
                        {open ? '▾' : '▸'}
                      </span>
                      Bonds
                    </td>
                    <td className="px-2 py-1.5 text-right font-mono text-fg-subtle"><Num prov={<Provenance source={pWeightSource} asOf={data.start} kind="formula"
                      what="Your opening-book share held in Bonds."
                      how={`The real opening-book weight, from ${pWeightSrc}.`}
                      worked={workedWeight(`${wt(bondsSleeve.portfolio_weight_pct)}%`)} />}>
                      {wt(bondsSleeve.portfolio_weight_pct)}%</Num></td>
                    <td className="px-2 py-1.5 text-right font-mono text-fg-subtle"><Num prov={<Provenance source="derived" kind="formula"
                      what="The policy benchmark allocation to Bonds." note="policy weight in Bonds"
                      how="The policy's stated bond allocation." worked={workedWeight(bondsSleeve.policy_weight_pct == null ? null : `${wt(bondsSleeve.policy_weight_pct)}%`)} />}>
                      {bondsSleeve.policy_weight_pct == null ? '—' : `${wt(bondsSleeve.policy_weight_pct)}%`}</Num></td>
                    <td className="px-2 py-1.5 text-right font-mono text-fg-subtle"><Num prov={<Provenance source={pSrc} asOf={benchmarkAsOf} kind="formula"
                      what="What your bond holdings returned in EUR." note="your bond return"
                      how={pReturnHow} worked={workedReturn(pct(bondsSleeve.portfolio_return_pct))}
                      calculation={<ReturnCalculation rows={bondsSleeve.holdings ?? []}
                        result={pct(bondsSleeve.portfolio_return_pct)} bucket="Bonds" owner={copy.prov.yours} />} />}>
                      {pct(bondsSleeve.portfolio_return_pct)}</Num></td>
                    <td className="px-2 py-1.5 text-right font-mono text-fg-subtle">
                      <Num prov={<Provenance source="benchmark" asOf={benchmarkAsOf} kind="formula"
                        what={`What the ${bondsSleeve.comparator ?? 'bond'} comparator returned in EUR.`}
                        note="bond comparator return" how="The EUR close at the end of the window against the close at the start."
                        worked={workedReturn(pct(bondsSleeve.comparator_return_pct))} />}>
                        {pct(bondsSleeve.comparator_return_pct)}</Num>
                    </td>
                    <td className="px-2 py-1.5 text-right font-mono"><Eff v={bondsSleeve.allocation_pct} prov={<Provenance source="derived" kind="formula"
                      what="Bond allocation effect." note="bond sleeve allocation"
                      how={`${bondCoverageNote} It compares your bond allocation with the policy allocation against the dedicated comparator.`} />} /></td>
                    <td className="px-2 py-1.5 text-right font-mono"><Eff v={bondsSleeve.selection_pct} prov={<Provenance source="derived" kind="formula"
                      what="Bond selection effect." note="bond sleeve selection"
                      how={`${bondCoverageNote} It compares your bond return with the dedicated comparator at the policy allocation.`} />} /></td>
                    {showExtraEffects && <><td className="px-2 py-1.5 text-right font-mono"><Eff v={bondsSleeve.interaction_pct} prov={<Provenance source="derived" kind="formula"
                      what="Bond interaction effect." note="bond sleeve interaction"
                      how={`${bondCoverageNote} It captures the combined allocation and return difference.`} />} /></td>
                    <td className="px-2 py-1.5 text-right font-mono font-semibold">
                      <Eff v={bondsSleeve.impact_pct} prov={<Provenance source="derived" kind="formula"
                        what="Bonds' whole contribution to the portfolio-versus-policy gap."
                        note="bond sleeve impact" how={`${bondCoverageNote} It compares your bond return contribution with the same-coverage policy comparator contribution.`} />} />
                    </td></>}
                  </tr>
                  {open && <tr className="bg-inset/60">
                    <td colSpan={showExtraEffects ? 9 : 7} className="px-3 py-3">
                      <BucketNames row={{ portfolio_holdings: bondsSleeve.holdings ?? [], benchmark_holdings: [] }}
                        bucket="Bonds" benchmark={bondsSleeve.comparator ?? 'bond comparator'}
                        startLabel={startLabel} portfolioProvenance={portfolioHoldingProvenance}
                        benchmarkProvenance={benchmarkHoldingProvenance} />
                    </td>
                  </tr>}
                  </Fragment>;
                })()}
              </tbody>
              <tfoot>
                {showExtraEffects && data.fund_implementation_residual_pct != null && (
                  <tr className="border-t border-neutral-800/25 text-fg-soft">
                    <td colSpan={8} className="px-2 py-1.5">
                      ETF implementation residual
                      <span className="ml-2 text-[10px] text-fg-faint">ETF sector weights are included in allocation; their total returns cannot be split into sector selection.</span>
                    </td>
                    <td className="px-2 py-1.5 text-right font-mono font-semibold">
                      <Eff v={data.fund_implementation_residual_pct} />
                    </td>
                  </tr>
                )}
                <tr className="border-t border-neutral-800/40 font-semibold">
                  <td className="px-2 py-1.5 text-fg">{copy.headers.totalExcess}</td>
                  <td className="px-2 py-1.5 text-right font-mono text-fg-subtle">
                    <Num prov={<Provenance source={pWeightSource} asOf={data.start} kind="formula"
                      what="Sum of the Your weight column."
                      note="column total"
                      how="Adds the values above in this column." />}>
                      {wt(portfolioWeightTotal)}%
                    </Num>
                  </td>
                  <td className="px-2 py-1.5 text-right font-mono text-fg-subtle">
                    <Num prov={<Provenance source="benchmark_caps" asOf={data.start} historical kind="formula"
                      what="Sum of the Benchmark weight column."
                      note="column total"
                      how="Adds the values above in this column." />}>
                      {wt(benchmarkWeightTotal)}%
                    </Num>
                  </td>
                  {/* Returns are weighted averages, not additive totals. Keep their two columns
                      empty rather than putting a mathematically meaningless sum underneath. */}
                  <td colSpan={2} />
                  <td className="px-2 py-1.5 text-right font-mono">
                    <Eff v={allocationColumnTotal}
                      prov={<Provenance source="derived" kind="formula" note="column total"
                        what="Sum of the Allocation column."
                        how="Adds the values above in this column." />} />
                  </td>
                  <td className="px-2 py-1.5 text-right font-mono">
                    <Eff v={selectionColumnTotal}
                      prov={<Provenance source="derived" kind="formula" note="column total"
                        what="Sum of the Selection column."
                        how="Adds the values above in this column." />} />
                  </td>
                  {showExtraEffects && <><td className="px-2 py-1.5 text-right font-mono">
                    <Eff v={interactionColumnTotal}
                      prov={<Provenance source="derived" kind="formula" note="column total"
                        what="Sum of the Interaction column."
                        how="Adds the values above in this column." />} />
                  </td>
                  <td className="px-2 py-1.5 text-right font-mono">
                    <Eff v={totalColumnTotal}
                      prov={<Provenance source="derived" kind="formula" note="column total"
                        what="Sum of the Total column."
                        how="Adds the values above in this column." />} />
                  </td></>}
                </tr>
              </tfoot>
            </table>
          </div>
          </>}

          <details open className="group rounded-lg border border-neutral-800/35 bg-card">
            <summary className="flex cursor-pointer list-none items-center gap-2 px-3 py-2.5
                                text-[12px] text-fg-muted hover:bg-overlay/[0.02]">
              <svg viewBox="0 0 16 16" aria-hidden="true"
                className="h-3 w-3 shrink-0 text-fg-faint transition-transform group-open:rotate-90"
                fill="none" stroke="currentColor" strokeWidth="1.6">
                <path d="m6 3 5 5-5 5" strokeLinecap="round" strokeLinejoin="round" />
              </svg>
              <span className="font-semibold text-fg-strong">Holdings behind the result</span>
            </summary>
            <div className="grid gap-5 border-t border-neutral-800/25 px-3 py-3 md:grid-cols-3">
            <Names title={copy.names.contributors} rows={data.top_contributors ?? []} benchmark={benchmark}
              weightSrc={pWeightSource} returnSrc={pSrc}
              weightAsOf={data.start} returnAsOf={benchmarkAsOf}
              weightHow={pWeightSrc} returnHow={pReturnHow} />
            <Names title={copy.names.detractors} rows={data.top_detractors ?? []} benchmark={benchmark}
              weightSrc={pWeightSource} returnSrc={pSrc}
              weightAsOf={data.start} returnAsOf={benchmarkAsOf}
              weightHow={pWeightSrc} returnHow={pReturnHow} />
            {/* The other half of "why" — and the half a holdings-only view can never show.
                 `held={false}`: these three columns are the INDEX's weight, the index's return
                and what the name was worth TO THE INDEX. Same columns as the two lists beside it,
                different subject — the per-cell text has to say so or a benchmark's gain reads as
                something that happened in your book. */}
            <Names title={copy.names.winners(benchmark)} rows={data.missed_winners ?? []}
              weightSrc="benchmark_caps" returnSrc="benchmark"
              weightAsOf={data.start} returnAsOf={benchmarkAsOf}
              owner={benchmark} held={false}
              weightHow="start-of-window cap weight"
              returnHow="EUR close at the window’s end ÷ its close at the start − 1" />
            </div>
          </details>
          </>}
        </>
      )}
      </div>
    </section>
  );
}
