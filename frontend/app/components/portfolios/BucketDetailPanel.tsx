'use client';

import { useEffect, useMemo, useRef, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { Provenance, type SourceKey } from '../../../lib/provenance';
import type { ModelPortfolioAttribution } from '../../../lib/types/api';
import { useBucketDetailCopy } from './bucketDetailCopy';
import { workedContribution, workedReturn, workedWeight } from './attributionFormulas';
import AirsWeightCalculation, { type AirsWeightComponent } from './AirsWeightCalculation';
import LoadingDots from './LoadingDots';

type Attr = ModelPortfolioAttribution;
type Bucket = NonNullable<Attr['rows']>[number];
type Name = NonNullable<Bucket['portfolio_holdings']>[number];

//  The axis labels moved into `bucketDetailCopy`, keyed the same way. The KEY (`sector` |
// `region` | `currency`) is what the request sends and must stay English.

/** A return / effect, coloured by sign. `—` when it could not be measured — never a 0. */
function Num({ v, pp, prov }: { v?: number | null; pp?: boolean; prov?: React.ReactNode }) {
  if (v == null) return (
    <span className="inline-flex items-center justify-end gap-1 text-fg-faint">—{prov}</span>
  );
  return (
    <span className={`inline-flex items-center justify-end gap-1 ${v >= 0 ? 'text-pos-400' : 'text-neg-400'}`}>
      <span>{v >= 0 ? '+' : ''}{v.toFixed(2)}{pp ? 'pp' : '%'}</span>{prov}
    </span>
  );
}

function ValueWithInfo({ children, prov }: { children: React.ReactNode; prov: React.ReactNode }) {
  return <span className="inline-flex min-w-0 items-center justify-end gap-1">{children}{prov}</span>;
}

/** One holdings table — your names or the index's. `table-fixed` + this shared colgroup gives
 *  the numeric columns identical widths in BOTH tables, so Weight/Return/Contrib line up between
 *  "your holdings" and the index's; the Name column takes the rest and truncates (full name on
 *  hover). */
function HoldingsCols() {
  return (
    <colgroup>
      {/* Rank — narrow and fixed, so a 2-digit number never steals width from the name.  NOT
          widened with the three below: it holds at most two digits, and the space would come
          straight out of the name column for nothing. */}
      <col className="w-[1.75rem]" />
      <col />
      {/*  1.5× THE ORIGINAL 4 / 4.25 / 4.5rem (2026-08-10, on request). The width comes out of
          the NAME column, which is the only auto one — so these three gain exactly what the
          truncated name loses. That is the trade being made deliberately: the figures are the
          reason the panel is open, and a clipped name still has its full text on hover. */}
      {/*  THE TWO WEIGHTS SHARE A WIDTH. They are one quantity at two dates and are compared
          by eye down the pair; a different width would read as a difference in kind. */}
      <col className="w-[6rem]" />
      <col className="w-[6rem]" />
      <col className="w-[6.375rem]" />
      <col className="w-[6.75rem]" />
    </colgroup>
  );
}

type SortKey = 'name' | 'weightNow' | 'weight' | 'return' | 'contrib';

const SORT_VAL: Record<SortKey, (h: Name) => number | string | null> = {
  name: (h) => (h.name ?? '').toLowerCase(),
  weightNow: (h) => h.weight_now_pct ?? null,
  weight: (h) => h.weight_pct ?? null,
  return: (h) => h.return_pct ?? null,
  contrib: (h) => h.contribution_pct ?? null,
};

/**  NAMED, BECAUSE A BARE "Weight" INVITES THE WRONG COMPARISON — and because this one used to be
 *  a genuinely different number. Until 2026-07-31 the composition chart divided TODAY's value by
 *  the whole equity sleeve while this panel divided the START-of-window value by the attributable
 *  holdings: Technology read 36% there and 39.1% here, ASML 7.30% against 5.75%. Both correct,
 *  which is what made it unarbitrable. The composition now adopts this basis, so the two agree —
 *  the label stays because the basis is still not self-evident from a percentage. */
/**
 *  This sentence's claim — "a bucket total here equals its bar" — WAS FALSE FOR THE INDEX FOR A
 * Year, and nobody could tell until the total row made it checkable. The composition chart weighed
 * the index by `market_cap_eur` (TODAY's cap) while this list has always used
 * `index_rows(label, start)` (the cap at the window's open), so SP500 Technology read **34.90% on
 * the bar against 31.24% here** — and the bar sat under an axis note saying "Start-of-window
 * weights".
 *
 *  It was not a labelling problem. `diff_pct`, the TILT the two bars exist to show, subtracted a
 * today-weighted index from a start-weighted book — a difference computed across two bases. Fixed
 * in `_airs_portfolio_analysis` (2026-08-10) by weighing the index at the window's open too,
 * dropping any constituent with no start cap rather than letting it keep today's (which had left a
 * 0.68pp residue). Both figures are now 31.24% on the same constituent set, by construction.
 *
 *  So the claim below is true again — for both sides — and it is worth keeping precisely because
 * it is the thing a reader can check in five seconds. If it ever stops holding, the cause is a
 * basis drifting apart again, not a rounding.
 */
/**  THE COLUMN THAT RECONCILES WITH THE BAR YOU CLICKED (2026-09-03, on request). The
 *  composition charts moved to CURRENT weights that day and this panel stayed on the window's
 *  open, so a Technology bar reading 36% opened a list totalling 39.1% — the same two-bases
 *  mismatch the note above records from the other direction, re-created by moving the other side.
 *
 *   It is a second column and not a replacement, because Return and Contribution are BUILT from
 *  the start weight: `Σ weight × return ÷ 100 == contribution` is exact, and it is exact only on
 *  the weights that earned the return. Re-weighting the decomposition on today's values would
 *  decompose a portfolio nobody held — the same argument the attribution endpoint already makes
 *  about design weights. So both dates are shown, each labelled, and the arithmetic keeps its own.
 *
 *   A dash means the source has no current values (`source=model`, whose weights are design
 *  percentages), not that the holding has none. */
const WEIGHT_NOW_HINT = 'Share of the complete portfolio or index, weighted by what each holding '
  + 'is worth now. Return and Contribution use the opening weight beside it, not this one.';

const WEIGHT_HINT = 'Share of the complete portfolio or index when the window opened. Portfolio '
  + 'positions keep their real AIRS Beginwaarde weight; they are not renormalised after funds, '
  + 'cash or certificates are left out. Return and Contribution are built from this weight.';

export type HoldingsProvenance = {
  owner: string;
  nameSource: SourceKey;
  currentWeightSource: SourceKey;
  startWeightSource: SourceKey;
  returnSource: SourceKey;
  currentWeightAsOf?: string | null;
  startWeightAsOf?: string | null;
  returnAsOf?: string | null;
  currentBasis: string;
  startBasis: string;
  returnBasis: string;
};

/**  EXPORTED, AND THE ATTRIBUTION TABLE'S ROW DRILL-DOWN USES THE SAME ONE. Both answer the
 *  identical question — "which names are behind this bucket, on each side" — off the identical
 *  payload. A second table with its own columns, sort and overlap treatment would be two
 *  appearances of one fact, and the reader would have to learn which is which. */
export function Holdings({ rows, provenance, startLabel = 'Start of window', weightBasis = 'start' }: {
  rows: Name[];
  provenance: HoldingsProvenance;
  /**
   * Which date the weight column is on — and it follows WHERE THE READER CAME FROM
   * (2026-09-03, on request: the composition drill-downs "should display WEIGHT (NOW) instead of
   * WEIGHT (START)").
   *
   *  It does not pick which columns render — both always do. It was briefly a switch that
   * showed one weight and hid the other; that was wrong in the direction it hid, because the two
   * columns answer different questions and a reader in this pane wants both: what the bucket is
   * TODAY (which ties to the bar they clicked) and what it was at the OPEN (which is what Return
   * and Contribution are built from). This only sets which of the two the list opens sorted by.
   *
   *  The order is fixed, `now` THEN `start`, in both modes. A reader moving between a composition
   * drill-down and an attribution one should not have to re-find the columns; the basis that sent
   * them here shows up as the sort, not as a different layout.
   *
   *  Return and contribution are always built from the start weight — `Σ weight × return ÷ 100 ==
   * contribution`, exact, and exact only on the weights that earned the return. That is why the
   * start column can never be the one dropped: it is the only one those two can be checked
   * against.
   */
  weightBasis?: 'now' | 'start';
  /**
   * WHEN the weight was measured, named in the header.
   *
   *  A prop, not the literal "Start of year", BECAUSE THIS TABLE SERVES TWO WINDOWS. The
   * `/bucket` drill-down pins `window=ytd`, so there the start IS 1 January — but `AttributionPanel`
   * has a window toggle and renders this same table for SINCE-INCEPTION, where the weight is the
   * one held at the model's inception and could be any date in 2024. Hardcoding the year would put
   * a wrong date on half the drill-downs, in the calmest possible way: a header that reads correctly
   * and describes a different measurement.
   *
   *  The default is the vague-but-true one. A caller that forgets to say which window it is on
   * gets "Start of window", which is right for every window; it does not get a confident "Start of
   * year" that is right for one of them.
   */
  startLabel?: string;
}) {
  const t = useBucketDetailCopy();
  // Sortable — click a header to toggle direction. Default: weight, largest first. Each table sorts
  // on its OWN state (your names and the index's are independent lists).
  //  The column, its hint and the default sort all come from one flag. Three places deciding
  //   "which weight" independently is how a table ends up sorted by a column it does not show.
  //  Both columns always render (2026-09-03, on request, reversing the single-column cut made
  //   an hour earlier). What `weightBasis` still decides is the DEFAULT SORT — the reader arrives
  //   from a bar weighed now or from a decomposition weighed at the open, and the list should
  //   open ordered by the number that sent them here.
  const wKey: SortKey = weightBasis === 'now' ? 'weightNow' : 'weight';
  const [key, setKey] = useState<SortKey>(wKey);
  const [dir, setDir] = useState<'asc' | 'desc'>('desc');
  /**
   * The three figures the total row shows — computed BEFORE the early return so the hook order is
   * fixed, which is why this sits above `if (!rows.length)`.
   *
   *  The return is the only one that is not a sum, and it is weighted by the START weight over
   * the names that HAVE a return. A plain mean would let a 0.1% holding that doubled pull as hard
   * as a 9% one that stood still; counting an unpriceable name as a zero would drag the average
   * toward nothing by exactly the weight we could not measure. Renormalising puts it out of both
   * sides of the ratio, which is the same discipline every other weighted figure in this app uses.
   *
   *  The three reconcile over the **PRICED** NAMES, NOT OVER THE WEIGHT CELL. Contribution is
   * `w · r / 100` per row, so `Σw(priced) × return ÷ 100 == contrib` exactly — but the Weight cell
   * sums EVERY name, including the ones with no return. Measured on a five-name bucket: weight
   * 8.50%, priced weight 7.60%, and it is the 7.60 that ties. Reading the row as
   * `8.50 × 14.29 ÷ 100` and finding 1.21 against a printed 1.09 is not a bug — it is the
   * unpriceable 0.90% showing up, which is why the return cell states its own denominator.
   */
  const totals = useMemo(() => {
    const weight = rows.reduce((s, h) => s + (h.weight_pct ?? 0), 0);
    //  Null when no row has one, rather than 0 — a source with no current values must print a
    //   dash here too, or the total would claim this bucket is worth nothing today.
    const nowRows = rows.filter((h) => h.weight_now_pct != null);
    const weightNow = nowRows.length
      ? nowRows.reduce((s, h) => s + (h.weight_now_pct ?? 0), 0) : null;
    const priced = rows.filter((h) => h.return_pct != null && (h.weight_pct ?? 0) > 0);
    const den = priced.reduce((s, h) => s + h.weight_pct!, 0);
    const contribRows = rows.filter((h) => h.contribution_pct != null);
    return {
      weight,
      weightNow,
      ret: den > 0 ? priced.reduce((s, h) => s + h.weight_pct! * h.return_pct!, 0) / den : null,
      pricedWeight: den,
      retRows: priced.length,
      contrib: contribRows.length
        ? contribRows.reduce((s, h) => s + h.contribution_pct!, 0) : null,
    };
  }, [rows]);
  if (!rows.length) return <p className="text-[12px] text-fg-faint py-1">Nothing held here.</p>;

  const sorted = [...rows].sort((a, b) => {
    const av = SORT_VAL[key](a);
    const bv = SORT_VAL[key](b);
    if (key === 'name') {
      const cmp = String(av).localeCompare(String(bv));
      return dir === 'asc' ? cmp : -cmp;
    }
    // A missing value (—) always sorts to the bottom, whichever direction.
    if (av == null && bv == null) return 0;
    if (av == null) return 1;
    if (bv == null) return -1;
    return dir === 'asc' ? (av as number) - (bv as number) : (bv as number) - (av as number);
  });

  const click = (k: SortKey) => {
    if (k === key) { setDir((d) => (d === 'asc' ? 'desc' : 'asc')); return; }
    setKey(k);
    setDir(k === 'name' ? 'asc' : 'desc');   // names A→Z, numbers large→small on first click
  };
  const caret = (k: SortKey) => (key === k ? (dir === 'asc' ? ' ▲' : ' ▼') : '');
  const weightText = (v?: number | null) => v == null ? '—' : `${v.toFixed(2)}%`;
  const weightProv = (v: number | null | undefined, now: boolean, what: string,
    valueEur?: number | null, totalEur?: number | null,
    components?: AirsWeightComponent[] | null, holdingName?: string | null) => {
    const raw = !now && valueEur != null && totalEur != null && components?.length;
    return <Provenance source={now ? provenance.currentWeightSource : provenance.startWeightSource}
      asOf={now ? provenance.currentWeightAsOf : provenance.startWeightAsOf}
      kind={raw ? undefined : 'formula'}
      what={what} note={now ? 'current weight' : 'opening weight'}
      how={now ? provenance.currentBasis : provenance.startBasis}
      worked={raw ? undefined : workedWeight(weightText(v), valueEur, totalEur)}
      calculation={raw ? <AirsWeightCalculation components={components} numerator={valueEur}
        denominator={totalEur} result={weightText(v)} holdingName={holdingName ?? 'Holding'} />
        : undefined} />;
  };
  const returnProv = (v: number | null | undefined, what: string) => (
    <Provenance source={provenance.returnSource} asOf={provenance.returnAsOf} kind="formula"
      what={what} note="EUR return over the window" how={provenance.returnBasis}
      worked={workedReturn(v == null ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(2)}%`)} />
  );
  const contributionProv = (w: number | null | undefined, r: number | null | undefined,
    v: number | null | undefined, what: string) => (
    <Provenance source="derived" asOf={provenance.returnAsOf ?? provenance.startWeightAsOf} kind="formula"
      what={what} note="contribution to return"
      how="The opening weight times the EUR return."
      worked={workedContribution(weightText(w),
        r == null ? '—' : `${r >= 0 ? '+' : ''}${r.toFixed(2)}%`,
        v == null ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(2)}pp`)} />
  );
  //  The hover is accent, not a shade (2026-09-03, on request: the headers "should be
  //   selectable when I hover over them"). Every one of these has been sortable and carried
  //   `cursor-pointer` the whole time — what it did not have was a state a reader could SEE. The
  //   old `hover:text-fg-soft` moved a faint grey one step towards a slightly less faint grey, on
  //   an 11px uppercase label, which is a change nobody notices and therefore an affordance nobody
  //   finds. Accent is what this app already means by "this reacts to you".
  //  And a row-wide wash, because a bare colour shift on a two-line header (`Weight` over
  //   `(now)`) leaves the reader guessing where the target ends — the two weight columns sit
  //   side by side, and the box is what says which one is under the cursor.
  //  NO `select-none` (2026-09-03, on request: "if I click my mouse and drag the cursor the
  //   headers should be highlighted, that is not possible now"). It was there to make a sortable
  //   header feel like a button — a drag across one used to leave a blue smear instead of sorting.
  //   That is a cosmetic worry, and it was costing something real: these labels name the basis a
  //   column is on ("Weight (now)", "Weight (Start of year)"), which is exactly the text somebody
  //   quotes when asking why two numbers differ, and it could not be selected or copied.
  //  The click is unaffected. `onClick` fires on mouseup over the same element whether or not a
  //   selection was made, so sorting still works on a plain click; only a deliberate DRAG now
  //   selects instead of doing nothing.
  const th = 'py-1 font-medium cursor-pointer whitespace-nowrap transition-colors '
    + 'hover:text-accent-300 hover:bg-overlay/5';

  return (
    <table className="w-full text-[12px] table-fixed">
      <HoldingsCols />
      <thead>
        <tr className="text-fg-faint text-[11px] uppercase tracking-wide">
          {/* Not sortable: the rank IS the position under the ACTIVE sort, so clicking it could
              only mean "sort by the current sort". It renumbers whenever the sort changes. */}
          <th className="pr-1 text-right font-normal">#</th>
          <th className={`${th} pr-2 text-left`} onClick={() => click('name')}>
            <span className="inline-flex items-center gap-1">{t.colName}{caret('name')}
              <Provenance source={provenance.nameSource} column kind="copied"
                what="The company represented by each holding." note="holding name" />
            </span>
          </th>
          {/*  THE QUALIFIER SITS ON ITS OWN LINE, not beside the word. "Weight (Start of year)" is
              ~130px of nowrap text in a 6rem column, and under `table-fixed` that does not shrink
              the column — it spills over Return. A `block` span wraps it instead, so the column
              keeps its width and the Name column beside it keeps the space it would have lost.
              `whitespace-normal` because the shared `th` class is nowrap, which would otherwise
              stop the qualifier wrapping inside its own line too. */}
          {/*  NOW BEFORE START, in both modes. A fixed order is what lets a reader move between
              a composition drill-down and an attribution one without re-finding the columns; the
              basis that sent them here shows up as the SORT, not as a different layout. */}
          <th className={`${th} px-1 text-right`} onClick={() => click('weightNow')} title={WEIGHT_NOW_HINT}>
            <span className="inline-flex items-center justify-end gap-1">{t.colWeight}{caret('weightNow')}
              <Provenance source={provenance.currentWeightSource} column kind="formula"
                what={`Each holding's current share of ${provenance.owner}.`}
                how={provenance.currentBasis} worked={workedWeight(null)} />
            </span>
            <span className="block normal-case whitespace-normal font-normal text-fg-subtle">
              ({t.colWeightNow})
            </span>
          </th>
          <th className={`${th} px-1 text-right`} onClick={() => click('weight')} title={WEIGHT_HINT}>
            <span className="inline-flex items-center justify-end gap-1">{t.colWeight}{caret('weight')}
              <Provenance source={provenance.startWeightSource} column kind="formula"
                what={`Each holding's opening share of ${provenance.owner}.`}
                how={provenance.startBasis} worked={workedWeight(null)} />
            </span>
            <span className="block normal-case whitespace-normal font-normal text-fg-subtle">
              ({startLabel})
            </span>
          </th>
          <th className={`${th} px-1 text-right`} onClick={() => click('return')}>
            <span className="inline-flex items-center justify-end gap-1">{t.colReturn}{caret('return')}
              <Provenance source={provenance.returnSource} column kind="formula"
                what="Each holding's EUR return over the window."
                how={provenance.returnBasis} worked={workedReturn(null)} />
            </span>
          </th>
          <th className={`${th} pl-1 text-right`} onClick={() => click('contrib')}>
            <span className="inline-flex items-center justify-end gap-1">{t.colContrib}{caret('contrib')}
              <Provenance source="derived" column kind="formula"
                what={`Each holding's contribution to ${provenance.owner}.`}
                how="The opening weight times the EUR return."
                worked={workedContribution(null, null, null)} />
            </span>
          </th>
        </tr>
      </thead>
      <tbody>
        {/*  THE TOTAL SITS AT THE TOP AND IS NOT NUMBERED. At the top because it is the answer the
            list is evidence for — on a 40-name index bucket a footer total is below the fold, and
            the reader is comparing this figure against the one in the other table, not reading to
            the end. Unnumbered because it is not a holding: a "1" here would push every name's
            rank up by one against the list it summarises. It also does not move when the headers
            are clicked — a total has no position in a sort. */}
        <tr className="border-t border-neutral-800/40 bg-inset font-semibold text-fg-strong">
          <td />
          <td className="py-1 pr-2 truncate" title={t.totalTitle(String(rows.length))}>
            <span className="inline-flex items-center gap-1">
              {t.total} <span className="text-fg-faint font-normal">({rows.length})</span>
              <Provenance source="derived" kind="formula"
                what={`The aggregate of the ${rows.length} holdings shown in this table.`}
                how="Weights and contributions are summed; return is weighted by opening weight." />
            </span>
          </td>
          <td className="py-1 px-1 text-right font-mono tabular-nums" title={WEIGHT_NOW_HINT}>
            <ValueWithInfo prov={weightProv(totals.weightNow, true,
              `The current weight of these ${rows.length} holdings in ${provenance.owner}.`)}>
              {weightText(totals.weightNow)}
            </ValueWithInfo>
          </td>
          <td className="py-1 px-1 text-right font-mono tabular-nums" title={WEIGHT_HINT}>
            <ValueWithInfo prov={weightProv(totals.weight, false,
              `The opening weight of these ${rows.length} holdings in ${provenance.owner}.`)}>
              {weightText(totals.weight)}
            </ValueWithInfo>
          </td>
          {/*  WEIGHTED BY THE START WEIGHT, NEVER A PLAIN MEAN — a 0.1% holding that doubled
              would otherwise pull this as hard as a 9% one that did nothing. Renormalised over the
              names that HAVE a return, so a row we could not price is out of both sides of the
              ratio rather than counted as a zero. */}
          <td className="py-1 px-1 text-right font-mono"
            title={totals.retRows < rows.length
              ? `Weighted by start weight, over the ${totals.retRows} of ${rows.length} names with a return`
              : 'Weighted by start weight'}>
            <Num v={totals.ret} prov={returnProv(totals.ret,
              `The opening-weighted EUR return of these ${rows.length} holdings.`)} />
          </td>
          {/*  A PLAIN SUM, AND IT IS ALLOWED TO BE ONE because contribution is percentage POINTS
              of the basket's return — points add, percentages do not. It ties to the two cells
              left of it as `priced weight × return ÷ 100`, which is the Weight cell only when
              every name has a return; see the memo for why that distinction is stated rather than
              rounded over. */}
          <td className="py-1 pl-1 text-right font-mono"><Num v={totals.contrib} pp
            prov={contributionProv(totals.pricedWeight, totals.ret, totals.contrib,
              `The contribution of these ${rows.length} holdings to ${provenance.owner}.`)} /></td>
        </tr>
        {sorted.map((h, i) => (
          // The intersection is the point: a name held on both sides is emphasised (tint + bold +
          // a ringed dot); everything else — index names you don't own, your names not in the
          // index — is faded so the shared holdings read at a glance without hiding the full lists.
          <tr key={h.isin ?? `${h.name}-${i}`}
            className={`border-t border-neutral-800/20 ${h.in_both ? 'bg-accent-500/15' : 'opacity-45'}`}>
            <td className="py-1 pr-1 text-right font-mono text-fg-faint tabular-nums">{i + 1}</td>
            <td className="py-1 pr-2" title={h.name ?? ''}>
              <span className="flex items-center gap-1.5 min-w-0">
                {/* Held on BOTH sides — a ringed dot so the overlap between your book and the index
                    is obvious. Matched by ISIN first, then by company so a share class still counts
                    as one business (backend `_overlaps`). */}
                {h.in_both && (
                  <span className="w-2 h-2 rounded-full bg-accent-500 shrink-0 ring-2 ring-accent-500/25"
                    title={t.inBothTitle} />
                )}
                <span className={`truncate ${h.in_both ? 'text-fg-strong font-medium' : 'text-fg-soft'}`}>{h.name ?? '—'}</span>
                <Provenance source={provenance.nameSource} asOf={provenance.startWeightAsOf} kind="copied"
                  what={`The company represented by this holding${h.isin ? ` (${h.isin})` : ''}.`}
                  note="holding name" />
              </span>
            </td>
            <td className="py-1 px-1 text-right font-mono text-fg" title={WEIGHT_NOW_HINT}>
              <ValueWithInfo prov={weightProv(h.weight_now_pct, true,
                `${h.name ?? 'This holding'}'s current weight in ${provenance.owner}.`,
                h.weight_now_value_eur, h.weight_now_total_eur)}>
                {weightText(h.weight_now_pct)}
              </ValueWithInfo>
            </td>
            <td className="py-1 px-1 text-right font-mono text-fg" title={WEIGHT_HINT}>
              <ValueWithInfo prov={weightProv(h.weight_pct, false,
                `${h.name ?? 'This holding'}'s opening weight in ${provenance.owner}.`,
                h.weight_value_eur, h.weight_total_eur,
                h.weight_denominator_components, h.airs_name ?? h.name)}>
                {weightText(h.weight_pct)}
              </ValueWithInfo>
            </td>
            <td className="py-1 px-1 text-right font-mono"><Num v={h.return_pct}
              prov={returnProv(h.return_pct, `What ${h.name ?? 'this holding'} returned, in EUR.`)} /></td>
            {/* Contribution = weight × return — percentage POINTS of the basket's return, not %. */}
            <td className="py-1 pl-1 text-right font-mono"><Num v={h.contribution_pct} pp
              prov={contributionProv(h.weight_pct, h.return_pct, h.contribution_pct,
                `${h.name ?? 'This holding'}'s contribution to ${provenance.owner}.`)} /></td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

/**
 * The click-through detail behind ONE composition bar: the model's holdings in that bucket, the
 * index's constituents in the same bucket, and the Brinson tilt (allocation vs selection). It
 * reuses the attribution endpoint — the same source the "Why" panel reads — so a bucket's detail
 * and the excess it rolls into can never disagree.
 *
 * Returns are window-dependent (a composition bar is point-in-time; a return is not), so the
 * panel carries its own YTD / Since-inception toggle and states the date it measures from.
 *
 * Funds / cash / unclassified are NOT a sector bet, so they have no attribution row. For those
 * buckets the panel shows the holdings alone (weight, and a return where we have one) and says so
 * — decomposing a world tracker as a sector call is exactly the false finding attribution avoids.
 */
export default function BucketDetailPanel({ id, benchmark, axis, bucket, source = 'model', onClose }: {
  id: number; benchmark: string; axis: string; bucket: string;
  /**  MUST MATCH THE MODAL — the same value the Attribution panel gets. Omit it and the backend
   *  defaults to `model` (the design percentages, a flat 5.00% each) while the Attribution panel
   *  above is decomposing BEGINWAARDE start weights. Two panels in one modal, same portfolio, same
   *  window, different weights — and neither says so. */
  source?: 'model' | 'book';
  onClose: () => void;
}) {
  const t = useBucketDetailCopy();
  const [attr, setAttr] = useState<Attr | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const ref = useRef<HTMLElement>(null);

  // Docked full-width BELOW the charts. `nearest` scrolls it into view only when it is not already
  //  NO `scrollIntoView` ANY MORE. It existed to reveal this panel when it sat in the flow
  // below the charts; in a dialog there is nothing to scroll to, and calling it would scroll
  // the modal BEHIND the backdrop while the reader looks at something fixed on top of it.

  // Keyed on axis + window + benchmark, NOT on bucket: one fetch serves every bucket in the axis,
  // so switching bars in the same chart is instant.
  useEffect(() => {
    let cancelled = false;
    setLoading(true); setError(null); setAttr(null);
    void (async () => {
      try {
        const r = await apiFetch(
          `${API_URL}/api/airs/model-portfolios/${id}/attribution`
          + `?benchmark=${benchmark}&window=ytd&axis=${axis}&source=${source}`);
        const b = await r.json().catch(() => null);
        if (cancelled) return;
        if (!r.ok) { setError(b?.detail ?? `HTTP ${r.status}`); return; }
        setAttr(b as Attr);
      } catch (e) {
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => { cancelled = true; };
  }, [id, benchmark, axis, source]);

  const row = attr?.rows?.find((r) => r.bucket === bucket);
  const excluded = (attr?.excluded ?? []).filter((e) => e.bucket === bucket);
  // A fund/cash/unclassified bucket has no attribution row — show its holdings alone.
  const nonAttributable = !row && excluded.length > 0;
  // How many names are the intersection (held on both sides) — surfaced in each list header.
  const portfolioProvenance: HoldingsProvenance = {
    owner: source === 'book' ? 'the complete AIRS book' : 'the complete AIRS model',
    nameSource: source === 'book' ? 'airs_volk' : 'airs_model',
    currentWeightSource: source === 'book' ? 'airs_volk' : 'airs_model',
    startWeightSource: source === 'book' ? 'airs_volk' : 'airs_model',
    returnSource: 'yfinance',
    currentWeightAsOf: undefined,
    startWeightAsOf: attr?.start,
    returnAsOf: undefined,
    currentBasis: source === 'book'
      ? 'Current EUR position value divided by the current EUR value of the complete AIRS book.'
      : 'The model source has no current position values, so this value is unavailable.',
    startBasis: source === 'book'
      ? 'Every raw AIRS Beginwaarde shown below is added together. That sum is the denominator for this holding.'
      : 'The stated AIRS model weight, kept on the complete model basis without renormalising the remaining stocks.',
    returnBasis: source === 'book'
      ? 'Current EUR value against AIRS Beginwaarde.'
      : 'The EUR close at the end of the window against the close at the start.',
  };
  const benchmarkProvenance: HoldingsProvenance = {
    owner: benchmark,
    nameSource: 'benchmark_caps',
    currentWeightSource: 'benchmark_caps',
    startWeightSource: 'benchmark_caps',
    returnSource: 'benchmark',
    currentWeightAsOf: undefined,
    startWeightAsOf: attr?.start,
    returnAsOf: undefined,
    currentBasis: `Current constituent market cap divided by the total current market cap represented by ${benchmark}.`,
    startBasis: `Constituent market cap at the start of the window divided by ${benchmark}'s total start market cap.`,
    returnBasis: 'The EUR close at the end of the window against the close at the start.',
  };

  return (
    /*  `h-full min-h-0 flex flex-col` + an inner scroll — the shape `PanelDialog` requires.
       The dialog is a FIXED box; a body that sizes to its content would overflow it silently,
       and `min-h-0` is what lets a flex child shrink below its content so the scroll actually
       engages. Same construction as `ActiveSharePanel`. */
    <section ref={ref} className="h-full min-h-0 flex flex-col bg-card border
      border-accent-500/30 rounded-xl p-4">
      {/*  `shrink-0` — the heading names which bar was clicked, which is the one thing that
          must stay visible while the tables under it scroll. */}
      <div className="shrink-0 flex items-start justify-between gap-3 mb-2">
        <h4 className="text-sm font-semibold text-fg-strong">
          {t.axis[axis as keyof typeof t.axis] ?? axis}: <span className="font-mono">{bucket}</span>
        </h4>
        <button onClick={onClose}
          className="cursor-pointer text-[12px] px-2 py-1 rounded-lg border border-neutral-700 text-fg-muted hover:text-accent-300 shrink-0">

        </button>
      </div>

      {/*  EVERYTHING BELOW THE HEADING SCROLLS AS ONE. `min-h-0` is what lets it: a flex child
          refuses to shrink below its content without it, so the fixed dialog would silently
          give way instead of the body scrolling — see the  on the root. */}
      <div className="flex-1 min-h-0 overflow-auto">

      {loading && <p className="text-xs text-fg-subtle">{t.computing} <LoadingDots /></p>}
      {error && (
        <div className="bg-neg-500/10 border border-neg-500/20 rounded-lg px-3 py-2 text-xs text-neg-300">{error}</div>
      )}

      {!loading && !error && attr && (
        <>
          {/*  NO BRINSON STRIP HERE (2026-09-03, on request). It read "Since 2026-01-01 ·
              allocation +3.16pp · selection -3.29pp · interaction -2.27pp · total -2.40pp" above
              the names. Three of those four are decomposition terms on the START weights, sitting
              on top of a table this panel now weighs on CURRENT values — two bases in one pane,
              with the reader's eye moving between them.
               NOTHING IS LOST: the identical four figures are the Attribution panel's own row for
              this bucket, which is where the decomposition is the subject rather than a header.
              This pane answers "which names are in this bucket, on each side", and it now answers
              only that. The payload still carries the terms (`row.allocation_pct` and the rest) —
              it is one `<div>` that went, not a computation. */}

          {nonAttributable && (
            <p className="text-[12px] text-fg-faint mb-2">
              {t.notDecomposed}
            </p>
          )}

          {row ? (
            <>
              {(row.portfolio_holdings ?? []).some((h) => h.in_both) && (
                <p className="text-[11px] text-fg-faint flex items-center gap-1.5 mb-2">
                  <span className="w-2 h-2 rounded-full bg-accent-500 inline-block shrink-0 ring-2 ring-accent-500/25" />
                  {t.overlapLegend(benchmark)}
                </p>
              )}
              {/* Your names and the index's, SIDE BY SIDE on wide screens so the full-width dock is
                  used — stacked on narrow ones. */}
              <div className="grid gap-4 lg:grid-cols-2 lg:items-start">
                <div>
                  <p className="text-[12px] font-medium text-fg-muted mb-1">
                    {t.yourHoldings} <span className="text-fg-faint">({row.portfolio_holdings?.length ?? 0})</span>
                  </p>
                  {/*  "Start of year" IS SAFE HERE ONLY BECAUSE THIS PANEL PINS `window=ytd` in
                      its own request (see the fetch above). If that ever becomes a toggle, this
                      label has to follow it — see `Holdings`'s `startLabel`. */}
                  <Holdings rows={row.portfolio_holdings ?? []} startLabel={t.startOfYear}
                    weightBasis="now" provenance={portfolioProvenance} />
                </div>
                <div>
                  <p className="text-[12px] font-medium text-fg-muted mb-1">
                    {t.constituents(benchmark)}{' '}
                    <span className="text-fg-faint">({row.benchmark_holdings?.length ?? 0})</span>
                    {/*  THIS NOTE EXISTS BECAUSE THE BASIS WAS WRONG HERE ONCE AND NOTHING SAID SO.
                        The index bar was weighted by today's caps against a list weighted at the
                        window's open (SP500 Technology: 34.90% vs 31.24%), under an axis label
                        claiming start-of-window. Both sides are now weighed at the open, so the
                        total below DOES match the bar — and naming the basis is what lets a reader
                        notice if that ever stops being true. */}
                    <span className="text-fg-faint font-normal"
                      title="Market caps as at the window's open, renormalised over the attributable constituents — the same basis the index bar on the composition chart uses, so this total matches it. A return must be weighted by what was held when it started, not by what the constituents are worth today.">
                      {' '}· {t.weightedAtOpen}
                    </span>
                  </p>
                  <Holdings rows={row.benchmark_holdings ?? []} startLabel={t.startOfYear}
                    weightBasis="now" provenance={benchmarkProvenance} />
                </div>
              </div>
            </>
          ) : nonAttributable ? (
            <table className="w-full text-[12px]">
              <thead>
                <tr className="text-fg-faint text-[11px] uppercase tracking-wide">
                  <th className="py-1 pr-2 text-left font-medium">Name</th>
                  <th className="py-1 px-2 text-right font-medium">Weight</th>
                  <th className="py-1 px-2 text-right font-medium">Return</th>
                  <th className="py-1 pl-2 text-left font-medium">Why excluded</th>
                </tr>
              </thead>
              <tbody>
                {excluded.map((e, i) => (
                  <tr key={e.isin ?? `${e.name}-${i}`} className="border-t border-neutral-800/20">
                    <td className="py-1 pr-2 text-fg-soft truncate max-w-[14rem]" title={e.name ?? ''}>{e.name ?? '—'}</td>
                    <td className="py-1 px-2 text-right font-mono text-fg">{(e.weight_pct ?? 0).toFixed(2)}%</td>
                    <td className="py-1 px-2 text-right font-mono"><Num v={e.return_pct} /></td>
                    <td className="py-1 pl-2 text-fg-faint">{e.reason ?? '—'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <p className="text-[12px] text-fg-faint">
              {t.noHoldings}
            </p>
          )}
        </>
      )}
      </div>
    </section>
  );
}
