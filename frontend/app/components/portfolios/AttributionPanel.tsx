'use client';

import { Fragment, useEffect, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import type { ModelPortfolioAttribution } from '../../../lib/types/api';
import { Provenance, type SourceKey } from '../../../lib/provenance';
import { Holdings } from './BucketDetailPanel';

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
 * ⚠ The three effects SUM to the excess. That identity is the whole point, and it is checked,
 * not assumed — if it ever fails, the table is three columns of numbers sitting next to each
 * other, and the banner says so instead of letting them be read as a decomposition.
 */
type Axis = 'sector' | 'region' | 'currency';

const pct = (v: number | null | undefined, dp = 2) =>
  v == null ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(dp)}%`;

/** Pydantic-defaulted fields come back optional in the generated types. A missing effect is 0 —
 *  it contributed nothing — which is a fact, not an unknown, so it is safe to default here. */
const n = (v: number | null | undefined) => v ?? 0;

/** `pp` — an effect is percentage POINTS, never percent. The excess is a difference of two
 *  returns; a `%` here would claim a different quantity. */
const pp = (v: number | null | undefined, dp = 2) =>
  v == null ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(dp)}pp`;

/**
 * A number with its OWN ⓘ.
 *
 * ⚠ THE ⓘ IS THE ONLY HOVER TARGET IN THIS PANEL, AND EVERY ONE OF THEM OPENS THE SAME CARD —
 * What · Source · When · How. Nothing else reacts to a pointer: not the value, not the column
 * label. That single rule is what makes the gesture worth learning, and it was arrived at by
 * removing two earlier answers. Free prose under each chip made the affordance unpredictable
 * (some opened a definition, some an essay, some a source). A second tooltip on the column label
 * was worse: two targets in one header, no way to tell which held the answer, so a reader found
 * one, read it, and concluded that was all there was.
 *
 * ⚠ THE ICON, NOT THE NUMBER, IS THE TRIGGER. Hovering a bare value is invisible — a tooltip
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
  const body = Math.abs(v) < 0.005
    ? <span className="text-fg-faint">—</span>
    : <span className={v >= 0 ? 'text-pos-400' : 'text-neg-400'}>{pp(v)}</span>;
  return prov ? <Num prov={prov}>{body}</Num> : body;
}

/**
 * ⚠ THE WORD FOLLOWS THE AXIS. It is "sector" only when the axis IS sector — switch to Region and
 * every "sector" in this panel becomes a lie. "Bucket" was correct but it is jargon; naming the
 * thing the reader actually chose is both correct AND plain.
 */
const AXIS_WORD: Record<string, string> = {
  sector: 'sector',
  region: 'region',
  currency: 'currency',
};

/**
 * A column header. The LABEL is inert; the ⓘ beside it is the only thing to hover.
 *
 * ⚠ ONE TARGET PER HEADER. The label used to carry its own dotted-underline tooltip explaining
 * what the column means, beside an ⓘ carrying What/Source/When/How — two hover targets, two
 * different cards, one header. A reader cannot see which of them holds the answer they want, so
 * they find one, read it, and stop; whichever they land on, they conclude that is all there is.
 * The ⓘ is now the single place, and anything the label's card said that its card did not must
 * be folded INTO it — deleting the second tooltip is only safe if nothing was left behind in it.
 */
function Th({ label, prov, align = 'right' }: {
  label: React.ReactNode;
  /** The column's ⓘ — What / Source / When / How. One per column; every cell repeats it with its
   *  own row's figures. */
  prov?: React.ReactNode;
  align?: 'left' | 'right';
}) {
  return (
    <th className={`px-2 py-1.5 font-medium whitespace-nowrap ${align === 'left' ? 'text-left' : 'text-right'}`}>
      {label}
      {prov}
    </th>
  );
}

function Names({ title, rows, hint, src, asOf, weightHow, returnHow,
  owner = 'the model', held = true }: {
  title: string;
  rows: NonNullable<ModelPortfolioAttribution['top_contributors']>;
  hint: string;
  src: SourceKey;
  asOf?: string | null;
  /** ⚠ The numerator of the weight, named exactly — the AIRS book weights by `Beginwaarde`, the
   *  model by its own stated percentage, and the index by start-of-window cap. Passed in rather
   *  than guessed here: all three land in the same three columns. */
  weightHow: string;
  /** How one holding's return is computed, in the same terms. */
  returnHow: string;
  /** Whose basket these weights are a share of — "the model", or the benchmark's name. */
  owner?: string;
  /** False for the index's winners you do NOT own: same columns, different meaning. */
  held?: boolean;
}) {
  if (!rows?.length) return null;
  return (
    <div>
      <h5 className="text-[11px] font-semibold text-fg-strong">{title}</h5>
      <p className="text-[10px] text-fg-faint mb-1">{hint}</p>
      {/* `table-fixed` + this colgroup pin the numeric columns to a fixed width and let the Name
          column take the rest and TRUNCATE — without it the table sizes to its content and, in the
          narrower dock, spills past its grid cell and overlaps the neighbouring list. */}
      <table className="w-full text-[11px] table-fixed">
        {/* ⚠ Widened for the per-cell ⓘ. Every numeric column now carries a 14px chip plus its
            gap OUTSIDE the digits, and at the old `w-9` the weight column could not fit "4.7%"
            and an icon — under `table-fixed` that does not wrap, it spills over the neighbour. */}
        <colgroup>
          <col />
          <col className="w-12" />
          <col className="w-[4.5rem]" />
          <col className="w-[4.5rem]" />
        </colgroup>
        {/* Three bare percentages in a row (10.0% · +59.2% · +5.92%) are unreadable without
            labels — worse than an unexplained header, because there is nothing to hover. */}
        <thead>
          <tr className="text-fg-faint text-[9px] uppercase tracking-wide">
            <th className="py-0.5 pr-2 text-left font-medium">Name</th>
            <th className="py-0.5 px-1 text-right font-medium whitespace-nowrap">
              Weight
              <Provenance source={src} column kind="copied" note={`weight in ${owner}`}
                what={`Each holding's share of ${owner}.`}
                how={`${weightHow} ÷ Σ over ${owner}.`} />
            </th>
            <th className="py-0.5 px-1 text-right font-medium whitespace-nowrap">
              Ret.
              <Provenance source={src} column kind="formula" note="EUR return over the window"
                what="What each holding returned, in EUR."
                how={`${returnHow}.`} />
            </th>
            <th className="py-0.5 pl-1 text-right font-medium whitespace-nowrap">
              Contr.
              <Provenance source="derived" column kind="formula" note="contribution"
                what={held
                  ? `How much of ${owner}'s return each holding is responsible for — a big move in a tiny position contributes almost nothing, so this column ranks and the return beside it does not.`
                  : `What each holding was worth to ${owner}.`}
                how="weight × return." />
            </th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr key={`${r.isin ?? r.ticker ?? r.name}`} className="border-t border-neutral-800/20">
              <td className="py-1 pr-2 text-fg truncate" title={r.name ?? r.ticker ?? r.isin ?? ''}>
                {r.name ?? r.ticker ?? r.isin}
              </td>
              <td className="py-1 px-1 text-right font-mono text-fg-subtle">
                <Num prov={<Provenance source={src} asOf={asOf} kind="copied"
                  what={`${r.name ?? r.ticker ?? r.isin}'s share of ${owner}.`}
                  note={`weight in ${owner}`}
                  how={`${weightHow} ÷ Σ over ${owner} = ${n(r.weight_pct).toFixed(1)}%.`} />}>
                  {n(r.weight_pct).toFixed(1)}%
                </Num>
              </td>
              <td className="py-1 px-1 text-right font-mono text-fg-subtle">
                <Num prov={<Provenance source={src} asOf={asOf} kind="formula"
                  what={`What ${r.name ?? r.ticker ?? r.isin} returned, in EUR.`}
                  note="EUR return over the window"
                  how={`${returnHow} = ${pct(r.return_pct, 1)}.`} />}>
                  {pct(r.return_pct, 1)}
                </Num>
              </td>
              <td className="py-1 pl-1 text-right font-mono font-semibold">
                <Eff v={r.contribution_pct}
                  prov={<Provenance source="derived" kind="formula"
                    what={held
                      ? `How much of ${owner}'s return ${r.name ?? r.ticker ?? r.isin} is responsible for.`
                      : `What ${r.name ?? r.ticker ?? r.isin} was worth to ${owner}.`}
                    note={held ? `share of ${owner}'s return` : `what it was worth to ${owner}`}
                    how={`${n(r.weight_pct).toFixed(1)}% × ${pct(r.return_pct, 1)} = ${pp(r.contribution_pct)}.`} />} />
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
 * ⚠ THE SAME TABLE THE SECTOR-BAR DRILL-DOWN USES, imported rather than rebuilt — same columns,
 * same rank, same sort, same overlap treatment (a shared name tinted and dotted, the rest faded),
 * off the same payload. Two tables for one question is two things to learn.
 *
 * ⚠ BOTH LISTS ARE ON THE SAME BASE AS THE ROW ABOVE THEM. The backend renormalises each side's
 * per-holding weights over what that side can attribute, so the weights in each list ADD UP to the
 * "Your wt" / "Index wt" figures in the row that opened it — the check a reader will actually try.
 * They were raw shares of the whole portfolio once: Technology read 34.38% while its own holdings
 * summed to 9.11%, out by exactly 100/attributable_pct, and neither number was wrong on its own.
 *
 * ⚠ AN EMPTY SIDE IS A FINDING, NOT A BLANK. A bucket the index holds and you do not is an
 * allocation bet with no picks to judge — exactly what the row's Selection column says by being
 * 0.00pp. Saying so beats an empty box.
 */
function BucketNames({ row, bucket, benchmark }: {
  row: NonNullable<ModelPortfolioAttribution['rows']>[number];
  bucket: string;
  benchmark: string;
}) {
  const mine = row.portfolio_holdings ?? [];
  const theirs = row.benchmark_holdings ?? [];
  const shared = (rows: typeof mine) => rows.filter((h) => h.in_both).length;
  const sum = (rows: typeof mine) => rows.reduce((s, h) => s + n(h.weight_pct), 0);
  return (
    <div className="space-y-2">
      <div className="grid gap-4 lg:grid-cols-2 lg:items-start">
        <div>
          <p className="text-[11px] font-medium text-fg-muted mb-1">
            Your holdings <span className="text-fg-faint">({mine.length})</span>
            {shared(mine) > 0 && <span className="text-accent-400"> · {shared(mine)} in both</span>}
            {mine.length > 0 && (
              <span className="text-fg-faint"> · {sum(mine).toFixed(1)}% of the attributable model</span>
            )}
          </p>
          {mine.length
            ? <Holdings rows={mine} />
            : (
              <p className="text-[11px] text-fg-subtle py-1">
                {`You hold nothing in ${bucket} — the whole effect is the decision not to own it, `}
                {'which is why Selection and Interaction are zero on this row.'}
              </p>
            )}
        </div>
        <div>
          <p className="text-[11px] font-medium text-fg-muted mb-1">
            {benchmark} constituents <span className="text-fg-faint">({theirs.length})</span>
            {shared(theirs) > 0 && <span className="text-accent-400"> · {shared(theirs)} in both</span>}
            {theirs.length > 0 && (
              <span className="text-fg-faint"> · {sum(theirs).toFixed(1)}% of the index</span>
            )}
          </p>
          {theirs.length
            ? <Holdings rows={theirs} />
            : (
              <p className="text-[11px] text-fg-subtle py-1">
                {`${benchmark} holds nothing in ${bucket}, so there is no index return to judge `}
                {'your picks against — the whole effect is allocation.'}
              </p>
            )}
        </div>
      </div>
      {mine.some((h) => h.in_both) && (
        <p className="text-[10px] text-fg-faint flex items-center gap-1.5">
          <span className="w-1.5 h-1.5 rounded-full bg-accent-500 inline-block shrink-0" />
          marked rows are held in both your portfolio and {benchmark} (a share class counts as the
          same company)
        </p>
      )}
    </div>
  );
}

export default function AttributionPanel({ id, benchmark, window, source = 'model',
  portfolioAsOf, benchmarkAsOf, realisedSharePct, onClose }: {
  id: number; benchmark: string; window: 'ytd' | 'since';
  source?: 'model' | 'book';
  portfolioAsOf?: string | null; benchmarkAsOf?: string | null;
  /** ⚠ HOW MUCH OF THE YEAR HAPPENED IN POSITIONS SINCE SOLD — and is therefore ABSENT from every
   *  number in this panel. Brinson is `(w_p − w_b)(…)`: definitionally weight-based, and a sold
   *  position has no recoverable opening weight, so it cannot be included. Same class of
   *  distortion as `unpriced_pct` below, and larger — measured 22.5% on one book against a
   *  typical unpriced share of a few points. Stated, never quietly omitted. */
  realisedSharePct?: number | null;
  onClose: () => void;
}) {
  const [axis, setAxis] = useState<Axis>('sector');
  const [data, setData] = useState<ModelPortfolioAttribution | null>(null);
  const [error, setError] = useState<string | null>(null);
  /**
   * The bucket whose names are open, or null.
   *
   * ⚠ ONE AT A TIME, AND CLEARED WHEN THE AXIS CHANGES. Bucket names are not unique across axes
   * — "Technology" is a sector and "United States" a region — so a key left over from the previous
   * axis would either open nothing or, worse, open a same-named bucket on a table it does not
   * belong to.
   */
  const [openBucket, setOpenBucket] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      try {
        const r = await apiFetch(
          `${API_URL}/api/airs/model-portfolios/${id}/attribution`
          + `?benchmark=${benchmark}&window=${window}&axis=${axis}&source=${source}`);
        const b = await r.json().catch(() => null);
        if (cancelled) return;
        if (!r.ok) { setError(b?.detail ?? `HTTP ${r.status}`); return; }
        setData(b as ModelPortfolioAttribution);
      } catch (e) {
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { cancelled = true; };
  }, [id, benchmark, window, axis, source]);

  const label = window === 'ytd' ? 'YTD' : 'Since inception';
  // ONE source for the word: the axis the server actually computed, NOT the picker's state. If
  // the two disagree — a response still in flight, a server that normalises an unknown axis —
  // the labels must describe the numbers ON SCREEN, not the request that asked for them.
  const w = AXIS_WORD[data?.axis ?? axis] ?? 'group';
  // Where the PORTFOLIO side of every number came from — the AIRS book's VOLK values, or our
  // yfinance reconstruction. The benchmark side is always yfinance; the effect columns are derived.
  const pSrc: SourceKey = (data?.source ?? source) === 'book' ? 'airs_volk' : 'yfinance';
  // ⚠ THE PORTFOLIO WEIGHT IS A DIFFERENT QUANTITY IN THE TWO SOURCES, so a How that names one
  // is wrong for the other. The BOOK weights by `Beginwaarde` — the position's value when the
  // window opened, which is why a holding bought mid-window has weight 0 and drops out. The
  // MODEL weights by the composition's own stated percentage. Both then renormalise over the
  // attributable sleeve; only the numerator differs.
  const isBook = (data?.source ?? source) === 'book';
  const pWeightSrc = isBook ? 'Beginwaarde' : 'the model’s stated weight';
  const pReturnHow = isBook
    ? 'value now ÷ Beginwaarde − 1'
    : 'EUR close at the window’s end ÷ its close at the start − 1';

  return (
    <section className="bg-card border border-accent-500/30 rounded-xl p-4">
      <div className="flex items-start justify-between gap-3 mb-2">
        <div>
          <h4 className="text-sm font-semibold text-fg-strong">
            {`Why — ${label} vs ${benchmark}`}
          </h4>
        </div>
        <div className="flex items-center gap-2 shrink-0">
          <select value={axis}
            onChange={(e) => { setData(null); setOpenBucket(null); setAxis(e.target.value as Axis); }}
            className="bg-page border border-neutral-700 rounded-lg px-2 py-1 text-[11px] text-fg focus:border-accent-500">
            <option value="sector">by Sector</option>
            <option value="region">by Region</option>
            <option value="currency">by Currency</option>
          </select>
          <button type="button" onClick={onClose}
            className="cursor-pointer text-[11px] px-2 py-1 rounded-lg border border-neutral-700 text-fg-muted hover:text-accent-300 transition-colors">
            Hide
          </button>
        </div>
      </div>

      {error && (
        <div className="bg-neg-500/10 border border-neg-500/20 rounded-lg px-3 py-2 text-xs text-neg-300">
          {error}
        </div>
      )}
      {!data && !error && <p className="text-xs text-fg-subtle">Computing attribution…</p>}

      {data && (
        <>
          {data.note && (
            <p className="text-[11px] text-warn-300 mb-2">⚠ {data.note}</p>
          )}
          {/* ⚠ The identity IS the decomposition. If it fails, these are just three columns. */}
          {!data.reconciles && (
            <p className="text-[11px] text-neg-300 mb-2">
              {'⚠ The effects do not sum to the excess (residual '}
              {pct(data.residual_pct, 4)}
              {'). This is NOT a valid decomposition — do not read the rows below as one.'}
            </p>
          )}

          {/* ⚠ THE TWO "EXCESS" FIGURES ON THIS SCREEN ARE DIFFERENT QUANTITIES, AND SAYING SO IS
              THE ONLY THING THAT MAKES EITHER READABLE. The tile one click away shows the
              ACCOUNT's excess — AIRS's own flow-aware return, cash included, carrying dividends
              from positions closed during the year. This table decomposes the ATTRIBUTABLE
              SLEEVE: the holdings that have a sector at all, renormalised once cash and funds
              come out (cash has no sector; leaving it in would score holding cash as a sector
              bet). Measured on AITopSelectie OFF DYN against the same benchmark: +24.26pp on the
              tile, +23.39pp here. Neither is wrong; presenting them as one number, in one word,
              one click apart, was. */}
          {data.unattributed_excess_pct != null
            && Math.abs(data.unattributed_excess_pct) > 0.005 && (
            <p className="text-[11px] text-fg-faint mb-2">
              {'This table explains '}
              <span className="font-mono text-fg-muted">{pp(data.excess_pct)}</span>
              {' of the account’s '}
              <span className="font-mono text-fg-muted">{pp(data.account_excess_pct)}</span>
              {' excess. The remaining '}
              <span className="font-mono text-fg-muted">{pp(data.unattributed_excess_pct)}</span>
              {' is cash, income on positions closed during the year, and the account’s own '
                + 'flows — real return with no '}
              {AXIS_WORD[axis] ?? 'bucket'}
              {' to attribute it to.'}
            </p>
          )}

          {/* ⚠⚠ A SOLD POSITION IS THE SAME FALSE FINDING AS AN UNPRICED ONE, CAUSED BY TIME
              RATHER THAN BY PRICING — its sector reads as UNOWNED here, so a sector the book
              traded out of entirely is credited or blamed for an allocation call it never made.
              It cannot be fixed by adding the sold names: an allocation effect needs an opening
              weight, and a sold parcel has none that is recoverable. Placed ABOVE the unpriced
              note because it is the larger of the two. */}
          {(realisedSharePct ?? 0) >= 1 && (
            <p className="text-[11px] text-warn-300 mb-2">
              {'⚠ '}
              <span className="font-mono">{(realisedSharePct ?? 0).toFixed(0)}%</span>
              {' of this book’s year happened in positions it has since SOLD. They carry no '
                + 'opening weight, so none of them is in this table — a sector traded out of '
                + 'entirely reads here as one that was never owned, which is a false finding, not '
                + 'a missing one. The sold names are itemised under Holdings.'}
            </p>
          )}

          {/* ⚠ An UNPRICED holding makes its sector read as UNOWNED, so the allocation effect on
              that row is a FALSE finding — not a missing one. Name the rows to discount. */}
          {(data.unpriced_pct ?? 0) > 0.05 && (
            <p className="text-[11px] text-warn-300 mb-2">
              {'⚠ '}
              <span className="font-mono">{(data.unpriced_pct ?? 0).toFixed(0)}%</span>
              {' of this model is a holding we cannot price ('}
              {(data.unpriced_buckets ?? []).join(', ')}
              {`). Those sectors read as unowned below, so their allocation effect there is a false `}
              {'finding — discount those rows.'}
            </p>
          )}

          {/* ⚠ Shown only when something IS excluded. At full coverage the old line read
              "Explains 100% of the model. 0% is excluded", spending three clauses to say
              nothing was left out — a caveat that fires when it does not apply trains a
              reader to skip it, which is exactly when it needs to be read. */}
          {(data.excluded_pct ?? 0) >= 0.5 && (
            <p className="text-xs text-fg-faint mb-3">
              {'Excludes '}
              <span className="font-mono">{(data.excluded_pct ?? 0).toFixed(0)}%</span>
              {' of the model (funds and cash).'}
            </p>
          )}

          {/* ⚠ NO FORMULA STRIP HERE. Each effect's arithmetic lives in its OWN column header's
              info icon (`Th prov` → `Provenance how`), stated in the SAME WORDS the headers use
              so the two cannot drift — a strip above the table restated all three permanently,
              so the panel carried every formula twice. */}
          <div className="overflow-auto rounded-lg border border-neutral-800/40 mb-3">
            <table className="w-full text-[11px]">
              <thead className="bg-card">
                <tr className="text-fg-faint text-[10px] uppercase tracking-wide border-b border-neutral-800/40">
                  <Th align="left" label={w}
                    prov={<Provenance source="derived" column kind="formula" note={`the ${w}s`}
                      what={`The ${w}s the excess is split across — including the ones ${benchmark} holds and you do NOT, because choosing not to own something is a decision the numbers can price.`}
                      how={`Every ${w} held on either side, from the same classification both sides are read with.`} />} />
                  {/* ⚠ WORDS, NOT `w_P` / `R_B`. The subscripted notation was readable only while
                      the formula strip below carried its key (w = weight, R = return, P = you,
                      B = index); with that strip gone the symbols arrive undecoded, and the
                      `uppercase` on this row was flattening the lowercase `w` that carries half
                      the convention. A header that needs a hover to be read at all is a header
                      that will be guessed at instead. The tooltip formulas use the same words, so
                      the two cannot drift apart the way symbols and a missing key did. */}
                  <Th label="Your wt"
                    prov={<Provenance source={pSrc} column kind="formula"
                      what={`Your share of the attributable model in each ${w} — funds and cash removed, the rest renormalised to 100%, so it is not the raw model weight.`}
                      note={`your weight in this ${w}`}
                      how={`Σ(${pWeightSrc}) over your ${w} holdings ÷ Σ over all attributable holdings.`} />} />
                  <Th label="Index wt"
                    prov={<Provenance source="benchmark" column kind="formula"
                      what={`${benchmark}'s share in each ${w}, at the START of the window — weighting by today’s cap would be look-ahead.`}
                      note={`${benchmark} weight in this ${w}`}
                      how={`Σ(start-of-window cap weight) over ${benchmark}'s ${w} constituents ÷ Σ over the index.`} />} />
                  <Th label="Your ret."
                    prov={<Provenance source={pSrc} column kind="formula"
                      what={`What your holdings in each ${w} returned, in EUR. A dash means you hold nothing there.`}
                      note={`your return in this ${w}`}
                      how={`Σ(wᵢ × rᵢ) ÷ Σwᵢ over your ${w} holdings, where rᵢ = ${pReturnHow}.`} />} />
                  {/* The reference point. Allocation is scored against THIS number, so it has to
                      be on the screen — an over/underweight is judged by whether its sector beat
                      or lagged the index as a whole, not by whether it went up. */}
                  <Th label="Index ret."
                    prov={<Provenance source="benchmark" column kind="formula"
                      what={`What ${benchmark}'s holdings in each ${w} returned, in EUR.`}
                      note={`${benchmark} return in this ${w}`}
                      how={`Σ(wᵢ × rᵢ) ÷ Σwᵢ over ${benchmark}'s ${w} constituents, rᵢ in EUR.`} />} />
                  <Th label="Allocation"
                    prov={<Provenance source="derived" column kind="formula" note="Brinson-Fachler allocation"
                      what={`What choosing where to put the money was worth — scored against the index total, so a ${w} that rose by LESS than the index counts against you.`}
                      how={`(your wt − index wt) × (index ret − index total ${pct(data.benchmark_return_pct, 1)}).`} />} />
                  <Th label="Selection"
                    prov={<Provenance source="derived" column kind="formula" note="Brinson selection"
                      what="What choosing which companies to hold was worth, scored at the index’s weight so sizing is held constant."
                      how="index wt × (your ret − index ret)." />} />
                  <Th label="Interact."
                    prov={<Provenance source="derived" column kind="formula" note="interaction (the cross term)"
                      what="What the tilt and the picks were worth together."
                      how="(your wt − index wt) × (your ret − index ret)." />} />
                  <Th label="Total"
                    prov={<Provenance source="derived" column kind="formula" note={`this ${w}'s share of the excess`}
                      what={`Each ${w}'s whole share of the excess. The column sums to the excess, and that identity is checked, not assumed.`}
                      how="Allocation + Selection + Interaction." />} />
                </tr>
              </thead>
              <tbody className="divide-y divide-neutral-800/20">
                {(data.rows ?? []).map((r) => {
                  // The row's own figures, formatted once — every card below quotes them, so a
                  // reader can check the arithmetic against the digits in the row.
                  const wP = n(r.portfolio_weight_pct);
                  const wB = n(r.benchmark_weight_pct);
                  const rP = pct(r.portfolio_return_pct, 1);
                  const rB = pct(r.benchmark_return_pct, 1);
                  const rBt = pct(data.benchmark_return_pct, 1);
                  const open = openBucket === r.bucket;
                  return (
                    <Fragment key={r.bucket}>
                    {/* ⚠ THE WHOLE ROW IS THE HIT TARGET, not a chevron in the first cell. Every
                        figure on it belongs to the bucket the drill-down explains, so any of them
                        is a reasonable place to click and ask "which names is this?". */}
                    <tr onClick={() => setOpenBucket(open ? null : r.bucket)}
                      title={open ? `Hide the names behind ${r.bucket}`
                        : `Show the names behind ${r.bucket} — what you hold and what ${benchmark} holds`}
                      className={`cursor-pointer transition-colors ${
                        open ? 'bg-accent-500/[0.07]' : 'hover:bg-overlay/[0.02]'}`}>
                      <td className="px-2 py-1.5 text-fg whitespace-nowrap">
                        <span className={`inline-block w-3 text-[9px] ${open ? 'text-accent-400' : 'text-fg-faint'}`}>
                          {open ? '▾' : '▸'}
                        </span>
                        {r.bucket}
                      </td>
                      <td className="px-2 py-1.5 text-right font-mono text-fg-subtle">
                        <Num prov={<Provenance source={pSrc} asOf={portfolioAsOf} kind="formula"
                          what={`Your share of the attributable model held in ${r.bucket}.`}
                          note={`your weight in ${r.bucket}`}
                          how={`Σ(${pWeightSrc}) over your ${r.bucket} holdings ÷ Σ over all attributable holdings = ${wP.toFixed(1)}%.`} />}>
                          {wP.toFixed(1)}
                        </Num>
                      </td>
                      <td className="px-2 py-1.5 text-right font-mono text-fg-subtle">
                        <Num prov={<Provenance source="benchmark" asOf={benchmarkAsOf} kind="formula"
                          what={`${benchmark}'s share held in ${r.bucket}.`}
                          note={`${benchmark} weight in ${r.bucket}`}
                          how={`Σ(start-of-window cap weight) over ${benchmark}'s ${r.bucket} constituents ÷ Σ over the index = ${wB.toFixed(1)}%.`} />}>
                          {wB.toFixed(1)}
                        </Num>
                      </td>
                      <td className="px-2 py-1.5 text-right font-mono text-fg-subtle">
                        <Num prov={<Provenance source={pSrc} asOf={portfolioAsOf} kind="formula"
                          what={`What your ${r.bucket} holdings returned, in EUR.`}
                          note={`your return in ${r.bucket}`}
                          how={`Σ(wᵢ × rᵢ) ÷ Σwᵢ over your ${r.bucket} holdings = ${rP}, with rᵢ = ${pReturnHow}.`} />}>
                          {rP}
                        </Num>
                      </td>
                      <td className="px-2 py-1.5 text-right font-mono text-fg-subtle">
                        <Num prov={<Provenance source="benchmark" asOf={benchmarkAsOf} kind="formula"
                          what={`What ${benchmark}'s ${r.bucket} holdings returned, in EUR.`}
                          note={`${benchmark} return in ${r.bucket}`}
                          how={`Σ(wᵢ × rᵢ) ÷ Σwᵢ over ${benchmark}'s ${r.bucket} constituents = ${rB}, rᵢ in EUR.`} />}>
                          {rB}
                        </Num>
                      </td>
                      <td className="px-2 py-1.5 text-right font-mono">
                        <Eff v={n(r.allocation_pct)}
                          prov={<Provenance source="derived" kind="formula" note={`allocation — ${r.bucket}`}
                            what={`What your ${r.bucket} over/underweight was worth, scored against the index total (${rBt}).`}
                            how={`(${wP.toFixed(1)}% − ${wB.toFixed(1)}%) × (${rB} − ${rBt}) = ${pp(r.allocation_pct)}.`} />} />
                      </td>
                      <td className="px-2 py-1.5 text-right font-mono">
                        <Eff v={n(r.selection_pct)}
                          prov={<Provenance source="derived" kind="formula" note={`selection — ${r.bucket}`}
                            what={`What your ${r.bucket} company picks were worth, scored at the index’s weight.`}
                            how={`${wB.toFixed(1)}% × (${rP} − ${rB}) = ${pp(r.selection_pct)}.`} />} />
                      </td>
                      <td className="px-2 py-1.5 text-right font-mono">
                        <Eff v={n(r.interaction_pct)}
                          prov={<Provenance source="derived" kind="formula" note={`interaction — ${r.bucket}`}
                            what={`What the ${r.bucket} tilt and picks were worth together.`}
                            how={`(${wP.toFixed(1)}% − ${wB.toFixed(1)}%) × (${rP} − ${rB}) = ${pp(r.interaction_pct)}.`} />} />
                      </td>
                      <td className="px-2 py-1.5 text-right font-mono font-semibold">
                        <Eff v={n(r.total_pct)}
                          prov={<Provenance source="derived" kind="formula" note={`${r.bucket}'s share of the excess`}
                            what={`${r.bucket}'s whole share of the excess.`}
                            how={`${pp(r.allocation_pct)} + ${pp(r.selection_pct)} + ${pp(r.interaction_pct)} = ${pp(r.total_pct)}.`} />} />
                      </td>
                    </tr>
                    {open && (
                      <tr className="bg-inset/60">
                        <td colSpan={9} className="px-3 py-3">
                          <BucketNames row={r} bucket={r.bucket} benchmark={benchmark} />
                        </td>
                      </tr>
                    )}
                    </Fragment>
                  );
                })}
              </tbody>
              <tfoot>
                <tr className="border-t border-neutral-800/40 font-semibold">
                  <td className="px-2 py-1.5 text-fg" colSpan={5}>Total (= the excess)</td>
                  <td className="px-2 py-1.5 text-right font-mono">
                    <Eff v={(data.rows ?? []).reduce((s, r) => s + n(r.allocation_pct), 0)}
                      prov={<Provenance source="derived" kind="formula" note="total allocation"
                        what={`What choosing where to put the money was worth, across every ${w}.`}
                        how={`Every ${w}'s allocation effect, summed.`} />} />
                  </td>
                  <td className="px-2 py-1.5 text-right font-mono">
                    <Eff v={(data.rows ?? []).reduce((s, r) => s + n(r.selection_pct), 0)}
                      prov={<Provenance source="derived" kind="formula" note="total selection"
                        what={`What choosing which companies to hold was worth, across every ${w}.`}
                        how={`Every ${w}'s selection effect, summed.`} />} />
                  </td>
                  <td className="px-2 py-1.5 text-right font-mono">
                    <Eff v={(data.rows ?? []).reduce((s, r) => s + n(r.interaction_pct), 0)}
                      prov={<Provenance source="derived" kind="formula" note="total interaction"
                        what={`The cross terms, across every ${w}.`}
                        how={`Every ${w}'s interaction effect, summed.`} />} />
                  </td>
                  <td className="px-2 py-1.5 text-right font-mono">
                    <Eff v={data.attributed_pct}
                      prov={<Provenance source="derived" kind="formula" note="the attributed excess"
                        what="The excess this table explains."
                        how={`Allocation + selection + interaction across every ${w}, over the attributable holdings.`} />} />
                  </td>
                </tr>
              </tfoot>
            </table>
          </div>

          <div className="grid gap-4 md:grid-cols-3">
            <Names title="Biggest contributors" rows={data.top_contributors ?? []}
              hint="weight × return, in EUR" src={pSrc} asOf={portfolioAsOf}
              weightHow={pWeightSrc} returnHow={pReturnHow} />
            <Names title="Biggest detractors" rows={data.top_detractors ?? []}
              hint="what cost you the most" src={pSrc} asOf={portfolioAsOf}
              weightHow={pWeightSrc} returnHow={pReturnHow} />
            {/* The other half of "why" — and the half a holdings-only view can never show.
                ⚠ `held={false}`: these three columns are the INDEX's weight, the index's return
                and what the name was worth TO THE INDEX. Same columns as the two lists beside it,
                different subject — the per-cell text has to say so or a benchmark's gain reads as
                something that happened in your book. */}
            <Names title={`${benchmark} winners you didn’t own`} rows={data.missed_winners ?? []}
              hint="matched by COMPANY, not ISIN — a share class is not a different business"
              src="benchmark" asOf={benchmarkAsOf} owner={benchmark} held={false}
              weightHow="start-of-window cap weight"
              returnHow="EUR close at the window’s end ÷ its close at the start − 1" />
          </div>
        </>
      )}
    </section>
  );
}
