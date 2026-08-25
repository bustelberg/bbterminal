'use client';

import { useEffect, useMemo, useRef, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import type { ModelPortfolioAttribution } from '../../../lib/types/api';

type Attr = ModelPortfolioAttribution;
type Bucket = NonNullable<Attr['rows']>[number];
type Name = NonNullable<Bucket['portfolio_holdings']>[number];

const AXIS_LABEL: Record<string, string> = { sector: 'Sector', region: 'Region', currency: 'Currency' };

/** A return / effect, coloured by sign. `—` when it could not be measured — never a 0. */
function Num({ v, pp }: { v?: number | null; pp?: boolean }) {
  if (v == null) return <span className="text-fg-faint">—</span>;
  return (
    <span className={v >= 0 ? 'text-pos-400' : 'text-neg-400'}>
      {v >= 0 ? '+' : ''}{v.toFixed(2)}{pp ? 'pp' : '%'}
    </span>
  );
}

/** One holdings table — your names or the index's. `table-fixed` + this shared colgroup gives
 *  the numeric columns identical widths in BOTH tables, so Weight/Return/Contrib line up between
 *  "your holdings" and the index's; the Name column takes the rest and truncates (full name on
 *  hover). */
function HoldingsCols() {
  return (
    <colgroup>
      {/* Rank — narrow and fixed, so a 2-digit number never steals width from the name. ⚠ NOT
          widened with the three below: it holds at most two digits, and the space would come
          straight out of the name column for nothing. */}
      <col className="w-[1.75rem]" />
      <col />
      {/* ⚠ 1.5× THE ORIGINAL 4 / 4.25 / 4.5rem (2026-08-10, on request). The width comes out of
          the NAME column, which is the only auto one — so these three gain exactly what the
          truncated name loses. That is the trade being made deliberately: the figures are the
          reason the panel is open, and a clipped name still has its full text on hover. */}
      <col className="w-[6rem]" />
      <col className="w-[6.375rem]" />
      <col className="w-[6.75rem]" />
    </colgroup>
  );
}

type SortKey = 'name' | 'weight' | 'return' | 'contrib';

const SORT_VAL: Record<SortKey, (h: Name) => number | string | null> = {
  name: (h) => (h.name ?? '').toLowerCase(),
  weight: (h) => h.weight_pct ?? null,
  return: (h) => h.return_pct ?? null,
  contrib: (h) => h.contribution_pct ?? null,
};

/** ⚠ NAMED, BECAUSE A BARE "Weight" INVITES THE WRONG COMPARISON — and because this one used to be
 *  a genuinely different number. Until 2026-07-31 the composition chart divided TODAY's value by
 *  the whole equity sleeve while this panel divided the START-of-window value by the attributable
 *  holdings: Technology read 36% there and 39.1% here, ASML 7.30% against 5.75%. Both correct,
 *  which is what made it unarbitrable. The composition now adopts this basis, so the two agree —
 *  the label stays because the basis is still not self-evident from a percentage. */
/**
 * ⚠⚠ THIS SENTENCE'S CLAIM — "a bucket total here equals its bar" — WAS FALSE FOR THE INDEX FOR A
 * YEAR, AND NOBODY COULD TELL UNTIL THE TOTAL ROW MADE IT CHECKABLE. The composition chart weighed
 * the index by `market_cap_eur` (TODAY's cap) while this list has always used
 * `index_rows(label, start)` (the cap at the window's open), so SP500 Technology read **34.90% on
 * the bar against 31.24% here** — and the bar sat under an axis note saying "Start-of-window
 * weights".
 *
 * ⚠ IT WAS NOT A LABELLING PROBLEM. `diff_pct`, the TILT the two bars exist to show, subtracted a
 * today-weighted index from a start-weighted book — a difference computed across two bases. Fixed
 * in `_airs_portfolio_analysis` (2026-08-10) by weighing the index at the window's open too,
 * dropping any constituent with no start cap rather than letting it keep today's (which had left a
 * 0.68pp residue). Both figures are now 31.24% on the same constituent set, by construction.
 *
 * ⚠ SO THE CLAIM BELOW IS TRUE AGAIN — for both sides — and it is worth keeping precisely because
 * it is the thing a reader can check in five seconds. If it ever stops holding, the cause is a
 * basis drifting apart again, not a rounding.
 */
const WEIGHT_HINT = 'Share of the attributable holdings (funds, cash and unpriced names removed, '
  + 'the rest renormalised to 100%), weighted by each position\'s value when the window OPENED. '
  + 'The composition chart is weighted the same way — your bar and the index\'s — so a bucket '
  + 'total here equals its bar.';

/** ⚠ EXPORTED, AND THE ATTRIBUTION TABLE'S ROW DRILL-DOWN USES THE SAME ONE. Both answer the
 *  identical question — "which names are behind this bucket, on each side" — off the identical
 *  payload. A second table with its own columns, sort and overlap treatment would be two
 *  appearances of one fact, and the reader would have to learn which is which. */
export function Holdings({ rows, startLabel = 'Start of window' }: {
  rows: Name[];
  /**
   * WHEN the weight was measured, named in the header.
   *
   * ⚠⚠ A PROP, NOT THE LITERAL "Start of year", BECAUSE THIS TABLE SERVES TWO WINDOWS. The
   * `/bucket` drill-down pins `window=ytd`, so there the start IS 1 January — but `AttributionPanel`
   * has a window toggle and renders this same table for SINCE-INCEPTION, where the weight is the
   * one held at the model's inception and could be any date in 2024. Hardcoding the year would put
   * a wrong date on half the drill-downs, in the calmest possible way: a header that reads correctly
   * and describes a different measurement.
   *
   * ⚠ THE DEFAULT IS THE VAGUE-BUT-TRUE ONE. A caller that forgets to say which window it is on
   * gets "Start of window", which is right for every window; it does not get a confident "Start of
   * year" that is right for one of them.
   */
  startLabel?: string;
}) {
  // Sortable — click a header to toggle direction. Default: weight, largest first. Each table sorts
  // on its OWN state (your names and the index's are independent lists).
  const [key, setKey] = useState<SortKey>('weight');
  const [dir, setDir] = useState<'asc' | 'desc'>('desc');
  /**
   * The three figures the total row shows — computed BEFORE the early return so the hook order is
   * fixed, which is why this sits above `if (!rows.length)`.
   *
   * ⚠ THE RETURN IS THE ONLY ONE THAT IS NOT A SUM, and it is weighted by the START weight over
   * the names that HAVE a return. A plain mean would let a 0.1% holding that doubled pull as hard
   * as a 9% one that stood still; counting an unpriceable name as a zero would drag the average
   * toward nothing by exactly the weight we could not measure. Renormalising puts it out of both
   * sides of the ratio, which is the same discipline every other weighted figure in this app uses.
   *
   * ⚠ THE THREE RECONCILE OVER THE **PRICED** NAMES, NOT OVER THE WEIGHT CELL. Contribution is
   * `w · r / 100` per row, so `Σw(priced) × return ÷ 100 == contrib` exactly — but the Weight cell
   * sums EVERY name, including the ones with no return. Measured on a five-name bucket: weight
   * 8.50%, priced weight 7.60%, and it is the 7.60 that ties. Reading the row as
   * `8.50 × 14.29 ÷ 100` and finding 1.21 against a printed 1.09 is not a bug — it is the
   * unpriceable 0.90% showing up, which is why the return cell states its own denominator.
   */
  const totals = useMemo(() => {
    const weight = rows.reduce((s, h) => s + (h.weight_pct ?? 0), 0);
    const priced = rows.filter((h) => h.return_pct != null && (h.weight_pct ?? 0) > 0);
    const den = priced.reduce((s, h) => s + h.weight_pct!, 0);
    const contribRows = rows.filter((h) => h.contribution_pct != null);
    return {
      weight,
      ret: den > 0 ? priced.reduce((s, h) => s + h.weight_pct! * h.return_pct!, 0) / den : null,
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
  const th = 'py-1 font-medium cursor-pointer select-none whitespace-nowrap hover:text-fg-soft';

  return (
    <table className="w-full text-[12px] table-fixed">
      <HoldingsCols />
      <thead>
        <tr className="text-fg-faint text-[11px] uppercase tracking-wide">
          {/* Not sortable: the rank IS the position under the ACTIVE sort, so clicking it could
              only mean "sort by the current sort". It renumbers whenever the sort changes. */}
          <th className="pr-1 text-right font-normal">#</th>
          <th className={`${th} pr-2 text-left`} onClick={() => click('name')}>Name{caret('name')}</th>
          {/* ⚠ THE QUALIFIER SITS ON ITS OWN LINE, not beside the word. "Weight (Start of year)" is
              ~130px of nowrap text in a 6rem column, and under `table-fixed` that does not shrink
              the column — it spills over Return. A `block` span wraps it instead, so the column
              keeps its width and the Name column beside it keeps the space it would have lost.
              `whitespace-normal` because the shared `th` class is nowrap, which would otherwise
              stop the qualifier wrapping inside its own line too. */}
          <th className={`${th} px-1 text-right`} onClick={() => click('weight')} title={WEIGHT_HINT}>
            Weight{caret('weight')}
            <span className="block normal-case whitespace-normal font-normal text-fg-subtle">
              ({startLabel})
            </span>
          </th>
          <th className={`${th} px-1 text-right`} onClick={() => click('return')}>Return{caret('return')}</th>
          <th className={`${th} pl-1 text-right`} onClick={() => click('contrib')}>Contrib.{caret('contrib')}</th>
        </tr>
      </thead>
      <tbody>
        {/* ⚠ THE TOTAL SITS AT THE TOP AND IS NOT NUMBERED. At the top because it is the answer the
            list is evidence for — on a 40-name index bucket a footer total is below the fold, and
            the reader is comparing this figure against the one in the other table, not reading to
            the end. Unnumbered because it is not a holding: a "1" here would push every name's
            rank up by one against the list it summarises. It also does not move when the headers
            are clicked — a total has no position in a sort. */}
        <tr className="border-t border-neutral-800/40 bg-inset font-semibold text-fg-strong">
          <td />
          <td className="py-1 pr-2 truncate" title={`All ${rows.length} name${rows.length === 1 ? '' : 's'} in this bucket`}>
            Total <span className="text-fg-faint font-normal">({rows.length})</span>
          </td>
          <td className="py-1 px-1 text-right font-mono tabular-nums" title={WEIGHT_HINT}>
            {totals.weight.toFixed(2)}%
          </td>
          {/* ⚠ WEIGHTED BY THE START WEIGHT, NEVER A PLAIN MEAN — a 0.1% holding that doubled
              would otherwise pull this as hard as a 9% one that did nothing. Renormalised over the
              names that HAVE a return, so a row we could not price is out of both sides of the
              ratio rather than counted as a zero. */}
          <td className="py-1 px-1 text-right font-mono"
            title={totals.retRows < rows.length
              ? `Weighted by start weight, over the ${totals.retRows} of ${rows.length} names with a return`
              : 'Weighted by start weight'}>
            <Num v={totals.ret} />
          </td>
          {/* ⚠ A PLAIN SUM, AND IT IS ALLOWED TO BE ONE because contribution is percentage POINTS
              of the basket's return — points add, percentages do not. It ties to the two cells
              left of it as `priced weight × return ÷ 100`, which is the Weight cell only when
              every name has a return; see the memo for why that distinction is stated rather than
              rounded over. */}
          <td className="py-1 pl-1 text-right font-mono"><Num v={totals.contrib} pp /></td>
        </tr>
        {sorted.map((h, i) => (
          // THE INTERSECTION IS THE POINT: a name held on both sides is emphasised (tint + bold +
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
                    title="Held in both your portfolio and the benchmark" />
                )}
                <span className={`truncate ${h.in_both ? 'text-fg-strong font-medium' : 'text-fg-soft'}`}>{h.name ?? '—'}</span>
              </span>
            </td>
            <td className="py-1 px-1 text-right font-mono text-fg" title={WEIGHT_HINT}>{(h.weight_pct ?? 0).toFixed(2)}%</td>
            <td className="py-1 px-1 text-right font-mono"><Num v={h.return_pct} /></td>
            {/* Contribution = weight × return — percentage POINTS of the basket's return, not %. */}
            <td className="py-1 pl-1 text-right font-mono"><Num v={h.contribution_pct} pp /></td>
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
  /** ⚠ MUST MATCH THE MODAL — the same value the Attribution panel gets. Omit it and the backend
   *  defaults to `model` (the design percentages, a flat 5.00% each) while the Attribution panel
   *  above is decomposing BEGINWAARDE start weights. Two panels in one modal, same portfolio, same
   *  window, different weights — and neither says so. */
  source?: 'model' | 'book';
  onClose: () => void;
}) {
  const [attr, setAttr] = useState<Attr | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const ref = useRef<HTMLElement>(null);

  // Docked full-width BELOW the charts. `nearest` scrolls it into view only when it is not already
  // ⚠ NO `scrollIntoView` ANY MORE. It existed to reveal this panel when it sat in the flow
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
  const shared = (list?: Name[] | null) => (list ?? []).filter((h) => h.in_both).length;

  return (
    /* ⚠⚠ `h-full min-h-0 flex flex-col` + an inner scroll — the shape `PanelDialog` requires.
       The dialog is a FIXED box; a body that sizes to its content would overflow it silently,
       and `min-h-0` is what lets a flex child shrink below its content so the scroll actually
       engages. Same construction as `ActiveSharePanel`. */
    <section ref={ref} className="h-full min-h-0 flex flex-col bg-card border
      border-accent-500/30 rounded-xl p-4">
      {/* ⚠ `shrink-0` — the heading names which bar was clicked, which is the one thing that
          must stay visible while the tables under it scroll. */}
      <div className="shrink-0 flex items-start justify-between gap-3 mb-2">
        <h4 className="text-sm font-semibold text-fg-strong">
          {AXIS_LABEL[axis] ?? axis}: <span className="font-mono">{bucket}</span>
        </h4>
        <button onClick={onClose}
          className="cursor-pointer text-[12px] px-2 py-1 rounded-lg border border-neutral-700 text-fg-muted hover:text-accent-300 shrink-0">
          ✕
        </button>
      </div>

      {/* ⚠ EVERYTHING BELOW THE HEADING SCROLLS AS ONE. `min-h-0` is what lets it: a flex child
          refuses to shrink below its content without it, so the fixed dialog would silently
          give way instead of the body scrolling — see the ⚠⚠ on the root. */}
      <div className="flex-1 min-h-0 overflow-auto">

      {loading && <p className="text-xs text-fg-subtle">Computing attribution…</p>}
      {error && (
        <div className="bg-neg-500/10 border border-neg-500/20 rounded-lg px-3 py-2 text-xs text-neg-300">{error}</div>
      )}

      {!loading && !error && attr && (
        <>
          {row && (
            <div className="text-[12px] text-fg-soft mb-3 flex flex-wrap items-center gap-x-4 gap-y-1">
              {attr?.start && (
                <span className="font-mono text-fg-muted">Since {attr.start}</span>
              )}
              <span title="Overweighting a group that beat the index (vs the whole index's return).">
                allocation <Num v={row.allocation_pct} pp />
              </span>
              <span title="Did your names in this group beat the index's names in it?">
                selection <Num v={row.selection_pct} pp />
              </span>
              <span title="The cross term (over/underweight × out/under-performance).">
                interaction <Num v={row.interaction_pct} pp />
              </span>
              <span className="font-semibold">total <Num v={row.total_pct} pp /></span>
            </div>
          )}

          {nonAttributable && (
            <p className="text-[12px] text-fg-faint mb-2">
              Funds, cash and unclassified holdings are not a sector bet, so this bucket is not
              decomposed — just the holdings in it.
            </p>
          )}

          {row ? (
            <>
              {/* Your names and the index's, SIDE BY SIDE on wide screens so the full-width dock is
                  used — stacked on narrow ones. */}
              <div className="grid gap-4 lg:grid-cols-2 lg:items-start">
                <div>
                  <p className="text-[12px] font-medium text-fg-muted mb-1">
                    Your holdings <span className="text-fg-faint">({row.portfolio_holdings?.length ?? 0})</span>
                    {shared(row.portfolio_holdings) > 0 && (
                      <span className="text-accent-400"> · {shared(row.portfolio_holdings)} in both</span>
                    )}
                  </p>
                  {/* ⚠ "Start of year" IS SAFE HERE ONLY BECAUSE THIS PANEL PINS `window=ytd` in
                      its own request (see the fetch above). If that ever becomes a toggle, this
                      label has to follow it — see `Holdings`'s `startLabel`. */}
                  <Holdings rows={row.portfolio_holdings ?? []} startLabel="Start of year" />
                </div>
                <div>
                  <p className="text-[12px] font-medium text-fg-muted mb-1">
                    {benchmark} constituents{' '}
                    <span className="text-fg-faint">({row.benchmark_holdings?.length ?? 0})</span>
                    {shared(row.benchmark_holdings) > 0 && (
                      <span className="text-accent-400"> · {shared(row.benchmark_holdings)} in both</span>
                    )}
                    {/* ⚠ THIS NOTE EXISTS BECAUSE THE BASIS WAS WRONG HERE ONCE AND NOTHING SAID SO.
                        The index bar was weighted by today's caps against a list weighted at the
                        window's open (SP500 Technology: 34.90% vs 31.24%), under an axis label
                        claiming start-of-window. Both sides are now weighed at the open, so the
                        total below DOES match the bar — and naming the basis is what lets a reader
                        notice if that ever stops being true. */}
                    <span className="text-fg-faint font-normal"
                      title="Market caps as at the window's open, renormalised over the attributable constituents — the same basis the index bar on the composition chart uses, so this total matches it. A return must be weighted by what was held when it started, not by what the constituents are worth today.">
                      {' '}· weighted at window open
                    </span>
                  </p>
                  <Holdings rows={row.benchmark_holdings ?? []} startLabel="Start of year" />
                </div>
              </div>
              {(row.portfolio_holdings ?? []).some((h) => h.in_both) && (
                <p className="text-[11px] text-fg-faint flex items-center gap-1.5 mt-3">
                  <span className="w-1.5 h-1.5 rounded-full bg-accent-500 inline-block shrink-0" />
                  marked rows are held in both your portfolio and {benchmark} (a share class counts as the same company)
                </p>
              )}
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
              No holdings behind this bucket in the YTD window.
            </p>
          )}
        </>
      )}
      </div>
    </section>
  );
}
