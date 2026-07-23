'use client';

import { useEffect, useRef, useState } from 'react';
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
      {/* Rank — narrow and fixed, so a 2-digit number never steals width from the name. */}
      <col className="w-[1.75rem]" />
      <col />
      <col className="w-[4rem]" />
      <col className="w-[4.25rem]" />
      <col className="w-[4.5rem]" />
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

function Holdings({ rows }: { rows: Name[] }) {
  // Sortable — click a header to toggle direction. Default: weight, largest first. Each table sorts
  // on its OWN state (your names and the index's are independent lists).
  const [key, setKey] = useState<SortKey>('weight');
  const [dir, setDir] = useState<'asc' | 'desc'>('desc');
  if (!rows.length) return <p className="text-[11px] text-fg-faint py-1">Nothing held here.</p>;

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
    <table className="w-full text-[11px] table-fixed">
      <HoldingsCols />
      <thead>
        <tr className="text-fg-faint text-[10px] uppercase tracking-wide">
          {/* Not sortable: the rank IS the position under the ACTIVE sort, so clicking it could
              only mean "sort by the current sort". It renumbers whenever the sort changes. */}
          <th className="pr-1 text-right font-normal">#</th>
          <th className={`${th} pr-2 text-left`} onClick={() => click('name')}>Name{caret('name')}</th>
          <th className={`${th} px-1 text-right`} onClick={() => click('weight')}>Weight{caret('weight')}</th>
          <th className={`${th} px-1 text-right`} onClick={() => click('return')}>Return{caret('return')}</th>
          <th className={`${th} pl-1 text-right`} onClick={() => click('contrib')}>Contrib.{caret('contrib')}</th>
        </tr>
      </thead>
      <tbody>
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
            <td className="py-1 px-1 text-right font-mono text-fg">{(h.weight_pct ?? 0).toFixed(2)}%</td>
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
  // visible — so a bar click reveals the panel when it is off-screen, and stays put otherwise.
  useEffect(() => {
    ref.current?.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
  }, [axis, bucket]);

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
    <section ref={ref} className="bg-card border border-accent-500/30 rounded-xl p-4">
      <div className="flex items-start justify-between gap-3 mb-2">
        <h4 className="text-sm font-semibold text-fg-strong">
          {AXIS_LABEL[axis] ?? axis}: <span className="font-mono">{bucket}</span>
        </h4>
        <button onClick={onClose}
          className="text-[11px] px-2 py-1 rounded-lg border border-neutral-700 text-fg-muted hover:text-accent-300 shrink-0">
          ✕
        </button>
      </div>

      {loading && <p className="text-xs text-fg-subtle">Computing attribution…</p>}
      {error && (
        <div className="bg-neg-500/10 border border-neg-500/20 rounded-lg px-3 py-2 text-xs text-neg-300">{error}</div>
      )}

      {!loading && !error && attr && (
        <>
          {row && (
            <div className="text-[11px] text-fg-soft mb-3 flex flex-wrap items-center gap-x-4 gap-y-1">
              {attr?.start && (
                <span className="font-mono text-fg-muted">Since {attr.start}</span>
              )}
              <span title="Overweighting a bucket that beat the index (vs the whole index's return).">
                allocation <Num v={row.allocation_pct} pp />
              </span>
              <span title="Did your names in this bucket beat the index's names in it?">
                selection <Num v={row.selection_pct} pp />
              </span>
              <span title="The cross term (over/underweight × out/under-performance).">
                interaction <Num v={row.interaction_pct} pp />
              </span>
              <span className="font-semibold">total <Num v={row.total_pct} pp /></span>
            </div>
          )}

          {nonAttributable && (
            <p className="text-[11px] text-fg-faint mb-2">
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
                  <p className="text-[11px] font-medium text-fg-muted mb-1">
                    Your holdings <span className="text-fg-faint">({row.portfolio_holdings?.length ?? 0})</span>
                    {shared(row.portfolio_holdings) > 0 && (
                      <span className="text-accent-400"> · {shared(row.portfolio_holdings)} in both</span>
                    )}
                  </p>
                  <Holdings rows={row.portfolio_holdings ?? []} />
                </div>
                <div>
                  <p className="text-[11px] font-medium text-fg-muted mb-1">
                    {benchmark} constituents{' '}
                    <span className="text-fg-faint">({row.benchmark_holdings?.length ?? 0})</span>
                    {shared(row.benchmark_holdings) > 0 && (
                      <span className="text-accent-400"> · {shared(row.benchmark_holdings)} in both</span>
                    )}
                  </p>
                  <Holdings rows={row.benchmark_holdings ?? []} />
                </div>
              </div>
              {(row.portfolio_holdings ?? []).some((h) => h.in_both) && (
                <p className="text-[10px] text-fg-faint flex items-center gap-1.5 mt-3">
                  <span className="w-1.5 h-1.5 rounded-full bg-accent-500 inline-block shrink-0" />
                  marked rows are held in both your portfolio and {benchmark} (a share class counts as the same company)
                </p>
              )}
            </>
          ) : nonAttributable ? (
            <table className="w-full text-[11px]">
              <thead>
                <tr className="text-fg-faint text-[10px] uppercase tracking-wide">
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
            <p className="text-[11px] text-fg-faint">
              No holdings behind this bucket in the YTD window.
            </p>
          )}
        </>
      )}
    </section>
  );
}
