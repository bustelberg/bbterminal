'use client';

import { Fragment, useState } from 'react';
import { DISPLAY_EPSILON } from './composition';
import type { ModelPortfolioAnalysis } from '../../../lib/types/api';

type Axis = NonNullable<ModelPortfolioAnalysis['axes']>[number];
type Row = Axis['rows'][number];
type Holding = NonNullable<Row['holdings']>[number];
type Excluded = NonNullable<Axis['excluded']>[number];

/** Why a holding has no bucket, in the reader's words rather than the ladder's.
 *
 * ⚠ `unclassified` COVERS A FUND, A BOND AND A GENUINE BLANK, AND THEY DO NOT READ ALIKE. Labelled
 * with the ladder's own word, an ETF appeared as "no classification on this axis", which sounds
 * like our data failed — when the truth is that a world tracker has no sector and never did. The
 * Class we already store says which it is, so it is used. */
function reasonLabel(e: Excluded, axisWord: string): string {
  if (e.reason === 'cash') return 'cash — has no ' + axisWord;
  if (e.reason === 'unpriced') return '⚠ held, but we have no price series for it';
  return e.asset_class ? `${e.asset_class} — has no ${axisWord}` : `no ${axisWord}`;
}

const AXIS_LABEL: Record<string, string> = {
  sector: 'Sector', region: 'Region', currency: 'Currency',
};

/**
 * The rows behind the Sector / Region / Currency bars — every holding, at the weight that bar
 * counted it at.
 *
 * ⚠ IT EXISTS BECAUSE TWO CORRECT NUMBERS LOOKED LIKE A CONTRADICTION. Bustelberg Offensief reads
 * Technology 36% on this chart and 39.1% as "our weight" in the Brinson attribution table. Neither
 * is wrong; they are shares of DIFFERENT DENOMINATORS — attribution drops funds, cash and anything
 * it could not price, renormalises what remains to 100%, and weights each position by its value
 * when the window OPENED, while this chart divides by the whole equity sleeve at today's value.
 * Until now attribution shipped its own per-bucket holdings and this chart shipped aggregates
 * only, so a reader could check the number that looked wrong and not the one it disagreed with.
 * An un-inspectable figure beside an inspectable one is where trust goes.
 *
 * ⚠ THE ROWS ARRIVE ALREADY DIVIDED (`_axis_holdings`), NOT RE-WEIGHTED HERE. A drill-down that
 * recomputes its own denominator can land near its bar instead of on it, which turns one
 * unexplained number into two. Σ over a bucket IS `portfolio_pct`, and both figures are printed
 * side by side so the identity is visible rather than promised.
 *
 * ⚠ FULL PRECISION, AND NOTHING IS FILTERED OUT — deliberately unlike the chart. `formatPct`
 * rounds to whole percent and blanks anything under 0.5%, and `visibleBuckets` drops a bucket that
 * is tiny on both sides; both are right for a bar label and wrong here, where a reader is adding
 * the column up. A row the chart does not draw is shown and MARKED, because "the bars sum to 99.7%"
 * is a discrepancy we created and the only place it can be explained is this table.
 *
 * ⚠ PORTFOLIO SIDE ONLY. The benchmark bar is a share of 493 (SP500) to 1,998 (ACWI) constituents;
 * shipping all of them on all three axes would be thousands of rows nobody opened this for. They
 * are already one click away per bucket on the bar-click panel — the footer says so.
 */
/** One list of holdings that carry no bucket on this axis. `tone` is the whole point: the amber
 *  one is a hole in the chart, the calm one is an answer. */
function ExcludedTable({ rows, title, note, axisWord, p2, tone }: {
  rows: Excluded[];
  title: string;
  note: string;
  axisWord: string;
  p2: (v: number | null | undefined) => string;
  tone: 'warn' | 'calm';
}) {
  return (
    <div className="space-y-1">
      <h3 className={`text-[11px] font-medium ${tone === 'warn' ? 'text-warn-300' : 'text-fg-strong'}`}>
        {title}
      </h3>
      <p className="text-[11px] text-fg-faint">{note}</p>
      <div className={`overflow-auto rounded-lg border ${
        tone === 'warn' ? 'border-warn-500/25' : 'border-neutral-800/40'}`}>
        <table className="w-full text-xs">
          <thead className="bg-page">
            <tr className="text-fg-faint text-[10px] uppercase tracking-wide border-b border-neutral-800/40">
              <th className="px-3 py-1.5 font-medium text-left">Holding</th>
              <th className="px-3 py-1.5 font-medium text-left">ISIN</th>
              <th className="px-3 py-1.5 font-medium text-right">Weight</th>
              <th className="px-3 py-1.5 font-medium text-left">Why</th>
            </tr>
          </thead>
          <tbody>
            {[...rows].sort((a, b) => (b.weight_pct ?? 0) - (a.weight_pct ?? 0)).map((e, i) => (
              <tr key={e.isin ?? `${e.name}-${i}`}
                className="border-t border-neutral-800/[0.15] hover:bg-overlay/[0.02]">
                <td className="px-3 py-1 text-fg-soft truncate max-w-0" title={e.name ?? undefined}>
                  {e.name ?? '—'}
                </td>
                <td className="px-3 py-1 font-mono text-[10px] text-fg-faint">{e.isin ?? '—'}</td>
                <td className="px-3 py-1 text-right font-mono text-fg">{p2(e.weight_pct)}</td>
                <td className={`px-3 py-1 text-[10px] ${
                  tone === 'warn' ? 'text-warn-300' : 'text-fg-muted'}`}>
                  {reasonLabel(e, axisWord)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export default function CompositionDataModal({
  axis, rows, basis, positions, attributablePct, unpricedPct, excluded = [], benchmark, name,
  onClose,
}: {
  axis: string;
  rows: Row[];
  /** The denominator, in words — computed server-side, because it differs PER AXIS. */
  basis?: string | null;
  positions?: number | null;
  /** How much of the book has a bucket here. Null on the fallback basis (nothing excluded). */
  attributablePct?: number | null;
  /** The part of the remainder that is a genuine hole rather than an answer. */
  unpricedPct?: number | null;
  /** The holdings this basis cannot weigh, and why — shown, never inferred from a total. */
  excluded?: Excluded[];
  benchmark: string;
  name: string;
  onClose: () => void;
}) {
  // Collapsed by default: eleven sectors expanded at once is a wall, and the subtotal line is the
  // part that answers "does this add up".
  const [open, setOpen] = useState<Set<string>>(new Set());
  const toggle = (b: string) => setOpen((prev) => {
    const next = new Set(prev);
    if (next.has(b)) next.delete(b); else next.add(b);
    return next;
  });

  // The chart's ordering, so row n here is bar n there — but NOT the chart's filter.
  const sorted = [...rows].sort((a, b) =>
    (b.portfolio_pct ?? 0) - (a.portfolio_pct ?? 0) || (b.benchmark_pct ?? 0) - (a.benchmark_pct ?? 0));
  const total = sorted.reduce((s, r) => s + (r.portfolio_pct ?? 0), 0);
  const legs = sorted.reduce((s, r) => s + (r.holdings?.length ?? 0), 0);
  const undrawn = sorted.filter((r) => (r.portfolio_pct ?? 0) < DISPLAY_EPSILON
    && (r.benchmark_pct ?? 0) < DISPLAY_EPSILON).length;

  const p2 = (v: number | null | undefined) => `${(v ?? 0).toFixed(2)}%`;
  const sum = (hs: Holding[]) => hs.reduce((s, h) => s + (h.weight_pct ?? 0), 0);
  const unpriced = excluded.filter((e) => e.reason === 'unpriced');
  const notApplicable = excluded.filter((e) => e.reason !== 'unpriced');
  const naWeight = notApplicable.reduce((s, e) => s + (e.weight_pct ?? 0), 0);
  const axisWord = (AXIS_LABEL[axis] ?? axis).toLowerCase();

  return (
    <div className="fixed inset-0 z-[70] flex items-center justify-center bg-scrim/60 p-4"
      onClick={onClose} role="presentation">
      <div className="bg-card rounded-xl border border-neutral-800/40 shadow-xl w-[80vw] h-[82vh] flex flex-col"
        onClick={(e) => e.stopPropagation()} role="dialog" aria-modal="true">
        <div className="flex items-baseline gap-3 px-6 py-4 border-b border-neutral-800/40">
          <h2 className="text-fg-strong font-medium">
            {AXIS_LABEL[axis] ?? axis} — the rows behind the bars
          </h2>
          <span className="text-sm text-fg-soft truncate max-w-[28ch]" title={name}>{name}</span>
          <button type="button" onClick={onClose}
            className="ml-auto text-fg-muted hover:text-fg-strong px-2">✕</button>
        </div>

        <div className="flex-1 overflow-auto px-6 py-4 space-y-3">
          <p className="text-[11px] text-fg-faint">
            {basis}{positions ? ` ${positions} positions in that total.` : ''}
            {' '}Each holding is listed at the weight this chart counted it at, so a bucket&apos;s rows
            add up to its bar exactly — both figures are printed below. Full precision, nothing
            filtered.
          </p>

          {/* ⚠ WHAT THIS BASIS CANNOT SHOW, LEADING RATHER THAN FOOTNOTED. The bars now match the
              Attribution table exactly, which was the point — but the price of that is real
              weight that has nowhere to go: a mid-window purchase has no Beginwaarde, an
              unpriceable holding has no return. Both vanish from the chart. Discovering that from
              a total that does not feel right is how a reader stops trusting the panel. */}
          {(unpricedPct ?? 0) > 0.005 && (
            <div className="rounded-lg border border-warn-500/25 bg-warn-500/[0.07] px-3 py-2 text-[11px] text-fg-soft">
              <span className="text-warn-300 font-medium">
                ⚠ {unpricedPct!.toFixed(2)}% of the book is held but unpriceable.
              </span>
              {' '}Those are real positions with a real {axisWord}, absent from the bars because we
              have no price series for them — so the buckets they belong to read lower than they
              are. Named at the bottom.
            </div>
          )}
          {attributablePct != null && attributablePct < 99.95 && (
            <p className="text-[11px] text-fg-faint">
              These bars cover the {attributablePct.toFixed(1)}% of the book that has a {axisWord}.
              The remaining <span className="font-mono">{naWeight.toFixed(2)}%</span> is funds,
              bonds and cash — not Stocks, and nothing with a {axisWord} to place. A position bought
              after the window opened has no start value and carries no weight here either, so it is
              on neither list.
            </p>
          )}

          <div className="overflow-auto rounded-lg border border-neutral-800/40">
            <table className="w-full text-xs">
              <thead className="bg-page sticky top-0 z-10">
                <tr className="text-fg-faint text-[10px] uppercase tracking-wide border-b border-neutral-800/40">
                  <th className="px-3 py-1.5 font-medium text-left">{AXIS_LABEL[axis] ?? axis} / holding</th>
                  <th className="px-3 py-1.5 font-medium text-left">ISIN</th>
                  <th className="px-3 py-1.5 font-medium text-left">Class</th>
                  <th className="px-3 py-1.5 font-medium text-right"
                    title="Sum of the holdings listed under this bucket. These are the same weights the Attribution table shows — each position's value when the window opened, over the attributable holdings.">
                    Σ rows
                  </th>
                  <th className="px-3 py-1.5 font-medium text-right"
                    title="The bar the chart draws for this bucket.">Bar</th>
                  <th className="px-3 py-1.5 font-medium text-right">{benchmark}</th>
                </tr>
              </thead>
              <tbody>
                {sorted.map((r) => {
                  const hs = r.holdings ?? [];
                  const isOpen = open.has(r.bucket);
                  const subtotal = sum(hs);
                  const bar = r.portfolio_pct ?? 0;
                  // A float-level gap is expected; anything visible at two decimals is a real
                  // defect and must not hide behind two independently-rounded numbers.
                  const mismatch = hs.length > 0 && Math.abs(subtotal - bar) > 0.005;
                  const notDrawn = bar < DISPLAY_EPSILON && (r.benchmark_pct ?? 0) < DISPLAY_EPSILON;
                  return (
                    <Fragment key={r.bucket}>
                      <tr className="border-t border-neutral-800/40 bg-inset/60 hover:bg-overlay/[0.03]">
                        <td className="px-3 py-1.5" colSpan={3}>
                          <button type="button" onClick={() => toggle(r.bucket)}
                            className="flex items-center gap-2 text-left text-fg-strong font-medium">
                            <span className="text-fg-faint w-3 inline-block">{hs.length ? (isOpen ? '▾' : '▸') : ''}</span>
                            {r.bucket}
                            <span className="text-fg-faint font-normal">
                              {hs.length === 0
                                // Not "no data" — a bucket the benchmark holds and we do not. The
                                // empty list IS the finding.
                                ? '· not held'
                                : `· ${hs.length} holding${hs.length === 1 ? '' : 's'}`}
                            </span>
                            {notDrawn && (
                              <span className="text-[10px] text-warn-300 font-normal"
                                title="Below the chart's display threshold — this row is in the totals but not drawn as a bar.">
                                not drawn
                              </span>
                            )}
                          </button>
                        </td>
                        <td className={`px-3 py-1.5 text-right font-mono ${mismatch ? 'text-neg-300' : 'text-fg-soft'}`}
                          title={mismatch
                            ? 'The rows do not sum to the bar — that is a bug, please report it.'
                            : 'Sum of the holdings under this bucket.'}>
                          {hs.length ? p2(subtotal) : '—'}
                        </td>
                        <td className="px-3 py-1.5 text-right font-mono font-medium text-fg-strong">{p2(bar)}</td>
                        <td className="px-3 py-1.5 text-right font-mono text-fg-muted">{p2(r.benchmark_pct)}</td>
                      </tr>
                      {isOpen && hs.map((h, i) => (
                        <tr key={h.isin ?? `${r.bucket}-${i}`}
                          className="border-t border-neutral-800/[0.15] hover:bg-overlay/[0.02]">
                          <td className="px-3 py-1 pl-9 text-fg-soft truncate max-w-0" title={h.name ?? undefined}>
                            {h.name ?? '—'}
                            {(h.via_names?.length ?? 0) > 0 && (
                              // A looked-through leg is not a line AIRS stores — naming the
                              // certificate is the only way to find the row back in the book.
                              <span className="ml-1.5 text-[10px] text-accent-400"
                                title={`Reached through ${(h.via_names ?? []).join(' · ')}`}>
                                via {h.via_names?.[0]}
                                {(h.via_names?.length ?? 0) > 1 ? ` +${(h.via_names?.length ?? 0) - 1}` : ''}
                              </span>
                            )}
                          </td>
                          <td className="px-3 py-1 font-mono text-[10px] text-fg-faint">{h.isin ?? '—'}</td>
                          <td className="px-3 py-1 text-[10px] text-fg-muted">{h.asset_class ?? '—'}</td>
                          <td className="px-3 py-1 text-right font-mono text-fg">{p2(h.weight_pct)}</td>
                          <td /><td />
                        </tr>
                      ))}
                    </Fragment>
                  );
                })}
              </tbody>
              <tfoot>
                <tr className="border-t-2 border-neutral-800/40 bg-page">
                  <td className="px-3 py-1.5 font-medium text-fg-strong" colSpan={3}>
                    Total · {legs} holdings
                    {undrawn > 0 && (
                      <span className="ml-2 text-[10px] font-normal text-fg-faint">
                        ({undrawn} bucket{undrawn === 1 ? '' : 's'} below the chart&apos;s threshold, included here)
                      </span>
                    )}
                  </td>
                  <td />
                  <td className="px-3 py-1.5 text-right font-mono font-medium text-fg-strong">{total.toFixed(2)}%</td>
                  <td className="px-3 py-1.5 text-right font-mono text-fg-muted">—</td>
                </tr>
              </tfoot>
            </table>
          </div>

          {/* ⚠ TWO LISTS, NOT ONE, BECAUSE THEY ARE DIFFERENT FACTS. Merged under "cannot be
              weighed" an ETF read as a failure of ours — but a fund, a bond and a cash line have
              no sector by definition, are not Stocks in our own classification, and have their own
              slice of the allocation chart. Only the unpriced list is a hole in this chart. */}
          {unpriced.length > 0 && (
            <ExcludedTable rows={unpriced} axisWord={axisWord} p2={p2} tone="warn"
              title={`⚠ Held, but missing from the bars — ${unpriced.length} holding${
                unpriced.length === 1 ? '' : 's'}`}
              note={`These are real positions in real ${axisWord}s that we have no price series for.
                They are absent from the bars above, so the buckets they belong to read LOWER than
                they are — discount those rows accordingly.`} />
          )}

          {notApplicable.length > 0 && (
            <ExcludedTable rows={notApplicable} axisWord={axisWord} p2={p2} tone="calm"
              title={`No ${axisWord} — ${notApplicable.length} holding${
                notApplicable.length === 1 ? '' : 's'}, ${naWeight.toFixed(2)}% of the book`}
              note={`Not a gap: a fund, a bond and a cash line have no ${axisWord}, and they are
                not Stocks in our classification either. They are their own slices of the
                allocation chart above.`} />
          )}

          <p className="text-[11px] text-fg-faint">
            The {benchmark} column is the bar only. Its constituents are per-bucket and there are
            hundreds of them, so they stay one click away: close this and click a bar to open that
            bucket&apos;s holdings on both sides — the weights there are these weights.
          </p>
        </div>
      </div>
    </div>
  );
}
