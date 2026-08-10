'use client';

import { useEffect, useMemo, useState } from 'react';
import type { CSSProperties } from 'react';
import { apiFetch } from '../../lib/apiFetch';
import { trace, traceEmpty, traceError } from '../../lib/debugTrace';
import { API_URL } from '../../lib/apiUrl';
import type { PortfolioCorrelationMatrix } from '../../lib/types/api';
import { sliceMatrix } from './correlationFilter';
import { VARIANT_FILTERS } from './portfolioVariants';
import type { Variant } from './portfolioVariants';

type Window = 'ytd' | 'trailing_12m';

const WINDOWS: { key: Window; label: string }[] = [
  { key: 'ytd', label: 'YTD' },
  { key: 'trailing_12m', label: 'Trailing 12m' },
];


const fmtCorr = (v: number | null | undefined) =>
  v == null ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(2)}`;

/** The "no number here" marker — a single calm black cross on white. ONE component, rendered
 * both in the grid's null cells and the legend swatch, so the key is exactly what's on the grid
 * and they cannot drift. Stroke is `fg-strong` (#11161d, near-black ink). */
function NullMark() {
  return (
    <svg viewBox="0 0 10 10" preserveAspectRatio="none" aria-hidden
      style={{ display: 'block', width: '100%', height: '100%' }}>
      <line x1="2.5" y1="2.5" x2="7.5" y2="7.5"
        stroke="var(--color-fg-strong)" strokeWidth="1" strokeLinecap="round" />
      <line x1="7.5" y1="2.5" x2="2.5" y2="7.5"
        stroke="var(--color-fg-strong)" strokeWidth="1" strokeLinecap="round" />
    </svg>
  );
}

/**
 * Diverging cell colour, CVD-SAFE by construction: blue (accent) for NEGATIVE correlation
 * (a diversifier) ↔ amber (warn) for POSITIVE (moves together), neutral at 0. Red↔green — the
 * intuitive choice — is exactly the pair that collapses under deuteranopia; the app's own
 * palette note measures blue+amber at ΔE 103 against that pair's 4.9, so we use it here.
 * Magnitude drives the tint alpha; the exact value lives in the hover readout.
 */
function cellStyle(v: number | null, isDiag: boolean): CSSProperties {
  const base: CSSProperties = { border: '1px solid var(--color-card)' };
  if (isDiag) return { ...base, background: 'var(--color-inset)' };
  if (v == null) {
    // No number to make — too few overlapping returns. A white cell carrying a single cross
    // (NullMark), read as "unknown", not as 0.
    return { ...base, background: 'var(--color-elevated)' };
  }
  const a = Math.min(1, Math.abs(v));
  const pct = Math.round(8 + a * 64); // 8% → 72% saturation
  const hue = v >= 0 ? 'var(--color-warn-500)' : 'var(--color-accent-500)';
  return { ...base, background: `color-mix(in srgb, ${hue} ${pct}%, transparent)` };
}

/** A label that bolds on hover WITHOUT changing width.
 *
 * ⚠ THE BUG THIS EXISTS FOR: A HOVER THAT RESIZES THE TABLE.
 *
 * Semibold text is wider than normal text. The row-header column is `sticky left-0` and
 * auto-width — a table column is as wide as its widest cell — so bolding the hovered label grew
 * the column and shifted the entire matrix sideways, under the cursor that caused it. It reads as
 * the chart twitching as you move across it.
 *
 * It could not happen while `max-w-[11rem] truncate` capped that column; removing the cap so names
 * show in FULL is exactly what exposed it. So the width is pinned to the BOLD measurement at all
 * times: an `invisible` copy — which stays in layout, unlike `hidden` — sets the box, and the real
 * label is painted over it. Hover then changes ink and nothing else.
 *
 * The cells (fixed 20×20) and the diagonal headers (fixed height, absolutely-positioned label) are
 * immune by construction. This column was the only one whose size followed its content.
 */
function BoldStable({ text, bold, className }: { text: string; bold?: boolean; className?: string }) {
  return (
    <span className="relative inline-block align-middle">
      <span aria-hidden className="invisible font-semibold">{text}</span>
      <span className={`absolute inset-0 text-right ${bold ? 'font-semibold' : ''} ${className ?? ''}`}>
        {text}
      </span>
    </span>
  );
}

/** Diagonal-header geometry.
 *
 * A label rotated 45° projects `len × cos(45°)` in BOTH axes — so the header must be that tall,
 * and the table must reserve that much room to its right or the last few labels are cut off by
 * the scroll container. Both come from the same number, which is why it is computed once.
 *
 * `_PX_PER_CHAR` is an ESTIMATE (Geist Sans at 10px) and deliberately generous: over-estimating
 * costs a little whitespace, under-estimating clips a name — and clipping is the thing we are
 * removing. It is only an estimate because measuring proportional text needs a canvas or a
 * layout pass, and neither is worth it to decide a header's height.
 */
const _PX_PER_CHAR = 5.8;
const _MIN_HEADER_PX = 48;
const _MAX_HEADER_PX = 340;

/** Base of the column-header stacking band — above the row headers (z-10) and the cells (auto),
 *  which is what the old flat `z-20` bought. */
const _Z_HEADER = 20;

/** ⚠ THE STACKING ORDER IS LOAD-BEARING, NOT COSMETIC.
 *
 * Every header cell is an OPAQUE `bg-card` box — it has to be, because body rows scroll under it.
 * But a 45° label ascends OUT of its own cell and across every header to its RIGHT. At a shared
 * z-index the paint order is DOM order, so each header's white background covers its LEFT
 * neighbour's label and the axis reads blank. (That shipped. It looked like a white overlay.)
 *
 * So headers rank DESCENDING left-to-right: label j out-ranks every box it crosses.
 */
export const headerZ = (j: number, n: number) => _Z_HEADER + n - j;

/** The sticky corner: column headers scroll left underneath it, so it out-ranks all of them. */
export const cornerZ = (n: number) => _Z_HEADER + n + 1;

/** The right-hand spacer: LAST in DOM order and opaque, so it must be the LOWEST header layer or
 *  it paints over the longest labels — the ones it exists to make room for. */
export const spacerZ = () => _Z_HEADER;

export function diagonalExtentPx(labels: string[]): number {
  const longest = labels.reduce((m, l) => Math.max(m, l.length), 0);
  const extent = Math.ceil(longest * _PX_PER_CHAR * Math.SQRT1_2) + 10;   // +10 breathing room
  // Clamped: a floor so a one-word filter still has a header, a ceiling so a pathological name
  // cannot push the matrix off the screen.
  return Math.min(Math.max(extent, _MIN_HEADER_PX), _MAX_HEADER_PX);
}

/**
 * Pairwise correlation of the LISTED (>5-holding) AIRS model portfolios' daily EUR returns —
 * the "42 of 95" the table above shows by default. Two windows (YTD, trailing 12m), each a
 * heatmap over the SAME return series the YTD column is read off, so the matrix cannot disagree
 * with the numbers it sits under. A cell is null (hatched) when the pair share fewer than
 * `min_overlap_days` common returns — a model defined last week has nothing to correlate yet.
 */
export default function CorrelationMatrix() {
  const [data, setData] = useState<PortfolioCorrelationMatrix | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [win, setWin] = useState<Window>('ytd');
  const [variant, setVariant] = useState<Variant>('all');
  const [hover, setHover] = useState<{ i: number; j: number } | null>(null);

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      try {
        const r = await apiFetch(`${API_URL}/api/airs/model-portfolios/correlations`);
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        const d = (await r.json()) as PortfolioCorrelationMatrix;
        // ⚠ THREE WAYS THIS PANEL COMES BACK EMPTY AND THEY NEED DIFFERENT ACTIONS. A correlation
        // needs two portfolios that BOTH have a daily return series over the same days, so a
        // fresh database yields no ids, a partly-populated one yields ids whose cells are all
        // null, and a healthy one yields a matrix. On screen the first two look identical — a
        // grey grid — so the console has to separate them.
        const n = d.portfolio_ids?.length ?? 0;
        if (!n) {
          traceEmpty('correlations', 'no portfolios in the matrix',
            'no model portfolio has a priceable daily return series yet — the matrix needs '
            + 'holdings with prices on both sides of a pair. Scan the model portfolios and '
            + 'refresh their instruments. Not an error.');
        } else {
          const cells = (d.ytd ?? []).flat();
          const filled = cells.filter((c) => c != null).length;
          trace('correlations', `${n}×${n} matrix, ${filled} of ${cells.length} YTD cells computed`
            + ` (min overlap ${d.min_overlap_days ?? '?'} days)`);
          if (n > 1 && filled <= n) {
            // Only the diagonal came back: every PAIR failed the overlap test.
            traceEmpty('correlations', 'every off-diagonal cell is null',
              `no two portfolios share ${d.min_overlap_days ?? 'the minimum'} days of returns. `
              + 'Their price series are too short or do not overlap — the grid will render '
              + 'blank even though the portfolios themselves are fine.');
          }
        }
        if (!cancelled) setData(d);
      } catch (e) {
        traceError('correlations', 'the correlation matrix could not be loaded', e);
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => { cancelled = true; };
  }, []);

  // ── the risk-profile filter ──────────────────────────────────────────────────────────────
  // Holding the risk level constant is what makes the remaining rows comparable: "how correlated
  // are my Neutraal products with each other" is a question about the product LINE, and it cannot
  // be asked while the matrix also varies by risk.
  //
  // Every profile is offered, always — a profile with no models in the current data yields an
  // empty matrix that SAYS so. Hiding the option instead would answer "does Pensioen have a
  // Beperkt Offensief?" by silently omitting the question.
  //
  // `keep` is the index list every axis is projected through — see `sliceMatrix`, which is where
  // the both-axes rule lives and is tested. (`data.variants` is read inside the callback: as a
  // `?? []` fallback it would be a fresh array each render and defeat the memo entirely.)
  const keep = useMemo(
    () => (data?.labels ?? []).map((_, i) => i)
      .filter((i) => variant === 'all' || (data?.variants ?? [])[i] === variant),
    [data, variant],
  );

  // All FILTERED views, projected through `keep`. The label is the chosen name where there is one,
  // else AIRS's code — the backend resolved that (`portfolio_label`), because an axis has one slot
  // and cannot render the "—" the table above uses for an unnamed model.
  const labels = useMemo(() => keep.map((i) => (data?.labels ?? [])[i] ?? ''), [keep, data]);
  const codes = useMemo(() => keep.map((i) => (data?.codes ?? [])[i] ?? ''), [keep, data]);
  const matrix = useMemo(
    () => sliceMatrix<number>(data ? data[win] : [], keep),
    [data, win, keep],
  );
  const obs = useMemo(() => {
    const o = data ? (win === 'ytd' ? data.ytd_obs : data.trailing_12m_obs) : [];
    return keep.map((i) => o[i] ?? 0);
  }, [data, win, keep]);

  // The header's height AND the room reserved to the right of the table — one number, because
  // they are the same projection of the same longest label.
  const diag = useMemo(() => diagonalExtentPx(labels), [labels]);

  /** Tooltip: the readable name plus AIRS's own code when they differ. The axis shows the name in
   *  full now, so this is only about the code — the identifier you would search AIRS with, which
   *  a chosen name hides. */
  const tip = (i: number) => {
    const l = labels[i] ?? '';
    const c = codes[i] ?? '';
    return c && c !== l ? `${l} · ${c}` : l;
  };

  const readout = data && hover
    ? {
      a: labels[hover.i], b: labels[hover.j],
      v: matrix[hover.i]?.[hover.j] ?? null,
      overlap: Math.min(obs[hover.i] ?? 0, obs[hover.j] ?? 0),
      self: hover.i === hover.j,
    }
    : null;

  return (
    // ⚠ `isolate` — THIS TABLE'S STACKING ORDER MUST NOT ESCAPE IT. `headerZ`/`cornerZ` climb
    // with the COLUMN COUNT (`20 + n - j`, corner `20 + n + 1`), so at ~56 portfolios the sticky
    // headers reach z≈77 and painted straight over the Analyse modal's `z-50` — the modal opened
    // *underneath* the correlation grid. Raising the modal would only move the collision to
    // whatever portfolio count crosses the new number next; `isolation: isolate` creates a
    // stacking context so every z-index in here is compared only against its siblings, and the
    // table can rank its own headers however it likes without competing with the page.
    <section className="isolate bg-card border border-neutral-800/40 rounded-xl p-5 space-y-3">
      <div className="flex items-start justify-between gap-3 flex-wrap">
        <div>
          <h3 className="text-sm font-semibold text-fg-strong">Portfolio correlations</h3>
          <p className="text-[12px] text-fg-faint mt-0.5"
            title="Computed from the same daily EUR return series the portfolios' YTD is read off.">
            Pairwise correlation of daily EUR returns
            {data ? ` · ${labels.length}` : ''}. Blue = diverging, amber = moving together.
          </p>
        </div>
        <div className="flex flex-col items-end gap-2">
        <div className="flex rounded-lg border border-neutral-800/40 overflow-hidden text-[12px]"
          title="Filter to ONE risk profile so the rows are comparable — a correlation between an Offensief and a Defensief model mostly measures the risk gap, not the strategies. Read off AIRS's own name, so renaming a model cannot change its profile. The 8 models not offered at a profile (the themed TopSelectie and WTS funds, Risicodragend/Risicomijdend) appear only under All.">
          {VARIANT_FILTERS.map((v) => (
            <button key={v.key} onClick={() => { setVariant(v.key); setHover(null); }}
              className={`px-2.5 py-1 transition-colors whitespace-nowrap ${
                variant === v.key ? 'bg-accent-600 text-white' : 'text-fg-soft hover:bg-overlay/5'
              }`}>
              {v.label}
            </button>
          ))}
        </div>
        <div className="flex rounded-lg border border-neutral-800/40 overflow-hidden text-[12px]">
          {WINDOWS.map((w) => (
            <button key={w.key} onClick={() => { setWin(w.key); setHover(null); }}
              className={`px-3 py-1 transition-colors ${
                win === w.key ? 'bg-accent-600 text-white' : 'text-fg-soft hover:bg-overlay/5'
              }`}>
              {w.label}
            </button>
          ))}
        </div>
        </div>
      </div>

      {loading && <p className="text-xs text-fg-subtle">Computing…</p>}
      {error && (
        <div className="bg-neg-500/10 border border-neg-500/20 rounded-lg px-3 py-2 text-xs text-neg-300">{error}</div>
      )}

      {/* A profile with no models is an ANSWER about the product range, not an error — and it is
          a different sentence from "the matrix failed to load". Saying it beats rendering an
          empty grid, and beats hiding the button (which would answer the question by making it
          unaskable). */}
      {!loading && !error && data && labels.length === 0 && (
        <p className="text-xs text-fg-subtle">
          No model portfolio is offered at <span className="text-fg">{variant}</span>.{' '}
          <button type="button" onClick={() => setVariant('all')}
            className="text-accent-400 hover:underline">Show all {data.labels.length}</button>.
        </p>
      )}

      {!loading && !error && data && labels.length > 0 && (
        <>
          {/* Hover readout — one pair at a time, so cells stay color-only at this density. */}
          <div className="text-[12px] min-h-[1.5rem] flex items-center gap-2">
            {readout ? (
              readout.self ? (
                <span className="text-fg-soft"><span className="font-medium text-fg">{readout.a}</span> — self</span>
              ) : (
                <span className="text-fg-soft">
                  <span className="font-medium text-fg">{readout.a}</span>
                  <span className="text-fg-faint"> ↔ </span>
                  <span className="font-medium text-fg">{readout.b}</span>
                  <span className="text-fg-faint"> · {WINDOWS.find((w) => w.key === win)!.label} corr </span>
                  <span className={`font-mono font-semibold ${
                    readout.v == null ? 'text-fg-faint'
                      : readout.v >= 0 ? 'text-warn-400' : 'text-accent-400'
                  }`}>{fmtCorr(readout.v)}</span>
                  {readout.v == null
                    ? <span className="text-fg-faint"> (&lt; {data.min_overlap_days}d overlap)</span>
                    : <span className="text-fg-faint"> · {readout.overlap} overlapping days</span>}
                </span>
              )
            ) : (
              <span className="text-fg-faint">Hover a cell to read the pair. As of {data.as_of}.</span>
            )}
          </div>

          {/* Legend */}
          <div className="flex items-center gap-x-4 gap-y-2 text-[11px] text-fg-faint flex-wrap">
            <div className="inline-flex items-center gap-2">
              <span className="text-accent-400">−1 perfect negative correlation</span>
              <div className="flex flex-col items-stretch gap-0.5">
                <div className="h-2.5 w-44 rounded border border-neutral-800/30"
                  style={{ background: 'linear-gradient(to right, var(--color-accent-500), var(--color-inset), var(--color-warn-500))' }} />
                <span className="text-center leading-none">0 uncorrelated</span>
              </div>
              <span className="text-warn-400">perfect positive correlation +1</span>
            </div>
            <span className="inline-flex items-center gap-1.5 ml-auto">
              <span className="inline-block w-5 h-5 rounded-sm border border-neutral-700 overflow-hidden"
                style={{ background: 'var(--color-elevated)' }}>
                <NullMark />
              </span>
              too few overlapping days
            </span>
          </div>

          {/* NO inner scroll: the matrix shows whole and the PAGE scrolls, rather than a chart
              inside a chart-sized window. `max-h-[75vh]` is what forced that; it is gone.
              ⚠ `overflow-auto` STAYS, and is not the same thing. It is the narrow-screen safety
              the design system mandates ("keep new dense tables wrapped in an overflow-auto
              container") — without it a 1,240px matrix stretches the whole page sideways on a
              laptop. With no height cap it never overflows vertically, so no scrollbar appears
              when there is room; it only engages when the viewport genuinely cannot fit the
              width. It also keeps the sticky row headers bound to THIS container rather than to
              the page, which is what stops them floating over the app while you scroll. */}
          <div className="overflow-auto rounded-lg border border-neutral-800/40">
            {/* Centred via `mx-auto`, NOT flex `justify-center`. In an overflow container a
                centred flex child clips its own leading edge once it outgrows the box — auto
                margins just resolve to 0, so a too-wide matrix falls back to left-aligned and
                scrollable instead of losing its first column. */}
            <table className="border-separate text-[11px] mx-auto" style={{ borderSpacing: 0 }}>
              <thead>
                <tr>
                  {/* Above every diagonal label: the column headers scroll left UNDER this
                      corner, so it has to out-rank all of them. */}
                  <th style={{ zIndex: cornerZ(labels.length) }}
                    className="sticky left-0 top-0 bg-card" />
                  {labels.map((l, j) => (
                    // 45°, ascending to the right, anchored at the column it names. Rotated
                    // content does not affect layout, so the height is set explicitly (`diag`)
                    // and `overflow` stays visible — a clipping header is what we just removed.
                    //
                    // ⚠ DESCENDING z-index, AND IT IS LOAD-BEARING. Each header is an OPAQUE
                    // `bg-card` box (it must be: body rows scroll under it), but a 45° label
                    // ascends OUT of its own box and across every header to its right. At equal
                    // z-index the paint order is DOM order — so each header's white background
                    // covers its left neighbour's label, and the whole axis reads as blank.
                    // Ranking them right-to-left puts every label above the boxes it crosses.
                    <th key={j} title={tip(j)}
                      style={{ height: diag, zIndex: headerZ(j, labels.length) }}
                      className={`sticky top-0 bg-card align-bottom p-0 font-normal ${
                        hover?.j === j ? 'text-accent-400 font-semibold' : 'text-fg-muted'
                      }`}>
                      <div className="relative h-full w-full">
                        <div className="absolute bottom-1 left-1/2 whitespace-nowrap"
                          style={{ transform: 'rotate(-45deg)', transformOrigin: 'left bottom' }}>
                          {l}
                        </div>
                      </div>
                    </th>
                  ))}
                  {/* The last labels ascend PAST the right edge — a real spacer column reserves
                      the room. Padding on the table would be the obvious fix and is the unreliable
                      one: whether it extends an overflow container's scrollWidth is a browser
                      quirk, and getting it wrong silently clips the very names this change is
                      about. A column cannot be argued with.

                      ⚠ It is the LOWEST header layer. It is last in DOM order and opaque, so at a
                      shared z-index it painted over every label that reaches into it — i.e. the
                      longest ones, the whole reason it exists. */}
                  <th aria-hidden style={{ width: diag, minWidth: diag, zIndex: spacerZ() }}
                    className="sticky top-0 bg-card p-0" />
                </tr>
              </thead>
              <tbody>
                {labels.map((l, i) => (
                  <tr key={i}>
                    {/* Full name, no clip. The column is sticky, so a long name costs horizontal
                        room in the scroll area but never stops you reading which row you are on.
                        ⚠ THE BOLD WIDTH IS RESERVED, ALWAYS — see `<BoldStable>`. */}
                    <th title={tip(i)}
                      className="sticky left-0 z-10 bg-card text-right pr-2 pl-1 whitespace-nowrap font-normal">
                      <BoldStable text={l} bold={hover?.i === i}
                        className={hover?.i === i ? 'text-accent-400' : 'text-fg-muted'} />
                    </th>
                    {matrix[i].map((v, j) => (
                      <td key={j}
                        onMouseEnter={() => setHover({ i, j })}
                        onMouseLeave={() => setHover(null)}
                        style={{ ...cellStyle(v, i === j), width: 20, height: 20, minWidth: 20 }}
                        className="cursor-default p-0">
                        {i !== j && v == null && <NullMark />}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <p className="text-[11px] text-fg-faint leading-relaxed">
            Pearson correlation of daily EUR returns, pairwise-complete: each pair uses only the
            trading days both portfolios have a return, so different inception dates shorten the
            overlap rather than inject zeros. A cell needs at least {data.min_overlap_days} common
            returns or it is left blank. The matrix is symmetric — a pair reads the same either
            way; the diagonal is a portfolio with itself.
          </p>
        </>
      )}
    </section>
  );
}
