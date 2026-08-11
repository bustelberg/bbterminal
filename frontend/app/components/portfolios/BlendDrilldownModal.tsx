'use client';

import { useEffect, useMemo, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { AspectCard } from '../../../lib/tipCard';
import InfoTip from '../InfoTip';

/** The holdings behind ONE point of a blended portfolio chart — and the ones missing from it.
 *
 * ⚠ IT DOES NOT COMPUTE ANYTHING. Every figure here is read from
 * `POST /api/earnings/fundamental-blend-breakdown`, which decomposes the point through the SAME
 * `_prepare` the chart's line is built from. Re-deriving the members here "the same way" would be
 * a second copy of the harmonic/ratio/level rules, and a panel that quietly disagrees with the
 * chart above it is worse than no panel — it gets checked once and trusted from then on.
 */

type Member = {
  isin: string | null;
  name: string | null;
  weight_pct: number;
  value: number;
  raw_value: number | null;
  share_pct: number | null;
  swing: number | null;
};

type Excluded = { isin: string | null; name: string | null; weight_pct: number; reason: string };

type Breakdown = {
  kind: 'multiple' | 'ratio' | 'level';
  metric_code: string;
  period: string;
  value: number | null;
  covered_pct: number;
  excluded_pct: number;
  members: Member[];
  excluded: Excluded[];
};

type Cell = { value: number; raw: number | null; dropped: boolean };
type MatrixMember = {
  isin: string | null; name: string | null; weight_pct: number;
  cells: Record<string, Cell>;
};
type Matrix = {
  kind: 'multiple' | 'ratio' | 'level';
  metric_code: string;
  periods: string[];
  members: MatrixMember[];
  blended: Record<string, number | null>;
  covered: Record<string, number>;
  below_floor: Record<string, boolean>;
  excluded: Excluded[];
};

type Tab = 'period' | 'matrix';
type SortKey = 'holding' | 'weight' | 'value' | 'raw' | 'share' | 'inverse' | 'contribution';
type Sort = { key: SortKey; dir: 'asc' | 'desc' };

/** The earnings yield (1 ÷ value) — the quantity a multiple actually blends in. Null for a
 *  non-positive value (a loss has no meaningful reciprocal). */
const inverseOf = (m: Member) => (m.value && m.value > 0 ? 1 / m.value : null);
/** What this holding contributes to the portfolio's aggregate yield: weight (as a fraction) ×
 *  (1 ÷ value). Σ of this over the holdings = portfolio yield; blended = Σweight ÷ Σcontribution. */
const contribOf = (m: Member) => {
  const iv = inverseOf(m);
  return iv == null ? null : (m.weight_pct / 100) * iv;
};

/** Compare two cell values, nulls ALWAYS last (in both directions — an absent figure is not a
 *  small number), strings case-insensitively, numbers numerically. */
function cmp(a: number | string | null | undefined, b: number | string | null | undefined,
  dir: 'asc' | 'desc'): number {
  if (a == null && b == null) return 0;
  if (a == null) return 1;
  if (b == null) return -1;
  const r = (typeof a === 'string' || typeof b === 'string')
    ? String(a).localeCompare(String(b))
    : (a as number) - (b as number);
  return dir === 'desc' ? -r : r;
}

/** ⚠ Each reason is a DIFFERENT fact and they must not read alike. "Has not reported yet" and
 *  "reported a loss, so the multiple was dropped" produce the same blank in a chart and mean
 *  opposite things about the holding. */
const REASON: Record<string, string> = {
  no_point_in_period: 'no figure for this period',
  non_positive_multiple: 'loss-making — a negative multiple has no reciprocal',
  non_positive_base: 'no positive base year, so it cannot be rebased',
  no_data: 'no data for this metric',
  no_weight: 'zero weight in the model',
};

/** The audit-grid footer, explained per kind — how the "Blended" row is actually computed. */
const MATRIX_NOTE: Record<Matrix['kind'], string> = {
  multiple: 'Blended = Σweight ÷ Σ(weight ÷ value) over the holdings with a positive value — the '
    + 'weighted harmonic mean, i.e. what the basket actually costs. Loss-making cells (struck '
    + 'through) have no reciprocal and are dropped.',
  ratio: 'Blended = Σ(weight × value) ÷ Σweight over the holdings that reported — the weighted '
    + 'mean, renormalised each period over the weight present.',
  level: 'Each holding is rebased to 100 at its own first year, so a cell shows that index (the '
    + 'amount as reported is in its tooltip); Blended is the weighted mean of the indices.',
};

/** How the components relate to the line — different per kind, and saying so is the point. */
const SHARE_NOTE: Record<Breakdown['kind'], string> = {
  multiple: "A portfolio's P/E is total price ÷ total earnings, so it blends HARMONICALLY. Each "
    + 'holding\'s share = (weight ÷ its P/E) ÷ Σ(weight ÷ P/E) over the holdings reporting here — '
    + 'its slice of the aggregate earnings YIELD (1/PE). Shares sum to 100%, and a cheap, '
    + 'heavily-weighted name carries the largest share, an expensive one the least, even at equal '
    + 'weight (e.g. 6.2% @ 16.2x contributes 6.2 ÷ 16.2 = 0.38; 9.7% @ 34.8x only 0.28).',
  ratio: 'Blends arithmetically: share = (weight × value) ÷ Σ(weight × value) over the holdings '
    + 'reporting here. Sums to 100%.',
  level: 'Each holding is rebased to 100 at its own first year (an index); share = (weight × index) '
    + '÷ Σ(weight × index). The amount as reported is shown beside it.',
};

export default function BlendDrilldownModal({
  title, metricCode, period, portfolioId, basket, format, onClose,
}: {
  title: string;
  metricCode: string;
  period: string;
  portfolioId?: number;
  basket?: { holdings: { isin: string; weight: number; name?: string }[] };
  /** The chart's own formatter, so a value reads the same here as on the axis it came from. */
  format?: (v: number) => string;
  onClose: () => void;
}) {
  const [data, setData] = useState<Breakdown | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [tab, setTab] = useState<Tab>('period');
  const [matrix, setMatrix] = useState<Matrix | null>(null);
  const [matrixErr, setMatrixErr] = useState<string | null>(null);

  // The holdings selector, identical for both endpoints.
  const target = basket
    ? { holdings: basket.holdings.map((h) => ({ isin: h.isin, name: h.name, weight: h.weight })) }
    : { portfolio_id: portfolioId };

  useEffect(() => {
    let alive = true;
    (async () => {
      setData(null); setErr(null);
      try {
        const r = await apiFetch(`${API_URL}/api/earnings/fundamental-blend-breakdown`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ metric_code: metricCode, period, ...target }),
        });
        const b = await r.json().catch(() => null);
        if (!alive) return;
        if (!r.ok) { setErr(b?.detail ?? `HTTP ${r.status}`); return; }
        setData(b as Breakdown);
      } catch (e) {
        if (alive) setErr(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { alive = false; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [metricCode, period, portfolioId, basket]);

  // The matrix is a heavier read (all periods × all holdings), so it loads only when the tab is
  // first opened and is cached thereafter.
  useEffect(() => {
    if (tab !== 'matrix' || matrix || matrixErr) return;
    let alive = true;
    (async () => {
      try {
        const r = await apiFetch(`${API_URL}/api/earnings/fundamental-blend-matrix`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ metric_code: metricCode, ...target }),
        });
        const b = await r.json().catch(() => null);
        if (!alive) return;
        if (!r.ok) { setMatrixErr(b?.detail ?? `HTTP ${r.status}`); return; }
        setMatrix(b as Matrix);
      } catch (e) {
        if (alive) setMatrixErr(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { alive = false; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tab, metricCode, portfolioId, basket]);

  const fmt = (v: number | null) => (v == null ? '—' : format ? format(v) : v.toFixed(2));

  // Bar scales for the period table — each bar is relative to the biggest in the set, so the
  // largest share/swing fills its track and the rest read as a proportion of it.
  const pMembers = useMemo(() => data?.members ?? [], [data]);
  const maxShare = Math.max(1e-9, ...pMembers.map((m) => m.share_pct ?? 0));

  // Sortable period table. Default: weight, high → low (what a reader scans first).
  const [sort, setSort] = useState<Sort>({ key: 'weight', dir: 'desc' });
  const toggleSort = (key: SortKey) => setSort((s) =>
    s.key === key ? { key, dir: s.dir === 'desc' ? 'asc' : 'desc' }
      // First click on a new column: names read A→Z, numbers biggest-first.
      : { key, dir: key === 'holding' ? 'asc' : 'desc' });

  const sortedMembers = useMemo(() => {
    const get: Record<SortKey, (m: Member) => number | string | null | undefined> = {
      holding: (m) => (m.name ?? m.isin ?? '').toLowerCase(),
      weight: (m) => m.weight_pct,
      value: (m) => m.value,
      raw: (m) => m.raw_value,
      share: (m) => m.share_pct,
      inverse: inverseOf,
      contribution: contribOf,
    };
    return [...pMembers].sort((a, b) => cmp(get[sort.key](a), get[sort.key](b), sort.dir));
  }, [pMembers, sort]);
  const caret = (k: SortKey) => (sort.key === k ? (sort.dir === 'desc' ? ' ▾' : ' ▴') : '');

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-scrim/60 p-4"
      onClick={onClose} role="presentation">
      <div className="bg-card rounded-xl border border-neutral-800/40 shadow-xl w-[80vw] h-[80vh] flex flex-col"
        onClick={(e) => e.stopPropagation()} role="dialog" aria-modal="true">
        <div className="flex items-baseline gap-3 px-6 py-4 border-b border-neutral-800/40">
          <h2 className="text-fg-strong font-medium">{title}</h2>
          {tab === 'period' && <span className="text-fg-muted text-sm">{period}</span>}
          {tab === 'period' && data?.value != null && (
            <span className="font-mono text-fg-strong">{fmt(data.value)}</span>
          )}
          {tab === 'period' && data && (
            <span className="text-[12px] text-fg-faint">
              blended over {data.covered_pct.toFixed(1)}% of weight
            </span>
          )}
          {/* Period = the clicked year decomposed; Matrix = every holding at every period. */}
          <div className="flex items-center gap-0.5 rounded-lg border border-neutral-700 p-0.5 ml-2">
            {(['period', 'matrix'] as Tab[]).map((t) => (
              <button key={t} type="button" onClick={() => setTab(t)}
                className={`px-2.5 py-1 rounded-md text-xs font-medium transition-colors ${
                  tab === t ? 'bg-accent-600 text-fg-strong'
                    : 'text-fg-muted hover:text-fg-strong hover:bg-overlay/5'}`}>
                {t === 'period' ? 'This period' : 'All periods'}
              </button>
            ))}
          </div>
          <button type="button" onClick={onClose}
            className="ml-auto text-fg-muted hover:text-fg-strong px-2">✕</button>
        </div>

        <div className="flex-1 overflow-auto px-6 py-4 space-y-4">
          {tab === 'period' && err && <p className="text-xs text-neg-300">{err}</p>}
          {tab === 'period' && !data && !err && (
            <p className="text-xs text-fg-subtle">Loading breakdown…</p>
          )}

          {tab === 'period' && data && (() => {
            const gf = `GuruFocus fundamentals — metric_data, code “${metricCode}”.`;
            const isMult = data.kind === 'multiple';
            // Per-column help, in the default WHAT / WHERE / WHEN / HOW template. Names the ACTUAL
            // metric (`title`) and its stored code so a column reads as that metric, not "value".
            const COL_INFO: Record<SortKey, { what: string; where: string; when: string; how: string }> = {
              holding: {
                what: 'The individual company behind this blended figure.',
                where: "Our company record, matched from the model position's ISIN.",
                when: "The model's current composition.",
                how: 'One row per position — a name can appear twice when held both directly and '
                  + 'through a linked certificate, each with its own weight.',
              },
              weight: {
                what: "The holding's weight in the portfolio.",
                where: 'The AIRS model composition.',
                when: 'Current composition.',
                how: `Renormalised over the holdings that reported ${title} this period, so the `
                  + 'column sums to the covered weight (the Portfolio row).',
              },
              value: data.kind === 'level'
                ? { what: `Each holding's ${title}, rebased to an index.`, where: gf,
                  when: 'The fiscal year of this point.',
                  how: "Rebased to 100 at the holding's own first year so companies on different "
                    + 'scales can be blended; the amount as reported is the next column.' }
                : { what: `Each holding's ${title}, as reported.`, where: gf,
                  when: 'The fiscal year of this point.',
                  how: `Taken as reported — the per-holding ${title} that enters the blend.` },
              inverse: {
                what: 'The earnings yield — €1 of price buys this many € of earnings.',
                where: `Computed: 1 ÷ ${title}.`, when: 'This period.',
                how: 'The reciprocal of the P/E. You cannot average P/Es, but you CAN add yields — '
                  + 'so this is the quantity the blend actually works in.',
              },
              contribution: {
                what: 'The earnings this holding brings, per € of the whole portfolio.',
                where: 'Computed on our side.', when: 'This period.',
                how: 'weight × (1 ÷ P/E). Summed over the holdings (the Portfolio row) it IS the '
                  + "portfolio's earnings yield, and the blended P/E is total weight ÷ that sum.",
              },
              raw: {
                what: `${title}, exactly as the company reported it.`,
                where: gf, when: 'Its fiscal year.', how: 'Shown before rebasing to an index.',
              },
              share: {
                what: `How much of the blended ${title} this holding accounts for.`,
                where: 'Computed on our side, from weight and value.',
                when: 'This period.',
                how: SHARE_NOTE[data.kind],
              },
            };

            // Portfolio totals (the reconciling footer) — Σ over the reporting holdings.
            const sumW = data.members.reduce((a, m) => a + (m.weight_pct ?? 0), 0);        // covered %
            const sumContrib = data.members.reduce((a, m) => a + (contribOf(m) ?? 0), 0);  // Σ weight×(1/PE)
            const num = (v: number | null | undefined, d = 4) => (v == null ? '—' : v.toFixed(d));
            const pct = (v: number | null | undefined) => (v == null ? '—' : `${v.toFixed(1)}%`);
            const shareCell = (m: Member) => {
              const s = m.share_pct ?? 0;
              return (
                <div className="flex items-center gap-2 justify-end">
                  <div className="h-1.5 w-20 rounded-full bg-overlay/10 overflow-hidden">
                    <div className="h-full rounded-full bg-accent-500/70"
                      style={{ width: `${Math.min(100, (s / maxShare) * 100)}%` }} />
                  </div>
                  <span className="font-mono text-fg-muted w-11 text-right">
                    {m.share_pct == null ? '—' : `${s.toFixed(1)}%`}
                  </span>
                </div>
              );
            };

            type Col = {
              key: SortKey; label: string; align: 'left' | 'right'; extra?: string; strong?: boolean;
              cell: (m: Member) => React.ReactNode; foot?: React.ReactNode;
            };
            const holdingCol: Col = { key: 'holding', label: 'Holding', align: 'left', extra: 'w-full',
              cell: (m) => <span className="block truncate" title={m.name ?? m.isin ?? ''}>{m.name ?? m.isin}</span>,
              foot: 'Portfolio' };
            const weightCol: Col = { key: 'weight', label: 'Weight', align: 'right',
              cell: (m) => pct(m.weight_pct), foot: pct(sumW) };
            const valueCol: Col = { key: 'value', label: isMult ? title : (data.kind === 'level' ? 'Index' : 'Value'),
              align: 'right', strong: true, cell: (m) => fmt(m.value), foot: fmt(data.value) };
            const inverseCol: Col = { key: 'inverse', label: '1 ÷ P/E', align: 'right',
              cell: (m) => num(inverseOf(m)),
              // Portfolio yield = Σcontribution ÷ Σweight(fraction) = 1 ÷ blended P/E.
              foot: sumW > 0 ? num(sumContrib / (sumW / 100)) : '—' };
            const contribCol: Col = { key: 'contribution', label: 'Contribution', align: 'right',
              cell: (m) => num(contribOf(m)), foot: num(sumContrib) };
            const rawCol: Col = { key: 'raw', label: 'As reported', align: 'right',
              cell: (m) => (m.raw_value == null ? '—' : m.raw_value.toLocaleString(undefined, { maximumFractionDigits: 2 })) };
            const shareCol: Col = { key: 'share', label: 'Share', align: 'right', cell: shareCell, foot: '100%' };

            // ⚠ For a MULTIPLE the whole arithmetic is laid out so it reconciles on screen: P/E,
            // its inverse, weight, contribution (weight × inverse), share (contribution ÷ Σ).
            const cols: Col[] = isMult
              ? [holdingCol, valueCol, inverseCol, weightCol, contribCol, shareCol]
              : data.kind === 'level'
                ? [holdingCol, weightCol, valueCol, rawCol, shareCol]
                : [holdingCol, weightCol, valueCol, shareCol];

            const bodyTd = (c: Col) => c.align === 'left'
              ? 'px-3 py-1.5 text-fg-soft max-w-0'
              : `px-3 py-1.5 text-right font-mono whitespace-nowrap ${c.strong ? 'text-fg-strong' : 'text-fg-muted'}`;

            return (
            <>
              <p className="text-[12px] text-fg-faint max-w-3xl">{SHARE_NOTE[data.kind]}</p>

              <div className="overflow-x-auto rounded-lg border border-neutral-800/40">
                <table className="w-full text-xs">
                  <thead className="bg-page">
                    <tr className="text-fg-faint text-[11px] uppercase tracking-wide border-b border-neutral-800/40 [&>th]:cursor-pointer [&>th]:select-none [&>th:hover]:text-fg-soft [&>th]:transition-colors">
                      {cols.map((c) => (
                        <th key={c.key} onClick={() => toggleSort(c.key)}
                          className={`px-3 py-2 font-medium ${c.align === 'left' ? 'text-left' : 'text-right whitespace-nowrap'} ${c.extra ?? ''}`}>
                          <span className={`inline-flex items-center gap-1 ${c.align === 'right' ? 'justify-end' : ''}`}>
                            {c.label}{caret(c.key)}
                            <InfoTip content={<AspectCard {...COL_INFO[c.key]} />} />
                          </span>
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {sortedMembers.map((m, i) => (
                      <tr key={`${m.isin ?? m.name}-${i}`}
                        className="border-b border-neutral-800/20 last:border-0 hover:bg-overlay/[0.03]">
                        {cols.map((c) => <td key={c.key} className={bodyTd(c)}>{c.cell(m)}</td>)}
                      </tr>
                    ))}
                  </tbody>
                  {/* ⚠ Only a MULTIPLE gets the reconciling row: total weight ÷ total contribution
                      = the blended P/E in the value cell, so the reader can watch it add up. */}
                  {isMult && (
                    <tfoot>
                      <tr className="border-t border-neutral-800/40 bg-page font-semibold text-fg-strong">
                        {cols.map((c) => (
                          <td key={c.key}
                            className={c.align === 'left' ? 'px-3 py-2 text-left' : 'px-3 py-2 text-right font-mono whitespace-nowrap'}>
                            {c.foot ?? ''}
                          </td>
                        ))}
                      </tr>
                    </tfoot>
                  )}
                </table>
              </div>

              {/* ⚠ The exclusions are half the answer — a thin point can only be recognised as
                  thin if the weight that is NOT in it is named. */}
              {data.excluded.length > 0 && (
                <div className="space-y-1">
                  <h3 className="text-xs font-semibold text-fg-strong">
                    Not in this figure ({data.excluded_pct.toFixed(1)}% of weight)
                  </h3>
                  <div className="overflow-auto rounded-lg border border-neutral-800/40">
                    <table className="w-auto text-xs whitespace-nowrap">
                      <tbody>
                        {data.excluded.map((e, i) => (
                          <tr key={`${e.isin ?? e.name}-${i}`} className="border-b border-neutral-800/20">
                            <td className="px-3 py-1.5 text-fg-soft">
                              <span className="inline-block max-w-[28ch] truncate align-bottom">{e.name ?? e.isin}</span>
                            </td>
                            <td className="px-3 py-1.5 text-right font-mono text-fg-muted">{e.weight_pct.toFixed(1)}%</td>
                            <td className="px-3 py-1.5 text-fg-faint">{REASON[e.reason] ?? e.reason}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
            </>
            );
          })()}

          {tab === 'matrix' && matrixErr && <p className="text-xs text-neg-300">{matrixErr}</p>}
          {tab === 'matrix' && !matrix && !matrixErr && (
            <p className="text-xs text-fg-subtle">Loading matrix…</p>
          )}
          {tab === 'matrix' && matrix && (
            <>
              <p className="text-[12px] text-fg-faint max-w-3xl">{MATRIX_NOTE[matrix.kind]}</p>
              <div className="overflow-auto rounded-lg border border-neutral-800/40">
                <table className="text-xs whitespace-nowrap">
                  <thead className="bg-page">
                    <tr className="text-fg-faint text-[11px] uppercase tracking-wide border-b border-neutral-800/40">
                      {/* Holding stays put while the years scroll. */}
                      <th className="px-3 py-1.5 font-medium text-left sticky left-0 bg-page z-10">Holding</th>
                      <th className="px-3 py-1.5 font-medium text-right">Wt</th>
                      {matrix.periods.map((p) => (
                        <th key={p}
                          title={matrix.below_floor[p]
                            ? `${matrix.covered[p].toFixed(0)}% covered — below the 60% floor, hidden on the chart`
                            : `${matrix.covered[p].toFixed(0)}% of weight covered`}
                          className={`px-3 py-1.5 font-medium text-right ${
                            matrix.below_floor[p] ? 'text-warn-300' : ''}`}>
                          {p}{matrix.below_floor[p] ? ' *' : ''}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {matrix.members.map((m, i) => (
                      <tr key={`${m.isin ?? m.name}-${i}`} className="border-b border-neutral-800/20 hover:bg-overlay/[0.02]">
                        <td className="px-3 py-1.5 text-fg-soft sticky left-0 bg-card z-10">
                          <span className="inline-block max-w-[24ch] truncate align-bottom"
                            title={m.name ?? m.isin ?? ''}>{m.name ?? m.isin}</span>
                        </td>
                        <td className="px-3 py-1.5 text-right font-mono text-fg-muted">{m.weight_pct.toFixed(1)}%</td>
                        {matrix.periods.map((p) => {
                          const c = m.cells[p];
                          if (!c) return <td key={p} className="px-3 py-1.5 text-right text-fg-faint">·</td>;
                          // A rebased LEVEL shows the index; its as-reported amount lives in the tooltip.
                          const tip = matrix.kind === 'level' && c.raw != null
                            ? `as reported: ${c.raw.toLocaleString(undefined, { maximumFractionDigits: 2 })}` : undefined;
                          return (
                            <td key={p} title={c.dropped ? 'loss-making — dropped from the blend' : tip}
                              className={`px-3 py-1.5 text-right font-mono ${
                                c.dropped ? 'text-fg-faint line-through' : 'text-fg-soft'}`}>
                              {fmt(c.value)}
                            </td>
                          );
                        })}
                      </tr>
                    ))}
                  </tbody>
                  <tfoot>
                    <tr className="border-t border-neutral-800/40 bg-page">
                      <td className="px-3 py-1.5 text-left font-semibold text-fg-strong sticky left-0 bg-page z-10">Blended</td>
                      <td className="px-3 py-1.5" />
                      {matrix.periods.map((p) => (
                        <td key={p} className={`px-3 py-1.5 text-right font-mono font-semibold ${
                          matrix.below_floor[p] ? 'text-warn-300' : 'text-fg-strong'}`}>
                          {fmt(matrix.blended[p])}
                        </td>
                      ))}
                    </tr>
                    <tr className="bg-page">
                      <td className="px-3 py-1 text-left text-[11px] uppercase tracking-wide text-fg-faint sticky left-0 bg-page z-10">Covered</td>
                      <td className="px-3 py-1" />
                      {matrix.periods.map((p) => (
                        <td key={p} className={`px-3 py-1 text-right font-mono text-[12px] ${
                          matrix.below_floor[p] ? 'text-warn-300' : 'text-fg-muted'}`}>
                          {matrix.covered[p].toFixed(0)}%
                        </td>
                      ))}
                    </tr>
                  </tfoot>
                </table>
              </div>
              <p className="text-[11px] text-fg-faint">
                * years below the 60% coverage floor — shown here for verification, hidden on the chart.
              </p>

              {matrix.excluded.length > 0 && (
                <div className="space-y-1">
                  <h3 className="text-xs font-semibold text-fg-strong">
                    No data for this metric ({matrix.excluded.length})
                  </h3>
                  <div className="overflow-auto rounded-lg border border-neutral-800/40">
                    <table className="w-auto text-xs whitespace-nowrap">
                      <tbody>
                        {matrix.excluded.map((e, i) => (
                          <tr key={`${e.isin ?? e.name}-${i}`} className="border-b border-neutral-800/20">
                            <td className="px-3 py-1.5 text-fg-soft">
                              <span className="inline-block max-w-[28ch] truncate align-bottom">{e.name ?? e.isin}</span>
                            </td>
                            <td className="px-3 py-1.5 text-right font-mono text-fg-muted">{e.weight_pct.toFixed(1)}%</td>
                            <td className="px-3 py-1.5 text-fg-faint">{REASON[e.reason] ?? e.reason}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
            </>
          )}
        </div>
      </div>
    </div>
  );
}
