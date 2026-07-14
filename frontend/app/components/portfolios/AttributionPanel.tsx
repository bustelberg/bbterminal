'use client';

import { Fragment, useEffect, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import type { ModelPortfolioAttribution } from '../../../lib/types/api';
import InfoTip from '../InfoTip';
import Formula, { Op, Paren, V, VBar } from '../Formula';

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

/** An effect cell. `title` (when given) is the row's explanation IN WORDS — rendered through
 *  `InfoTip`, not the native attribute, because the browser delays that by ~1-2 seconds. */
function Eff({ v, title }: { v?: number | null; title?: string }) {
  if (v == null) return <span className="text-fg-faint">—</span>;
  const body = Math.abs(v) < 0.005
    ? <span className="text-fg-faint">—</span>
    : <span className={v >= 0 ? 'text-pos-400' : 'text-neg-400'}>{pct(v)}</span>;
  return title ? <InfoTip text={title}>{body}</InfoTip> : body;
}

/**
 * The allocation effect, IN WORDS.
 *
 * The column is doing something specific and non-obvious, and a bare number does not say it: an
 * over/underweight is scored against how that bucket did *relative to the INDEX AS A WHOLE*, not
 * against zero. Overweighting a sector that rose 12.8% while the index rose 36.9% is a BAD call
 * even though the sector went up — and that is exactly the case a reader misreads without being
 * told. So every cell explains itself.
 */
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

function allocationWhy(
  group: string, axis: string, wP: number, wB: number,
  rB: number | null | undefined, rBTotal: number,
): string {
  const w = AXIS_WORD[axis] ?? 'group';
  const tilt = wP - wB;
  if (rB == null) return `The index holds nothing in ${group}, so there is no index return to judge the tilt against.`;
  const side = tilt >= 0 ? 'overweight' : 'underweight';
  const beat = rB >= rBTotal;
  const verdict = (tilt >= 0) === beat
    ? (tilt >= 0
      ? `You leaned into a ${w} that beat the index, so the tilt paid.`
      : `You avoided a ${w} that lagged the index, so the tilt paid.`)
    : (tilt >= 0
      ? `You leaned into a ${w} that lagged the index, so the tilt cost you.`
      : `You avoided a ${w} that beat the index, so the tilt cost you.`);
  return (
    `You held ${wP.toFixed(1)}% vs the index's ${wB.toFixed(1)}% — a ${Math.abs(tilt).toFixed(1)}pp `
    + `${side}.\n\nIn the index, ${group} returned ${pct(rB, 1)} while the index as a whole `
    + `returned ${pct(rBTotal, 1)}, so it ${beat ? 'beat' : 'lagged'} the index. ${verdict}\n\n`
    + `This is judged at the index's returns, not yours. Whether the companies you picked in `
    + `${group} were any good is the Selection column.`
  );
}

function selectionWhy(
  group: string, wB: number, rP: number | null | undefined, rB: number | null | undefined,
): string {
  if (rP == null) return `You hold nothing in ${group}, so there are no companies to judge. The whole effect is the decision not to own it — see Allocation.`;
  if (rB == null) return `The index holds nothing in ${group}, so there is nothing to measure your companies against.`;
  return (
    `Your ${group} companies returned ${pct(rP, 1)}; the index's returned ${pct(rB, 1)}.\n\n`
    + `Scored at the index's weight (${wB.toFixed(1)}%), so this is purely about the companies you `
    + `picked — not how much you held.`
  );
}

/**
 * A column header that explains itself.
 *
 * Every column here is a term of art or a subscripted symbol — `w_P`, `R_B`, "Interact." — and a
 * reader who has to guess what one means will guess wrong in a way that LOOKS right. The
 * allocation column especially: its number is meaningless until you know it is scored against the
 * index's TOTAL return, not against zero.
 *
 * ⚠ NOT the native `title=` attribute. The browser sits on that for ~1-2 SECONDS before showing
 * it, and the delay is not configurable — by which time the reader has already explained the
 * column to themselves, wrongly. `InfoTip` renders on hover, immediately.
 *
 * The dotted underline is the affordance. A tooltip nobody knows is there is a tooltip that does
 * not exist.
 */
function Th({ label, sub, help, align = 'right' }: {
  label: React.ReactNode;
  sub?: string;
  help: string;
  align?: 'left' | 'right';
}) {
  return (
    <th className={`px-2 py-1.5 font-medium ${align === 'left' ? 'text-left' : 'text-right'}`}>
      <InfoTip text={help}>
        <span className="decoration-dotted underline decoration-neutral-600 underline-offset-2 hover:text-accent-400 transition-colors">
          {label}
        </span>
      </InfoTip>
      {sub && (
        <div className="text-[9px] font-normal normal-case text-fg-faint">{sub}</div>
      )}
    </th>
  );
}

/**
 * The four effects: what each one MEANS, and the arithmetic that produces it.
 *
 * A prose paragraph could say what allocation is, but not what it *is* — and a reader looking at
 * a −5.07 wants to know which numbers made it. So each row carries the formula next to the
 * sentence. They are the same four columns, in the same order, as the table below.
 *
 * The symbols are the table's: w = weight, R = return, P = your portfolio, B = the benchmark
 * inside that bucket, and R̄B = the benchmark's TOTAL return — the reference allocation is scored
 * against, which is the single fact that makes the column readable.
 */
function Legend({ rIndex, axis }: { rIndex: string; axis: string }) {
  const w = AXIS_WORD[axis] ?? 'group';
  // The two deviations every effect is built from — spelled once, reused, so the formulas below
  // read as variations on one idea rather than four unrelated products.
  const tilt = (
    <Paren>
      <V name="w" sub="P" /><Op>−</Op><V name="w" sub="B" />
    </Paren>
  );
  const edge = (
    <Paren>
      <V name="R" sub="P" /><Op>−</Op><V name="R" sub="B" />
    </Paren>
  );

  const rows: Array<{ name: string; formula: React.ReactNode; meaning: string }> = [
    {
      name: 'Allocation',
      formula: (
        <Formula>
          {tilt}<Op>×</Op>
          <Paren>
            <V name="R" sub="B" /><Op>−</Op><VBar name="R" sub="B" />
          </Paren>
        </Formula>
      ),
      meaning: `Your tilt × how that ${w} did against the index as a whole (${rIndex}). Did you put the money in the right ${w}s? A ${w} that rose but rose by less than the index was still the wrong place to be.`,
    },
    {
      name: 'Selection',
      formula: <Formula><V name="w" sub="B" /><Op>×</Op>{edge}</Formula>,
      meaning: `Your companies vs the index’s companies inside the ${w}, priced at the index’s weight. Purely the picks — how much you held is Allocation’s job.`,
    },
    {
      name: 'Interaction',
      formula: <Formula>{tilt}<Op>×</Op>{edge}</Formula>,
      meaning: 'Tilt × pick-edge. The part that needs both: it only exists because you deviated on weight and on companies at once. Positive when they agree (overweight where you were good), negative when they fight.',
    },
    {
      name: 'Total',
      formula: <span className="text-fg-subtle">Allocation + Selection + Interaction</span>,
      meaning: `This ${w}’s share of the excess. The column sums to the whole excess — that identity is checked, not assumed.`,
    },
  ];
  return (
    <dl className="mt-2 mb-3 grid gap-x-3 gap-y-1.5 text-[11px]"
      style={{ gridTemplateColumns: 'auto auto 1fr' }}>
      {rows.map((r) => (
        <Fragment key={r.name}>
          <dt className="font-semibold text-fg whitespace-nowrap">{r.name}</dt>
          <dd className="font-mono text-accent-400 whitespace-nowrap">{r.formula}</dd>
          <dd className="text-fg-subtle leading-relaxed">{r.meaning}</dd>
        </Fragment>
      ))}
    </dl>
  );
}

function Names({ title, rows, hint }: {
  title: string;
  rows: NonNullable<ModelPortfolioAttribution['top_contributors']>;
  hint: string;
}) {
  if (!rows?.length) return null;
  return (
    <div>
      <h5 className="text-[11px] font-semibold text-fg-strong">{title}</h5>
      <p className="text-[10px] text-fg-faint mb-1">{hint}</p>
      <table className="w-full text-[11px]">
        {/* Three bare percentages in a row (10.0% · +59.2% · +5.92%) are unreadable without
            labels — worse than an unexplained header, because there is nothing to hover. */}
        <thead>
          <tr className="text-fg-faint text-[9px] uppercase tracking-wide">
            <th className="py-0.5 pr-2 text-left font-medium">Name</th>
            <Th label="Wt" help="Its weight in the model over this window." />
            <Th label="Return" help="What it returned over the window, in EUR." />
            <Th label="Contrib."
              help={"Weight × return — how many percentage points of the model's return this single company is responsible for.\n\nA big move in a tiny position contributes little, so this is the column that says which companies actually mattered."} />
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr key={`${r.isin ?? r.ticker ?? r.name}`} className="border-t border-neutral-800/20">
              <td className="py-1 pr-2 text-fg truncate max-w-[11rem]">
                {r.name ?? r.ticker ?? r.isin}
              </td>
              <td className="py-1 px-2 text-right font-mono text-fg-subtle">
                {n(r.weight_pct).toFixed(1)}%
              </td>
              <td className="py-1 px-2 text-right font-mono text-fg-subtle">
                {pct(r.return_pct, 1)}
              </td>
              <td className="py-1 pl-2 text-right font-mono font-semibold">
                <Eff v={r.contribution_pct} />
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default function AttributionPanel({ id, benchmark, window, onClose }: {
  id: number; benchmark: string; window: 'ytd' | 'since'; onClose: () => void;
}) {
  const [axis, setAxis] = useState<Axis>('sector');
  const [data, setData] = useState<ModelPortfolioAttribution | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      try {
        const r = await apiFetch(
          `${API_URL}/api/airs/model-portfolios/${id}/attribution`
          + `?benchmark=${benchmark}&window=${window}&axis=${axis}`);
        const b = await r.json().catch(() => null);
        if (cancelled) return;
        if (!r.ok) { setError(b?.detail ?? `HTTP ${r.status}`); return; }
        setData(b as ModelPortfolioAttribution);
      } catch (e) {
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { cancelled = true; };
  }, [id, benchmark, window, axis]);

  const label = window === 'ytd' ? 'YTD' : 'Since inception';
  // ONE source for the word: the axis the server actually computed, NOT the picker's state. If
  // the two disagree — a response still in flight, a server that normalises an unknown axis —
  // the labels must describe the numbers ON SCREEN, not the request that asked for them.
  const w = AXIS_WORD[data?.axis ?? axis] ?? 'group';

  return (
    <section className="bg-card border border-neutral-800/40 rounded-xl p-4 lg:col-span-2">
      <div className="flex items-start justify-between gap-3 mb-2">
        <div>
          <h4 className="text-sm font-semibold text-fg-strong">
            {`Why — ${label} vs ${benchmark}`}
          </h4>
          {/* The key. Set in the SAME notation as the formulas below — a legend written in a
              different alphabet from the thing it explains is not a legend. */}
          <p className="text-[11px] text-fg-faint mt-0.5">
            {'Brinson-Fachler. '}
            <Formula className="text-fg-subtle"><V name="w" /></Formula>
            {' = weight, '}
            <Formula className="text-fg-subtle"><V name="R" /></Formula>
            {' = return; subscript '}
            <Formula className="text-fg-subtle"><V name="P" /></Formula>
            {' = you, '}
            <Formula className="text-fg-subtle"><V name="B" /></Formula>
            {' = the index in that group. '}
            <Formula className="text-fg-subtle"><VBar name="R" sub="B" /></Formula>
            {' = the index’s TOTAL return, which is what Allocation is scored against. '}
            {'Hover any header or cell for that row in words.'}
          </p>
        </div>
        <div className="flex items-center gap-2 shrink-0">
          <select value={axis} onChange={(e) => { setData(null); setAxis(e.target.value as Axis); }}
            className="bg-page border border-neutral-700 rounded-lg px-2 py-1 text-[11px] text-fg focus:border-accent-500">
            <option value="sector">by Sector</option>
            <option value="region">by Region</option>
            <option value="currency">by Currency</option>
          </select>
          <button type="button" onClick={onClose}
            className="text-[11px] px-2 py-1 rounded-lg border border-neutral-700 text-fg-muted hover:text-accent-300 transition-colors">
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
          {/* ⚠ The identity IS the decomposition. If it fails, these are just three columns. */}
          {!data.reconciles && (
            <p className="text-[11px] text-neg-300 mb-2">
              {'⚠ The effects do not sum to the excess (residual '}
              {pct(data.residual_pct, 4)}
              {'). This is NOT a valid decomposition — do not read the rows below as one.'}
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

          {/* What each column MEANS and the arithmetic that produces it — a reader looking at a
              −5.07 wants to know which numbers made it, and prose alone cannot say. */}
          <Legend rIndex={pct(data.benchmark_return_pct, 1)} axis={data.axis ?? axis} />

          {/* The conclusion, in a sentence. The totals row already contains it, but "which of the
              two mistakes was this?" is the entire question the reader opened this panel to ask,
              and making them subtract two numbers to find out is making them do the work. */}
          {(() => {
            const alloc = (data.rows ?? []).reduce((s, r) => s + n(r.allocation_pct), 0);
            const sel = (data.rows ?? []).reduce((s, r) => s + n(r.selection_pct), 0);
            const dominant = Math.abs(sel) >= Math.abs(alloc) ? 'selection' : 'allocation';
            // "cost you +2.75%" is not a sentence. The verb already carries the sign, so the
            // number must not carry it again.
            const verb = (v: number) => (v >= 0 ? 'added' : 'cost you');
            const mag = (v: number) => `${Math.abs(v).toFixed(2)}pp`;
            return (
              <p className="text-xs text-fg mb-2 bg-inset rounded-lg px-3 py-2">
                {dominant === 'selection'
                  ? `Mostly the companies, not the ${AXIS_WORD[data.axis ?? 'sector'] ?? 'group'}s: `
                  : `Mostly the ${AXIS_WORD[data.axis ?? 'sector'] ?? 'group'}s, not the companies: `}
                {`your ${AXIS_WORD[data.axis ?? 'sector'] ?? 'group'} tilts `}
                <span className={alloc >= 0 ? 'text-pos-400' : 'text-neg-400'}>
                  {verb(alloc)} {mag(alloc)}
                </span>
                {', and the companies you picked inside them '}
                <span className={sel >= 0 ? 'text-pos-400' : 'text-neg-400'}>
                  {verb(sel)} {mag(sel)}
                </span>
                {'.'}
              </p>
            );
          })()}

          <p className="text-[11px] text-fg-subtle mb-2">
            {'Explains '}
            <span className="font-mono text-fg">{(data.attributable_pct ?? 0).toFixed(0)}%</span>
            {' of the model. '}
            <span className="font-mono">{(data.excluded_pct ?? 0).toFixed(0)}%</span>
            {' is excluded — funds and cash are not a sector bet, so they are not decomposed as '}
            {'one. Attributed excess '}
            <span className="font-mono">{pct(data.excess_pct)}</span>
            {' = portfolio '}
            <span className="font-mono">{pct(data.portfolio_return_pct)}</span>
            {' − '}
            {benchmark}
            {' '}
            <span className="font-mono">{pct(data.benchmark_return_pct)}</span>
            {'.'}
          </p>

          <div className="overflow-auto rounded-lg border border-neutral-800/40 mb-3">
            <table className="w-full text-[11px]">
              <thead className="bg-card">
                <tr className="text-fg-faint text-[10px] uppercase tracking-wide border-b border-neutral-800/40">
                  <Th align="left" label={w}
                    help={`The ${w}s the excess is split across.\n\nEvery ${w} either side holds appears — including the ones the index holds and you don't, because choosing not to own something is a decision the numbers can price.`} />
                  <Th label={<>w<sub>P</sub></>} sub="you"
                    help={`Your weight in this ${w}, as a % of the attributable sleeve — funds and cash removed, the rest renormalised to 100%.\n\nNot the raw model weight: the table only explains what it can decompose.`} />
                  <Th label={<>w<sub>B</sub></>} sub="index"
                    help={`The index's weight in this ${w}, at the start of the window.\n\nStart-of-window, because weighting by today's market cap is look-ahead bias: a company that doubled would retroactively be given twice the share of the index it actually had.`} />
                  <Th label={<>R<sub>P</sub></>} sub="your return"
                    help={`What your holdings in this ${w} returned, in EUR, over this window.\n\nA dash means you hold nothing here.`} />
                  {/* The reference point. Allocation is scored against THIS number, so it has to
                      be on the screen — an over/underweight is judged by whether its sector beat
                      or lagged the index as a whole, not by whether it went up. */}
                  <Th label={<>R<sub>B</sub></>}
                    sub={`index total ${pct(data.benchmark_return_pct, 1)}`}
                    help={`What the index's holdings in this ${w} returned, in EUR.\n\nCompare it to the index's total (${pct(data.benchmark_return_pct, 1)}) — that comparison is the allocation effect. A ${w} can rise and still have been a bad place to be, if it rose by less than the index.`} />
                  <Th label="Allocation" sub={`right ${w}s?`}
                    help={`Your over/underweight, multiplied by how that ${w} did versus the index as a whole.\n\nPositive means you leaned into a ${w} that beat the index, or avoided one that lagged it. It is judged at the index's returns, so your company-picking is held constant — this column is only about where you placed the money.\n\nThe catch: overweighting a ${w} that rose 5% while the index rose 10% is negative. It went up, and it was still the wrong place to be.`} />
                  <Th label="Selection" sub="right companies?"
                    help={`Your companies against the index's companies inside the ${w}, scored at the index's weight.\n\nUsing the index's weight is what makes it purely about the picks — how much you held is the Allocation column's job, not this one.`} />
                  <Th label="Interact." sub="the cross term"
                    help={`Your tilt multiplied by your selection edge.\n\nThe part that can't be assigned cleanly to either column: a big overweight and good picks in the same ${w} reinforce each other, and that reinforcement belongs to neither alone.\n\nUsually small. When it is large, the tilt and the picks were pulling hard in the same direction.`} />
                  <Th label="Total" sub={`= this ${w}'s excess`}
                    help={`Allocation + selection + interaction for this ${w} — how much of the total excess came from here.\n\nThe column sums to the whole excess. That identity is checked, not assumed.`} />
                </tr>
              </thead>
              <tbody className="divide-y divide-neutral-800/20">
                {(data.rows ?? []).map((r) => (
                  <tr key={r.bucket} className="hover:bg-overlay/[0.02]">
                    <td className="px-2 py-1.5 text-fg whitespace-nowrap">{r.bucket}</td>
                    <td className="px-2 py-1.5 text-right font-mono text-fg-subtle">{n(r.portfolio_weight_pct).toFixed(1)}</td>
                    <td className="px-2 py-1.5 text-right font-mono text-fg-subtle">{n(r.benchmark_weight_pct).toFixed(1)}</td>
                    <td className="px-2 py-1.5 text-right font-mono text-fg-subtle">{pct(r.portfolio_return_pct, 1)}</td>
                    <td className="px-2 py-1.5 text-right font-mono text-fg-subtle">{pct(r.benchmark_return_pct, 1)}</td>
                    <td className="px-2 py-1.5 text-right font-mono">
                      <Eff v={n(r.allocation_pct)} title={allocationWhy(
                        r.bucket, data.axis ?? axis, n(r.portfolio_weight_pct), n(r.benchmark_weight_pct),
                        r.benchmark_return_pct, n(data.benchmark_return_pct))} />
                    </td>
                    <td className="px-2 py-1.5 text-right font-mono">
                      <Eff v={n(r.selection_pct)} title={selectionWhy(
                        r.bucket, n(r.benchmark_weight_pct),
                        r.portfolio_return_pct, r.benchmark_return_pct)} />
                    </td>
                    <td className="px-2 py-1.5 text-right font-mono">
                      <Eff v={n(r.interaction_pct)}
                        title={`The cross term: your tilt multiplied by your selection edge.\n\nIt is the part that can't be assigned cleanly to either column — a big overweight and good picks in the same ${w} reinforce each other.`} />
                    </td>
                    <td className="px-2 py-1.5 text-right font-mono font-semibold"><Eff v={n(r.total_pct)} /></td>
                  </tr>
                ))}
              </tbody>
              <tfoot>
                <tr className="border-t border-neutral-800/40 font-semibold">
                  <td className="px-2 py-1.5 text-fg" colSpan={5}>Total (= the excess)</td>
                  <td className="px-2 py-1.5 text-right font-mono">
                    <Eff v={(data.rows ?? []).reduce((s, r) => s + n(r.allocation_pct), 0)} />
                  </td>
                  <td className="px-2 py-1.5 text-right font-mono">
                    <Eff v={(data.rows ?? []).reduce((s, r) => s + n(r.selection_pct), 0)} />
                  </td>
                  <td className="px-2 py-1.5 text-right font-mono">
                    <Eff v={(data.rows ?? []).reduce((s, r) => s + n(r.interaction_pct), 0)} />
                  </td>
                  <td className="px-2 py-1.5 text-right font-mono"><Eff v={data.attributed_pct} /></td>
                </tr>
              </tfoot>
            </table>
          </div>

          <div className="grid gap-4 md:grid-cols-3">
            <Names title="Biggest contributors" rows={data.top_contributors ?? []}
              hint="weight × return, in EUR" />
            <Names title="Biggest detractors" rows={data.top_detractors ?? []}
              hint="what cost you the most" />
            {/* The other half of "why" — and the half a holdings-only view can never show. */}
            <Names title={`${benchmark} winners you didn’t own`} rows={data.missed_winners ?? []}
              hint="matched by COMPANY, not ISIN — a share class is not a different business" />
          </div>
        </>
      )}
    </section>
  );
}
