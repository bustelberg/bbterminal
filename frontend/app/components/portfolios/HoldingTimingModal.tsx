'use client';

import { useEffect, useMemo, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { chartTheme } from '../../../lib/chartTheme';
import type { HoldingTiming } from '../../../lib/types/api';
import { TL, buildTimeline, shortDay } from './timingTimeline';

/**
 * WHY THE TRADING MATTERED — one holding's year, split into "doing nothing" and "each decision".
 *
 * ⚠ IT ANSWERS THE QUESTION THE TWO RETURN COLUMNS RAISE AND CANNOT SETTLE. `Return` is what the
 * INSTRUMENT did (AIRS's opening value restated to today's quantity, so timing is erased on
 * purpose); `On money invested` is what YOUR money did, and the gap between them IS the trading.
 * Neither says which trade, or by how much. This does:
 *
 *     buy & hold        what the position you held on 1 January would have made, untouched
 *     + each trade      what it added or cost against not having made it
 *     = actual          what the money really made
 *
 * ⚠ THE IDENTITY IS EXACT, AND SHOWN. Measured, residual 0.00 on every position tried. If it ever
 * fails, `reconciles` is false and the panel says these are three numbers rather than a
 * decomposition — the alternative is a reader trusting a sum that does not hold.
 *
 * ⚠ AGAINST DOING NOTHING, NOT AGAINST A PERFECT DECISION. A buy gains if the price rose after it;
 * a sell gains if it fell. A lucky call and a good one produce the same number, and nothing here
 * claims to tell them apart.
 */
export default function HoldingTimingModal({ portfolioId, name, onClose }: {
  portfolioId: number; name: string; onClose: () => void;
}) {
  const [d, setD] = useState<HoldingTiming | null>(null);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    let live = true;
    void (async () => {
      try {
        const r = await apiFetch(`${API_URL}/api/airs/model-portfolios/${portfolioId}`
          + `/holding-timing?name=${encodeURIComponent(name)}`);
        if (!r.ok) throw new Error(`${r.status} ${await r.text()}`);
        const j: HoldingTiming = await r.json();
        if (live) setD(j);
      } catch (e) {
        console.warn('[holding timing] failed', e);
        if (live) setErr('Could not load this holding’s trades — see the console.');
      }
    })();
    return () => { live = false; };
  }, [portfolioId, name]);

  const traded = (d?.trades ?? []).length;
  // ⚠ `--default-non-nullable=false` makes every Pydantic-defaulted field optional in TS. A
  // missing effect is 0 — it changed nothing — which is a fact rather than an unknown, so these
  // are safe to default and the alternative is `?? 0` scattered through the prose below.
  const timing = d?.timing_eur ?? 0;
  const buyHold = d?.buy_hold_eur ?? 0;
  const actual = d?.actual_eur ?? 0;
  const tl = useMemo(() => buildTimeline(d?.period_start, d?.period_end,
    d?.price_open_eur, d?.price_now_eur, d?.trades ?? []), [d]);

  return (
    <div className="fixed inset-0 z-[60] flex items-center justify-center p-4 bg-scrim/60"
      onClick={onClose}>
      <div className="bg-page border border-neutral-800/40 rounded-xl shadow-xl w-[46rem] max-w-full max-h-[85vh] overflow-auto p-5"
        onClick={(e) => e.stopPropagation()}>
        <div className="flex items-start justify-between gap-4 mb-3">
          <div>
            <h3 className="text-sm font-semibold text-fg-strong">{name}</h3>
            <p className="text-[11px] text-fg-faint">
              What the trading was worth — against having left the position alone.
            </p>
          </div>
          <button type="button" onClick={onClose}
            className="text-fg-faint hover:text-fg-strong text-lg leading-none px-1">×</button>
        </div>

        {!d && !err && <p className="text-xs text-fg-subtle">Loading…</p>}
        {err && <p className="text-xs text-neg-400">{err}</p>}
        {/* ⚠ A REFUSAL NAMES ITSELF. Four different things stop this working (no pairing, no
            transactions loaded, sold out, an unprovable deposit) and each has its own sentence —
            "no data" for all of them would send a reader to fix the wrong one. */}
        {d && !d.available && <p className="text-xs text-warn-500">{d.note}</p>}

        {d?.available && (
          <div className="space-y-4">
            {/* ── The decomposition. Three lines, and the third is the sum of the first two. */}
            <div className="rounded-lg border border-neutral-800/40 overflow-hidden">
              <table className="w-full text-xs">
                <tbody className="divide-y divide-neutral-800/20">
                  <tr>
                    <td className="px-3 py-2 text-fg-soft">
                      Doing nothing
                      <span className="ml-2 text-[10px] text-fg-faint">
                        holding the {num(d.qty_open)} share{d.qty_open === 1 ? '' : 's'} you had on
                        {' '}1 January, untouched — €{num2(d.price_open_eur)} → €{num2(d.price_now_eur)}
                      </span>
                    </td>
                    <td className={`px-3 py-2 text-right font-mono tabular-nums ${tone(d.buy_hold_eur)}`}>
                      {eur(d.buy_hold_eur)}
                    </td>
                    <td className={`px-3 py-2 text-right font-mono tabular-nums w-24 ${tone(d.buy_hold_pct)}`}>
                      {pct(d.buy_hold_pct)}
                    </td>
                  </tr>
                  <tr>
                    <td className="px-3 py-2 text-fg-soft">
                      What the trading changed
                      <span className="ml-2 text-[10px] text-fg-faint">
                        {traded === 0 ? 'no trades this year'
                          : `${traded} trade${traded === 1 ? '' : 's'}, itemised below`}
                      </span>
                    </td>
                    <td className={`px-3 py-2 text-right font-mono tabular-nums ${tone(d.timing_eur)}`}>
                      {eur(d.timing_eur)}
                    </td>
                    <td className={`px-3 py-2 text-right font-mono tabular-nums w-24 ${tone(d.timing_pp)}`}>
                      {pp(d.timing_pp)}
                    </td>
                  </tr>
                  <tr className="bg-overlay/[0.04] font-semibold">
                    <td className="px-3 py-2 text-fg-strong">
                      What the money actually made
                      {!!d.income_eur && (
                        <span className="ml-2 text-[10px] font-normal text-fg-faint">
                          plus {eur(d.income_eur)} of dividends, not in these lines
                        </span>
                      )}
                    </td>
                    <td className={`px-3 py-2 text-right font-mono tabular-nums ${tone(d.actual_eur)}`}>
                      {eur(d.actual_eur)}
                    </td>
                    <td className={`px-3 py-2 text-right font-mono tabular-nums w-24 ${tone(d.actual_pct)}`}>
                      {pct(d.actual_pct)}
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>

            {/* ⚠⚠ NAME THE BASE, ALWAYS. These percentages divide by the value of the position on
                1 January — which is NOT the denominator behind either return column in the table
                that opened this modal. KLA reads +54% there and +93% here, both correct, and a
                percent with no stated base is how a reader concludes one of them is broken. */}
            {d.open_value_eur != null ? (
              <p className="text-[10px] text-fg-faint">
                Percentages are of {eur(d.open_value_eur)} — what the {num(d.qty_open)} shares you
                held on 1 January were worth. One base for all three lines, which is what lets them
                add up. It is not the base behind the Holdings table’s two return columns, so these
                figures will differ from both.
              </p>
            ) : (
              /* qty_open = 0. Measured: AITopSelectie bought its whole KLA position on 5 January.
                 A 0% here would read as "the decisions did not matter" when they were everything. */
              <p className="text-[10px] text-fg-faint">
                No percentages: nothing was held when the year opened — the whole position was
                bought during it — so there is no starting value to be a percentage of. The euro
                figures are unaffected, and each trade still shows how far the price moved after it.
              </p>
            )}

            {/* ⚠ THE VERDICT IN A SENTENCE. A reader who has to subtract two numbers to learn
                whether the trading helped has been given data, not an answer. */}
            {traded > 0 && (
              <p className="text-[11px] text-fg-soft">
                {timing >= 0
                  ? <>Trading <span className="text-pos-400 font-medium">added {eur(timing)}</span>{' '}
                    against leaving the position alone.</>
                  : <>Trading <span className="text-neg-400 font-medium">cost {eur(Math.abs(timing))}</span>{' '}
                    against leaving the position alone.</>}
                {buyHold < 0 && actual >= 0
                  && ' Doing nothing would have lost money here; the trades turned it positive.'}
                {buyHold >= 0 && actual < 0
                  && ' Doing nothing would have made money here; the trades turned it negative.'}
              </p>
            )}

            {/* ── The year, with each decision on it.
                ⚠ The segments connect OBSERVATIONS, not the path the price took — see
                `timingTimeline.ts`. The caption says so, because a straight line from the sale to
                the repurchase makes a round trip look like a slide. */}
            {tl && (
              <div className="rounded-lg border border-neutral-800/40 bg-card px-2 pt-1 pb-2">
                <svg viewBox={`0 0 ${TL.width} ${TL.height}`} className="w-full h-auto"
                  role="img" aria-label={`Price and decisions for ${name} across the period`}>
                  {/* Axis rule under the plot. */}
                  <line x1={TL.padL} x2={TL.width - TL.padR}
                    y1={TL.height - TL.bottom + 12} y2={TL.height - TL.bottom + 12}
                    stroke={chartTheme.zeroLine} strokeWidth={1} />

                  {/* Stems: each decision dropped to the axis, and lifted to its label. */}
                  {tl.points.filter((p) => p.effect != null).map((p, i) => (
                    <line key={`s${i}`} x1={p.x} x2={p.x} y1={p.lane * TL.laneGap + 22}
                      y2={TL.height - TL.bottom + 12}
                      stroke={chartTheme.grid} strokeWidth={1} strokeDasharray="2 3" />
                  ))}

                  {/* The observed price line. */}
                  <path d={tl.path} fill="none" stroke={chartTheme.accentStrong}
                    strokeWidth={1.75} strokeLinejoin="round" strokeLinecap="round" />

                  {tl.points.map((p, i) => {
                    const isTrade = p.effect != null;
                    const fill = !isTrade ? chartTheme.universe
                      : p.kind === 'buy' ? chartTheme.accent : chartTheme.warn;
                    return (
                      <g key={i}>
                        <circle cx={p.x} cy={p.y} r={isTrade ? 5 : 3.5}
                          fill={fill} stroke="#ffffff" strokeWidth={isTrade ? 2 : 1.5} />
                        {isTrade && (
                          <text x={anchorX(p.x)} y={p.lane * TL.laneGap + 14}
                            textAnchor={anchor(p.x)} fontSize={9.5}>
                            <tspan fill={chartTheme.axisLabel}>
                              {p.kind === 'buy' ? 'Bought' : 'Sold'} {num(p.quantity)}
                              {p.rescaled ? '*' : ''}
                            </tspan>
                            <tspan fill={(p.effect ?? 0) >= 0 ? chartTheme.pos : chartTheme.neg}
                              fontWeight={600}>{'  '}{eur(p.effect)}</tspan>
                          </text>
                        )}
                        {isTrade && (
                          <text x={anchorX(p.x)} y={TL.height - TL.bottom + 26}
                            textAnchor={anchor(p.x)} fontSize={9} fill={chartTheme.axisTick}>
                            {shortDay(p.date)}
                          </text>
                        )}
                      </g>
                    );
                  })}

                  {/* The two ends of the window, with the price at each. */}
                  <text x={TL.padL} y={TL.height - 6} fontSize={9} fill={chartTheme.axisTick}>
                    1 Jan · €{num2(d.price_open_eur)}
                  </text>
                  <text x={TL.width - TL.padR} y={TL.height - 6} textAnchor="end"
                    fontSize={9} fill={chartTheme.axisTick}>
                    {shortDay(tl.end)} · €{num2(d.price_now_eur)}
                  </text>
                </svg>
                <p className="text-[9.5px] text-fg-faint px-1 leading-snug">
                  Each point is a price we hold — the opening value, what you actually traded at,
                  and today. The lines between them connect those observations; they are not the
                  path the price took.
                  {tl.points.some((p) => p.rescaled) && ' * converted to today’s share basis.'}
                  {tl.undated > 0 && ` ${tl.undated} trade${tl.undated === 1 ? '' : 's'} `
                    + 'could not be placed (AIRS gave no date) — see the table below.'}
                </p>
              </div>
            )}

            {/* ── Each decision. */}
            {traded > 0 && (
              <div className="rounded-lg border border-neutral-800/40 overflow-x-auto">
                <table className="w-full text-xs whitespace-nowrap">
                  <thead className="bg-card [&_th]:bg-card text-[10px] uppercase tracking-wide text-fg-faint">
                    <tr className="border-b border-neutral-800/40">
                      <th className="px-3 py-1.5 text-left font-medium">Date</th>
                      <th className="px-3 py-1.5 text-left font-medium">Decision</th>
                      <th className="px-3 py-1.5 text-right font-medium">Shares</th>
                      <th className="px-3 py-1.5 text-right font-medium">Price (€)</th>
                      <th className="px-3 py-1.5 text-right font-medium">Amount (€)</th>
                      <th className="px-3 py-1.5 text-right font-medium"
                        title="Against not having made this trade. A buy gains if the price rose after it; a sell gains if the price fell after it.">
                        Gained / cost
                      </th>
                      {/* ⚠ TWO NORMALISATIONS, BECAUSE THEY ANSWER DIFFERENT QUESTIONS AND A READER
                          given one will ask the other. "%" is how GOOD the call was per euro moved;
                          "pp" is how MUCH it mattered to the position. A brilliant call on 3 shares
                          scores a huge % and a pp of nothing — which is the honest reading. */}
                      <th className="px-3 py-1.5 text-right font-medium"
                        title="The effect per euro that changed hands — i.e. how far the price moved in your favour since this decision. A buy: what it has made since you bought. A sell: what it avoided since you sold. Says how good the call was, not whether it mattered.">
                        %
                      </th>
                      <th className="px-3 py-1.5 text-right font-medium"
                        title="Points of this position's year. The effect over what the position was worth on 1 January — the same base as the three lines above, which is why they all add up. Says how much the decision mattered.">
                        pp
                      </th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-neutral-800/20">
                    {(d.trades ?? []).map((t, i) => (
                      <tr key={i} className="hover:bg-overlay/[0.02]">
                        <td className="px-3 py-1.5 text-fg-muted">{t.datum ?? '—'}</td>
                        <td className="px-3 py-1.5 text-fg-soft">
                          {t.kind === 'buy' ? 'Bought' : 'Sold'}
                          {/* ⚠ A pre-split trade has been converted to today's share basis, and
                              the row says so — otherwise the share count and price look wrong
                              against the contract note. */}
                          {t.rescaled && (
                            <span className="ml-2 text-[9px] text-warn-500"
                              title={`Converted to today's share basis (${num2(d.split_ratio ?? 1)}:1 split) so it can be compared with the current price. The euro amount is unchanged.`}>
                              split-adjusted
                            </span>
                          )}
                        </td>
                        <td className="px-3 py-1.5 text-right font-mono tabular-nums text-fg-muted">{num(t.quantity)}</td>
                        <td className="px-3 py-1.5 text-right font-mono tabular-nums text-fg-muted">{num2(t.price_eur)}</td>
                        <td className="px-3 py-1.5 text-right font-mono tabular-nums text-fg-muted">{eur(t.amount_eur)}</td>
                        <td className={`px-3 py-1.5 text-right font-mono tabular-nums font-semibold ${tone(t.effect_eur)}`}>
                          {eur(t.effect_eur)}
                        </td>
                        <td className={`px-3 py-1.5 text-right font-mono tabular-nums ${tone(t.move_pct)}`}>
                          {pct(t.move_pct)}
                        </td>
                        <td className={`px-3 py-1.5 text-right font-mono tabular-nums ${tone(t.effect_pp)}`}>
                          {pp(t.effect_pp)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}

            <div className="text-[10px] space-y-1">
              {d.reconciles ? (
                <p className="text-pos-400">
                  ✓ The two lines add to the third exactly — this is a decomposition of the
                  result, not three figures beside each other.
                </p>
              ) : (
                <p className="text-warn-500">
                  ⚠ These lines do not add up ({eur(d.residual_eur)} out), so they are three
                  separate figures rather than a decomposition. Do not read the split as the cause.
                </p>
              )}
              {/* ⚠⚠ THE ONE NUMBER THAT WILL NOT MATCH THE TABLE BEHIND THIS MODAL, NAMED HERE
                  RATHER THAN LEFT TO BE DISCOVERED. AIRS restates the opening value to TODAY's
                  share count, so shares bought later are priced at January's price instead of what
                  was paid. Both are correct answers to different questions. */}
              {d.restatement_eur != null && Math.abs(d.restatement_eur) >= 1 && (
                <p className="text-fg-faint">
                  The Holdings table shows {eur(d.airs_result_eur)} for this position — {eur(Math.abs(d.restatement_eur))}
                  {(d.restatement_eur ?? 0) > 0 ? ' more' : ' less'} than the figure above. AIRS
                  prices the shares you bought later at January’s price rather than what you paid;
                  that difference is real and cancels against the cash line at book level.
                </p>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

const eur = (v?: number | null) =>
  (v == null ? '—'
    : `${v < 0 ? '−' : ''}€${Math.abs(v).toLocaleString('en-US', { maximumFractionDigits: 0 })}`);
const num = (v?: number | null) => (v == null ? '—' : v.toLocaleString('en-US', { maximumFractionDigits: 2 }));
const num2 = (v?: number | null) => (v == null ? '—' : v.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }));
/**
 * ⚠ A BLANK, NEVER A ZERO. Both of these are null exactly when the position had no opening value,
 * and "0.00pp" there would read as "this decision did not matter" when in fact it was the whole
 * result — see the AITopSelectie KLA case, bought outright on 5 January.
 */
const pct = (v?: number | null) => (v == null ? '—' : `${v >= 0 ? '+' : '−'}${Math.abs(v).toFixed(2)}%`);
const pp = (v?: number | null) => (v == null ? '—' : `${v >= 0 ? '+' : '−'}${Math.abs(v).toFixed(2)}pp`);

/** Keep a callout inside the frame — centred, unless that would push it off an edge. */
const EDGE = 62;
const anchor = (x: number): 'start' | 'middle' | 'end' =>
  (x < EDGE ? 'start' : x > TL.width - EDGE ? 'end' : 'middle');
const anchorX = (x: number) => Math.min(Math.max(x, TL.padL), TL.width - TL.padR);
const tone = (v?: number | null) => (v == null ? 'text-fg-faint' : v >= 0 ? 'text-pos-400' : 'text-neg-400');
