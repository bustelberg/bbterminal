'use client';

/**
 * The per-company score breakdown, rendered — "why was this company picked on this day".
 *
 * Extracted from `TickerTimelineModal` (2026-07-31) so the /schedule "Daily holdings" table can
 * open the SAME view from a clicked row. ⚠ ONE RENDERER, DELIBERATELY: this shows the arithmetic
 * behind a selection — raw signal, universe min/max, normalised 0-100, weight, then the category
 * blend into the final score. A second copy is a second explanation of one number, and the two
 * would drift the first time a pillar is added (MomentumExtra's `trend` already arrives as an
 * extra category here, with no change to this code).
 *
 * Both components take the payload of `POST /api/momentum/signal-breakdown` verbatim. They compute
 * nothing beyond formatting — the normalisation, the weights and the score all come from the
 * engine that made the pick.
 */

import { guruFocusUrl } from './utils';

export type SignalComponent = { label: string; value_str?: string };
export type SignalBreakdown = {
  key: string;
  label: string;
  description: string;
  category: string;
  raw_value: number | null;
  components: SignalComponent[];
  universe_min: number | null;
  universe_max: number | null;
  // Who sits at each end of that range — the two companies whose values scale
  // everyone else's 0-100 on this signal. Absent on a signal with no usable
  // values (and on responses from a backend older than 2026-08-02).
  min_company?: ExtremeCompany | null;
  max_company?: ExtremeCompany | null;
  normalized_score: number | null;
  weight: number;
};

/** The company at one end of a signal's universe range. Ticker + exchange +
 * ISIN + a GuruFocus link, because a bare ticker is a hint and this needs to be
 * checkable — an unverifiable extreme is how a corrupted series sets the scale
 * for 1,478 other companies without anyone noticing. */
export type ExtremeCompany = {
  company_id: number;
  ticker?: string | null;
  exchange?: string | null;
  company_name?: string | null;
  isin?: string | null;
};
export type CategoryScore = {
  category: string;
  score: number | null;
  weight: number;
  contribution: number | null;
};
export type BreakdownData = {
  company_id: number;
  ticker: string;
  exchange: string;
  company_name: string;
  as_of_date: string;
  anchor_date: string;
  anchor_price: number;
  signals: SignalBreakdown[];
  category_scores: CategoryScore[];
  category_weights_normalized: Record<string, number>;
  momentum_score: number | null;
  universe_size: number;
  in_universe_at_cutoff: boolean;
  universe_label_used: string | null;
};

export function BreakdownView({ data }: { data: BreakdownData }) {
  const fmtRaw = (v: number | null) => (v == null ? '—' : Number.isInteger(v) ? `${v}` : `${v.toFixed(4)}`);
  return (
    <div className="space-y-3">
      <div className="text-[12px] text-fg-subtle">
        Anchor: latest close strictly before {data.as_of_date} ={' '}
        <span className="text-fg-soft font-mono">{data.anchor_price.toFixed(4)}</span> on{' '}
        <span className="text-fg-soft font-mono">{data.anchor_date}</span>{' '}
        · universe size at this cutoff:{' '}
        <span className="text-fg-soft font-mono">{data.universe_size}</span>
        {data.universe_label_used && (
          <span> · scoped to <span className="text-fg-soft font-mono">{data.universe_label_used}</span></span>
        )}
        {!data.in_universe_at_cutoff && (
          <span className="text-warn-400">
            {' '}· note: this company was not in the universe at this cutoff, so the displayed normalized scores may differ from the live selection
          </span>
        )}
      </div>

      {/* Per-signal breakdown */}
      <div className="space-y-2">
        {data.signals.map((s) => {
          const rangeStr = (s.universe_min != null && s.universe_max != null)
            ? `[${fmtRaw(s.universe_min)}, ${fmtRaw(s.universe_max)}]`
            : '—';
          return (
            <div key={s.key} className="rounded-md border border-neutral-800/60 px-3 py-2">
              <div className="flex items-baseline justify-between gap-3 flex-wrap">
                <div className="flex items-baseline gap-2">
                  <span className="text-fg text-xs font-medium">{s.label}</span>
                  <span className="text-[11px] text-fg-faint">·</span>
                  <span className="text-[11px] text-fg-subtle capitalize">{s.category}</span>
                  <span className="text-[11px] text-fg-faint">· weight</span>
                  <span className="text-[11px] text-fg-soft font-mono">{s.weight}</span>
                </div>
                <div className="text-[12px] flex items-center gap-3 font-mono">
                  <span>
                    <span className="text-fg-subtle">raw </span>
                    <span className="text-fg-bright">{fmtRaw(s.raw_value)}</span>
                  </span>
                  <span>
                    <span className="text-fg-subtle">norm </span>
                    <span className="text-accent-300">{s.normalized_score != null ? `${s.normalized_score.toFixed(1)}` : '—'}</span>
                    <span className="text-fg-faint"> /100</span>
                  </span>
                </div>
              </div>
              {/* ⚠ NAME THE TWO COMPANIES THAT SET THE SCALE. Normalisation is
                  (raw − min) / (max − min), so these two decide every other
                  company's 0-100 on this signal. An unnamed range is
                  unfalsifiable — "max 5221.78" reads as a fact about the market,
                  while "max 5221.78 — WLN" is a claim you can go and check.
                  (That is exactly how a 1-for-40 reverse split that our price
                  history never re-read was found.) */}
              <div className="text-[12px] text-fg-subtle mt-0.5 flex flex-wrap items-baseline gap-x-2">
                <span>Universe range: <span className="font-mono">{rangeStr}</span></span>
                {s.min_company && <ExtremeRef label="low" c={s.min_company} value={s.universe_min} />}
                {s.max_company && <ExtremeRef label="high" c={s.max_company} value={s.universe_max} />}
              </div>
              <NormStep s={s} />
              <ul className="mt-1.5 space-y-0.5">
                {s.components.map((c, idx) => (
                  <li key={idx} className="text-[12px] text-fg-muted flex gap-2">
                    <span className="text-fg-faint shrink-0">↳</span>
                    <span className="shrink-0">{c.label}</span>
                    {c.value_str && <span className="text-fg font-mono ml-auto">{c.value_str}</span>}
                  </li>
                ))}
              </ul>
            </div>
          );
        })}
      </div>

      {/* Per-category math: how each signal's normalized score rolls up into
          the category score. Backend's scoring engine does
            score_cat = Σ (signal_norm × signal_weight / Σ weights_in_cat)
          so the per-signal "share" within a category is just its weight as a
          fraction of the category's total weight. */}
      <CategoryMathBreakdown data={data} />

      {/* Final roll-up: per-category scores combined via the category weights. */}
      <div className="rounded-md border border-accent-500/20 bg-accent-500/[0.04] px-3 py-2">
        <div className="text-[12px] text-fg-muted mb-1.5">
          Final score · combine per-category scores via the category weights:
          <span className="font-mono"> Σ (category_score × category_weight) = momentum_score</span>.
        </div>
        <table className="w-full text-[12px]">
          <thead>
            <tr className="text-fg-faint">
              <th className="text-left font-medium">Category</th>
              <th className="text-right font-medium">Score (0–100)</th>
              <th className="text-right font-medium">Weight</th>
              <th className="text-right font-medium">Contribution = score × weight</th>
            </tr>
          </thead>
          <tbody>
            {data.category_scores.map((c) => (
              <tr key={c.category} className="border-t border-neutral-800/30">
                <td className="py-1 capitalize text-fg-soft">{c.category}</td>
                <td className="py-1 text-right font-mono text-fg-bright">{c.score != null ? c.score.toFixed(2) : '—'}</td>
                <td className="py-1 text-right font-mono text-fg-soft">{(c.weight * 100).toFixed(0)}%</td>
                <td className="py-1 text-right font-mono text-fg-bright">{c.contribution != null ? c.contribution.toFixed(2) : '—'}</td>
              </tr>
            ))}
            <tr className="border-t border-accent-500/30">
              <td className="py-1.5 text-fg font-medium">Final momentum_score</td>
              <td colSpan={2} />
              <td className="py-1.5 text-right font-mono text-accent-300 font-medium">
                {data.momentum_score != null ? data.momentum_score.toFixed(2) : '—'}
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  );
}

/** Inline arithmetic for the min-max normalization step. The scoring engine
 * does `norm = (raw − min) / (max − min)`, then × 100 and combines with the
 * weight share (see backend/momentum/scoring.py:_score_category). Showing
 * the substituted formula here lets the user verify the 0–100 figure rather
 * than trusting it.
 *
 * Three branches:
 *  - raw missing → backend reports None; selection treats it as 50.
 *  - min == max (no spread) → engine substitutes 50 for everyone.
 *  - normal → render the substituted formula and the result.
 */
/**
 * One end of a signal's universe range, named and checkable.
 *
 * ⚠ THESE TWO COMPANIES SET THE 0-100 SCALE FOR EVERY OTHER NAME
 * (`(raw − min) / (max − min)`), so a single corrupted series at an extreme
 * silently compresses the whole universe toward the middle. Worldline read
 * +1142% on a 1-for-40 reverse split our history never re-read; VERBUND's
 * volume-trend of 39,211,500 came from zero-volume phantom bars and flattened
 * every other company's volume score to ~0. Both were invisible until the range
 * named who was in it, and neither is checkable from a bare ticker — hence the
 * exchange, the ISIN and the link out.
 */
function ExtremeRef({ label, c, value }: {
  label: string; c: ExtremeCompany; value: number | null;
}) {
  const tic = c.ticker || c.company_name || `#${c.company_id}`;
  const href = c.ticker ? guruFocusUrl(c.ticker, c.exchange ?? '') : null;
  const title = [
    c.company_name,
    c.exchange ? `${c.exchange}:${c.ticker}` : c.ticker,
    c.isin ? `ISIN ${c.isin}` : null,
    value != null ? `${label} = ${value}` : null,
  ].filter(Boolean).join(' · ');
  return (
    <span className="text-fg-faint whitespace-nowrap" title={title}>
      · {label}{' '}
      {href ? (
        <a href={href} target="_blank" rel="noopener noreferrer"
          className="font-mono text-accent-400 hover:text-accent-300 hover:underline">{tic}</a>
      ) : (
        <span className="font-mono text-fg-muted">{tic}</span>
      )}
      {c.exchange && <span className="text-fg-faint"> ({c.exchange})</span>}
      {c.isin && <span className="font-mono text-fg-subtle"> {c.isin}</span>}
    </span>
  );
}

function NormStep({ s }: { s: SignalBreakdown }) {
  // 4-decimal default matches the rest of the breakdown modal so the
  // numbers in the formula line up visually with `raw` and `Universe range`.
  const fmt = (v: number | null) => (v == null ? '—' : Number.isInteger(v) ? `${v}` : `${v.toFixed(4)}`);
  // Wrap negatives in parens so subtraction reads cleanly: `0.045 − (−0.12)`
  // rather than the ambiguous `0.045 − -0.12`.
  const term = (v: number) => (v < 0 ? `(${fmt(v)})` : fmt(v));

  if (s.raw_value == null) {
    return (
      <div className="text-[12px] text-fg-subtle mt-0.5">
        Norm: raw value missing → defaults to <span className="text-fg-soft font-mono">50</span>{' '}in scoring
      </div>
    );
  }
  if (s.universe_min == null || s.universe_max == null) {
    return (
      <div className="text-[12px] text-fg-subtle mt-0.5">
        Norm: universe range unavailable → <span className="text-fg-soft font-mono">50</span>
      </div>
    );
  }
  if (s.universe_min === s.universe_max) {
    return (
      <div className="text-[12px] text-fg-subtle mt-0.5">
        Norm: range collapsed (every company has{' '}
        <span className="font-mono">{fmt(s.universe_min)}</span>) → <span className="text-fg-soft font-mono">50</span>
      </div>
    );
  }
  return (
    <div className="text-[12px] text-fg-subtle mt-0.5 font-mono break-words">
      Norm = (raw − min) / (max − min) × 100 ={' '}
      <span className="text-fg-soft">
        ({term(s.raw_value)} − {term(s.universe_min)}) / ({term(s.universe_max)} − {term(s.universe_min)}) × 100
      </span>{' '}
      = <span className="text-accent-300">{s.normalized_score != null ? s.normalized_score.toFixed(1) : '—'}</span>
    </div>
  );
}


/** Shows per-category arithmetic: for each category, the active signals,
 * their weight share within the category, normalized 0-100 score, and
 * each signal's contribution to the category score. The sum of
 * contributions equals the category score the scoring engine produced. */
export function CategoryMathBreakdown({ data }: { data: BreakdownData }) {
  // Group signals by category; only count signals whose weight > 0 — those
  // are the "active" ones the scoring engine includes (zero-weight signals
  // are filtered out in `_score_category`).
  const byCategory = new Map<string, SignalBreakdown[]>();
  for (const s of data.signals) {
    if (s.weight <= 0) continue;
    const arr = byCategory.get(s.category) ?? [];
    arr.push(s);
    byCategory.set(s.category, arr);
  }

  if (byCategory.size === 0) return null;

  return (
    <div className="space-y-2">
      <div className="text-[12px] text-fg-muted">
        Category math · each signal&apos;s weight share within its category × its 0–100 normalized score = contribution; the sum is the category score the scoring engine produced.
      </div>
      {Array.from(byCategory.entries()).map(([cat, sigs]) => {
        const totalWeight = sigs.reduce((s, x) => s + x.weight, 0);
        // The category score from the scoring engine, for the cross-check footer.
        const cs = data.category_scores.find((c) => c.category === cat);
        const checkSum = sigs.reduce((acc, s) => {
          if (s.normalized_score == null || totalWeight <= 0) return acc;
          return acc + (s.normalized_score * s.weight) / totalWeight;
        }, 0);
        return (
          <div key={cat} className="rounded-md border border-neutral-800/60 px-3 py-2">
            <div className="flex items-baseline justify-between mb-1.5">
              <div>
                <span className="text-fg text-xs font-medium capitalize">{cat}</span>
                <span className="text-[11px] text-fg-subtle ml-2">category</span>
              </div>
              <div className="text-[12px]">
                <span className="text-fg-subtle">score </span>
                <span className="text-accent-300 font-mono">
                  {cs?.score != null ? cs.score.toFixed(2) : '—'}
                </span>
                <span className="text-fg-faint"> /100</span>
              </div>
            </div>
            <table className="w-full text-[12px]">
              <thead>
                <tr className="text-fg-faint">
                  <th className="text-left font-medium">Signal</th>
                  <th className="text-right font-medium">Normalized</th>
                  <th className="text-right font-medium">Weight</th>
                  <th className="text-right font-medium">Share</th>
                  <th className="text-right font-medium">Contribution</th>
                </tr>
              </thead>
              <tbody>
                {sigs.map((s) => {
                  const share = totalWeight > 0 ? s.weight / totalWeight : 0;
                  const contribution = s.normalized_score != null ? s.normalized_score * share : null;
                  return (
                    <tr key={s.key} className="border-t border-neutral-800/30">
                      <td className="py-1 text-fg-soft">{s.label}</td>
                      <td className="py-1 text-right font-mono text-fg-bright">
                        {s.normalized_score != null ? s.normalized_score.toFixed(2) : '—'}
                      </td>
                      <td className="py-1 text-right font-mono text-fg-soft">{s.weight}</td>
                      <td className="py-1 text-right font-mono text-fg-soft">{(share * 100).toFixed(1)}%</td>
                      <td className="py-1 text-right font-mono text-fg-bright">
                        {contribution != null ? contribution.toFixed(2) : '—'}
                      </td>
                    </tr>
                  );
                })}
                <tr className="border-t border-neutral-700/50">
                  <td className="py-1 text-fg-subtle italic">Σ (share × normalized)</td>
                  <td colSpan={3} />
                  <td className="py-1 text-right font-mono text-accent-300">
                    {Number.isFinite(checkSum) ? checkSum.toFixed(2) : '—'}
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        );
      })}
    </div>
  );
}
