'use client';

/**
 * Portfolio per-member breakdown for chart tooltips. When a comparison side is
 * a portfolio, the chart receives each member's metrics (EUR-converted, from
 * `/api/earnings/portfolios/{id}/member-metrics`) and uses the SAME series
 * builder it uses for the blended line to compute each member's own value.
 * The tooltip then lists holdings ranked best→worst on that metric.
 */
import type { MetricRow } from './types';

export type PortfolioMemberMetrics = {
  company_id: number;
  ticker: string | null;
  name: string | null;
  weight: number;
  metrics: MetricRow[];
};

export type MemberSeries = { label: string; weight: number; values: { date: string; value: number }[] };

/** Build a per-member {date,value}[] using the chart's own series builder. */
export function buildMemberSeries(
  breakdown: PortfolioMemberMetrics[],
  buildOne: (m: MetricRow[]) => { date: string; value: number }[],
): MemberSeries[] {
  return breakdown.map((m) => ({
    label: m.ticker ?? m.name ?? String(m.company_id),
    weight: m.weight,
    values: buildOne(m.metrics),
  }));
}

/** Weighted-average line across members — the portfolio's value on a chart is
 * the weighted mean of what each holding shows, at every date any member has a
 * point (each member carried forward via as-of, weights renormalized to those
 * present). This is correct even for DERIVED series (PEG, ratios) where blending
 * raw components per exact date would be meaningless. */
export function weightedAverageSeries(members: MemberSeries[]): { date: string; value: number }[] {
  const dates = new Set<string>();
  for (const m of members) for (const p of m.values) dates.add(p.date);
  const out: { date: string; value: number }[] = [];
  for (const d of [...dates].sort()) {
    let wsum = 0, acc = 0;
    for (const m of members) {
      const w = m.weight || 0;
      if (w <= 0) continue;
      const v = asofValue(m.values, d);
      if (v == null) continue;
      acc += w * v;
      wsum += w;
    }
    if (wsum > 0) out.push({ date: d, value: acc / wsum });
  }
  return out;
}

/** Anchor (latest first-positive date, so every weighted holding exists there)
 * + each member's base value at that anchor. Null when no holding qualifies. */
function anchorAndBases(members: MemberSeries[]): { anchor: string; bases: (number | null)[] } | null {
  const eligible = members.filter((m) => (m.weight || 0) > 0 && m.values.some((p) => p.value > 0));
  if (eligible.length === 0) return null;
  const anchor = eligible.map((m) => m.values.find((p) => p.value > 0)!.date).reduce((a, b) => (a > b ? a : b));
  return { anchor, bases: members.map((m) => asofValue(m.values, anchor)) };
}

/** Weighted buy-and-hold return index (base 100) given a fixed anchor + per-
 * member bases — so several series (e.g. actual vs estimate) can share one
 * scale. Non-positive points and members without a positive base are skipped. */
function indexWithBases(members: MemberSeries[], anchor: string, bases: (number | null)[]): { date: string; value: number }[] {
  const dates = new Set<string>();
  for (let i = 0; i < members.length; i++) {
    const base = bases[i];
    if (base == null || base <= 0 || (members[i].weight || 0) <= 0) continue;
    for (const p of members[i].values) if (p.date >= anchor) dates.add(p.date);
  }
  const out: { date: string; value: number }[] = [];
  for (const d of [...dates].sort()) {
    let wsum = 0, acc = 0;
    for (let i = 0; i < members.length; i++) {
      const base = bases[i], w = members[i].weight || 0;
      if (base == null || base <= 0 || w <= 0) continue;
      const v = asofValue(members[i].values, d);
      if (v == null || v <= 0) continue;
      acc += w * (v / base);
      wsum += w;
    }
    if (wsum > 0) out.push({ date: d, value: (acc / wsum) * 100 });
  }
  return out;
}

/** Buy-and-hold weighted TOTAL-RETURN index (base 100) for a metric across the
 * holdings — each member normalized to a common anchor then weighted, so the
 * line is a true portfolio return rather than a level blend (which over-weights
 * higher-priced/higher-EPS holdings). Used by Share Price vs Owner Earnings. */
export function weightedReturnIndex(members: MemberSeries[]): { date: string; value: number }[] {
  const ab = anchorAndBases(members);
  return ab ? indexWithBases(members, ab.anchor, ab.bases) : [];
}

/** Two return indices that SHARE the primary's anchor + per-member bases, so
 * `secondary` (e.g. EPS estimates) is on the same scale as and continues
 * `primary` (e.g. actual EPS) instead of restarting at 100. */
export function weightedReturnIndexShared(
  primary: MemberSeries[],
  secondary: MemberSeries[],
): { primary: { date: string; value: number }[]; secondary: { date: string; value: number }[] } {
  const ab = anchorAndBases(primary);
  if (!ab) return { primary: [], secondary: [] };
  return {
    primary: indexWithBases(primary, ab.anchor, ab.bases),
    secondary: indexWithBases(secondary, ab.anchor, ab.bases),
  };
}

/** Latest value at or before `date` (values sorted ascending). */
export function asofValue(values: { date: string; value: number }[], date: string): number | null {
  let lo = 0, hi = values.length - 1, idx = -1;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    if (values[mid].date <= date) { idx = mid; lo = mid + 1; } else hi = mid - 1;
  }
  return idx >= 0 ? values[idx].value : null;
}

function rank(members: MemberSeries[], date: string, betterIsLower: boolean) {
  const rows = members
    .map((m) => ({ label: m.label, weight: m.weight, value: asofValue(m.values, date) }))
    .filter((r): r is { label: string; weight: number; value: number } => r.value != null);
  rows.sort((a, b) => (betterIsLower ? a.value - b.value : b.value - a.value));
  return rows;
}

/** Ranked member list rendered inside a chart tooltip. `betterIsLower` flips
 * the ordering so "best" matches the metric's good direction (e.g. low P/E,
 * high FCF). `color` tints the tickers to match the side's line. */
export function MemberRanking({
  date, members, format, betterIsLower, color,
}: {
  date: string;
  members: MemberSeries[];
  format: (v: number) => string;
  betterIsLower: boolean;
  color: string;
}) {
  const ranked = rank(members, date, betterIsLower);
  if (ranked.length === 0) return null;
  const wsum = ranked.reduce((s, r) => s + (r.weight || 0), 0) || 1;
  return (
    <div className="mt-1.5 pt-1.5 border-t border-neutral-800/40 space-y-0.5">
      <div className="text-[10px] text-fg-faint mb-0.5">By impact (best → worst)</div>
      {ranked.map((m, i) => (
        <div key={m.label} className="flex items-center gap-2 text-[11px] leading-tight">
          <span className="text-fg-faint w-3 text-right">{i + 1}</span>
          <span className="font-mono" style={{ color }}>{m.label}</span>
          <span className="text-fg-faint">{Math.round((m.weight / wsum) * 100)}%</span>
          <span className="ml-auto font-mono text-fg-soft">{format(m.value)}</span>
        </div>
      ))}
    </div>
  );
}
