'use client';

import type { ModelPortfolioAnalysis } from '../../../lib/types/api';
import { useAnalyseCopy } from './analyseCopy';

type BookHolding = NonNullable<ModelPortfolioAnalysis['book_holdings']>[number];

/** WHERE the position came from — and, when there is more than one way in, HOW MUCH came each way.
 *
 *  In Analyse, the percentages appear only on a split row. Names alone cannot tell a
 *  position held entirely through a certificate from one that is 96% the book's own shares:
 *  MasterCard is €50,489 held outright against €1,991 through Star, and chipped only "Star" it
 *  read as a holding the book does not own. On a single-route row the split would restate the
 *  Weight column beside it, so it is left off — 49 of this book's 52 rows say just "direct".
 *
 *  The percentages are shares of the BOOK and add up to that row's Weight. The share of the ROW
 *  ("3.8% of this position") answers a different question and ties to nothing else on screen, so
 *  it rides in the tooltip. Performance supplies dated selection weights explicitly. */
export default function ViaChips({ names, sources, weights, weightTitle }: {
  names: string[];
  sources?: BookHolding['sources'];
  weights?: { label: string; weight_pct: number | null }[];
  weightTitle?: string;
}) {
  const copy = useAnalyseCopy();
  const routes = sources ?? [];
  const rowTotal = routes.reduce((s, r) => s + (r.weight_now_pct ?? 0), 0);
  const title = routes.length
    ? routes.map((r) => copy.row.routeTitle(r.label ?? copy.row.heldDirectly, r.weight_now_pct.toFixed(2),
      `€${Math.round(r.value_eur).toLocaleString('en-US')}`,
      rowTotal > 0 ? (100 * r.weight_now_pct / rowTotal).toFixed(1) : undefined))
      .join('\n')
    : names.join(' · ');

  const weightedRoutes = weights ?? (routes.length > 1
    ? routes.map((r) => ({ label: r.label, weight_pct: r.weight_now_pct })) : []);
  if (weightedRoutes.length) {
    return (
      <span className="flex flex-wrap items-center gap-1" title={weightTitle ?? title}>
        {weightedRoutes.map((r) => (
          <span key={r.label ?? '__direct'}
            className={`px-1.5 py-0.5 rounded-md text-[11px] whitespace-nowrap flex items-baseline gap-1 ${
              r.label ? 'bg-accent-500/10 text-accent-400' : 'bg-overlay/5 text-fg-muted'}`}>
            <span className="max-w-[9rem] truncate">{r.label ?? copy.holdings.direct}</span>
            <span className="font-mono opacity-80">{r.weight_pct == null ? '—' : `${r.weight_pct.toFixed(2)}%`}</span>
          </span>
        ))}
      </span>
    );
  }

  if (!names.length) return <span className="text-[11px] text-fg-faint" title={title || undefined}>{copy.holdings.direct}</span>;
  const shown = names.slice(0, 2);
  return (
    <span className="flex flex-wrap items-center gap-1" title={title}>
      {shown.map((n) => (
        <span key={n}
          className="px-1.5 py-0.5 rounded-md bg-accent-500/10 text-accent-400 text-[11px] whitespace-nowrap max-w-[11rem] truncate">
          {n}
        </span>
      ))}
      {names.length > shown.length && (
        <span className="text-[11px] text-fg-faint">+{names.length - shown.length}</span>
      )}
    </span>
  );
}

