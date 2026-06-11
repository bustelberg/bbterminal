'use client';

import { useMemo } from 'react';

import type { BacktestResult, UniverseEntry } from '../../../lib/stores/momentum';
import { useCompanyExchangeMap, useCompanyIsinMap } from '../../../lib/hooks/apiData';
import DailyReturnsHistograms from './DailyReturnsHistograms';
import MonthlyReturnsHeatmap from './MonthlyReturnsHeatmap';
import EquityCurveCard from './EquityCurveCard';
import FeeWaterfallPanel from './FeeWaterfallPanel';
import MarketHealthCard from './MarketHealthCard';
import MonthlyHoldingsTable, { type ScoringConfig } from './MonthlyHoldingsTable';
import SectorTimelineChart from './SectorTimelineChart';
import type { SavedRun } from './types';

type Props = {
  /** The backtest result to render. */
  result: BacktestResult;
  /** Universe bundled with a saved result — drives the per-company
   * exchange lookup (fee parentheticals + exchange column in the holdings
   * table). Optional; without it those degrade gracefully (no fees, blank
   * exchange cells). */
  universe?: UniverseEntry[];
  /** Saved-run id when this result was loaded from disk — labels the
   * active series and disables that row in the comparison dropdown. */
  loadedRunId?: number | null;
  /** Full label for the active strategy row in the summary table. */
  activeStrategyLabel?: string;
  /** Scoring config for the holdings table's per-category score columns.
   * Defaults to empty weights when not supplied (columns still render
   * from each holding's `category_scores`). */
  scoringConfig?: ScoringConfig;
  /** Saved runs offered in EquityCurveCard's comparison dropdown. */
  savedRuns?: SavedRun[];
  /** Optional "go-live" date (YYYY-MM-DD) drawn as a red dashed marker on
   * the equity curve. /schedule passes the strategy's start_date here. */
  markerDate?: string;
  /** Render every card collapsed by default. /schedule's strategy detail
   * sets this so an expanded strategy stays compact; /backtest leaves it
   * false (cards expanded). */
  defaultCollapsed?: boolean;
};

/** The complete /backtest result view: the equity-curve card (log-scale
 * toggle, summary stats, yearly breakdown + alpha subplots, benchmark /
 * saved-run comparison), the sector timeline, and the per-month holdings
 * table. Extracted from `MomentumBacktester` so other surfaces — notably
 * /schedule's source-backtest detail — render an experience identical to
 * having just run the backtest on /backtest, with no duplication. */
export default function BacktestResultView({
  result,
  universe = [],
  loadedRunId = null,
  activeStrategyLabel,
  scoringConfig,
  savedRuns = [],
  markerDate,
  defaultCollapsed = false,
}: Props) {
  // Live company directory — the fallback exchange source. Saved backtests
  // often bundle an EMPTY `universe` payload (e.g. run 32), so without this
  // every exchange cell would be blank except HKSE-inferred tickers. Same
  // two-tier construction as MomentumBacktester.
  const companyExchangeMap = useCompanyExchangeMap();
  const isinByCompany = useCompanyIsinMap();

  // Per-company exchange map — same construction as MomentumBacktester:
  // the universe payload bundled with the result (skipping junk strings),
  // then the live company directory fills in anything the universe didn't
  // supply (so a fresh run still wins over the static directory).
  const exchangeByCompany = useMemo(() => {
    const m = new Map<number, string>();
    for (const u of universe) {
      const e = (u.exchange ?? '').trim();
      if (!e) continue;
      const upper = e.toUpperCase();
      if (upper === 'NONE' || upper === 'NAN' || upper === 'NULL') continue;
      m.set(u.company_id, e);
    }
    for (const [cid, exch] of companyExchangeMap) {
      if (!m.has(cid)) m.set(cid, exch);
    }
    return m;
  }, [universe, companyExchangeMap]);

  // Score categories (e.g. price, volume) derived from the holdings'
  // `category_scores` keys — avoids a /api/momentum/signals fetch and
  // stays correct for whatever categories the saved run actually carried.
  // Price sorts first to match /backtest's column order.
  const categories = useMemo(() => {
    const set = new Set<string>();
    for (const mr of result.monthly_records ?? []) {
      for (const h of mr.holdings ?? []) {
        for (const k of Object.keys(h.category_scores ?? {})) set.add(k);
      }
    }
    return Array.from(set).sort((a, b) =>
      a === 'price' ? -1 : b === 'price' ? 1 : a.localeCompare(b),
    );
  }, [result]);

  const resolvedScoring: ScoringConfig = scoringConfig ?? {
    universe_label: null,
    index_universe: null,
    signal_weights: {},
    category_weights: {},
  };

  return (
    <>
      <EquityCurveCard
        result={result}
        loadedRunId={loadedRunId}
        savedRuns={savedRuns}
        activeStrategyLabel={activeStrategyLabel}
        markerDate={markerDate}
        defaultCollapsed={defaultCollapsed}
      />
      <FeeWaterfallPanel result={result} defaultCollapsed={defaultCollapsed} />
      {/* Only renders when the regime filter was active (records carry a
          market_health score) — otherwise returns null. */}
      <MarketHealthCard result={result} defaultCollapsed={defaultCollapsed} />
      <SectorTimelineChart result={result} markerDate={markerDate} defaultCollapsed={defaultCollapsed} />
      <DailyReturnsHistograms result={result} defaultCollapsed={defaultCollapsed} />
      <MonthlyReturnsHeatmap result={result} defaultCollapsed={defaultCollapsed} />
      <MonthlyHoldingsTable
        result={result}
        categories={categories}
        exchangeByCompany={exchangeByCompany}
        isinByCompany={isinByCompany}
        scoringConfig={resolvedScoring}
        markerDate={markerDate}
        defaultCollapsed={defaultCollapsed}
      />
    </>
  );
}
