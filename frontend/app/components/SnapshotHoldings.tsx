'use client';

import { useEffect, useMemo, useState } from 'react';
import MonthlyHoldingsTable from './momentum/MonthlyHoldingsTable';
import type { BacktestResult, Holding, PeriodRecord, Summary } from '../../lib/stores/momentum';

const API_URL = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000';

type SnapshotResponse = {
  snapshot_id: number;
  as_of_date: string;
  latest_price_date: string | null;
  config: Record<string, unknown> | null;
  holdings: Holding[];
  daily_picks: unknown[];
};

/** Loads a current-picks snapshot and renders its holdings via
 * MonthlyHoldingsTable. Used by both /schedule's per-run holdings
 * section and the per-strategy run-history detail. */
export default function SnapshotHoldings({ snapshotId }: { snapshotId: number }) {
  const [snapshot, setSnapshot] = useState<SnapshotResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect
    setLoading(true);
    fetch(`${API_URL}/api/momentum/current-picks/${snapshotId}`)
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`))))
      .then((data: SnapshotResponse) => {
        setSnapshot(data);
        setError(null);
      })
      .catch((e) => setError(e instanceof Error ? e.message : String(e)))
      .finally(() => setLoading(false));
  }, [snapshotId]);

  const { result, categories, scoringConfig } = useMemo(() => {
    if (!snapshot) {
      return {
        result: null as BacktestResult | null,
        categories: [] as string[],
        scoringConfig: null as {
          universe_label: string | null;
          index_universe: string | null;
          signal_weights: Record<string, number>;
          category_weights: Record<string, number>;
        } | null,
      };
    }
    const cfg = (snapshot.config ?? {}) as Record<string, unknown>;
    const signalWeights = (cfg.signal_weights as Record<string, number>) ?? {};
    const categoryWeights = (cfg.category_weights as Record<string, number>) ?? {};
    const cats = Object.keys(categoryWeights);
    const fallbackCats = cats.length > 0 ? cats : ['price', 'volume'];

    const holdings = (snapshot.holdings ?? []) as Holding[];
    const validReturns = holdings
      .map((h) => h.forward_return_pct)
      .filter((v): v is number => v != null && Number.isFinite(v));
    const meanReturn = validReturns.length > 0
      ? validReturns.reduce((a, b) => a + b, 0) / validReturns.length
      : null;

    const period: PeriodRecord = {
      date: snapshot.as_of_date,
      holdings,
      portfolio_return_pct: meanReturn,
      cumulative_return_pct: meanReturn ?? 0,
      is_open: true,
      as_of_date: snapshot.latest_price_date ?? undefined,
    };
    const summary: Summary = {
      total_return_pct: meanReturn ?? 0,
      annualized_return_pct: 0,
      max_drawdown_pct: 0,
      sharpe_ratio: null,
      avg_monthly_turnover_pct: 0,
      total_months: 1,
      avg_holdings: holdings.length,
      top_drawdowns: [],
    };
    const res: BacktestResult = {
      monthly_records: [period],
      summary,
      daily_records: [],
    };
    return {
      result: res,
      categories: fallbackCats,
      scoringConfig: {
        universe_label: (cfg.universe_label as string | null) ?? null,
        index_universe: (cfg.index_universe as string | null) ?? null,
        signal_weights: signalWeights,
        category_weights: categoryWeights,
      },
    };
  }, [snapshot]);

  if (loading) return <div className="text-xs text-gray-500">Loading snapshot…</div>;
  if (error) return <div className="text-xs text-rose-300">Failed to load snapshot: {error}</div>;
  if (!result || !scoringConfig) return <div className="text-xs text-gray-500">No data.</div>;

  return (
    <MonthlyHoldingsTable
      result={result}
      categories={categories}
      exchangeByCompany={new Map()}
      scoringConfig={scoringConfig}
    />
  );
}
