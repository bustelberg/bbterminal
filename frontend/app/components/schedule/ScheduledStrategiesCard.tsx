'use client';

import LoadingDots from '../LoadingDots';
import ScheduledStrategyDetail from '../ScheduledStrategyDetail';
import { fmtTimestamp } from '../../../lib/format';
import { strategyChips, chipStyle } from './utils';
import type { UseScheduledStrategiesResult } from './useScheduledStrategies';

/** "Scheduled strategies" card — the expandable list of saved strategies
 * the pipeline keeps up to date. All state + mutations live in
 * `useScheduledStrategies`; the parent owns the hook (so it can render the
 * page-level error banner) and threads the result down here. */
export default function ScheduledStrategiesCard({ sched }: { sched: UseScheduledStrategiesResult }) {
  const {
    strategies,
    strategiesLoading,
    expandedStrategyId,
    setExpandedStrategyId,
    historyCache,
    cacheRunHistory,
    loadStrategies,
    renameStrategy,
    removeStrategy,
    removeAllStrategies,
  } = sched;

  return (
    <div className="bg-card rounded-xl border border-neutral-800/40">
      <div className="px-5 py-3 border-b border-neutral-800/40 flex items-center justify-between">
        <h3 className="text-sm font-medium text-fg-strong">Scheduled strategies</h3>
        <div className="flex items-center gap-2 shrink-0">
          {strategies.length > 0 && (
            <button
              type="button"
              onClick={() => void removeAllStrategies()}
              className="text-xs px-3 py-1.5 rounded-lg text-neg-300 hover:bg-neg-500/10 transition-colors"
              title="Delete every scheduled strategy (snapshots stay)"
            >
              Remove all
            </button>
          )}
        </div>
      </div>

      {strategiesLoading ? (
        <div className="px-5 py-5 text-sm text-fg-subtle"><LoadingDots label="Loading" /></div>
      ) : strategies.length === 0 ? (
        <div className="px-5 py-6 text-sm text-fg-subtle">
          No strategies scheduled yet. Strategies must originate from a backtested variant: run a sweep on <a href="/backtest" className="text-accent-300 hover:text-accent-200 underline">/backtest</a>, then click <span className="text-fg-soft">+ Schedule</span> on any OK variant row in the Variants table.
        </div>
      ) : (
        <div className="divide-y divide-neutral-800/30">
          {strategies.map((s) => {
            const isExpanded = expandedStrategyId === s.id;
            return (
              <div key={s.id}>
                <div className="px-5 py-3 flex items-center gap-3 hover:bg-overlay/[0.02] transition-colors">
                  <button
                    type="button"
                    onClick={() => setExpandedStrategyId(isExpanded ? null : s.id)}
                    className="flex items-center gap-3 flex-1 min-w-0 text-left"
                  >
                    <span className="text-fg-subtle font-mono text-xs w-4 shrink-0">{isExpanded ? '▾' : '▸'}</span>
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-1.5 flex-wrap">
                        <span className={`text-sm font-semibold ${s.enabled ? 'text-fg-strong' : 'text-fg-subtle'}`}>
                          {s.name || 'MomentumTopSelectie'}
                        </span>
                        {!s.enabled && (
                          <span className="text-[10px] uppercase tracking-wider px-1.5 py-0.5 rounded border bg-neutral-500/10 text-fg-muted border-neutral-500/30">
                            paused
                          </span>
                        )}
                        {/* Property chips — each colour encodes which
                            property it is (frequency, direction, universe,
                            grouping, sizing, …). Derived from the config. */}
                        {strategyChips(s.config, s.frequency).map((c) => (
                          <span
                            key={c.text}
                            className="text-[10px] px-1.5 py-0.5 rounded-full border font-medium"
                            style={chipStyle(c.hue)}
                          >
                            {c.text}
                          </span>
                        ))}
                      </div>
                      <div className="text-xs text-fg-faint mt-1 font-mono">
                        {s.next_due_at
                          ? <>next rebalance {fmtTimestamp(s.next_due_at)}</>
                          : <span className="text-fg-subtle">not scheduled</span>}
                      </div>
                      {s.last_snapshot && (
                        <div className="text-xs text-fg-subtle mt-0.5 font-mono flex flex-wrap items-center gap-x-2 gap-y-1">
                          {s.last_snapshot.sectors.length > 0 && (
                            <span className="text-fg-muted">
                              {s.last_snapshot.sectors
                                .map((sec) => `${sec.sector} ×${sec.count}`)
                                .join(' · ')}
                            </span>
                          )}
                          {(s.last_snapshot.mtd_return_pct != null
                            || s.last_snapshot.ytd_return_pct != null) && (
                            <span className="text-fg-faint">|</span>
                          )}
                          {s.last_snapshot.mtd_return_pct != null && (
                            <span>
                              <span className="text-fg-subtle">MTD </span>
                              <span className={s.last_snapshot.mtd_return_pct >= 0 ? 'text-pos-400' : 'text-neg-400'}>
                                {s.last_snapshot.mtd_return_pct >= 0 ? '+' : ''}
                                {s.last_snapshot.mtd_return_pct.toFixed(2)}%
                              </span>
                            </span>
                          )}
                          {s.last_snapshot.ytd_return_pct != null && (
                            <span>
                              <span className="text-fg-subtle">YTD </span>
                              <span className={s.last_snapshot.ytd_return_pct >= 0 ? 'text-pos-400' : 'text-neg-400'}>
                                {s.last_snapshot.ytd_return_pct >= 0 ? '+' : ''}
                                {s.last_snapshot.ytd_return_pct.toFixed(2)}%
                              </span>
                            </span>
                          )}
                          {s.last_snapshot.as_of_date && (
                            <span className="text-fg-faint">
                              (as of {s.last_snapshot.as_of_date})
                            </span>
                          )}
                        </div>
                      )}
                    </div>
                  </button>
                  <button
                    type="button"
                    onClick={() => void renameStrategy(s.id, s.name || `Strategy #${s.id}`)}
                    className="text-xs px-2 py-1 rounded-lg text-fg-muted hover:bg-overlay/5 hover:text-fg transition-colors"
                    title="Rename strategy"
                  >
                    Rename
                  </button>
                  <button
                    type="button"
                    onClick={() => void removeStrategy(s.id)}
                    className="text-xs px-2 py-1 rounded-lg text-neg-300 hover:bg-neg-500/10 transition-colors"
                    title="Remove from schedule"
                  >
                    ×
                  </button>
                </div>
                {isExpanded && (
                  <ScheduledStrategyDetail
                    strategyId={s.id}
                    initialData={historyCache.get(s.id) ?? null}
                    onLoaded={(d) => cacheRunHistory(s.id, d)}
                    onMutated={() => void loadStrategies()}
                  />
                )}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
