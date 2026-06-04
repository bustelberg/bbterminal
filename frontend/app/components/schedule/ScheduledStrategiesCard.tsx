'use client';

import LoadingDots from '../LoadingDots';
import ScheduledStrategyDetail from '../ScheduledStrategyDetail';
import { fmtTimestamp } from '../../../lib/format';
import { WEEKDAY_LABELS } from '../momentum/utils';
import { strategySummary } from './utils';
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
    toggleStrategy,
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
          <a
            href="/backtest"
            className="text-xs px-3 py-1.5 rounded-lg bg-accent-600 hover:bg-accent-500 text-fg-strong transition-colors"
            title="Strategies can only be scheduled from a backtested variant — run a sweep on /backtest, then use the '+ Schedule' button on any OK variant row."
          >
            Add via /backtest →
          </a>
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
                      <div className="flex items-center gap-2 flex-wrap">
                        <span className={`text-sm font-medium truncate ${s.enabled ? 'text-fg-strong' : 'text-fg-subtle'}`}>
                          {s.name || `Strategy #${s.id}`}
                        </span>
                        {s.frequency && (
                          <span className="text-[10px] uppercase tracking-wider px-1.5 py-0.5 rounded border bg-accent-500/10 text-accent-300 border-accent-500/30">
                            {s.frequency}
                          </span>
                        )}
                        {!s.enabled && (
                          <span className="text-[10px] uppercase tracking-wider px-1.5 py-0.5 rounded border bg-neutral-500/10 text-fg-muted border-neutral-500/30">
                            paused
                          </span>
                        )}
                      </div>
                      <div className="text-xs text-fg-subtle mt-0.5 font-mono">
                        {strategySummary(s.config)}
                        {s.last_run_at ? (
                          <span className="text-fg-faint">
                            {' · '}last run {fmtTimestamp(s.last_run_at)}
                          </span>
                        ) : (
                          <span className="text-fg-faint">{' · '}not run yet</span>
                        )}
                        {s.next_due_at && (
                          <span className="text-fg-faint">
                            {' · '}next {fmtTimestamp(s.next_due_at)}
                          </span>
                        )}
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
                  {/* Rebalance day is fixed at schedule time from the
                      variant — read-only here (no post-hoc edits). */}
                  <span
                    className="flex items-center gap-1.5 text-xs text-fg-muted shrink-0"
                    title="Weekday each rebalance period enters on (first <day> of the period). Set from the scheduled variant — not editable."
                  >
                    <span className="text-fg-subtle">rebal</span>
                    <span className="text-fg-soft font-mono">
                      {WEEKDAY_LABELS[(s.config.rebalance_weekday as number | undefined) ?? 0]?.slice(0, 3) ?? 'Mon'}
                    </span>
                  </span>
                  <label className="flex items-center gap-1.5 text-xs text-fg-muted cursor-pointer">
                    <input
                      type="checkbox"
                      checked={s.enabled}
                      onChange={(e) => void toggleStrategy(s.id, e.target.checked)}
                      className="accent-accent-500"
                    />
                    enabled
                  </label>
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
