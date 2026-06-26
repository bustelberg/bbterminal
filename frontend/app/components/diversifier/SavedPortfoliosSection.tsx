'use client';

import { dialog } from '../../../lib/dialog';
import type { PortfolioStateResponse, SavedPortfolio } from '../../../lib/types/api';

const pct = (v: number | null | undefined) =>
  v == null ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(2)}%`;
const toneOf = (v: number | null | undefined) =>
  v == null ? 'text-fg-subtle' : v >= 0 ? 'text-pos-400' : 'text-neg-400';

/** Lists saved diversified portfolios and, for the one being viewed, shows its
 * live state: current drifted weights vs targets + whether a rebalance is due.
 * State is fetched on demand (no pipeline/cron) — `as of` the latest data. */
export default function SavedPortfoliosSection({
  portfolios, state, onView, onDelete, title = 'Saved portfolios',
}: {
  portfolios: SavedPortfolio[];
  state: PortfolioStateResponse | null;
  onView: (id: number) => void;
  onDelete: (id: number) => void;
  title?: string;
}) {
  if (portfolios.length === 0) return null;

  return (
    <div className="bg-card rounded-xl border border-neutral-800/40 p-5">
      <h3 className="text-fg-muted text-xs font-medium uppercase tracking-wider mb-3">{title}</h3>
      <div className="space-y-1.5">
        {portfolios.map((p) => {
          const isOpen = state?.id === p.id;
          return (
            <div key={p.id} className="border-b border-neutral-800/40 last:border-0">
              <div className="flex items-center gap-3 py-2">
                <div className="flex-1 min-w-0">
                  <span className="text-fg-strong text-sm font-medium">{p.name}</span>
                  {p.strategy_name && <span className="text-fg-subtle text-xs ml-2 truncate">over {p.strategy_name}</span>}
                </div>
                <button onClick={() => onView(p.id)} className="text-xs font-medium text-accent-400 hover:text-accent-500 transition-colors">
                  {isOpen ? 'Refresh state' : 'View state'}
                </button>
                <button
                  onClick={async () => {
                    if (await dialog.confirm(`Delete portfolio "${p.name}"?`, { destructive: true, confirmLabel: 'Delete' })) onDelete(p.id);
                  }}
                  className="text-xs text-fg-muted hover:text-neg-400 transition-colors"
                >
                  Delete
                </button>
              </div>

              {isOpen && state && (
                <div className="pb-3 pl-1">
                  <div className="flex items-center gap-3 mb-2 text-xs flex-wrap">
                    {state.rebalance_needed ? (
                      <span className="px-2 py-0.5 rounded-md bg-warn-500/15 border border-warn-500/40 text-warn-300 font-medium">Rebalance due</span>
                    ) : (
                      <span className="px-2 py-0.5 rounded-md bg-pos-500/15 border border-pos-500/40 text-pos-300 font-medium">Within bands</span>
                    )}
                    <span className="text-fg-subtle">as of {state.as_of ?? '—'}</span>
                    {state.last_rebalance && <span className="text-fg-subtle">· last rebalance {state.last_rebalance}</span>}
                  </div>

                  {/* Blended-portfolio performance (same anchoring as a scheduled strategy). */}
                  <div className="flex gap-5 mb-3 text-sm flex-wrap">
                    <span>MTD <span className={`font-mono ${toneOf(state.mtd_return_pct)}`}>{pct(state.mtd_return_pct)}</span></span>
                    <span>YTD <span className={`font-mono ${toneOf(state.ytd_return_pct)}`}>{pct(state.ytd_return_pct)}</span></span>
                    <span>
                      Since inception <span className={`font-mono ${toneOf(state.since_inception_pct)}`}>{pct(state.since_inception_pct)}</span>
                      {state.inception_date && <span className="text-fg-subtle text-xs"> (since {state.inception_date})</span>}
                    </span>
                  </div>
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="text-fg-subtle text-xs border-b border-neutral-800/60">
                        <th className="text-left font-medium py-1.5 pr-2">Holding</th>
                        <th className="text-right font-medium py-1.5 px-2">Target</th>
                        <th className="text-right font-medium py-1.5 px-2">Current</th>
                        <th className="text-right font-medium py-1.5 px-2">Band</th>
                        <th className="text-right font-medium py-1.5 px-2" title="The sleeve's own return over the since-inception window — cross-check against its price move">Return ↗</th>
                      </tr>
                    </thead>
                    <tbody>
                      {state.holdings.map((h) => (
                        <tr key={h.label} className="border-b border-neutral-800/30">
                          <td className="py-1.5 pr-2">
                            <span className={`font-mono ${h.group === 'strategy' ? 'text-accent-400 font-medium' : 'text-fg-strong'}`}>{h.label}</span>
                            {h.name && <span className="text-fg-subtle text-xs ml-2">{h.name}</span>}
                          </td>
                          <td className="py-1.5 px-2 text-right font-mono text-fg-muted">{h.target_pct.toFixed(0)}%</td>
                          <td className={`py-1.5 px-2 text-right font-mono ${h.breached ? 'text-warn-400 font-medium' : 'text-fg-strong'}`}>
                            {h.current_pct.toFixed(1)}%{h.breached ? ' ⚠' : ''}
                          </td>
                          <td className="py-1.5 px-2 text-right font-mono text-fg-subtle">±{h.band_pct.toFixed(0)}%</td>
                          <td className={`py-1.5 px-2 text-right font-mono ${toneOf(h.return_since_inception_pct)}`}>{pct(h.return_since_inception_pct)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                  <p className="text-xs text-fg-subtle mt-2">
                    Weights drifted from the last rebalance through {state.as_of ?? 'the latest data'}; a holding is flagged ⚠ when outside its band.
                    <strong className="text-fg-soft"> Return ↗</strong> is each sleeve&apos;s own gain over the since-inception window — cross-check it against the strategy / ETF&apos;s actual price move to verify the prices used.
                  </p>
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
