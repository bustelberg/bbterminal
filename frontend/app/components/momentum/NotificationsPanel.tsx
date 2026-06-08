'use client';

import type { WarningEntry, InfoEntry } from '../../../lib/stores/momentum';

/**
 * The collapsible "N warnings / N notes" panel that lives between the
 * config and the backtest results on /backtest. Two stacked accordions:
 * warnings (amber) on top, notes (sky-blue) below. Both render the
 * scope tag + message and cap at 64rem before scrolling.
 *
 * Renders nothing when both lists are empty — the panel only appears
 * when there's actually something to show.
 */
export default function NotificationsPanel({
  warnings,
  infos,
  showWarnings,
  showInfos,
  onToggleWarnings,
  onToggleInfos,
}: {
  warnings: WarningEntry[];
  infos: InfoEntry[];
  showWarnings: boolean;
  showInfos: boolean;
  onToggleWarnings: () => void;
  onToggleInfos: () => void;
}) {
  if (warnings.length === 0 && infos.length === 0) return null;

  return (
    <div className="bg-card border border-neutral-800/40 rounded-lg overflow-hidden divide-y divide-neutral-800/40">
      {warnings.length > 0 && (
        <div className="bg-warn-500/10">
          <button
            type="button"
            onClick={onToggleWarnings}
            className="w-full flex items-center justify-between px-4 py-2.5 text-left hover:bg-warn-500/5 transition-colors"
          >
            <span className="text-warn-300 text-sm font-medium">
              {warnings.length} warning{warnings.length === 1 ? '' : 's'}
            </span>
            <span className="text-warn-400/70 text-xs font-mono">{showWarnings ? '▾' : '▸'}</span>
          </button>
          {showWarnings && (
            <ul className="max-h-64 overflow-auto border-t border-warn-500/20 divide-y divide-warn-500/10">
              {warnings.map((w, i) => (
                <li key={i} className="px-4 py-2 text-xs text-warn-200 flex gap-2">
                  <span className="uppercase text-[10px] tracking-wider font-mono text-warn-400/70 shrink-0 w-16">
                    {w.scope}
                  </span>
                  <span className="break-words">{w.message}</span>
                </li>
              ))}
            </ul>
          )}
        </div>
      )}
      {infos.length > 0 && (
        <div className="bg-sky-500/10">
          <button
            type="button"
            onClick={onToggleInfos}
            className="w-full flex items-center justify-between px-4 py-2.5 text-left hover:bg-sky-500/5 transition-colors"
          >
            <span className="text-sky-700 text-sm font-medium">
              {infos.length} note{infos.length === 1 ? '' : 's'}
            </span>
            <span className="text-sky-600/80 text-xs font-mono">{showInfos ? '▾' : '▸'}</span>
          </button>
          {showInfos && (
            <ul className="max-h-64 overflow-auto border-t border-sky-500/20 divide-y divide-sky-500/10">
              {infos.map((n, i) => (
                <li key={i} className="px-4 py-2 text-xs text-sky-800 flex gap-2">
                  <span className="uppercase text-[10px] tracking-wider font-mono text-sky-600/80 shrink-0 w-16">
                    {n.scope}
                  </span>
                  <span className="break-words">{n.message}</span>
                </li>
              ))}
            </ul>
          )}
        </div>
      )}
    </div>
  );
}
