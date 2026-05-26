'use client';

import { useEffect, useRef } from 'react';

export type StepStatus = 'pending' | 'in_progress' | 'done' | 'error';
export type StepState = { status: StepStatus; message?: string };
export type StepDef = { key: string; label: string };

/** Log entry. `relativeMs` is milliseconds since the run started — when
 * present we render it as a "+1.3s" prefix so each line shows when it
 * arrived. Plain strings fall back to no prefix (backward compat). */
export type LogEntry = string | { message: string; relativeMs?: number };

export type ProgressTimelineProps = {
  steps: StepDef[];
  state?: Record<string, StepState>;
  log?: LogEntry[];
  doneSummary?: string | null;
  errorMessage?: string | null;
  running?: boolean;
  /** Optional explicit 0-100 override; otherwise derived from step state. */
  pct?: number;
  className?: string;
  /** Auto-scroll the verbose log to the bottom on new entries. Default true. */
  autoScrollLog?: boolean;
  /** Show the verbose log expanded by default (otherwise wrapped in <details>). */
  defaultLogOpen?: boolean;
  /** Title for the panel (shown above the progress bar). */
  title?: string;
  /** Optional dismiss button (top-right of the panel). */
  onDismiss?: () => void;
  /** When set, the panel header shows live "Running 12s" while running and
   * "Completed in 1m 23s" when done. ms since run started. */
  totalElapsedMs?: number | null;
};

function StepIcon({ status }: { status: StepStatus }) {
  if (status === 'done') return <span className="text-emerald-400 mt-0.5">✓</span>;
  if (status === 'in_progress') return <span className="text-indigo-400 animate-pulse mt-0.5">●</span>;
  if (status === 'error') return <span className="text-rose-400 mt-0.5">✗</span>;
  return <span className="text-gray-600 mt-0.5">○</span>;
}

function derivedPct(steps: StepDef[], state: Record<string, StepState>): number {
  if (!steps.length) return 0;
  let score = 0;
  for (const s of steps) {
    const st = state[s.key];
    if (!st) continue;
    if (st.status === 'done') score += 1;
    else if (st.status === 'in_progress') score += 0.5;
  }
  return Math.round((score / steps.length) * 100);
}

/** Compact `Xh Ym Zs` formatter for the elapsed / remaining / total
 * chips in the header. Drops higher units when zero (e.g. "47s" not
 * "0h 0m 47s"). Used three times per render so brevity matters more
 * than the long-form "1 minute 23 seconds" the old verbose form used. */
function formatCompact(ms: number): string {
  const totalSeconds = Math.max(0, Math.floor(ms / 1000));
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  const parts: string[] = [];
  if (hours > 0) parts.push(`${hours}h`);
  if (minutes > 0 || hours > 0) parts.push(`${minutes}m`);
  parts.push(`${seconds}s`);
  return parts.join(' ');
}

function formatRelative(ms: number): string {
  if (ms < 1000) return `+${ms}ms`;
  if (ms < 60_000) return `+${(ms / 1000).toFixed(1)}s`;
  const totalSeconds = Math.floor(ms / 1000);
  const m = Math.floor(totalSeconds / 60);
  const rs = totalSeconds % 60;
  return `+${m}m ${rs.toString().padStart(2, '0')}s`;
}

export default function ProgressTimeline({
  steps,
  state = {},
  log = [],
  doneSummary,
  errorMessage,
  running = false,
  pct,
  className,
  autoScrollLog = true,
  defaultLogOpen = false,
  title,
  onDismiss,
  totalElapsedMs,
}: ProgressTimelineProps) {
  const logEndRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!autoScrollLog) return;
    const el = logEndRef.current?.parentElement;
    if (el) el.scrollTop = el.scrollHeight;
  }, [log, autoScrollLog]);

  const hasAny = running || Object.keys(state).length > 0 || doneSummary || errorMessage || log.length > 0;
  if (!hasAny) return null;

  const computedPct = pct ?? derivedPct(steps, state);

  const showProgressBar = steps.length > 0 || pct != null;

  // Linear ETA from `pct`: total ≈ elapsed × (100/pct); remaining =
  // total − elapsed. Only shown when running AND we have enough signal
  // to extrapolate without flashing absurd numbers — the 5% floor + 2s
  // floor together cover the early-noise window where extrapolation
  // produces nonsense like "Total ~5h" because pct=1 after 0.5s of
  // wall-clock. ETA is suppressed at 100% (run is done — show
  // "Completed in X" instead). Caller-provided `pct` is the only source
  // (derived pct from steps would skip per-message progress events).
  const hasEta =
    running &&
    totalElapsedMs != null &&
    totalElapsedMs >= 2000 &&
    computedPct > 5 &&
    computedPct < 100;
  const remainingMs = hasEta
    ? Math.max(0, Math.round((totalElapsedMs! / computedPct) * (100 - computedPct)))
    : null;
  const totalEstMs = remainingMs != null ? totalElapsedMs! + remainingMs : null;

  return (
    <div className={`bg-[#0f1117] border border-gray-800 rounded-lg p-3 space-y-2 ${className ?? ''}`}>
      {(title || onDismiss || totalElapsedMs != null) && (
        <div className="flex items-center justify-between gap-3">
          {title && <div className="text-xs font-medium text-gray-300">{title}</div>}
          {totalElapsedMs != null && (
            <div className={
              running
                ? 'flex items-center gap-2 px-3 py-1.5 rounded-md bg-indigo-500/10 border border-indigo-500/30 ml-auto'
                : 'flex items-center gap-2 px-3 py-1.5 rounded-md bg-emerald-500/10 border border-emerald-500/30 ml-auto'
            }>
              {running ? (
                <>
                  <span className="text-[11px] uppercase tracking-wide text-gray-400">Elapsed</span>
                  <span className="text-[13px] font-mono font-semibold text-gray-100 tabular-nums">
                    {formatCompact(totalElapsedMs)}
                  </span>
                  {remainingMs != null && totalEstMs != null && (
                    <>
                      <span className="text-gray-700">·</span>
                      <span className="text-[11px] uppercase tracking-wide text-gray-400">Left</span>
                      <span className="text-[13px] font-mono font-semibold text-indigo-300 tabular-nums">
                        ~{formatCompact(remainingMs)}
                      </span>
                      <span className="text-gray-700">·</span>
                      <span className="text-[11px] uppercase tracking-wide text-gray-400">Total</span>
                      <span className="text-[13px] font-mono font-semibold text-gray-100 tabular-nums">
                        ~{formatCompact(totalEstMs)}
                      </span>
                    </>
                  )}
                </>
              ) : (
                <>
                  <span className="text-[11px] uppercase tracking-wide text-gray-400">Completed in</span>
                  <span className="text-[13px] font-mono font-semibold text-gray-100 tabular-nums">
                    {formatCompact(totalElapsedMs)}
                  </span>
                </>
              )}
            </div>
          )}
          {onDismiss && (
            <button
              onClick={onDismiss}
              className="text-gray-500 hover:text-gray-300 transition-colors ml-auto"
              aria-label="Dismiss"
            >
              <svg viewBox="0 0 20 20" fill="currentColor" className="w-3.5 h-3.5">
                <path d="M6.28 5.22a.75.75 0 00-1.06 1.06L8.94 10l-3.72 3.72a.75.75 0 101.06 1.06L10 11.06l3.72 3.72a.75.75 0 101.06-1.06L11.06 10l3.72-3.72a.75.75 0 00-1.06-1.06L10 8.94 6.28 5.22z" />
              </svg>
            </button>
          )}
        </div>
      )}
      {/* Unified progress bar */}
      {showProgressBar && (
        <div className="space-y-1">
          <div className="flex items-center justify-between text-[11px] text-gray-500">
            <span>{running ? 'Running…' : (errorMessage ? 'Failed' : doneSummary ? 'Complete' : 'Idle')}</span>
            <span className="font-mono">{computedPct}%</span>
          </div>
          <div className="h-1.5 bg-gray-800 rounded-full overflow-hidden">
            <div
              className={`h-full transition-all duration-300 ${
                errorMessage ? 'bg-rose-500' : computedPct === 100 ? 'bg-emerald-500' : 'bg-indigo-500'
              }`}
              style={{ width: `${computedPct}%` }}
            />
          </div>
        </div>
      )}

      {/* Step list */}
      {steps.length > 0 && (
        <div className="space-y-1 pt-1">
          {steps.map(s => {
            const st = state[s.key];
            const status: StepStatus = st?.status ?? 'pending';
            return (
              <div key={s.key} className="flex items-start gap-2 text-xs">
                <StepIcon status={status} />
                <span className={
                  status === 'done' ? 'text-gray-400'
                  : status === 'in_progress' ? 'text-gray-200'
                  : status === 'error' ? 'text-rose-400'
                  : 'text-gray-600'
                }>
                  <span className="font-medium">{s.label}</span>
                  {st?.message && <span className="text-gray-500"> — {st.message}</span>}
                </span>
              </div>
            );
          })}
        </div>
      )}

      {doneSummary && (
        <div className="text-xs text-emerald-400 font-medium pt-1 border-t border-gray-800/60">
          {doneSummary}
        </div>
      )}

      {errorMessage && (
        <div className="text-xs text-rose-400 pt-1 border-t border-gray-800/60 whitespace-pre-wrap">
          {errorMessage}
        </div>
      )}

      {log.length > 0 && (() => {
        const renderLine = (l: LogEntry, i: number) => {
          if (typeof l === 'string') return <div key={i}>{l}</div>;
          return (
            <div key={i} className="flex gap-2">
              <span className="text-gray-600 shrink-0 w-12 text-right">
                {l.relativeMs != null ? formatRelative(l.relativeMs) : ''}
              </span>
              <span>{l.message}</span>
            </div>
          );
        };
        const inner = (
          <div className="max-h-48 overflow-auto text-[11px] font-mono text-gray-400 space-y-0.5">
            {log.map(renderLine)}
            <div ref={logEndRef} />
          </div>
        );
        return defaultLogOpen ? (
          <div>
            <div className="text-gray-500 text-xs mb-1">Verbose log ({log.length} events)</div>
            {inner}
          </div>
        ) : (
          <details>
            <summary className="text-gray-500 text-xs cursor-pointer hover:text-gray-300 select-none">
              Verbose log ({log.length} events)
            </summary>
            <div className="mt-2">{inner}</div>
          </details>
        );
      })()}
    </div>
  );
}
