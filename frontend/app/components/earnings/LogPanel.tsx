'use client';

import type { RefObject } from 'react';

/** Live event log shown while a refresh is in flight + a "done" summary
 * line afterwards. Renders nothing when there are no events. An optional
 * `label` (e.g. ticker) lets the caller disambiguate stacked panels
 * during a comparison-mode dual refresh. */
export default function LogPanel({
  logs,
  logEndRef,
  running,
  onClose,
  label,
}: {
  logs: { type: string; message: string }[];
  logEndRef: RefObject<HTMLDivElement | null>;
  running: boolean;
  onClose?: () => void;
  label?: string;
}) {
  if (logs.length === 0) return null;
  const isDone = !running;
  return (
    <div className="bg-sidebar border border-neutral-800/40 rounded-lg overflow-hidden">
      <div className="px-3 py-1.5 border-b border-neutral-800/40 flex items-center gap-2">
        {isDone
          ? <div className="w-1.5 h-1.5 rounded-full bg-pos-400" />
          : <div className="w-1.5 h-1.5 rounded-full bg-accent-400 animate-pulse" />}
        <span className="text-fg-subtle text-xs font-medium">
          {label && <span className="text-fg-muted mr-1.5 font-mono">{label}</span>}
          {isDone ? 'Refresh Complete' : 'Refresh Progress'}
        </span>
        <button onClick={onClose} className="ml-auto text-fg-subtle hover:text-fg-soft transition-colors" aria-label="Close">
          <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" className="w-3.5 h-3.5">
            <path d="M6.28 5.22a.75.75 0 00-1.06 1.06L8.94 10l-3.72 3.72a.75.75 0 101.06 1.06L10 11.06l3.72 3.72a.75.75 0 101.06-1.06L11.06 10l3.72-3.72a.75.75 0 00-1.06-1.06L10 8.94 6.28 5.22z" />
          </svg>
        </button>
      </div>
      <div className="max-h-[5.5rem] overflow-y-auto p-3 font-mono text-xs">
      {logs.map((l, i) => (
        <div key={i} className={l.type === 'error' ? 'text-neg-400' : l.type === 'warning' ? 'text-warn-400' : l.type === 'done' ? 'text-pos-400' : 'text-fg-muted'}>
          {l.message}
        </div>
      ))}
      <div ref={logEndRef} />
      </div>
    </div>
  );
}
