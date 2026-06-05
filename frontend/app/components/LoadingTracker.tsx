'use client';

import { loadingStore } from '../../lib/loading';
import { useNow } from '../../lib/hooks/useNow';

export default function LoadingTracker() {
  const items = loadingStore.use((s) => s.items);
  // Tick once per second (only while the panel has items) so elapsed
  // times update live.
  const now = useNow(1000, items.length > 0);

  if (items.length === 0) return null;

  return (
    <div className="fixed bottom-4 right-4 z-50 w-72 max-w-[calc(100vw-2rem)] bg-card border border-neutral-800/60 rounded-xl shadow-xl overflow-hidden">
      <div className="px-3 py-2 border-b border-neutral-800/60 flex items-center gap-2">
        <span className="inline-block w-2 h-2 rounded-full bg-accent-400 animate-pulse" />
        <span className="text-xs font-medium text-fg-soft">
          {items.length} request{items.length === 1 ? '' : 's'} in flight
        </span>
      </div>
      <ul className="divide-y divide-neutral-800/40 max-h-72 overflow-y-auto">
        {items.map((item) => {
          const elapsed = Math.max(0, Math.floor((now - item.startedAt) / 1000));
          return (
            <li key={item.id} className="px-3 py-2 flex items-center justify-between gap-3">
              <span className="text-xs text-fg truncate">{item.label}</span>
              <span className="text-[11px] font-mono text-fg-subtle shrink-0">{elapsed}s</span>
            </li>
          );
        })}
      </ul>
    </div>
  );
}
