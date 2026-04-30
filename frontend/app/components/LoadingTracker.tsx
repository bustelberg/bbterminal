'use client';

import { useEffect, useState } from 'react';
import { loadingStore } from '../../lib/loading';

export default function LoadingTracker() {
  const items = loadingStore.use((s) => s.items);
  const [now, setNow] = useState(Date.now());

  // Tick once per second so the elapsed time updates while the panel is open.
  useEffect(() => {
    if (items.length === 0) return;
    const t = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(t);
  }, [items.length]);

  if (items.length === 0) return null;

  return (
    <div className="fixed bottom-4 right-4 z-50 w-72 max-w-[calc(100vw-2rem)] bg-[#151821] border border-gray-800/60 rounded-xl shadow-xl overflow-hidden">
      <div className="px-3 py-2 border-b border-gray-800/60 flex items-center gap-2">
        <span className="inline-block w-2 h-2 rounded-full bg-indigo-400 animate-pulse" />
        <span className="text-xs font-medium text-gray-300">
          {items.length} request{items.length === 1 ? '' : 's'} in flight
        </span>
      </div>
      <ul className="divide-y divide-gray-800/40 max-h-72 overflow-y-auto">
        {items.map((item) => {
          const elapsed = Math.max(0, Math.floor((now - item.startedAt) / 1000));
          return (
            <li key={item.id} className="px-3 py-2 flex items-center justify-between gap-3">
              <span className="text-xs text-gray-200 truncate">{item.label}</span>
              <span className="text-[11px] font-mono text-gray-500 shrink-0">{elapsed}s</span>
            </li>
          );
        })}
      </ul>
    </div>
  );
}
