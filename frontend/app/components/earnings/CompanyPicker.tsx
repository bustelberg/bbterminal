'use client';

import { useMemo, useRef, useState } from 'react';
import { useClickOutside } from '../../../lib/hooks/useClickOutside';
import type { Company } from './types';

/** Search-as-you-type company picker. Filters the full `companies` list
 * by name or ticker (case-insensitive), capped at 50 visible rows. */
export default function CompanyPicker({
  companies,
  selected,
  onSelect,
  className,
}: {
  companies: Company[];
  selected: Company | null;
  onSelect: (c: Company) => void;
  /** Width/layout classes for the wrapper. Defaults to `w-full max-w-md`;
   * pass a fixed width (e.g. `w-72`) to keep multiple pickers equally sized. */
  className?: string;
}) {
  const [query, setQuery] = useState('');
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  const filtered = useMemo(() => {
    if (!query.trim()) return companies.slice(0, 50);
    const q = query.toLowerCase();
    return companies.filter(
      (c) =>
        (c.company_name || '').toLowerCase().includes(q) ||
        c.gurufocus_ticker.toLowerCase().includes(q)
    ).slice(0, 50);
  }, [query, companies]);

  useClickOutside(ref, () => setOpen(false));

  return (
    <div ref={ref} className={`relative ${className ?? 'w-full max-w-md'}`}>
      <input
        type="text"
        value={query}
        onChange={(e) => { setQuery(e.target.value); setOpen(true); }}
        onFocus={() => setOpen(true)}
        placeholder={selected ? `${selected.company_name || selected.gurufocus_ticker}` : 'Search company or ticker...'}
        className="w-full h-10 bg-page border border-neutral-700 rounded-lg px-3 text-fg-strong placeholder-fg-subtle focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none"
      />
      {open && filtered.length > 0 && (
        <div className="absolute z-50 mt-1 w-full max-h-64 overflow-y-auto bg-card border border-neutral-700 rounded-lg shadow-xl">
          {filtered.map((c) => (
            <button
              key={c.company_id}
              onClick={() => {
                onSelect(c);
                setQuery('');
                setOpen(false);
              }}
              className="w-full px-3 py-2 text-left hover:bg-overlay/[0.04] transition-colors flex items-center gap-3"
            >
              <span className="font-mono text-accent-400 text-sm">{c.gurufocus_ticker}</span>
              <span className="text-fg-soft text-sm truncate">{c.company_name || '—'}</span>
              <span className="text-fg-faint text-xs ml-auto">{c.gurufocus_exchange}</span>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
