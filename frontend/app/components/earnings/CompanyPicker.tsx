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
}: {
  companies: Company[];
  selected: Company | null;
  onSelect: (c: Company) => void;
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
    <div ref={ref} className="relative w-full max-w-md">
      <input
        type="text"
        value={query}
        onChange={(e) => { setQuery(e.target.value); setOpen(true); }}
        onFocus={() => setOpen(true)}
        placeholder={selected ? `${selected.company_name || selected.gurufocus_ticker}` : 'Search company or ticker...'}
        className="w-full bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2.5 text-white placeholder-gray-500 focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
      />
      {open && filtered.length > 0 && (
        <div className="absolute z-50 mt-1 w-full max-h-64 overflow-y-auto bg-[#151821] border border-gray-700 rounded-lg shadow-xl">
          {filtered.map((c) => (
            <button
              key={c.company_id}
              onClick={() => {
                onSelect(c);
                setQuery('');
                setOpen(false);
              }}
              className="w-full px-3 py-2 text-left hover:bg-white/[0.04] transition-colors flex items-center gap-3"
            >
              <span className="font-mono text-indigo-400 text-sm">{c.gurufocus_ticker}</span>
              <span className="text-gray-300 text-sm truncate">{c.company_name || '—'}</span>
              <span className="text-gray-600 text-xs ml-auto">{c.gurufocus_exchange}</span>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
