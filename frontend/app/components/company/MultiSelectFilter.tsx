'use client';

import { useMemo, useRef, useState } from 'react';
import { useClickOutside, useEscapeKey } from '../../../lib/hooks/useClickOutside';

// ─── Multi-select checklist filter ───────────────────────────────────────────
// Replaces the single-select dropdowns so the universe filter can pick out
// e.g. ACWI ∩ LEONTEQ by checking both. `combineMode` is purely cosmetic —
// it shows "(AND)" / "(OR)" in the panel header so the user knows whether
// two checked entries narrow (AND) or widen (OR) the result. The actual
// AND/OR application lives in the caller's filter useMemo.
export default function MultiSelectFilter({
  label,
  options,
  selected,
  onChange,
  combineMode,
}: {
  label: string;
  options: string[];
  selected: string[];
  onChange: (next: string[]) => void;
  combineMode?: 'AND' | 'OR';
}) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState('');
  const ref = useRef<HTMLDivElement>(null);
  useClickOutside(ref, () => setOpen(false), open);
  useEscapeKey(() => setOpen(false), open);

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    return q ? options.filter((o) => o.toLowerCase().includes(q)) : options;
  }, [options, query]);

  const buttonLabel =
    selected.length === 0
      ? `All ${label.toLowerCase()}`
      : selected.length <= 2
      ? selected.join(', ')
      : `${selected.length} ${label.toLowerCase()}`;

  const toggle = (opt: string) => {
    onChange(
      selected.includes(opt)
        ? selected.filter((s) => s !== opt)
        : [...selected, opt],
    );
  };

  return (
    <div ref={ref} className="relative">
      <button
        type="button"
        onClick={() => setOpen((o) => !o)}
        className={`bg-card border rounded-lg px-3 py-2 text-sm text-fg-strong transition-colors inline-flex items-center gap-2 ${
          selected.length > 0
            ? 'border-accent-500/60 text-accent-200'
            : 'border-neutral-800/60 hover:border-neutral-700'
        }`}
      >
        <span className="truncate max-w-[180px]">{buttonLabel}</span>
        <svg
          className={`w-3.5 h-3.5 text-fg-subtle transition-transform ${open ? 'rotate-180' : ''}`}
          viewBox="0 0 20 20"
          fill="currentColor"
        >
          <path
            fillRule="evenodd"
            d="M5.23 7.21a.75.75 0 011.06.02L10 11.06l3.71-3.83a.75.75 0 111.08 1.04l-4.25 4.39a.75.75 0 01-1.08 0L5.21 8.27a.75.75 0 01.02-1.06z"
            clipRule="evenodd"
          />
        </svg>
      </button>
      {open && (
        <div className="absolute left-0 mt-1 w-64 bg-card border border-neutral-700 rounded-lg shadow-xl z-50 max-h-80 overflow-hidden flex flex-col">
          <div className="px-3 py-2 border-b border-neutral-800/60 flex items-center justify-between gap-2">
            <span className="text-xs text-fg-muted">
              {label}
              {combineMode && selected.length >= 2 && (
                <span className="ml-1.5 text-[10px] uppercase tracking-wide text-fg-faint">
                  ({combineMode})
                </span>
              )}
            </span>
            {selected.length > 0 && (
              <button
                type="button"
                onClick={() => onChange([])}
                className="text-[11px] text-fg-subtle hover:text-fg-strong"
              >
                Clear
              </button>
            )}
          </div>
          {options.length > 8 && (
            <div className="px-2 pt-2">
              <input
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder="Filter…"
                className="w-full bg-page border border-neutral-700 rounded px-2 py-1 text-xs text-fg-strong placeholder-fg-faint focus:outline-none focus:border-accent-500"
              />
            </div>
          )}
          <div className="flex-1 overflow-auto p-1">
            {filtered.length === 0 ? (
              <div className="px-3 py-2 text-xs text-fg-faint">No matches</div>
            ) : (
              filtered.map((opt) => {
                const checked = selected.includes(opt);
                return (
                  <label
                    key={opt}
                    className={`flex items-center gap-2 px-2 py-1.5 rounded hover:bg-overlay/[0.04] cursor-pointer text-sm ${
                      checked ? 'text-fg-strong' : 'text-fg-soft'
                    }`}
                  >
                    <input
                      type="checkbox"
                      checked={checked}
                      onChange={() => toggle(opt)}
                      className="accent-accent-500 w-3.5 h-3.5"
                    />
                    <span className="truncate">{opt}</span>
                  </label>
                );
              })
            )}
          </div>
        </div>
      )}
    </div>
  );
}
