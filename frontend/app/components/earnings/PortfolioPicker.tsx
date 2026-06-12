'use client';

import type { Portfolio } from './usePortfolios';

/** Dropdown that selects one saved portfolio. Used in place of `CompanyPicker`
 * when an earnings comparison side is in portfolio mode. */
export default function PortfolioPicker({
  portfolios,
  selected,
  onSelect,
  className,
}: {
  portfolios: Portfolio[];
  selected: Portfolio | null;
  onSelect: (p: Portfolio | null) => void;
  className?: string;
}) {
  return (
    <select
      value={selected?.id ?? ''}
      onChange={(e) => {
        const id = Number(e.target.value);
        onSelect(portfolios.find((p) => p.id === id) ?? null);
      }}
      className={`h-10 bg-page border border-neutral-700 rounded-lg px-3 text-fg-strong outline-none focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 transition-colors ${className ?? 'w-72'}`}
    >
      <option value="">
        {portfolios.length === 0 ? 'No portfolios yet' : 'Select portfolio…'}
      </option>
      {portfolios.map((p) => (
        <option key={p.id} value={p.id}>
          {p.name} ({p.members.length})
        </option>
      ))}
    </select>
  );
}
