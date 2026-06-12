'use client';

import type { Basket, Portfolio } from './usePortfolios';
import { portfolioToBasket } from './usePortfolios';
import type { UniverseBasket } from './useEarningsUniverses';
import { universeToBasket } from './useEarningsUniverses';

/** Dropdown that selects one basket — a saved portfolio OR a frozen-universe
 * snapshot — for an earnings comparison side in portfolio mode. Options are
 * grouped so the two sources stay visually distinct; the selection encodes the
 * kind (`p:`/`u:`) so the same id can exist in both groups without collision. */
export default function PortfolioPicker({
  portfolios,
  universes,
  selected,
  onSelect,
  className,
}: {
  portfolios: Portfolio[];
  universes: UniverseBasket[];
  selected: Basket | null;
  onSelect: (b: Basket | null) => void;
  className?: string;
}) {
  const value = selected ? `${selected.kind === 'universe' ? 'u' : 'p'}:${selected.id}` : '';
  const empty = portfolios.length === 0 && universes.length === 0;

  return (
    <select
      value={value}
      onChange={(e) => {
        const v = e.target.value;
        if (!v) { onSelect(null); return; }
        const [kind, idStr] = v.split(':');
        const id = Number(idStr);
        if (kind === 'u') {
          const u = universes.find((x) => x.universe_id === id);
          onSelect(u ? universeToBasket(u) : null);
        } else {
          const p = portfolios.find((x) => x.id === id);
          onSelect(p ? portfolioToBasket(p) : null);
        }
      }}
      className={`h-10 bg-page border border-neutral-700 rounded-lg px-3 text-fg-strong outline-none focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 transition-colors ${className ?? 'w-72'}`}
    >
      <option value="">
        {empty ? 'No portfolios yet' : 'Select portfolio or universe…'}
      </option>
      {portfolios.length > 0 && (
        <optgroup label="Portfolios">
          {portfolios.map((p) => (
            <option key={`p:${p.id}`} value={`p:${p.id}`}>
              {p.name} ({p.members.length})
            </option>
          ))}
        </optgroup>
      )}
      {universes.length > 0 && (
        <optgroup label="Universes (frozen snapshots)">
          {universes.map((u) => (
            <option key={`u:${u.universe_id}`} value={`u:${u.universe_id}`}>
              {u.label} ({u.count})
            </option>
          ))}
        </optgroup>
      )}
    </select>
  );
}
