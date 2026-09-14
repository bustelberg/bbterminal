'use client';

import PortfolioFundamentalsRefresh, { type RefreshScope } from './PortfolioFundamentalsRefresh';
import { type Target } from './HoldingsRevenueModal';

function scopeFor(target: Target, name?: string | null): RefreshScope | undefined {
  if (target.portfolio_id != null) {
    return { kind: 'portfolio', id: target.portfolio_id, name: name || `portfolio ${target.portfolio_id}` };
  }
  if (target.holdings?.length) {
    return { kind: 'basket', holdings: target.holdings.map((h) => ({ isin: h.isin })), name: name || 'portfolio' };
  }
  if (target.universe) {
    return { kind: 'universe', label: target.universe, name: target.universe, feeds: 'smart' };
  }
  return undefined;
}

/** Empty chart state shared by every fundamental card. The action refreshes the complete suite. */
export default function MissingFundamentals({ message, target, name, onDone }: {
  message: string;
  target?: Target;
  name?: string | null;
  onDone?: () => void;
}) {
  const scope = target ? scopeFor(target, name) : undefined;
  return (
    <div className="py-16 flex flex-col items-center gap-3 text-center px-4">
      <p className="text-[12px] text-fg-faint">{message}</p>
      {scope ? <PortfolioFundamentalsRefresh scope={scope} everything onDone={onDone} /> : null}
    </div>
  );
}
