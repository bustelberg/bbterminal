'use client';

import { isUniverseTarget, type BenchTarget } from './benchSeries';
import PortfolioFundamentalsRefresh from './PortfolioFundamentalsRefresh';

/** A benchmark refresh loads the complete data set once per reachable constituent. */
export default function BenchmarkFundamentalsRefresh({ benchTarget, benchLabel, onDone }: {
  benchTarget: BenchTarget | null | undefined;
  benchLabel?: string | null;
  onDone: () => void;
}) {
  if (!benchTarget || !isUniverseTarget(benchTarget)) return null;

  return (
    <span className="ml-auto shrink-0">
      <PortfolioFundamentalsRefresh
        scope={{ kind: 'universe', label: benchTarget.universe,
          name: benchLabel || benchTarget.label }}
        label="Refresh benchmark"
        onDone={onDone} />
    </span>
  );
}
