'use client';

import { type Target } from './HoldingsRevenueModal';
import MissingFundamentals from './MissingFundamentals';

export default function HoldingsIngestPanel({ target, metric: _metric, noun, onIngested }: {
  target: Target;
  metric: string;
  noun: string;            // 'dividend/share' — for the sentences
  onIngested?: () => void; // reload the tab's metrics (the caller's charts)
}) {
  void _metric;
  return <MissingFundamentals message={`No ${noun} ingested for this portfolio.`}
    target={target} name="portfolio" onDone={onIngested} />;
}
