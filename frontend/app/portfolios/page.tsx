'use client';

import AssetNav from '../components/AssetNav';
import BenchmarksPanel from '../components/BenchmarksPanel';
import PortfoliosPanel from '../components/PortfoliosPanel';

/** The AIRS model portfolios — a sibling of Execution instruments in the asset-pipeline
 * nav, not a tab inside it: they are different universes. Instruments are per-ISIN rows we
 * resolve and price; portfolios are AirSPMS constructs whose positions happen to carry the
 * ISIN that joins the two. */
export default function Page() {
  return (
    <div className="min-h-screen bg-page text-fg">
      <AssetNav />
      <div className="px-8 py-5 border-b border-neutral-800/40">
        <h1 className="text-xl font-semibold text-fg-strong">Portfolios</h1>
      </div>

      <div className="px-8 py-6 space-y-6">
        <PortfoliosPanel />
        <BenchmarksPanel />
      </div>
    </div>
  );
}
