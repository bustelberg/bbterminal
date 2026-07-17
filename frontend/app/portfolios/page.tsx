'use client';

import { useState } from 'react';
import AccountModelLinkPanel from '../components/AccountModelLinkPanel';
import AirsAccountsPanel from '../components/AirsAccountsPanel';
import AssetNav from '../components/AssetNav';
import BenchmarksPanel from '../components/BenchmarksPanel';
import CorrelationMatrix from '../components/CorrelationMatrix';
import PortfolioOverviewPanel from '../components/PortfolioOverviewPanel';
import PortfoliosPanel from '../components/PortfoliosPanel';

/**
 * The AIRS portfolios — a sibling of Execution instruments in the asset-pipeline nav, not a tab
 * inside it: they are different universes. Instruments are per-ISIN rows we resolve and price;
 * portfolios are AirSPMS constructs whose positions happen to carry the ISIN that joins the two.
 *
 * WHY ONE TABLE ON TOP AND THREE BEHIND A BUTTON
 *   A portfolio is TWO rows in AIRS — the Fixed one (weights, ISINs, your nickname; AIRS values
 *   none of it) and the Dynamic one (the real book: money, returns, no ISIN). Overlap: ZERO, of
 *   58 Fixed and 31 valued Dynamic. Reading it took three tables and a naming convention held in
 *   your head, so `PortfolioOverviewPanel` composes the pair into the row you actually want.
 *
 *   The three sources stay, collapsed — NOT deleted. The overview is a JOIN, and every join here
 *   is a claim: 27 of 28 pairings are an unconfirmed name match, and the ISIN on a holding is a
 *   name match too. When a number looks wrong, the question is always "which side said that",
 *   and these three are the only place that can answer it.
 */
export default function Page() {
  const [showSources, setShowSources] = useState(false);

  return (
    <div className="min-h-screen bg-page text-fg">
      <AssetNav />
      <div className="px-8 py-5 border-b border-neutral-800/40">
        <h1 className="text-xl font-semibold text-fg-strong">Portfolios</h1>
      </div>

      <div className="px-8 py-6 space-y-6">
        <PortfolioOverviewPanel />

        <div>
          <button
            onClick={() => setShowSources((v) => !v)}
            className="text-xs text-fg-subtle hover:text-fg px-3 py-1.5 rounded-lg border border-neutral-800/40 hover:bg-overlay/5 transition-colors"
          >
            {showSources ? '▾' : '▸'} Source tables
            <span className="text-fg-faint ml-1.5">Fixed · Dynamic · pairing</span>
          </button>
        </div>

        {showSources && (
          <div className="space-y-6">
            {/* The FIXED portfolios (compositions, priced from yfinance — nothing else can value
                a set of weights) and the DYNAMIC ones (AIRS's own EUR values) are different
                objects answering different questions: "would the strategy work" vs "what did the
                book make". Their overlap is literally zero — no Fixed portfolio has AIRS values,
                and none can. Adjacent, not merged, because the gap between them IS drift. */}
            <PortfoliosPanel />
            <AirsAccountsPanel />
            {/* The pairing. Nothing in either dataset says which book runs which strategy, and
                the contents cannot say either: the risk variants hold the SAME instruments
                (BUS_FTS_Bepoff/DEF/NEU_AFS share 27 of 27 ISINs). Only a human reliably can —
                which is why the overview above marks a guessed name. */}
            <AccountModelLinkPanel />
          </div>
        )}

        <CorrelationMatrix />
        <BenchmarksPanel />
      </div>
    </div>
  );
}
