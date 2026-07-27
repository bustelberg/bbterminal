'use client';

import { useMemo, useState } from 'react';
import { type Basket } from './PerformanceModal';
import FundamentalCharts from './FundamentalCharts';
import FundamentalCoverage from './FundamentalCoverage';
import LongEquityTab from './LongEquityTab';

type Tab = 'fundamentals' | 'longequity';

/**
 * The Fundamental modal: one company's fundamental chart suite.
 *
 * ⚠ IT USED TO OPEN ON AN OWNER-EARNINGS / STOCK-PRICE CHART, and that whole section was removed
 * 2026-07-23 — the tab bar, the currency and cadence toggles, the R²/growth-SD/CAGR headline, the
 * log-axis trend chart and its footnote, plus the two data fetches behind them (the owner-earnings
 * SSE stream and the price series). ~300 lines. `FundamentalCharts` is the whole modal now.
 *
 * ⚠ THAT SECTION WAS THE ONLY CONTENT AN AGGREGATE EVER HAD. A basket or a whole portfolio has no
 * single company to chart, so `FundamentalCharts` has never rendered for one. Rather than open an
 * empty box, an aggregate now says so — see the note below. The /portfolios portfolio-level
 * "Fundamental" button opens exactly this case.
 */
export default function OwnerEarningsModal({
  isin, name, basket, portfolioId, onClose,
}: {
  isin?: string;
  name?: string | null;
  basket?: Basket;
  portfolioId?: number;    // a whole model portfolio, resolved to a basket server-side
  onClose: () => void;
}) {
  const isAgg = !!basket || portfolioId != null;
  const title = basket?.label ?? name ?? isin ?? '';
  // Bumped whenever the coverage panel ingests something, so the blended charts re-fetch and pick
  // up the data that ingest just created (they'd otherwise show the pre-ingest blend until reopen).
  // Used as FundamentalCharts' `key` — a remount is the clean way to force one fresh blend fetch.
  const [blendKey, setBlendKey] = useState(0);
  // ⚠ MEMOISED, OR IT REFETCHES FOR EVER. A fresh object literal here is a new reference every
  // render, and it is in the child's effect deps — so each fetch would set state, re-render, and
  // fetch again. The blend is an expensive multi-company query; this is not a micro-optimisation.
  const blend = useMemo(() => ({ basket, portfolioId }), [basket, portfolioId]);
  const [tab, setTab] = useState<Tab>('fundamentals');
  const hasInstrument = isAgg || !!isin;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-scrim/60"
      onClick={onClose} role="dialog" aria-modal="true">
      <div className="bg-card border border-neutral-800/40 rounded-xl shadow-xl w-[80vw] h-[80vh] overflow-auto p-4"
        onClick={(e) => e.stopPropagation()}>

        <div className="flex items-start justify-between gap-3 mb-2">
          <div className="min-w-0">
            <div className="flex items-baseline gap-2 flex-wrap">
              <span className="text-base font-semibold text-fg-strong">Fundamental</span>
              {isAgg
                ? <span className="text-sm text-fg-soft truncate">{title}{basket ? ' · group' : ' · portfolio'}</span>
                : <>
                    <span className="text-sm font-mono text-fg-soft">{isin}</span>
                    {name && <span className="text-sm text-fg-soft truncate">{name}</span>}
                  </>}
            </div>
          </div>
          <button type="button" onClick={onClose} aria-label="Close"
            className="text-fg-faint hover:text-fg-strong text-xl leading-none px-1 -mt-1">×</button>
        </div>

        {/* Tabs: the chart suite, and the LongEquity revenue-growth read. */}
        {hasInstrument && (
          <div className="flex items-center gap-0.5 rounded-lg border border-neutral-700 p-0.5 w-fit mb-3">
            {([['fundamentals', 'Fundamentals'], ['longequity', 'Long Equity']] as [Tab, string][]).map(([t, label]) => (
              <button key={t} type="button" onClick={() => setTab(t)}
                className={`px-3 py-1 rounded-md text-xs font-medium transition-colors ${
                  tab === t ? 'bg-accent-600 text-fg-strong'
                    : 'text-fg-muted hover:text-fg-strong hover:bg-overlay/5'}`}>
                {label}
              </button>
            ))}
          </div>
        )}

        {tab === 'longequity' && hasInstrument ? (
          <LongEquityTab isin={isin} name={title} basket={basket} portfolioId={portfolioId} />
        ) : /* An aggregate gets the SAME chart suite, blended across its holdings, with the coverage
              breakdown beneath it — how much of the book those charts actually span, and which
              holdings are missing. */
          isAgg ? (
            // ⚠ THE SAME COMPONENT AS A SINGLE COMPANY. The portfolio is fetched as a blended
            // pseudo-company in the identical payload shape, so this is the same screen — not a
            // second, parallel implementation that would drift from it.
            <>
              <FundamentalCharts key={blendKey} blend={blend} name={title} />
              <div className="mt-6 pt-5 border-t border-neutral-800/40">
                <FundamentalCoverage basket={basket} portfolioId={portfolioId}
                  onIngested={() => setBlendKey((k) => k + 1)} />
              </div>
            </>
          ) : isin ? (
            <FundamentalCharts isin={isin} name={name} />
          ) : (
            <p className="text-sm text-fg-subtle py-16 text-center">No instrument to look up.</p>
          )}
      </div>
    </div>
  );
}
