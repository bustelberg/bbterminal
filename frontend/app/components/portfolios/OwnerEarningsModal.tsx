'use client';

import { useMemo, useState } from 'react';
import { trace } from '../../../lib/debugTrace';
import { type Basket } from './types';
import FundamentalCharts from './FundamentalCharts';
import FundamentalCoverage from './FundamentalCoverage';
import LongEquityTab from './LongEquityTab';
import QuickValuationTab from './QuickValuationTab';
import DeepValuationTab from './DeepValuationTab';

type Tab = 'fundamentals' | 'longequity' | 'quickval' | 'deepval';

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
  // ⚠ THE FIRST TAB, WHICH IS NO LONGER `fundamentals`. Demoting those charts to last and naming
  // them "Old charts" while still opening on them would say two opposite things at once — and it
  // is the one tab both an aggregate and a single company have, so the landing tab never depends
  // on which the modal was opened for.
  const [tab, setTab] = useState<Tab>('longequity');
  /** Which tabs have been opened at least once — the mount set. A tab enters it on first visit
   *  and never leaves, so its data survives every subsequent switch. Seeded with the landing tab
   *  so it mounts on open like it always did. */
  const [visited, setVisited] = useState<Set<Tab>>(() => new Set<Tab>(['longequity']));
  const openTab = (t: Tab) => {
    setVisited((v) => {
      if (v.has(t)) {
        // Nothing refetches. Traced so the difference is visible rather than inferred from how
        // long the tab took — which is exactly how the repeated loading went unnoticed.
        trace('fundamental', `tab "${t}" already mounted — shown from memory, no refetch`);
        return v;
      }
      trace('fundamental', `tab "${t}" first visit — mounting and loading it`);
      return new Set(v).add(t);
    });
    setTab(t);
  };
  const hasInstrument = isAgg || !!isin;
  /**
   * ⚠ HOISTED OUT OF `LongEquityTab` SO IT CAN SIT IN THE TAB ROW. The setting belongs to that tab
   * and governs only its charts, but the row is the modal's — and the row is in the fixed head, so
   * putting the control there is what keeps it visible without any sticky positioning of its own.
   * Rendered only on the tab it affects: a checkbox on screen while Old charts is open would
   * claim to be doing something to charts it cannot reach.
   */
  const [sbcCorrection, setSbcCorrection] = useState(true);

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-scrim/60"
      onClick={onClose} role="dialog" aria-modal="true">
      {/* ⚠ A FIXED HEAD OVER A SCROLLING BODY, NOT ONE `overflow-auto` BOX. Everything used to
          scroll together, so on a tab twelve charts long the title, the tab bar and a tab's own
          controls all left the screen — and a control you cannot see is a setting you forget is
          set. `min-h-0` on the body is what actually lets it scroll: a flex child defaults to
          min-height:auto and would grow to its content instead, pushing the head off-screen and
          reproducing the original behaviour. Same shape as QuickValuationInputsModal. */}
      <div className="bg-card border border-neutral-800/40 rounded-xl shadow-xl w-[80vw] h-[80vh] flex flex-col p-4"
        onClick={(e) => e.stopPropagation()}>

        <div className="flex items-start justify-between gap-3 mb-2 shrink-0">
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

        {/* Tabs: the chart suite, the LongEquity revenue-growth read, and — for a single company
            only — the price-vs-FCF/share valuation read.

            ⚠ QUICK VALUATION IS NOT OFFERED FOR AN AGGREGATE. It compares a share price with free
            cash flow PER SHARE, and a basket has neither: no portfolio share exists, and the
            per-share amounts sit in different currencies and cannot be summed into one. Showing
            the tab and then explaining the emptiness inside it would be an invitation to a number
            that does not exist. */}
        {hasInstrument && (
          <div className="flex items-center gap-3 mb-3 shrink-0 flex-wrap">
          <div className="flex items-center gap-0.5 rounded-lg border border-neutral-700 p-0.5 w-fit">
            {((isAgg
              // ⚠ LAST, AND CALLED WHAT IT IS. These charts are superseded by the three reads to
              // their left; keeping them first made the modal open on the oldest thing in it.
              ? [['longequity', 'Long Equity'], ['fundamentals', 'Old charts']]
              : [['longequity', 'Long Equity'], ['quickval', 'Quick Valuation'],
                ['deepval', 'Deep Valuation'], ['fundamentals', 'Old charts']]
            ) as [Tab, string][]).map(([t, label]) => (
              <button key={t} type="button" onClick={() => openTab(t)}
                className={`px-3 py-1 rounded-md text-xs font-medium transition-colors ${
                  tab === t ? 'bg-accent-600 text-fg-strong'
                    : 'text-fg-muted hover:text-fg-strong hover:bg-overlay/5'}`}>
                {label}
              </button>
            ))}
          </div>

          {/* Right of the tabs, on the same row. Only on the tab it governs — and it governs four
              of that tab's charts, which is why it is a tab-level control rather than something
              inside one card. Stock-based compensation is a real cost paid in shares that never
              leaves the cash-flow statement, so reported FCF flatters anyone paying in equity;
              ticked by default, because the uncorrected figure is the flattering one. */}
          {tab === 'longequity' && (
            <label className="ml-auto flex items-center gap-2 text-[11px] text-fg-soft cursor-pointer"
              title="Subtract stock-based compensation from free cash flow before computing FCF margin, FCF yield, cash return on capital and FCF / Net Income. ⚠ No effect on ROIC, which is GuruFocus's own published ratio — there is no numerator of ours to adjust.">
              <input type="checkbox" checked={sbcCorrection}
                onChange={(e) => setSbcCorrection(e.target.checked)}
                className="accent-accent-600 w-3.5 h-3.5" />
              {/* ⚠ NO STATE-DEPENDENT TEXT HERE. It used to append "— FCF net of stock comp" /
                  "— FCF as reported", two different widths, on a right-aligned (`ml-auto`) label —
                  so the checkbox jumped sideways on every toggle, away from the pointer that had
                  just clicked it. The state is already legible from the box itself, the affected
                  card titles change with it, and the full explanation is in the label's `title`. */}
              SBC correction
            </label>
          )}
          </div>
        )}

        {/* The ONLY scrolling region. A tab that wants its own always-visible controls makes
            them `sticky top-0` inside here — see LongEquityTab. */}
        <div className="flex-1 min-h-0 overflow-auto">
        {/* ⚠ A VISITED TAB STAYS MOUNTED. This was a conditional chain, so switching tabs
            UNMOUNTED the previous one and threw away everything it had: its fetches, its parsed
            series, its toggles, its scroll position. Switching back re-ran every request from
            scratch, so flipping between two tabs paid for both of them again on every flip — and
            a Deep Valuation is several seconds of work to rebuild something the browser had a
            moment ago.

            Mounted on FIRST VISIT and hidden thereafter, never mounted up front: pre-mounting all
            four would move the cost to modal-open, which is the one moment the reader is
            definitely waiting. So the first visit to a tab costs what it always did, and every
            visit after it is instant.

            ⚠ `hidden`, NOT A ZERO-HEIGHT WRAPPER. `display:none` takes the tab out of layout
            entirely, so a hidden tab cannot contribute scroll height to the shared container or
            steal a click. Each chart therefore MOUNTS while visible and measures correctly;
            recharts' ResponsiveContainer re-measures on the resize that showing it fires. */}
        {(visited.has('quickval') && !isAgg && isin) && (
          <div className={tab === 'quickval' ? undefined : 'hidden'}>
            <QuickValuationTab isin={isin} name={name} />
          </div>
        )}
        {(visited.has('deepval') && !isAgg && isin) && (
          <div className={tab === 'deepval' ? undefined : 'hidden'}>
            {/* Keyed on the ISIN: the panel reads its saved assumptions in a state initialiser, so
                a different instrument has to remount to pick up its own overrides. */}
            <DeepValuationTab key={isin} isin={isin} name={name} />
          </div>
        )}
        {(visited.has('longequity') && hasInstrument) && (
          <div className={tab === 'longequity' ? undefined : 'hidden'}>
            <LongEquityTab isin={isin} name={title} basket={basket} portfolioId={portfolioId}
              sbcCorrection={sbcCorrection} />
          </div>
        )}
        {visited.has('fundamentals') && (
          <div className={tab === 'fundamentals' ? undefined : 'hidden'}>
            {/* An aggregate gets the SAME chart suite, blended across its holdings, with the
                coverage breakdown beneath it — how much of the book those charts actually span,
                and which holdings are missing. */}
            {isAgg ? (
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
        )}
        {/* The selected tab has nothing to render at all — an aggregate on a single-company tab,
            or no instrument. Kept as its own branch so the empty case cannot be confused with a
            tab that simply has not been visited yet. */}
        {!hasInstrument && !isin && (
          <p className="text-sm text-fg-subtle py-16 text-center">No instrument to look up.</p>
        )}
        </div>
      </div>
    </div>
  );
}
