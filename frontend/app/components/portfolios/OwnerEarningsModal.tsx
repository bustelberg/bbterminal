'use client';

import { useEffect, useMemo, useState } from 'react';
import { trace } from '../../../lib/debugTrace';
import { type Basket } from './types';
import FundamentalCharts from './FundamentalCharts';
import FundamentalCoverage from './FundamentalCoverage';
import LongEquityTab from './LongEquityTab';
import TablesTab from './TablesTab';
import QuickValuationTab from './QuickValuationTab';
import DeepValuationTab from './DeepValuationTab';
import PortfolioFundamentalsRefresh, { type RefreshScope } from './PortfolioFundamentalsRefresh';
import LangSwitch from '../LangSwitch';
import { useLang } from '../../../lib/i18n';

type Tab = 'fundamentals' | 'longequity' | 'quickval' | 'deepval' | 'tables';

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
  isin, name, bookName, sharePct, basket, portfolioId, refreshScope, onClose,
}: {
  isin?: string;
  name?: string | null;
  /**
   * The BOOK this modal was opened from — "Bustelberg Offensief" — as distinct from the slice of it
   * on screen.
   *
   * ⚠⚠ WITHOUT IT THE HEADER NAMES ONLY THE SLICE, AND THE SLICE IS NOT AN IDENTITY. Opening the
   * equity sleeve of a model portfolio gave "Fundamental · Stocks · group": true, and true of every
   * book on the page. Which portfolio's stocks was nowhere in the dialog — not in the title, not in
   * a tooltip — so two of these open side by side were indistinguishable, and a reader who came in
   * from a row three clicks ago had nothing to check against.
   *
   * ⚠ A SEPARATE PROP, NOT `refreshScope.name`. That one is provenance for a WRITE (what a fill
   * would act on) and is deliberately allowed to differ from what is displayed — see its own note
   * about `portfolioId`. Reading a label out of it would tie the heading to the refresh button's
   * scoping rules, so the day one changes the other silently follows. Two facts, two props.
   */
  bookName?: string | null;
  /**
   * How much of the book this slice IS, as a percentage — "Stocks, 62.4% of the portfolio".
   *
   * ⚠⚠ IT REPLACES THE WORD "group", WHICH WAS A CATEGORY WHERE A QUANTITY BELONGS. "Stocks · group"
   * told the reader what kind of thing they had opened, which they already knew from having clicked
   * it; the fact they cannot get from anywhere else on this screen is how much of the book these
   * charts actually speak for. A 62% slice and a 4% one produce identically confident-looking lines.
   *
   * ⚠ IT IS THE ALLOCATION SLICE'S FIGURE, NOT THE BASKET'S — see `onFundamental` in
   * `PortfolioAnalysisModal`. The basket drops cash and anything unmapped, so its own total is the
   * part we can chart rather than the part the portfolio holds.
   *
   * Absent for a whole portfolio (it is trivially 100%) and for a single instrument.
   */
  sharePct?: number | null;
  basket?: Basket;
  portfolioId?: number;    // a whole model portfolio, resolved to a basket server-side
  /**
   * What the fundamentals refresh is scoped to — the book this modal was opened FROM, either as a
   * stored model portfolio or as the basket of ISINs an unpaired account resolves to.
   *
   * ⚠ NOT `portfolioId`, AND IT MUST NOT BE. That one means "this modal is showing a whole
   * portfolio as an aggregate" and drives `isAgg`, which decides the tab set: reusing it to carry
   * provenance for a single instrument would silently strip Quick and Deep Valuation from the
   * modal. Two facts, two props.
   */
  refreshScope?: RefreshScope;
  onClose: () => void;
}) {
  const isAgg = !!basket || portfolioId != null;
  const title = basket?.label ?? name ?? isin ?? '';
  /**
   * The book's name, when it adds something the title does not already say.
   *
   * ⚠ SUPPRESSED WHEN IT WOULD ONLY REPEAT. Opened on the WHOLE portfolio the title already IS the
   * book, and "Bustelberg Offensief   Bustelberg Offensief · portfolio" reads as a rendering fault
   * rather than as emphasis. It earns its place exactly when the two differ — which is the group
   * case ("Stocks"), the one that had no identity at all.
   */
  const book = isAgg && bookName && bookName !== title ? bookName : null;
  /**
   * What every surface inside this modal calls the thing it is charting — the heading, the hover on
   * each line, the per-holding drill-down, and the name an ingest is filed under.
   *
   * ⚠⚠ ONE STRING, COMPUTED ONCE. These used to read `title` independently, so the chart hovers
   * said "Stocks" while the row that opened them said "Bustelberg Offensief" — the same series
   * under two names on two screens, which is indistinguishable from two different series.
   */
  const subject = book ?? title;
  /**
   * Everything that is NOT the name, on one muted line above it: the dialog, and which slice of the
   * book is on screen.
   *
   * ⚠⚠ THE NAME GETS ITS OWN LINE BECAUSE A ROW OF EQUAL WORDS HIDES IT. Run together as
   * "Fundamental  Bustelberg Offensief  Stocks · group" the one word that says WHOSE book this is
   * sits between two that do not, at the same size, and the eye has no reason to stop on it. An
   * eyebrow-over-title split is the ordinary way round that: the small line answers "what am I
   * looking at", the large one answers "at what", and nothing has to be read left-to-right to be
   * found.
   *
   * ⚠ THE SLICE ONLY APPEARS WHEN IT IS NOT THE SUBJECT. Opened on the whole book the title IS the
   * book, so it belongs on the title line and the eyebrow reduces to "Fundamental" alone — naming
   * it in both places would be the same repetition `book` already exists to suppress.
   *
   * ⚠⚠ AND THE SLICE IS FOLLOWED BY ITS **WEIGHT**, NOT BY THE WORD "group". A reader who clicked
   * the Stocks header knows it is a group; what they cannot see anywhere on this screen is how much
   * of the book it is — and a 62% sleeve and a 4% one draw equally confident lines. `sharePct` is
   * the allocation slice's own figure, so it agrees with the bars on the screen behind this one.
   */
  const eyebrow = ['Fundamental', book ? title : null,
    book && sharePct != null ? `${sharePct.toFixed(1)}% of the portfolio` : null,
  ].filter(Boolean).join(' · ');
  // Bumped whenever the coverage panel ingests something, so the blended charts re-fetch and pick
  // up the data that ingest just created (they'd otherwise show the pre-ingest blend until reopen).
  // Used as FundamentalCharts' `key` — a remount is the clean way to force one fresh blend fetch.
  const [blendKey, setBlendKey] = useState(0);
  // ⚠ MEMOISED, OR IT REFETCHES FOR EVER. A fresh object literal here is a new reference every
  // render, and it is in the child's effect deps — so each fetch would set state, re-render, and
  // fetch again. The blend is an expensive multi-company query; this is not a micro-optimisation.
  const blend = useMemo(() => ({ basket, portfolioId }), [basket, portfolioId]);
  // ⚠ A MISSING CONTROL LOOKS IDENTICAL TO A BROKEN ONE, SO THE ABSENCE EXPLAINS ITSELF. The
  // fundamentals refresh needs a real model portfolio to scope to, and the Analyse modal only has
  // one when it was opened WITH an id — `/portfolios` always passes one, the overview panel's
  // `analyse` state has `id?: number` and an account or ad-hoc basket row carries none. Without
  // this line the button is simply not there and there is nothing on screen or in the log to say
  // why, which is exactly the state this codebase keeps removing.
  /**
   * What the refresh acts on — DERIVED FROM WHAT THIS MODAL IS SHOWING, not from how it was
   * opened.
   *
   * ⚠⚠ A CONTROL'S SCOPE MUST MATCH ITS SCREEN, OR IT IS A TRAP. Opened on one company, the modal
   * charts that company; a button beside those charts that quietly refetched the other nineteen
   * holdings would spend nineteen API calls the reader never asked for, and take minutes to do
   * something they cannot see. Opened on the whole book (a basket, or a portfolio aggregate) the
   * same button correctly means all of it.
   *
   * ⚠ ONE COMPANY IS SENT AS A BASKET OF ONE, so there is no third code path — see `RefreshScope`.
   */
  const scope = useMemo<RefreshScope | undefined>(
    () => (!isAgg && isin ? { kind: 'company', isin, name: name || isin } : refreshScope),
    [isAgg, isin, name, refreshScope]);

  useEffect(() => {
    if (!scope) {
      trace('fundamentals', 'no Refresh-fundamentals button: this modal is showing neither a '
        + 'single instrument nor a book (no isin, no basket, no portfolio) to scope the fill to.');
    }
  }, [scope]);
  // ⚠ THE FIRST TAB, WHICH IS NO LONGER `fundamentals`. Demoting those charts to last and naming
  // them "Old charts" while still opening on them would say two opposite things at once — and it
  // is the one tab both an aggregate and a single company have, so the landing tab never depends
  // on which the modal was opened for.
  const [tab, setTab] = useState<Tab>('longequity');
  /** Which tabs have been opened at least once — the mount set. A tab enters it on first visit
   *  and never leaves, so its data survives every subsequent switch. Seeded with the landing tab
   *  so it mounts on open like it always did.
   *
   *  ⚠ THIS ONLY SURVIVES AS LONG AS THE MODAL DOES, WHICH IS WHY IT IS NOT THE WHOLE STORY. Close
   *  it and reopen the same holding, open a drill-down over a card that just fetched the identical
   *  body, flip the cadence to quarterly and back — all of those were full re-reads, because the
   *  mount set cannot outlive the dialog. The requests themselves are now cached a level down, in
   *  `lib/readCache.ts` (served by `apiFetch`, dropped by any successful write), so a tab that HAS
   *  to remount still costs nothing. The two work together: this one keeps the parsed charts and
   *  the toggles, that one keeps the payloads. */
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
  /** ⚠ PERSISTED PER BROWSER, NOT PER MODAL — a language is a property of the reader, so it has to
   *  survive closing the dialog. See `lib/i18n.ts` for why it cannot be seeded synchronously. */
  const [lang, setLang] = useLang();

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
            {/* ⚠ THE EYEBROW IS EVERYTHING THAT IS TRUE OF OTHER ROWS TOO — the dialog, the slice,
                the identifier. The line under it is the one thing that is true only of this one.
                Keeping them at the same size is what made the portfolio's name disappear into a
                sentence; the size difference IS the answer to "which of these words matters". */}
            {/* ⚠ SMALL AND MUTED, NOT UPPERCASED. The size and the ink already set the name below
                apart; small caps on top of that was decoration, and it stopped being harmless once
                the line carried a sentence — "62.4% OF THE PORTFOLIO" shouts a footnote. */}
            <div className="flex items-baseline gap-2 flex-wrap text-sm text-fg-muted">
              <span>{eyebrow}</span>
              {/* ⚠ NOT WHEN IT IS THE SUBJECT. A company with no name on file is titled BY its ISIN
                  below, where printing it twice would read as two different identifiers. */}
              {!isAgg && name && <span className="font-mono">{isin}</span>}
            </div>
            {/* ⚠ `leading-tight` IS WHAT PAYS FOR THE SIZE. This sits in the modal's FIXED head,
                above a body that scrolls — every pixel here is taken off the charts for the whole
                session, not just at the top of the scroll. Default line-height at 2xl would add
                more than the type itself does. */}
            <div className="text-2xl font-semibold text-fg-strong truncate leading-tight">{subject}</div>
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
              // ⚠ `Tables` SITS BESIDE `Long Equity`, NOT AFTER `Old charts`. It is the same three
              // reads those cards draw, summarised — so it belongs with them, and putting it last
              // would file the newest thing in the modal behind the one named for being superseded.
              ? [['longequity', 'Long Equity'], ['tables', 'Tables'], ['fundamentals', 'Old charts']]
              : [['longequity', 'Long Equity'], ['tables', 'Tables'],
                ['quickval', 'Quick Valuation'],
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

          {/* Right of the tabs, on the same row: the portfolio-wide fundamentals refresh, then the
              SBC toggle.

              ⚠ THE REFRESH IS NOT TAB-SCOPED — it fetches data every tab reads, so hiding it with
              the SBC box would make the same action appear and disappear depending on which chart
              you were looking at. It sits in the right-aligned group so it keeps its place when
              the SBC label comes and goes; `ml-auto` moved onto this wrapper for the same reason. */}
          <div className="ml-auto flex items-center gap-3 min-w-0">
          {scope && (
            <PortfolioFundamentalsRefresh scope={scope}
              onDone={() => setBlendKey((k) => k + 1)} />
          )}
          {/* ⚠ ALWAYS ON, NOT ONLY ON THE TAB IT CURRENTLY TRANSLATES. It was tab-scoped first, on
              the same reasoning as the SBC checkbox below — a control that governs nothing on the
              visible tab is noise. That was wrong here, and reported immediately as "I do not see
              it": a LANGUAGE is a property of the reader, not of one tab, so a switch that appears
              and disappears as you move along the tab bar reads as a control that is missing rather
              than one that is scoped. The tab-scoped version also could not be FOUND — you have to
              already be on `Tables` to discover the thing that translates `Tables`.

              Its `title` carries the coverage instead, which is the honest place for it: the
              control is stable, the scope is stated. */}
          <LangSwitch lang={lang} onChange={setLang}
            title="Language. English is the source; so far only the Tables tab is translated — the other tabs stay English whichever is selected." />
          {/* Only on the tab it governs — and it governs four of that tab's charts, which is why it
              is a tab-level control rather than something inside one card. Stock-based compensation
              is a real cost paid in shares that never leaves the cash-flow statement, so reported
              FCF flatters anyone paying in equity; ticked by default, because the uncorrected
              figure is the flattering one. */}
          {tab === 'longequity' && (
            <label className="flex items-center gap-2 text-[12px] text-fg-soft cursor-pointer"
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
            {/* ⚠ `subject`, NOT `title` — this name reaches the chart hovers, the per-holding
                drill-down and the ingest, and on a group it was the bare "Stocks". See `subject`. */}
            <LongEquityTab isin={isin} name={subject} basket={basket} portfolioId={portfolioId}
              sbcCorrection={sbcCorrection} />
          </div>
        )}
        {(visited.has('tables') && hasInstrument) && (
          <div className={tab === 'tables' ? undefined : 'hidden'}>
            {/* ⚠ THE SAME `holdingsTarget` SHAPE THE LONG EQUITY CARDS BUILD, and deliberately
                `cadence: 'annual'` — a 5-year window of QUARTERS is fifteen months, and a "5y CAGR"
                off it would be wrong by a factor of four while looking entirely plausible. */}
            <TablesTab
              holdingsTarget={isAgg
                ? (basket
                  ? { holdings: basket.holdings.map((h) => ({ isin: h.isin, name: h.name, weight: h.weight })), cadence: 'annual' }
                  : { portfolio_id: portfolioId, cadence: 'annual' })
                : { holdings: [{ isin: isin ?? '', weight: 1 }], cadence: 'annual' }}
              holdingsName={subject}
              sbcCorrection={sbcCorrection}
              lang={lang} />
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
                <FundamentalCharts key={blendKey} blend={blend} name={subject} />
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
