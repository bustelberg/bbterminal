'use client';

import { useState } from 'react';
import { API_URL } from '../../../lib/apiUrl';
import { traceError } from '../../../lib/debugTrace';
import { invalidateReadCache } from '../../../lib/readCache';
import { cancelJob, startJob } from '../../../lib/stores/jobs';

/**
 * Refresh the GuruFocus fundamentals for every company the PORTFOLIO holds — not just the one on
 * screen, because the next holding you open is the one you would otherwise wait for.
 *
 * ⚠ IT IS A JOB, SO PROGRESS BELONGS TO THE TOAST STACK. `startJob` returns a handle and
 * `lib/stores/jobs.ts` draws the card from the root layout: one line, a bar, the running GuruFocus
 * quota spend and a Cancel — and it OUTLIVES this modal, which is the point. A fill over twenty
 * holdings is minutes, and a reader who closes the dialog has not cancelled anything. A progress
 * bar drawn in here would vanish with the dialog while the work carried on invisibly.
 *
 * ⚠ `force=true`, OR IT DOES NOTHING. Two caches sit in front of this: `needs()` skips a company
 * whose sentinel row exists, and `is_cache_fresh` replays the stored GuruFocus blob for months
 * after the quarter it is missing (a quarterly filer's blob counts as fresh for ~4.5 months). That
 * pairing is exactly why the fundamentals grid's per-row Fetch is a no-op for a company that
 * already has data — pressing it for ASML today fetches nothing.
 *
 * ⚠ `only_due=true` IS WHAT KEEPS IT CHEAP. The detector (`ingest.earnings.due`) drops the holdings
 * whose next fiscal period cannot plausibly have been filed yet, so a press costs one API call per
 * company that might actually have something — and nothing at all when none do.
 */
/**
 * What the refresh is scoped to.
 *
 * ⚠⚠ BOTH SHAPES ARE NEEDED, AND ASSUMING THE FIRST HID THE BUTTON WHERE IT WAS MOST WANTED. On
 * /management-dashboard, `openModal` carries a model-portfolio id ONLY when the account is paired
 * with a fixed model; every other book resolves its own ISINs into a basket and opens the same
 * Analyse view. Requiring an id therefore made the control vanish on the rows a reader is most
 * likely to be on — an account is the unit of work here, the model is the optional extra.
 */
/** Which GuruFocus feed a UNIVERSE fill spends on. See the ⚠⚠ on `run`. */
export type IndexFeeds = 'statements' | 'estimates' | 'smart';

export type RefreshScope =
  | { kind: 'portfolio'; id: number; name: string }
  | { kind: 'basket'; holdings: { isin: string }[]; name: string }
  // ⚠ ONE COMPANY IS A BASKET OF ONE — same endpoint, same fill, one API call. It is a separate
  // `kind` only so the wording can be right: "every company in Fortinet Inc." is not a sentence.
  // The scope follows what the modal is SHOWING, which is the only rule a reader can predict.
  | { kind: 'company'; isin: string; name: string }
  /**
   * An INDEX's constituents — the benchmark line drawn beside the book.
   *
   * ⚠ A DIFFERENT ENDPOINT, ON PURPOSE, AND A DIFFERENT SPEND. The three above go through
   * `/api/airs/…`, which resolves ISINs to companies; an index is already a list of company rows
   * (`/api/benchmarks/index/{label}/…`). Sharing this component is still right — one place knows
   * how to start a fundamentals fill, follow its toast, and drop the read cache when it lands —
   * but see `run()`: the index is deliberately NOT forced.
   */
  | { kind: 'universe'; label: string; name: string; feeds?: IndexFeeds };

export default function PortfolioFundamentalsRefresh({ scope, onDone, label }: {
  scope: RefreshScope;
  /** Called when the fill ends without failing, so the caller can re-read what it wrote. */
  onDone: () => void;
  /**
   * What the button says at rest. Default: "Refresh fundamentals" (the tab row, where it is the
   * only such control on screen), or "Fetch missing fundamentals" for an index.
   *
   * ⚠ IT EXISTS BECAUSE TWO OF THESE CAN SHARE A SCREEN. In the drill-down the book's fill and the
   * index's fill sit one above the other, and two buttons reading the same words are one button as
   * far as the reader is concerned — so there they name what they act ON ("Refresh portfolio" /
   * "Refresh benchmark"), which is the thing that differs. The `title` still carries what each
   * will actually do, including what the index one will NOT.
   */
  label?: string;
}) {
  const [busy, setBusy] = useState(false);
  const [note, setNote] = useState<string | null>(null);
  /**
   * The running job, so this button can BE the Cancel while it runs.
   *
   * ⚠ THE TOAST'S CANCEL IS NOT ENOUGH ON ITS OWN. A fill over twenty holdings is minutes, and the
   * reader who wants to stop it is looking at the button they just pressed — not at the corner of
   * the screen. Same shape as the Overview scan button: one control, two states.
   *
   * ⚠ AND IT IS THE JOB ID, NOT `busy`, THAT DECIDES. There is a gap between the press and the id
   * coming back, and offering a Cancel in that window would be a button that cannot do what it
   * says. `busy && !jobId` renders it inert for exactly that gap.
   */
  const [jobId, setJobId] = useState<string | null>(null);

  const run = async () => {
    setBusy(true);
    try {
      const q = '?force=true&only_due=true';
      // A company and a basket post the same body — one holding or many. `/api/airs/basket/…` is
      // already the codebase's shape for "an ad-hoc set of holdings"; a single stock is a set of
      // one, which is exactly how `/api/airs/basket/analysis` treats it.
      const holdings = scope.kind === 'company' ? [{ isin: scope.isin }]
        : scope.kind === 'basket' ? scope.holdings.map((h) => ({ isin: h.isin }))
          : null;
      /**
       * ⚠⚠ THE INDEX IS NOT FORCED, AND THAT ASYMMETRY IS THE POINT.
       *
       * A book is ~20 holdings and `only_due=true` bounds a forced press to the companies that
       * could plausibly have filed since we last looked — usually none, often a handful. An index
       * is 206 constituents on the S&P and ~1,900 on ACWI, with no due-filter on that endpoint: a
       * forced press from a chart drill-down would be a four-figure quota spend nobody asked for.
       * Un-forced, it fills exactly the constituents MISSING the statements feed, which is what
       * makes the benchmark line cover more of its index — the reason to press it.
       *
       * ⚠ SO IT CANNOT REFRESH A CONSTITUENT WE ALREADY HOLD, EVEN A STALE ONE, and the button
       * says so. `needs()` answers "is the sentinel row present", not "is it current" — the
       * deliberate full reload lives on the /benchmarks fundamentals grid, which is that page's
       * whole subject.
       */
      /**
       * ⚠⚠ WHICH FEED AN INDEX FILL SPENDS ON, AND THE DEFAULT IS STILL `statements`.
       *
       * `statements` is one call per constituent and fills every column the fundamentals grid
       * and the reported side of the Long Equity tab draw. It never asks for a consensus — so an
       * index's analyst-expectation line can never appear however often that button is pressed.
       *
       * `estimates` is the targeted fill for exactly that: one call per constituent MISSING a
       * consensus and none for the rest. Measured 2026-08-14 on ACWI — 351 of 1,715 charted names
       * carry one, so it is ~1,364 calls against the ~5,145 that fetching all three feeds for
       * every constituent would cost, of which two thirds would refill data already held.
       */
      const url = scope.kind === 'universe'
        ? `${API_URL}/api/benchmarks/index/${encodeURIComponent(scope.label)}/fundamentals/ingest/job`
          + (scope.feeds ? `?feeds=${scope.feeds}` : '')
        : holdings
          ? `${API_URL}/api/airs/basket/fundamentals/ingest/job${q}`
          : `${API_URL}/api/airs/model-portfolios/${(scope as { id: number }).id}`
            + `/fundamentals/ingest/job${q}`;
      const { id, done, body } = await startJob(
        url,
        `${scope.name} fundamentals`,
        holdings
          ? { headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ holdings, label: scope.name }) }
          : undefined);
      setJobId(id);
      // ⚠⚠ THE UNREACHED REMAINDER IS TWO DIFFERENT ABSENCES, AND MERGING THEM MAKES A CORRECT
      // ANSWER LOOK BROKEN. Measured: AITopSelectie reaches 20 of 20, while BUS_Neutraal_FX reaches
      // 24 of 40 — of the other 16, ELEVEN are ETFs, funds or cash, which HAVE no company
      // fundamentals by definition, and only FIVE are a real gap worth fixing. "24 of 40" alone
      // reads as a failure in the second case and says nothing about which five to chase.
      const c = body as unknown as {
        holdings?: number; reachable?: number; no_fundamentals?: number; no_company?: number;
      };
      if (c?.holdings != null) {
        if (scope.kind === 'company') {
          // ⚠ "1 of 1 have company fundamentals" IS NOISE; the only thing worth saying about a
          // single instrument is when it CANNOT be fetched — an ETF or a bond has no accounts, and
          // a silent no-op would read as a broken button.
          setNote(c.reachable ? null
            : c.no_fundamentals ? 'no fundamentals exist for this instrument (a fund, bond or cash)'
              : '⚠ no company record for this ISIN');
        } else {
          const bits = [`${c.reachable} of ${c.holdings} have company fundamentals`];
          if (c.no_fundamentals) bits.push(`${c.no_fundamentals} funds/bonds/cash (none exist)`);
          if (c.no_company) bits.push(`⚠ ${c.no_company} with no company record`);
          setNote(bits.join(' · '));
        }
      }
      const job = await done;
      // ⚠ RE-READ ON ANYTHING BUT A FAILURE, INCLUDING A CANCEL. A cancelled fill has still loaded
      // every company it got through, and leaving the pre-fill charts on screen would hide real
      // work that was really done.
      //
      // ⚠⚠ AND DROP THE CACHED READS FIRST, OR THE RE-READ IS SERVED FROM BEFORE THE FILL. This is
      // the ONE write on this screen the automatic rule in `apiFetch` cannot cover: that fires when
      // the request succeeds, and what succeeded here was merely STARTING a job that then ran for
      // minutes. Every chart would refetch, hit the entries cached during the fill, and show the
      // pre-fill book — a refresh button that visibly does nothing.
      if (job.status !== 'failed') {
        invalidateReadCache(`fundamentals fill finished for ${scope.name}`);
        onDone();
      }
    } catch (e) {
      traceError('fundamentals', `could not start the fill for ${scope.name}`, e);
      setNote('could not start — see the console');
    } finally {
      setBusy(false);
      setJobId(null);
    }
  };

  /** Stop the fill. ⚠ NO INLINE MESSAGE — `cancelJob` puts "cancelling…" on the job's own card the
   *  instant it is pressed, and that card carries the outcome and how far it got. Two places
   *  reporting one job is two places to keep in step. */
  const cancel = async () => {
    if (jobId) await cancelJob(jobId);
  };

  return (
    <span className="flex items-center gap-2 min-w-0">
      {/* The count sits LEFT of the button so the button keeps a fixed position as the text
          arrives — a control that slides sideways when its own result lands is a control you
          have to chase with the pointer. */}
      <span className="text-[11px] text-fg-faint truncate max-w-[22rem]">{note}</span>
      {/* ⚠ ONE CONTROL, TWO STATES — the button BECOMES the Cancel while the fill runs. The toast
          carries a Cancel too and both are correct, but a fill is minutes and the reader who wants
          to stop it is looking at the button they just pressed, not at the corner of the screen.
          Inert only in the gap between the press and the job id arriving. */}
      <button type="button"
        onClick={() => { if (jobId) { void cancel(); } else { void run(); } }}
        disabled={busy && !jobId}
        title={jobId
          ? 'Stop the fill. The companies already being read finish first (up to eight at a time), '
            + 'and everything loaded so far is kept — the charts still re-read afterwards.'
          : (scope.kind === 'company'
            ? `Fetch the latest GuruFocus fundamentals for ${scope.name}, if it could plausibly have `
              + 'filed since we last looked. One API call, and none at all when its next quarter '
              + 'cannot be out yet.'
            : scope.kind === 'universe'
              // ⚠ IT SAYS WHAT IT WILL *NOT* DO. An index fill is un-forced (see `run`), so it adds
              // the constituents we are missing and cannot update one we already hold — press it
              // twice and the second press is free and changes nothing, which without this reads as
              // a broken button rather than as a deliberate scope.
              ? `Fetch GuruFocus fundamentals for the ${scope.name} constituents we are MISSING — `
                + 'the ones absent from the table below, which is why the index line covers less of '
                + 'its index than 100%. One API call each. It does not re-fetch a constituent we '
                + 'already hold, however old: that full reload lives on the /benchmarks '
                + 'fundamentals grid.'
              : `Fetch the latest GuruFocus fundamentals for every company in ${scope.name} that could `
                + 'plausibly have filed since we last looked — one API call each, and none for a '
                + 'company whose next quarter cannot be out yet.')
            + ' Progress, the running quota spend and a Cancel appear in the pop-ups bottom-right, '
            + 'and carry on if you close this.'}
        className={`text-[12px] px-2.5 py-1 rounded-lg border transition-colors
                    disabled:opacity-50 disabled:cursor-wait whitespace-nowrap shrink-0 ${jobId
          ? 'border-warn-500/50 text-warn-400 hover:bg-warn-500/10'
          : 'border-neutral-700 text-fg-muted hover:bg-overlay/5'}`}>
        {/* ⚠ THE TWO STATES OUTRANK THE CALLER'S LABEL. Whatever the button is named at rest, while
            it runs it says what pressing it will now DO — a control that keeps its old name while
            its action has changed underneath is the trap this replaced. */}
        {jobId ? 'Cancel'
          : busy ? 'Refreshing…'
            : label ?? (scope.kind === 'universe' ? 'Fetch missing fundamentals'
              : 'Refresh fundamentals')}
      </button>
    </span>
  );
}
