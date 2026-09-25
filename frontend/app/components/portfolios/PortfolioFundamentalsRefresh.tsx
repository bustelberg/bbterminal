'use client';

import { useEffect, useState } from 'react';
import { API_URL } from '../../../lib/apiUrl';
import { traceError } from '../../../lib/debugTrace';
import { invalidateReadCache } from '../../../lib/readCache';
import { cancelJob, jobsStore, startJob, watchJob } from '../../../lib/stores/jobs';
import { fundamentalJobMessage } from '../jobs/fundamentalJobMessage';
import { useFundamentalChromeCopy } from './fundamentalChromeCopy';

/**
 * Refresh the GuruFocus fundamentals for every company the PORTFOLIO holds — not just the one on
 * screen, because the next holding you open is the one you would otherwise wait for.
 *
 *  It is a job, so progress belongs to the toast stack. `startJob` returns a handle and
 * `lib/stores/jobs.ts` draws the card from the root layout: one line, a bar, the running GuruFocus
 * quota spend and a Cancel — and it OUTLIVES this modal, which is the point. A fill over twenty
 * holdings is minutes, and a reader who closes the dialog has not cancelled anything. A progress
 * bar drawn in here would vanish with the dialog while the work carried on invisibly.
 *
 *  `force=true`, OR IT DOES NOTHING. Two caches sit in front of this: `needs()` skips a company
 * whose sentinel row exists, and `is_cache_fresh` replays the stored GuruFocus blob for months
 * after the quarter it is missing (a quarterly filer's blob counts as fresh for ~4.5 months). That
 * pairing is exactly why the fundamentals grid's per-row Fetch is a no-op for a company that
 * already has data — pressing it for ASML today fetches nothing.
 *
 *  `only_due=true` IS WHAT KEEPS IT CHEAP. The detector (`ingest.earnings.due`) drops the holdings
 * whose next fiscal period cannot plausibly have been filed yet, so a press costs one API call per
 * company that might actually have something — and nothing at all when none do.
 */
/**
 * What the refresh is scoped to.
 *
 *  Both shapes are needed, and assuming the first hid the button where it was most wanted. On
 * /management-dashboard, `openModal` carries a model-portfolio id ONLY when the account is paired
 * with a fixed model; every other book resolves its own ISINs into a basket and opens the same
 * Analyse view. Requiring an id therefore made the control vanish on the rows a reader is most
 * likely to be on — an account is the unit of work here, the model is the optional extra.
 */
/** Which GuruFocus feed a UNIVERSE fill spends on. See the  on `run`. */
export type IndexFeeds = 'statements' | 'estimates' | 'smart';

export type RefreshScope =
  | { kind: 'portfolio'; id: number; name: string }
  | { kind: 'basket'; holdings: { isin: string }[]; name: string }
  //  One company is a basket of one — same endpoint, same fill, one API call. It is a separate
  // `kind` only so the wording can be right: "every company in Fortinet Inc." is not a sentence.
  // The scope follows what the modal is SHOWING, which is the only rule a reader can predict.
  | { kind: 'company'; isin: string; name: string }
  /**
   * An INDEX's constituents — the benchmark line drawn beside the book.
   *
   *  A different endpoint, on purpose, and a different spend. The three above go through
   * `/api/airs/…`, which resolves ISINs to companies; an index is already a list of company rows
   * (`/api/benchmarks/index/{label}/…`). Sharing this component is still right — one place knows
   * how to start a fundamentals fill, follow its toast, and drop the read cache when it lands —
   * but see `run()`: the index is deliberately NOT forced.
   */
  | { kind: 'universe'; label: string; name: string; feeds?: IndexFeeds };

export const refreshScopeKey = (s: RefreshScope): string => s.kind === 'portfolio'
  ? `portfolio:${s.id}`
  : s.kind === 'universe' ? `universe:${s.label}`
    : s.kind === 'company' ? `company:${s.isin}`
      : `basket:${s.holdings.map((h) => h.isin).sort().join('|')}`;

/** The primary selection followed by its comparison target, without refreshing one scope twice. */
export function refreshScopes(scope: RefreshScope, additional?: RefreshScope): RefreshScope[] {
  return additional && refreshScopeKey(additional) !== refreshScopeKey(scope)
    ? [scope, additional] : [scope];
}

export default function PortfolioFundamentalsRefresh({ scope, additionalScope, onDone, label, everything,
  allPeriods = false, broadcast = true }: {
  scope: RefreshScope;
  /** A selected benchmark/comparison refreshed after the primary scope by the same button. */
  additionalScope?: RefreshScope;
  /**
   * Fetch EVERYTHING the Fundamental modal draws, not just the statements feed.
   *
   *  "Refresh fundamentals" DID NOT REFRESH WHAT THE PAGE UNDER IT SHOWED, and on Quick
   * Valuation it refreshed almost none of it (reported 2026-08-21). The default fill is
   * `feeds=statements` — ONE of GuruFocus's three feeds — and prices are not a feed at all:
   *
   *     statements (fin)   the reported accounts. This is all the button ever fetched.
   *     indicators (ind)   `indicator_q_forward_pe_ratio` — the ONLY line left on Quick
   *                        Valuation's multiple chart, so that chart could never be refreshed.
   *     estimates  (est)   `annual_eps_nri_estimate` — the dotted consensus leg on Long Equity's
   *                        EPS card, and the forecast Quick Valuation's calculator projects from.
   *     prices             `metric_data.close_price` — a SEPARATE ingest. Today's share price and
   *                        the closes the multiple is priced off were untouched entirely.
   *
   *  So it is the whole modal, not one tab. The gap is worst on Quick Valuation but not confined
   * to it — Long Equity's forecast leg is the `est` feed too. A button whose behaviour depended on
   * which tab happened to be open would be a different button wearing the same words.
   *
   *  The cost is ~4 CALLS PER COMPANY INSTEAD OF 1 (three feeds plus a price fetch), which is why
   * this is opt-in rather than the default: the drill-down's per-row press and the index fill have
   * their own, narrower reasons to exist, and a four-figure index spend is not one of them.
   */
  everything?: boolean;
  /** Re-download the full source history, including older fiscal years that are already past due. */
  allPeriods?: boolean;
  /** Notify every mounted fundamentals card. A drill-down that reloads itself keeps this false. */
  broadcast?: boolean;
  /** Called when the fill ends without failing, so the caller can re-read what it wrote. */
  /**  OPTIONAL, AND THE CACHE DROP IS NOT. `invalidateReadCache` runs beside every call of
   *  this, unconditionally — that is what makes the new data reachable. `onDone` is only for a
   *  caller holding a MOUNTED view it must re-key (the drill-down matrix does). The Fundamental
   *  modal had one for the Old-charts tab and no longer needs it; a required no-op there would
   *  read as a caller that forgot to do something. */
  onDone?: () => void;
  /**
   * What the button says at rest. Default: "Refresh fundamentals" (the tab row, where it is the
   * only such control on screen), or "Fetch missing fundamentals" for an index.
   *
   *  It exists because two of these can share a screen. In the drill-down the book's fill and the
   * index's fill sit one above the other, and two buttons reading the same words are one button as
   * far as the reader is concerned — so there they name what they act ON ("Refresh portfolio" /
   * "Refresh benchmark"), which is the thing that differs. The `title` still carries what each
   * will actually do, including what the index one will NOT.
   */
  label?: string;
}) {
  //  The four states of this button's own label. A caller-supplied `label` still wins — see the
  // note on that prop: where two of these share a screen they name what they act ON, and that
  // string comes from the caller's own copy module, not from here.
  const chrome = useFundamentalChromeCopy();
  const [busy, setBusy] = useState(false);
  const [note, setNote] = useState<string | null>(null);
  /**
   * The running job, so this button can BE the Cancel while it runs.
   *
   *  The toast's cancel is not enough on its own. A fill over twenty holdings is minutes, and the
   * reader who wants to stop it is looking at the button they just pressed — not at the corner of
   * the screen. Same shape as the Overview scan button: one control, two states.
   *
   *  And it is the job id, not `busy`, THAT DECIDES. There is a gap between the press and the id
   * coming back, and offering a Cancel in that window would be a button that cannot do what it
   * says. `busy && !jobId` renders it inert for exactly that gap.
   */
  const [jobId, setJobId] = useState<string | null>(null);
  /** Cancel has been asked for and the workers have not stopped yet — see `cancel`. */
  const [cancelling, setCancelling] = useState(false);

  /**
   * The server's identity for the work THIS button does. `null` where there is no stable one.
   *
   *  It must match `jobs.start`'s KEY EXACTLY, because that is what the server de-duplicates on
   * and therefore the only thing that can identify "my run" from the outside.
   */
  const indexedScope = scope.kind === 'universe' ? scope
    : additionalScope?.kind === 'universe' ? additionalScope : null;
  const jobKey = indexedScope
    ? { kind: 'fundamentals.index', label: indexedScope.label }
    : null;

  /**
   *  Adopt a run already in flight, instead of offering to start another.
   *
   * This button knew it had a job only from its own React state, so reopening the modal — or
   * reloading the page — brought it back reading "Refresh benchmark" while the fill was still
   * running. Pressing it again was the obvious thing to do, and it launched a SECOND run over the
   * same 1,712 constituents: two fills sharing one global rate limiter so both crawl, and a Cancel
   * that stops exactly one of them. From the reader's seat that is a Cancel button that does
   * nothing, because a different run is still going.
   *
   * `jobs.start` now refuses to duplicate, so the press would attach rather than double — but the
   * button should not have needed pressing to find that out. `attachRunningJobs` has already put
   * every live job in the store by the time this mounts.
   */
  const runningJobs = jobsStore.use((s) => s.jobs);
  useEffect(() => {
    if (jobId || !jobKey) return;
    const live = runningJobs.find(
      (j) => j.status === 'running' && j.kind === jobKey.kind && j.label === jobKey.label);
    if (!live) return;
    setJobId(live.id);
    setBusy(true);
    setNote('already running — this button now stops it');
    //  Follow it to the end, or `busy`/`jobId` never clear and the control is stuck on Cancel
    // long after the run finished.
    void watchJob(live.id, `${indexedScope?.name ?? scope.name} fundamentals`).then((job) => {
      if (job.status !== 'failed') {
        invalidateReadCache(`fundamentals fill finished for ${indexedScope?.name ?? scope.name}`);
        if (broadcast) window.dispatchEvent(new Event('bb:fundamentals-finished'));
        onDone?.();
      }
      setBusy(false);
      setJobId(null);
      setCancelling(false);
    });
    // `runningJobs` is deliberately the only trigger — re-running on `onDone`/`scope` identity
    // would re-adopt the same job on every parent render.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [runningJobs, jobId]);

  const run = async () => {
    setBusy(true);
    let changed = false;
    try {
      //  `feeds=all` NARROWS NOTHING and `prices=true` adds the ingest that is not a feed — see
      // the `everything` prop for what the four things are and which chart each one was missing.
      const q = `?force=true&only_due=${allPeriods ? 'false' : 'true'}${everything ? '&feeds=all&prices=true' : ''}`;
      // A company and a basket post the same body — one holding or many. `/api/airs/basket/…` is
      // already the codebase's shape for "an ad-hoc set of holdings"; a single stock is a set of
      // one, which is exactly how `/api/airs/basket/analysis` treats it.
      const holdings = scope.kind === 'company' ? [{ isin: scope.isin }]
        : scope.kind === 'basket' ? scope.holdings.map((h) => ({ isin: h.isin }))
          : null;
      const url = scope.kind === 'universe'
        ? `${API_URL}/api/benchmarks/index/${encodeURIComponent(scope.label)}/fundamentals/ingest/job`
          // This is the Graphs tab's explicit Refresh benchmark control. It must refresh every
          // series it shows — reported statements (income/balance sheet/cash flow), estimates,
          // indicators and daily prices — for every reachable constituent. A smart/missing-only
          // pass leaves existing-but-old FCF/share cells untouched and makes this button lie.
          + '?force=true&feeds=all&prices=true'
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
      //  The unreached remainder is two different absences, and merging them makes a correct
      // Answer look broken. Measured: AITopSelectie reaches 20 of 20, while BUS_Neutraal_FX reaches
      // 24 of 40 — of the other 16, ELEVEN are ETFs, funds or cash, which HAVE no company
      // fundamentals by definition, and only FIVE are a real gap worth fixing. "24 of 40" alone
      // reads as a failure in the second case and says nothing about which five to chase.
      const c = body as unknown as {
        holdings?: number; reachable?: number; no_fundamentals?: number; no_company?: number;
        already_running?: boolean;
      };
      //  Say so when the press attached rather than started — see `jobs.start`. Silently adopting
      // a run in flight is right, but leaving the reader to believe they just kicked off a fresh
      // one is how "I pressed it twice and nothing changed" becomes "the button is broken".
      if (c?.already_running) setNote('already running — this button now stops it');
      else if (c?.holdings != null) {
        if (scope.kind === 'company') {
          //  "1 of 1 have company fundamentals" IS NOISE; the only thing worth saying about a
          // single instrument is when it CANNOT be fetched — an ETF or a bond has no accounts, and
          // a silent no-op would read as a broken button.
          setNote(c.reachable ? null
            : c.no_fundamentals ? 'no fundamentals exist for this instrument (a fund, bond or cash)'
              : ' no company record for this ISIN');
        } else {
          const bits = [`${c.reachable} of ${c.holdings} have company fundamentals`];
          if (c.no_fundamentals) bits.push(`${c.no_fundamentals} funds/bonds/cash (none exist)`);
          if (c.no_company) bits.push(` ${c.no_company} with no company record`);
          setNote(bits.join(' · '));
        }
      }
      const job = await done;
      changed = job.status !== 'failed';
      // Between the two serial jobs there is nothing cancellable. Leave the control disabled until
      // the benchmark job id arrives instead of sending Cancel to the already-finished first job.
      setJobId(null);
      // A pre-flight subscription refusal spends no call and is therefore a completed job, but
      // it is not a successful refresh. Keep the server's reason beside the button after the toast
      // leaves; otherwise the still-empty cards immediately invite the same impossible press.
      if (job.summary?.includes(' — unavailable:')) setNote(fundamentalJobMessage(job.summary));
      let additionalSucceeded = false;
      const extra = refreshScopes(scope, additionalScope)[1];
      if (extra && job.status !== 'cancelled') {
        // One press, two deliberately SERIAL jobs. Both use the global GuruFocus rate limiter, so
        // parallel jobs cannot finish sooner and would make both progress cards appear stalled.
        const extraQ = `?force=true&only_due=${allPeriods ? 'false' : 'true'}`
          + `${everything ? '&feeds=all&prices=true' : ''}`;
        const extraHoldings = extra.kind === 'company' ? [{ isin: extra.isin }]
          : extra.kind === 'basket' ? extra.holdings.map((h) => ({ isin: h.isin })) : null;
        const extraUrl = extra.kind === 'universe'
          ? `${API_URL}/api/benchmarks/index/${encodeURIComponent(extra.label)}`
            + '/fundamentals/ingest/job?force=true&feeds=all&prices=true'
          : extraHoldings
            ? `${API_URL}/api/airs/basket/fundamentals/ingest/job${extraQ}`
            : `${API_URL}/api/airs/model-portfolios/${(extra as { id: number }).id}`
              + `/fundamentals/ingest/job${extraQ}`;
        const extraStarted = await startJob(
          extraUrl, `${extra.name} fundamentals`, extraHoldings
            ? { headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ holdings: extraHoldings, label: extra.name }) }
            : undefined);
        setJobId(extraStarted.id);
        const extraJob = await extraStarted.done;
        if (extraJob.summary?.includes(' — unavailable:')) {
          setNote(fundamentalJobMessage(extraJob.summary));
        }
        additionalSucceeded = extraJob.status !== 'failed';
        changed ||= additionalSucceeded;
        if (extraJob.status === 'done' && !extraJob.summary?.includes(' — unavailable:')) {
          setNote(`updated ${scope.name} and ${extra.name}`);
        }
      }
      //  Re-read on anything but a failure, including a cancel. A cancelled fill has still loaded
      // every company it got through, and leaving the pre-fill charts on screen would hide real
      // work that was really done.
      //
      //  And drop the cached reads first, or the re-read is served from before the fill. This is
      // the ONE write on this screen the automatic rule in `apiFetch` cannot cover: that fires when
      // the request succeeds, and what succeeded here was merely STARTING a job that then ran for
      // minutes. Every chart would refetch, hit the entries cached during the fill, and show the
      // pre-fill book — a refresh button that visibly does nothing.
    } catch (e) {
      traceError('fundamentals', `could not start the fill for ${scope.name}`, e);
      setNote(`Could not start the fundamentals refresh: ${
        e instanceof Error ? e.message : String(e)}`);
    } finally {
      // Also runs when the second job could not start: work completed by the first job must still
      // become visible instead of being stranded behind the pre-refresh read cache.
      if (changed) {
        invalidateReadCache(`fundamentals fill finished for ${scope.name}`
          + (additionalScope ? ` and ${additionalScope.name}` : ''));
        if (broadcast) window.dispatchEvent(new Event('bb:fundamentals-finished'));
        onDone?.();
      }
      setBusy(false);
      setJobId(null);
      setCancelling(false);
    }
  };

  /**
   * Stop the fill.  NO INLINE MESSAGE — `cancelJob` puts "cancelling…" on the job's own card the
   * instant it is pressed, and that card carries the outcome and how far it got. Two places
   * reporting one job is two places to keep in step.
   *
   *  But the button itself must acknowledge the press, and it did not. Cancellation is
   * cooperative: the workers stop at their next feed boundary, which is seconds away, and for that
   * whole time the button kept saying "Cancel" — unchanged, still clickable, indistinguishable from
   * a press that went nowhere. This component's own argument for owning a Cancel at all is that the
   * reader is looking HERE and not at the corner of the screen; the same argument applies to the
   * acknowledgement. Local state, because the job's `cancel_requested` only comes back on the next
   * stream tick and the gap is exactly the moment being explained.
   */
  const cancel = async () => {
    if (!jobId) return;
    setCancelling(true);
    //  And back out again if the request did not land. A disabled "Cancelling…" over a job that
    // never heard the press is the worse version of the bug this fixes: the run continues and the
    // one control that could stop it has switched itself off.
    if (!await cancelJob(jobId)) setCancelling(false);
  };

  const extraTitle = refreshScopes(scope, additionalScope)[1]
    ? ` This press then refreshes the active benchmark, ${additionalScope!.name}.`
    : '';

  return (
    <span className="flex items-center gap-2 min-w-0">
      {/* The count sits LEFT of the button so the button keeps a fixed position as the text
          arrives — a control that slides sideways when its own result lands is a control you
          have to chase with the pointer. */}
      <span className="text-[11px] leading-snug text-fg-faint whitespace-normal break-words max-w-[22rem]"
        title={note ?? undefined}>{note}</span>
      {/*  ONE CONTROL, TWO STATES — the button BECOMES the Cancel while the fill runs. The toast
          carries a Cancel too and both are correct, but a fill is minutes and the reader who wants
          to stop it is looking at the button they just pressed, not at the corner of the screen.
          Inert only in the gap between the press and the job id arriving. */}
      <button type="button"
        onClick={() => { if (jobId) { void cancel(); } else { void run(); } }}
        //  Inert once cancelling, or the control invites a second press it cannot act on — the
        // request is already in and pressing again only re-POSTs the same idempotent stop.
        disabled={(busy && !jobId) || cancelling}
        title={jobId
          ? (cancelling
            ? 'Stopping. Each company in flight finishes the GuruFocus feed it is on — that is where '
              + 'the database is left consistent — and everything already fetched stays written. '
              + 'The pop-up bottom-right reports how far it got.'
            : 'Stop the fill. Work still queued is dropped at once and the three companies in flight '
              + 'stop at their next feed boundary, seconds away. Everything loaded so far is kept — '
              + 'press again later and it carries on from there.')
          : ((allPeriods
            ? `Re-download the full GuruFocus fundamentals history for ${scope.name}, including older fiscal years.`
            : (scope.kind === 'company'
              ? `Fetch the latest GuruFocus fundamentals for ${scope.name}, if it could plausibly have `
              + 'filed since we last looked. One API call, and none at all when its next quarter '
              + 'cannot be out yet.'
              : scope.kind === 'universe'
              ? `Refresh all reachable ${scope.name} constituents: their financial statements, cash `
                + 'flow and balance sheet, analyst estimates, indicators and daily share prices. '
                + 'This updates every Graphs card; exchanges outside the GuruFocus subscription are '
                + 'skipped without spending a call.'
              : `Fetch the latest GuruFocus fundamentals for every company in ${scope.name} that could `
                + 'plausibly have filed since we last looked — one API call each, and none for a '
                + 'company whose next quarter cannot be out yet.')
            + ' Progress, the running quota spend and a Cancel appear in the pop-ups bottom-right, '
            + 'and carry on if you close this.' + extraTitle))}
        className={`text-[12px] px-2.5 py-1 rounded-lg border transition-colors
                    disabled:opacity-50 disabled:cursor-wait whitespace-nowrap shrink-0 ${jobId
          ? 'border-warn-500/50 text-warn-400 hover:bg-warn-500/10'
          : 'border-neutral-700 text-fg-muted hover:bg-overlay/5'}`}>
        {/*  THE STATES OUTRANK THE CALLER'S LABEL. Whatever the button is named at rest, while it
            runs it says what pressing it will now DO — a control that keeps its old name while its
            action has changed underneath is the trap this replaced.
             AND "Cancelling…" IS A STATE, NOT A THIRD WHEEL. The earlier argument against a
            transient label (see `busy` in `refreshOne`) was that ~200ms of "Refreshing…" is a state
            nobody can act on that flickers past; this one lasts as long as the in-flight feeds do
            and answers the question the reader actually has, which is whether the press landed. */}
        {jobId ? (cancelling ? chrome.cancelling : chrome.cancel)
          : busy ? chrome.refreshing
            : label ?? (scope.kind === 'universe' ? chrome.refreshUniverse
              : chrome.refresh)}
      </button>
    </span>
  );
}
