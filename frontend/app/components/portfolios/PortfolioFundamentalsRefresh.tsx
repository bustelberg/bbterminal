'use client';

import { useState } from 'react';
import { API_URL } from '../../../lib/apiUrl';
import { traceError } from '../../../lib/debugTrace';
import { startJob } from '../../../lib/stores/jobs';

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
export type RefreshScope =
  | { kind: 'portfolio'; id: number; name: string }
  | { kind: 'basket'; holdings: { isin: string }[]; name: string }
  // ⚠ ONE COMPANY IS A BASKET OF ONE — same endpoint, same fill, one API call. It is a separate
  // `kind` only so the wording can be right: "every company in Fortinet Inc." is not a sentence.
  // The scope follows what the modal is SHOWING, which is the only rule a reader can predict.
  | { kind: 'company'; isin: string; name: string };

export default function PortfolioFundamentalsRefresh({ scope, onDone }: {
  scope: RefreshScope;
  /** Called when the fill ends without failing, so the caller can re-read what it wrote. */
  onDone: () => void;
}) {
  const [busy, setBusy] = useState(false);
  const [note, setNote] = useState<string | null>(null);

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
      const { done, body } = await startJob(
        holdings
          ? `${API_URL}/api/airs/basket/fundamentals/ingest/job${q}`
          : `${API_URL}/api/airs/model-portfolios/${(scope as { id: number }).id}`
            + `/fundamentals/ingest/job${q}`,
        `${scope.name} fundamentals`,
        holdings
          ? { headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ holdings, label: scope.name }) }
          : undefined);
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
      if (job.status !== 'failed') onDone();
    } catch (e) {
      traceError('fundamentals', `could not start the fill for ${scope.name}`, e);
      setNote('could not start — see the console');
    } finally {
      setBusy(false);
    }
  };

  return (
    <span className="flex items-center gap-2 min-w-0">
      {/* The count sits LEFT of the button so the button keeps a fixed position as the text
          arrives — a control that slides sideways when its own result lands is a control you
          have to chase with the pointer. */}
      <span className="text-[11px] text-fg-faint truncate max-w-[22rem]">{note}</span>
      <button type="button" onClick={() => void run()} disabled={busy}
        title={(scope.kind === 'company'
          ? `Fetch the latest GuruFocus fundamentals for ${scope.name}, if it could plausibly have `
            + 'filed since we last looked. One API call, and none at all when its next quarter '
            + 'cannot be out yet.'
          : `Fetch the latest GuruFocus fundamentals for every company in ${scope.name} that could `
            + 'plausibly have filed since we last looked — one API call each, and none for a '
            + 'company whose next quarter cannot be out yet.')
          + ' Progress, the running quota spend and a Cancel appear in the pop-ups bottom-right, '
          + 'and carry on if you close this.'}
        className="text-[12px] px-2.5 py-1 rounded-lg border border-neutral-700 text-fg-muted
                   hover:bg-overlay/5 disabled:opacity-50 whitespace-nowrap shrink-0">
        {busy ? 'Refreshing…' : 'Refresh fundamentals'}
      </button>
    </span>
  );
}
