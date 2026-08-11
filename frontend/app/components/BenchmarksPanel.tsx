'use client';

import { Fragment, useEffect, useRef, useState } from 'react';
import { apiFetch } from '../../lib/apiFetch';
import { API_URL } from '../../lib/apiUrl';
import { trace, traceEmpty, traceError } from '../../lib/debugTrace';
import { dialog } from '../../lib/dialog';
import { useIsAdmin } from '../../lib/hooks/useEffectiveRole';
import { cancelJob, startJob } from '../../lib/stores/jobs';
import type { ReconstructedIndex } from '../../lib/types/api';
import FundamentalGridPane from './benchmarks/FundamentalGridPane';

/** `runOwner` when the HEADER button started the run. A sentinel rather than a label because no
 *  index can be called this, and the alternative — a separate boolean — would let "a row owns it"
 *  and "the header owns it" both be true. */
const ALL = '*';

/** The indices we rebuild from our own constituents.
 *
 * All three are priced from yfinance (`asset_price`) — the SAME source as the portfolios on this
 * page. That is the panel's entire claim ("same basis as a portfolio"), and until 2026-07-16 it
 * was false: the panel ran on GuruFocus while the portfolios ran on yfinance, which compares two
 * price universes and calls the difference alpha.
 *
 * ACWI is still not FULLY priced (the published index has names we hold no series for), so its
 * coverage ratio is surfaced and it is called indicative rather than exact. AEX is fully covered
 * (25/25) and is the one index that CAPS: uncapped, ASML is 37.5% of it. */
// ⚠ `rebuildable` = FILL CAN PUT IT BACK, mirroring the backend's `_benchmark_fill.rebuildable()`
// — a registered `UniverseTemplate`, or SP500's Wikipedia reconstruction, which is deliberately
// NOT in the template registry (registering it would stamp `template_key` on its universe row and
// hide the index from the /sp500 page that owns it). All three are rebuildable today, so the flag
// looks redundant; it is here so a fourth index added without a route back does not silently get a
// one-way Delete. The backend refuses regardless (422 naming why), so a drift here can only hide a
// button, never destroy anything.
const INDICES = [
  { label: 'SP500', name: 'S&P 500', rebuildable: true },
  { label: 'ACWI', name: 'ACWI', rebuildable: true },
  { label: 'AEX', name: 'AEX', rebuildable: true },
];

const pct = (v: number | null | undefined, dp = 2) =>
  v == null ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(dp)}%`;

const tone = (v: number | null | undefined) =>
  v == null ? 'text-fg-faint' : v >= 0 ? 'text-pos-400' : 'text-neg-400';

/** Benchmarks — a cap-weighted index rebuilt from OUR membership, prices and FX.
 *
 * Why rebuild an index we could just read off SPY: because this one is computed the same way
 * a portfolio is, so a benchmark number and a portfolio number are comparable line for line.
 * It is checked against the real thing — 2026 YTD comes out +9.10% USD against SPY's +9.02%.
 *
 * The two things that make that agreement possible are both counter-intuitive, so they are
 * spelled out in the footnote rather than buried: the weights are as of the START of the
 * year (weighting by today's market cap is look-ahead bias, and would print +21.70%), and
 * three constituents' price series had to be un-split (our stored closes are not
 * split-adjusted and cannot self-heal).
 */
/**
 * ⚠ THE SSE FRAME TYPES AND `refreshSummary` ARE GONE — the job transport retired them, and the
 * receipt sentence they built now lives in `benchmark_refresh_job` on the server (reproduced there
 * clause for clause, including the two "never silent" ones about an uncapped constituent and about
 * "already at the vendor's latest"). Keeping a second copy here would be a second place for the
 * wording of a run's outcome to drift; the panel now shows `job.summary` verbatim.
 *
 * `GET …/refresh` (SSE) is still live on the backend for `/api` and curl — this panel simply is
 * not one of its consumers any more.
 */

/**
 * ⚠ THE FUNDAMENTALS-FILL PROGRESS BOX IS GONE (2026-08-06) — it is a JOB now, and the toast
 * stack reports it.
 *
 * It was a `FillRun` state here plus a bordered box with a bar and a six-line tail, and every part
 * of it existed to work around the same defect: the run was an SSE stream owned by this panel, so
 * it had to be held high in the tree to survive collapsing an index, and it died entirely on a
 * route change while the server kept spending quota. A job has none of those properties — the
 * toast outlives the page, re-attaches after a reload, shows the same bar, and adds a Cancel this
 * never had.
 *
 * What is deliberately NOT reproduced: the tail of recent lines. The toast is one line plus the
 * console, which is this app's rule everywhere else; a scrolling tail on a floating card would
 * reflow the stack under the reader's cursor.
 */

export default function BenchmarksPanel() {
  const isAdmin = useIsAdmin();
  const [data, setData] = useState<Record<string, ReconstructedIndex>>({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [open, setOpen] = useState<string | null>(null);
  /**
   * WHICH BUTTON OWNS THE RUN — a row's label, or `ALL` for the header button. `null` when idle.
   *
   * ⚠⚠ IT IS THE OWNER, NOT A SET OF WHAT IS RUNNING, AND THAT IS THE WHOLE POINT. Only the button
   * that was PRESSED turns into Cancel; every other Refresh merely disables. A set of in-flight
   * labels cannot express that — during "Refresh all" all three labels are in it, so all three row
   * buttons would offer to cancel, and pressing one would raise the question of what exactly it
   * cancels. One owner, one Cancel, one run.
   */
  const [runOwner, setRunOwner] = useState<string | null>(null);
  /** The job in flight RIGHT NOW, so Cancel has something to cancel. A run is a SEQUENCE of jobs
   *  (prices then fundamentals, per index), so this changes several times within one press. */
  const [activeJob, setActiveJob] = useState<string | null>(null);
  /**
   * ⚠ A REF, NOT STATE, AND IT MUST BE. The loop in `refresh` closes over its variables once; a
   * `cancelled` state read inside it would be the value from the render that started the run and
   * would still be false after Cancel, so the sequence would carry on to the next index having
   * duly cancelled the current job. The ref is read through, so the loop sees the press.
   */
  const abort = useRef(false);
  const [runMsg, setRunMsg] = useState<{ text: string; kind: 'info' | 'warn' } | null>(null);
  const [deleting, setDeleting] = useState<string | null>(null);
  // ⚠ THE TABLE HAS TO RE-READ, OR THE WHOLE POINT IS LOST. Reset → Refresh is a loop you watch:
  // without a reload the row keeps showing the members it had before you deleted them, which reads
  // as the button having done nothing.
  const [reloadKey, setReloadKey] = useState(0);
  const reload = () => setReloadKey((k) => k + 1);
  /** ⚠ A SECOND SIGNAL, NOT `reloadKey`. That one re-reads the INDEX payloads (prices, caps, YTD)
   *  and re-renders the whole table; the fundamentals grid holds its own two cadences and is
   *  expensive to refetch, so it is told separately and only when a fill actually wrote something.
   *  Reusing one key would refetch both halves on every press of either. */
  const [fundKey, setFundKey] = useState(0);
  /**
   * ⚠ NEITHER BACKFILL IS OWNED HERE ANY MORE (2026-08-06). Both are jobs.
   *
   * The panel used to hold the fill's progress state precisely because the detail pane is
   * collapsible — a box inside it would unmount mid-run and leave a page that showed nothing
   * happening. Lifting it here fixed the collapse and not the route change, and neither fixed the
   * real problem, which was that the work was tied to a stream instead of to a handle.
   *
   * A job outlives all of it, so `IndexDetail` starts its own fill directly and there is nothing
   * to prop-drill. The single-company variant lives on the fundamentals grid's per-row button; the
   * blocking endpoint it used to call is untouched and still what `scripts/` reach for, and both
   * paths run the same `ingest_company`, so "ingest" continues to mean exactly one thing.
   */

  /**
   * The fundamentals half of a Refresh: the GuruFocus fill for one index.
   *
   * ⚠⚠ THIS IS A SECOND VENDOR WITH A SEPARATE, MONTHLY QUOTA — which is exactly why it was split
   * OUT of this button on 2026-08-06, and why chaining it back on is a deliberate choice rather
   * than a tidy-up. What made the old arrangement wrong was not the pairing, it was that the count
   * beside it came from a third endpoint with a third denominator, so it offered to fetch
   * constituents the fill would then refuse. The fill now counts its own work (`needs`/`eligible`),
   * so the pairing is safe in a way it was not then.
   *
   * ⚠ AWAITED, SO THE THREE INDICES FILL ONE AT A TIME. `startJob` returns as soon as the job is
   * running, so firing all three would put three concurrent fills on GuruFocus — eight companies
   * in flight each. The prices half is already sequential for the same reason on the Yahoo side;
   * this keeps the whole button to one external consumer at a time.
   *
   * ⚠ IT RUNS EVEN WHEN THE PRICE HALF FAILED. Different vendor, different data: a Yahoo outage
   * says nothing about whether GuruFocus can fill the grid, and skipping it would make a partial
   * failure quietly halve the button.
   *
   * ⚠⚠ `force=true` — EVERY CONSTITUENT, NOT THE ONES WITH NOTHING. Without it the fill selects on
   * a sentinel row's EXISTENCE, so a constituent whose statements were loaded a year ago is never
   * missing and never refetched: the grid goes on showing last year's figures and looks full. That
   * is the same conclusion the price half reached (`_benchmark_refresh`: a press always fetches,
   * every constituent, no staleness tolerance), and it is what makes one Refresh mean the same
   * thing on both halves. One GuruFocus call per constituent — `fetch_financials` is a single blob
   * carrying every column this grid draws, market cap included, so "all fundamentals" costs one
   * call, not nineteen.
   *
   * ⚠ THE GRID'S OWN "All {n}" BUTTON IS DELIBERATELY *NOT* FORCED. It counts what is missing and
   * fetches exactly that — the cheap, targeted press. This one is the rebuild.
   *
   * Returns the sentence for the receipt, or '' when there was nothing to say.
   */
  const fill = async (label: string): Promise<string> => {
    try {
      const { id, done } = await startJob(
        `${API_URL}/api/benchmarks/index/${encodeURIComponent(label)}/fundamentals/ingest/job`
        + '?feeds=statements&force=true',
        `${label} fundamentals`);
      setActiveJob(id);
      const job = await done;
      // ⚠ RE-READ ON ANYTHING BUT A FAILURE, including a cancel: a cancelled bulk run has still
      // loaded every company it got through, and leaving the pre-fill grid on screen would hide
      // real work that was really done. Mirrors `fillAll` in the grid pane.
      if (job.status !== 'failed') setFundKey((k) => k + 1);
      return job.summary || `fundamentals ${job.status}`;
    } catch (e) {
      // The toast already carries the failure; this line keeps the panel's receipt honest about
      // which half of the button did not run.
      traceError('benchmarks', `could not start the fundamentals fill for ${label}`, e);
      return 'fundamentals could not start';
    }
  };

  /** Refresh one index, or every index in sequence: constituents → market caps → the two prices,
   *  then that index's GuruFocus fundamentals fill (see `fill`).
   *
   * ⚠ SEQUENTIALLY, NEVER `Promise.all`. Every step calls Yahoo; three indices at once is three
   * concurrent consumers on the throttle, which is the failure this whole pipeline is arranged
   * to avoid (an overloaded caller gets an EMPTY result, not a 429).
   *
   * ⚠ THE DETAIL GOES TO THE CONSOLE, THE RECEIPT TO THE PANEL. Step 3 emits a line per
   * constituent — 491 for the S&P — which is exactly what you want when checking a price and
   * exactly what you do not want in a status bar. The panel gets the latest line while it runs
   * and one sentence at the end. */
  const refresh = async (labels: string[], owner: string) => {
    setRunOwner(owner);
    abort.current = false;
    setRunMsg({ text: `Refreshing ${labels.join(', ')}…`, kind: 'info' });
    const lines: string[] = [];
    try {
      for (const label of labels) {
        // ⚠ CHECKED BEFORE EACH LEG, NOT ONLY AT THE TOP. Cancel is asked for mid-run, and the
        // legs are minutes long — a press during ACWI's prices must not be followed by ACWI's
        // fundamentals and then two more indices. See `cancelRun`.
        if (abort.current) { lines.push(`${label}: not started — cancelled`); continue; }
        let priced: string;
        try {
          const { id, done } = await startJob(
            `${API_URL}/api/benchmarks/index/${encodeURIComponent(label)}/refresh/job`,
            `${label} prices`);
          setActiveJob(id);
          const job = await done;
          priced = `${label}: ${job.summary || job.status}`;
        } catch (e) {
          traceError('benchmarks', `could not start the refresh for ${label}`, e);
          priced = `${label}: refresh could not start`;
        }
        if (abort.current) { lines.push(`${priced}  ·  fundamentals skipped — cancelled`); continue; }
        lines.push([priced, await fill(label)].filter(Boolean).join('  ·  '));
      }
      // ⚠ A DIFFERENT SEPARATOR BETWEEN INDICES THAN WITHIN ONE. Each line is two halves joined by
      // '·' (prices · fundamentals); joining the three lines with '·' as well produced one
      // undifferentiated run in which you could not see where SP500 ended and ACWI began.
      setRunMsg({ text: lines.join('   |   '), kind: 'warn' });
      // Caps and prices are written as the run goes, so the table's numbers change now.
      reload();
    } catch (e) {
      console.warn('[benchmark refresh] failed', e);
      setRunMsg({ text: e instanceof Error ? e.message : String(e), kind: 'warn' });
    } finally {
      setRunOwner(null);
      setActiveJob(null);
      abort.current = false;
    }
  };

  /**
   * Stop the whole run this button started — not just the leg currently executing.
   *
   * ⚠⚠ TWO ACTIONS, AND EITHER ALONE IS A CANCEL THAT DOES NOT CANCEL. `cancelJob` stops the job
   * in flight; the `abort` ref stops the SEQUENCE. Without the ref, cancelling ACWI's prices would
   * be followed immediately by ACWI's fundamentals and then SP500 — five more legs, after the
   * reader asked it to stop. Without `cancelJob`, the current leg would run to completion first,
   * which on ACWI is eleven more minutes of Yahoo calls.
   *
   * ⚠ THE TOAST'S OWN CANCEL IS A DIFFERENT SCOPE AND BOTH ARE CORRECT: it cancels THAT LEG and
   * the sequence moves on to the next; this cancels the RUN. The tooltips say which is which.
   */
  const cancelRun = async () => {
    abort.current = true;
    setRunMsg({ text: 'Cancelling — the current step stops at its next safe point…', kind: 'warn' });
    if (activeJob) await cancelJob(activeJob);
  };

  /**
   * Delete the live universe behind one benchmark, so Refresh can be watched rebuilding it.
   *
   * ⚠ THE CONFIRM NAMES WHAT SURVIVES, NOT JUST WHAT GOES. "Delete SP500?" invites the reading
   * that the prices and the market caps go with it; they do not, and knowing that is the
   * difference between trying this and not daring to.
   */
  const del = async (label: string, members?: number) => {
    const ok = await dialog.confirm(
      `Reset ${label}?\n\n`
      + `Deletes all three things Refresh puts in place, so the whole button can be tested:\n`
      + `  • its ${members ?? ''} membership rows\n`
      + '  • its constituents’ market caps\n'
      + '  • their closes from mid-November onward (the start-of-year mark and everything since)\n\n'
      + 'The asset grid, the Yahoo symbol and the older history stay — so Refresh re-fetches prices '
      + 'for a KNOWN listing and nothing is ever re-resolved.\n\n'
      + '⚠ Prices are shared: some of these constituents are also held in AIRS books, and '
      + 'those portfolio figures will read short until the prices are refilled. One press of Refresh '
      + 'refills 50; the 06:00 price tick finishes the rest overnight.',
    );
    if (!ok) return;
    setDeleting(label);
    try {
      const r = await apiFetch(`${API_URL}/api/benchmarks/index/${label}`, { method: 'DELETE' });
      const b = (await r.json().catch(() => null)) as
        { deleted?: boolean; members_deleted?: number; caps_cleared?: number;
          price_rows_deleted?: number; prices_from?: string;
          note?: string; detail?: string } | null;
      if (!r.ok) {
        // 422 carries a sentence about THIS label (no template, frozen, has children) — show it
        // rather than a status code, because it names what to do instead.
        console.warn(`[benchmarks] delete ${label} refused`, { status: r.status, body: b });
        setRunMsg({ text: b?.detail ?? `${label}: delete failed — HTTP ${r.status}`, kind: 'warn' });
        return;
      }
      setRunMsg({
        text: b?.deleted
          ? `${label}: reset — ${b.members_deleted ?? 0} members, ${b.caps_cleared ?? 0} market `
            + `caps and ${(b.price_rows_deleted ?? 0).toLocaleString('en-US')} closes from `
            + `${b.prices_from ?? 'the window open'}. Press Refresh to rebuild all three.`
          : b?.note ?? `${label}: nothing to delete.`,
        kind: 'warn',
      });
      reload();
    } catch (e) {
      console.warn(`[benchmarks] delete ${label} threw`, e);
      setRunMsg({ text: e instanceof Error ? e.message : String(e), kind: 'warn' });
    } finally {
      setDeleting(null);
    }
  };

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      // Independently, in parallel: ACWI is ~8s and one index failing must not blank the
      // other. Only surface an error if EVERY index failed.
      const results = await Promise.allSettled(
        INDICES.map(async (ix) => {
          const r = await apiFetch(`${API_URL}/api/benchmarks/index/${ix.label}`);
          if (!r.ok) throw new Error(`${ix.label}: HTTP ${r.status}`);
          return [ix.label, (await r.json()) as ReconstructedIndex] as const;
        }),
      );
      if (cancelled) return;
      const out: Record<string, ReconstructedIndex> = {};
      const errs: string[] = [];
      for (const res of results) {
        if (res.status === 'fulfilled') out[res.value[0]] = res.value[1];
        else errs.push(res.reason instanceof Error ? res.reason.message : String(res.reason));
      }
      // ⚠ AN INDEX THAT LOADS WITH ZERO MEMBERS IS THE FRESH-DATABASE CASE AND IT IS NOT AN
      // ERROR — the request succeeded, the universe simply has not been built or its
      // constituents are not in the asset grid. It renders as "0 —", which is identical to a
      // failure on screen, so the console has to tell them apart. The backend already sends the
      // reason in `note`; this is where it becomes visible.
      for (const [label, d] of Object.entries(out)) {
        if (!d.member_count) {
          traceEmpty('benchmarks', `${label} priced 0 constituents`,
            d.note ?? 'no universe, or none of its constituents are resolved and capped in the '
            + 'asset grid. Press Refresh on the row — it builds the universe, resolves what it '
            + 'can and fetches the prices. Not an error.');
        } else {
          trace('benchmarks', `${label}: ${d.member_count} priced (${d.priced_of_universe ?? '?'} `
            + `of the universe), YTD ${d.ytd_eur_pct?.toFixed(2) ?? '—'}% EUR, as of ${d.as_of ?? '—'}`
            + (d.split_adjusted?.length ? ` · ${d.split_adjusted.length} split-adjusted` : ''));
        }
      }
      for (const e of errs) traceError('benchmarks', `an index failed to load: ${e}`);
      setData(out);
      if (Object.keys(out).length === 0) {
        traceError('benchmarks', 'every index failed — the panel will show an error, not a table');
        setError(errs.join('; ') || 'Failed to load');
      }
      setLoading(false);
    })();
    return () => { cancelled = true; };
  }, [reloadKey]);

  return (
    <section className="bg-card border border-neutral-800/40 rounded-xl p-5 space-y-3">
      <div className="flex items-start gap-3 flex-wrap">
        <div>
          {/* The subtitle line was removed on request (2026-08-06). Its one load-bearing claim —
              that these are rebuilt from our own membership, prices and FX, which is what makes an
              index number and a portfolio number comparable — moves onto the heading's own
              tooltip rather than being lost with it. */}
          <h3 className="text-sm font-semibold text-fg-strong"
            title="Cap-weighted indices rebuilt from our own membership, prices and FX — the same basis a portfolio is measured on, which is what makes the two comparable.">
            Benchmarks
          </h3>
        </div>
        {/* Ingesting constituents is admin work — hidden rather than left to 403. The index itself
            reads the same for everyone. */}
        {isAdmin && (runOwner === ALL ? (
            <button type="button" onClick={() => void cancelRun()}
              title="Stop the whole run — the step in flight stops at its next safe point (between constituents, or between companies in a fill) and nothing further is started. Everything already fetched is kept."
              className="ml-auto text-[12px] px-2.5 py-1 rounded-lg border border-warn-500/40 bg-warn-500/10 text-warn-500 hover:bg-warn-500/20">
              Cancel
            </button>
          ) : (
            <button type="button" onClick={() => void refresh(INDICES.map((i) => i.label), ALL)}
              disabled={runOwner !== null}
              title="For each index, in order: gather its constituents, get every one's market cap from Yahoo, then each one's start-of-year price and current price — then refetch every constituent's fundamentals from GuruFocus, which is what the expanded row's grid reads. Both halves fetch EVERY constituent, holes or not, so figures that are present but stale are replaced. One GuruFocus call each (~490 for the S&P) against a monthly quota, read out before the run starts. Minutes per index; progress and Cancel are in the pop-ups bottom-right. Runs one index at a time — concurrent callers are how a constituent lands on the wrong listing."
              className="ml-auto text-[12px] px-2.5 py-1 rounded-lg bg-accent-600 hover:bg-accent-500 text-white disabled:opacity-50">
              Refresh all
            </button>
          ))}
      </div>


      {runMsg && (
        <div className={`text-[12px] rounded-lg px-3 py-1.5 border ${
          runMsg.kind === 'warn'
            ? 'text-warn-300 bg-warn-500/10 border-warn-500/20'
            : 'text-fg-subtle bg-overlay/[0.03] border-neutral-800/40'}`}>
          {/* ⚠ THE LIVE `tick` LINE IS GONE, DELIBERATELY. It existed because the SSE run had
              nowhere else to show motion; the run is a job now and the toast bottom-right carries
              the line, a progress bar, the quota spent and Cancel. Keeping both would give a
              minutes-long run two progress readouts that update at different moments and can
              disagree about which step it is on. What stays here is the RECEIPT — one sentence per
              index, after the fact, which the toast deliberately does not keep. */}
          {runMsg.text}
        </div>
      )}
      {runOwner !== null && <div className="loading-bar h-0.5 w-full rounded-full" aria-hidden />}

      {loading && <p className="text-xs text-fg-subtle">Computing…</p>}
      {error && (
        <div className="bg-neg-500/10 border border-neg-500/20 rounded-lg px-3 py-2 text-xs text-neg-300">{error}</div>
      )}

      {!loading && !error && (
        <div className="overflow-auto rounded-lg border border-neutral-800/40">
          <table className="w-full text-xs">
            <thead className="bg-card">
              <tr className="text-fg-faint text-[11px] uppercase tracking-wide border-b border-neutral-800/40">
                <th className="px-3 py-1.5 font-medium text-left">Benchmark</th>
                <th className="px-3 py-1.5 font-medium text-right">Members</th>
                <th className="px-3 py-1.5 font-medium text-right">YTD (€)</th>
                <th className="px-3 py-1.5 font-medium text-right">YTD (local)</th>
                <th className="px-3 py-1.5 font-medium text-left">As of</th>
                <th className="px-3 py-1.5 font-medium text-right"> </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-neutral-800/20">
              {INDICES.map((ix) => {
                const d = data[ix.label];
                if (!d) return null;
                const isOpen = open === ix.label;
                return (
                  <Fragment key={ix.label}>
                    <tr onClick={() => setOpen(isOpen ? null : ix.label)}
                      className="hover:bg-accent-500/10 transition-colors cursor-pointer">
                      <td className="px-3 py-1.5 text-fg whitespace-nowrap">
                        <span className="text-fg-faint mr-1.5">{isOpen ? '▾' : '▸'}</span>
                        {ix.name}
                      </td>
                      <td className="px-3 py-1.5 text-right font-mono text-fg-soft">{d.member_count}</td>
                      <td className={`px-3 py-1.5 text-right font-mono font-semibold ${tone(d.ytd_eur_pct)}`}>
                        {pct(d.ytd_eur_pct)}
                      </td>
                      <td className={`px-3 py-1.5 text-right font-mono ${tone(d.ytd_local_pct)}`}>
                        {pct(d.ytd_local_pct)}
                      </td>
                      <td className="px-3 py-1.5 font-mono text-fg-subtle whitespace-nowrap">{d.as_of ?? '—'}</td>
                      <td className="px-3 py-1.5 text-right">
                        {/* ⚠ `stopPropagation` — the whole row is the expand toggle, and a Refresh
                            that also opened the detail would look like it had rendered a result. */}
                        {isAdmin && (
                          <div className="inline-flex items-center gap-1">
                            {/* ⚠ ONLY THE PRESSED BUTTON BECOMES CANCEL. During "Refresh all" all
                                three rows are being worked on, but none of them owns the run, so
                                all three stay disabled — a Cancel on a row would otherwise have to
                                answer "cancel what, this index or the run?" and either answer is
                                a surprise. */}
                            {runOwner === ix.label ? (
                              <button type="button"
                                onClick={(e) => { e.stopPropagation(); void cancelRun(); }}
                                title={`Stop refreshing ${ix.name}. The step in flight stops at its next safe point and the fundamentals fill is not started; everything already fetched is kept.`}
                                className="text-[12px] px-2 py-0.5 rounded-lg border border-warn-500/40 bg-warn-500/10 text-warn-500 hover:bg-warn-500/20">
                                Cancel
                              </button>
                            ) : (
                              <button type="button" disabled={runOwner !== null || deleting != null}
                                onClick={(e) => { e.stopPropagation(); void refresh([ix.label], ix.label); }}
                                title={`Refresh ${ix.name}: everything this row and its expanded grid read, for every constituent — not only the ones with gaps. Constituents, market caps and the two prices from Yahoo, then a refetch of every constituent's fundamentals from GuruFocus (one call each, against a monthly quota). Present-but-stale figures are replaced. Takes minutes — progress and Cancel are in the pop-ups bottom-right.`}
                                className="text-[12px] px-2 py-0.5 rounded-lg border border-neutral-700 text-fg-muted hover:bg-overlay/5 disabled:opacity-50">
                                Refresh
                              </button>
                            )}
                            {/* Only where Refresh can put it back — see `rebuildable`. */}
                            {ix.rebuildable && (
                              <button type="button" disabled={runOwner !== null || deleting != null}
                                onClick={(e) => { e.stopPropagation(); void del(ix.label, d?.member_count); }}
                                title={`Delete the ${ix.name} universe so Refresh can be watched rebuilding it. Membership only — prices and market caps are untouched.`}
                                aria-label={`Delete the ${ix.name} universe`}
                                className="text-[12px] px-2 py-0.5 rounded-lg border border-neutral-800/40 text-fg-faint hover:text-neg-400 hover:border-neg-500/40 disabled:opacity-50">
                                {deleting === ix.label ? '…' : 'Delete'}
                              </button>
                            )}
                          </div>
                        )}
                      </td>
                    </tr>
                    {isOpen && (
                      <tr>
                        <td colSpan={6} className="px-3 py-3 bg-inset">
                          <IndexDetail d={d} fundKey={fundKey} />
                        </td>
                      </tr>
                    )}
                  </Fragment>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}

/** ⚠ NO `fill` / `onFillAll` PROPS ANY MORE. They existed so the PANEL could own the stream and
 *  keep a progress box alive while this collapsible pane unmounted. A job needs neither: the run
 *  has a handle, the toast lives in the root layout, and this pane can simply start it. */
function IndexDetail({ d, fundKey = 0 }: { d: ReconstructedIndex; fundKey?: number }) {
  return (
    <div className="space-y-2">
      {/* ⚠ THE PANE CANNOT NOTICE A FILL ON ITS OWN — it caches both cadences precisely so that
          scrubbing the slider never refetches, which also means nothing tells it the data
          underneath changed. `refreshKey` is that telling, bumped by Refresh's fundamentals half. */}
      <FundamentalGridPane label={d.label} refreshKey={fundKey} />
      {/* ⚠ THE FILL BUTTON MOVED INTO THE GRID'S TOTAL ROW (2026-08-06). It sat here beside the
          price/constituent Refresh, which is a different vendor with a different quota — and its
          count came from a THIRD endpoint with a third denominator, so it could offer to fetch
          constituents the fill would then refuse. In the Total row's Fetch cell it is the
          all-companies form of the per-company button directly above it, and the grid counts what
          the fill will actually do. This line is now just the index's own provenance. */}
      <div className="flex items-center gap-3 flex-wrap text-[12px]">
        <span className="text-fg-soft">
          <span className="font-mono text-fg">{d.priced_of_universe}</span> priced ·{' '}
          weights as of <span className="font-mono">{d.start_date}</span>
        </span>
      </div>

      {/* A corrected price is a CLAIM. Show it — never adjust silently. */}
      {(d.split_adjusted?.length ?? 0) > 0 && (
        <div className="bg-warn-500/10 border border-warn-500/20 rounded-lg px-3 py-2 text-[12px] text-warn-300">
          <span className="font-semibold">Split-adjusted on the fly:</span>{' '}
          {d.split_adjusted!.map((s) => `${s.ticker} ×${s.factor.toFixed(3)}`).join(' · ')}.
          Our stored closes are not split-adjusted and cannot self-heal (the ingest only
          fetches dates newer than what we hold), so these series were rescaled here. Left
          raw, KLA alone would read −80% and take a weight it never had.
        </div>
      )}

      <p className="text-[11px] text-fg-faint leading-relaxed">
        {d.note} Weights are as of the <strong>start of the year</strong>: weighting by
        today&apos;s market cap is look-ahead bias — it retroactively overweights whatever
        went up.{' '}
        {d.label === 'SP500' && (
          <>
            It would print <span className="font-mono">+21.70%</span> instead of{' '}
            <span className="font-mono">{pct(d.ytd_local_pct)}</span>. Checked against SPY (the
            real index), which is <span className="font-mono">+9.02%</span>{' '}USD.
          </>
        )}
        {d.label === 'ACWI' && (
          <>
            Coverage is <strong>partial</strong> (<span className="font-mono">{d.priced_of_universe}</span>{' '}
            priced): we hold no price series for some published constituents, and a cap-weighted
            rebuild does not lose that weight — it renormalises it across the rest. Treat this as
            indicative, not exact.
          </>
        )}
        {d.label === 'AEX' && (
          <>
            Fully covered (<span className="font-mono">{d.priced_of_universe}</span> priced) and{' '}
            <strong>capped at 15%</strong>{' '}per constituent, as the real index is — uncapped, ASML
            alone would be <span className="font-mono">37.5%</span>{' '}of it. Composition is
            Wikipedia&apos;s, as of the date above; the cap is applied at the start of the window
            rather than at Euronext&apos;s review date, and full market cap is not the index&apos;s
            free float.
          </>
        )}
      </p>
    </div>
  );
}
