'use client';

import { Fragment, useCallback, useEffect, useRef, useState } from 'react';
import { apiFetch } from '../../lib/apiFetch';
import { API_URL } from '../../lib/apiUrl';
import { trace, traceEmpty, traceError, traceRows, traceScope } from '../../lib/debugTrace';
import { dialog } from '../../lib/dialog';
import { useIsAdmin } from '../../lib/hooks/useEffectiveRole';
import { Provenance } from '../../lib/provenance';
import { trimStop } from '../../lib/provenanceText';
import { LinkCell, type LinkCtx } from './PortfoliosPanel';
import PortfolioAnalysisModal from './portfolios/PortfolioAnalysisModal';
import { RefreshIcon } from './portfolios/RefreshIcon';
import { cancelJob, startJob } from '../../lib/stores/jobs';
import AllocationBandsModal from './portfolios/AllocationBandsModal';
import AccountTransactions from './portfolios/AccountTransactions';
import AccountTotalReturn from './portfolios/AccountTotalReturn';
import { type Basket } from './portfolios/types';
import { allocColor, bucketLabel, BUCKET_ORDER } from './portfolios/allocationColors';
import {
  aggregateGroups, combineWeighted, groupStats, holdingTotalReturn, startBasis, weightedReturn,
  WEIGHT_BASES, type GroupStats, type WeightBasis, type WeightedReturn,
} from './portfolios/startWeights';

import type {
  AirsAccountDetail, AirsAccountIsins, AirsHoldingSegment, AirsPortfolioOverview,
} from '../../lib/types/api';

/**
 * The one table: a portfolio, by the name you gave it, on AIRS's own numbers.
 *
 * A portfolio lives in AIRS as TWO rows that share nothing but a strategy — the Fixed one
 * (`_FX`/`_AFS`: weights, ISINs, your nickname, and nothing AIRS will value) and the Dynamic one
 * (`_DYN`: the real book — quantities, EUR values, returns, and NO ISIN). Measured: 58 Fixed with
 * a composition, 31 valued Dynamic, overlap ZERO. Neither is the portfolio. The pair is.
 *
 * So: the NAME is the Fixed side's; every NUMBER is the Dynamic side's, because AIRS is the
 * system of record for what a book made and we are not. Expanding a row shows the holdings —
 * ISIN and fund name from the Fixed side, everything else AIRS's own.
 *
 * ⚠ 27 OF 28 PAIRINGS ARE AN UNCONFIRMED GUESS, AND THE ROW MUST SAY SO. This is not a small
 *   doubt: the risk variants of a strategy hold the SAME instruments (BUS_FTS_Bepoff/DEF/NEU_AFS
 *   share 27 of 27 ISINs), so a mis-pairing files a real book's money under another strategy's
 *   name and NOTHING else on the row looks wrong. Confirm them in Dynamic → Fixed.
 */

const pct = (v: number | null | undefined) =>
  v == null ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(2)}%`;
const eur = (v: number | null | undefined) =>
  v == null ? '—' : `€${Math.round(v).toLocaleString('en-US')}`;
const tone = (v: number | null | undefined) =>
  v == null ? 'text-fg-faint' : v >= 0 ? 'text-pos-400' : 'text-neg-400';

/** The four AIRS reports, by the code the scan records, in AIRS's own words — a badge saying
 *  "volk" would send a reader to the wrong screen. Mirrors `airs_vermogen.REPORTS`. */
/** One CAUSE of failure, with how many accounts hit it — see the backend's `summarise_errors`. */
type FailureGroup = {
  report: string; error_type: string; message: string; count: number; accounts: string[];
};

/**
 * ⚠ THE DETAIL GOES TO THE CONSOLE, NOT ONTO THE PAGE. The panel gets ONE short line saying what
 * happened; everything needed to diagnose it — the HTTP status, the backend's own error list, the
 * per-cause failure groups and which accounts hit them — is logged.
 *
 * This is not only a tidiness rule. The prose block that used to sit under the message explained
 * the SCAN's failure modes ("every account it reached is listed; the ones short a report carry a
 * ⚠…") and was gated on the amber colour alone — so a plain successful DELETE, which is amber
 * because it leaves a gap, printed a paragraph about reports that were never fetched. An
 * explanation attached to a colour rather than to an outcome will eventually explain the wrong one.
 */
const logDetail = (what: string, ...detail: unknown[]) =>
  console.warn(`[AIRS portfolios] ${what}`, ...detail);

/** One step of a running scan, as `airs_vermogen._emit` stamps it. `seq` is append-only. */
type ScanStep = {
  seq: number; kind: string; message?: string;
  names?: string[]; todo?: string[]; current?: string[];
  account?: string; report?: string; status?: string; detail?: string;
  got?: string[]; failed?: string[]; complete?: boolean; count?: number;
};

/** ✓ / — / ✗ per report outcome. `no_data` is AIRS ANSWERING (this book has no such report), so it
 *  must not wear the failure mark: 14 of 44 books have no fixed model and never will. */
const STEP_MARK: Record<string, string> = { ok: '✓', no_data: '—', failed: '✗' };

/**
 * Print every scan step the console has not seen yet; returns the new high-water mark.
 *
 * ⚠ THE ROSTER IS PRINTED IN FULL, NOT COUNTED. "44 found" is a number nobody can check; the 44
 * NAMES are what you compare against AIRS's own "44 Items in selectie" to confirm the scan is
 * looking at the Interne/actief population and not some other one.
 */
function logSteps(log: ScanStep[], from: number): number {
  let high = from;
  for (const s of log) {
    if (s.seq < from) continue;
    high = Math.max(high, s.seq + 1);
    if (s.kind === 'discovered') {
      console.warn(`[AIRS scan] ${s.message}`);
      // A table, so the names are readable and sortable rather than a wrapped comma list.
      console.table((s.names ?? []).map((n, i) => ({ '#': i + 1, portfolio: n })));
    } else if (s.kind === 'plan') {
      console.warn(`[AIRS scan] ${s.message}`);
      if (s.current?.length) console.warn('[AIRS scan]   already current (skipped):', s.current);
      if (s.todo?.length) console.warn('[AIRS scan]   to scan:', s.todo);
    } else if (s.kind === 'report') {
      console.warn(`[AIRS scan]     ${STEP_MARK[s.status ?? ''] ?? '?'} ${s.report}`
        + (s.detail ? ` — ${s.detail}` : ''));
    } else {
      console.warn(`[AIRS scan] ${s.message ?? s.kind}`);
    }
  }
  return high;
}

/**
 * Can this row be ANALYSED — i.e. is there a composition for the modals to open?
 *
 * ⚠ THIS IS THE PREDICATE THE ANALYSE AND FUNDAMENTAL BUTTONS RENDER ON, defined once so the list
 * and the buttons cannot disagree.
 *
 * ⚠ AND IT ASKS FOR HOLDINGS, NOT FOR A MODEL PORTFOLIO. It used to require `fixed_portfolio_id`,
 * which made both buttons depend on the Stamgegevens model scan — a SECOND workflow the account
 * scan never touches. Measured in production 2026-07-30: 46 books, every report fetched, and not
 * one button anywhere, because that scan had never run. Neither modal ever needed it; both take a
 * plain basket of `{isin, weight}`, and the Vermogensoverzicht's `ISIN-code` column is exactly
 * that. The pairing is preferred where it exists (attribution and the bucket drill-downs are
 * id-only) but it is an upgrade, not a prerequisite — see `openModal`.
 *
 * ⚠ AND UNDER `MIN_REAL_HOLDINGS` IS NOT A PORTFOLIO. The AIRS benchmarks carry exactly 1 holding
 * and the `_MV` / `WTS test` shells carry none, against 10-29 for every real book — so the same
 * threshold the backend now uses to skip them in the SCAN (`airs_vermogen.MIN_REAL_HOLDINGS`)
 * decides whether they are worth a row here. One rule, both ends: a book the scan stops fetching
 * must not keep a seat in the table.
 */
const MIN_REAL_HOLDINGS = 5;

const canAnalyse = (r: AirsPortfolioOverview) =>
  r.fixed_portfolio_id != null || (r.holdings ?? 0) >= MIN_REAL_HOLDINGS;

// ⚠ KEEP IN STEP WITH `airs_vermogen.REPORTS` — a code with no label here renders as a bare
// mnemonic in the "missing reports" gap list, which reads as a bug rather than as a named report.
const REPORT_LABELS: Record<string, string> = {
  att: 'Rendement',
  volk: 'Vermogensoverzicht',
  mut: 'Mutaties',
  trans: 'Transacties',
  model: 'Model',
};


export default function PortfolioOverviewPanel() {
  // ⚠ THE TABLE IS FOR EVERYONE; CHANGING IT IS NOT. Scraping AIRS, deleting an account and
  // pinning a Class / ISIN / Link are all admin-only at the API gate, so the controls are hidden
  // rather than left to 403 — a button that only fails is worse than no button. Every FIGURE stays
  // visible: a non-admin reads the same portfolios, holdings and returns an admin does.
  const isAdmin = useIsAdmin();
  const [rows, setRows] = useState<AirsPortfolioOverview[] | null>(null);
  // Sort. Name ascending by default: the list is read to FIND a portfolio far more often than to
  // rank one, and alphabetical is the only order you can navigate without reading every row.
  const [sortKey, setSortKey] = useState<SortKey>('name');
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('asc');
  const toggleSort = (k: SortKey) => {
    // Same column -> flip. New column -> its own natural first direction: A-Z for a name, but
    // biggest-first for a number, because nobody opens a returns column to see the worst.
    if (k === sortKey) setSortDir((d) => (d === 'asc' ? 'desc' : 'asc'));
    else { setSortKey(k); setSortDir(k === 'name' ? 'asc' : 'desc'); }
  };
  const [err, setErr] = useState<string | null>(null);
  const [open, setOpen] = useState<string | null>(null);
  const [detail, setDetail] = useState<Record<string, AirsAccountDetail>>({});
  const [isins, setIsins] = useState<Record<string, AirsAccountIsins>>({});
  const [hideSmall, setHideSmall] = useState(true);
  // The allocation-policy editor. Mounted only while open — it fetches its own grid, and a policy
  // nobody asked to see is not worth a request on every page load.
  const [showBands, setShowBands] = useState(false);
  // The Fixed portfolio to analyse. Its id, not the row's — the modal describes the strategy.
  /** `pf` is the AIRS portefeuille code — what `refreshOne` is keyed on, carried so the modal's
   *  Refresh fires the identical scan the row's does. */
  const [analyse, setAnalyse] = useState<
    { id?: number; name: string; basket?: Basket; pf?: string } | null>(null);
  // Refresh state: the fleet job is running; a status/error line; which single rows are re-scanning.
  const [refreshingAll, setRefreshingAll] = useState(false);
  /**
   * ⚠ `warn` IS NOT `error`, AND CONFLATING THEM COST A DAY. The fleet refresh fetches FOUR
   * reports for each of ~44 accounts, so 27 individual failures out of ~176 attempts is a job that
   * WORKED and is incomplete for some accounts — the backend says `status: "ok"` and stored a
   * snapshot. Painting that red says "the scan failed", which sends you looking for a broken
   * session or missing credentials when the actual answer is "thirteen books had no valuation
   * yet". A partial result and a dead job need different colours because they need different
   * reactions: one is "look at which accounts", the other is "the scraper is down".
   *
   * ⚠ THE KIND IS A COLOUR, NOT A STORY. It was called `partial` and a block of scan-specific
   * prose hung off that name, so every other amber outcome — a delete, most obviously — got the
   * scan's explanation printed underneath it. The kinds now say only how loud the line is; what
   * happened is in `text`, and the detail is in the console.
   */
  const [refreshMsg, setRefreshMsg] = useState<
    { text: string; kind: 'info' | 'error' | 'warn' | 'ok' } | null>(null);
  const [refreshingRows, setRefreshingRows] = useState<Set<string>>(new Set());
  /**
   * The job id behind each in-flight action, so its button can become a CANCEL.
   *
   * ⚠ THE ID, NOT A BOOLEAN. `refreshingAll` / `refreshingRows` already say "this is busy" and
   * that is all a spinner needs — but Cancel needs something to cancel, and it must be the job
   * THIS button started. A shared "is anything running" flag would offer a Cancel on every row
   * while only one of them could act, which is worse than not offering it at all.
   *
   * ⚠ THE TOAST'S OWN CANCEL IS THE SAME SCOPE HERE, deliberately: one press, one job. (In the
   * Benchmarks panel the two differ — there a run is a SEQUENCE of jobs, so the toast cancels a
   * leg and the panel's button cancels the run. Nothing on this panel is a sequence.)
   */
  const [fleetJob, setFleetJob] = useState<string | null>(null);
  const [rowJobs, setRowJobs] = useState<Record<string, string>>({});
  /**
   * WHICH ROWS THE READER HAS ASKED TO STOP — the PRESS, not the job's state.
   *
   * ⚠⚠ THE BUTTON MUST FLIP ON THE PRESS, NOT ON THE JOB ID ARRIVING, and that is what this is for.
   * Keyed on `rowJobs` the flip waited a round-trip: for that window the control read "Refresh",
   * enabled, over work that had already started — so a second press started a SECOND job (the
   * backend `_LOCK` then answered "busy") and a second toast appeared beside the first. The reader
   * sees their own click, so the state that follows it has to be one we set ourselves, synchronously.
   *
   * ⚠ AND A CANCEL PRESSED IN THAT SAME WINDOW MUST NOT BE LOST. There is nothing to cancel until
   * `startJob` returns an id, so the intent is recorded here and `refreshOne` fires it the instant
   * the id lands. Dropping it would be the same broken control seen from the other side.
   *
   * ⚠ A REF BESIDE THE STATE, KEPT IN STEP BY THE TWO HELPERS BELOW AND NOTHING ELSE. The state is
   * what renders; the ref is what `refreshOne` reads AFTER its `await`, where the closure's copy is
   * a snapshot from before the press. Neither is optional and neither is written directly.
   */
  const [cancelWanted, setCancelWanted] = useState<Set<string>>(new Set());
  const cancelWantedRef = useRef<Set<string>>(new Set());
  /** ⚠ THE SYNCHRONOUS "is this row already running", which `refreshingRows` cannot be: React
   *  batches, so two clicks in one tick both read the pre-click Set and both start a job. */
  const refreshingRef = useRef<Set<string>>(new Set());
  const setCancelWantedFor = (pf: string, wanted: boolean) => {
    const next = new Set(cancelWantedRef.current);
    if (wanted) next.add(pf); else next.delete(pf);
    cancelWantedRef.current = next;
    setCancelWanted(next);
  };
  /** Bumped when a row refresh finishes, so an open Analyse modal re-reads what it rebuilt. */
  const [refreshSeq, setRefreshSeq] = useState(0);
  const [deletingRows, setDeletingRows] = useState<Set<string>>(new Set());
  const [scanningModels, setScanningModels] = useState(false);
  /** Phase two's job id — the same shape as `fleetJob`, so the one button can offer Cancel for
   *  whichever half of "Refresh all" is currently running. */
  const [modelsJob, setModelsJob] = useState<string | null>(null);
  /** Whichever half of "Refresh all" is in flight. They run in sequence, never together, so one
   *  id is the whole truth — and the ✕ has to mean the same thing in both. */
  const allJob = fleetJob ?? modelsJob;
  const [opening, setOpening] = useState<string | null>(null);

  /**
   * Name one book, or clear the name.
   *
   * ⚠ THE NAME IS THE ACCOUNT'S, NOT THE MODEL'S. `display_name` on a model names a STRATEGY, and
   * an account only borrowed it through its pairing — so a book paired with no model could not be
   * named at all, which is backwards: those are precisely the rows still wearing AIRS's own code.
   *
   * ⚠ EMPTY CLEARS, AND CANCEL DOES NOT. `dialog.prompt` returns null on cancel and "" when the
   * field is emptied deliberately; collapsing the two would make "clear this name" unreachable and
   * every accidental Escape a silent rename.
   */
  const renameAccount = async (r: AirsPortfolioOverview) => {
    const p = r.dynamic_portefeuille;
    const next = await dialog.prompt(
      `A readable name for ${p}.\n\nLeave it empty to clear it and fall back to `
      + `${r.fixed_name ? 'the paired model portfolio’s name' : 'AIRS’s own code'}.`,
      { title: 'Name this portfolio', defaultValue: r.name_is_custom ? r.name : '', placeholder: p },
    );
    if (next == null) return;                       // cancelled — not the same as cleared
    try {
      const res = await apiFetch(`${API_URL}/api/airs/accounts/${encodeURIComponent(p)}/display-name`, {
        method: 'PUT', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ display_name: next.trim() || null }),
      });
      if (!res.ok) {
        const b = (await res.json().catch(() => null)) as { detail?: string } | null;
        console.warn(`[AIRS name] ${p} failed`, { status: res.status, body: b });
        setRefreshMsg({ text: b?.detail ?? `${p}: could not save the name (HTTP ${res.status})`, kind: 'error' });
        return;
      }
      await loadOverview();
    } catch (e) {
      console.warn(`[AIRS name] ${p} threw`, e);
      setRefreshMsg({ text: e instanceof Error ? e.message : String(e), kind: 'error' });
    }
  };

  /**
   * What Analyse should open for a row — the model portfolio if it has one, else the book's OWN
   * holdings. (Fundamental buttons used to share this — one per row, one per holding and one per
   * segment. All were removed 2026-08-04; this panel opens Analyse only.)
   *
   * ⚠ IT NEVER NEEDED A MODEL PORTFOLIO, AND WIRING IT TO ONE COST DAYS. It accepts a
   * plain basket of `{isin, weight}` — `PortfolioAnalysisModal`'s own comment says "a basket is
   * treated as a portfolio-of-N: same view" — and every account already carries exactly that, from
   * the Vermogensoverzicht's `ISIN-code` column. Gating the buttons on `fixed_portfolio_id` instead
   * made them depend on a SECOND scan (Stamgegevens → model portfolios) that the account workflow
   * never touches: in production 46 books were listed, fully scanned, and not one could be opened.
   *
   * The pairing is still PREFERRED where it exists, because the id-based view can do more — Brinson
   * attribution and the per-bucket drill-downs are portfolio-only. But it is an upgrade, not a
   * prerequisite: Front-Office → the four reports per book is the whole workflow, and it is enough.
   *
   * The ISINs come from `/isins`, which an expanded row has already loaded; otherwise one fetch.
   */
  const openModal = async (r: AirsPortfolioOverview) => {
    const set = setAnalyse;
    // ⚠ THE PORTEFEUILLE CODE TRAVELS WITH THE MODAL. `refreshOne` is keyed on it, not on the
    // fixed portfolio id, so without it the modal's Refresh has nothing to call — the row and the
    // modal must fire the identical scan.
    if (r.fixed_portfolio_id != null) {
      set({ id: r.fixed_portfolio_id, name: r.name, pf: r.dynamic_portefeuille });
      return;
    }
    const p = r.dynamic_portefeuille;
    setOpening(p);
    try {
      let resolved = isins[p];
      if (!resolved) {
        const i = await apiFetch(`${API_URL}/api/airs/accounts/${encodeURIComponent(p)}/isins`);
        if (!i.ok) throw new Error(`HTTP ${i.status}`);
        resolved = (await i.json()) as AirsAccountIsins;
        setIsins((m) => ({ ...m, [p]: resolved }));
      }
      // ⚠ ISIN-BEARING ROWS ONLY, AND WEIGHTS AS AIRS STATES THEM. A cash line has no ISIN and no
      // instrument to analyse; including it as a zero would put a phantom holding in every bucket.
      const holdings = (resolved.rows ?? [])
        .filter((h) => h.isin && (h.weight ?? 0) > 0)
        .map((h) => ({ isin: h.isin!, weight: h.weight!, name: h.holding_name }));
      if (!holdings.length) {
        setRefreshMsg({ text: `${r.name}: no ISIN-bearing holdings to analyse.`, kind: 'warn' });
        return;
      }
      set({ name: r.name, basket: { holdings, label: r.name }, pf: r.dynamic_portefeuille });
    } catch (e) {
      console.warn(`[AIRS expand] could not build a basket for ${p}`, e);
      setRefreshMsg({ text: `${r.name}: ${e instanceof Error ? e.message : String(e)}`, kind: 'error' });
    } finally {
      setOpening(null);
    }
  };

  /**
   * Run the MODEL-PORTFOLIO scan — the prerequisite this page has no other way to satisfy.
   *
   * ⚠ IT IS A SEPARATE SCAN FROM "Refresh all", AND THAT IS THE WHOLE TRAP. Refresh all scans the
   * ACCOUNTS (returns, holdings, mutations); this one scans the MODEL portfolios that give an
   * account its name, its ISINs and — the part you notice — its Analyse button.
   * Until 2026-07-30 it had no button anywhere in the app, so a fresh deployment could never get
   * past "0 analysable" from inside the UI.
   *
   * ⚠⚠ A JOB, NOT AN SSE STREAM INTO THE CONSOLE (2026-08-17). It is minutes long — the list lands
   * in ~6s, then an edit-page GET and an XLS download for each of ~58 fixed portfolios — and it
   * used to report ONLY via `console.warn`. So for the whole slow half of "Refresh all" the single
   * thing on screen saying anything was happening was this button's own label: no `i/n`, no name of
   * the portfolio in flight, nothing at all after a route change or a reload, and no way to stop
   * it. Phase one had been a proper job since 2026-08-13; the two halves of one button reported
   * two different ways, and the silent way was the one that took the minutes.
   *
   * Now it lands in the shared toast stack beside every other job on this page, re-attaches after a
   * reload via `attachRunningJobs`, and its Cancel reaches the scan (which stops between
   * portfolios, keeping everything already counted).
   *
   * ⚠ THE CONSOLE NARRATION IS KEPT, DELIBERATELY REDUCED TO THE OUTCOME. The toast answers "is it
   * moving"; the console answers "what did it do". What is gone is the per-event `console.warn`
   * relay, which was only ever a stand-in for the progress line this now has.
   */
  const scanModels = async (force: boolean) => {
    // ⚠ SKIPPED UNLESS IT WOULD CHANGE SOMETHING. This is the slow half — a list page plus an
    // edit-page GET and an XLS download for each of ~95 portfolios, minutes every time — and it
    // describes models, which change when somebody EDITS one, not daily. Running it on every press
    // would make the routine refresh unusable to keep a nickname current. Missing entirely is the
    // case that matters (a fresh deployment), and shift-click forces the rest.
    const have = (rows ?? []).some((r) => r.fixed_portfolio_id != null);
    if (have && !force) {
      console.warn('[AIRS models] skipped — models already scanned (shift-click Refresh all to re-scan)');
      return;
    }
    setScanningModels(true);
    console.warn('[AIRS models] scanning Stamgegevens → Model portefeuilles (minutes)…');
    try {
      const { id, done } = await startJob(
        `${API_URL}/api/airs/model-portfolios/scan/job`, 'Scan model portfolios');
      setModelsJob(id);
      const job = await done;
      // ⚠ `failed` GETS THE CONSOLE, THE OTHER TWO GET A LINE. The toast already carries the
      // outcome; this is the copy you can scroll back to after the card has dismissed itself.
      if (job.status === 'failed') logDetail('model scan failed', job.summary);
      else console.warn(`[AIRS models] ${job.status} — ${job.summary ?? ''}`);
    } catch (e) {
      // ⚠ REPORTED, NEVER RAISED INTO THE ACCOUNT SCAN'S RESULT. This is the CRM lesson: a failure
      // in a scan of DIFFERENT objects must not appear in the account refresh's error summary, or
      // a portfolio refresh reports a fault in a report it was never asked to fetch. One button,
      // two subjects, two verdicts.
      console.warn('[AIRS models] scan failed — the accounts are unaffected', e);
    } finally {
      setScanningModels(false);
      setModelsJob(null);
    }
  };

  /**
   * Delete one account's scraped rows — the way to prove Refresh all actually refills a gap.
   *
   * ⚠ IT ASKS FIRST, AND THE QUESTION NAMES WHAT DOES NOT COME BACK. A scan fetches `1 Jan →
   * today`, so any performance month before January is gone permanently; "the refresh will
   * restore it" is true only for this year. `dialog.confirm`, never the native one.
   */
  const deleteOne = async (portefeuille: string) => {
    const ok = await dialog.confirm(
      `Delete every scraped row for ${portefeuille}?\n\n`
      + 'Removes its returns, holdings, mutations, model weights, roster entry and model pairing. '
      + 'A refresh rebuilds them — but only from 1 January, so any earlier month is lost for good.\n\n'
      + 'CRM records and the hidden-account decision are not touched.',
    );
    if (!ok) return;
    setDeletingRows((s) => new Set(s).add(portefeuille));
    try {
      const r = await apiFetch(`${API_URL}/api/airs/portfolios/${encodeURIComponent(portefeuille)}`,
        { method: 'DELETE' });
      const b = (await r.json().catch(() => null)) as
        { total_rows?: number; deleted?: Record<string, number> } | null;
      if (!r.ok) {
        logDetail(`delete ${portefeuille} failed`, { status: r.status, body: b });
        setRefreshMsg({ text: `${portefeuille}: delete failed — HTTP ${r.status}`, kind: 'error' });
      } else {
        // The row count and its per-table breakdown are what tell "it deleted nothing" from "it
        // deleted 400 rows" — a diagnostic. A number of rows is not something the operator asked
        // for or can act on: they deleted an account, and the only thing left to say is how to get
        // it back. So the counts go to the console and the line says what happened and what next.
        logDetail(`deleted ${portefeuille}`,
          { total_rows: b?.total_rows ?? 0, per_table: b?.deleted ?? {} });
        setRefreshMsg({
          text: `${portefeuille}: deleted. Run Refresh all to rebuild it.`,
          kind: 'warn',
        });
      }
      await loadOverview();
    } catch (e) {
      logDetail(`delete ${portefeuille} threw`, e);
      setRefreshMsg({ text: e instanceof Error ? e.message : String(e), kind: 'error' });
    } finally {
      setDeletingRows((s) => { const n = new Set(s); n.delete(portefeuille); return n; });
    }
  };

  // Fetch ONE account's holdings + ISIN resolution into the caches. Split out of `expand` because
  // a refresh has to be able to re-fetch a row that is ALREADY open: clearing the caches without
  // re-fetching left the open row on "Loading holdings…" for ever, since nothing re-requests until
  // the next click — the row had to be collapsed and re-expanded by hand to recover.
  const loadDetail = useCallback(async (p: string) => {
    // ⚠ TIMED, BECAUSE "IT TAKES A WHILE" IS NOT A BUG REPORT. Expanding a row fires three
    // requests and the slow one is not the obvious one: measured 2026-07-30, `/isins` spent
    // 11,537 ms of 11,793 ms inside a single step (refreshing stale prices from Yahoo) while
    // every DB read was under 60 ms. The backend returns its own per-phase breakdown; this
    // logs the wall time around it so network and server time are told apart.
    const t0 = performance.now();
    // Fetched together: a holding briefly showing its value without its identity, or worse with
    // the wrong one, is not an improvement over showing neither.
    const [h, i] = await Promise.all([
      apiFetch(`${API_URL}/api/airs/accounts/${encodeURIComponent(p)}/holdings`),
      apiFetch(`${API_URL}/api/airs/accounts/${encodeURIComponent(p)}/isins`),
    ]);
    console.warn(`[AIRS expand] ${p}: holdings+isins in ${Math.round(performance.now() - t0)}ms`);
    if (!h.ok) return;
    // Awaited BEFORE the updaters: a setState callback is not async, so `await` inside one
    // stores the Promise itself and the row renders `[object Promise]`-shaped nothing.
    const holdings = (await h.json()) as AirsAccountDetail;
    const resolved = i.ok ? ((await i.json()) as AirsAccountIsins) : null;
    // The server's own breakdown, slowest step first — this is what names the phase to argue with.
    const ms = (resolved as { timings_ms?: Record<string, number> } | null)?.timings_ms;
    if (ms) {
      console.warn(`[AIRS expand] ${p}: server phases (ms)`,
        Object.fromEntries(Object.entries(ms).sort((a, b) => b[1] - a[1])));
    }
    setDetail((d) => ({ ...d, [p]: holdings }));
    if (resolved) setIsins((m) => ({ ...m, [p]: resolved }));
  }, []);

  const loadOverview = useCallback(async () => {
    await traceScope('overview', 'loading the portfolios table', async () => {
      try {
        const r = await apiFetch(`${API_URL}/api/airs/portfolios/overview`);
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        const body = (await r.json()) as AirsPortfolioOverview[];
        // ⚠ AN EMPTY TABLE IS THE STATE A FRESH PRODUCTION DATABASE IS IN, and it renders exactly
        // like a broken page. It is not an error — nothing has been scanned yet — so it is a
        // WARN with the remedy in it, not a red banner and not silence.
        traceRows('overview', 'portfolios', body,
          'nothing in `airs_account_roster` / `airs_performance` yet. This is what a fresh '
          + 'database looks like — press "Refresh all from AIRS" to run the Front-Office '
          + 'discovery and populate it. Not an error.');
        if (body.length) {
          // The three facts that decide whether every OTHER number on the page can exist. A row
          // with no pairing has no Analyse-by-id; a row with no YTD had no Rendement report.
          const paired = body.filter((x) => x.fixed_portfolio_id != null).length;
          const withYtd = body.filter((x) => x.ytd_pct != null).length;
          const incomplete = body.filter((x) => (x.missing_reports ?? []).length > 0).length;
          trace('overview', `${paired}/${body.length} paired with a model portfolio, `
            + `${withYtd} carry a YTD, ${incomplete} are missing at least one AIRS report`);
          if (!withYtd) {
            traceEmpty('overview', 'no portfolio has a YTD',
              '`airs_performance` has no rows for these books — the Rendement (ATT) report has '
              + 'never been scraped. Every return column on this page will be blank.');
          }
        }
        setRows(body);
        setErr(null);
      } catch (e) {
        traceError('overview', 'the portfolios table could not be loaded', e);
        setErr(e instanceof Error ? e.message : String(e));
      }
    });
  }, []);

  useEffect(() => { void loadOverview(); }, [loadOverview]);

  /**
   * Re-scan ONE portfolio's AIRS reports (Rendement + Vermogensoverzicht) and reload its figures.
   *
   * ⚠ IT RUNS AS A JOB, AND THE PROGRESS GOES TO THE SHARED TOAST STACK (`lib/stores/jobs.ts`,
   * rendered from the root layout) — the same one the fundamentals ingests report into. Three
   * things that a plain POST could not give it, and this refresh needs all three:
   *   * a line that MOVES. With the cascade this is five downloads per account over a chain that
   *     reaches nine (TOPS_BEOFF_BEH_DYN); a disabled button with nothing happening reads as hung;
   *   * progress that SURVIVES navigating away — the work carried on regardless, it just had
   *     nothing on screen to say so;
   *   * re-attachment on reload (`attachRunningJobs`), so a refresh is never invisible.
   *
   * ⚠ THE INLINE `refreshMsg` LINE IS GONE FOR THIS ACTION, deliberately. Two places reporting one
   * job is two places to keep in step, and the toast already carries the outcome, the failure and
   * the countdown. `refreshMsg` stays for everything else on this panel that is NOT a job.
   */
  const refreshOne = async (portefeuille: string) => {
    // ⚠ ONE JOB PER ROW, ENFORCED HERE AND NOT BY THE BUTTON'S `disabled`. A second job on the same
    // account cannot do any work — the backend `_LOCK` answers "busy" — but it DOES get a job id
    // and therefore a second toast, which is the duplicate card this guard exists to prevent. The
    // ref, not `refreshingRows`: React batches, so two clicks in one tick see the same stale Set.
    if (refreshingRef.current.has(portefeuille)) return;
    refreshingRef.current = new Set(refreshingRef.current).add(portefeuille);
    setRefreshingRows((s) => new Set(s).add(portefeuille));
    try {
      const { id, done } = await startJob(
        `${API_URL}/api/airs/portfolios/${encodeURIComponent(portefeuille)}/refresh/job`,
        portefeuille);
      setRowJobs((m) => ({ ...m, [portefeuille]: id }));
      // ⚠ THE CANCEL THAT ARRIVED BEFORE THE ID. The button flips on the press, so Cancel is
      // pressable during the round-trip above — and a press that reached a control offering it must
      // act, not evaporate because the handle it needed was still in flight.
      if (cancelWantedRef.current.has(portefeuille)) void cancelJob(id);
      const job = await done;
      // ⚠ RELOAD ON ANYTHING THAT REACHED THE SERVER, not only on `done`. A failed cascade still
      // wrote every account it got through, so leaving the pre-refresh figures on screen would
      // hide real work that was really done — the same rule the bulk fundamentals fill follows.
      if (job.status === 'failed') logDetail(`refresh ${portefeuille} failed`, job.summary);
      setDetail((d) => { const n = { ...d }; delete n[portefeuille]; return n; });
      setIsins((m) => { const n = { ...m }; delete n[portefeuille]; return n; });
      await loadOverview();
      // ⚠ Dropping the cache is only half of it. An OPEN row re-renders straight into
      // "Loading holdings…" and stays there, because only a click re-requests — so re-fetch here.
      if (open === portefeuille) await loadDetail(portefeuille);
    } catch (e) {
      logDetail(`refresh ${portefeuille} threw`, e);
      setRefreshMsg({ text: `${portefeuille}: ${e instanceof Error ? e.message : String(e)}`, kind: 'error' });
    } finally {
      refreshingRef.current = (() => {
        const n = new Set(refreshingRef.current); n.delete(portefeuille); return n;
      })();
      setRefreshingRows((s) => { const n = new Set(s); n.delete(portefeuille); return n; });
      setRowJobs((m) => { const n = { ...m }; delete n[portefeuille]; return n; });
      // The press is spent with the job it was aimed at — the row goes back to offering Refresh.
      setCancelWantedFor(portefeuille, false);
      // ⚠ AND THE ANALYSE MODAL, IF IT IS OPEN ON THIS PORTFOLIO. It is drawn from the composition
      // and holdings this scan just re-read, and it has already loaded — so without a nudge it
      // would sit showing pre-scan figures while the row behind it updated, which reads as the
      // button having done nothing. A counter, not a boolean: two scans in a row must both land.
      setRefreshSeq((s) => s + 1);
    }
  };

  /**
   * Scan every live portfolio that NEEDS scanning — as a CANCELLABLE JOB.
   *
   * ⚠ INCREMENTAL, AND THE BACKEND DECIDES. Discovery always runs against AIRS, so an account that
   * is new or was deleted is always fetched; one whose last pass got all four reports recently is
   * skipped. A full pass is minutes, a no-op pass is seconds — hence `force` for when you actually
   * want the four downloads again.
   *
   * ⚠⚠ THIS WAS A POLL LOOP AND THAT IS WHY THE BUTTON KEPT READING AS BROKEN (2026-08-11). It
   * POSTed `/vermogen/refresh`, then re-read `/vermogen/status` every 2.5s and painted its own
   * banner. Three consequences, all of them the reader's problem rather than the code's:
   *   * the work was INVISIBLE the moment you navigated away or reloaded — it carried on, with
   *     nothing on screen to say so;
   *   * there was NO WAY TO STOP IT, so a full re-scan started by accident ran its minutes out;
   *   * the panel carried a second progress vocabulary that had to be kept in step with the toast
   *     every other button on this page already used.
   * As a job it reports into the shared toast stack, survives the route change, re-attaches on
   * reload (`attachRunningJobs`), and Cancel reaches the scan.
   *
   * ⚠ THE INLINE `refreshMsg` LINE IS GONE FOR THIS ACTION, deliberately — the same rule
   * `refreshOne` follows. Two places reporting one job is two places to keep in step, and the
   * toast already carries the outcome, the failure and the countdown. `refreshMsg` stays for
   * everything on this panel that is NOT a job.
   *
   * ⚠ THE STEP-BY-STEP CONSOLE NARRATION IS KEPT, BUT PRINTED ONCE AT THE END. It used to come
   * from the polled `log` array via a high-water mark on `seq`; there is no poll any more, so it
   * is read from `/vermogen/status` in a single request after the job resolves. That log is the
   * only place the ROSTER appears in full (44 names to compare against AIRS's own "44 Items in
   * selectie") and the only per-report ✓/—/✗ breakdown — the comments on `logSteps` record that
   * this is where the bugs kept being found, so dropping it to save one request would have been a
   * real loss of diagnosability disguised as cleanup. The toast carries `i/n: name` live, which is
   * what answers "is it moving"; this answers "what did it actually do".
   */
  const refreshAll = async (force = false) => {
    if (refreshingAll) return;
    setRefreshingAll(true);
    try {
      const { id, done } = await startJob(
        `${API_URL}/api/airs/vermogen/refresh/job${force ? '?force=true' : ''}`,
        force ? 'Refresh all (full re-scan)' : 'Refresh all portfolios');
      setFleetJob(id);
      const job = await done;
      // ⚠ CLEARED THE MOMENT PHASE ONE RESOLVES, not in the `finally`. Phase two is a job of its
      // own now, and `allJob = fleetJob ?? modelsJob` is only the LIVE one if the finished half
      // stops claiming to be running — otherwise the ✕ would spend the whole model scan wired to a
      // job that had already ended. The `finally` still clears it; setting null twice is free.
      setFleetJob(null);
      // ⚠ RELOAD ON ANYTHING THAT REACHED THE SERVER — done, failed OR cancelled. The scan stores
      // each account as it goes, so a run stopped after 30 of 44 wrote 30 books; leaving the
      // pre-refresh figures on screen would hide real work that was really done.
      if (job.status === 'failed') logDetail('fleet refresh failed', job.summary);
      // ⚠ BEST-EFFORT, AND NEVER A REASON TO FAIL THE REFRESH. The scan is already finished and
      // its rows are already stored; a console diagnostic that could not be fetched must not turn
      // a completed run into an error on screen.
      try {
        const st = await (await apiFetch(`${API_URL}/api/airs/vermogen/status`)).json() as
          { detail?: string; errors?: string[]; error_summary?: FailureGroup[];
            log?: ScanStep[] } | null;
        logSteps(st?.log ?? [], 0);
        if (st?.detail) logDetail('scan detail', st.detail);
        // The REASON grouped by cause, not 27 individual lines nobody reads looking for a pattern.
        if (st?.error_summary?.length) {
          logDetail('reports that failed, grouped by cause', st.error_summary);
        }
        if (st?.errors?.length) logDetail('raw scan errors', st.errors);
      } catch (e) {
        logDetail('could not read the scan log (the scan itself finished)', e);
      }
      setDetail({});
      setIsins({});
      await loadOverview();
      // ⚠ PHASE TWO OF THE SAME BUTTON, AND IT IS SKIPPED ON CANCEL. Front-Office → the four
      // reports per book is the whole workflow; this adds the model portfolios, which supply the
      // readable nickname and the id-only views. Running it after the reader pressed Cancel would
      // be minutes more of exactly what they asked to stop.
      if (job.status === 'done') {
        await scanModels(force);
        setDetail({});
        setIsins({});
        await loadOverview();
      }
      if (open) await loadDetail(open);   // same trap as refreshOne — an open row must re-fetch
    } catch (e) {
      logDetail('fleet refresh threw', e);
      setRefreshMsg({ text: e instanceof Error ? e.message : String(e), kind: 'error' });
    } finally {
      setRefreshingAll(false);
      setFleetJob(null);
    }
  };

  /**
   * Stop the fleet scan the reader started.
   *
   * ⚠ IT STOPS BETWEEN ACCOUNTS, NOT INSIDE ONE, and the button says so. An account's four reports
   * are downloaded and stored as a unit — stopping midway would leave a book holding two fresh
   * reports and two stale ones with nothing on the row to say which. So Cancel waits out the
   * account in flight (seconds), and everything already stored is kept.
   */
  /**
   * Stop ONE row's re-scan.
   *
   * ⚠ THE CASCADE IS WHY THIS IS WORTH HAVING. A single row is not one download: with the
   * look-through chain it is five per account over a chain reaching NINE books, so a press on the
   * wrong row is minutes, not seconds.
   *
   * ⚠ IT REPORTS RATHER THAN STOPS MID-CHAIN, and that is the backend's rule, not an oversight:
   * `/refresh/job` deliberately has no `ctx.check()` inside the scan, because a half-finished
   * cascade leaves a parent fresh against stale children — the exact state that endpoint exists to
   * avoid. So Cancel is honoured at the job boundary; the button says the chain finishes first.
   */
  /**
   * ⚠ NO INLINE `refreshMsg` LINE ON EITHER CANCEL — the same rule the two refresh actions above
   * already follow, which these two had quietly broken. `cancelJob` puts "cancelling…" on the job's
   * own card the instant the button is pressed (`cancelRequested`), and that card then carries the
   * outcome, how far it got and the countdown. A banner saying the same thing is a second place to
   * keep in step, in a different corner of the screen, that nothing ever clears.
   *
   * ⚠ AND IT WAS NOT CARRYING THE NUANCE EITHER — both buttons' own tooltips say the in-flight
   * account (or chain) finishes first and everything already downloaded is kept, which is the one
   * moment it is worth reading: BEFORE the press.
   */
  const cancelRefreshRow = async (portefeuille: string) => {
    if (cancelWantedRef.current.has(portefeuille)) return;   // already asked; asking twice is a no-op
    // ⚠ RECORDED BEFORE THE REQUEST, so the button changes on the press rather than on the reply.
    // If the job id is not back yet this is ALL that happens here and `refreshOne` fires the cancel
    // the moment it has one — the press is never dropped, only deferred.
    setCancelWantedFor(portefeuille, true);
    const id = rowJobs[portefeuille];
    if (!id) return;
    await cancelJob(id);
  };

  /**
   * ⚠ WHICHEVER HALF IS RUNNING. "Refresh all" is two jobs in sequence and only one of them can be
   * in flight, so ONE id is enough — but it must be the live one. Keying this on `fleetJob` alone
   * left the button reading "Cancel scan" through phase two and cancelling nothing, which is the
   * decorative-Cancel failure the job registry exists to prevent.
   */
  const cancelRefreshAll = async () => {
    if (modelsJob) { await cancelJob(modelsJob); return; }
    if (!fleetJob) return;
    await cancelJob(fleetJob);
  };

  /**
   * ⚠ ADMIN ONLY (2026-08-06). The expanded row is the ACCOUNT's own book — its positions and
   * their EUR values, its mutations for the year, and the reconciliation against AIRS. The table
   * above it is a summary; this is the money.
   *
   * ⚠ THE GUARD IS HERE AS WELL AS ON THE ROW, and neither is the access rule. Making the `<tr>`
   * inert covers the click, but `expand` is a plain function on a component a non-admin renders —
   * anything that reaches it later (a keyboard handler, a deep link, an "expand all") would walk
   * straight past a `cursor-default`. The rule that actually holds is on the server: the four
   * sub-resources this opens are admin-only in `_auth_middleware.py`, so the worst a bypassed UI
   * can do is render an empty panel. `/isins` is NOT among them — the Analyse button shares it and
   * non-admins keep Analyse.
   */
  const expand = async (p: string) => {
    if (!isAdmin) return;
    setOpen(open === p ? null : p);
    if (detail[p] || open === p) return;
    await loadDetail(p);
  };

  // Re-fetch just one account's ISIN resolution (after a manual Class override), so the row
  // re-groups under its new bucket without collapsing/re-opening the whole holdings table.
  const refreshIsins = useCallback(async (p: string) => {
    const i = await apiFetch(`${API_URL}/api/airs/accounts/${encodeURIComponent(p)}/isins`);
    if (!i.ok) return;
    const resolved = (await i.json()) as AirsAccountIsins;
    setIsins((m) => ({ ...m, [p]: resolved }));
  }, []);

  /**
   * ⚠ THE FILTER KEEPS WHAT THE PAGE CAN ACTUALLY DO SOMETHING WITH — `canAnalyse`, the same
   * predicate the Analyse button renders on.
   *
   * It went through a wrong turn worth recording. It was "Linked only", which hid two books that
   * showed real figures (`BUS_Ris_bepOff_Kl_AFS_Dy`, 24 holdings; `BUS_WTS_StMerken_Dyn`, 22), so
   * it was changed to keep anything with real holdings. That surfaced them — and surfaced that
   * they are the ONLY two rows with no Analyse button, because they pair with no
   * model portfolio. Both turned out to be bogus books. Holdings measure whether a row has DATA;
   * the buttons measure whether a row is USABLE, and this table exists to open them.
   *
   * Defining it as the buttons' own condition is the point: a row can no longer appear in the list
   * and then refuse to do the thing the list is for.
   *
   * ⚠ AND IT CAN NEVER EMPTY A FULL TABLE. On a fresh deployment the accounts scan runs before any
   * model scan, so nothing is paired; that filter hid all 44 and the page read as a failed scan.
   * Whatever the rule, if it would leave nothing it does not apply.
   */
  const substantial = (rows ?? []).filter(canAnalyse).length;
  const effectiveHideSmall = hideSmall && substantial > 0;

  const view = (() => {
    const base = (rows ?? []).filter((r) => (effectiveHideSmall ? canAnalyse(r) : true));
    const dir = sortDir === 'asc' ? 1 : -1;
    const val = (r: AirsPortfolioOverview): string | number | null => (
      sortKey === 'name' ? (r.name ?? '')
        : sortKey === 'isins' ? r.isins ?? null
          : sortKey === 'ytd' ? r.ytd_pct ?? null
            : r.latest_month_pct ?? null);
    return [...base].sort((a, b) => {
      const x = val(a), y = val(b);
      // ⚠ ABSENT SORTS TO THE BOTTOM IN BOTH DIRECTIONS. A portfolio with no ISINs or no return
      // has no value here — it is not a very small one. Letting null fall through to a numeric
      // compare would park every unlinked book at the top of an ascending sort and read as "these
      // are the worst performers", which is a claim the data never made.
      if (x == null && y == null) return 0;
      if (x == null) return 1;
      if (y == null) return -1;
      return (typeof x === 'string'
        // localeCompare so "AziëTopSelectie" files under A, not after Z.
        ? x.localeCompare(String(y), undefined, { sensitivity: 'base' })
        : (x as number) - (y as number)) * dir;
    });
  })();

  return (
    <section className="bg-card border border-neutral-800/40 rounded-xl p-5 space-y-3">
      <div className="flex items-baseline justify-between gap-4 flex-wrap">
        <div>
          <h3 className="text-sm font-semibold text-fg-strong">
            Portfolios{rows ? ` · ${view.length}` : ''}
          </h3>
          <p className="text-[12px] text-fg-faint mt-0.5 max-w-3xl">
            {'Named from the Fixed portfolio; figures from AIRS, year to date.'}
            {/* Only an admin can open a row, so only an admin is told to — an instruction that
                does not work is worse than none. */}
            {isAdmin && ' Expand a row for holdings.'}
          </p>
        </div>
        <div className="flex items-center gap-3 flex-wrap">
          {/* Scan every portfolio that NEEDS it — the backend skips an account whose last pass got
              all four reports within the last few hours, so a press after a full run is seconds
              rather than minutes. Shift-click forces a full re-scan. */}
          {isAdmin && (
            <button type="button"
              onClick={(e) => { if (allJob) { void cancelRefreshAll(); } else { void refreshAll(e.shiftKey); } }}
              // Inert only in the gap between the press and the job id arriving — see the row button.
              disabled={refreshingAll && !allJob}
              title={allJob
                ? (modelsJob
                  ? 'Stop the model-portfolio scan. The portfolio being counted finishes first (seconds), then it stops — every count already stored is kept.'
                  : 'Stop the scan. The account being read finishes first (seconds), then it stops — everything already downloaded is kept.')
                : 'Everything AIRS has: Rapportage → Front-Office (Actieve · Interne · zonder consolidatie), then Rendement, Vermogensoverzicht, Mutaties and Model for each book — plus the model portfolios if they have never been scanned. An account fully scanned in the last few hours is skipped. Shift-click forces a full re-scan of everything (minutes).'}
              className={`inline-flex items-center gap-1.5 text-xs px-2.5 py-1 rounded-lg border transition-colors disabled:opacity-50 disabled:cursor-wait ${
                allJob
                  ? 'border-warn-500/50 text-warn-400 hover:bg-warn-500/10'
                  : 'border-neutral-700 text-fg-subtle hover:text-accent-300 hover:border-accent-500/50'}`}>
              {allJob ? <span className="text-[11px] leading-none">✕</span>
                      : <RefreshIcon spinning={refreshingAll} size={12} />}
              {/* ⚠ BOTH PHASES NOW OFFER CANCEL. Phase two used to run after the fleet job had
                  resolved, so `fleetJob` was already null and the label was the only thing left
                  saying the button was busy — a control that reads "Scanning models…" for minutes
                  with no way to stop it. It is a job of its own now, so `allJob` is whichever half
                  is live and the ✕ means the same thing throughout. */}
              {allJob ? (modelsJob ? 'Cancel model scan' : 'Cancel scan')
                : refreshingAll ? (scanningModels ? 'Scanning models…' : 'Refreshing…')
                : 'Refresh all'}
            </button>
          )}
          {/* The POLICY, beside the thing that measures against it. Shown to everyone and editable
              by admins only — a non-admin reading what the bands are supposed to be is exactly the
              use this has for them, and hiding it would leave the numbers on this page with no
              stated target at all. */}
          <button type="button" onClick={() => setShowBands(true)}
            title="Per risk profile (Offensief / Beperkt Offensief / Neutraal / Defensief), the minimum, default and maximum share each asset class may take."
            className="inline-flex items-center gap-1.5 text-xs px-2.5 py-1 rounded-lg border border-neutral-700 text-fg-subtle hover:text-accent-300 hover:border-accent-500/50 transition-colors">
            Asset allocatie
          </button>
          {/* ⚠ ONE BUTTON. It ran as two for a while — accounts here, model portfolios on a second
              control — which put an implementation rule (keep the two scans' error verdicts apart)
              in front of the operator as a chore. They are phases of one action now; only the
              reporting stays separate. */}
          {rows && (
            <label className={`flex items-center gap-1.5 text-xs cursor-pointer whitespace-nowrap ${
              substantial === 0 ? 'text-fg-faint' : 'text-fg-subtle'}`}
              title={substantial === 0
                ? 'No account is paired with a model portfolio yet, so this filter would hide every row — it is inactive until a model-portfolio scan has run.'
                : 'Keeps only the books that can actually be opened: Analyse describes the paired model portfolio, so a book with no model has no button and nothing this page can do with it. Hides the AIRS benchmarks and the test shells for the same reason.'}>
              <input type="checkbox" checked={hideSmall} disabled={substantial === 0}
                onChange={(e) => setHideSmall(e.target.checked)} />
              {/* ⚠ Disabled at zero — see `effectiveHideSmall`. A checkbox that silently does
                  nothing is worse than one that says it cannot. */}
              Analysable only ({substantial} of {rows.length})
            </label>
          )}
        </div>
      </div>

      {/* Refresh outcome — LOUD, so a failed AIRS scan (session expired, backend down, no
          credentials) is never mistaken for "nothing happened". Three states, because there are
          three: green = every account came back whole; RED = nothing was stored, the scan is
          broken; AMBER = a snapshot was written but some reports did not arrive, which is a thing
          to read rather than a thing to fix. */}
      {/* ⚠ NO "MISSING PREREQUISITE" BANNER, BECAUSE THERE IS NO PREREQUISITE. One lived here
          briefly, announcing that the model scan had to run before anything could be analysed —
          which was the bug, not the diagnosis: both modals take a basket, and Front-Office → the
          four reports per book already supplies one. The model scan is now what it always was, an
          upgrade (nicknames, Brinson attribution, the bucket drill-downs), and it sits beside
          Refresh all rather than blocking the page. */}
      {refreshMsg && (
        <div className={`text-[12px] rounded-lg px-3 py-1.5 border ${
          refreshMsg.kind === 'error' ? 'text-neg-300 bg-neg-500/10 border-neg-500/20'
            : refreshMsg.kind === 'warn' ? 'text-warn-300 bg-warn-500/10 border-warn-500/20'
              : refreshMsg.kind === 'ok' ? 'text-pos-300 bg-pos-500/10 border-pos-500/20'
                : 'text-fg-subtle bg-overlay/[0.03] border-neutral-800/40'}`}>
          {/* ⚠ ONE LINE, AND NOTHING UNDER IT. What failed and why is `logDetail`'d — see the note
              on it. The per-account detail is not lost from the PAGE either: a row short a report
              still carries its own ⚠ badge naming which, right where you would act on it. */}
          {refreshMsg.text}
        </div>
      )}

      {/* ⚠ NO "N PAIRINGS UNCONFIRMED" BANNER. A name match IS how a pairing is normally made —
          it was firing on 27 of 28 rows, every session, with no action attached to it, which is a
          warning about the software's ordinary behaviour rather than about this data. The guess is
          still recomputed on every read (never frozen into the table), so it self-corrects when a
          portfolio is renamed, and the Link control still overrides it per row. */}

      {!rows && !err && <p className="text-xs text-fg-subtle">Loading…</p>}
      {err && (
        <div className="bg-neg-500/10 border border-neg-500/20 rounded-lg px-3 py-2 text-xs text-neg-300">{err}</div>
      )}

      {rows && (
        <div className="overflow-x-auto rounded-lg border border-neutral-800/40">
          {/* ⚠ `overflow-x-auto`, NOT `overflow-auto`, and no max-height: the table grows to its
              content and the PAGE scrolls it. The horizontal container has to stay — 17 columns
              are wider than a phone, and the repo rule is that a dense table scrolls inside its
              own box so the page never scrolls sideways. */}
          <table className="w-full text-xs whitespace-nowrap">
            <thead className="bg-card z-10 [&_th]:bg-card">
              <tr className="text-fg-faint text-[11px] uppercase tracking-wide border-b border-neutral-800/40">
                {/* ⚠ A POSITION IN THE LIST, NOT AN ID. It renumbers when the list is filtered
                    or re-sorted, which is the point — it is there to say "the 14th row", so two
                    people can talk about the same line. `text-right` so the digits align. */}
                <th className="px-3 py-1.5 font-medium text-right w-8">#</th>
                <th className="px-3 py-1.5 font-medium text-left" />{/* Analyse */}
                <SortTh label="Name" k="name" sortKey={sortKey} sortDir={sortDir} onSort={toggleSort} />
                <SortTh label="ISINs" k="isins" align="right"
                  sortKey={sortKey} sortDir={sortDir} onSort={toggleSort}
                  title="Positions in the Fixed portfolio — the ISINs this pairing can reach. Blank = not linked to one." />
                <SortTh label="YTD" k="ytd" align="right"
                  sortKey={sortKey} sortDir={sortDir} onSort={toggleSort}
                  title="AIRS's own cumulatief_rendement for the year — each month's investment return compounded. It accounts for deposits and withdrawals, so it is not just (end value ÷ start value − 1)." />
                <SortTh label="Current month" k="month" align="right"
                  sortKey={sortKey} sortDir={sortDir} onSort={toggleSort}
                  title="AIRS's rendement from its newest row — the current (latest) month, a different window from the year. Not a rival YTD." />
                {/* Rightmost, and unlabelled: a destructive action wants distance from the figures,
                    not a heading advertising it. */}
                <th className="px-3 py-1.5 font-medium text-right w-8" />
              </tr>
            </thead>
            <tbody className="divide-y divide-neutral-800/20">
              {view.map((r, rowNo) => {
                const isOpen = open === r.dynamic_portefeuille;
                return (
                  <Fragment key={r.dynamic_portefeuille}>
                    {/* ⚠ THE ROW IS ONLY A CONTROL FOR AN ADMIN. For everyone else it carries no
                        handler and no pointer — the accent hover is what says "this opens", so
                        leaving it on a row that cannot open reads as a broken table rather than a
                        restricted one. `group` stays either way: the action cells inside still
                        reveal on hover. */}
                    <tr onClick={isAdmin ? () => void expand(r.dynamic_portefeuille) : undefined}
                      className={`group transition-colors ${isAdmin
                        ? 'hover:bg-accent-500/10 cursor-pointer'
                        : 'hover:bg-overlay/[0.02]'}`}>
                      <td className="px-3 py-1.5 text-right font-mono text-fg-faint tabular-nums">
                        {rowNo + 1}
                      </td>
                      {/* Analyse, leftmost. Describes the FIXED portfolio (composition +
                          attribution), which is why it needs `fixed_portfolio_id` and an unlinked
                          row cannot offer it. stopPropagation so it does not also toggle the row. */}
                      <td className="px-3 py-1.5 whitespace-nowrap">
                        <div className="flex items-stretch gap-1">
                          {canAnalyse(r) && (
                            <button
                              onClick={(e) => { e.stopPropagation(); void openModal(r); }}
                              disabled={opening === r.dynamic_portefeuille}
                              className="inline-flex items-center text-[11px] px-1.5 py-0.5 rounded border border-neutral-800/40 text-fg-subtle hover:bg-overlay/5 hover:text-fg disabled:opacity-50"
                            >
                              {opening === r.dynamic_portefeuille ? '…' : 'Analyse'}
                            </button>
                          )}
                          {/* Re-scan just this portfolio (a few seconds). stopPropagation so it does
                              not also toggle the row's holdings. `items-stretch` on the wrapper keeps
                              this exactly the height of the Analyse button beside it. */}
                          {isAdmin && (() => {
                            const pf = r.dynamic_portefeuille;
                            const busy = refreshingRows.has(pf);
                            const stopping = cancelWanted.has(pf);
                            // ⚠ THE FLIP KEYS ON `busy` — THE PRESS — NOT ON `rowJobs`. See
                            // `cancelWanted`: keyed on the id it waited a round-trip, during which
                            // the control read "Refresh" over work already running and a second
                            // press started a second job and a second toast.
                            const cancellable = busy && !stopping;
                            return (
                              <button
                                onClick={(e) => {
                                  e.stopPropagation();
                                  if (stopping) return;                 // already asked
                                  if (busy) void cancelRefreshRow(pf);
                                  else void refreshOne(pf);
                                }}
                                // ⚠ INERT ONLY ONCE THE CANCEL IS IN. A disabled spinner is the
                                // state this panel kept being reported as "stuck": nothing to
                                // press, nothing moving, no way out — so while it runs there is
                                // always something to press, and afterwards there is nothing left
                                // to ask for.
                                disabled={stopping}
                                title={stopping
                                  ? 'Cancelling — the account being downloaded finishes first.'
                                  : cancellable
                                    ? 'Cancel this re-scan. It stops at the next account boundary; everything already downloaded is kept, and the toast names the books left un-refreshed.'
                                    : "Re-scan this portfolio's AIRS Rendement + Vermogensoverzicht now."}
                                aria-label={stopping ? 'Cancelling this refresh'
                                  : cancellable ? 'Cancel this refresh' : 'Refresh this portfolio'}
                                className={`inline-flex items-center justify-center px-1.5 py-0.5 rounded border transition-colors disabled:opacity-50 disabled:cursor-wait ${
                                  busy
                                    ? 'border-warn-500/40 text-warn-400 hover:bg-warn-500/10'
                                    : 'border-neutral-800/40 text-fg-subtle hover:bg-overlay/5 hover:text-accent-300'}`}
                              >
                                {busy
                                  ? <span className="text-[10px] leading-none px-0.5">✕</span>
                                  : <RefreshIcon spinning={false} size={12} />}
                              </button>
                            );
                          })()}
                        </div>
                      </td>
                      <td className="px-3 py-1.5 text-fg whitespace-nowrap">
                        <span className="text-fg-faint mr-1.5">{isOpen ? '▾' : '▸'}</span>
                        {/* ⚠ AIRS NAMES ONE PORTFOLIO THREE WAYS — our readable name, the Fixed
                            code and the Dynamic code — and printing all three ran them together
                            ("ToppenbergBeheer DefensiefTOPS_DEF_BEH_DYN · TOPS_DEF_BEH"), so the
                            one name a reader wants was the hardest thing on the line to find.
                            The codes are what you search AirSPMS for, which is a deliberate act
                            and can afford a hover — but it has to hang off something hoverable,
                            so it hangs off the name rather than an empty span nobody can reach. */}
                        {/* ⚠ CLICK TO NAME IT — and admin-only, because a nickname is shared. The
                            books still wearing AIRS's own code (`BUS_Ris_bepOff_Kl_AFS_Dy`) are
                            exactly the ones paired with no model, i.e. the ones nothing else can
                            name; before this the only way to give a book a readable name was to
                            rename a model it might not even have. stopPropagation so naming a row
                            does not also expand it. */}
                        {isAdmin ? (
                          <button type="button"
                            onClick={(e) => { e.stopPropagation(); void renameAccount(r); }}
                            title={`AIRS: ${r.dynamic_portefeuille}${r.fixed_name ? ` · ${r.fixed_name}` : ''}\n\nClick to ${r.name_is_custom ? 'change or clear this name' : 'give this book a name'}.`}
                            className="text-left hover:text-accent-300 hover:underline decoration-dotted underline-offset-2">
                            {r.name}
                            {r.name_is_custom && (
                              <span className="text-accent-400 text-[10px] leading-none ml-1 align-middle">✎</span>
                            )}
                          </button>
                        ) : (
                          <span className="cursor-help"
                            title={`AIRS: ${r.dynamic_portefeuille}${r.fixed_name ? ` · ${r.fixed_name}` : ''}`}>
                            {r.name}
                          </span>
                        )}
                        {/* ⚠ MARKED, NOT WITHHELD. These rows used to be hidden from the list
                            entirely, so a scan that reached all 44 portfolios displayed 22 and the
                            operator could not see which report was short, or for whom. The row's
                            figures are still real — they just do not all describe the same date,
                            and the badge names exactly which one is stale. */}
                        {(r.missing_reports?.length ?? 0) > 0 && (
                          <span className="ml-1.5 text-[11px] text-warn-300"
                            title={`This account's last scan did not retrieve: ${r.missing_reports!
                              .map((c) => REPORT_LABELS[c] ?? c).join(', ')}. Its other figures are from the newer scan, so the row mixes dates — ${
                              // Don't send a reader to a button their role does not render.
                              isAdmin ? 'retry with the Refresh button on the left.'
                                : 'the daily scan retries it automatically.'}`}>
                            ⚠ {r.missing_reports!.map((c) => REPORT_LABELS[c] ?? c).join(', ')}
                          </span>
                        )}
                        {/* ⚠ NO BADGE FOR A GUESSED PAIRING. A name match is how nearly every row
                            is paired, so an amber ⚠ on 27 of 28 of them marked the NORMAL case as
                            exceptional — which is how a badge stops being read, and takes the ones
                            that matter with it. The provenance below still states that the pairing
                            is a name match; it is reachable, just not shouted. */}
                        {/* The name is the FIXED side's, reached through a pairing — so its card
                            states the pairing and how it was made. */}
                        <Provenance source="airs_model" kind={r.fixed_name ? 'formula' : 'copied'}
                          what={r.fixed_name
                            ? 'The name of this account, taken from the model portfolio it is paired with.'
                            : 'The name of this account, as AIRS itself calls it.'}
                          note={r.fixed_name ? 'name — from the Fixed portfolio this book is paired with' : 'name — the AIRS book itself; no Fixed portfolio paired'}
                          how={r.fixed_name
                            ? `${r.dynamic_portefeuille} paired with ${r.fixed_name}${
                              r.link_source === 'guess'
                                ? ` by a name match (${trimStop(r.link_reason ?? 'name match')}). Set it explicitly from the Link control if it looks wrong — the risk variants of a strategy hold the same instruments, so no other column would reveal a wrong pairing.`
                                : ' by a link somebody set explicitly'}`
                            : undefined} />
                      </td>
                      <td className="px-3 py-1.5 text-right font-mono text-fg-subtle">
                        {r.isins ?? '—'}
                        {r.isins != null && (
                          <Provenance source="airs_model" kind="formula" what="How many instruments the paired model portfolio names."
                            note="position count"
                            how="a count of the positions in the paired Fixed portfolio" />
                        )}
                      </td>
                      <td className={`px-3 py-1.5 text-right font-mono font-semibold ${tone(r.ytd_pct)}`}>
                        {pct(r.ytd_pct)}
                        <Provenance source="airs_att" asOf={r.as_of} fetchedAt={r.fetched_at} kind="copied"
                          what="This account's return so far this year, as AIRS itself reports it."
                          note="cumulatief_rendement — AIRS's own compounded year, net of deposit/withdrawal timing" />
                      </td>
                      <td className={`px-3 py-1.5 text-right font-mono ${tone(r.latest_month_pct)}`}>
                        {pct(r.latest_month_pct)}
                        <Provenance source="airs_att" asOf={r.as_of} fetchedAt={r.fetched_at} kind="copied"
                          what="What this account returned in the most recent month AIRS has closed."
                          note="rendement — AIRS's return for the most recent month" />
                      </td>
                      <td className="px-3 py-1.5 text-right">
                        {/* ⚠ stopPropagation — the row is the expand toggle, and a delete that also
                            opened the detail would leave a confirm sitting over a panel loading
                            data for a row about to disappear. */}
                        {isAdmin && (
                        <button type="button" disabled={deletingRows.has(r.dynamic_portefeuille)}
                          onClick={(e) => { e.stopPropagation(); void deleteOne(r.dynamic_portefeuille); }}
                          title="Delete this account's scraped rows (returns, holdings, mutations, model weights) so a refresh can be watched rebuilding them."
                          aria-label={`Delete ${r.name}`}
                          className={`${deletingRows.has(r.dynamic_portefeuille)
                            ? 'opacity-100' : 'opacity-0 group-hover:opacity-100 focus:opacity-100'
                            } text-fg-faint hover:text-neg-400 px-1`}>
                          {deletingRows.has(r.dynamic_portefeuille) ? '…' : '🗑'}
                        </button>
                        )}
                      </td>
                    </tr>
                    {isOpen && (
                      <tr>
                        <td colSpan={7} className="px-3 py-3 bg-inset space-y-2">
                          <Holdings d={detail[r.dynamic_portefeuille]} i={isins[r.dynamic_portefeuille]}
                            portefeuille={r.dynamic_portefeuille} onOverride={refreshIsins}
                            canEdit={isAdmin} />
                          {/* ⚠ WHAT THE BOOK DID, beneath what it holds. The positions answer
                              "where is the money now"; only this answers "how did it get there" —
                              a name that appeared mid-year, one sold out entirely, and a weight
                              that drifted purely on price look identical without it.
                              ⚠ ITS OWN COLLAPSED SECTION, AND ITS OWN LAZY FETCH: the first open
                              of an account goes out to AIRS behind the shared headless session
                              and takes seconds, so it must not ride on expanding the row. */}
                          <AccountTransactions portefeuille={r.dynamic_portefeuille} />
                          {/* ⚠ THE TWO PANELS ABOVE ARE HALVES OF ONE YEAR, AND THEY DISAGREE
                              WITH THE ROW'S OWN YTD UNTIL BOTH ARE COUNTED. Measured across 39
                              accounts, the held positions alone miss the book's own figure by
                              more than 1pp on 23 of them; adding what was SOLD closes
                              AITopSelectie to €0.04 on a €387k year. This is where that sum is
                              done — and checked against AIRS rather than asserted. */}
                          <AccountTotalReturn portefeuille={r.dynamic_portefeuille} />
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
      {analyse && (
        // ⚠ KEYED BY PORTFOLIO. The modal deliberately does NOT clear `data` when its request key
        // changes (clearing inside the effect cascades a render), so a surviving instance would
        // paint the PREVIOUS portfolio's composition for the ~4s the next one takes to load —
        // a complete, plausible, wrong answer with no loading state to warn the reader. The key
        // forces a fresh mount, so an unloaded modal can only ever show "Loading composition…".
        // ⚠ THE ROW'S OWN `refreshOne`, PASSED THROUGH — not a second implementation. Admin-gated
        // exactly as the row's button is (the scan writes, and the API gate holds it to admins),
        // and `refreshSeq` bumps when it finishes so the modal re-reads what the scan rebuilt.
        <PortfolioAnalysisModal key={analyse.id ?? analyse.name} id={analyse.id} basket={analyse.basket}
          name={analyse.name}
          onRefresh={isAdmin && analyse.pf ? () => void refreshOne(analyse.pf!) : undefined}
          refreshing={!!analyse.pf && refreshingRows.has(analyse.pf)}
          refreshTitle="Re-scan this portfolio's AIRS Rendement + Vermogensoverzicht now."
          // ⚠ THE ROW'S OWN CANCEL, NOT A SECOND ONE — and passed unconditionally while the modal
          // has a portfolio behind it, so the button flips on the PRESS exactly as the row's does.
          // Gating it on `rowJobs` would reintroduce the round-trip window where the modal offered
          // "Refresh" over work already running (see `cancelWanted`).
          onCancelRefresh={analyse.pf ? () => void cancelRefreshRow(analyse.pf!) : undefined}
          cancelRequested={!!analyse.pf && cancelWanted.has(analyse.pf)}
          cancelTitle="Cancel this re-scan. It stops at the next account boundary; everything already downloaded is kept, and the toast names the books left un-refreshed."
          refreshSeq={refreshSeq}
          onClose={() => setAnalyse(null)} />
      )}
      {showBands && (
        <AllocationBandsModal canEdit={isAdmin} onClose={() => setShowBands(false)} />
      )}
    </section>
  );
}

/**
 * ⚠ `unpriced` IS NOT A PASS — the name matched and NOTHING checked it, which for a fund is
 * exactly where the share-class trap lives (IE00BNDS1P30 vs IE00BNDS1Q47: both "Vanguard ESG
 * Global Corporate Bond UCITS ETF EUR Hedged", Acc and Inc, €4.79 vs €3.99, compounding
 * differently). It must not look like `ok`.
 */
/** The smart asset-class label with its palette dot — shares the allocation bar's colours so the
 *  Class column and the bar read as one system. `—` when the row has no bucket (unresolved).
 *
 *  For an ISIN-bearing row it is EDITABLE: an overlaid `<select>` lets a user pin the Class (or
 *  pick "Auto" to revert to the calculated one). The choice is persisted per ISIN and beats the
 *  calculation forever; an overridden badge wears a ring on its dot. Cash (no ISIN) is read-only. */
type SortKey = 'name' | 'isins' | 'ytd' | 'month';

/** A sortable column heading. The arrow shows the ACTIVE column only — an indicator on every
 *  header tells the reader nothing about which one is in force. */
function SortTh({ label, k, sortKey, sortDir, onSort, align = 'left', title }: {
  label: string; k: SortKey; sortKey: SortKey; sortDir: 'asc' | 'desc';
  onSort: (k: SortKey) => void; align?: 'left' | 'right'; title?: string;
}) {
  const active = sortKey === k;
  return (
    <th className={'px-3 py-1.5 font-medium ' + (align === 'right' ? 'text-right' : 'text-left')}
      title={title}>
      <button type="button" onClick={() => onSort(k)}
        className={'inline-flex items-center gap-1 hover:text-accent-400 transition-colors '
          + (active ? 'text-fg-soft' : '')}>
        {label}
        <span className={'text-[9px] ' + (active ? 'text-accent-400' : 'text-fg-faint/40')}>
          {active ? (sortDir === 'asc' ? '▲' : '▼') : '▾'}
        </span>
      </button>
    </th>
  );
}


function BucketBadge({ bucket, isin, overridden, onOverride }: {
  bucket?: string | null; isin?: string | null; overridden?: boolean | null;
  onOverride?: (isin: string, bucket: string | null) => void | Promise<void>;
}) {
  const [saving, setSaving] = useState(false);
  if (!bucket) return <span className="text-fg-faint">—</span>;

  const dot = (
    <span className="w-2 h-2 rounded-sm inline-block shrink-0"
      style={{ backgroundColor: allocColor(bucket), boxShadow: overridden ? '0 0 0 1.5px var(--color-bg-page), 0 0 0 2.5px currentColor' : undefined }} />
  );
  // Read-only for cash / unresolved rows (no ISIN to pin).
  if (!isin || !onOverride) {
    return (
      <span className="inline-flex items-center gap-1.5 whitespace-nowrap">
        {dot}<span className="text-fg-soft">{bucketLabel(bucket)}</span>
      </span>
    );
  }
  return (
    <span className={`relative inline-flex items-center gap-1.5 whitespace-nowrap rounded px-1 -mx-1 ${saving ? 'opacity-50' : 'hover:bg-overlay/5'}`}
      title={overridden ? 'Class manually set — pick “Auto” to revert to the calculated class.' : 'Auto-classified — click to override the Class.'}>
      {dot}
      <span className="text-fg-soft">{bucketLabel(bucket)}</span>
      {overridden && <span className="text-accent-400 text-[10px] leading-none">✎</span>}
      {/* The picker overlays the whole cell, invisible, so the badge stays the visible affordance. */}
      <select
        aria-label="Set Class"
        value={overridden ? bucket : ''}
        disabled={saving}
        onClick={(e) => e.stopPropagation()}
        onChange={async (e) => {
          const v = e.target.value || null;   // '' = Auto (clear the override)
          setSaving(true);
          try { await onOverride(isin, v); } finally { setSaving(false); }
        }}
        className="absolute inset-0 w-full h-full opacity-0 cursor-pointer"
      >
        <option value="">Auto (calculated)</option>
        {BUCKET_ORDER.map((b) => <option key={b} value={b}>{bucketLabel(b)}</option>)}
      </select>
    </span>
  );
}

/** HOW an ISIN came to be on this row — the name match that proposed it AND the price check that
 *  did or did not confirm it. `verdict` is the field that matters; `name_score` alone is not a pass
 *  (a fund's Acc/Inc share classes have near-identical names and different ISINs). */
/** HOW an ISIN came to be on this row. Two sources only since the fixed↔dynamic pairing was
 *  deleted (2026-07-23): AIRS states it, or a human supplied it for a row AIRS gives none for. */
function isinHow(r: NonNullable<AirsAccountIsins['rows']>[number]): string {
  // ⚠ The price check no longer tests a name match — there is none. It tests OUR price series for
  // the instrument AIRS names, so a mismatch points at our listing, not at the identity.
  if (r.verdict === 'cross_listed') {
    return `read straight off the holding — AIRS's own ISIN-code column. It is priced from ${r.served_by}, which its execution row is linked to on purpose, so the two prices are NOT the same number: €${r.implied_price_eur}/unit implied here against €${r.our_price_eur} (ratio ${r.price_ratio}). For an ADR that gap is the share ratio and the premium, and it confirms nothing either way about the identity.`;
  }
  const checked = r.verdict === 'ok'
    ? `Our own price series agrees: €${r.implied_price_eur}/unit implied here vs €${r.our_price_eur} (ratio ${r.price_ratio}).`
    : r.verdict === 'price_mismatch'
      ? `⚠ Our own price series DISAGREES: €${r.implied_price_eur}/unit implied here vs €${r.our_price_eur} for ${r.our_instrument ?? 'our instrument'} (ratio ${r.price_ratio}). The identity is AIRS's, so this points at OUR listing for it.`
      // ⚠ NOT A SOFTER MISMATCH — a different question, unanswered. Our newest close for this line
      // is from another day, and the refresh already tried to fetch a newer one, so the gap is time
      // rather than identity.
      : r.verdict === 'stale_price'
        ? `Nothing to compare against: our newest close for ${r.our_instrument ?? 'this instrument'} is €${r.our_price_eur} from ${r.our_price_date}, ${r.price_lag_days} days before this valuation, and Yahoo has nothing newer. The prices differ (ratio ${r.price_ratio}) because they are from different days — that says nothing about the listing.`
        : 'We hold no price series for it, so nothing cross-checks our side.';
  if (r.isin_overridden) {
    return `set by hand${r.isin_override_note ? ` — ${trimStop(r.isin_override_note)}` : ''}. AIRS gives this holding no ISIN, so one was supplied. ${checked}`;
  }
  return `read straight off the holding — AIRS's own ISIN-code column. ${checked}`;
}

function IsinCell({ r, onPin }: {
  r: NonNullable<AirsAccountIsins['rows']>[number] | undefined;
  /** Supply/clear this holding's ISIN by hand. Absent = read-only. */
  onPin?: (holdingName: string, current?: string | null) => void | Promise<void>;
}) {
  // ⚠ NOT a dead dash. AIRS gives no ISIN for its cash line, and none for a snapshot taken before
  // `ISIN-code` existed — so a blank is the entry point for supplying one, not a full stop.
  if (!r?.isin) {
    if (!r || !onPin) return <span className="text-fg-faint">—</span>;
    return (
      <button type="button"
        onClick={(e) => { e.stopPropagation(); void onPin(r.holding_name, null); }}
        title="AIRS gives this holding no ISIN. Click to supply one by hand; it is price-checked afterwards and applies to every book holding this instrument."
        className="text-fg-faint hover:text-accent-400 underline decoration-dotted underline-offset-2">
        —
      </button>
    );
  }
  const mismatch = r.verdict === 'price_mismatch';
  const unpriced = r.verdict === 'unpriced';
  // ⚠ NOT RED. The ratio is out of tolerance, but the two prices are from different days and the
  // refresh has already tried to close that gap — so this is "we cannot check it", not "the
  // listing is wrong". Painting it like a mismatch is exactly the false alarm it exists to stop.
  const stale = r.verdict === 'stale_price';
  // ⚠ NOT A WARNING. This ISIN's execution row is deliberately served by another instrument, so
  // the prices differ BY DESIGN (an ADR against the main company's listing — TSMC is 1 ADR = 5
  // ordinary shares). A red ⚠ here trains a reader to ignore the ones that mean something.
  const crossListed = r.verdict === 'cross_listed';
  return (
    <span className="font-mono whitespace-nowrap">
      <span className={mismatch ? 'text-neg-400'
        : unpriced || stale ? 'text-fg-muted' : 'text-fg-soft'}>
        {r.isin}
      </span>
      {/* A hand-supplied identity must never read like one AIRS stated. */}
      {r.isin_overridden && (
        <button type="button" disabled={!onPin}
          onClick={(e) => { e.stopPropagation(); void onPin?.(r.holding_name, r.isin); }}
          title="ISIN set by hand — AIRS gives this holding none. Click to change or clear it."
          className="text-accent-400 text-[10px] leading-none ml-1 align-middle hover:text-accent-300">
          ✎
        </button>
      )}
      {crossListed && (
        <span className="text-fg-muted ml-1" title={`Priced from ${r.served_by} — this execution row is linked to another instrument on purpose, so the two prices are not the same number. This holding implies €${r.implied_price_eur}/unit against €${r.our_price_eur} for ${r.our_instrument ?? 'the linked instrument'} (ratio ${r.price_ratio}); for an ADR that difference is the share ratio and the ADR premium, not an error.`}>↗</span>
      )}
      {mismatch && (
        <span className="text-neg-400 ml-1" title={`⚠ OUR price series disagrees with this instrument. This holding implies €${r.implied_price_eur}/unit; ${r.isin} last closed at €${r.our_price_eur} (${r.our_instrument ?? 'our instrument'}) — a ratio of ${r.price_ratio}. The ISIN is AIRS's own, so this points at OUR listing for it, not at the identity.`}>⚠</span>
      )}
      {stale && (
        <span className="text-fg-faint ml-1" title={`Not checked — our newest close for this instrument is from ${r.our_price_date}, ${r.price_lag_days} days before this valuation, and Yahoo has nothing newer. Two prices from different days cannot confirm or deny a listing.`}>⏱</span>
      )}
      {unpriced && (
        <span className="text-fg-faint ml-1" title="We hold no price series for this instrument, so nothing cross-checks our side of it.">?</span>
      )}
    </span>
  );
}

/**
 * An asset class, and what it returned. AIRS's own `Beleggingscategorie` — not our inference.
 *
 * ⚠ THE RETURN AND THE WEIGHT DO NOT COVER THE SAME HOLDINGS. A holding with no opening value
 *   has an undefined return but real exposure, so it counts in the weight and not in the return.
 *   Cash is exactly this, and so is a short (Nestle India, -3,504 shares). Where they differ the
 *   header says how much the return spans, rather than quietly averaging over a smaller basket.
 *
 * ⚠ ETFs ARE COUNTED, NEVER BUCKETED. An equity ETF is Equity and a bond ETF is Bonds — that is
 *   AIRS's classification and it is the right one: 10 of the 11 bond ISINs are ETFs, so an "ETF"
 *   bucket would empty Bonds and make a defensive book read as holding almost none.
 */
function SegmentHeader({ s, asOf, stats, altReturnPct, basisKey }: {
  s: AirsHoldingSegment; asOf?: string | null;
  /** ⚠ EVERY FIGURE ON THIS ROW COMES FROM THE HOLDINGS UNDER IT (`groupStats`), not from the
   *  backend's own per-segment numbers — it computes those over a different row set, and a header
   *  that disagrees with the lines beneath it is a second source of truth with no way to tell
   *  which is right. `s` is used only for the label. */
  stats: GroupStats;
  /** Return on the chosen weight basis, when that is not the start basis. Null = show the real one. */
  altReturnPct?: number | null;
  /** ⚠ THE SAME BASIS THE <thead> AND THE ROWS BELOW USE. Only one weight column is rendered —
   *  the one the Return is computed on — so a header that gated on a different value than its
   *  rows would put this segment's figure under someone else's column heading. */
  basisKey: WeightBasis;
}) {
  const { etfPct, partial } = stats;
  // The figure this row actually prints: the chosen weight basis, falling back to the row's own
  // start-weighted return when the basis is "start" (where the two are the same by construction).
  const shown = altReturnPct ?? stats.returnPct;
  const label = bucketLabel(s.asset_class) || 'Group';
  return (
    <tr className="bg-overlay/[0.03] border-t border-neutral-800/40">
      <td className="px-3 py-1 font-semibold text-fg-strong">
        {label}
        <span className="text-fg-faint font-normal ml-2">
          {stats.holdings} holding{stats.holdings === 1 ? '' : 's'}
          <Provenance source="airs_volk" asOf={asOf} kind="formula" what={`How many positions this ${label} segment holds.`}
              note="holdings in this segment"
            how="a count of the rows below" />
          {etfPct >= 0.5 && (
            <span title={`${eur(stats.valueEur * (etfPct / 100))} of this segment is held via ETFs. An equity ETF is Equity and a bond ETF is Bonds — holding it through a fund does not change the exposure.`}>
              {' · '}{etfPct.toFixed(0)}% via ETFs
              <Provenance source="airs_volk" asOf={asOf} kind="formula" what={`How much of this ${label} segment is held through funds rather than directly.`}
                note="ETF share of the segment"
                how="the value of the fund rows below, divided by this segment's total value" />
            </span>
          )}
        </span>
      </td>
      {/* ⚠ ONE CELL PER COLUMN. The table is Fund · ISIN · Class · Link · Sector · Region · Ccy
          · Beginwaarde · Huidige waarde · Direct result · Div tax · <one weight> · Return —
          THIRTEEN. (It was fourteen: a leading Fundamental column was removed 2026-08-04, one
          cell from each of the four row shapes — thead, this header, the holdings and Total. Drop
          one and not the others and every figure below shifts a column, silently: a weight
          renders perfectly well under "Ccy".)
          ⚠ EXACTLY ONE WEIGHT COLUMN, and WHICH one is `basisKey`. All four rows — this header,
          the <thead>, the Total row and the holdings — gate on the same value, so a gate added to
          one and forgotten in another does not shift a column here; it puts this segment's figure
          under a heading that belongs to a different weight. */}
      <td />{/* ISIN */}
      <td />{/* Class */}
      <td />{/* Link */}
      <td />{/* Sector */}
      <td />{/* Region */}
      <td />{/* Ccy */}
      <td className="px-3 py-1 text-right font-mono text-fg-muted">
        {eur(stats.startEurAll)}
        <Provenance source="airs_volk" asOf={asOf} kind="formula" what="What this segment was worth when the year opened."
          note="segment opening value"
          how="a sum of the Beginwaarde column of the rows below" />
      </td>
      <td className="px-3 py-1 text-right font-mono text-fg-soft">
        {eur(stats.valueEur)}
        <Provenance source="airs_volk" asOf={asOf} kind="formula" what="What this segment is worth today."
          note="segment value now"
          how="a sum of the Huidige waarde column of the rows below" />
      </td>
      <td className="px-3 py-1 text-right font-mono text-fg-soft">
        {stats.dividendEur == null ? '—' : eur(stats.dividendEur)}
        {stats.dividendEur != null && (
          <Provenance source="airs_volk" asOf={asOf} kind="formula" what="The dividends and coupons this segment received this year."
            note="segment direct result"
            how="a sum of the Direct result column of the rows below" />
        )}
      </td>
      <td className="px-3 py-1 text-right font-mono text-neg-400">
        {stats.dividendTaxEur == null ? '—' : eur(stats.dividendTaxEur)}
        {stats.dividendTaxEur != null && (
          <Provenance source="airs_volk" asOf={asOf} kind="formula" what="The withholding tax deducted from this segment's income."
            note="segment dividend tax"
            how="a sum of the Div tax column of the rows below" />
        )}
      </td>
      {basisKey === 'start' && (
      <td className="px-3 py-1 text-right font-mono text-fg-subtle">
        {stats.startWeightPct == null ? '—' : `${stats.startWeightPct.toFixed(2)}%`}
        {stats.startWeightPct != null && (
          <Provenance source="airs_volk" asOf={asOf} kind="formula" what="How much of the account this segment was at the start of the year."
            note="segment start weight"
            how={`a sum of the Start wt column of the ${stats.holdings} row${stats.holdings === 1 ? '' : 's'} below`} />
        )}
      </td>
      )}
      {basisKey === 'now' && (
      <td className="px-3 py-1 text-right font-mono text-fg-subtle">
        {stats.weightPct == null ? '—' : `${stats.weightPct.toFixed(2)}%`}
        {stats.weightPct != null && (
          <Provenance source="airs_volk" asOf={asOf} kind="formula" what="How much of the account this segment is today."
            note="segment weight, as of today"
            how={`a sum of the Weight column of the ${stats.holdings} row${stats.holdings === 1 ? '' : 's'} below`} />
        )}
      </td>
      )}
      {basisKey === 'model' && (
      <td className="px-3 py-1 text-right font-mono text-fg-subtle">
        {stats.modelPct == null ? '—' : `${stats.modelPct.toFixed(2)}%`}
        {stats.modelPct != null && (
          <Provenance source="airs_model" asOf={asOf} kind="formula" what="How much of this segment the model portfolio says the account should hold."
            note="segment model weight"
            how="a sum of the Model wt column of the rows below" />
        )}
      </td>
      )}
      {basisKey === 'actual' && (
      <td className="px-3 py-1 text-right font-mono text-fg-muted">
        {stats.actualPct == null ? '—' : `${stats.actualPct.toFixed(2)}%`}
        {stats.actualPct != null && (
          <Provenance source="airs_model" asOf={asOf} kind="formula" what="How much of this segment the model report says the account actually holds."
            note="segment actual weight"
            how="a sum of the Werkelijk column of the rows below" />
        )}
      </td>
      )}
      <td className={`px-3 py-1 text-right font-mono font-semibold ${tone(stats.returnPct)}`}
        title={partial
          ? `Start-weighted value change of this segment's priced holdings (${eur(stats.pricedValueEur)} of ${eur(stats.valueEur)}). The rest has no opening value — not held when the year opened — so its return is undefined, not zero.`
          : 'The start-weighted value change — Σ current ÷ Σ start − 1, each holding weighted by its OPENING value. Price return only — no income, not flow-aware.'}>
        {shown == null ? '—' : pct(shown)}
        {partial && shown != null && <span className="text-warn-400 ml-1">*</span>}
        {/* ONE formula, in the two columns the reader can see. `contributionPct` is Σ(Start wt ×
            Return) over the rows below; dividing by the segment's own Start wt turns that
            book-level figure into the segment's own return. */}
        <Provenance source="airs_volk" asOf={asOf} kind="formula" what="What this segment returned this year, from prices alone."
          note="segment return"
          how={stats.returnPct == null || stats.contributionPct == null || !stats.startWeightPct
            ? 'no holding here has an opening value, so this segment has no return to state'
            // ⚠ pp, not %. The contribution is a share OF THE BOOK's return; printing it "+5.46%"
            // beside the segment's own "+6.60%" reads as two rival returns.
            : altReturnPct != null
              ? `Σ (the chosen weight × Return) ÷ Σ those weights, over this segment's rows that carry both. ⚠ A hypothetical: the segment's real return is ${pct(stats.returnPct)}`
              : `Σ (Start wt × Return) of the rows below = ${stats.contributionPct >= 0 ? '+' : ''}${stats.contributionPct.toFixed(2)}pp, ÷ this segment's ${stats.startWeightPct.toFixed(2)}% Start wt${partial ? ', priced rows only' : ''}`} />
      </td>
    </tr>
  );
}

function Holdings({ d, i, portefeuille, onOverride, canEdit }: {
  d?: AirsAccountDetail; i?: AirsAccountIsins;
  portefeuille?: string; onOverride?: (p: string) => void | Promise<void>;
  /** Admin. The three pins on this table (Class, ISIN, Link) are admin-only at the API gate, so a
   *  non-admin sees each one's ANSWER as plain text and no control to change it. */
  canEdit?: boolean;
}) {
  // ⚠ THE 21-ROW TABLE IS BEHIND A SECOND CLICK (2026-08-05, on request). Expanding an account
  // used to land the reader straight in the full position list, which is the DETAIL — the thing
  // you go looking for once you already know which book you are in. Collapsed by default, the
  // expanded row opens on the book's own summary and the rows are one more click away.
  //
  // ⚠ COLLAPSED IS NOT EMPTY. The bar carries the Total row's own three figures (holdings, value
  // now, return), from the identical variables that row renders — not a second aggregation of the
  // same data, which is exactly the drift this file warns about two comments below. A disclosure
  // that says only "Current portfolio" gives the reader nothing to decide on and makes the click
  // mandatory rather than optional.
  //
  // ⚠ RESET ON EVERY EXPAND, and that is free: the parent renders this component inside
  // `{isOpen && …}`, so collapsing the account unmounts it. There is no stale "still open from
  // last time" state to reason about.
  const [showRows, setShowRows] = useState(false);
  // Which column's weights the segment/Total returns are weighted by. "start" is the book’s own
  // return; everything else is a HYPOTHETICAL and the UI says so.
  const [basisKey, setBasisKey] = useState<WeightBasis>('start');
  // Everything the Link dropdowns need, in ONE call for the whole table — per row it would be a
  // request per holding. Null until it lands, which the cell renders as "…" rather than an empty
  // select that looks like "no options".
  const [linkCtx, setLinkCtx] = useState<LinkCtx | null>(null);
  // ⚠ FETCHED WHEN THE TABLE IS OPENED, NOT WHEN THE ACCOUNT IS. It feeds the Link dropdowns and
  // nothing else, so behind a collapsed table it buys a request whose result no one can see —
  // and expanding an account already costs two (holdings + isins). A reader scanning down the
  // list now pays for the rows they actually ask for.
  const linkFetchedFor = useRef<string | null>(null);
  useEffect(() => {
    if (!portefeuille || !showRows || linkFetchedFor.current === portefeuille) return;
    linkFetchedFor.current = portefeuille;
    let live = true;
    void (async () => {
      // The third request an expand fires. Timed alongside the other two so the console accounts
      // for the whole wait rather than two thirds of it.
      const t0 = performance.now();
      try {
        const r = await apiFetch(
          `${API_URL}/api/airs/accounts/${encodeURIComponent(portefeuille)}/linkable`);
        console.warn(`[AIRS expand] ${portefeuille}: linkable in ${Math.round(performance.now() - t0)}ms`);
        if (live && r.ok) setLinkCtx(await r.json());
      } catch {
        // The table is still usable without the dropdown — but let a re-open try again rather
        // than pinning the cells on "…" for the life of the expand.
        linkFetchedFor.current = null;
      }
    })();
    return () => { live = false; };
  }, [portefeuille, showRows]);
  // Persist a manual Class pin (or clear it → Auto), then re-fetch this account so the row
  // re-groups under its new bucket.
  const setBucket = useCallback(async (isin: string, bucket: string | null) => {
    await apiFetch(`${API_URL}/api/airs/asset-bucket-override`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ isin, bucket }),
    });
    if (portefeuille && onOverride) await onOverride(portefeuille);
  }, [portefeuille, onOverride]);
  // Supply this holding's ISIN by hand. The ONLY route when the model has no position for it —
  // no matching can find an ISIN that is not in the data. Keyed by name, so it fixes every book
  // holding the same instrument at once; an empty answer clears the pin.
  const pinIsin = useCallback(async (holdingName: string, current?: string | null) => {
    const v = await dialog.prompt(
      `The Fixed portfolio has no position for “${holdingName}”, so its ISIN has to be supplied by hand. It is still price-checked afterwards, and applies to every portfolio holding this instrument. Leave empty to clear.`,
      { title: 'Set ISIN', defaultValue: current ?? '', placeholder: 'e.g. IE000OEF25S1' });
    if (v == null) return;                       // cancelled — not the same as cleared
    const res = await apiFetch(`${API_URL}/api/airs/holding-isin-override`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ holding_name: holdingName, isin: v.trim() || null }),
    });
    if (!res.ok) {
      const b = (await res.json().catch(() => null)) as { detail?: string } | null;
      await dialog.alert(b?.detail || `Could not save the ISIN (HTTP ${res.status}).`);
      return;
    }
    if (portefeuille && onOverride) await onOverride(portefeuille);
  }, [portefeuille, onOverride]);
  if (!d) return <p className="text-[12px] text-fg-subtle">Loading holdings…</p>;
  if (!d.rows?.length) return <p className="text-[12px] text-fg-subtle">No holdings snapshot stored.</p>;
  const byName = new Map((i?.rows ?? []).map((r) => [r.holding_name, r]));

  // Grouped by the CALCULATED Class (the `bucket`, incl. manual overrides), in the backend's order
  // (Cash and Unclassified last — they are what is left). A holding whose class we do not know
  // still renders: it falls in the trailing ungrouped block rather than vanishing from a table
  // that is supposed to account for the whole book.
  // ⚠ AIRS bills one instrument on SEVERAL lines — BUS_Neutraal lists "6,5% Rabobank Certificaten
  // 14-perp." at 1.64% AND 0.01%. The ISIN/segment side already dedupes (resolve_account_isins), so
  // merge by name here too, summing weight + values, or the same holding shows as two rows. The
  // return % is identical for two lines of one instrument (same price move), so keep the first's.
  const merged = new Map<string, NonNullable<AirsAccountDetail['rows']>[number]>();
  for (const r of d.rows ?? []) {
    const cur = merged.get(r.holding_name);
    if (!cur) { merged.set(r.holding_name, { ...r }); continue; }
    const add = (a?: number | null, b?: number | null) => (a == null && b == null ? a : (a ?? 0) + (b ?? 0));
    cur.weight = add(cur.weight, r.weight);
    cur.quantity = add(cur.quantity, r.quantity);
    cur.current_value_eur = add(cur.current_value_eur, r.current_value_eur);
    cur.start_value_eur = add(cur.start_value_eur, r.start_value_eur);
    cur.ytd_return_eur = add(cur.ytd_return_eur, r.ytd_return_eur);
    cur.fund_result_eur = add(cur.fund_result_eur, r.fund_result_eur);
    cur.fx_result_eur = add(cur.fx_result_eur, r.fx_result_eur);
  }
  const all = [...merged.values()];
  const segs = i?.segments ?? [];
  const classOf = (name: string) => byName.get(name)?.bucket ?? null;
  const ordered: [AirsHoldingSegment | null, typeof all][] = segs.length
    ? segs
      .map((s) => [s, all.filter((r) => classOf(r.holding_name) === s.asset_class)] as
        [AirsHoldingSegment, typeof all])
      .filter(([, g]) => g.length)
    : [[null, all]];
  const grouped = new Set(ordered.flatMap(([, g]) => g.map((r) => r.holding_name)));
  const rest = all.filter((r) => !grouped.has(r.holding_name));
  if (rest.length) ordered.push([null, rest]);
  // ONE aggregation rule, applied twice. A segment row sums the columns of the holdings under it
  // (start-weighted for the return); the TOTAL row does exactly the same over the segment rows.
  //
  // ⚠ THE TOTAL USED TO RE-DERIVE ITSELF FROM THE HOLDINGS. That is a second code path that only
  // HAPPENED to agree with the headers above it — and a figure that agrees by coincidence starts
  // disagreeing the day either side changes. Now it can only be the sum of what is on screen.
  //
  // ⚠ NOT Σ(displayed-weight × return) at either level: the Weight column is today's value share,
  // and weighting by it lets a big winner (up +148%, now 3× its share) dominate — that read
  // +56.11% on a book whose true return was +41.98%. Start-weighting is the honest number and the
  // one that lines up with `cumulatief_rendement`.
  const basis = startBasis(all);
  const { priced: pricedRows, startSum, weightOf: startWeight } = basis;
  const statsFor = (group: typeof all) => groupStats(group, basis, {
    weightOfRow: (r) => r.weight,
    isEtf: (r) => !!byName.get(r.holding_name)?.is_etf,
    dividendOf: (r) => r.dividend_eur,
    dividendTaxOf: (r) => r.dividend_tax_eur,
    modelOf: (r) => r.model_pct,
    actualOf: (r) => r.model_actual_pct,
  });
  // ⚠ EVERY group, including the trailing ungrouped block (which draws no header). Aggregating
  // only the groups that rendered a header would drop those holdings from the book's totals.
  const groupStatsOf = new Map(ordered.map(([seg, g]) => [seg?.asset_class ?? 'rest', statsFor(g)]));
  const total = aggregateGroups([...groupStatsOf.values()]);
  // The chosen basis. "start" reuses the identity above verbatim; the others are renormalised
  // over the rows that carry both a weight and a return (see weightedReturn).
  const weightForBasis = (r: typeof all[number]) => (
    basisKey === 'start' ? startWeight(r)
      : basisKey === 'now' ? r.weight
        : basisKey === 'model' ? r.model_pct
          : r.model_actual_pct);
  const wrOf = (group: typeof all) =>
    weightedReturn(group, weightForBasis, (r) => holdingTotalReturn(r));
  const wrByGroup = new Map([...groupStatsOf.keys()].map((k, idx) =>
    [k, wrOf(ordered[idx][1])] as [string, WeightedReturn]));
  const wrTotal = combineWeighted([...wrByGroup.values()]);
  const isHypothetical = basisKey !== 'start';
  const totalReturn = total.returnPct == null ? null : total.returnPct / 100;
  return (
    <div className="space-y-2">
      {/* ⚠ THE WHOLE BAR IS THE TOGGLE, not a caret you have to hit. The figures on it are the
          Total row's, so a reader who only wanted the summary has already been answered and the
          click is genuinely optional. */}
      <button type="button" onClick={() => setShowRows((v) => !v)}
        aria-expanded={showRows}
        className="w-full flex items-center gap-2 text-left text-[12px] px-2 py-1.5 rounded-lg border border-neutral-800/40 bg-card hover:bg-overlay/5 transition-colors">
        <span className={`text-[9px] text-fg-faint transition-transform ${showRows ? 'rotate-90' : ''}`}>▶</span>
        <span className="font-medium text-fg-strong">Current portfolio</span>
        <span className="text-fg-faint">
          {all.length} holding{all.length === 1 ? '' : 's'}
        </span>
        {/* ⚠ THE SAME VARIABLES THE TOTAL ROW PRINTS, through the same formatters — never a
            second aggregation of `all`. One that merely HAPPENS to agree starts disagreeing the
            day either side changes, which is the trap the comment above `basis` records. */}
        <span className="ml-auto flex items-center gap-3 font-mono">
          <span className="text-fg-soft">{eur(total.valueEur)}</span>
          {/* ⚠ THE BOOK'S REAL RETURN, EVEN WHEN A HYPOTHETICAL BASIS IS ARMED INSIDE. The
              control that marks a basis as hypothetical is itself hidden while this is collapsed,
              so showing the hypothetical here would be the one number on screen with nothing left
              to qualify it. The chip says a different basis is waiting rather than quietly
              printing its answer. */}
          {isHypothetical && (
            <span className="text-warn-500 font-sans text-[11px]"
              title={`Inside, the returns are weighted by ${WEIGHT_BASES.find((x) => x.key === basisKey)!.label} — a hypothetical. The figure here is the book's own start-weighted return.`}>
              ⚠ {WEIGHT_BASES.find((x) => x.key === basisKey)!.label} inside
            </span>
          )}
          <span className={totalReturn == null ? 'text-fg-faint' : tone(totalReturn)}
            title="The book's own start-weighted total return — the identical figure the Total row inside shows on Start wt.">
            {totalReturn == null ? '—' : pct(totalReturn * 100)}
          </span>
        </span>
      </button>
      {showRows && (<>
      {/* Which weights the segment + Total returns use. Four discrete, named options, so this is
          a segmented control rather than a literal slider — a slider would put "Model wt" at an
          unlabelled 3/4 position and make the default indistinguishable from a nudge.
          ⚠ ONLY "Start wt" is the book's own return; the rest are clearly-marked hypotheticals. */}
      <div className="flex items-center gap-2 flex-wrap text-[11px]">
        <span className="text-fg-faint">Weight returns by</span>
        <div className="inline-flex rounded-lg border border-neutral-800/40 overflow-hidden">
          {WEIGHT_BASES.map((b) => (
            <button key={b.key} type="button" onClick={() => setBasisKey(b.key)}
              title={b.note}
              className={`px-2 py-1 transition-colors ${basisKey === b.key
                ? 'bg-accent-600 text-white'
                : 'text-fg-subtle hover:bg-overlay/5'}`}>
              {b.label}
            </button>
          ))}
        </div>
        {isHypothetical && (
          <span className="text-warn-500"
            title="Weighted by a column that is not the year's opening share, so this is what the book WOULD have returned held that way — not what it did. Only Start wt reproduces the real return.">
            ⚠ hypothetical — the real return is on Start wt
          </span>
        )}
      </div>
      <div className="overflow-x-auto rounded-lg border border-neutral-800/40">
        {/* ⚠ `overflow-x-auto`, NOT `overflow-auto`, and no max-height: the holdings grow to their
            content and the PAGE scrolls them. The horizontal container stays — on a narrow
            viewport these columns are still wider than the screen, and a dense table must scroll
            inside its own box so the page never scrolls sideways. The header loses `sticky top-0`
            with the scrollport it was sticking to; leaving the class would read as if it still did
            something.
            ⚠ `w-full`, NOT `w-auto`. `w-auto` sizes to content, which was invisible while four
            weight columns made the table wider than any container — it only ever grew. Showing
            one weight column instead of four can leave it NARROWER, and `w-auto` then parks the
            whole table against the left edge with dead space beside it, which reads as a broken
            layout rather than a shorter table. `w-full` is a FLOOR, not a cap: past the container
            width the table still grows and the box still scrolls. */}
        <table className="w-full text-xs whitespace-nowrap">
          <thead className="bg-card z-20 [&_th]:bg-card">
            <tr className="text-fg-faint text-[11px] uppercase tracking-wide border-b border-neutral-800/40">
              <th className="px-3 py-1.5 font-medium text-left">Fund</th>
              <th className="px-3 py-1.5 font-medium text-left"
                title="AIRS's own ISIN-code where the book carries one (exact), else matched by name to a Fixed portfolio position, else pinned by hand. Always price-checked against that instrument's own close. ⚠ = the price disagrees; ? = no series, so nothing cross-checks it.">
                ISIN
              </th>
              <th className="px-3 py-1.5 font-medium text-left"
                title="Smart asset-class label — Stocks · Stock ETF · Bonds · Alternatives · Cash · Unclassified (genuinely unsure). AIRS's own class first, then the instrument's grid data and name.">
                Class
              </th>
              {/* ⚠ Some holdings are not instruments — they are other model portfolios, wrapped as
                  a Leonteq certificate so they can be held like a security. Those are CH ISINs
                  Yahoo can never price, so they sit here as dead rows (`?`) whose weight leaves
                  the coverage denominator. The link is what lets a reader see through the wrapper
                  to the strategy behind it. Same store as the /portfolios positions table — the
                  link is keyed on the HOLDING, so a decision made on either screen is the same
                  decision and the two cannot disagree. */}
              <th className="px-3 py-1.5 font-medium text-left"
                title="The model portfolio this holding IS, for the few positions that are certificates wrapping another strategy rather than instruments. The badge is the confidence of our automatic guess; pick from the dropdown to overrule it, and the choice applies to this holding everywhere it is held.">
                Link
              </th>
              <th className="px-3 py-1.5 font-medium text-left" title="The instrument's own yfinance sector. A fund is opaque, so it reads “—”.">Sector</th>
              <th className="px-3 py-1.5 font-medium text-left" title="MSCI region from the instrument's yfinance geo. ⚠ For an ETF this describes its listing, not what it holds.">Region</th>
              <th className="px-3 py-1.5 font-medium text-left">Ccy</th>
              <th className="px-3 py-1.5 font-medium text-right"
                title="Beginwaarde lopend jaar EUR — what this holding was worth when the year opened, restated by AIRS to the CURRENT quantity so a purchase does not read as a gain. EUR 0 means it was not held then.">
                Beginwaarde
              </th>
              <th className="px-3 py-1.5 font-medium text-right"
                title="Huidige waarde EUR — what the holding is worth now, at the snapshot date. The Return is (this + net dividend) over Beginwaarde.">
                Huidige waarde
              </th>
              <th className="px-3 py-1.5 font-medium text-right"
                title="Dividend received on this holding this year, GROSS, in EUR — from the AIRS Mutaties journal. A price return cannot see it: the money leaves the position's value and arrives as cash. Blank = no journal line for it (which is not the same as “paid nothing”).">
                Direct result
              </th>
              <th className="px-3 py-1.5 font-medium text-right"
                title="Withholding tax on that dividend, as AIRS books it (NEGATIVE). Kept in its own column because a US name losing 15% and a Dutch one losing nothing is a fact about the holding. Net income is the two added.">
                Div tax
              </th>
              {/* ⚠ ONE WEIGHT COLUMN, THE SELECTED ONE. The other three are not hidden to save
                  space — they are hidden because only this one produced the Return beside them,
                  and four identical-looking columns gave the reader no way to tell which. */}
              {basisKey === 'start' && (
              <th className="px-3 py-1.5 font-medium text-right"
                title="Share of the book at the START of the year (Beginwaarde ÷ total Beginwaarde). This is the weight the Return column belongs to: weighting each return by it reproduces the Total exactly. “—” = no opening value, so the holding was not there when the year began.">
                Start wt
              </th>
              )}
              {basisKey === 'now' && (
              <th className="px-3 py-1.5 font-medium text-right"
                title="AIRS's own Weging — today's share of the book. It answers what you hold NOW; it is NOT the weight behind the Return column, because a holding that rose carries a bigger share today than it held while it was rising.">
                Weight
              </th>
              )}
              {basisKey === 'model' && (
              <th className="px-3 py-1.5 font-medium text-right"
                title="Model percentage — what this book's own strategy says it should hold, from the AIRS Model report. Blank = the model does not name this holding, which is drift, not 0%.">
                Model wt
              </th>
              )}
              {basisKey === 'actual' && (
              <th className="px-3 py-1.5 font-medium text-right"
                title="Werkelijk percentage — what the Model report says the book actually holds. ⚠ A different report from the Weight column beside it (Vermogensoverzicht), computed on its own date, so the two can legitimately differ.">
                Werkelijk
              </th>
              )}
              <th className="px-3 py-1.5 font-medium text-right">Return</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-neutral-800/20">
            {/* TOTAL — all weights summed and the value-weighted price return, at the top. */}
            <tr className="bg-overlay/[0.04] font-semibold border-b border-neutral-800/40">
              <td className="px-3 py-1.5 text-fg-strong" colSpan={7}>
                Total · {all.length} holding{all.length === 1 ? '' : 's'}
                <Provenance source="airs_volk" asOf={d.as_of} kind="formula" what="How many positions this account holds in total."
                  note="holdings in the book"
                  how="a count of the AIRS positions, merged where one instrument is billed on several lines" />
              </td>
              {/* ⚠ Over ALL rows, not just the priced ones — the column is what a reader adds up,
                  and the Return's denominator (`startSum`) deliberately spans fewer rows. */}
              <td className="px-3 py-1.5 text-right font-mono text-fg-muted">
                {eur(total.startEurAll)}
                <Provenance source="airs_volk" asOf={d.as_of} kind="formula"
                  what="What the whole account was worth when the year opened."
                  note="the book's opening value"
                  how={`a sum of the Beginwaarde column of the segment rows. The Return is computed over the ${pricedRows.length} holding${pricedRows.length === 1 ? '' : 's'} with a non-zero one, i.e. ${eur(startSum)}`} />
              </td>
              <td className="px-3 py-1.5 text-right font-mono text-fg-strong">
                {eur(total.valueEur)}
                <Provenance source="airs_volk" asOf={d.as_of} kind="formula"
                  what="What the whole account is worth today."
                  note="the book's value now"
                  how="a sum of the Huidige waarde column of the segment rows" />
              </td>
              {/* ⚠ The book's income is NOT this column's sum. A position sold during the year
                  paid real dividends and has no row left to carry them, so the total states the
                  held part and its card names the difference. */}
              <td className="px-3 py-1.5 text-right font-mono text-fg-strong">
                {total.dividendEur == null ? '—' : eur(total.dividendEur)}
                {total.dividendEur != null && (
                  <Provenance source="airs_volk" asOf={d.as_of} kind="formula"
                    what="The dividends and coupons received this year by the positions still held."
                    note="Direct result of the holdings still held"
                    how={d.dividend_sold_eur
                      ? `a sum of the Direct result column of the segment rows. ⚠ It is NOT the book's income: ${eur(d.dividend_sold_eur)} more was paid by ${d.dividend_sold_funds?.join(', ')}, sold during the year and no longer in the table`
                      : "a sum of the Direct result column of the segment rows. Every fund that paid this year is still held, so it is also the book's dividend income"} />
                )}
              </td>
              <td className="px-3 py-1.5 text-right font-mono text-neg-400">
                {total.dividendTaxEur == null ? '—' : eur(total.dividendTaxEur)}
                {total.dividendTaxEur != null && (
                  <Provenance source="airs_volk" asOf={d.as_of} kind="formula"
                    what="The withholding tax deducted from that income."
                    note="Dividend tax on the holdings still held"
                    how="a sum of the Div tax column of the segment rows" />
                )}
              </td>
              {basisKey === 'start' && (
              <td className="px-3 py-1.5 text-right font-mono text-fg-strong">
                {total.startWeightPct == null ? '—' : `${total.startWeightPct.toFixed(2)}%`}
                <Provenance source="airs_volk" asOf={d.as_of} kind="formula" what="The start-of-year weights added up — 100% by construction."
                  note="total start weight"
                  how="a sum of the Start wt column of the segment rows, which is 100% by construction" />
              </td>
              )}
              {basisKey === 'now' && (
              <td className="px-3 py-1.5 text-right font-mono text-fg-strong">
                {total.weightPct == null ? '—' : `${total.weightPct.toFixed(2)}%`}
                <Provenance source="airs_volk" asOf={d.as_of} kind="formula" what="Today's weights added up across every segment."
                  note="total weight"
                  how="a sum of the Weight column of the segment rows" />
              </td>
              )}
              {basisKey === 'model' && (
              <td className="px-3 py-1.5 text-right font-mono text-fg-strong">
                {total.modelPct == null ? '—' : `${total.modelPct.toFixed(2)}%`}
                {total.modelPct != null && (
                  <Provenance source="airs_model" asOf={d.as_of} kind="formula" note="the model's total"
                    what="The model portfolio's weights added up across every segment."
                    how="a sum of the Model wt column of the segment rows. ⚠ Short of 100% by whatever the model names and this account does not hold" />
                )}
              </td>
              )}
              {basisKey === 'actual' && (
              <td className="px-3 py-1.5 text-right font-mono text-fg-muted">
                {total.actualPct == null ? '—' : `${total.actualPct.toFixed(2)}%`}
                {total.actualPct != null && (
                  <Provenance source="airs_model" asOf={d.as_of} kind="formula" what="The model report's own actual weights, added up."
                    note="the actual total"
                    how="a sum of the Werkelijk column of the segment rows" />
                )}
              </td>
              )}
              <td className={`px-3 py-1.5 text-right font-mono ${totalReturn == null ? 'text-fg-faint' : tone(totalReturn)}`}
                title="Start-weighted value change — Σ current ÷ Σ start − 1 over holdings with an opening value (each position's return weighted by its OPENING value, the same basis each bucket uses). Price return only — not flow-aware, so it is close to but not exactly the book's cumulatief_rendement.">
                {(isHypothetical ? wrTotal.pct : totalReturn == null ? null : totalReturn * 100) == null
                  ? '—' : pct(isHypothetical ? wrTotal.pct! : totalReturn! * 100)}
                {/* Same formula as a segment row, minus the renormalising step: the Start wt
                    column already sums to 100% here, so there is nothing to divide by. */}
                <Provenance source="airs_volk" asOf={d.as_of} kind="formula" what="What the whole account returned this year, from prices alone."
                  note="start-weighted price return"
                  how={totalReturn == null
                    ? 'no holding has an opening value, so the book has no price return to compute'
                    : isHypothetical
                      ? `Σ (${WEIGHT_BASES.find((x) => x.key === basisKey)!.label} × Return) ÷ Σ those weights, over the rows carrying both. ⚠ NOT the book's return — that is on Start wt (${pct(totalReturn! * 100)}). Renormalised, because these weights sum to ${wrTotal.weightSum < 2 ? (100 * wrTotal.weightSum).toFixed(2) : wrTotal.weightSum.toFixed(2)}%, not 100`
                      : `Σ (Start wt × Return) of the segment rows above = ${total.contributionPct! >= 0 ? '+' : ''}${total.contributionPct!.toFixed(2)}pp, ÷ their ${total.startWeightPct!.toFixed(2)}% Start wt. Price + income, not flow-aware`} />
              </td>
            </tr>
            {ordered.map(([seg, group]) => {
              return (
              <Fragment key={seg?.asset_class ?? 'x'}>
                {seg && <SegmentHeader s={seg} asOf={d.as_of}
                  stats={groupStatsOf.get(seg.asset_class ?? 'rest')!}
                  altReturnPct={isHypothetical ? wrByGroup.get(seg.asset_class ?? 'rest')?.pct : null}
                  basisKey={basisKey} />}
                {group.map((r, n) => {
                  const g = byName.get(r.holding_name);
                  return (
              <tr key={`${r.holding_name}-${n}`} className="hover:bg-overlay/[0.02]">
                <td className="px-3 py-1.5 text-fg-soft pl-6">
                  <span className="inline-block max-w-[24ch] truncate align-bottom"
                    title={r.holding_name}>{r.holding_name}</span>
                  <Provenance source="airs_volk" asOf={d.as_of} kind="copied"
                    what="The instrument this row is, named as AIRS names it."
                    note="Fonds — the position's own name in the AIRS book" />
                </td>
                <td className="px-3 py-1.5">
                  <IsinCell r={g} onPin={canEdit ? pinIsin : undefined} />
                  {/* ⚠ The ISIN is the one column NOT read off a source — it is INFERRED (a name
                      match, then a price check), so its card carries both steps. */}
                  {g?.isin && (
                    <Provenance
                      source={g.isin_source === 'book' ? 'airs_volk'
                        : g.isin_overridden ? 'derived' : 'airs_model'}
                      asOf={g.isin_source === 'book' ? d.as_of : undefined}
                      kind={g.isin_source === 'book' ? 'copied' : 'formula'}
                      what={g.isin_overridden
                        ? 'Which security this row is, supplied by hand because AIRS gave none.'
                        : 'Which security this row is, by its international identifier.'}
                      note={g.isin_overridden
                        ? 'ISIN — supplied by hand, then price-checked'
                        : "ISIN-code — the holding's own ISIN in the AIRS book"}
                      how={isinHow(g)} />
                  )}
                </td>
                {/* Provenance sits OUTSIDE the badge: BucketBadge overlays an invisible `<select>`
                    across its whole span, which would swallow the hover. */}
                <td className="px-3 py-1.5">
                  <BucketBadge bucket={g?.bucket} isin={g?.isin}
                    overridden={g?.bucket_overridden}
                    onOverride={canEdit ? setBucket : undefined} />
                  {g?.bucket && (
                    <Provenance source="derived" kind="formula"
                      what={g.bucket_overridden
                        ? 'What kind of asset this is — set by hand for this instrument.'
                        : 'What kind of asset this is: equity, bonds, cash and so on.'}
                      note={g.bucket_overridden ? 'Class — manually pinned' : 'Class — the smart asset-class label'}
                      how={g.bucket_overridden
                        ? 'a manual override pinned to this ISIN, which beats the calculated class for good'
                        : "the instrument's own grid data (asset class, sector, fund wrapper) and its name. AIRS's Beleggingscategorie is no longer used: it came from a paired model portfolio, and the pairing is gone"} />
                  )}
                </td>
                <LinkCell
                  p={{ isin: g?.isin, fonds: r.holding_name,
                       linked_portfolio_id: g?.linked_portfolio_id,
                       link_source: g?.link_source,
                       link_confidence: g?.link_confidence,
                       link_reason: g?.link_reason }}
                  ctx={linkCtx} ownerId={0} readOnly={!canEdit}
                  linkBase={`/api/airs/accounts/${encodeURIComponent(portefeuille ?? '')}`}
                  onSaved={() => { if (portefeuille && onOverride) void onOverride(portefeuille); }} />
                <td className="px-3 py-1.5 text-fg-subtle">
                  {g?.sector || '—'}
                  {g?.sector && (
                    <Provenance source="yfinance" kind="copied"
                      what="The industry the issuer operates in."
                      note="sector — the instrument's own sector in asset_grid, joined by ISIN" />
                  )}
                </td>
                <td className="px-3 py-1.5 text-fg-subtle">
                  {g?.region || '—'}
                  {g?.region && (
                    <Provenance source="yfinance" kind="formula" what="Which part of the world the issuer belongs to."
                      note="region — the MSCI ACWI region"
                      how="derived from the resolved country above. ⚠ For an ETF it describes the fund's own listing, not what the fund holds" />
                  )}
                </td>
                <td className="px-3 py-1.5 font-mono text-fg-muted">
                  {r.currency || '—'}
                  {r.currency && (
                    <Provenance source="airs_volk" asOf={d.as_of} kind="copied"
                      what="The currency this position is administered in."
                      note="Valuta — the currency AIRS books this position in" />
                  )}
                </td>
                {/* ⚠ €0 here IS a value, not a gap: AIRS reports Beginwaarde 0 for a holding that
                    was not there when the year opened, which is why its Return is blank. */}
                <td className="px-3 py-1.5 text-right font-mono text-fg-muted">
                  {eur(r.start_value_eur)}
                  {r.start_value_eur != null && (
                    <Provenance source="airs_volk" asOf={d.as_of} kind="copied"
                      what="What this holding was worth when the year opened."
                      note="Beginwaarde lopend jaar EUR — restated to the current quantity" />
                  )}
                </td>
                <td className="px-3 py-1.5 text-right font-mono text-fg-soft">
                  {eur(r.current_value_eur)}
                  {r.current_value_eur != null && (
                    <Provenance source="airs_volk" asOf={d.as_of} kind="copied"
                      what="What this holding is worth today."
                      note="Huidige waarde EUR — the position's value at the snapshot date" />
                  )}
                </td>
                {/* ⚠ Blank, never €0. "Paid nothing" and "this book's journal has no line for it"
                    are different claims, and a 0.00 in a money column reads as the first. */}
                <td className="px-3 py-1.5 text-right font-mono text-fg-soft">
                  {r.dividend_eur == null ? '—' : eur(r.dividend_eur)}
                  {r.dividend_eur != null && (
                    <Provenance source="airs_volk" asOf={d.as_of} kind="formula"
                      what="The dividends and coupons this holding paid this year, before tax."
                      note="Direct result — dividend received this year, gross"
                      how={`a sum of this holding's ${r.dividend_payments ?? 0} Dividend line${r.dividend_payments === 1 ? '' : 's'} in the AIRS Mutaties journal, in AIRS's own EUR`} />
                  )}
                </td>
                <td className={`px-3 py-1.5 text-right font-mono ${(r.dividend_tax_eur ?? 0) < 0 ? 'text-neg-400' : 'text-fg-muted'}`}>
                  {r.dividend_tax_eur == null ? '—' : eur(r.dividend_tax_eur)}
                  {r.dividend_tax_eur != null && (
                    <Provenance source="airs_volk" asOf={d.as_of} kind="formula"
                      what="The withholding tax deducted from this holding's income."
                      note="Dividendbelasting — withholding tax, as AIRS books it"
                      how="a sum of this holding's Dividendbelasting lines in the AIRS Mutaties journal; negative, so net income is this plus the Direct result" />
                  )}
                </td>
                {/* ⚠ A dash, never 0.00%. No opening value means the holding was NOT THERE when
                    the year began, which is why it has no return either — not that it held none
                    of the book. */}
                {basisKey === 'start' && (
                <td className="px-3 py-1.5 text-right font-mono text-fg-subtle">
                  {(() => { const sw = startWeight(r); return sw == null ? '—' : `${(sw * 100).toFixed(2)}%`; })()}
                  {startWeight(r) != null && (
                    <Provenance source="airs_volk" asOf={d.as_of} kind="formula"
                      what="How much of the account this holding was at the start of the year."
                      note="start weight — the share of the book this holding was at the year's open"
                      how={`Beginwaarde ÷ the book's total Beginwaarde = ${eur(r.start_value_eur)} ÷ ${eur(startSum)}`} />
                  )}
                </td>
                )}
                {basisKey === 'now' && (
                <td className="px-3 py-1.5 text-right font-mono text-fg-subtle">
                  {r.weight != null ? `${(r.weight * 100).toFixed(2)}%` : '—'}
                  {r.weight != null && (
                    <Provenance source="airs_volk" asOf={d.as_of} kind="copied"
                      what="How much of the account this holding is today."
                      note="Weging — AIRS's own position weight, as of today" />
                  )}
                </td>
                )}
                {/* ⚠ Blank, never 0%. A model that does not name this holding is DRIFT — the book
                    bought something the strategy never asked for — and a 0% would read as the
                    strategy deliberately wanting none of it. */}
                {basisKey === 'model' && (
                <td className="px-3 py-1.5 text-right font-mono text-fg-subtle">
                  {r.model_pct == null ? '—' : `${r.model_pct.toFixed(2)}%`}
                  {r.model_pct != null && (
                    <Provenance source="airs_model" asOf={d.as_of} kind="copied"
                      what="How much of the account the model portfolio says this holding should be."
                      note="Model percentage — from this book's own MODEL report, no pairing involved" />
                  )}
                </td>
                )}
                {basisKey === 'actual' && (
                <td className="px-3 py-1.5 text-right font-mono text-fg-muted">
                  {r.model_actual_pct == null ? '—' : `${r.model_actual_pct.toFixed(2)}%`}
                  {r.model_actual_pct != null && (
                    <Provenance source="airs_model" asOf={d.as_of} kind="copied"
                      what="How much of the account the model report says this holding actually is."
                      note="Werkelijk percentage — the same report's view of what is actually held" />
                  )}
                </td>
                )}
                {/* ⚠ A dash, never 0%. No opening value = the return is UNDEFINED, not flat.
                    This is now a TOTAL return: the income sits in the numerator, so a high-yield
                    holding stops reading as a laggard beside one that pays nothing. */}
                {(() => { const tr = holdingTotalReturn(r); return (
                <td className={`px-3 py-1.5 text-right font-mono ${tone(tr)}`}
                  title={tr == null
                    ? 'No opening value — not held when the year opened (or a cash line). Its return is undefined, not zero.'
                    : undefined}>
                  {tr == null ? '—' : pct(tr * 100)}
                  {/* ⚠ EVERY TERM IS A COLUMN ON SCREEN, INCLUDING THE TAX. Printing the netted
                      dividend as one number hides the tax term entirely, and a reader checking the
                      row against the Div tax column cannot find it. Both are rendered with their
                      own signs, so the tax reads negative here exactly as it does there. */}
                  {tr != null && (
                    <Provenance source="airs_volk" asOf={d.as_of} kind="formula" what="What this holding returned this year — its price move plus the income it paid."
                      note="total return: price + income"
                      how={r.dividend_eur == null
                        ? `Now ÷ Start − 1 = ${eur(r.current_value_eur)} ÷ ${eur(r.start_value_eur)} − 1`
                        : `(Now ${eur(r.current_value_eur)} + Direct result ${eur(r.dividend_eur)} + Div tax ${eur(r.dividend_tax_eur)}) ÷ Start ${eur(r.start_value_eur)} − 1`} />
                  )}
                </td>
                ); })()}
              </tr>
                  );
                })}
              </Fragment>
              );
            })}
          </tbody>
        </table>
      </div>
      </>)}
    </div>
  );
}
