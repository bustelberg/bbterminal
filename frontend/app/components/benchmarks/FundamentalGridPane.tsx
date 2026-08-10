'use client';

import { useVirtualizer } from '@tanstack/react-virtual';
import { useEffect, useMemo, useRef, useState } from 'react';

import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { traceError } from '../../../lib/debugTrace';
import { useIsAdmin } from '../../../lib/hooks/useEffectiveRole';
import { startJob } from '../../../lib/stores/jobs';
import type { FundamentalGrid, FundamentalGridRow } from '../../../lib/types/api';
import {
  aggregateRow, capOf, fmtCell, fmtMillions, gridWidths, orderedIds, periodAxes, periodKey,
  periodTitle, valueOf, weightPct,
} from './fundamentalGrid';

/**
 * EVERY CONSTITUENT'S FUNDAMENTALS FOR ONE PERIOD, WITH THE CAP THAT WEIGHTS THEM.
 *
 * The sibling of the coverage pane: that one says which periods we HOLD, this one shows the
 * numbers. Rows are companies, columns are the lines, and the period is a slider — because
 * weighting is CROSS-SECTIONAL. To weight FY2021 you need every constituent's FY2021 cap at once,
 * which is a screen of rows, not a screen of years.
 *
 * ⚠⚠ THE CONTROLS MOVE THE NUMBERS AND NOTHING ELSE. Every column, every row, the row ORDER, the
 * index row and the two notice lines are all present and in the same place at every slider
 * position — so scrubbing reads as one table being re-valued rather than as a new table each time.
 * Four separate things had to be fixed to make that true, and each is marked ⚠ STABLE below:
 *   1. the sort is anchored to a FIXED period, never the selected one;
 *   2. the index row is always rendered, showing dashes when its coverage floor withholds it;
 *   3. the notice lines occupy reserved height whether or not they have anything to say;
 *   4. both cadences are cached, so the year/quarter control never blanks the table to refetch.
 * A table that re-orders under the cursor cannot be read across periods at all — the reader is
 * comparing row 3 to a different company's row 3.
 */
/** The per-row ingest button.
 *
 *  ⚠ IT NO LONGER REQUIRES AN ISIN. It used to disable itself on `!isin`, because the job endpoint
 *  resolved the company that way — which greyed the button out for the 12 S&P constituents whose
 *  `company.isin` is null, Assurant among them, even though every one of them has a ticker, an
 *  exchange and a company row. The job is keyed on `company_id` now, which the grid always has. */
function FetchButton({ busy, title, onClick }: {
  busy: boolean; title: string; onClick: () => void;
}) {
  return (
    <button type="button" onClick={onClick} disabled={busy} title={title}
      className="cursor-pointer text-[10px] px-1.5 py-0.5 rounded border border-neutral-800/40
                 text-fg-subtle hover:bg-overlay/5 hover:text-accent-300 transition-colors
                 disabled:opacity-40 disabled:cursor-not-allowed">
      {busy ? '…' : 'Fetch'}
    </button>
  );
}

/** One cadence's payload. Shared by the initial load and the post-ingest reload so the two cannot
 *  come to disagree about the URL or the error handling. */
async function fetchGrid(label: string, cadence: string): Promise<FundamentalGrid> {
  const r = await apiFetch(`${API_URL}/api/benchmarks/index/${encodeURIComponent(label)}`
    + `/fundamentals/grid?cadence=${cadence}`);
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return (await r.json()) as FundamentalGrid;
}

export default function FundamentalGridPane({ label, refreshKey = 0 }: {
  label: string;
  /** Bump to drop the cached payloads and re-read — the caller does this after an ingest. The
   *  cache exists so scrubbing never refetches, which also means the pane cannot notice on its own
   *  that the data underneath it changed. */
  refreshKey?: number;
}) {
  /**
   * ⚠ STABLE (4) — BOTH CADENCES ARE KEPT, NOT SWAPPED. The quarter control moves between the
   * annual payload and the quarterly one, and clearing `data` to refetch would drop the whole
   * table to "Loading…" and back on every press. They are two views of ONE GuruFocus fetch, so
   * holding both costs nothing but memory and makes the control feel like a slider instead of a
   * navigation.
   */
  const [byCadence, setByCadence] = useState<Record<string, FundamentalGrid>>({});
  const [cadence, setCadence] = useState<'annual' | 'quarterly'>('annual');
  const [err, setErr] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [q, setQ] = useState('');
  const [sortKey, setSortKey] = useState<string>('market_cap');
  const [dir, setDir] = useState<'asc' | 'desc'>('desc');
  /** ONLY what the reader picked — '' until they touch the slider. The year actually in force is
   *  derived below; see the ⚠⚠ there for why this is not simply `year`.
   *  It is held as the YEAR, not as a slider index, because the two cadences do not necessarily
   *  start at the same one and an index would silently jump the reader to a different year when
   *  the period control switches payload. */
  const [pickedYear, setPickedYear] = useState<string>('');
  const [quarter, setQuarter] = useState<number | null>(null);
  /**
   * Rows with an ingest in flight — a SET, not a single id.
   *
   * ⚠ CONCURRENCY IS THE POINT OF MOVING TO JOBS. As a blocking POST this had to disable every
   * other button, because a second press would have opened a second long request with no way to
   * tell them apart. A job is a handle: press five rows and five toasts report separately, each
   * cancellable on its own. Only the pressed row spins.
   */
  const [fetching, setFetching] = useState<ReadonlySet<number>>(new Set());
  /** Guards the Total row's button only. ⚠ The run's progress belongs to the toast — a second
   *  rendering of one job is a second thing to keep in step. */
  const [filling, setFilling] = useState(false);
  const isAdmin = useIsAdmin();

  const data = byCadence[cadence] ?? null;

  // ⚠ THE YEAR IS RESET ONLY ON A LABEL CHANGE, NOT ON A REFRESH. After an ingest the reader is
  // looking at a period they chose; dropping them back to the newest year would lose their place
  // for no reason — the payload changes, the question they were asking does not.
  //
  // ⚠ THE UPDATER RETURNS THE SAME OBJECT WHEN THERE IS NOTHING TO CLEAR, AND THAT IS NOT A
  // MICRO-OPTIMISATION. This effect also runs on MOUNT, and a bare `setByCadence({})` hands React
  // a NEW object every time — which is a real state change, which re-runs the fetch effect below
  // (`byCadence` is in its deps), which aborts the request it had just started and fires a second
  // one. Two 8-second queries per open, one of them thrown away. Returning `m` unchanged lets
  // React bail out of the re-render entirely.
  useEffect(() => {
    setByCadence((m) => (Object.keys(m).length ? {} : m));
    setPickedYear('');
    setQuarter(null);
  }, [label]);
  useEffect(() => { if (refreshKey) setByCadence({}); }, [refreshKey]);

  useEffect(() => {
    if (byCadence[cadence]) return;                 // already held — no refetch, no blank
    let alive = true;
    setErr(null); setLoading(true);
    void (async () => {
      try {
        const g = await fetchGrid(label, cadence);
        if (alive) setByCadence((m) => ({ ...m, [cadence]: g }));
      } catch (e) {
        traceError('benchmarks', 'the fundamentals grid could not be loaded', e);
        if (alive) setErr(e instanceof Error ? e.message : String(e));
      } finally {
        if (alive) setLoading(false);
      }
    })();
    return () => { alive = false; };
  }, [label, cadence, byCadence]);

  /**
   * FETCH ONE COMPANY'S FUNDAMENTALS — every metric in this table, both cadences.
   *
   * ⚠ IT STARTS A JOB AND RETURNS. The outcome is reported by the toast stack
   * (`lib/stores/jobs.ts`, rendered from the root layout), not here — which is what buys three
   * things this button did not have as a plain POST:
   *   * a Cancel that actually stops the work, at the boundary between two GuruFocus feeds;
   *   * several rows fetchable at once, each reporting separately;
   *   * progress that survives navigating away from this page.
   * The old blocking endpoint is untouched and still what scripts use; both run the same
   * `ingest_company`, so "ingest" cannot come to mean two things.
   *
   * ⚠ ONE PRESS COVERS BOTH SLIDER POSITIONS: `fetch_financials` writes the `annuals` AND
   * `quarterly` blocks of a single blob, which is also why the reload refreshes every cadence
   * held rather than only the one on screen.
   *
   * ⚠ BY ISIN, NEVER BY THE ROW'S `company_id`. In the benchmark payloads that field can be an
   * `analysis_id` (the price machinery keys on that name) and the two id spaces are disjoint —
   * measured, analysis_id 1457 is a real asset row and not a company at all. ISIN is what both
   * worlds carry, and a row without one cannot be ingested at all.
   */
  const fetchOne = async (companyId: number, name: string | null) => {
    const who = name || `company ${companyId}`;
    setFetching((s) => new Set(s).add(companyId));
    try {
      // ⚠ `feeds=statements` — ONE call, and it fills every column this grid draws. The endpoint
      // still accepts `all`; that is what the blocking endpoint and `scripts/` use, and it is the
      // seam that lets this button be the cheap one without forking the ingest.
      const { done } = await startJob(
        `${API_URL}/api/benchmarks/company/${companyId}/fundamentals/ingest/job`
        + '?feeds=statements',
        who);
      const job = await done;
      // ⚠ RELOAD ON `done` ONLY. A cancelled run may have written one feed of three and a failed
      // one may have written none — re-reading either is harmless but pointless, and re-reading
      // after a cancel would quietly undo the impression that anything was stopped.
      if (job.status === 'done') await reloadHeld();
    } catch (e) {
      traceError('benchmarks', `could not start the fundamentals ingest for ${who}`, e);
    } finally {
      setFetching((s) => { const n = new Set(s); n.delete(companyId); return n; });
    }
  };

  /**
   * FETCH EVERY CONSTITUENT THAT IS MISSING — the Total row's own button.
   *
   * ⚠ IT LIVES IN THE TOTAL ROW'S FETCH CELL, which is the intersection of "all rows" and "the
   * fetch action" — the same column each company's own button sits in. It used to be a control in
   * the panel header, where it was adjacent to the price/constituent Refresh and easy to read as
   * part of it; they are different vendors with different quotas.
   *
   * ⚠ AND IT COUNTS WHAT IT WILL FETCH, not what the grid is missing. `fillable` comes from the
   * FILL's own `needs`/`eligible` — on the AEX the grid is missing 3 constituents and `fillable`
   * is 0, because all three are LSE listings outside the subscription and no press could ever load
   * them. A button offering to fetch three companies it will then refuse is worse than no button.
   */
  const fillAll = async () => {
    setFilling(true);
    try {
      const { done } = await startJob(
        `${API_URL}/api/benchmarks/index/${encodeURIComponent(label)}/fundamentals/ingest/job`,
        `${label} fundamentals`);
      const job = await done;
      // ⚠ RE-READ AFTER A CANCEL TOO, UNLIKE A SINGLE ROW. A cancelled bulk run has still loaded
      // every company it got through — often hundreds — so leaving the pre-fill figures on screen
      // would hide real work that was really done.
      if (job.status !== 'failed') await reloadHeld();
    } catch (e) {
      traceError('benchmarks', `could not start the fill for ${label}`, e);
    } finally {
      setFilling(false);
    }
  };

  /**
   * Re-read every cadence we hold, IN PLACE.
   *
   * ⚠ IT MUST NOT CLEAR FIRST. `setByCadence({})` would drop the table to its empty state and back
   * — the exact flash that was fixed two passes ago — and it would do so at the one moment the
   * reader is watching a specific row to see whether it filled in. Fetching first and swapping
   * after means the table never leaves the screen.
   */
  const reloadHeld = async () => {
    const held = Object.keys(byCadence);
    if (!held.length) return;
    const fresh = await Promise.all(held.map(async (c) => {
      try {
        return [c, await fetchGrid(label, c)] as const;
      } catch (e) {
        traceError('benchmarks', `could not re-read the ${c} grid after an ingest`, e);
        return null;                  // keep the payload we already have rather than blanking it
      }
    }));
    const ok = fresh.filter(Boolean) as (readonly [string, FundamentalGrid])[];
    if (ok.length) setByCadence((m) => ({ ...m, ...Object.fromEntries(ok) }));
  };

  const axes = useMemo(() => periodAxes(data?.periods ?? []), [data?.periods]);
  const quarterAxes = useMemo(
    () => periodAxes(byCadence.quarterly?.periods ?? []), [byCadence.quarterly?.periods],
  );

  /**
   * ⚠⚠ DERIVED DURING RENDER, NOT SET BY AN EFFECT — AND THAT IS THE FLASH.
   *
   * This used to be `useState('')` plus an effect that landed on the newest year once a payload
   * arrived. An effect runs AFTER the commit, so the first render with data still had `year = ''`,
   * which makes `period = ''`, which matches nothing in `by_period`: React painted a complete
   * table of dashes under a `—` heading, then immediately repainted it with the real figures. Two
   * frames, and the first one looks like a broken table rather than a loading state.
   *
   * The rule it broke is the general one: state that can be computed from props/state is not
   * state. `pickedYear` holds only what the READER chose; everything else is derived here, so
   * there is no window in which the two disagree.
   *
   * The fallback also does the work the effect's dependency list used to: an unset choice, or one
   * that does not exist in this payload, lands on the newest year — which is how the reader's year
   * survives a cadence switch whenever the other basis also has it.
   */
  const year = pickedYear && axes.years.includes(pickedYear)
    ? pickedYear
    : (axes.years[axes.years.length - 1] ?? '');
  const yearIdx = Math.max(0, axes.years.indexOf(year));
  // Which quarters that year has anywhere in the index. Read from the QUARTERLY payload, which may
  // not be loaded while the reader is still on Full year — until it is, no quarter is known to be
  // missing, so none is greyed. Availability changing is a style change, never a layout one.
  const quartersHere = quarterAxes.quartersByYear[year] ?? [];
  const knowsQuarters = Boolean(byCadence.quarterly);
  const period = year ? periodKey(year, cadence === 'quarterly' ? quarter : null) : '';
  const summary = data?.by_period?.[period];
  const columns = useMemo(() => data?.columns ?? [], [data?.columns]);
  // Market cap has its own pinned column (it is the weight, not a line item), so it is not
  // repeated among the measures.
  const measures = useMemo(() => columns.filter((c) => c.key !== 'market_cap'), [columns]);

  /**
   * ⚠ STABLE (5) — ONE ROW LIST ACROSS BOTH CADENCES, NOT ONE PER PAYLOAD.
   *
   * The two payloads do not carry identical constituent sets: a TTM point needs four quarters
   * behind it, so a company with three has annual lines and no quarterly ones (measured on SP500:
   * 264 with annual Free Cash Flow, 263 with the quarterly line). Rendering each payload's own rows
   * would drop a row out of the middle of the table on the way to Q3 and put it back on the way
   * home. The list is the UNION, and a company the current cadence cannot answer for shows a row of
   * dashes — which is what it is: present in the index, nothing to report on this basis.
   */
  const loaded = useMemo(
    () => [byCadence.annual, byCadence.quarterly].filter(Boolean) as FundamentalGrid[],
    [byCadence.annual, byCadence.quarterly],
  );
  /** Name / ticker / currency, from whichever payload saw the company first — identity does not
   *  depend on the basis, so it is read once and never from the period. */
  const identity = useMemo(() => {
    const m = new Map<number, FundamentalGridRow>();
    for (const p of loaded) {
      for (const r of p.rows) if (!m.has(r.company_id)) m.set(r.company_id, r);
    }
    return m;
  }, [loaded]);

  /**
   * ⚠ STABLE (1) — THE SORT IS ANCHORED TO A FIXED PAYLOAD AND A FIXED PERIOD, NEVER THE SELECTED
   * ONE.
   *
   * Sorting on the visible period is the obvious implementation and it makes the table unreadable
   * across periods: every company changes rank as you scrub, so the eye cannot follow one row, and
   * "the third-largest" silently means a different business at each stop. Anchoring on the newest
   * ANNUAL period gives one fixed running order — the index as it stands today — which the year
   * slider AND the quarter buttons both leave alone; they only re-value the cells inside it.
   *
   * Clicking a column still re-sorts, but on that column's value AT THE ANCHOR, so the order is a
   * property of the table rather than of the control positions.
   */
  const orderSource = byCadence.annual ?? byCadence.quarterly ?? null;
  const anchor = orderSource?.periods?.[orderSource.periods.length - 1] ?? '';
  const orderRows = useMemo(() => {
    const m = new Map<number, FundamentalGridRow>();
    for (const r of orderSource?.rows ?? []) m.set(r.company_id, r);
    return m;
  }, [orderSource]);
  /** The CURRENT basis' rows — where every displayed number comes from. */
  const current = useMemo(() => {
    const m = new Map<number, FundamentalGridRow>();
    for (const r of data?.rows ?? []) m.set(r.company_id, r);
    return m;
  }, [data?.rows]);

  const needle = q.trim().toLowerCase();
  const rows = useMemo(
    () => orderedIds(identity, orderRows, { anchor, sortKey, dir, needle })
      .map((id) => ({ id, ident: identity.get(id)!, cur: current.get(id) ?? null })),
    [identity, orderRows, current, needle, anchor, sortKey, dir],
  );

  /**
   * The index row, built from EVERY row the current basis can answer for.
   *
   * ⚠ NOT FROM THE FILTERED LIST. It is labelled with the index's own name, so computing it over
   * whatever the search box has narrowed to would put "SP500" on the total of four companies —
   * true of the rows on screen, false of the label above them. Typing in the filter now leaves it
   * untouched, which is also what makes it usable: you can search for a company and still read its
   * cells against the index's.
   */
  const agg = useMemo(
    () => aggregateRow(data?.rows ?? [], period, columns),
    [data?.rows, period, columns],
  );
  const totalCap = summary?.total_market_cap_eur ?? null;
  const usable = summary?.weights_usable ?? false;

  /**
   * ⚠⚠ ROW VIRTUALIZATION — AND THE REASON IS THE SLIDER, NOT THE SCROLLBAR.
   *
   * ACWI is 1,949 constituents x ~26 columns, so the table was **~50,000 `<td>` elements**, all
   * mounted. Scrolling that is merely heavy; the slider is what made it painful, because moving
   * the period changes EVERY cell's text — so each tick reconciled the entire 50,000-node tree and
   * rebuilt every cell's `title` string. There is no fetch involved (both cadences are held in
   * `byCadence`), which is exactly why this reads as the UI being stuck rather than as loading.
   *
   * With ~40 rows mounted, a tick re-renders about 1,000 cells instead of 50,000.
   *
   * ⚠ PADDING ROWS, NOT ABSOLUTE POSITIONING. TanStack's own table example positions each `<tr>`
   * absolutely with a transform — which cannot work here: this table is `table-fixed` over a
   * `<colgroup>`, and it has two STICKY columns (`#` and Company). Taking the rows out of the
   * table's flow throws away the colgroup widths and the sticky offsets together. A spacer row
   * above and below keeps every row a normal table row; the same pattern `AssetPipelineTable` uses
   * over 16,150 instruments.
   *
   * ⚠ THE TOTAL ROW IS NOT VIRTUALIZED. It is one row, it is the denominator every weight below
   * divides by, and it must render at every scroll position — it sits ahead of the top spacer.
   *
   * ⚠ `measureElement` RATHER THAN A FIXED HEIGHT, because the row height is not a constant here:
   * this app scales its whole UI off `html { font-size }` and steps it down at three breakpoints
   * (17.5 -> 16 -> 15 -> 14px), so a hardcoded estimate would be right on a desktop and drift on a
   * phone. Every row is a single line by construction (`truncate` inside fixed widths), so the
   * measurements converge to one value immediately and cannot oscillate.
   */
  const scrollRef = useRef<HTMLDivElement>(null);
  const rowVirtualizer = useVirtualizer<HTMLDivElement, HTMLTableRowElement>({
    count: rows.length,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => 32,
    // ⚠ GENEROUS ON PURPOSE. The virtualizer measures the scroll container, whose top is the
    // sticky header rather than the first data row, so its idea of the offset runs ahead of the
    // real one by the header + Total row. That error is CONSTANT (it does not grow with scrolling)
    // and a couple of rows wide; the overscan absorbs it rather than leaving a gap at the seam.
    overscan: 16,
  });
  const vItems = rowVirtualizer.getVirtualItems();
  const padTop = vItems.length ? vItems[0].start : 0;
  const padBottom = vItems.length
    ? rowVirtualizer.getTotalSize() - vItems[vItems.length - 1].end
    : 0;

  const click = (k: string) => {
    if (k === sortKey) setDir((d) => (d === 'desc' ? 'asc' : 'desc'));
    else { setSortKey(k); setDir('desc'); }
  };
  const caret = (k: string) => (k === sortKey ? (dir === 'desc' ? ' ↓' : ' ↑') : '');
  const th = 'py-2 font-medium cursor-pointer select-none hover:text-fg';

  /**
   * ⚠⚠ NO EARLY RETURNS. THE PANE HAS ONE SHAPE, LOADED OR NOT.
   *
   * There used to be three: an error box, a one-line "Loading…", and a one-line "nothing
   * ingested". Each is a different height, and the pane sits ABOVE the constituent price table —
   * so opening an index painted a single line of text, then replaced it with ~9rem of controls and
   * a full-height table, shoving everything below it down the page. That is the switch you see.
   *
   * Now the controls and the table box are always rendered at the same size and only their
   * CONTENTS change: the sliders are inert until there are periods to pick, and the box below
   * holds either the message or the table. Nothing on the page moves when the fetch lands.
   */
  const empty = !data || !data.periods.length;
  const emptyMessage = err && !data
    ? err
    : !data
      ? 'Loading the fundamentals grid…'
      : `No fundamentals ingested for ${label} yet — use the “Fill all” button below.`;

  const staleYear = year && year < String(new Date().getFullYear());
  /**
   * ⚠ THIS INDEX CAPS, SO IT GETS NO WEIGHTS AND NO INDEX ROW.
   *
   * `cap / Σcap` is the index's weighting only where the index does not cap. The AEX caps a
   * constituent at 15% and ASML is 37.53% of it uncapped — a total built on that is an ASML
   * tracker wearing the AEX's name. The per-company figures are untouched: each is true on its own
   * and none of them is a weight.
   */
  const capped = data?.weight_cap_pct != null;
  /** Coverage is genuinely the reason the Total row's line aggregates are withheld — as opposed to
   *  the index capping, which withholds them at any coverage. ⚠ Compared against the floor
   *  DIRECTLY rather than reading `weights_usable`, which folds both reasons into one boolean and
   *  cannot tell the notice which sentence to print. */
  const coverageShort = !empty
    && (summary?.cap_covered_pct ?? 0) < (data?.min_coverage_pct ?? 0);
  /**
   * ⚠ NO LONGER A GAP — AND THE BADGE THAT SHOWED IT IS GONE.
   *
   * This used to be "constituents the grid never saw": `_members` dropped anything with no stored
   * market cap, so the AEX listed 22 of 25 and Shell, Unilever and RELX were absent rather than
   * unpriced. The grid now asks for every constituent (`require_market_cap=False`) and they are
   * rows of dashes, which is what they are.
   *
   * What is left of the difference is the share classes the dedupe FOLDS (GOOGL+GOOG are one
   * company — 2 on the S&P), and folding them is correct, not a loss. Flagging that number as
   * missing would have replaced a real warning with a false one, which is worse than no warning.
   */
  // ⚠ DERIVED FROM THE HEADINGS AND THE CAPPED FLAG ONLY — never from the rows. See `gridWidths`:
  // the moment a width depends on the data, the table's shape depends on the slider.
  const { widths, total } = gridWidths(measures.map((c) => c.label), isAdmin);

  return (
    <div className="space-y-3">
      {/* ── Year slider. */}
      <div className="flex items-center gap-4 flex-wrap text-[11px]">
        <label className="flex items-center gap-2">
          <span className="text-fg-faint w-10">Year</span>
          <input type="range" min={0} max={Math.max(0, axes.years.length - 1)} value={yearIdx}
            disabled={empty}
            onChange={(e) => setPickedYear(axes.years[Number(e.target.value)] ?? year)}
            className="w-56 accent-accent-600 disabled:opacity-40" />
          <span className="font-mono text-fg tabular-nums w-10">{year || '—'}</span>
        </label>
        {/* Reserved so the row does not reflow when a fetch starts. ⚠ INGEST PROGRESS IS NOT HERE:
            it belongs to the toast stack, which outlives this page. A second copy on the panel
            would be a second thing to keep in step with the job's real state. */}
        <span className="text-fg-faint w-24">{loading ? 'loading…' : ''}</span>
      </div>

      {/* ── The period control: ONE row, five positions, always present.
          ⚠ `Full year` IS THE ANNUAL CADENCE, not a quarter — which is why this is one control and
          not a cadence toggle beside a quarter picker. Those were two controls whose product
          included combinations that do not exist (there is no "2025" period in the quarterly
          payload), and picking one showed an empty table. Here every position resolves. */}
      <div className="flex items-center gap-2 text-[11px]">
        <span className="text-fg-faint w-10">Period</span>
        <div className="inline-flex rounded-lg border border-neutral-800/40 overflow-hidden">
          {([null, 1, 2, 3, 4] as const).map((qq) => {
            const active = qq === null ? cadence === 'annual' : cadence === 'quarterly' && quarter === qq;
            // ⚠ GREYED, NOT HIDDEN — removing the button would make the control's width jump from
            // year to year and hide WHICH quarters exist. Until the quarterly payload is loaded
            // nothing is known to be missing, so nothing is greyed.
            const missing = (qq !== null && knowsQuarters && !quartersHere.includes(qq)) || empty;
            return (
              <button key={String(qq)} type="button" disabled={missing}
                onClick={() => {
                  if (qq === null) { setCadence('annual'); return; }
                  setQuarter(qq); setCadence('quarterly');
                }}
                title={qq === null
                  ? 'The fiscal year as reported.'
                  : missing
                    ? `No constituent has filed Q${qq} of ${year}`
                    : `Trailing twelve months ending in Q${qq} — a 12-month figure, like the `
                      + 'fiscal year beside it, measured to a different date.'}
                className={`px-2.5 py-1 transition-colors ${active
                  ? 'bg-accent-600 text-white'
                  : missing
                    ? 'text-fg-dim cursor-not-allowed'
                    : 'text-fg-muted hover:bg-overlay/5'}`}>
                {qq === null ? 'Full year' : `Q${qq}`}
              </button>
            );
          })}
        </div>
      </div>

      {/* ── What this cross-section is, and how much of the index it covers. */}
      <div className="flex items-baseline gap-3 flex-wrap text-[11px]">
        <span className="text-sm font-semibold text-fg-strong">
          {period ? periodTitle(period) : '—'}
        </span>
        <span className="text-fg-soft tabular-nums">
          <span className="font-mono text-fg">{summary?.covered ?? 0}</span>
          {' of '}<span className="font-mono text-fg">{summary?.members ?? data?.members ?? 0}</span>
          {' constituents · '}
          <span className="font-mono text-fg">{fmtMillions(totalCap)}</span>{' total cap'}
        </span>
      </div>

      {/* ⚠ STABLE (3) — TWO RESERVED LINES, ALWAYS RENDERED. These used to be conditional blocks,
          so every slider position that crossed the coverage floor or the current year pushed the
          whole table up or down by a line. Fixed height + `truncate` means the text can change
          without the geometry doing so; the full sentence is on the title. */}
      {/* ⚠ THE CAPPED-INDEX BANNER WAS REMOVED ON REQUEST (2026-08-06). It said the Weight column
          is a share of the summed caps rather than the AEX's own weighting — still true, and still
          on that column's header tooltip, just no longer shouted on every view of the index.

          ⚠ SO THIS LINE IS NOW **ONLY** ABOUT COVERAGE, and it has to gate on coverage rather than
          on `usable`. `weights_usable` is false for a capped index at ANY coverage, so falling
          through to the coverage sentence would have printed "caps cover 88% of 25, floor 60%" on
          the AEX — an explanation that contradicts itself, and a reader sent to fix something that
          was never the problem. */}
      <div className="text-[11px] leading-5">
        <p className="h-5 truncate text-warn-300"
          title={coverageShort
            ? `Market caps cover ${summary?.cap_covered_pct ?? 0}% of the ${data?.members ?? 0} `
              + `constituents, under the ${data?.min_coverage_pct}% floor, so the Total row’s `
              + 'per-line aggregates are withheld. The Weight column and the per-company figures '
              + 'are unaffected — both are shares of, and statements about, what is on screen. '
              + 'The “Fill all” button beneath this grid fetches the rest.'
            : undefined}>
          {coverageShort
            ? `⚠ Total-row line aggregates withheld — caps cover `
              + `${summary?.cap_covered_pct ?? 0}% of ${data?.members ?? 0}, `
              + `floor ${data?.min_coverage_pct}%.`
            : ''}
        </p>
        {/* ⚠ SURVIVORSHIP, STATED. These are TODAY's constituents shown at an older period's
            figures — the index did not hold this exact set back then. */}
        <p className="h-5 truncate text-fg-faint" title={staleYear
          ? 'Companies that have since left the index are absent, and recent joiners are shown '
            + 'with their older figures.' : undefined}>
          {data?.membership_as_of === 'today' && staleYear
            ? `Membership is today’s ${label} list, not ${year}’s.` : ''}
        </p>
      </div>

      <input value={q} onChange={(e) => setQ(e.target.value)} disabled={empty}
        placeholder="Filter by name or ticker…"
        className="bg-page border border-neutral-700 rounded-lg px-2 py-1 text-xs w-64
                   focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30
                   disabled:opacity-40" />

      {/* ⚠ THE TABLE SCROLLS INSIDE ITS OWN CONTAINER — twenty columns are far wider than the
          viewport, and without this the page itself scrolls sideways.

          ⚠⚠ `h-`, NOT `max-h-`. With a max-height the box is as tall as its contents: one line
          while loading, full height once the rows land — so everything below the pane jumped down
          the page as the fetch completed. A fixed height means the box is the same size before and
          after, and the arrival of data changes only what is inside it. The cost is some empty
          space under a short index (AEX has 22 rows); the alternative is the page moving under the
          reader every time they open an index. */}
      <div ref={scrollRef} className="overflow-auto h-[65vh] border border-neutral-800/40 rounded-lg">
        {empty ? (
          <p className={`px-3 py-3 text-xs ${err && !data ? 'text-neg-300' : 'text-fg-subtle'}`}>
            {emptyMessage}
          </p>
        ) : (
        <>
        {/* ⚠⚠ `table-fixed` PLUS AN EXPLICIT WIDTH, NOT `w-full`. Auto layout sizes columns from
            their CONTENT, which made the geometry a function of the period: 2018 holds fewer and
            shorter figures than 2025, so columns shrank and the headings wrapped to two lines —
            the header bar visibly rebuilding as the slider moved. Fixed layout takes its widths
            from the colgroup below and ignores the cells entirely, so the only thing a slider can
            change is the text inside them. The width must be DEFINITE: with `width: auto` a fixed
            table falls back to distributing the container's width and the colgroup is ignored. */}
        <table className="text-xs table-fixed" style={{ width: `${total}rem` }}>
          <colgroup>
            {widths.map((w, i) => <col key={i} style={{ width: `${w}rem` }} />)}
          </colgroup>
          <thead className="text-fg-muted border-b border-neutral-800/40 sticky top-0 bg-card z-20">
            {/* ⚠ EVERY HEADING IS `whitespace-nowrap` AS WELL. The colgroup stops the columns from
                resizing; nowrap is what guarantees a heading cannot wrap inside the width it was
                given — belt and braces, because the two failures look identical on screen. */}
            <tr>
              {/* ⚠ THE ROW NUMBER PINS TOO, AND COMPANY'S OFFSET MUST MATCH ITS WIDTH. Both are
                  sticky; leaving Company at `left-0` would slide it over the numbers the moment
                  the table scrolls sideways — 3rem here, `left-[3rem]` there, and the same 3 in
                  `fixedWidthsRem`. */}
              <th className="text-right px-2 py-2 font-medium sticky left-0 bg-card z-30"
                title="Position in the current sort. It renumbers when you re-sort or filter; the
sliders never change it, because the running order is anchored to a fixed period.">
                #
              </th>
              <th className="text-left px-3 py-2 font-medium sticky left-[3rem] bg-card z-30 whitespace-nowrap">
                Company
              </th>
              {/* ⚠ ADMIN ONLY, AND HIDDEN RATHER THAN DISABLED. The ingest spends GuruFocus quota,
                  so the API gate holds it to admins; showing a user a button that 403s is the
                  thing `useIsAdmin` exists to prevent. `gridWidths` is passed the same flag, or
                  every column after these takes its neighbour's width.

                  ⚠⚠ ONE BUTTON, AND IT FETCHES ONLY WHAT THIS TABLE SHOWS. All nineteen columns
                  here come from ONE GuruFocus feed (`fetch_financials`) — market cap included. It
                  was briefly two (`Table` / `All`), the second adding analyst estimates and
                  indicators: two extra calls per company for data this grid cannot render. Those
                  feeds are reachable where they ARE rendered (`/api/earnings/{cid}/refresh` takes
                  a `source`), so nothing was lost by dropping them from here — and a per-row
                  button that fetches what its own table shows is the rule worth keeping. */}
              {isAdmin && (
                <th className="px-2 py-2 font-medium text-left whitespace-nowrap
                               text-[10px] text-fg-faint"
                  title="ONE GuruFocus call. Loads the statements blob — every column in this
table, market cap included, for every year and both the annual and trailing-twelve-month views.">
                  Fetch
                </th>
              )}
              <th className="text-left px-2 py-2 font-medium whitespace-nowrap" title="GuruFocus exchange code.
The other half of the identifier — GuruFocus addresses a stock as EXCHANGE:TICKER, and a bare ticker is
ambiguous across venues. US listings are the exception: GuruFocus addresses those by ticker alone.">Exch</th>
              <th className="text-left px-2 py-2 font-medium whitespace-nowrap" title="GuruFocus ticker — click to
open the company's GuruFocus summary page. The link is built server-side because the symbol is not simply
EXCHANGE:TICKER: US venues drop the prefix, HKSE codes are zero-padded to five digits, and a class share like
BRK/B becomes BRK.B.">Ticker</th>
              <th className="text-left px-2 py-2 font-medium whitespace-nowrap" title="The currency the figures are
reported in. Every value column is converted to EUR; this is what the native tooltip is in.">Ccy</th>
              <th className={`text-right px-2 whitespace-nowrap ${th}`} onClick={() => click('market_cap')}
                title={`Market cap in the selected period. Sorted on ${anchor}, so the running order does not move when the period does.`}>
                Cap (€){caret('market_cap')}
              </th>
              {/* ⚠ UNCONDITIONAL, INCLUDING ON A CAPPED INDEX (2026-08-06, on request) — see
                  `weightPct`. Defined as cap ÷ Σ available caps it is arithmetic over the numbers
                  on screen, so it is shown everywhere; what it is NOT is the index's weighting,
                  and the tooltip has to say so because for the AEX the gap is 37.53% vs 15.00%. */}
              <th className="text-right px-2 py-2 font-medium whitespace-nowrap"
                title={'Share of the summed market caps in this period: this company’s cap ÷ the '
                  + 'Total row.\n\n⚠ NOT the index’s weight. The denominator is only the caps we '
                  + 'hold, so a constituent showing a dash does not dilute anyone — it inflates '
                  + 'everyone else pro rata.'
                  + (capped
                    ? `\n⚠ ${label} caps a constituent at ${data?.weight_cap_pct}% at each review, `
                      + 'and this column does not — ASML reads ~37% here against the real index’s '
                      + '15.00%.'
                    : '')}>
                Weight
              </th>
              {measures.map((c) => (
                <th key={c.key} className={`text-right px-2 whitespace-nowrap ${th}`}
                  onClick={() => click(c.key)} title={c.note ?? undefined}>
                  {c.label}{caret(c.key)}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-neutral-800/20">
            {/* ⚠ STABLE (2) — ALWAYS RENDERED, INCLUDING ON A CAPPED INDEX. It appeared and
                disappeared with the coverage floor at first, so scrubbing across it moved all 264
                rows by one line; then it was suppressed outright for the AEX. Both are gone: the
                row is always here and only its CELLS change.

                ⚠⚠ A SUM OF CAPS IS NOT A WEIGHTING, AND THAT DISTINCTION IS WHY THIS ROW CAN BE
                UNCONDITIONAL WHILE THE WEIGHTS STAY GATED. Adding up the market caps we hold is
                arithmetic over observed numbers — it is true for the AEX exactly as it is for the
                S&P, because Euronext's 15% cap changes what each constituent's SHARE of the index
                is and not what any of them is WORTH. The weight column and the per-line aggregates
                are claims about the index and keep every gate they had.

                ⚠ SO THE ROW MUST SAY WHAT IT SUMMED. `Total · 22/25 caps` is a sum over the
                constituents we could price, not the index's market cap — three missing names make
                it smaller than the real figure, and a bare "Total" would invite it to be read as
                the latter. The count is the whole difference between a partial sum and a wrong
                one. */}
            <tr className="bg-inset font-medium">
              {/* ⚠ THE DENOMINATOR IS NOW THE WHOLE INDEX, which is what makes this count worth
                  reading. It used to be "constituents that have a stored market cap", so the AEX
                  read "22/22 caps" — apparently complete coverage of a 25-name index, with Shell,
                  Unilever and RELX not merely unpriced but uncounted. Every constituent has a row
                  now, so `members` IS the membership and "22/25" is the real shortfall.

                  ⚠ THE TOTAL ROW IS NOT NUMBERED. It is not a constituent, and a "1" here would
                  push every company's rank up by one against the list it is summarising. */}
              <td className="sticky left-0 bg-inset z-10" />
              <td className="px-3 py-2 sticky left-[3rem] bg-inset z-10 text-fg-strong truncate"
                title={`The sum of the market caps we hold for ${periodTitle(period)} — `
                  + `${summary?.with_market_cap ?? 0} of ${label}'s ${summary?.members ?? 0} `
                  + 'constituents. The rest are listed below as dashes: they are in the index, we '
                  + 'simply cannot price them here. Not the index’s market cap — a constituent '
                  + 'missing from the sum makes this a floor, never the total.'}>
                Total · {summary?.with_market_cap ?? 0}/{summary?.members ?? 0} caps
              </td>
              {/* ⚠ THE ONE TOTAL-ROW CELL WITH SOMETHING TO SAY. Every company's own Fetch sits in
                  this column, so the all-companies action belongs in the all-companies row of it.

                  ⚠ ALWAYS RENDERED, INCLUDING AT ZERO (2026-08-06, on request). It was hidden when
                  nothing was missing, which made the control appear and disappear with the data —
                  the same class of thing as the index row that used to vanish. And a press at zero
                  is not wasted: `fillable` is a snapshot taken when the grid loaded, so pressing
                  RE-ASKS the question against the live database. Verified on the AEX, whose fill
                  has no work: the job ran to completion having spent **zero** API calls. The
                  tooltip says so, so the zero state reads as "confirm" rather than as a dead
                  button. */}
              {isAdmin && (
                <td className="px-2 py-2">
                  <button type="button" onClick={() => void fillAll()} disabled={filling}
                    title={(data?.fillable ?? 0) > 0
                      ? `Fetch the statements feed for the ${data?.fillable} constituents missing `
                        + 'it — one API call each, eight at a time. Progress, the running quota '
                        + 'spend and a Cancel appear bottom-right.'
                      : 'Nothing is missing as of this table’s last load. Pressing re-checks '
                        + 'against the database and costs no API calls if that is still true.'}
                    className="cursor-pointer text-[10px] px-1.5 py-0.5 rounded border
                               border-neutral-700 text-fg-subtle hover:text-accent-300
                               hover:border-accent-500/50 transition-colors whitespace-nowrap
                               disabled:opacity-40 disabled:cursor-wait">
                    {filling ? '…' : `All ${data?.fillable ?? 0}`}
                  </button>
                </td>
              )}
              {/* Exch · Ticker · Ccy — the identity columns, which a total has no value for. The
                  count here must track `fixedWidthsRem`: one short and every figure after it
                  shifts a column left, silently, still rendering as a plausible number. */}
              <td /><td /><td />
              <td className="px-2 py-2 text-right font-mono tabular-nums text-fg-strong whitespace-nowrap">
                {fmtMillions(totalCap)}
              </td>
              {/* 100% by construction — this row IS the denominator every weight below divides
                  by. A dash here would suggest the column beneath it does not add up. */}
              <td className="px-2 py-2 text-right font-mono tabular-nums text-fg-muted">
                {totalCap ? '100.0%' : '—'}
              </td>
              {/* ⚠ THE MEASURE CELLS KEEP EVERY GATE — only the Cap sum was un-gated. Adding up
                  caps is arithmetic; a cap-weighted line aggregate is a claim about the index, and
                  it is still withheld below the coverage floor and on a capped index. */}
              {measures.map((c) => {
                const a = usable ? agg[c.key] : undefined;
                return (
                  <td key={c.key}
                    className="px-2 py-2 text-right font-mono tabular-nums whitespace-nowrap"
                    title={a
                      ? `${c.agg === 'weighted_mean' ? 'Cap-weighted mean' : 'Sum'} over `
                        + `${a.contributors} of ${summary?.members ?? 0} constituents`
                      : c.agg === 'none'
                        ? `${c.label} has no index-level total — a share count and a per-share `
                          + 'amount do not sum across companies into anything.'
                        : undefined}>
                    {a ? fmtCell(a.value, c.unit) : '—'}
                    {/* ⚠ A SHORT COLUMN SAYS SO. An aggregate built from a third of the index
                        reads identically to one built from all of it. */}
                    {a && summary && a.contributors < summary.covered && (
                      <span className="ml-1 text-[9px] text-warn-400">{a.contributors}</span>
                    )}
                  </td>
                );
              })}
            </tr>
            {/* ⚠ THE SPACERS CARRY THE HEIGHT OF EVERY ROW NOT MOUNTED, so the scrollbar and the
                scroll position describe the whole index rather than the visible window.
                `colSpan` is `widths.length` — the colgroup's own length, so it cannot fall out of
                step with the column count the way a hardcoded number would. */}
            {padTop > 0 && (
              <tr aria-hidden><td colSpan={widths.length} style={{ height: padTop }} /></tr>
            )}
            {vItems.map((vi) => {
              const { id, ident, cur } = rows[vi.index];
              // ⚠ THE RANK COMES FROM THE VIRTUAL INDEX, NOT FROM THE MAP POSITION. `vItems` is a
              // window into the list, so its own index starts at 0 wherever you have scrolled to —
              // using it would number every screen 1..40 and quietly renumber the index.
              const i = vi.index;
              // ⚠ IDENTITY FROM THE UNION, NUMBERS FROM THE CURRENT BASIS. `cur` is null for a
              // company this cadence cannot answer for — the row stays, every figure is a dash.
              // ⚠ AGAINST `totalCap` UNCONDITIONALLY — no longer gated on `usable`. The gate said
              // "we will not publish a weight over a partial index"; the column's definition now
              // IS the partial one (share of summed available caps), so gating it would withhold
              // the very number it was asked to show. What stays gated is the line aggregates.
              const w = cur ? weightPct(cur, period, totalCap) : null;
              return (
                // `data-index` + the measure ref are what let the virtualizer learn the real row
                // height instead of trusting `estimateSize` — see the ⚠ on the virtualizer.
                <tr key={id} data-index={i} ref={rowVirtualizer.measureElement}
                  className="hover:bg-overlay/[0.02] transition-colors">
                  {/* The position in the list as shown — 1-based, so it reads as a rank rather
                      than an index. Pinned alongside the name; see the header's ⚠. */}
                  <td className="px-2 py-1.5 text-right font-mono tabular-nums text-fg-faint
                                 sticky left-0 bg-card z-10">
                    {i + 1}
                  </td>
                  {/* ⚠ `truncate` NOW THAT THE WIDTH IS FIXED. Under auto layout the column grew
                      to fit "Koninklijke Ahold Delhaize"; under fixed layout a nowrap name simply
                      overflows into the Ticker column beside it. The full name is on the title. */}
                  {/* ⚠ THE BADGE LIVES HERE, IN THE STICKY NAME CELL, ON PURPOSE. This table
                      scrolls sideways through nineteen metric columns, and the moment you are
                      asking "why is this row empty?" you are looking at those columns — a badge
                      in Exch, Ticker or Fetch has scrolled out of view by then. This column pins.
                      It also has to be visible to NON-ADMINS, which rules out the Fetch column
                      (admin-only), and it has to cover "no GuruFocus ticker" too, which rules out
                      hanging it off the Exch cell — a row refused for having no exchange has
                      nothing to badge there. */}
                  <td className="px-3 py-1.5 text-fg sticky left-[3rem] bg-card z-10"
                    title={ident.name ?? undefined}>
                    <span className="flex items-center gap-1.5 min-w-0">
                      <span className="truncate">{ident.name ?? '—'}</span>
                      {ident.unavailable_label && (
                        <span
                          className="shrink-0 text-[9px] leading-none px-1 py-0.5 rounded border
                                     border-warn-500/40 bg-warn-500/10 text-warn-300 font-medium
                                     tracking-wide cursor-help"
                          title={`${ident.unavailable}.\n\nThis row cannot be filled — the dashes `
                            + 'are an answer, not a gap. Being outside the subscription is a fact '
                            + 'about the EXCHANGE, so it applies to every constituent listed '
                            + 'there and no fetch is attempted for any of them.'}>
                          {ident.unavailable_label}
                        </span>
                      )}
                    </span>
                  </td>
                  {isAdmin && (
                    <td className="px-2 py-1.5">
                      {/* ⚠ NO BUTTON ON A ROW THAT CANNOT BE FETCHED. The backend refuses these
                          before spending a call, so pressing it was always free — but it returned
                          a refusal that read like a failure, and offering an action that never
                          works is how a real gap and a permanent answer come to look alike. The
                          badge in the name cell says why. */}
                      {ident.unavailable_label
                        ? <span className="text-fg-faint text-[10px]">—</span>
                        : (
                          <FetchButton
                            busy={fetching.has(id)}
                            title={`ONE GuruFocus call. Loads every column of this table for ${
                              ident.name ?? `company ${id}`} — market cap included — for every year, `
                              + 'annual and trailing-twelve-month.'}
                            onClick={() => void fetchOne(id, ident.name ?? null)} />
                        )}
                    </td>
                  )}
                  <td className="px-2 py-1.5 font-mono text-fg-faint truncate"
                    title={ident.exchange ?? undefined}>
                    {ident.exchange ?? '—'}
                  </td>
                  {/* ⚠ PLAIN TEXT WHEN THERE IS NO URL, NEVER A DEAD LINK. `gf_url` is null when
                      the row has no ticker or no exchange, and an anchor that goes nowhere reads
                      as "GuruFocus has no page for this" rather than "we could not build one".
                      `rel="noreferrer"` because `target="_blank"` without it hands the opened tab
                      a `window.opener` handle back to this app. */}
                  <td className="px-2 py-1.5 font-mono text-fg-subtle truncate">
                    {ident.gf_url && ident.ticker
                      ? (
                        <a href={ident.gf_url} target="_blank" rel="noreferrer"
                          className="text-accent-400 hover:text-accent-300 hover:underline"
                          title={`Open ${ident.ticker} on GuruFocus`}>
                          {ident.ticker}
                        </a>
                      )
                      : ident.ticker ?? '—'}
                  </td>
                  <td className="px-2 py-1.5 font-mono text-fg-faint truncate">
                    {ident.currency ?? '—'}
                  </td>
                  <td className="px-2 py-1.5 text-right font-mono tabular-nums text-fg whitespace-nowrap">
                    {fmtMillions(cur ? capOf(cur, period) : null)}
                  </td>
                  <td className="px-2 py-1.5 text-right font-mono tabular-nums text-fg-muted">
                    {w == null ? '—' : `${w.toFixed(2)}%`}
                  </td>
                  {measures.map((c) => {
                    const v = cur ? valueOf(cur, period, c.key) : null;
                    const native = cur
                      ? (cur.n as Record<string, Record<string, number>>)[period]?.[c.key]
                      : undefined;
                    const rate = cur ? (cur.fx as Record<string, number>)[period] : undefined;
                    // ⚠ THE NATIVE READING RIDES ALONG ONLY WHERE A CONVERSION HAPPENED. A share
                    // count and a percent are not currency and were never converted, so offering
                    // "native" for them would imply a second reading that does not exist.
                    const converted = c.unit === 'millions' || c.unit === 'per_share';
                    return (
                      <td key={c.key}
                        className="px-2 py-1.5 text-right font-mono tabular-nums text-fg-soft whitespace-nowrap"
                        title={v != null && converted && native != null && rate
                          ? `${native.toLocaleString('en-US')} ${ident.currency ?? ''} ÷ ${rate} = `
                            + `${fmtCell(v, c.unit)}`
                          : undefined}>
                        {fmtCell(v, c.unit)}
                      </td>
                    );
                  })}
                </tr>
              );
            })}
            {padBottom > 0 && (
              <tr aria-hidden><td colSpan={widths.length} style={{ height: padBottom }} /></tr>
            )}
          </tbody>
        </table>
        </>
        )}
      </div>
    </div>
  );
}
