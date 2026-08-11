'use client';

import { Fragment, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import Spinner from '../Spinner';
import { API_URL } from '../../../lib/apiUrl';
import { guruFocusUrl } from '../../../lib/gurufocusUrl';
import { runSSE } from '../../../lib/stream';
import CollapsibleCard from '../momentum/CollapsibleCard';
// ⚠ ONE MODAL, SHARED WITH THE 'Current portfolio' TABLE. It used to live in this file;
// a second copy beside the other table would be a second explanation of the same number.
import BreakdownModal, { type BreakdownTarget } from '../momentum/BreakdownModal';
import { computeMarks, pickedSectors } from './dailyHoldingsMarks';
import SectorRankChart from './SectorRankChart';
// ⚠ THE SHARED PALETTE, NOT A NEW ONE. The same mapping colours /backtest's sector timeline and
// the /schedule run-row sector chips, so a sector is one colour everywhere in the app — which is
// the only thing that makes a bare square legible after you have seen it elsewhere.
import { colorForSector } from '../../../lib/sectorColors';
// ⚠ THE CODE IS THE IDENTITY; THE COLOUR IS THE FAST SCAN. 14 sectors is roughly double what
// categorical colour can carry, and the validator FAILS two pairs on the NORMAL-vision floor
// (Services/Energy dE 3.5, Industrials/Technology dE 0.9 deutan) — see `lib/sectorCodes.ts`.
import { inkForBackground, sectorCode } from '../../../lib/sectorCodes';
import type { ScheduledStrategy } from './types';

/** How far back the walk goes. Whole months, anchored to a month start — a
 *  monthly-rebalanced strategy's chain-linked return has to open on a rebalance,
 *  not partway through a holding period.
 *
 *  ⚠ YTD IS DERIVED, NOT A CONSTANT. `month - 1` months back lands on 1 January of the current
 *  year, so the window follows the calendar instead of drifting a month out every January. */
/** ⚠ `months` IS "START OF THE MONTH N MONTHS AGO", SO IT SPANS N+1 CALENDAR MONTHS. On 31 July,
 *  `months: 2` opens 1 MAY — three months of trading days, not two. The label says "from the start
 *  of" rather than "last 2 months" because the honest description is the anchor, not the span, and
 *  the actual first day is printed beside the results either way. */
const WINDOWS = {
  '2m': { months: 2, label: 'From 2 months back' },
  ytd: { months: Math.max(1, new Date().getMonth()), label: `YTD ${new Date().getFullYear()}` },
} as const;
type WindowKey = keyof typeof WINDOWS;

type Holding = {
  company_id: number;
  ticker?: string | null;
  /** Stamped on by the backend purely so the GuruFocus link resolves — without it
   *  the URL builder falls back to a bare ticker, which 404s for any non-US name. */
  exchange?: string | null;
  company_name?: string | null;
  sector?: string | null;
  weight?: number | null;
  entry_price_eur?: number | null;
  exit_price_eur?: number | null;
  forward_return_pct?: number | null;
  score?: number | null;
  /** 1 = the best-scoring sector picked that day. Drives the sector-square order. */
  sector_rank?: number | null;
  /** The 0-100 pillar scores behind `score` — `{price, volume}` (plus `trend` on the
   *  MomentumExtra strategies, which is why this is a map and not two fields). */
  category_scores?: Record<string, number | null> | null;
};
/** One sector's aggregate for a day, over the SAME pool the sector ranking used —
 *  every sector, not only the chosen ones. */
type SectorScore = {
  sector: string;
  rank?: number | null;
  momentum_score?: number | null;
  category_scores?: Record<string, number | null> | null;
  companies?: number | null;
};
type DailyPick = {
  date: string;
  holdings: Holding[];
  sector_scores?: SectorScore[];
  portfolio_return_pct?: number | null;
  next_day_return_pct?: number | null;
  turnover_abs?: number | null;
  turnover_pct?: number | null;
};
type Computed = {
  daily_picks?: DailyPick[];
  /** The days the pipeline actually DECIDED, from `current_picks_day`. */
  daily_picks_history?: DailyPick[];
  read_only?: boolean;
  /** How much of this run came out of `daily_holdings_cache` rather than being computed. */
  cache_stats?: { reused: number; computed: number; stored: number };
};

const pctTone = (v?: number | null) =>
  (v == null ? 'text-fg-faint' : v >= 0 ? 'text-pos-400' : 'text-neg-400');
const fmtPct = (v?: number | null, d = 2) =>
  (v == null ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(d)}%`);
/** A 0-100 pillar score. ⚠ `—` for null, never 0 — a company excluded from a pillar
 *  (no volume history, say) scored nothing; printing 0 says it scored worst. */
const fmtScore = (v?: number | null) => (v == null ? '—' : v.toFixed(1));

/**
 * The floor the table never goes below — roughly 20 rows.
 *
 * ⚠ IT IS ALLOWED TO PUSH THE PAGE PAST THE VIEWPORT, DELIBERATELY. This card sits under three
 * other pipeline sections, so by the time it renders there is often very little viewport left, and
 * fitting into whatever remains produced a 240px window onto a 145-row table — a scroll container
 * so short that finding a date meant scrolling inside a box that was itself barely on screen. A
 * page scrollbar is the cheaper cost: the browser already handles it and the reader already
 * expects it.
 */
const MIN_TABLE_PX = 560;
/** Breathing room under the table so it doesn't butt against the viewport edge. */
const BOTTOM_GUTTER_PX = 24;

/**
 * Max height for a scroll container that should claim the rest of the viewport below itself.
 *
 * ⚠ MEASURED, NOT A `calc(100vh - Xrem)` GUESS. This card sits under three other pipeline
 * sections whose heights change with pipeline state (a running job adds a progress bar, a failed
 * one adds an error block), so any hardcoded offset is right for one state and wrong for the rest.
 *
 * ⚠ IT CLAIMS THE REMAINING VIEWPORT *OR* `MIN_TABLE_PX`, WHICHEVER IS LARGER — so when the card
 * starts near the bottom of the screen the table keeps a usable height and the PAGE scrolls
 * instead. Growing into free space is the nice-to-have; the floor is the requirement.
 *
 * Recomputed on resize and whenever `deps` change — NOT on scroll. Scrolling does move the
 * element's top, so the table could grow as you go down the page; resizing a scroll container
 * mid-scroll fights the user for the scrollbar, and the gain is a few rows.
 */
function useAvailableHeight(deps: unknown[]): [React.RefObject<HTMLDivElement | null>, number] {
  const ref = useRef<HTMLDivElement>(null);
  const [h, setH] = useState(MIN_TABLE_PX * 2);
  useEffect(() => {
    const measure = () => {
      const el = ref.current;
      if (!el) return;
      const top = el.getBoundingClientRect().top;
      setH(Math.max(MIN_TABLE_PX, window.innerHeight - top - BOTTOM_GUTTER_PX));
    };
    measure();
    window.addEventListener('resize', measure);
    return () => window.removeEventListener('resize', measure);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps);
  return [ref, h];
}

/**
 * One sector, as a coloured chip carrying its two-letter code.
 *
 * ⚠ THE CODE IS NOT DECORATION ON TOP OF THE COLOUR — IT IS THE IDENTITY. Two pairs in this
 * palette fail the NORMAL-vision separation floor, so for those the colour is genuinely not
 * readable by anyone; the code is what distinguishes them. It also survives greyscale printing and
 * forced-colors mode, and it means the legend is a convenience rather than a requirement.
 *
 * ⚠ THE INK IS COMPUTED PER CHIP. White text is unreadable on the light half of this palette
 * (Utilities and Materials sit at 1.63 and 1.92 contrast against white) — see `inkForBackground`.
 */
function SectorChip({ sector, rank, showName = false }: {
  sector: string; rank?: number; showName?: boolean;
}) {
  const bg = colorForSector(sector);
  return (
    <span className="inline-flex items-center gap-1.5 shrink-0"
      title={rank ? `#${rank} ${sector}` : sector}>
      <span
        className="inline-flex items-center justify-center rounded-sm font-mono font-semibold
                   shrink-0 tracking-tight"
        style={{
          backgroundColor: bg, color: inkForBackground(bg),
          width: '1.35rem', height: '0.95rem', fontSize: '0.5rem',
        }}
      >
        {sectorCode(sector)}
      </span>
      {showName && <span className="text-fg-muted">{sector}</span>}
    </span>
  );
}

/** Sectors this day actually bought into — so the sector table can mark them. */
const heldSectors = (d: DailyPick) => new Set(d.holdings.map((h) => h.sector).filter(Boolean));

/**
 * Per-sector price / volume / momentum score for one day.
 *
 * ⚠ EVERY SECTOR IN THE POOL, NOT JUST THE ONES PICKED — that is the point of showing it. The
 * sector that ranked one place below the cut is the row that explains the day's selection; a table
 * of only the chosen sectors just restates the holdings above it. The picked ones are marked, so
 * the boundary is visible without hiding what sits on the other side of it.
 */
function SectorScores({ rows, held }: { rows: SectorScore[]; held: Set<string | null | undefined> }) {
  if (!rows.length) {
    // ⚠ Explained, not blank: a day cached before sector scores existed has none, and an empty
    // area under an expanded row otherwise reads as "this day had no sectors".
    return (
      <p className="text-[11px] text-fg-faint">
        No sector scores stored for this day — recalculate it to fill them in.
      </p>
    );
  }
  return (
    <div>
      <p className="text-[11px] uppercase tracking-wide text-fg-faint mb-1">
        Sector scores
        <span className="normal-case tracking-normal text-fg-subtle">
          {' '}· every sector ranked that day; the highlighted ones were bought
        </span>
      </p>
      <table className="w-full text-[12px]">
        <thead>
          <tr className="text-fg-faint text-[11px] uppercase tracking-wide">
            <th className="py-1 pr-2 text-left font-medium">#</th>
            <th className="py-1 px-2 text-left font-medium">Sector</th>
            <th className="py-1 px-2 text-right font-medium">Companies</th>
            <th className="py-1 px-2 text-right font-medium">Price</th>
            <th className="py-1 px-2 text-right font-medium">Volume</th>
            <th className="py-1 pl-2 text-right font-medium"
              title="The average momentum score across the sector's companies — the number the sector ranking is made on.">
              Momentum
            </th>
          </tr>
        </thead>
        <tbody>
          {rows.map((s) => {
            const picked = held.has(s.sector);
            return (
              <tr key={s.sector}
                className={`border-t border-neutral-800/20 ${picked ? 'bg-accent-500/10' : 'opacity-70'}`}>
                <td className="py-1 pr-2 font-mono text-fg-faint tabular-nums">{s.rank ?? '—'}</td>
                <td className={`py-1 px-2 ${picked ? 'text-fg-strong font-medium' : 'text-fg-muted'}`}>
                  {s.sector}
                </td>
                <td className="py-1 px-2 text-right font-mono text-fg-faint">{s.companies ?? '—'}</td>
                <td className="py-1 px-2 text-right font-mono text-fg-muted">
                  {fmtScore(s.category_scores?.price)}
                </td>
                <td className="py-1 px-2 text-right font-mono text-fg-muted">
                  {fmtScore(s.category_scores?.volume)}
                </td>
                <td className="py-1 pl-2 text-right font-mono text-fg">{fmtScore(s.momentum_score)}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

/**
 * "Daily holdings" — what this strategy WOULD have held on each trading day of the last two months.
 *
 * ⚠ IT IS A CALCULATION, NOT A RECORD, AND THE DIFFERENCE IS THE WHOLE VALUE OF IT. The pipeline
 * stores one decision per trading day in `current_picks_day`, made on the data available AT THE
 * TIME. This recomputes the same days on the data we hold NOW. Where the two disagree, the cause is
 * a price that arrived late or was revised — GuruFocus publishes some closes days after the fact and
 * `ingest/prices.py` writes them with their true (earlier) target_date, so `metric_data` is
 * append-only in `recorded_at` but NOT in `target_date`. That is a real finding about the data, and
 * it is only visible if both numbers are on screen.
 *
 * ⚠ SO IT SAVES NOTHING. The backend refuses to persist a retrospective walk: the upsert is keyed
 * `(strategy_hash, target_date)`, so writing a recomputed past would REPLACE the original decision
 * rather than sit beside it, and the original would be gone. The card says so where the reader is
 * looking, not in a tooltip.
 */
export default function DailyHoldingsSection({ strategies }: {
  strategies: ScheduledStrategy[] | null;
}) {
  const usable = useMemo(
    () => (strategies ?? []).filter((s) => s.config && Object.keys(s.config).length > 0),
    [strategies],
  );
  const [strategyId, setStrategyId] = useState<number | null>(null);
  // ⚠ DEFAULTS TO THE CHEAP WINDOW. YTD is ~145 trading days against 2 months' ~42 — on a
  // few-thousand-name universe the first YTD run is minutes, not seconds (the cache makes every
  // later one cheap). Making it the default would turn a quick look into a long wait by surprise.
  const [win, setWin] = useState<WindowKey>('2m');
  const [status, setStatus] = useState<'idle' | 'running' | 'done' | 'error'>('idle');
  const [message, setMessage] = useState<string>('');
  const [pct, setPct] = useState(0);
  const [result, setResult] = useState<Computed | null>(null);
  const [openDay, setOpenDay] = useState<string | null>(null);
  // Which pick the reader asked "why?" about. Null = no modal.
  const [breakdown, setBreakdown] = useState<BreakdownTarget | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  const selected = usable.find((s) => s.id === strategyId) ?? usable[0] ?? null;

  const calculate = useCallback(async (force = false) => {
    if (!selected || status === 'running') return;
    abortRef.current?.abort();
    const ac = new AbortController();
    abortRef.current = ac;
    setStatus('running');
    setResult(null);
    setOpenDay(null);
    setPct(0);
    setMessage('Starting…');
    try {
      await runSSE(
        `${API_URL}/api/momentum/backtest`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          // The strategy's OWN stored config, with only the two fields this view
          // needs overridden. Rebuilding a config here would answer for a
          // different strategy than the one named in the picker.
          body: JSON.stringify({
            ...selected.config,
            mode: 'current_portfolio',
            daily_months_back: WINDOWS[win].months,
            // Ignore every cached day and re-select the whole window from scratch.
            force_recompute: force,
          }),
        },
        (raw) => {
          const evt = raw as { type?: string; pct?: number; message?: string; data?: Computed };
          if (evt.type === 'progress') {
            if (typeof evt.pct === 'number') setPct(evt.pct);
            if (evt.message) setMessage(evt.message);
          } else if (evt.type === 'warning' && evt.message) {
            // Full detail to the console; the card gets one short line.
            console.warn('[daily-holdings]', evt.message);
          } else if (evt.type === 'current_portfolio' && evt.data) {
            setResult(evt.data);
          } else if (evt.type === 'error') {
            throw new Error(evt.message ?? 'Calculation failed');
          }
        },
        ac.signal,
      );
      setStatus('done');
      setPct(100);
    } catch (e) {
      if (ac.signal.aborted) return;
      console.warn('[daily-holdings] failed', e);
      setMessage(e instanceof Error ? e.message : String(e));
      setStatus('error');
    }
    // ⚠ `win` IS A DEPENDENCY. Without it the callback keeps the window it was created with, so
    // switching to YTD and hitting Calculate would quietly recompute the 2-month window and label
    // the result "YTD" — a wrong answer with no error anywhere.
  }, [selected, status, win]);

  const days = useMemo(
    () => [...(result?.daily_picks ?? [])].sort((a, b) => (a.date < b.date ? 1 : -1)),
    [result],
  );
  // Every sector that appears anywhere in the window, ordered by its best rank across the days
  // (so the legend reads roughly top-pick-first rather than alphabetically).
  const sectorLegend = useMemo(() => {
    const best = new Map<string, number>();
    for (const d of days) {
      pickedSectors(d.holdings).forEach((s, i) => {
        const prev = best.get(s);
        if (prev == null || i < prev) best.set(s, i);
      });
    }
    return [...best.entries()].sort((a, b) => a[1] - b[1] || a[0].localeCompare(b[0])).map(([s]) => s);
  }, [days]);
  // Grow the table into whatever viewport is left below it. Re-measured when the row count or
  // the expanded day changes, since both move what sits under the container.
  const [tableRef, tableMaxH] = useAvailableHeight([result, openDay, status]);
  // Entered / sold per day. Extracted to `dailyHoldingsMarks.ts` and unit-tested: the rule that
  // matters is what the WINDOW EDGES do (neither is marked — see that module), and it is exactly
  // the kind of rule a later refactor "simplifies" into an empty-set comparison that turns every
  // row on the oldest day green.
  const marks = useMemo(() => computeMarks(days), [days]);
  // The days the pipeline actually decided, for the side-by-side check.
  const storedByDate = useMemo(() => {
    const m = new Map<string, DailyPick>();
    for (const d of result?.daily_picks_history ?? []) m.set(d.date, d);
    return m;
  }, [result]);

  return (
    <CollapsibleCard
      title="Daily holdings"
      defaultCollapsed
      bodyClassName="px-5 py-4 space-y-3 text-xs"
      rightSlot={
        <>
          {status === 'running' ? (
            <span className="flex items-center gap-1.5 text-accent-300">
              <Spinner className="h-3 w-3 shrink-0" />
              <span className="font-mono">{pct}%</span>
            </span>
          ) : status === 'done' ? (
            <span className="text-fg-faint">{days.length} trading days</span>
          ) : status === 'error' ? (
            <span className="text-neg-300">failed</span>
          ) : (
            <span className="text-fg-faint">{WINDOWS[win].label.toLowerCase()} · on demand</span>
          )}
          <span className="text-fg-faint">read-only</span>
        </>
      }
    >
      {/* The intro paragraph was removed on request (2026-07-31). What it carried still shows:
          the `read-only` chip in the header, the per-row `vs stored` column (which is where a
          recalculation actually visibly differs from the pipeline's own decision), and the cache
          line below. */}

      {/* ⚠ SAID OUT LOUD, because "42 days" reads the same whether it cost four minutes or four
          seconds — and because a reader who cannot see that a day was REUSED cannot tell a stale
          answer from a fresh one. The most recent days are always recomputed (a late close still
          moves them), so a full-reuse run is not possible and the counts show it. */}
      {result?.cache_stats && (
        <p className="text-[12px] text-fg-faint">
          {result.cache_stats.reused > 0
            ? <>Reused <span className="font-mono text-fg-soft">{result.cache_stats.reused}</span> previously
              calculated day{result.cache_stats.reused === 1 ? '' : 's'} · computed{' '}
              <span className="font-mono text-fg-soft">{result.cache_stats.computed}</span></>
            : <>Computed all <span className="font-mono text-fg-soft">{result.cache_stats.computed}</span> days
              — nothing was cached for this strategy and window yet</>}
          {result.cache_stats.stored > 0 && <> · stored {result.cache_stats.stored} for next time</>}
          {/* ⚠ THE ACTUAL FIRST AND LAST DAY, not the window's name. "2 months back" opens on the
              1st of the month two months ago — three months of days — and only the dates say so. */}
          {days.length > 0 && (
            <> · <span className="font-mono text-fg-soft">
              {days[days.length - 1].date} → {days[0].date}
            </span></>
          )}
          <span title="A day's selection depends on the closes known before it, and some closes arrive days late. The newest days are always recomputed so a late price can still correct them.">
            {' '}· the newest days are always recomputed
          </span>
        </p>
      )}

      <div className="flex items-center gap-2 flex-wrap">
        <label className="flex items-center gap-1.5 text-fg-muted">
          Strategy
          <select
            value={selected?.id ?? ''}
            onChange={(e) => { setStrategyId(Number(e.target.value)); setResult(null); setStatus('idle'); }}
            disabled={status === 'running' || usable.length === 0}
            className="bg-page border border-neutral-700 rounded-lg px-2 py-1 text-[12px] text-fg
                       focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 max-w-[18rem]"
          >
            {usable.length === 0 && <option value="">No scheduled strategies</option>}
            {usable.map((s) => (
              <option key={s.id} value={s.id}>{s.name}{s.enabled ? '' : ' (disabled)'}</option>
            ))}
          </select>
        </label>
        <span className="flex items-center gap-1 text-[12px]">
          {(Object.keys(WINDOWS) as WindowKey[]).map((k) => (
            <button key={k} type="button"
              onClick={(e) => { e.stopPropagation(); setWin(k); setResult(null); setStatus('idle'); }}
              disabled={status === 'running'}
              title={k === 'ytd'
                ? 'Every trading day since 1 January. Roughly 3x the work of the 2-month window on the first run; cached days make later runs cheap.'
                : undefined}
              className={`px-2 py-1 rounded-lg border transition-colors disabled:opacity-40 ${
                win === k
                  ? 'bg-accent-500/15 text-accent-200 border-accent-500/40'
                  : 'text-fg-muted border-neutral-700 hover:border-accent-500/50'}`}>
              {WINDOWS[k].label}
            </button>
          ))}
        </span>
        <button
          type="button"
          onClick={(e) => { e.stopPropagation(); void calculate(false); }}
          disabled={!selected || status === 'running'}
          title={`Re-select this strategy's holdings for every trading day of ${WINDOWS[win].label.toLowerCase()}. Days already calculated are reused; only new ones are computed.`}
          className="text-xs px-2.5 py-1 rounded-lg bg-accent-600 hover:bg-accent-500 text-white
                     disabled:opacity-40 disabled:cursor-not-allowed transition-colors flex items-center gap-1.5"
        >
          {status === 'running' && <Spinner className="h-3 w-3" />}
          {status === 'running' ? 'Calculating…' : 'Calculate'}
        </button>
        <button
          type="button"
          onClick={(e) => { e.stopPropagation(); void calculate(true); }}
          disabled={!selected || status === 'running'}
          title="Ignore every previously calculated day and re-select the whole window from scratch. Slow — use it when prices for the window have been refreshed."
          className="text-[12px] px-2 py-1 rounded-lg border border-neutral-700 text-fg-muted
                     hover:text-accent-300 hover:border-accent-500/50 disabled:opacity-40
                     disabled:cursor-not-allowed transition-colors"
        >
          Recalculate all
        </button>
        {status === 'running' && <span className="text-fg-subtle truncate">{message}</span>}
        {status === 'error' && <span className="text-neg-300">✗ {message}</span>}
      </div>

      {status === 'running' && (
        <div className="h-1.5 rounded-full bg-inset overflow-hidden">
          <div className="h-full bg-accent-500 transition-all" style={{ width: `${pct}%` }} />
        </div>
      )}

      {status === 'done' && days.length === 0 && (
        <div className="text-fg-subtle">
          No trading days came back — the universe had no company with enough price history over
          this window.
        </div>
      )}

      {/* ⚠ THE LEGEND IS NOT DECORATION — the squares are unreadable without it. It lists every
          sector that appears anywhere in the window, in the order it was most often ranked, so a
          colour seen on a row can be named without hovering each one. */}
      {sectorLegend.length > 0 && (
        <div className="flex items-center gap-x-3 gap-y-1 flex-wrap text-[11px]">
          <span className="text-fg-faint uppercase tracking-wide">Sectors</span>
          {sectorLegend.map((s) => <SectorChip key={s} sector={s} showName />)}
        </div>
      )}

      {/* Rank-over-time per sector, from the same `sector_scores` the expanded rows show. */}
      {days.length > 0 && (
        <SectorRankChart days={days} windowLabel={WINDOWS[win].label}
          topN={typeof selected?.config?.top_n_sectors === 'number'
            ? selected.config.top_n_sectors : null} />
      )}

      {days.length > 0 && (
        <div className="flex items-center gap-3 flex-wrap text-[11px] text-fg-faint">
          <span className="flex items-center gap-1.5">
            <span className="inline-block w-3 h-2 rounded-sm bg-pos-500/25" />entered that day
          </span>
          <span className="flex items-center gap-1.5">
            <span className="inline-block w-3 h-2 rounded-sm bg-neg-500/25" />gone the next day
          </span>
          <span className="flex items-center gap-1.5">
            <span className="inline-block w-3 h-2 rounded-sm bg-warn-500/25" />held for one day only
          </span>
          {/* ⚠ Stated, not silently absent: the two edge days genuinely cannot be marked. */}
          <span title="The oldest day has no previous day to compare against, and the newest has no next day. Marking them would report the window's edges as portfolio activity.">
            · the oldest and newest days carry no marks
          </span>
        </div>
      )}

      {days.length > 0 && (
        <div ref={tableRef} style={{ maxHeight: tableMaxH }}
          className="overflow-auto rounded-lg border border-neutral-800/40">
          <table className="w-full text-xs">
            <thead className="sticky top-0 bg-card z-10">
              <tr className="text-fg-faint text-[11px] uppercase tracking-wide border-b border-neutral-800/40">
                <th className="px-3 py-1.5 text-left font-medium">Date</th>
                <th className="px-3 py-1.5 text-left font-medium"
                  title="The sectors picked that day, best-ranked first. One colour per sector, shared with the /backtest sector timeline.">
                  Sectors
                </th>
                <th className="px-3 py-1.5 text-right font-medium">Holdings</th>
                <th className="px-3 py-1.5 text-right font-medium" title="Names that changed versus the previous trading day.">Turnover</th>
                <th className="px-3 py-1.5 text-right font-medium" title="One trading day forward, equal-weighted across that day's picks.">Next day</th>
                <th className="px-3 py-1.5 text-right font-medium" title="Chain-linked from the first day of the window.">Cumulative</th>
                <th className="px-3 py-1.5 text-left font-medium" title="Whether the pipeline stored a decision for this day, and whether it matches what this recalculation produced.">vs stored</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-neutral-800/20">
              {days.map((d) => {
                const stored = storedByDate.get(d.date);
                const open = openDay === d.date;
                // ⚠ COMPARED BY THE NAMES HELD, NOT BY THE RETURN. Two baskets can post the
                // same return and be different portfolios; the holdings are the decision.
                const ids = (p?: DailyPick) => (p?.holdings ?? []).map((h) => h.company_id).sort().join(',');
                const same = stored ? ids(stored) === ids(d) : null;
                return (
                  <Fragment key={d.date}>
                    <tr
                      className="hover:bg-overlay/[0.03] cursor-pointer"
                      onClick={() => setOpenDay(open ? null : d.date)}
                    >
                      <td className="px-3 py-1.5 font-mono text-fg-soft">
                        <span className="text-fg-faint mr-1.5">{open ? '▾' : '▸'}</span>{d.date}
                      </td>
                      {/* One square per picked sector, best-ranked first. Colour alone is never
                          the only carrier: each square names its sector on hover, and the legend
                          above the table spells out every colour in the window. */}
                      <td className="px-3 py-1.5">
                        <span className="flex items-center gap-1">
                          {pickedSectors(d.holdings).map((s, i) => (
                            <SectorChip key={s} sector={s} rank={i + 1} />
                          ))}
                        </span>
                      </td>
                      <td className="px-3 py-1.5 text-right font-mono text-fg">{d.holdings.length}</td>
                      <td className="px-3 py-1.5 text-right font-mono text-fg-muted">
                        {d.turnover_abs ? `${d.turnover_abs}` : '—'}
                      </td>
                      <td className={`px-3 py-1.5 text-right font-mono ${pctTone(d.next_day_return_pct)}`}>
                        {fmtPct(d.next_day_return_pct)}
                      </td>
                      <td className={`px-3 py-1.5 text-right font-mono ${pctTone(d.portfolio_return_pct)}`}>
                        {fmtPct(d.portfolio_return_pct)}
                      </td>
                      {/* Three states that must not look alike: no stored decision at all
                          (this day was never computed live), stored and identical, and stored
                          but DIFFERENT — the last is the interesting one. */}
                      <td className="px-3 py-1.5">
                        {same == null ? (
                          <span className="text-fg-faint" title="The pipeline stored no picks for this day.">not stored</span>
                        ) : same ? (
                          <span className="text-pos-400" title="The recalculation picked exactly the same companies the pipeline stored.">match</span>
                        ) : (
                          <span className="text-warn-300" title="The recalculation picked different companies than the pipeline stored for this day — usually a close that arrived or was revised after the decision was made.">differs</span>
                        )}
                      </td>
                    </tr>
                    {open && (
                      <tr>
                        <td colSpan={7} className="px-3 py-2 bg-inset/50 space-y-2">
                          <SectorScores rows={d.sector_scores ?? []} held={heldSectors(d)} />
                          <table className="w-full text-[12px]">
                            <thead>
                              <tr className="text-fg-faint text-[11px] uppercase tracking-wide">
                                <th className="py-1 pr-2 text-left font-medium">Ticker</th>
                                <th className="py-1 px-2 text-left font-medium">Company</th>
                                <th className="py-1 px-2 text-left font-medium">Sector</th>
                                <th className="py-1 px-2 text-right font-medium">Weight</th>
                                {/* The pillars BEFORE the combined score, so the number on the
                                    right reads as what it is — a weighted blend of the two on
                                    its left. */}
                                <th className="py-1 px-2 text-right font-medium"
                                  title="0-100 price-momentum pillar for this company on this day.">Price</th>
                                <th className="py-1 px-2 text-right font-medium"
                                  title="0-100 volume pillar for this company on this day.">Volume</th>
                                <th className="py-1 px-2 text-right font-medium"
                                  title="The combined momentum score — the pillars blended by the strategy's category weights.">Score</th>
                                <th className="py-1 pl-2 text-right font-medium" title="That day's close to the next trading day's close, in EUR.">1-day</th>
                              </tr>
                            </thead>
                            <tbody>
                              {d.holdings.map((h) => {
                                const m = marks.get(d.date);
                                const isNew = m?.entered.has(h.company_id) ?? false;
                                const isSold = m?.sold.has(h.company_id) ?? false;
                                // ⚠ BOTH AT ONCE IS A REAL CASE — bought and gone the next day.
                                // Tinting it either green or red would report half of what
                                // happened, so a one-day holding gets its own colour and says so.
                                const tint = isNew && isSold ? 'bg-warn-500/10'
                                  : isNew ? 'bg-pos-500/10'
                                    : isSold ? 'bg-neg-500/10' : '';
                                return (
                                <tr key={h.company_id}
                                  onClick={() => setBreakdown({
                                    companyId: h.company_id, date: d.date,
                                    name: h.company_name ?? h.ticker ?? String(h.company_id),
                                    ticker: h.ticker ?? null,
                                  })}
                                  title="Show the full calculation behind this pick on this day"
                                  className={`border-t border-neutral-800/20 cursor-pointer hover:bg-overlay/[0.05] ${tint}`}>
                                  <td className="py-1 pr-2 font-mono">
                                    {h.ticker ? (
                                      <a
                                        href={guruFocusUrl(h.ticker, h.exchange)}
                                        target="_blank"
                                        rel="noopener noreferrer"
                                        onClick={(e) => e.stopPropagation()}
                                        className="text-accent-400 hover:text-accent-300 hover:underline"
                                      >
                                        {h.ticker}
                                      </a>
                                    ) : <span className="text-fg-soft">—</span>}
                                    {isNew && (
                                      <span className="ml-1.5 text-[10px] uppercase tracking-wide text-pos-400"
                                        title="Not held on the previous trading day — entered on this day.">
                                        new
                                      </span>
                                    )}
                                    {isSold && (
                                      <span className="ml-1.5 text-[10px] uppercase tracking-wide text-neg-400"
                                        title="Not held on the next trading day — sold out of the basket after this day.">
                                        sold
                                      </span>
                                    )}
                                  </td>
                                  <td className="py-1 px-2 text-fg truncate max-w-0" title={h.company_name ?? ''}>
                                    {h.company_name ?? '—'}
                                  </td>
                                  <td className="py-1 px-2 text-fg-muted">{h.sector ?? '—'}</td>
                                  <td className="py-1 px-2 text-right font-mono text-fg-muted">
                                    {h.weight == null ? '—' : `${(h.weight * 100).toFixed(1)}%`}
                                  </td>
                                  <td className="py-1 px-2 text-right font-mono text-fg-muted">
                                    {fmtScore(h.category_scores?.price)}
                                  </td>
                                  <td className="py-1 px-2 text-right font-mono text-fg-muted">
                                    {fmtScore(h.category_scores?.volume)}
                                  </td>
                                  <td className="py-1 px-2 text-right font-mono text-fg">
                                    {fmtScore(h.score)}
                                  </td>
                                  {/* Blank on the newest day by construction: there is no next
                                      trading day to sell into yet. Not a zero. */}
                                  <td className={`py-1 pl-2 text-right font-mono ${pctTone(h.forward_return_pct)}`}>
                                    {fmtPct(h.forward_return_pct)}
                                  </td>
                                </tr>
                                );
                              })}
                            </tbody>
                          </table>
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

      {breakdown && selected && (
        <BreakdownModal target={breakdown} config={selected.config}
          onClose={() => setBreakdown(null)} />
      )}
    </CollapsibleCard>
  );
}
