'use client';

import { Fragment, useEffect, useState } from 'react';
import { apiFetch } from '../../lib/apiFetch';
import { Provenance, type SourceKey } from '../../lib/provenance';
import { SnapshotAge } from '../../lib/snapshotAge';
import { VARIANT_FILTERS } from './portfolioVariants';
import { runSSE } from '../../lib/stream';
import { API_URL } from '../../lib/apiUrl';
import type {
  ModelPortfolioPerformance, ModelPortfolioPositions, StoredModelPortfolio,
} from '../../lib/types/api';
import PortfolioAnalysisModal from './portfolios/PortfolioAnalysisModal';
import FundamentalsModal from './portfolios/FundamentalsModal';

type StoredPortfolio = StoredModelPortfolio;

type Portfolio = {
  id: number;
  name: string;
  /** A name WE chose. Empty = none chosen, and the row falls back to AIRS's `name`.
   *
   *  It sits BESIDE the code, never instead of it: `BUS_BM_AAN_kw_USD_2026_d` is what you search
   *  for in AIRS itself, so replacing it would cost more than the prettier label gives. AIRS caps
   *  `Portefeuille` at 24 chars — the code is an identifier squeezed through a legacy form field,
   *  not a label anyone chose. */
  display_name: string;
  /** The risk profile AIRS offers this model at, or '' when it is not offered at one.
   *
   *  Classified in the BACKEND (`_airs_portfolio_variant`), never here — "bep offensief" contains
   *  "offensief", and a second copy of that rule in TypeScript would put the same portfolio in
   *  different filters on this panel and the correlation matrix below it. */
  variant: string;
  truncated: boolean;
  omschrijving: string;
  fixed: string;
  fixed_datum: string;
  /** DISTINCT instruments in the fixed model. Distinct, because a portfolio can list one
   *  instrument on two lines (VTopSelectie OFF FX holds CapitaLand at 2% and again at 3%) —
   *  that is one instrument, and counting rows would say 29 where it holds 28. The cash line
   *  has no ISIN and is not an instrument either.
   *
   *  Four ways for this to be absent, and NONE of them is "zero":
   *    undefined     — not counted yet (the count phase hasn't reached this row)
   *    null          — NO FIXED MODEL EXISTS (a `normaal`/`meervoudig` portfolio)
   *    no_snapshot   — we looked; AIRS has no DATED composition (its date dropdown held only
   *                    the empty "today" placeholder). Measured on 2 portfolios, both of
   *                    which naive counting reported as "0 holdings".
   *    holdings_error— we asked and it broke.
   *  A real 0 (an empty fixed model) stays expressible, and is currently observed on none. */
  holdings?: number | null;
  no_snapshot?: boolean;
  holdings_error?: string;
  perf?: Perf;
};

type Perf = ModelPortfolioPerformance;

/** YTD is a buy-and-hold of the composition WE HOLD — which is the CURRENT one. AIRS keeps only
 *  2–3 snapshot dates, so January's composition is not recoverable, and the YTD window
 *  therefore opens at `max(1 Jan, inception)` — never before these weights existed.
 *
 *  So this flag no longer means "the number is a backtest" (it isn't any more). It means the
 *  window is SHORT: the model is younger than the year, so its "YTD" covers days rather than
 *  months. MoTopSelectie_FX has held its weights for eight days — +0.51%. Priced back to 1 Jan
 *  it would read +75.85%, on a basket it never held, and top the table. Both facts are why the
 *  ⚠ stays: a partial year is honest, but it is not comparable to a full one. */
const isPartialYear = (p: Perf) => p.model_changed_in_period;

type PosState = { loading: boolean; data?: ModelPortfolioPositions; error?: string;
  // Which source produced this data — so a re-open knows whether to refetch, and the table
  // knows whether it is showing the model composition or the AIRS book's own holdings.
  source?: 'model' | 'book' };
/** One row of a model's composition, as the API shapes it — derived, never re-declared, so it
 *  cannot drift from the contract. */
type PositionRow = NonNullable<ModelPortfolioPositions['rows']>[number];

/** AirSPMS only stores a composition for a `fixed (…)` portfolio. */
const hasFixedModel = (p: Portfolio) => p.fixed?.trim().toLowerCase().startsWith('fixed');

type SortKey =
  | 'name' | 'display_name' | 'holdings' | 'resolved' | 'ytd' | 'since' | 'sharpe' | 'sortino'
  | 'cagr' | 'years' | 'fixed' | 'id';
type Sort = { key: SortKey; dir: 'asc' | 'desc' };

/** Sorting on `holdings` has to answer "where do the un-counted and the model-less rows
 *  go?" — and the answer is: to the bottom, always, in BOTH directions. They aren't small
 *  numbers or large ones; they're absent. Sorting them as if they were 0 would put every
 *  benchmark above a portfolio that genuinely holds one instrument. */
const holdingsRank = (p: Portfolio) =>
  typeof p.holdings === 'number' ? p.holdings : null;

/** "Hide small portfolios" keeps ONLY the rows with a counted model holding more than this —
 *  42 of 95. Everything else goes, and that is deliberate: a row we cannot show a real holdings
 *  count for is not a portfolio worth comparing on this table, whether the count is small (the
 *  single-instrument TOPS_*_L / BUS_BM_* wrappers), absent because the portfolio has no fixed
 *  model at all (`normaal`/`meervoudig`), or absent because AIRS has no dated composition
 *  ("no snapshot") or the count failed.
 *
 *  So this is a KEEP rule, not a drop rule — the filter is "show me the real models", and the
 *  three kinds of absence are excluded BY the rule rather than exempted from it. The Holdings
 *  cell still keeps them strictly apart once they are on screen (unchecking the box shows all
 *  53 again); it is only this filter that treats "no countable model" as one thing. */
const MIN_HOLDINGS_SHOWN = 5;
const isSmall = (p: Portfolio) =>
  !(typeof p.holdings === 'number' && p.holdings > MIN_HOLDINGS_SHOWN);

/** The numeric columns, and how each one ranks. Every one of them can be ABSENT, and absent is
 *  never a value: a model too young for a Sharpe is not a Sharpe of 0, and a portfolio we
 *  cannot price is not flat. They all sink in both directions (see the comparator). */
const NUMERIC: Record<string, (p: Portfolio) => number | null> = {
  holdings: holdingsRank,
  // A portfolio we have not priced at all has no perf row, so it has no resolved count either —
  // that is unknown, not zero, and it sinks like every other absence.
  resolved: (p) => p.perf?.resolved_holdings ?? null,
  ytd: (p) => p.perf?.ytd_pct ?? null,
  since: (p) => p.perf?.since_model_pct ?? null,
  sharpe: (p) => p.perf?.sharpe ?? null,
  sortino: (p) => p.perf?.sortino ?? null,
  cagr: (p) => p.perf?.cagr_pct ?? null,
  years: (p) => p.perf?.years_running ?? null,
};

/** The AirSPMS model portfolios — Stamgegevens > Onderhoud portefeuilles > Model
 * portefeuilles.
 *
 * SSE, not a plain GET, because the scrape is slow and chatty by necessity: one request per
 * list page, PLUS one per row whose name the list truncated. The list cell only ever says
 * "BUS_WTS_Dividend..." — the real "BUS_WTS_Dividend_Fx" exists nowhere on that page, not
 * even in a title attribute, so it has to be re-read from the row's edit page. ~95
 * portfolios is a couple of minutes of authenticated round-trips, so it streams progress
 * instead of hanging a request. */
export default function PortfoliosPanel() {
  const [rows, setRows] = useState<Portfolio[] | null>(null);
  const [scanning, setScanning] = useState(false);
  const [loading, setLoading] = useState(true);
  const [scannedAt, setScannedAt] = useState<string | null>(null);
  const [progress, setProgress] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [q, setQ] = useState('');
  const [hideSmall, setHideSmall] = useState(true);
  const [variant, setVariant] = useState<string>('all');
  const [analyse, setAnalyse] = useState<{ id: number; name: string } | null>(null);
  const [sort, setSort] = useState<Sort>({ key: 'name', dir: 'asc' });

  // The stored copy — instant. Scraping AirSPMS costs minutes, so it is never what a page
  // load does; "Rescan" is the explicit refresh.
  useEffect(() => {
    let cancelled = false;
    void (async () => {
      try {
        const r = await apiFetch(`${API_URL}/api/airs/model-portfolios`);
        const b = (await r.json()) as StoredPortfolio[];
        if (cancelled || !r.ok || !Array.isArray(b) || b.length === 0) return;
        setRows(b.map((p) => ({
          id: p.id,
          name: p.name,
          display_name: p.display_name ?? '',
          variant: p.variant ?? '',
          truncated: p.truncated ?? false,
          omschrijving: p.omschrijving ?? '',
          fixed: p.portfolio_type ?? '',
          fixed_datum: p.fixed_datum ?? '',
          // The view already keeps the three absences apart; map them straight across.
          //   no fixed model      -> null      ("no model")
          //   never counted       -> undefined ("…")
          //   counted (0 is real) -> the number
          holdings: !p.has_fixed_model ? null
            : p.positions_scanned_at == null ? undefined
              : p.no_snapshot ? undefined
                : (p.holdings ?? 0),
          no_snapshot: p.no_snapshot ?? false,
          holdings_error: p.positions_error ?? undefined,
        })));
        setScannedAt(b.reduce<string | null>(
          (m, p) => (p.scanned_at && (!m || p.scanned_at > m) ? p.scanned_at : m), null));

        // YTD is a separate call: it prices every holding of every portfolio, so it is much
        // slower than the list. The table renders first and the column fills in.
        const pr = await apiFetch(`${API_URL}/api/airs/model-portfolios/performance`);
        if (cancelled || !pr.ok) return;
        const perf = (await pr.json()) as Perf[];
        const byId = new Map(perf.map((x) => [x.portfolio_id, x]));
        setRows((prev) => prev?.map((p) => ({ ...p, perf: byId.get(p.id) })) ?? prev);
      } catch {
        // A cold DB is not an error state — the empty prompt below already says what to do.
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => { cancelled = true; };
  }, []);
  // Positions are fetched lazily, per portfolio, on expand — each one is an authenticated
  // AirSPMS round-trip plus an .xls parse, so loading 95 of them up front would be minutes
  // of work for a table nobody asked to see.
  const [open, setOpen] = useState<number | null>(null);
  const [pos, setPos] = useState<Record<number, PosState>>({});
  // The expanded table shows either the MODEL composition (yfinance-priced) or the paired AIRS
  // BOOK's own holdings (AIRS's own EUR values). A single view mode across rows — one is open at
  // a time — mirroring the Analyse modal's Source toggle.
  const [posSource, setPosSource] = useState<'model' | 'book'>('model');

  // Cached by default — the scan already downloaded this XLS to count the holdings, so
  // re-scraping AirSPMS on every expand is a several-second wait for data we hold. `refresh`
  // and a historical `datum` both go live. `source=book` reads the paired book from our DB.
  const loadPositions = async (id: number, datum?: string, refresh?: boolean,
                               sourceOverride?: 'model' | 'book') => {
    const src = sourceOverride ?? posSource;
    setPos((p) => ({ ...p, [id]: { loading: true, source: src } }));
    try {
      const params = new URLSearchParams();
      if (datum) params.set('datum', datum);
      if (refresh) params.set('refresh', 'true');
      if (src === 'book') params.set('source', 'book');
      const qs = params.toString() ? `?${params}` : '';
      const r = await apiFetch(`${API_URL}/api/airs/model-portfolios/${id}/positions${qs}`);
      const b = await r.json().catch(() => null);
      if (!r.ok) { setPos((p) => ({ ...p, [id]: { loading: false, source: src, error: b?.detail ?? `HTTP ${r.status}` } })); return; }
      setPos((p) => ({ ...p, [id]: { loading: false, source: src, data: b as ModelPortfolioPositions } }));
    } catch (e) {
      setPos((p) => ({ ...p, [id]: { loading: false, source: src, error: e instanceof Error ? e.message : String(e) } }));
    }
  };

  const toggle = (id: number) => {
    if (open === id) { setOpen(null); return; }
    setOpen(id);
    if (!pos[id]?.data || pos[id]?.source !== posSource) void loadPositions(id);
  };

  // Flip the whole expanded table between the model composition and the AIRS book, and refetch
  // the open row in the new source.
  const setSource = (id: number, s: 'model' | 'book') => {
    setPosSource(s);
    void loadPositions(id, undefined, false, s);
  };

  const scan = async () => {
    setScanning(true); setError(null); setProgress('Logging in to AirSPMS…'); setRows(null);
    try {
      await runSSE(`${API_URL}/api/airs/model-portfolios/scan`, { method: 'GET' }, (evt) => {
        const e = evt as {
          type?: string; message?: string; portfolios?: Portfolio[];
          id?: number; holdings?: number | null; error?: string; no_snapshot?: boolean;
        };
        if (e.type === 'progress' && e.message) setProgress(e.message);
        if (e.type === 'error') setError(e.message ?? 'scan failed');
        // The list lands first and renders immediately — counting holdings is minutes of
        // AIRS round-trips and must not hold the table hostage.
        if (e.type === 'portfolios' && e.portfolios) {
          // A portfolio with no fixed model is never counted, so mark it `null` up front
          // rather than leaving it looking like a row we simply haven't reached yet.
          setRows(e.portfolios.map((p) => ({ ...p, holdings: hasFixedModel(p) ? undefined : null })));
        }
        // …then each count trickles in and fills its cell.
        if (e.type === 'count' && e.id != null) {
          setRows((prev) => prev?.map((p) => (p.id === e.id
            ? {
              ...p,
              // `holdings` stays undefined for a no-snapshot row — it is unknown, not zero.
              holdings: e.no_snapshot ? undefined : (e.holdings ?? null),
              no_snapshot: e.no_snapshot ?? false,
              holdings_error: e.error,
            }
            : p)) ?? prev);
          if (e.message) setProgress(e.message);
        }
        if (e.type === 'done') setProgress(null);
      });
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setScanning(false);
    }
  };

  const needle = q.trim().toLowerCase();
  const smallCount = (rows ?? []).filter(isSmall).length;
  const view = (rows ?? [])
    .filter((r) => !hideSmall || !isSmall(r))
    // Risk profile. A model not offered at one ('') matches no profile — it is not a hidden
    // Neutraal, it is a product sold without a risk axis, so it appears only under "All".
    .filter((r) => variant === 'all' || r.variant === variant)
    // The chosen name is searchable too — it is the name a reader now knows the model BY, so a
    // search box that could not find it would be worse than not having the column.
    .filter((r) => !needle
      || `${r.name} ${r.display_name} ${r.omschrijving}`.toLowerCase().includes(needle))
    .sort((a, b) => {
      const dir = sort.dir === 'asc' ? 1 : -1;
      const rank = NUMERIC[sort.key];
      if (rank) {
        const x = rank(a), y = rank(b);
        // Absent is not a value — a portfolio we cannot price is not "0% YTD", so it sinks
        // in BOTH directions rather than sorting as if it were flat.
        if (x === null && y === null) return a.name.localeCompare(b.name);
        if (x === null) return 1;
        if (y === null) return -1;
        return (x - y) * dir || a.name.localeCompare(b.name);
      }
      if (sort.key === 'id') return (a.id - b.id) * dir;
      if (sort.key === 'display_name') {
        // A row with NO chosen name is not a row whose name sorts first — it is a row with
        // nothing to sort. It sinks in BOTH directions, exactly like an unpriceable YTD above:
        // absent is not an empty string. (Falling back to `name` here would silently interleave
        // AIRS codes among the chosen labels and make the column look half-populated.)
        const x = a.display_name || null, y = b.display_name || null;
        if (!x && !y) return a.name.localeCompare(b.name);
        if (!x) return 1;
        if (!y) return -1;
        return x.localeCompare(y) * dir || a.name.localeCompare(b.name);
      }
      const s = sort.key === 'fixed' ? [a.fixed, b.fixed] : [a.name, b.name];
      return s[0].localeCompare(s[1]) * dir;
    });

  const counted = (rows ?? []).filter((r) => typeof r.holdings === 'number');
  const totalHoldings = counted.reduce((s, r) => s + (r.holdings ?? 0), 0);

  // Tailwind scans for LITERAL class strings, so `text-${align}` would only ever work by
  // accident (when the same literal happens to appear elsewhere in the file). Full strings.
  const th = (key: SortKey, label: string, align: 'text-left' | 'text-right' = 'text-left',
              title?: string) => (
    <th className={`px-3 py-1.5 font-medium ${align}`} title={title}>
      <button type="button"
        onClick={() => setSort((s) => ({ key, dir: s.key === key && s.dir === 'asc' ? 'desc' : 'asc' }))}
        className={`inline-flex items-center gap-1 hover:text-fg-soft transition-colors ${sort.key === key ? 'text-accent-400' : ''}`}>
        {label}
        <span aria-hidden className={sort.key === key ? '' : 'opacity-0 group-hover:opacity-40'}>
          {sort.key === key ? (sort.dir === 'asc' ? '▲' : '▼') : '▲'}
        </span>
      </button>
    </th>
  );

  return (
    <section className="bg-card border border-neutral-800/40 rounded-xl p-5 space-y-3">
      <div className="flex items-center justify-between gap-3 flex-wrap">
        <div>
          <h3 className="text-sm font-semibold text-fg-strong">
            {/* "Fixed" is AIRS's own word: these are the `fixed (…)` portfolio types, the ones
                that store a composition — and the `_FX` suffix in every name means exactly this.
                Their counterpart is the Dynamic table below. */}
            AIRS Fixed Portfolio&apos;s{rows ? ` · ${view.length}/${rows.length}` : ''}
          </h3>
          <p className="text-[11px] text-fg-faint mt-0.5">
            Stamgegevens › Onderhoud portefeuilles › Model portefeuilles
            {counted.length > 0 && (
              <> · <span className="font-mono text-fg-subtle">{totalHoldings}</span>{' '}positions
                across <span className="font-mono text-fg-subtle">{counted.length}</span>{' '}counted
                model{counted.length === 1 ? '' : 's'}</>
            )}
            {scannedAt && !scanning && (
              <> · stored{' '}
                <span className="font-mono text-fg-subtle" title={scannedAt}>
                  {new Date(scannedAt).toLocaleString()}
                </span>
              </>
            )}
          </p>
        </div>
        <div className="flex items-center gap-2">
          {rows && (
            <div className="flex rounded-lg border border-neutral-800/40 overflow-hidden text-[11px]"
              title="Filter to one risk profile. Read off AIRS's own name, so renaming a model in the Name column cannot change its profile. The models not offered at a profile — the themed TopSelectie and WereldTopSelectie funds, and Risicodragend/Risicomijdend (a different axis entirely) — appear only under All. Same classifier the correlation matrix uses, so the two panels always agree.">
              {VARIANT_FILTERS.map((v) => (
                <button key={v.key} type="button" onClick={() => setVariant(v.key)}
                  className={`px-2.5 py-1 transition-colors whitespace-nowrap ${
                    variant === v.key ? 'bg-accent-600 text-white' : 'text-fg-soft hover:bg-overlay/5'
                  }`}>
                  {v.label}
                </button>
              ))}
            </div>
          )}
          {rows && (
            <label className="flex items-center gap-1.5 text-xs text-fg-muted cursor-pointer select-none whitespace-nowrap"
              title={`Shows only the portfolios whose counted model holds more than ${MIN_HOLDINGS_SHOWN} instruments. Hides ${smallCount}: the single-instrument portfolios, plus every row with no countable model at all — no fixed model, no snapshot, or a count that failed.`}>
              <input type="checkbox" checked={hideSmall} onChange={(e) => setHideSmall(e.target.checked)}
                className="accent-accent-600 cursor-pointer" />
              Hide small portfolios
              {smallCount > 0 && (
                <span className="font-mono text-fg-faint">({smallCount})</span>
              )}
            </label>
          )}
          {rows && (
            <input value={q} onChange={(e) => setQ(e.target.value)} placeholder="Search name / description…"
              className="bg-page border border-neutral-700 rounded-lg px-3 py-1.5 text-xs text-fg focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 w-60" />
          )}
          <button type="button" onClick={() => void scan()} disabled={scanning}
            className="text-sm px-4 py-2 rounded-lg bg-accent-600 hover:bg-accent-500 text-white disabled:opacity-40 disabled:cursor-not-allowed transition-colors">
            {scanning ? 'Scanning…' : rows ? 'Refresh from AIRS' : 'Scan AIRS'}
          </button>
        </div>
      </div>

      {scanning && <div className="loading-bar h-0.5 w-full rounded-full" aria-hidden />}
      {progress && <p className="text-[11px] text-fg-subtle font-mono">{progress}</p>}
      {error && (
        <div className="bg-neg-500/10 border border-neg-500/20 rounded-lg px-3 py-2 text-xs text-neg-300">{error}</div>
      )}

      {loading && !rows && <p className="text-xs text-fg-subtle">Loading stored portfolios…</p>}

      {!loading && !rows && !scanning && !error && (
        <p className="text-xs text-fg-subtle">
          Nothing stored yet — hit <span className="text-accent-300">Scan AIRS</span>. It walks the
          paginated list, then downloads each portfolio&apos;s XLS to count its holdings, and
          saves both. After that this page is an instant DB read.
        </p>
      )}

      {rows && rows.length > 0 && (
        <div className="overflow-auto rounded-lg border border-neutral-800/40 max-h-[70vh]">
          <table className="w-full text-xs">
            <thead className="bg-card sticky top-0 z-10">
              <tr className="group text-fg-faint text-[10px] uppercase tracking-wide border-b border-neutral-800/40">
                <th className="px-3 py-1.5 font-medium text-left w-[5.5rem]"
                  title="Composition of this model — sector, region and currency — beside the SP500 benchmark, on one set of groups.">
                  Analyse
                </th>
                {th('name', 'Portfolio')}
                <th className="px-3 py-1.5 font-medium text-left">Description</th>
                {th('display_name', 'Name', 'text-left',
                  "A name you choose, for reading. AIRS's own is a 24-char code squeezed through a legacy form field — this is the label. Click a cell to set it; clear it to fall back to the code. Sortable, and rows with no chosen name sort last in both directions — absent is not an empty string.")}
                {th('holdings', 'Holdings', 'text-right')}
                {th('resolved', 'Resolved', 'text-right',
                  'How many of those instruments we can actually price — i.e. have a Yahoo (yfinance) price series for. The gap is what the returns are renormalised over, and under 60% of the weight it is why a row shows n/a.')}
                {th('ytd', 'YTD (€)', 'text-right')}
                {/* The since-inception block. All three ride ONE window — from the model's own
                    fixed date — because that is the only stretch in which the weights we hold
                    were the weights it held. A ratio is only as honest as the return under it. */}
                {th('since', 'Since incep. (€)', 'text-right',
                  'Return in EUR since the model\'s fixed date — the composition\'s own effective date. Never borrows hindsight, for any portfolio.')}
                {th('sharpe', 'Sharpe', 'text-right',
                  'Annualized return ÷ annualized volatility of the daily EUR curve since the fixed date, rf = 0. Absent under 20 trading days — a ratio off a week-old model is noise.')}
                {th('sortino', 'Sortino', 'text-right',
                  'Same, but divided by DOWNSIDE deviation only — it does not penalise a portfolio for rising. Absent when the curve never fell (undefined, not infinite).')}
                {th('cagr', 'CAGR', 'text-right',
                  'Geometric annualized return since the fixed date. ABSENT under one year of trading — compounding a four-month return out to a year extrapolates it (AITopSelectie would read +114.8% off 135 days). For those, the realized number is in Since incep.')}
                {th('years', 'Years', 'text-right',
                  'How long this model has been running: its fixed date to today, in calendar years. The unit the ratios to the left have to be read against — and why a CAGR can be missing (under 1.00, there is none).')}
                {th('fixed', 'Type')}
                <th className="px-3 py-1.5 font-medium text-left" title="The model's own effective date — when this composition took effect. It is the inception the three columns to the left are measured from.">
                  Fixed date
                </th>
                {th('id', 'AIRS id', 'text-right')}
              </tr>
            </thead>
            <tbody className="divide-y divide-neutral-800/20">
              {view.map((r) => (
                <Fragment key={r.id}>
                <tr onClick={() => toggle(r.id)}
                  className="hover:bg-accent-500/10 transition-colors cursor-pointer">
                  <td className="px-3 py-1.5">
                    {/* stopPropagation: the row's own onClick expands the positions table, and a
                        button that also expanded the row would do two things on one press. */}
                    <button type="button"
                      onClick={(e) => { e.stopPropagation(); setAnalyse({ id: r.id, name: r.name }); }}
                      disabled={noComposition(r)}
                      title={noComposition(r)
                        ? 'This portfolio has no fixed model — AIRS stores no composition for it, so there is nothing to analyse.'
                        : 'Sector / region / currency split vs the SP500 benchmark'}
                      className="text-[11px] px-2 py-1 rounded-lg border border-neutral-700 text-accent-400 hover:border-accent-500/50 hover:bg-overlay/5 disabled:opacity-30 disabled:cursor-not-allowed transition-colors">
                      Analyse
                    </button>
                  </td>
                  <td className="px-3 py-1.5 font-mono text-fg whitespace-nowrap">
                    <span className="text-fg-faint mr-1.5">{open === r.id ? '▾' : '▸'}</span>
                    {r.name}
                    {/* Should never appear: a truncated name means the edit-page lookup for
                        this row failed, and the value shown is the CLIPPED one. Say so
                        rather than presenting a half-name as if it were the real one. */}
                    {r.truncated && (
                      <span title="The list page truncates this name and its edit page could not be read — this value is CLIPPED, not the real portfolio name."
                        className="ml-2 text-[9px] uppercase tracking-wider font-semibold px-1 py-0.5 rounded border bg-warn-500/15 text-warn-300 border-warn-500/25">
                        clipped
                      </span>
                    )}
                  </td>
                  <td className="px-3 py-1.5 text-fg-soft">{r.omschrijving || '—'}</td>
                  <td className="px-3 py-1.5">
                    <DisplayNameCell p={r} onSaved={(v) => setRows((rs) => (rs ?? []).map(
                      (x) => (x.id === r.id ? { ...x, display_name: v } : x)))} />
                  </td>
                  <td className="px-3 py-1.5 text-right font-mono whitespace-nowrap">
                    <HoldingsCell p={r} />
                  </td>
                  <td className="px-3 py-1.5 text-right font-mono whitespace-nowrap">
                    <ResolvedCell p={r} />
                  </td>
                  <td className="px-3 py-1.5 text-right font-mono whitespace-nowrap">
                    <YtdCell p={r} />
                  </td>
                  <td className="px-3 py-1.5 text-right font-mono whitespace-nowrap">
                    <SinceCell p={r} />
                  </td>
                  <td className="px-3 py-1.5 text-right font-mono whitespace-nowrap">
                    <RatioCell p={r} kind="sharpe" />
                  </td>
                  <td className="px-3 py-1.5 text-right font-mono whitespace-nowrap">
                    <RatioCell p={r} kind="sortino" />
                  </td>
                  <td className="px-3 py-1.5 text-right font-mono whitespace-nowrap">
                    <CagrCell p={r} />
                  </td>
                  <td className="px-3 py-1.5 text-right font-mono whitespace-nowrap">
                    <YearsCell p={r} />
                  </td>
                  <td className="px-3 py-1.5 text-fg-subtle whitespace-nowrap">{r.fixed || '—'}</td>
                  <td className="px-3 py-1.5 font-mono text-fg-subtle whitespace-nowrap">
                    <FixedDateCell p={r} />
                  </td>
                  <td className="px-3 py-1.5 text-right font-mono text-fg-faint">{r.id}</td>
                </tr>
                {open === r.id && (
                  <tr>
                    <td colSpan={14} className="px-3 py-3 bg-inset">
                      <Positions
                        state={pos[r.id]}
                        source={pos[r.id]?.source ?? 'model'}
                        onSource={(s) => setSource(r.id, s)}
                        onPickDate={(d) => void loadPositions(r.id, d)}
                        onRefresh={() => void loadPositions(r.id, undefined, true)}
                        // A link edit re-reads from OUR cache, never from AIRS: the composition
                        // did not change, only what we say one of its rows is.
                        onLinkSaved={() => void loadPositions(r.id)}
                      />
                    </td>
                  </tr>
                )}
                </Fragment>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {analyse && (
        <PortfolioAnalysisModal id={analyse.id} name={analyse.name}
          onClose={() => setAnalyse(null)} />
      )}
    </section>
  );
}

/** The YTD cell. The number is only half the story, so the cell carries the other half.
 *
 *   n/a         — under 60% of the model's weight is priceable (structured products, in-house
 *                 funds). We return NOTHING rather than renormalise 1% of a portfolio up to
 *                 100% and print it to two decimals. That actually happened: TOPS_OFF_BEH
 *                 read "+0.00%", which was its cash line, alone.
 *   ⚠ (amber)   — a PARTIAL year. The model is younger than the year, so the window opens at
 *                 its inception rather than 1 Jan (it never held these weights in January, and
 *                 pricing them back there would be a backtest). The figure is real — it is just
 *                 not a year, and it is sitting in a column of them. Hover for the window.
 *   plain       — the model predates Jan 1: it held these weights all year, so this IS its
 *                 return, over the full year.
 */
function YtdCell({ p }: { p: Portfolio }) {
  const f = p.perf;
  if (!f) return <NoNumber p={p} />;

  if (f.ytd_pct == null) {
    return (
      <NotAvailable title={`Only ${(f.covered_pct ?? 0).toFixed(0)}% of this model's weight can be priced (${f.unpriced_holdings} holding(s) have no price series — typically Leonteq structured products or in-house funds). A return renormalised over the rest would be an invention, so none is shown.`} />
    );
  }

  const v = f.ytd_pct;
  const colour = v >= 0 ? 'text-pos-400' : 'text-neg-400';
  const hint = isPartialYear(f)
    ? `⚠ PARTIAL YEAR — measured from ${f.ytd_from}, not 1 January. This model took effect on ${f.model_effective}, DURING the year: it never held these weights in January, and pricing them back there would backtest a basket chosen with hindsight (that would read very differently). So this is ${statDays(f)} trading day(s) of realized return, sitting in a column of full years — do not rank it against one.`
    : `Full year — measured from ${f.ytd_from}. This model has held these weights since ${f.model_effective}, before the year began, so this IS what it earned.`;
  // ONE approximation marker, not two. Renormalised coverage and an interpolated opening mark
  // are different CAUSES, but to a reader they are the same fact — this number is an estimate —
  // and two symbols side by side read as two separate problems rather than one. The tooltip
  // says which cause(s) apply; the glyph just says "approximate". Same size as the percentage:
  // it qualifies that number, so it is not a footnote to it.
  const cov = f.partial_coverage
    ? ` ≈ Only ${(f.covered_pct ?? 0).toFixed(0)}% of its weight is priceable; the rest is assumed to have behaved the same.`
    : '';
  const est = (f.interpolated_holdings ?? 0) > 0;
  const estWhy = est
    ? ` ≈ ${f.interpolated_holdings} holding(s) had no close near the start of the window, so their opening price was INTERPOLATED between the closes either side of it — this return is partly modelled. Expand the row to see which.`
    : '';
  const approx = f.partial_coverage || est;

  return (
    <span title={hint + cov + estWhy} className="inline-flex items-center gap-1">
      {isPartialYear(f) && <span className="text-warn-400" aria-label="partial year">⚠</span>}
      {approx && <span className="text-warn-400" aria-label="approximate">≈</span>}
      <span className={colour}>{v >= 0 ? '+' : ''}{v.toFixed(2)}%</span>
      <Provenance source="yfinance" asOf={f.sources?.yf_close}
        what="What this model has returned so far this year, holding its current weights."
        note="asset_price close, EUR via fx_rate"
        how="Buy-and-hold EUR return of the composition from the YTD start date (max of 1 Jan and the model's inception) to the latest close, weighted by the model's percentages." />
    </span>
  );
}

/** How many of the model's instruments we can actually PRICE — i.e. have a Yahoo (`asset_price`)
 *  series for. Read against the Holdings column to its left, which it reconciles with exactly:
 *  `resolved + unresolved == holdings` (both count DISTINCT ISINs and both exclude cash, which
 *  has no ISIN and is not an instrument).
 *
 *  This is the number behind every `n/a` on the row. BUS_Alternatives_FX holds 9 and we can
 *  price 5 — 45% of its weight is a Global X / YieldMax / WisdomTree ETF still `queued` in the
 *  grid, plus an in-house fund with no listing at all — so its coverage is 55%, under the 60%
 *  floor, and no return is shown. Without this column that refusal has no visible cause.
 *
 *  Amber whenever it is short of the holdings count: a gap here is not cosmetic, it is the
 *  weight the returns are renormalised over. */
function ResolvedCell({ p }: { p: Portfolio }) {
  const f = p.perf;
  if (!f) return <NoNumber p={p} />;

  const n = f.resolved_holdings ?? 0;
  const missing = f.unresolved_holdings ?? 0;
  const total = n + missing;
  if (missing === 0) {
    return (
      <span className="text-fg" title={`All ${n} instrument(s) have a Yahoo price series.`}>
        {n}
      </span>
    );
  }
  return (
    <span className="text-warn-300"
      title={`${n} of ${total} instrument(s) have a Yahoo price series — ${missing} do not (typically a Leonteq structured product, an in-house fund with no listing, or an ETF still unresolved in the instrument grid). That is ${(100 - (f.covered_pct ?? 0)).toFixed(0)}% of this model's weight, and the returns are renormalised over the rest.`}>
      {n}<span className="text-fg-faint">/{total}</span>
    </span>
  );
}

/** "We cannot say" — ONE rendering of it, used by every derived column (YTD, Since incep.,
 *  Sharpe, Sortino, CAGR). They all refuse for the same reason and they must therefore LOOK the
 *  same: five refusals in three different styles reads as three different problems across a row
 *  where there is only one. The `title` is where the columns differ, not the badge. */
function NotAvailable({ title }: { title: string }) {
  return (
    <span title={title}
      className="text-[9px] uppercase tracking-wider font-semibold px-1 py-0.5 rounded border bg-neutral-500/10 text-fg-faint border-neutral-600/30">
      n/a
    </span>
  );
}

/** A `%` return, coloured. The one place the sign is turned into a colour. */
function Pct({ v }: { v: number }) {
  return (
    <span className={v >= 0 ? 'text-pos-400' : 'text-neg-400'}>
      {v >= 0 ? '+' : ''}{v.toFixed(2)}%
    </span>
  );
}

/** A row with no fixed model has no performance row and never will — AIRS stores no
 *  composition for a `normaal`/`meervoudig` portfolio at all. Rendering that as the same "…"
 *  the priced rows wear while they load leaves a benchmark wrapper looking like a row that is
 *  still loading, for ever. It isn't pending; there is nothing to price. */
const noComposition = (p: Portfolio) => p.holdings === null;

function NoNumber({ p }: { p: Portfolio }) {
  return noComposition(p)
    ? (
      <span className="text-fg-faint"
        title="This portfolio is not of type fixed (…) — AIRS stores no composition for it, so there is nothing to price. Not a pending number: an absent one.">
        —
      </span>
    )
    : <span className="text-fg-faint" title="Not computed yet.">…</span>;
}

/** Trading days of daily return the backend needs before it will report a ratio at all
 *  (mirrors `MIN_STAT_DAYS` in `_airs_portfolio_perf.py`). Used here only to EXPLAIN the
 *  absence — the backend decides it; a disagreement would show as a wrong tooltip, never as a
 *  ratio the backend withheld. */
const MIN_STAT_DAYS = 20;

/** Daily returns behind a portfolio's ratios. Optional in the payload (Pydantic-defaulted), and
 *  a missing sample is 0 days of it, not an unknown number of them. */
const statDays = (f: Perf) => f.stat_days ?? 0;

/** Why a since-inception figure is missing — and the three reasons are NOT the same thing. The
 *  cell must never render a bare blank: a blank reads as "flat", and "we could not measure it"
 *  is not "it made 0%". */
function absentSince(f: Perf): string | null {
  if (f.low_coverage) {
    return `Only ${(f.covered_pct ?? 0).toFixed(0)}% of this model's weight can be priced (${f.unpriced_holdings} holding(s) have no price series — typically Leonteq structured products or in-house funds). A return renormalised over the rest would be an invention, so none is shown.`;
  }
  if (f.since_model_pct == null) {
    return `At this model's inception (${f.model_effective}) only ${(f.since_covered_pct ?? 0).toFixed(0)}% of its weight had a price series — a holding that had not listed yet cannot be held from there. Its YTD is still measurable; this window is not.`;
  }
  return null;
}

/** The since-inception cell — the model's return since ITS OWN effective date.
 *
 * This is the honest number for every portfolio, hindsight-flagged or not, which is exactly why
 * it sits next to the YTD: where the ⚠ says the YTD is a backtest, this column says what the
 * portfolio actually did. MoTopSelectie_FX: +75.85% YTD, +0.51% since the model took effect.
 *
 * NOTE the window is the COMPOSITION's effective date, which is not always AIRS's "Fixed date"
 * column (they disagree on 39 of 56, usually by days but once by five weeks). Anchoring on the
 * Fixed date where it is EARLIER — 33 of them — would price the weights before they were
 * chosen, which is the hindsight bug this whole module exists to refuse. The Fixed date cell
 * shows the model date underneath itself wherever the two differ. */
function SinceCell({ p }: { p: Portfolio }) {
  const f = p.perf;
  if (!f) return <NoNumber p={p} />;

  const absent = absentSince(f);
  if (absent || f.since_model_pct == null) {
    return <NotAvailable title={absent ?? 'No return since inception.'} />;
  }
  return (
    <span title={`EUR return since this composition took effect on ${f.model_effective} — ${statDays(f)} trading day(s) ago. Realized, not backtested: these are the weights it has held for that whole window.`}>
      <Pct v={f.since_model_pct} />
      <Provenance source="yfinance" asOf={f.sources?.yf_close}
        what="What this model has returned since the day its composition took effect."
        note="asset_price close, EUR via fx_rate"
        how="Same buy-and-hold EUR return, measured from the composition's own inception (model_effective) to the latest close." />
    </span>
  );
}

/** Sharpe / Sortino over the since-inception window. Absent is not a small number.
 *
 *   n/a   — the return underneath the ratio doesn't exist (see `absentSince`).
 *   —     — the window is too SHORT (under 20 trading days: 27 of 56 models were redefined this
 *           year, one of them eight days before it was measured), or the denominator is
 *           undefined: a flat curve has no volatility, and a curve that never fell has no
 *           downside deviation. Rendering that as a big number would be a lie about its risk.
 */
function RatioCell({ p, kind }: { p: Portfolio; kind: 'sharpe' | 'sortino' }) {
  const f = p.perf;
  if (!f) return <NoNumber p={p} />;

  const v = kind === 'sharpe' ? f.sharpe : f.sortino;
  if (v == null) {
    const absent = absentSince(f);
    const why = absent
      ? absent
      : statDays(f) < MIN_STAT_DAYS
        ? `Only ${statDays(f)} trading day(s) since this model took effect (${f.model_effective}). A ratio off that few points is noise with two decimals — and it would render exactly like one measured over two years. So none is shown.`
        : kind === 'sortino'
          ? 'Its curve never fell over this window, so downside deviation is zero — Sortino is undefined, not infinite.'
          : 'Its curve never moved, so volatility is zero — Sharpe is undefined, not infinite.';
    // `n/a` = we cannot say (no priceable return underneath). `—` = we CAN say, and the answer is
    // "undefined" or "too short a window". Different facts, so different marks — but every `n/a`
    // in the row wears the same badge.
    return absent
      ? <NotAvailable title={why} />
      : <span className="text-fg-faint" title={why}>—</span>;
  }

  const vol = f.ann_vol_pct != null ? ` Annualized volatility ${f.ann_vol_pct.toFixed(1)}%.` : '';
  return (
    <span className={v >= 0 ? 'text-fg' : 'text-neg-400'}
      title={`${kind === 'sharpe' ? 'Sharpe' : 'Sortino'} of the daily EUR curve since ${f.model_effective}, annualized over ${statDays(f)} trading days at rf = 0.${vol}`}>
      {v.toFixed(2)}
      <Provenance source="yfinance" asOf={f.sources?.yf_close}
        what={kind === 'sharpe'
          ? 'Return measured against how much the model bounced around, up or down, to earn it.'
          : 'Return measured against the FALLS only — a rise is not a risk.'}
        note="asset_price daily EUR curve"
        how={kind === 'sharpe'
          ? 'Mean ÷ standard deviation of the daily EUR return series since inception, annualized ×√252, risk-free = 0.'
          : 'Mean ÷ downside deviation (negative days only) of the daily EUR returns since inception, annualized ×√252, risk-free = 0.'} />
    </span>
  );
}

/** The geometric annualized return since the fixed date.
 *
 *  ⚠ ABSENT under a year of trading, and that is the whole design of the cell. A CAGR compounds
 *  a window's return out to a year, so a short window is not merely noisy — it is systematically
 *  amplified: AITopSelectie OFF FX made +50.61% in 135 trading days, which annualizes to
 *  +114.8%. That number would sit in this column, same font, beside one earned over two years.
 *  Fund reporting does not annualize a sub-year period for exactly this reason; it shows the
 *  cumulative return, which is the `Since incep.` column and is always there. */
function CagrCell({ p }: { p: Portfolio }) {
  const f = p.perf;
  if (!f) return <NoNumber p={p} />;

  if (f.cagr_pct == null) {
    const absent = absentSince(f);
    const yrs = f.years_running;
    const why = absent
      ?? (yrs != null && yrs < 1
        ? `This model has been running ${yrs.toFixed(2)} years — under one. Annualizing a shorter window extrapolates it (${f.since_model_pct?.toFixed(2)}% over ${statDays(f)} trading days would compound to a far larger yearly figure on the strength of a few months), so no CAGR is shown. Its realized return is the Since incep. column.`
        : 'Not enough of a window to annualize.');
    return absent
      ? <NotAvailable title={why} />
      : <span className="text-fg-faint" title={why}>—</span>;
  }
  return (
    <span title={`Geometric annualized return since ${f.model_effective}, over ${f.years_running?.toFixed(2)} years. Compounding this rate for that long reproduces the ${f.since_model_pct?.toFixed(2)}% in Since incep.`}>
      <Pct v={f.cagr_pct} />
      <Provenance source="yfinance" asOf={f.sources?.yf_close}
        what="The model's return restated as a per-year rate, so different ages compare."
        note="asset_price daily EUR curve"
        how="Geometric annualized return: (1 + since-inception return) ^ (365.25 / days held) − 1." />
    </span>
  );
}

/** How long the model has been running — its fixed date to today, in calendar years.
 *
 *  Not decoration: it is the unit every annualized figure to its left has to be read against,
 *  and it is the visible reason a CAGR is missing (under 1.00, there isn't one). */
function YearsCell({ p }: { p: Portfolio }) {
  const f = p.perf;
  if (!f || f.years_running == null) return <NoNumber p={p} />;
  const y = f.years_running;
  return (
    <span className={y < 1 ? 'text-warn-300' : 'text-fg'}
      title={`Running since ${f.model_effective} — ${statDays(f)} trading day(s).${y < 1 ? ' Under a year, so it has no CAGR and its Sharpe/Sortino rest on a short sample.' : ''}`}>
      {y.toFixed(2)}
    </span>
  );
}

/** AIRS's own "Fixed date" — and, where it differs, the date the numbers are ACTUALLY measured
 *  from.
 *
 *  These are two different AIRS fields and they disagree on 39 of 56 portfolios. The one on the
 *  list page is `fixed_datum`; the composition we hold and price is the one dated
 *  `positions_datum`, its effective date. Showing only the first, beside three columns measured
 *  from the second, invites the reader to check a return against a window it was never computed
 *  over — so where they differ, both are shown. */
function FixedDateCell({ p }: { p: Portfolio }) {
  const eff = p.perf?.model_effective;
  const differs = eff && p.fixed_datum && eff !== p.fixed_datum;
  return (
    <span title={differs
      ? `AIRS's list shows ${p.fixed_datum}, but the composition we hold is dated ${eff} — that is the inception the Since / Sharpe / Sortino columns measure from.`
      : undefined}>
      {p.fixed_datum || '—'}
      {differs && (
        <span className="block text-[10px] text-fg-faint">model {eff}</span>
      )}
    </span>
  );
}

/** The Holdings cell. Four states, and three of them are NOT "zero":
 *
 *   n           — instruments in the fixed model (ISIN-bearing rows; cash isn't an instrument)
 *   0           — a real, EMPTY fixed model. Exactly one portfolio is like this, and it is
 *                 the whole reason `null` and `0` are kept apart.
 *   no model    — a `normaal`/`meervoudig` portfolio (benchmark, multi-model wrapper). AIRS
 *                 stores no composition for these AT ALL, so "0 holdings" would be a claim
 *                 about a model that doesn't exist.
 *   …/failed    — we didn't learn the answer. Writing 0 here would be a fabricated fact.
 */
function HoldingsCell({ p }: { p: Portfolio }) {
  if (p.holdings_error) {
    return (
      <span title={p.holdings_error}
        className="text-[9px] uppercase tracking-wider font-semibold px-1 py-0.5 rounded border bg-neg-500/15 text-neg-300 border-neg-500/25">
        failed
      </span>
    );
  }
  if (p.holdings === null) {
    return (
      <span title="This portfolio is not of type fixed (…), so AIRS stores no composition for it at all. That is not zero holdings — there is no model to hold anything."
        className="text-[9px] uppercase tracking-wider font-semibold px-1 py-0.5 rounded border bg-neutral-500/10 text-fg-faint border-neutral-600/30">
        no model
      </span>
    );
  }
  if (p.no_snapshot) {
    return (
      <span title="AIRS has no dated composition for this portfolio — its snapshot dropdown held nothing but the empty 'today' placeholder. So we do not know what it holds; that is NOT the same as holding nothing."
        className="text-[9px] uppercase tracking-wider font-semibold px-1 py-0.5 rounded border bg-warn-500/15 text-warn-300 border-warn-500/25">
        no snapshot
      </span>
    );
  }
  if (p.holdings === undefined) {
    return <span className="text-fg-faint" title="Not counted yet — the scan is still walking the portfolios.">…</span>;
  }
  if (p.holdings === 0) {
    return <span className="text-warn-300" title="A fixed model that contains no instruments — genuinely empty, not un-counted. Currently observed on none.">0</span>;
  }
  return <span className="text-fg">{p.holdings}</span>;
}

type Position = ModelPortfolioPositions['rows'][number];

/** The five price columns of a position row: what it was worth when the YTD window opened, what
 *  it is worth now, and the EUR return between them — the arithmetic BEHIND the portfolio's YTD.
 *  Weight these returns by the model's percentages and you get that number back exactly (checked:
 *  AITopSelectie OFF FX, 51.4812% both ways).
 *
 *  ⚠ The prices are in EUR, deliberately, because the return is an EUR return and carries the FX
 *  leg. Showing the LOCAL closes here would print two numbers whose ratio is not the third — a
 *  USD holding can rise in dollars and fall in euros on the same days. The local close and its
 *  currency are in the tooltip, where they inform without pretending to be the sum.
 *
 *  Three rows have no marks, and none of them is a zero return:
 *    cash          — no ISIN, no series. It IS priced, at a flat 0%, inside the portfolio figure.
 *    unresolved    — no Yahoo listing (a structured product, an in-house fund, a queued ETF).
 *    not-yet-held  — no close on or before the window opened, so it cannot be marked from there.
 */
function MarkCells({ p, ytdFrom, source }: {
  p: Position; ytdFrom?: string | null; source: 'model' | 'book';
}) {
  const eur = (v: number) => v.toLocaleString('en-GB', {
    minimumFractionDigits: 2, maximumFractionDigits: 2,
  });
  // Where each mark comes from: the book's own EUR position values (AIRS VOLK) or our yfinance
  // close × FX. A look-through row is neither — it's a computed basket index. See <Provenance>.
  const isBookMark = source === 'book';
  const markSrc: SourceKey = p.lookthrough ? 'derived' : isBookMark ? 'airs_volk' : 'yfinance';
  const startNote = p.lookthrough ? 'look-through basket, indexed to 100'
    : isBookMark ? 'Beginwaarde (start-of-year value)' : 'close in EUR at that date’s FX';
  const endNote = p.lookthrough ? 'look-through basket, indexed to 100'
    : isBookMark ? 'Huidige waarde (snapshot value)' : 'latest close in EUR';
  const startHow = p.lookthrough
    ? 'Look-through: the linked model’s basket indexed to 100 at the window open — only the Start→End return is a real number.'
    : isBookMark
      ? 'AIRS Beginwaarde — the position’s own EUR value at the start of the year (1 Jan).'
      : 'The holding’s last yfinance close on or before the window opened, converted to EUR at that date’s FX rate.';
  const endHow = p.lookthrough
    ? 'Look-through: the linked model’s basket value now, indexed the same way as Start.'
    : isBookMark
      ? 'AIRS Huidige waarde — the position’s own EUR value at the snapshot date.'
      : 'The holding’s latest yfinance close, converted to EUR at that date’s FX rate.';
  const returnHow = 'End ÷ Start − 1. An EUR return, so it carries the FX leg — a USD holding can rise in dollars yet fall here.';

  if (p.start_price_eur == null || p.end_price_eur == null) {
    // ⚠ A STALE SERIES IS NOT A BROKEN MAPPING, and the blank looks identical. Meta Platforms is
    // correctly mapped to META with years of data — but its last close is 2026-07-02 while
    // BUS_2.0_NEU_FX's window opens 2026-07-09, so there is NO PRICE INSIDE THE WINDOW and no
    // return over it can exist. Telling the reader "it listed later" (the old text) sends them
    // hunting for a mapping bug that isn't there. Name the last close and let it speak.
    const stale = p.isin && p.last_close && ytdFrom && p.last_close < ytdFrom;
    const why = !p.isin
      ? 'Cash — it has no price series. It is not skipped: it is priced at a flat 0% inside the portfolio return, because its drag is real.'
      : !p.known_instrument
        ? 'This ISIN is not an instrument in our grid, so we have no price series for it — typically an in-house fund, or an ETF still queued for resolution.'
        : stale
          ? `⚠ STALE PRICES, not a bad mapping. This holding's latest close is ${p.last_close} — BEFORE this window opened on ${ytdFrom} — so there is no price inside the window and no return over it can exist. The instrument and its listing are fine; the price series just hasn't been refreshed. Refresh it from the instrument grid.`
          : `No close on or before ${ytdFrom ?? 'the window'}, so the holding cannot be marked from there — it listed later, or its series has no data that far back.${p.last_close ? ` Its latest close is ${p.last_close}.` : ''}`;
    return (
      <>
        {[0, 1, 2, 3, 4].map((i) => (
          <td key={i} className={`px-3 py-1.5 font-mono text-right ${stale ? 'text-warn-300' : 'text-fg-faint'}`}>
            <span title={why}>{stale && i === 0 ? '⚠ ' : ''}—</span>
          </td>
        ))}
      </>
    );
  }

  const local = (v: number | null | undefined, d: string | null | undefined) =>
    v != null && p.currency
      ? `${p.currency} ${v.toLocaleString('en-GB', { maximumFractionDigits: 2 })} on ${d} — converted at that date's own FX rate.`
      : undefined;

  // ⚠ The opening price is an ESTIMATE, not a close. This holding has no price near the date the
  // window opened — it trades rarely, or is pointed at a listing that does — so the value was
  // straight-lined between the two real closes either side of it. It renders in the same column,
  // same font, as an observed price, so it must SAY it is not one.
  const est = p.start_interpolated;
  const estWhy = `⚠ ESTIMATE, not a traded price. This holding has no close near ${p.start_date} — the two real closes bracketing that date are ${p.start_gap_days} days apart — so its opening value was linearly INTERPOLATED between them. Everything downstream of it (this row's return, and its share of the portfolio's) is therefore partly modelled. Usually the real cause is a bad listing: check the instrument's Yahoo symbol.`;

  // ⚠ These marks are a LOOK-THROUGH, not a traded price: this holding is a certificate wrapping
  // another model, which Yahoo cannot price. Start/End are that model's BASKET indexed to 100 at
  // the window open — only the Return between them is a real number (and it weights into the
  // portfolio total exactly like a priced holding). The index must never read as a share price,
  // so it wears the accent colour and says what it is.
  const lt = p.lookthrough;
  const ltWhy = `Look-through, not a traded price. This holding is a certificate wrapping the model portfolio “${p.linked_portfolio_name ?? 'linked'}”, which Yahoo cannot price directly. Start and End are that model's basket indexed to 100 when the window opened — only the Return between them is a real number, and it weights into this portfolio's total exactly like a priced holding.`;

  const startTitle = lt ? ltWhy : est ? estWhy : local(p.start_price_local, p.start_date);
  const startClass = lt ? 'text-accent-400' : est ? 'text-warn-300' : 'text-fg';

  return (
    <>
      <td className="px-3 py-1.5 text-right font-mono whitespace-nowrap">
        <span className={startClass} title={startTitle}>
          {est && <span aria-label="interpolated" className="text-warn-400 mr-1">⚠</span>}
          {lt && <span aria-label="priced via the linked model portfolio" className="text-accent-400 mr-1">↳</span>}
          {eur(p.start_price_eur)}
        </span>
        <Provenance source={markSrc} asOf={p.start_date} note={startNote} how={startHow}
          what={`What one unit of ${p.fonds ?? 'this holding'} was worth when the window opened.`} />
      </td>
      <td className="px-3 py-1.5 font-mono whitespace-nowrap">
        <span className={est ? 'text-warn-300' : lt ? 'text-accent-400/80' : 'text-fg-subtle'}
          title={lt ? ltWhy : est ? estWhy : undefined}>
          {p.start_date}{est && <span className="text-fg-faint"> (est)</span>}
        </span>
      </td>
      <td className="px-3 py-1.5 text-right font-mono whitespace-nowrap">
        <span className={lt ? 'text-accent-400' : 'text-fg'}
          title={lt ? ltWhy : local(p.end_price_local, p.end_date)}>{eur(p.end_price_eur)}</span>
        <Provenance source={markSrc} asOf={p.end_date} note={endNote} how={endHow}
          what={`What one unit of ${p.fonds ?? 'this holding'} is worth now.`} />
      </td>
      <td className={`px-3 py-1.5 font-mono whitespace-nowrap ${lt ? 'text-accent-400/80' : 'text-fg-subtle'}`}>{p.end_date}</td>
      <td className="px-3 py-1.5 text-right font-mono whitespace-nowrap">
        {p.return_pct != null ? <Pct v={p.return_pct} /> : <span className="text-fg-faint">—</span>}
        {p.return_pct != null && (
          <Provenance source="derived" asOf={p.end_date}
            what={`What ${p.fonds ?? 'this holding'} returned over the window, in euros.`}
            note={source === 'book' ? 'Huidige / Beginwaarde − 1 (EUR)' : 'End / Start − 1 (EUR)'}
            how={returnHow} />
        )}
      </td>
    </>
  );
}

/** The chosen name — click to edit, blank to clear back to AIRS's code.
 *
 * ⚠ THE FALLBACK IS SHOWN, NOT SUBSTITUTED. A row with no chosen name renders a muted "—", not a
 * greyed-out copy of the AIRS code: a cell that quietly repeats the column to its left makes the
 * table look fully populated and gives you no way to see which models still need naming — which
 * is the only thing this column is for while it is being filled in.
 *
 * Saves on blur or Enter, reverts on Escape. There is no Save button because there is nothing to
 * batch: one field, one row, one call.
 */
function DisplayNameCell({ p, onSaved }: { p: Portfolio; onSaved: (v: string) => void }) {
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState(p.display_name);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState(false);

  const commit = async () => {
    const next = draft.trim();
    setEditing(false);
    if (next === p.display_name) return;        // nothing changed — don't spend a request
    setBusy(true);
    setErr(false);
    try {
      const r = await apiFetch(`${API_URL}/api/airs/model-portfolios/${p.id}/display-name`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        // '' clears it. The backend maps blank -> NULL, so the row falls back to AIRS's code
        // rather than storing a deliberate-looking empty label.
        body: JSON.stringify({ display_name: next || null }),
      });
      if (!r.ok) throw new Error(String(r.status));
      onSaved(next);
    } catch {
      // Don't keep an optimistic value the server rejected — the whole point of the column is
      // that the name shown IS the name stored.
      setErr(true);
      setDraft(p.display_name);
    } finally {
      setBusy(false);
    }
  };

  if (editing) {
    return (
      <input autoFocus value={draft} disabled={busy}
        onChange={(e) => setDraft(e.target.value)}
        onBlur={commit}
        onKeyDown={(e) => {
          if (e.key === 'Enter') void commit();
          if (e.key === 'Escape') { setDraft(p.display_name); setEditing(false); }
        }}
        placeholder="a name you choose…"
        className="bg-page border border-neutral-700 rounded-lg px-2 py-0.5 text-xs text-fg w-44 focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30" />
    );
  }
  return (
    <button type="button" onClick={() => { setDraft(p.display_name); setEditing(true); }}
      title={p.display_name ? `Chosen name — click to edit. AIRS calls this ${p.name}.`
        : `No chosen name — click to give this model one. AIRS calls it ${p.name}.`}
      className={`text-left rounded px-1 -mx-1 hover:bg-overlay/5 transition-colors ${
        p.display_name ? 'text-fg' : 'text-fg-faint'}`}>
      {p.display_name || '—'}
      {err && <span className="ml-1.5 text-[10px] text-neg-400" title="Save failed — not stored.">failed</span>}
    </button>
  );
}

/** The "is this sound?" launcher.
 *
 * ⚠ IT ONLY OFFERS ITSELF WHERE IT CAN WORK, and the reasons it cannot are DIFFERENT reasons, not
 * one blank. A dead button that opens a modal saying "no data" is worse than no button: it invites
 * a click on every row and answers nothing, and after the third one nobody trusts the column.
 *
 *   cash          no ISIN, no company, no accounts. There is no question to ask.
 *   not in grid   the ISIN is not an instrument we hold (an in-house fund) — nothing to bridge.
 *   a portfolio   a Leonteq certificate that IS another model. It has no accounts of its own;
 *                 the Link column is what looks through it.
 *
 * Everything else gets the button. It may still 404 (GuruFocus does not cover every listing), and
 * the modal says so — that is a real answer about coverage, not a row we could have known about
 * in advance.
 */
function SoundnessCell({ p, onOpen }: {
  p: PositionRow;
  onOpen: (v: { isin: string; fonds: string }) => void;
}) {
  if (!p.isin) {
    return <span className="text-fg-faint" title="Cash has no accounts to read.">—</span>;
  }
  if (!p.known_instrument) {
    return (
      <span className="text-fg-faint"
        title="This ISIN is not an instrument in our grid — usually an in-house fund, which has no listing to resolve and therefore no financials to read.">
        —
      </span>
    );
  }
  return (
    <button type="button" onClick={() => onOpen({ isin: p.isin!, fonds: p.fonds ?? p.isin! })}
      title={`Is ${p.fonds ?? p.isin} fundamentally sound? Price vs fair value, yield, ROIC vs WACC, safety.`}
      className="text-[10px] px-1.5 py-0.5 rounded border border-neutral-700 text-accent-400 hover:bg-overlay/5 transition-colors whitespace-nowrap">
      Sound?
    </button>
  );
}

/** ⚠ `name` IS THE PRETTY NAME, `code` IS AIRS'S. The backend already resolves
 *  `display_name || Portefeuille`, so the dropdown shows the strategy a reader recognises while
 *  the code stays available for looking the row up in AirSPMS. */
type LinkOption = { id: number; name: string; code?: string | null;
                    omschrijving?: string | null; positions: number };
export type LinkCtx = { options: LinkOption[]; excluded_by_isin: Record<string, number[]> };

/** The model portfolio a holding IS.
 *
 * Some positions are not instruments at all — they are other models, wrapped as a Leonteq
 * certificate so they can be held like a security. "Star Selection Index" (CH1381833321) is held
 * by 11 models and IS `StarTopSelectie OFF FX`. Yahoo has no listing for a structured product,
 * so those rows can never be priced directly; the link is what lets us look through.
 *
 * The dropdown offers every model EXCEPT the ones a link to would be a cycle — this portfolio
 * itself, and any portfolio that already holds this position. The confidence badge is on the
 * GUESS only: once a human picks, it is a decision, not an estimate, and showing a number next
 * to it would imply we were still unsure.
 *
 * ⚠ The edit applies to the HOLDING, not to this row: the same certificate in the other ten
 * portfolios that hold it gets the same link. One fact, stored once. */
/** Just the fields the link cell reads. Deliberately NOT `Position`: the ACCOUNT holdings table
 *  carries the same five facts under one different name (`holding_name` rather than `fonds`), and
 *  a cell tied to one table's row type can only be reused by faking the other table's shape. */
export type LinkRow = {
  isin?: string | null;
  fonds?: string | null;
  linked_portfolio_id?: number | null;
  link_source?: string | null;
  link_confidence?: number | null;
  link_reason?: string | null;
};

export function LinkCell({ p, ctx, ownerId, linkBase, onSaved }: {
  p: LinkRow;
  ctx: LinkCtx | null;
  /** Excluded from the dropdown — a portfolio is not its own holding. For an ACCOUNT this is the
   *  model it runs (0 when unpaired, which excludes nothing). */
  ownerId: number;
  /** ⚠ WHICH TABLE IS ASKING, not which row is written. The two screens post to different URLs
   *  — `/model-portfolios/{id}` and `/accounts/{portefeuille}` — but both land on the SAME
   *  `airs_model_portfolio_link` row, because the link is keyed on the holding and not on
   *  (parent, holding). One certificate is the same portfolio wherever it is held, so this is a
   *  routing detail, never a second copy of the fact. */
  linkBase: string;
  onSaved: () => void;
}) {
  const [busy, setBusy] = useState(false);

  // Cash is not a holding, so it cannot be a portfolio.
  if (!p.isin && (p.fonds ?? '').toLowerCase().includes('liquiditeit')) {
    return <td className="px-3 py-1.5 text-fg-faint">—</td>;
  }
  if (!ctx) return <td className="px-3 py-1.5 text-fg-faint">…</td>;

  const banned = new Set(p.isin ? (ctx.excluded_by_isin[p.isin] ?? []) : []);
  const options = ctx.options.filter((o) => o.id !== ownerId && !banned.has(o.id));
  const value = p.linked_portfolio_id ?? '';
  const isGuess = p.link_source === 'auto' && p.linked_portfolio_id != null;
  const conf = p.link_confidence ?? 0;

  // The currently-linked portfolio must ALWAYS have an option to render against — otherwise the
  // native <select> shows a blank instead of its name, and the holdings count in parentheses (the
  // whole point of this cell for a manual link, just as for an auto one) never appears. A linked
  // target could be missing from `options` if it now holds this ISIN (`banned`); surface it anyway
  // so the selection reads correctly.
  const linkedOpt = p.linked_portfolio_id != null
    ? ctx.options.find((o) => o.id === p.linked_portfolio_id)
    : undefined;
  const shown = linkedOpt && !options.some((o) => o.id === linkedOpt.id)
    ? [linkedOpt, ...options]
    : options;
  // The full "Name (count)" — the select clips long *TopSelectie names, so the count also lives
  // in the tooltip where it can never be truncated away.
  const linkedLabel = linkedOpt
    ? `${linkedOpt.name}${linkedOpt.positions ? ` (${linkedOpt.positions})` : ''}`
    : undefined;

  const save = async (raw: string) => {
    setBusy(true);
    try {
      await apiFetch(`${API_URL}${linkBase}/link`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          isin: p.isin ?? null,
          fonds: p.fonds ?? '',
          // '' is the user saying "not a portfolio" — a DECISION, stored as a null, not a
          // reset. Clearing it back to the guess is a separate action (the ↺).
          linked_portfolio_id: raw === '' ? null : Number(raw),
        }),
      });
      onSaved();
    } finally {
      setBusy(false);
    }
  };

  const reset = async () => {
    setBusy(true);
    try {
      await apiFetch(
        `${API_URL}${linkBase}/link` +
        `?isin=${encodeURIComponent(p.isin ?? '')}&fonds=${encodeURIComponent(p.fonds ?? '')}`,
        { method: 'DELETE' },
      );
      onSaved();
    } finally {
      setBusy(false);
    }
  };

  return (
    <td className="px-3 py-1.5 whitespace-nowrap">
      <span className="inline-flex items-center gap-1.5">
        <select
          value={value}
          disabled={busy}
          onChange={(e) => void save(e.target.value)}
          title={linkedLabel}
          className={`bg-page border rounded-lg px-1.5 py-0.5 text-[11px] max-w-[15rem] focus:border-accent-500 disabled:opacity-50 ${
            p.linked_portfolio_id != null
              ? 'border-accent-600/40 text-accent-400'
              : 'border-neutral-800/40 text-fg-faint'
          }`}
        >
          <option value="">— not a portfolio —</option>
          {shown.map((o) => (
            <option key={o.id} value={o.id}
              title={o.code && o.code !== o.name ? `${o.name} — AIRS: ${o.code}` : undefined}>
              {o.name}{o.positions ? ` (${o.positions})` : ''}
            </option>
          ))}
        </select>

        {/* The confidence belongs to the GUESS. A low one is not a worse link — it is a link we
            are not sure about, and the two must not look the same. */}
        {isGuess && (
          <span
            title={`Automatic guess — ${(conf * 100).toFixed(0)}% confidence.${p.link_reason ? ` ${p.link_reason}` : ''} Pick from the dropdown to overrule it.`}
            className={`text-[9px] font-mono px-1 py-0.5 rounded border ${
              conf >= 0.9
                ? 'bg-pos-500/15 text-pos-400 border-pos-500/25'
                : conf >= 0.7
                  ? 'bg-warn-500/15 text-warn-300 border-warn-500/25'
                  : 'bg-neg-500/10 text-neg-300 border-neg-500/25'
            }`}
          >
            {(conf * 100).toFixed(0)}%
          </span>
        )}
        {p.link_source === 'manual' && (
          <button
            type="button"
            onClick={() => void reset()}
            disabled={busy}
            title="Forget this manual choice and fall back to the automatic guess."
            className="text-[10px] text-fg-faint hover:text-accent-400 transition-colors"
          >
            ↺
          </button>
        )}
      </span>
    </td>
  );
}

/** One portfolio's positions, from its AIRS XLS export — the sheet that carries an ISIN.
 *
 * The ISIN is the whole point: it's an EXACT join into `asset_execution`, where the AIRS
 * holdings sheet only ever gave us a fund name ("Alphabet - C", "L` Oreal") that no amount
 * of fuzzy matching resolves safely. */
function Positions({ state, source, onSource, onPickDate, onRefresh, onLinkSaved }: {
  state?: PosState;
  source: 'model' | 'book';
  onSource: (s: 'model' | 'book') => void;
  onPickDate: (datum: string) => void;
  onRefresh: () => void;
  onLinkSaved: () => void;
}) {
  const isBook = source === 'book';
  const pid = state?.data?.portfolio_id;
  const [linkCtx, setLinkCtx] = useState<LinkCtx | null>(null);
  // Which holding's soundness charts are open. Null = none.
  const [fundamentals, setFundamentals] = useState<{ isin: string; fonds: string } | null>(null);
  useEffect(() => {
    if (pid == null) return;
    let alive = true;
    // ⚠ `API_URL`. A bare `/api/...` goes to the Next.js origin, not the backend — it 404s,
    // `.json()` throws, and the cell renders a permanent "…" with nothing in the console to
    // say why. Every apiFetch in this file is absolute for that reason.
    apiFetch(`${API_URL}/api/airs/model-portfolios/${pid}/linkable`)
      .then((r) => r.json())
      .then((j: LinkCtx) => { if (alive) setLinkCtx(j); })
      .catch(() => { if (alive) setLinkCtx(null); });
    return () => { alive = false; };
  }, [pid]);

  if (!state || state.loading) {
    return <p className="text-[11px] text-fg-subtle">Loading positions…</p>;
  }
  if (state.error) {
    return (
      <div className="bg-neg-500/10 border border-neg-500/20 rounded-lg px-3 py-2 text-[11px] text-neg-300">
        {state.error}
      </div>
    );
  }
  const d = state.data;
  if (!d) return null;

  // Model composition (yfinance) vs the paired AIRS book's own holdings. Rendered in both the
  // empty and populated states, so a book with no rows can always be switched back to Model.
  const sourceToggle = (
    <label className="flex items-center gap-1.5 text-[11px] text-fg-muted"
      title="Source: Model = this portfolio's composition, priced from yfinance (per-share closes). Book (AIRS) = the paired AIRS book's ACTUAL holdings, valued by AIRS itself (Beginwaarde / Huidige waarde in EUR, over the calendar year). Different rows — a book holds a different set than the composition it tracks.">
      Source
      <select value={source} aria-label="Positions source"
        onChange={(e) => onSource(e.target.value as 'model' | 'book')}
        className="bg-page border border-neutral-700 rounded-lg px-2 py-1 text-[11px] text-fg focus:border-accent-500">
        <option value="model">Model</option>
        <option value="book">Book (AIRS)</option>
      </select>
    </label>
  );

  if (d.rows.length === 0) {
    return (
      <div className="space-y-2">
        <div className="flex items-center gap-3 text-[11px]">{sourceToggle}</div>
        <p className="text-[11px] text-fg-faint">
          {isBook ? (
            <>No AIRS book is paired with this model, so there are no book holdings to value.
            Pair one on this page, or switch Source back to <span className="font-mono">Model</span>.</>
          ) : (
            <>No fixed-model rows for any of its {d.dates.length} snapshot date(s). AIRS only stores a
            composition for portfolios of type <span className="font-mono">fixed (…)</span> — the{' '}
            <span className="font-mono">meervoudig</span> / <span className="font-mono">normaal</span>{' '}
            ones (benchmarks, multi-model) have none. That is an answer, not a failed fetch.</>
          )}
        </p>
      </div>
    );
  }

  const total = d.rows.reduce((s, r) => s + (r.percentage ?? 0), 0);

  return (
    <div className="space-y-2">
      <div className="flex items-center gap-3 flex-wrap text-[11px]">
        {sourceToggle}
        <span className="text-fg-soft">
          <span className="font-mono text-fg">{d.rows.length}</span>{' '}
          {isBook ? 'book holdings' : 'positions'} ·{' '}
          <span className="text-pos-400 font-mono">{d.matched}</span> matched to our instruments ·{' '}
          <span className={d.unmatched ? 'text-warn-300 font-mono' : 'text-fg-faint font-mono'}>{d.unmatched}</span> not ·{' '}
          <span className="font-mono">{total.toFixed(2)}%</span>{' '}total
          {/* "Since when" is half of what a return means, and the answer is NOT 1 January for
              half the portfolios — it is the date the composition took effect. */}
          {d.ytd_from && (
            <> · marks from{' '}
              <span className="font-mono text-fg" title={isBook
                ? "AIRS's calendar-year window (1 Jan → the snapshot). Weighting the Return (€) column by these START-of-window (Beginwaarde) weights reproduces the book's PRICE return — a hair below its headline cumulatief_rendement, which also includes income."
                : "The YTD window: max(1 Jan, this composition's effective date). Weighting the Return (€) column by these percentages reproduces the portfolio's YTD exactly."}>
                {d.ytd_from}
              </span>
            </>
          )}
          {isBook && d.datum && (
            <> · <SnapshotAge asOf={d.datum} prefix="valued" /></>
          )}
        </span>
        {/* AirSPMS's date dropdown always LEADS with today, which is an empty placeholder —
            so the default is the newest snapshot that actually has rows, not the first one.
            Book holdings are a single DB snapshot, so neither the picker nor Refresh applies. */}
        {!isBook && d.dates.length > 0 && (
          <label className="flex items-center gap-1.5 text-fg-muted">
            Snapshot
            <select value={d.datum ?? ''} onChange={(e) => onPickDate(e.target.value)}
              className="bg-page border border-neutral-700 rounded-lg px-2 py-1 text-[11px] font-mono text-fg focus:border-accent-500">
              {d.dates.map((x) => <option key={x} value={x}>{x}</option>)}
            </select>
          </label>
        )}

        {/* Say it's cached. A cached answer shown as if it were fresh is exactly how a stale
            holding gets trusted — and this one can be minutes or days old. */}
        {isBook ? (
          <span className="ml-auto text-accent-400"
            title="These holdings and values come from the paired AIRS book (airs_holding), read from our DB.">
            AIRS book
          </span>
        ) : (
          <span className="flex items-center gap-1.5 ml-auto">
            {d.cached_at ? (
              <span className="text-fg-faint" title={`Served from our DB, stored ${new Date(d.cached_at).toLocaleString()}. AIRS was not contacted.`}>
                cached <span className="font-mono">{new Date(d.cached_at).toLocaleDateString()}</span>
              </span>
            ) : (
              <span className="text-pos-400" title="Fetched live from AirSPMS just now.">live</span>
            )}
            <button type="button" onClick={onRefresh}
              className="text-[11px] px-2 py-1 rounded-lg hover:bg-overlay/5 text-accent-400 transition-colors">
              Refresh from AIRS
            </button>
          </span>
        )}
      </div>

      <div className="overflow-auto rounded-lg border border-neutral-800/40 max-h-[50vh]">
        <table className="w-full text-xs">
          <thead className="bg-card sticky top-0">
            <tr className="text-fg-faint text-[10px] uppercase tracking-wide border-b border-neutral-800/40">
              {/* Position in the model as AIRS lists it — these rows are not re-sorted here, so
                  the number is stable and can be read back against the XLS export. */}
              <th className="px-3 py-1.5 font-medium text-right w-8">#</th>
              <th className="px-3 py-1.5 font-medium text-left">ISIN</th>
              <th className="px-3 py-1.5 font-medium text-left">Fund</th>
              <th className="px-3 py-1.5 font-medium text-left w-16"
                title="Is this company fundamentally sound, and are we paying a sensible price for it? Four charts off one already-cached GuruFocus blob: price against five independent fair values, what a euro of price buys (FCF / earnings / shareholder yield), ROIC against WACC, and the value-trap screen (Piotroski, Altman, Beneish, interest coverage). The price line is our own daily yfinance close in EUR — never GuruFocus's.">
                Sound?
              </th>
              {/* Some holdings are not instruments — they are other model portfolios, wrapped as
                  a Leonteq certificate. "Star Selection Index" IS StarTopSelectie OFF FX. */}
              <th className="px-3 py-1.5 font-medium text-left" title="The model portfolio this holding IS. A few positions are not instruments at all but other models, held via a Leonteq certificate — those can never be priced directly, so the link is what lets us price them from the model they stand for. The badge is the confidence of our automatic guess; pick from the dropdown to overrule it. An edit applies to the holding everywhere it is held, not just to this row.">
                Link
              </th>
              <th className="px-3 py-1.5 font-medium text-right">Weight</th>
              <th className="px-3 py-1.5 font-medium text-left">Ccy</th>
              <th className="px-3 py-1.5 font-medium text-left">Sector</th>
              <th className="px-3 py-1.5 font-medium text-left">Region</th>
              {/* The arithmetic behind the portfolio's YTD, one holding at a time. Weight these
                  returns and you get the number in the row above, exactly. */}
              <th className="px-3 py-1.5 font-medium text-right" title={isBook
                ? "The POSITION's value in EUR at the start of the year (AIRS Beginwaarde) — not a per-share price. Weighting the Return column by these start values reproduces the book's price return."
                : "The holding's price in EUR when the YTD window opened — its last close on or before that date."}>
                Start (€)
              </th>
              <th className="px-3 py-1.5 font-medium text-left" title={isBook
                ? "The start of AIRS's calendar-year window (1 Jan)."
                : "The date of that close. It can sit a day or two before the window opened (a weekend, a holiday) — it is the last price at which the position was actually marked."}>
                Start date
              </th>
              <th className="px-3 py-1.5 font-medium text-right" title={isBook
                ? "The POSITION's value in EUR at the snapshot (AIRS Huidige waarde) — not a per-share price."
                : "Its latest close, in EUR."}>
                End (€)
              </th>
              <th className="px-3 py-1.5 font-medium text-left" title={isBook
                ? "The date AIRS valued the book (its latest snapshot)."
                : "The date of that close. It LAGS for some holdings — vendors publish unevenly — so these dates are not all the same day, and the stale ones are marked at their last known price."}>
                End date
              </th>
              <th className="px-3 py-1.5 font-medium text-right" title={isBook
                ? "AIRS's own EUR price return for this holding over the year (Huidige waarde / Beginwaarde − 1). Weighting this by the Start-value weights reproduces the book's price return."
                : "Return in EUR from start to end. This is the exact quantity the portfolio's YTD weights together — it carries the FX leg, so a USD holding can rise in dollars and fall here."}>
                Return (€)
              </th>
            </tr>
          </thead>
          <tbody className="divide-y divide-neutral-800/20">
            {d.rows.map((p, i) => (
              <tr key={`${p.isin ?? 'cash'}-${i}`} className="hover:bg-overlay/[0.02]">
                <td className="px-3 py-1.5 text-right font-mono text-fg-faint tabular-nums">{i + 1}</td>
                <td className="px-3 py-1.5 font-mono whitespace-nowrap">
                  {p.isin ? (
                    <span className={p.known_instrument ? 'text-fg' : 'text-warn-300'}>
                      {p.isin}
                      {!p.known_instrument && (
                        <span title="This ISIN is not an instrument in our grid — usually an in-house fund (e.g. High Income Quality fund), which has no listing to resolve."
                          className="ml-2 text-[9px] uppercase tracking-wider font-semibold px-1 py-0.5 rounded border bg-warn-500/15 text-warn-300 border-warn-500/25">
                          not in grid
                        </span>
                      )}
                    </span>
                  ) : (
                    // Cash has no ISIN, and that is correct — not a missing value.
                    <span className="text-fg-faint" title="Cash — no ISIN exists for it.">—</span>
                  )}
                </td>
                <td className="px-3 py-1.5 text-fg-soft">{p.fonds ?? '—'}</td>
                <td className="px-3 py-1.5">
                  <SoundnessCell p={p} onOpen={setFundamentals} />
                </td>
                <LinkCell p={p} ctx={linkCtx} ownerId={d.portfolio_id}
                  linkBase={`/api/airs/model-portfolios/${d.portfolio_id}`}
                  onSaved={onLinkSaved} />
                <td className="px-3 py-1.5 text-right font-mono text-fg">
                  {p.percentage != null ? `${p.percentage.toFixed(2)}%` : '—'}
                  {p.percentage != null && (
                    <Provenance source={isBook ? 'airs_volk' : 'airs_model'} asOf={d.datum}
                      what={isBook
                        ? 'How much of the account this holding was at the start of the year.'
                        : 'How much of this model the holding is meant to be.'}
                      note={isBook ? 'start-of-year value weight' : 'nominal % from the fixed model'}
                      how={isBook
                        ? 'The holding’s Beginwaarde as a share of the book’s total start-of-year value.'
                        : 'The model’s own nominal percentage for this holding, as scraped from the AIRS composition.'} />
                  )}
                </td>
                <td className="px-3 py-1.5 font-mono text-fg-muted">{p.valuta ?? '—'}</td>
                <td className="px-3 py-1.5 text-fg-subtle">{p.sector ?? '—'}</td>
                <td className="px-3 py-1.5 text-fg-subtle">{p.regio ?? '—'}</td>
                <MarkCells p={p} ytdFrom={d.ytd_from} source={source} />
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {fundamentals && (
        <FundamentalsModal isin={fundamentals.isin} fonds={fundamentals.fonds}
          onClose={() => setFundamentals(null)} />
      )}
    </div>
  );
}
