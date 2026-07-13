'use client';

import { Fragment, useEffect, useState } from 'react';
import { apiFetch } from '../../lib/apiFetch';
import { runSSE } from '../../lib/stream';
import { API_URL } from '../../lib/apiUrl';
import type {
  ModelPortfolioPerformance, ModelPortfolioPositions, StoredModelPortfolio,
} from '../../lib/types/api';

type StoredPortfolio = StoredModelPortfolio;

type Portfolio = {
  id: number;
  name: string;
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

/** YTD is a buy-and-hold of the composition WE HOLD — which is the CURRENT one. AIRS keeps
 *  only 2–3 snapshot dates, so January's composition is not recoverable. When the model was
 *  (re)defined DURING the year, applying today's weights back to Jan 1 backtests a basket
 *  chosen knowing how the year went. It flatters, and not subtly: MoTopSelectie_FX shows
 *  +75.85% YTD on a model defined EIGHT DAYS AGO — its return since that model took effect is
 *  +0.86%. So the flag is not a footnote, and `since_model_pct` is the honest number. */
const isHindsight = (p: Perf) => p.model_changed_in_period;

type PosState = { loading: boolean; data?: ModelPortfolioPositions; error?: string };

/** AirSPMS only stores a composition for a `fixed (…)` portfolio. */
const hasFixedModel = (p: Portfolio) => p.fixed?.trim().toLowerCase().startsWith('fixed');

type SortKey = 'name' | 'holdings' | 'ytd' | 'fixed' | 'id';
type Sort = { key: SortKey; dir: 'asc' | 'desc' };

/** Sorting on `holdings` has to answer "where do the un-counted and the model-less rows
 *  go?" — and the answer is: to the bottom, always, in BOTH directions. They aren't small
 *  numbers or large ones; they're absent. Sorting them as if they were 0 would put every
 *  benchmark above a portfolio that genuinely holds one instrument. */
const holdingsRank = (p: Portfolio) =>
  typeof p.holdings === 'number' ? p.holdings : null;

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

  // Cached by default — the scan already downloaded this XLS to count the holdings, so
  // re-scraping AirSPMS on every expand is a several-second wait for data we hold. `refresh`
  // and a historical `datum` both go live.
  const loadPositions = async (id: number, datum?: string, refresh?: boolean) => {
    setPos((p) => ({ ...p, [id]: { loading: true } }));
    try {
      const params = new URLSearchParams();
      if (datum) params.set('datum', datum);
      if (refresh) params.set('refresh', 'true');
      const qs = params.toString() ? `?${params}` : '';
      const r = await apiFetch(`${API_URL}/api/airs/model-portfolios/${id}/positions${qs}`);
      const b = await r.json().catch(() => null);
      if (!r.ok) { setPos((p) => ({ ...p, [id]: { loading: false, error: b?.detail ?? `HTTP ${r.status}` } })); return; }
      setPos((p) => ({ ...p, [id]: { loading: false, data: b as ModelPortfolioPositions } }));
    } catch (e) {
      setPos((p) => ({ ...p, [id]: { loading: false, error: e instanceof Error ? e.message : String(e) } }));
    }
  };

  const toggle = (id: number) => {
    if (open === id) { setOpen(null); return; }
    setOpen(id);
    if (!pos[id]?.data) void loadPositions(id);
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
  const view = (rows ?? [])
    .filter((r) => !needle || `${r.name} ${r.omschrijving}`.toLowerCase().includes(needle))
    .sort((a, b) => {
      const dir = sort.dir === 'asc' ? 1 : -1;
      if (sort.key === 'holdings' || sort.key === 'ytd') {
        const rank = sort.key === 'holdings'
          ? holdingsRank
          : (p: Portfolio) => (p.perf?.ytd_pct ?? null);
        const x = rank(a), y = rank(b);
        // Absent is not a value — a portfolio we cannot price is not "0% YTD", so it sinks
        // in BOTH directions rather than sorting as if it were flat.
        if (x === null && y === null) return a.name.localeCompare(b.name);
        if (x === null) return 1;
        if (y === null) return -1;
        return (x - y) * dir || a.name.localeCompare(b.name);
      }
      if (sort.key === 'id') return (a.id - b.id) * dir;
      const s = sort.key === 'fixed' ? [a.fixed, b.fixed] : [a.name, b.name];
      return s[0].localeCompare(s[1]) * dir;
    });

  const counted = (rows ?? []).filter((r) => typeof r.holdings === 'number');
  const totalHoldings = counted.reduce((s, r) => s + (r.holdings ?? 0), 0);

  // Tailwind scans for LITERAL class strings, so `text-${align}` would only ever work by
  // accident (when the same literal happens to appear elsewhere in the file). Full strings.
  const th = (key: SortKey, label: string, align: 'text-left' | 'text-right' = 'text-left') => (
    <th className={`px-3 py-1.5 font-medium ${align}`}>
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
            AIRS model portfolios{rows ? ` · ${view.length}/${rows.length}` : ''}
          </h3>
          <p className="text-[11px] text-fg-faint mt-0.5">
            Stamgegevens › Onderhoud portefeuilles › Model portefeuilles
            {counted.length > 0 && (
              <> · <span className="font-mono text-fg-subtle">{totalHoldings}</span> positions
                across <span className="font-mono text-fg-subtle">{counted.length}</span> counted
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
            <input value={q} onChange={(e) => setQ(e.target.value)} placeholder="Search name / description…"
              className="bg-page border border-neutral-700 rounded-lg px-3 py-1.5 text-xs text-fg focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 w-60" />
          )}
          <button type="button" onClick={() => void scan()} disabled={scanning}
            className="text-sm px-4 py-2 rounded-lg bg-accent-600 hover:bg-accent-500 text-white disabled:opacity-40 disabled:cursor-not-allowed transition-colors">
            {scanning ? 'Scanning…' : rows ? 'Rescan' : 'Scan AIRS'}
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

      {/* The ⚠ is on half the rows, so it needs to mean something specific and stated. */}
      {(rows ?? []).some((r) => r.perf && isHindsight(r.perf)) && (
        <p className="text-[11px] text-fg-subtle leading-relaxed">
          <span className="text-warn-400">⚠</span> YTD is a buy-and-hold of the model&apos;s{' '}
          <em>current</em> composition — AIRS keeps no January snapshot to recover. Where the
          model was (re)defined <em>during</em> the year, that makes the figure a{' '}
          <strong>backtest of weights chosen with hindsight</strong>, not a track record
          (MoTopSelectie_FX reads +75.85% on a model defined 8 days ago; since it took effect
          it has made +0.86%). Hover any ⚠ for that portfolio&apos;s real number.{' '}
          <span className="text-fg-faint">~</span> marks partial price coverage.
        </p>
      )}

      {rows && rows.length > 0 && (
        <div className="overflow-auto rounded-lg border border-neutral-800/40 max-h-[70vh]">
          <table className="w-full text-xs">
            <thead className="bg-card sticky top-0 z-10">
              <tr className="group text-fg-faint text-[10px] uppercase tracking-wide border-b border-neutral-800/40">
                {th('name', 'Portfolio')}
                <th className="px-3 py-1.5 font-medium text-left">Description</th>
                {th('holdings', 'Holdings', 'text-right')}
                {th('ytd', 'YTD (€)', 'text-right')}
                {th('fixed', 'Type')}
                <th className="px-3 py-1.5 font-medium text-left">Fixed date</th>
                {th('id', 'AIRS id', 'text-right')}
              </tr>
            </thead>
            <tbody className="divide-y divide-neutral-800/20">
              {view.map((r) => (
                <Fragment key={r.id}>
                <tr onClick={() => toggle(r.id)}
                  className="hover:bg-accent-500/10 transition-colors cursor-pointer">
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
                  <td className="px-3 py-1.5 text-right font-mono whitespace-nowrap">
                    <HoldingsCell p={r} />
                  </td>
                  <td className="px-3 py-1.5 text-right font-mono whitespace-nowrap">
                    <YtdCell p={r} />
                  </td>
                  <td className="px-3 py-1.5 text-fg-subtle whitespace-nowrap">{r.fixed || '—'}</td>
                  <td className="px-3 py-1.5 font-mono text-fg-subtle whitespace-nowrap">{r.fixed_datum || '—'}</td>
                  <td className="px-3 py-1.5 text-right font-mono text-fg-faint">{r.id}</td>
                </tr>
                {open === r.id && (
                  <tr>
                    <td colSpan={7} className="px-3 py-3 bg-inset">
                      <Positions
                        state={pos[r.id]}
                        onPickDate={(d) => void loadPositions(r.id, d)}
                        onRefresh={() => void loadPositions(r.id, undefined, true)}
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
    </section>
  );
}

/** The YTD cell. The number is only half the story, so the cell carries the other half.
 *
 *   n/a         — under 60% of the model's weight is priceable (structured products, in-house
 *                 funds). We return NOTHING rather than renormalise 1% of a portfolio up to
 *                 100% and print it to two decimals. That actually happened: TOPS_OFF_BEH
 *                 read "+0.00%", which was its cash line, alone.
 *   ⚠ (amber)   — the model was (re)defined DURING the year, so this YTD is a BACKTEST of
 *                 today's weights, not what the portfolio earned. Hover for the real number.
 *   plain       — the model predates Jan 1: it held these weights all year, so this IS its
 *                 return.
 */
function YtdCell({ p }: { p: Portfolio }) {
  const f = p.perf;
  if (!f) return <span className="text-fg-faint" title="Not computed yet.">…</span>;

  if (f.ytd_pct == null) {
    return (
      <span title={`Only ${(f.covered_pct ?? 0).toFixed(0)}% of this model's weight can be priced (${f.unpriced_holdings} holding(s) have no price series — typically Leonteq structured products or in-house funds). A return renormalised over the rest would be an invention, so none is shown.`}
        className="text-[9px] uppercase tracking-wider font-semibold px-1 py-0.5 rounded border bg-neutral-500/10 text-fg-faint border-neutral-600/30">
        n/a
      </span>
    );
  }

  const v = f.ytd_pct;
  const colour = v >= 0 ? 'text-pos-400' : 'text-neg-400';
  const hint = isHindsight(f)
    ? `⚠ BACKTEST, not a track record. This model's weights took effect ${f.model_effective} — DURING the year — so applying them back to 1 Jan uses a basket chosen with hindsight. Its return since the model actually took effect is ${f.since_model_pct?.toFixed(2)}%.`
    : `Real: this model has held these weights since ${f.model_effective}, before the year began.`;
  const cov = f.partial_coverage
    ? ` Only ${(f.covered_pct ?? 0).toFixed(0)}% of its weight is priceable; the rest is assumed to have behaved the same.`
    : '';

  return (
    <span title={hint + cov} className="inline-flex items-center gap-1">
      {isHindsight(f) && <span className="text-warn-400" aria-label="backtest">⚠</span>}
      {f.partial_coverage && <span className="text-fg-faint text-[9px]">~</span>}
      <span className={colour}>{v >= 0 ? '+' : ''}{v.toFixed(2)}%</span>
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

/** One portfolio's positions, from its AIRS XLS export — the sheet that carries an ISIN.
 *
 * The ISIN is the whole point: it's an EXACT join into `asset_execution`, where the AIRS
 * holdings sheet only ever gave us a fund name ("Alphabet - C", "L` Oreal") that no amount
 * of fuzzy matching resolves safely. */
function Positions({ state, onPickDate, onRefresh }: {
  state?: PosState;
  onPickDate: (datum: string) => void;
  onRefresh: () => void;
}) {
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

  if (d.rows.length === 0) {
    return (
      <p className="text-[11px] text-fg-faint">
        No fixed-model rows for any of its {d.dates.length} snapshot date(s). AIRS only stores a
        composition for portfolios of type <span className="font-mono">fixed (…)</span> — the{' '}
        <span className="font-mono">meervoudig</span> / <span className="font-mono">normaal</span>{' '}
        ones (benchmarks, multi-model) have none. That is an answer, not a failed fetch.
      </p>
    );
  }

  const total = d.rows.reduce((s, r) => s + (r.percentage ?? 0), 0);

  return (
    <div className="space-y-2">
      <div className="flex items-center gap-3 flex-wrap text-[11px]">
        <span className="text-fg-soft">
          <span className="font-mono text-fg">{d.rows.length}</span> positions ·{' '}
          <span className="text-pos-400 font-mono">{d.matched}</span> matched to our instruments ·{' '}
          <span className={d.unmatched ? 'text-warn-300 font-mono' : 'text-fg-faint font-mono'}>{d.unmatched}</span> not ·{' '}
          <span className="font-mono">{total.toFixed(2)}%</span> total
        </span>
        {/* AirSPMS's date dropdown always LEADS with today, which is an empty placeholder —
            so the default is the newest snapshot that actually has rows, not the first one. */}
        <label className="flex items-center gap-1.5 text-fg-muted">
          Snapshot
          <select value={d.datum ?? ''} onChange={(e) => onPickDate(e.target.value)}
            className="bg-page border border-neutral-700 rounded-lg px-2 py-1 text-[11px] font-mono text-fg focus:border-accent-500">
            {d.dates.map((x) => <option key={x} value={x}>{x}</option>)}
          </select>
        </label>

        {/* Say it's cached. A cached answer shown as if it were fresh is exactly how a stale
            holding gets trusted — and this one can be minutes or days old. */}
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
      </div>

      <div className="overflow-auto rounded-lg border border-neutral-800/40 max-h-[50vh]">
        <table className="w-full text-xs">
          <thead className="bg-card sticky top-0">
            <tr className="text-fg-faint text-[10px] uppercase tracking-wide border-b border-neutral-800/40">
              <th className="px-3 py-1.5 font-medium text-left">ISIN</th>
              <th className="px-3 py-1.5 font-medium text-left">Fund</th>
              <th className="px-3 py-1.5 font-medium text-right">Weight</th>
              <th className="px-3 py-1.5 font-medium text-left">Ccy</th>
              <th className="px-3 py-1.5 font-medium text-left">Sector</th>
              <th className="px-3 py-1.5 font-medium text-left">Region</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-neutral-800/20">
            {d.rows.map((p, i) => (
              <tr key={`${p.isin ?? 'cash'}-${i}`} className="hover:bg-overlay/[0.02]">
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
                <td className="px-3 py-1.5 text-right font-mono text-fg">
                  {p.percentage != null ? `${p.percentage.toFixed(2)}%` : '—'}
                </td>
                <td className="px-3 py-1.5 font-mono text-fg-muted">{p.valuta ?? '—'}</td>
                <td className="px-3 py-1.5 text-fg-subtle">{p.sector ?? '—'}</td>
                <td className="px-3 py-1.5 text-fg-subtle">{p.regio ?? '—'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
