'use client';

import { Fragment, useEffect, useState } from 'react';
import { apiFetch } from '../../lib/apiFetch';
import { API_URL } from '../../lib/apiUrl';
import { dialog } from '../../lib/dialog';
import { useIsAdmin } from '../../lib/hooks/useEffectiveRole';
import { Provenance } from '../../lib/provenance';
import type { ReconstructedIndex } from '../../lib/types/api';

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

/** A price in its listing's own currency — NO symbol, because the Ccy column names the unit and a
 *  "€" glued to a JPY close is the kind of wrong that still looks like a number. Two decimals
 *  everywhere so the column aligns; a ¥8,000 close and a €46.75 one read on the same scale. */
const price = (v: number | null | undefined) =>
  v == null ? '—' : v.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });

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
/** `POST /api/benchmarks/index/{label}/fill` — the fields the button reads. */
type FillResult = {
  label: string; universe_members: number; usable: number; needs_resolve: number;
  needs_cap: number; no_isin: number; no_isin_names?: string[];
  queued: number; skipped_existing: number; capped: number; note?: string | null;
  /** The template was run because no universe existed — AEX had none at all. */
  universe_built?: boolean;
  /** Prices re-fetched this press, and how many constituents still have no mark in the window.
   *  `price_pending` is not a failure — a press re-prices a bounded slice on purpose. */
  repriced?: number; price_pending?: number; price_failed?: number;
};

/** One sentence saying what the press actually achieved — and, when nothing was queued, WHY.
 *
 * ⚠ IT NEVER SAYS "DONE". Resolution is handed to a single paced worker precisely so a second
 * Yahoo consumer cannot corrupt it (an overloaded caller gets an empty result, not a 429), so the
 * members count will not move on the next load. A button that implied otherwise would be pressed
 * again, and again, each press queueing nothing and looking broken. */
function fillSummary(f: FillResult): string {
  if (f.note) return f.note;
  const bits: string[] = [];
  if (f.universe_built) bits.push('built the universe from its template');
  if (f.queued) bits.push(`${f.queued} queued for ingest (a paced worker drains them — minutes to hours)`);
  if (f.capped) bits.push(`${f.capped} market caps written`);
  if (f.repriced) bits.push(`${f.repriced} price series refilled`);
  // ⚠ SAID EVERY TIME IT IS NON-ZERO. A press re-prices a bounded slice, so silence here would
  // read as "finished" over a benchmark still missing 1,600 windows.
  if (f.price_pending) bits.push(`${f.price_pending} still to refill — press again, or the 06:00 tick clears them`);
  if (f.price_failed) bits.push(`${f.price_failed} could not be repriced (see the console)`);
  if (f.skipped_existing) bits.push(`${f.skipped_existing} already queued or ingested`);
  if (!bits.length) {
    // ⚠ ZERO MEMBERS IS NOT "EVERYTHING IS FINE". `usable === universe_members` is vacuously true
    // at 0 === 0, so an EMPTY universe reported "every constituent is already priced and
    // weighted" — the most reassuring possible sentence about a benchmark with nothing in it.
    // Measured on AEX, whose universe did not exist at all.
    if (!f.universe_members) return 'The universe is empty — nothing to price. Its template produced no members.';
    if (f.usable === f.universe_members) return 'Nothing to do — every constituent is already priced and weighted.';
    if (f.no_isin) return `Nothing this can fix: ${f.no_isin} of ${f.universe_members} members have no ISIN, which is the only bridge into the price world.`;
    return 'Nothing to do.';
  }
  let s = `${bits.join(', ')}.`;
  if (f.no_isin) s += ` ⚠ ${f.no_isin} members have no ISIN and can never be reached from here.`;
  return s;
}

export default function BenchmarksPanel() {
  const isAdmin = useIsAdmin();
  const [data, setData] = useState<Record<string, ReconstructedIndex>>({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [open, setOpen] = useState<string | null>(null);
  const [filling, setFilling] = useState<Set<string>>(new Set());
  const [fillMsg, setFillMsg] = useState<{ text: string; kind: 'info' | 'warn' } | null>(null);
  const [deleting, setDeleting] = useState<string | null>(null);
  // ⚠ THE TABLE HAS TO RE-READ, OR THE WHOLE POINT IS LOST. Delete → Fill is a loop you watch:
  // without a reload the row keeps showing the members it had before you deleted them, which reads
  // as the button having done nothing.
  const [reloadKey, setReloadKey] = useState(0);
  const reload = () => setReloadKey((k) => k + 1);

  /** Fill one label, or every label in sequence.
   *
   * ⚠ SEQUENTIALLY, NEVER `Promise.all`. Each fill issues batched Yahoo quotes for market caps;
   * three at once is three concurrent consumers on the throttle, which is the failure this whole
   * pipeline is arranged to avoid. */
  const fill = async (labels: string[]) => {
    setFilling(new Set(labels));
    setFillMsg({ text: `Filling ${labels.join(', ')}…`, kind: 'info' });
    const lines: string[] = [];
    try {
      for (const label of labels) {
        const r = await apiFetch(`${API_URL}/api/benchmarks/index/${label}/fill`, { method: 'POST' });
        if (!r.ok) { lines.push(`${label}: failed — HTTP ${r.status}`); continue; }
        lines.push(`${label}: ${fillSummary((await r.json()) as FillResult)}`);
      }
      setFillMsg({ text: lines.join('  ·  '), kind: 'warn' });
      // Caps are written inline, and a rebuilt universe lands immediately — both are visible now.
      // (The RESOLVES are not: they went to the paced worker, which is what the amber says.)
      reload();
    } catch (e) {
      setFillMsg({ text: e instanceof Error ? e.message : String(e), kind: 'warn' });
    } finally {
      setFilling(new Set());
    }
  };

  /**
   * Delete the live universe behind one benchmark, so Fill can be watched rebuilding it.
   *
   * ⚠ THE CONFIRM NAMES WHAT SURVIVES, NOT JUST WHAT GOES. "Delete SP500?" invites the reading
   * that the prices and the market caps go with it; they do not, and knowing that is the
   * difference between trying this and not daring to.
   */
  const del = async (label: string, members?: number) => {
    const ok = await dialog.confirm(
      `Reset ${label}?\n\n`
      + `Deletes all three things Fill puts in place, so the whole button can be tested:\n`
      + `  • its ${members ?? ''} membership rows\n`
      + '  • its constituents’ market caps\n'
      + '  • their closes from mid-November onward (the start-of-year mark and everything since)\n\n'
      + 'The asset grid, the Yahoo symbol and the older history stay — so Fill re-fetches prices '
      + 'for a KNOWN listing and nothing is ever re-resolved.\n\n'
      + '⚠ Prices are shared: some of these constituents are also held in AIRS books, and '
      + 'those portfolio figures will read short until the prices are refilled. One press of Fill '
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
        setFillMsg({ text: b?.detail ?? `${label}: delete failed — HTTP ${r.status}`, kind: 'warn' });
        return;
      }
      setFillMsg({
        text: b?.deleted
          ? `${label}: reset — ${b.members_deleted ?? 0} members, ${b.caps_cleared ?? 0} market `
            + `caps and ${(b.price_rows_deleted ?? 0).toLocaleString('en-US')} closes from `
            + `${b.prices_from ?? 'the window open'}. Press Fill to rebuild all three.`
          : b?.note ?? `${label}: nothing to delete.`,
        kind: 'warn',
      });
      reload();
    } catch (e) {
      console.warn(`[benchmarks] delete ${label} threw`, e);
      setFillMsg({ text: e instanceof Error ? e.message : String(e), kind: 'warn' });
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
      setData(out);
      if (Object.keys(out).length === 0) setError(errs.join('; ') || 'Failed to load');
      setLoading(false);
    })();
    return () => { cancelled = true; };
  }, [reloadKey]);

  return (
    <section className="bg-card border border-neutral-800/40 rounded-xl p-5 space-y-3">
      <div className="flex items-start gap-3 flex-wrap">
        <div>
          <h3 className="text-sm font-semibold text-fg-strong">Benchmarks</h3>
          <p className="text-[11px] text-fg-faint mt-0.5"
            title="Rebuilt from our own membership, prices and FX — the same basis a portfolio is measured on, which is what makes the two comparable.">
            Cap-weighted, rebuilt from our own constituents.
          </p>
        </div>
        {/* Ingesting constituents is admin work — hidden rather than left to 403. The index itself
            reads the same for everyone. */}
        {isAdmin && (
          <button type="button" onClick={() => void fill(INDICES.map((i) => i.label))}
            disabled={filling.size > 0}
            title="For each index: queue its un-ingested constituents for the price worker and write market caps for the ones already resolved. Runs one index at a time — concurrent Yahoo callers are how a constituent lands on the wrong listing."
            className="ml-auto text-[11px] px-2.5 py-1 rounded-lg bg-accent-600 hover:bg-accent-500 text-white disabled:opacity-50">
            {filling.size > 1 ? 'Filling…' : 'Fill all'}
          </button>
        )}
      </div>

      {fillMsg && (
        // ⚠ AMBER, NOT GREEN. The work is handed to a paced background worker, so this is a
        // receipt for what was QUEUED — the table above will not change on the next load.
        <div className={`text-[11px] rounded-lg px-3 py-1.5 border ${
          fillMsg.kind === 'warn'
            ? 'text-warn-300 bg-warn-500/10 border-warn-500/20'
            : 'text-fg-subtle bg-overlay/[0.03] border-neutral-800/40'}`}>
          {fillMsg.text}
        </div>
      )}

      {loading && <p className="text-xs text-fg-subtle">Computing…</p>}
      {error && (
        <div className="bg-neg-500/10 border border-neg-500/20 rounded-lg px-3 py-2 text-xs text-neg-300">{error}</div>
      )}

      {!loading && !error && (
        <div className="overflow-auto rounded-lg border border-neutral-800/40">
          <table className="w-full text-xs">
            <thead className="bg-card">
              <tr className="text-fg-faint text-[10px] uppercase tracking-wide border-b border-neutral-800/40">
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
                        {/* ⚠ `stopPropagation` — the whole row is the expand toggle, and a Fill
                            that also opened the detail would look like it had rendered a result. */}
                        {isAdmin && (
                          <div className="inline-flex items-center gap-1">
                            <button type="button" disabled={filling.size > 0 || deleting != null}
                              onClick={(e) => { e.stopPropagation(); void fill([ix.label]); }}
                              title={`Queue ${ix.name}'s un-ingested constituents for the price worker and write market caps for the ones already resolved.`}
                              className="text-[11px] px-2 py-0.5 rounded-lg border border-neutral-700 text-fg-muted hover:bg-overlay/5 disabled:opacity-50">
                              {filling.has(ix.label) && filling.size === 1 ? 'Filling…' : 'Fill'}
                            </button>
                            {/* Only where Fill can put it back — see `rebuildable`. */}
                            {ix.rebuildable && (
                              <button type="button" disabled={filling.size > 0 || deleting != null}
                                onClick={(e) => { e.stopPropagation(); void del(ix.label, d?.member_count); }}
                                title={`Delete the ${ix.name} universe so Fill can be watched rebuilding it. Membership only — prices and market caps are untouched.`}
                                aria-label={`Delete the ${ix.name} universe`}
                                className="text-[11px] px-2 py-0.5 rounded-lg border border-neutral-800/40 text-fg-faint hover:text-neg-400 hover:border-neg-500/40 disabled:opacity-50">
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
                          <IndexDetail d={d} />
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

function IndexDetail({ d }: { d: ReconstructedIndex }) {
  const [q, setQ] = useState('');
  const needle = q.trim().toLowerCase();
  const members = (d.members ?? []).filter((m) =>
    !needle || `${m.company_name ?? ''} ${m.ticker ?? ''}`.toLowerCase().includes(needle));

  return (
    <div className="space-y-2">
      <div className="flex items-center gap-3 flex-wrap text-[11px]">
        <span className="text-fg-soft">
          <span className="font-mono text-fg">{d.priced_of_universe}</span> priced ·{' '}
          weights as of <span className="font-mono">{d.start_date}</span>
        </span>
        <input value={q} onChange={(e) => setQ(e.target.value)} placeholder="Search constituent…"
          className="bg-page border border-neutral-700 rounded-lg px-3 py-1 text-[11px] text-fg focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 w-52 ml-auto" />
      </div>

      {/* A corrected price is a CLAIM. Show it — never adjust silently. */}
      {(d.split_adjusted?.length ?? 0) > 0 && (
        <div className="bg-warn-500/10 border border-warn-500/20 rounded-lg px-3 py-2 text-[11px] text-warn-300">
          <span className="font-semibold">Split-adjusted on the fly:</span>{' '}
          {d.split_adjusted!.map((s) => `${s.ticker} ×${s.factor.toFixed(3)}`).join(' · ')}.
          Our stored closes are not split-adjusted and cannot self-heal (the ingest only
          fetches dates newer than what we hold), so these series were rescaled here. Left
          raw, KLA alone would read −80% and take a weight it never had.
        </div>
      )}

      <div className="overflow-auto rounded-lg border border-neutral-800/40 max-h-[50vh]">
        <table className="w-full text-xs">
          <thead className="bg-card sticky top-0">
            <tr className="text-fg-faint text-[10px] uppercase tracking-wide border-b border-neutral-800/40">
              <th className="px-3 py-1.5 font-medium text-left">Company</th>
              <th className="px-3 py-1.5 font-medium text-left">Ticker</th>
              {/* ⚠ THE CURRENCY IS THE LISTING'S, AND IT IS WHAT MAKES THE TWO YTD COLUMNS
                  DIFFERENT. Without it "YTD (local)" is a number in an unnamed unit, and the gap
                  to "YTD (€)" — the FX leg — reads as an error rather than as the exchange rate. */}
              <th className="px-3 py-1.5 font-medium text-left">Ccy</th>
              <th className="px-3 py-1.5 font-medium text-right">Weight</th>
              {/* The arithmetic behind YTD (local): these two, in the listing's own currency.
                  Their ratio IS that column, which is why they sit immediately before it. */}
              <th className="px-3 py-1.5 font-medium text-right">Start</th>
              <th className="px-3 py-1.5 font-medium text-right">Now</th>
              <th className="px-3 py-1.5 font-medium text-right">YTD (local)</th>
              <th className="px-3 py-1.5 font-medium text-right">YTD (€)</th>
              <th className="px-3 py-1.5 font-medium text-right">Mkt cap (€bn)</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-neutral-800/20">
            {members.map((m) => (
              <tr key={m.company_id} className="hover:bg-overlay/[0.02]">
                <td className="px-3 py-1.5 text-fg-soft">{m.company_name ?? '—'}</td>
                <td className="px-3 py-1.5 font-mono text-fg-muted">{m.ticker ?? '—'}</td>
                <td className="px-3 py-1.5 font-mono text-fg-subtle">{m.currency ?? '—'}</td>
                <td className="px-3 py-1.5 text-right font-mono text-fg">{m.weight_pct.toFixed(2)}%</td>
                {/* ⚠ EACH MARK CARRIES ITS OWN DATE IN THE TOOLTIP, AND THEY ARE NOT THE SAME
                    ACROSS ROWS. The opening mark is the last close ON OR BEFORE 1 January — 31
                    December for most, earlier for a thin line — and the closing mark is that
                    instrument's own latest close, so a name whose vendor lags a day is marked a
                    day back. Two prices without their dates cannot be checked against anything. */}
                <td className="px-3 py-1.5 text-right font-mono text-fg-muted"
                  title={`Opening mark: the last close on or before the window opened (${m.start_date}).`}>
                  {price(m.start_price)}
                </td>
                {/* ⚠ THE ICON IS PER ROW, NOT ON THE HEADER, BECAUSE THE ANSWER IS PER ROW. Every
                    other cell in this column shares a definition; this one does not share a DATE.
                    Each constituent is marked at its own latest close, so a Tokyo line sits a day
                    behind a Paris one on any given afternoon, and a dormant listing can sit weeks
                    behind — which is the difference between "this stock fell" and "we stopped
                    hearing about it". A header tooltip can state the rule; only the cell can say
                    which day this number is from. `asOf` also turns the icon amber when that date
                    is genuinely stale, so the rows worth doubting mark themselves. */}
                <td className="px-3 py-1.5 text-right font-mono text-fg-soft whitespace-nowrap">
                  {price(m.end_price)}
                  <Provenance source="benchmark" asOf={m.end_date} kind="copied"
                    what={`What ${m.company_name ?? 'this constituent'} last closed at, in ${m.currency ?? 'its own currency'}.`}
                    note={`close on ${m.end_date} — this listing's own latest, not the fleet's`}
                    how={`the newest bar we hold for ${m.ticker ?? 'this listing'}. It is the closing mark of the YTD window: ${price(m.start_price)} on ${m.start_date} to ${price(m.end_price)} on ${m.end_date} is the ${pct(m.return_local_pct)} beside it. The euro column applies the FX rate of each of those two days, so it is a different number and not a conversion of this one.`} />
                </td>
                <td className={`px-3 py-1.5 text-right font-mono ${tone(m.return_local_pct)}`}>
                  {pct(m.return_local_pct)}
                </td>
                <td className={`px-3 py-1.5 text-right font-mono ${tone(m.return_eur_pct)}`}>
                  {pct(m.return_eur_pct)}
                </td>
                <td className="px-3 py-1.5 text-right font-mono text-fg-subtle">
                  {(m.market_cap_eur / 1e9).toFixed(1)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <p className="text-[10px] text-fg-faint leading-relaxed">
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
