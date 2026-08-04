'use client';

import { useEffect, useMemo, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { chartTheme } from '../../../lib/chartTheme';
import { guruFocusUrl } from '../../../lib/gurufocusUrl';
import { xToPeriod } from './marginData';

/**
 * Everything behind one growth chart: the SERIES AS PLOTTED, then the raw per-company figures both
 * of its lines were built from.
 *
 * ⚠ THE PLOTTED SERIES IS SHOWN FIRST BECAUSE IT IS NOWHERE IN THE TABLE UNDER IT. A portfolio's
 * line is a blended growth INDEX (each holding rebased to 100 at its first year, then weighted)
 * and the benchmark's is that index scaled onto ours — neither number appears in a table of
 * reported revenues, so "click the chart to see the data" opened a table that could not be
 * reconciled with the line that was clicked. Both are here now, in the units the chart drew.
 *
 * ⚠ THE BENCHMARK'S CONSTITUENTS LOAD ON DEMAND, and only when a benchmark is active. Same
 * endpoint, same table — `{holdings|portfolio_id}` swapped for `{universe}` — but the S&P is ~490
 * rows against a book's 20, and fetching it on every open would make the common case pay for the
 * rare one.
 */

type Row = {
  isin: string; name: string; weight_pct: number; currency: string | null;
  ticker: string | null; exchange: string | null;
  status: 'ok' | 'unsubscribed' | 'no_data'; revenue: Record<string, number | null>;
  /** INDEX ROWS ONLY — the numerator the weight beside it was divided out of (cap ÷ Σcap).
   *  Absent on a portfolio, where the weight is a holding weight and no cap is involved. */
  market_cap_eur?: number | null;
};
/** Universe requests only: how the weights were arrived at, and who fell out. See the backend's
 *  `weight_basis` — the names it lists are NOT in the index at any weight. */
type WeightBasis = {
  members: number; weighted: number; excluded: { name: string | null; reason: string }[];
};
type Resp = { years: string[]; rows: Row[]; holdings: number; weight_basis?: WeightBasis };
export type Target = {
  portfolio_id?: number;
  holdings?: { isin: string; name?: string; weight: number }[];
  /** Set INSTEAD of the two above to read an index's constituents — see `benchSeries`. */
  universe?: string;
  /** ⚠ `'daily'` IS A REAL THIRD VALUE, not a typo — the two yield cards send it (a daily market
   *  cap off the daily close). Narrowing this to the tab's two would reject them at the type
   *  level, so it stays a string here and the tab-level toggle owns the other two. */
  cadence?: string;
};

/** One point of the line as the chart drew it. `bench` is already rebased (see `rebaseOnto`). */
export type PlottedPoint = {
  year: number; value: number | null; trend?: number | null; bench?: number | null;
};

// Sort key is a fixed column ('name'|'weight'|'ccy') OR a year string ('2018').
function cmp(a: number | string | null | undefined, b: number | string | null | undefined, dir: 'asc' | 'desc') {
  if (a == null && b == null) return 0;
  if (a == null) return 1;        // nulls last, both directions
  if (b == null) return -1;
  const r = (typeof a === 'string' || typeof b === 'string') ? String(a).localeCompare(String(b)) : a - b;
  return dir === 'desc' ? -r : r;
}

/**
 * The per-company matrix — one row per constituent, one column per period.
 *
 * ⚠ ONE COMPONENT FOR THE BOOK AND FOR THE INDEX. They are the same payload from the same
 * endpoint, and a second copy for the benchmark is how the two tables come to format a number, or
 * sort a null, differently — on a screen whose whole purpose is comparing them. Only the ingest
 * action differs, so it arrives as an optional callback rather than as a separate table.
 */
function MatrixTable({ data, fmt, noun, onFetch }: {
  data: Resp;
  fmt: (v: number | null | undefined) => string;
  noun: string;
  /** Holdings only: fetch a `no_data` company's financials. Absent ⇒ the cell states the gap,
   *  which is right for an index nobody is curating row by row. */
  onFetch?: (isin: string, name: string) => Promise<void>;
}) {
  const [sort, setSort] = useState<{ key: string; dir: 'asc' | 'desc' }>({ key: 'weight', dir: 'desc' });
  const [ingest, setIngest] = useState<Record<string, { busy?: boolean; msg?: string }>>({});
  /** Only the index carries a cap — see the row type. A column of dashes on a portfolio would
   *  imply the caps are missing rather than inapplicable. */
  const hasCap = data.rows.some((r) => r.market_cap_eur != null);
  /** €bn, because a raw 628076627000 beside a 51.76% is unreadable and the point of the column is
   *  that the division can be checked by eye. */
  const capBn = (v: number | null | undefined) => (
    v == null ? '—' : `${(v / 1e9).toLocaleString('en-US', { maximumFractionDigits: 1 })}`);
  const toggle = (key: string) => setSort((s) => (s.key === key
    ? { key, dir: s.dir === 'desc' ? 'asc' : 'desc' }
    // Names/currency read A→Z first; weight, cap and figures read biggest-first.
    : { key, dir: (key === 'name' || key === 'ccy') ? 'asc' : 'desc' }));
  const caret = (k: string) => (sort.key === k ? (sort.dir === 'desc' ? ' ▾' : ' ▴') : '');

  const rows = useMemo(() => {
    const get = (r: Row): number | string | null | undefined => (
      sort.key === 'name' ? r.name.toLowerCase()
        : sort.key === 'exchange' ? (r.exchange ?? '')
          : sort.key === 'ticker' ? (r.ticker ?? '')
            : sort.key === 'weight' ? r.weight_pct
              : sort.key === 'cap' ? (r.market_cap_eur ?? null)
                : sort.key === 'ccy' ? (r.currency ?? '')
                  : r.revenue[sort.key]);      // a period column
    return [...data.rows].sort((a, b) => cmp(get(a), get(b), sort.dir));
  }, [data, sort]);

  const fetchOne = async (isin: string, name: string) => {
    if (!onFetch) return;
    setIngest((s) => ({ ...s, [isin]: { busy: true } }));
    try {
      await onFetch(isin, name);
      setIngest((s) => ({ ...s, [isin]: {} }));
    } catch (e) {
      // ⚠ THE REASON STAYS ON THE ROW. A fetch that loaded financials carrying no income statement
      // is a real answer; swallowing it would read as "nothing happened".
      setIngest((s) => ({ ...s, [isin]: { msg: e instanceof Error ? e.message : String(e) } }));
    }
  };

  return (
    <div className="overflow-auto rounded-lg border border-neutral-800/40">
      <table className="w-full text-xs">
        <thead className="bg-page">
          <tr className="text-fg-faint text-[10px] uppercase tracking-wide border-b border-neutral-800/40 [&>th]:cursor-pointer [&>th]:select-none [&>th:hover]:text-fg-soft">
            {/* Company takes the slack so the table fills the width; periods keep natural size. */}
            <th className="px-3 py-1.5 font-medium text-left sticky left-0 bg-page z-10 w-full" onClick={() => toggle('name')}>Company{caret('name')}</th>
            <th className="px-3 py-1.5 font-medium text-left whitespace-nowrap" onClick={() => toggle('exchange')}>GF exch{caret('exchange')}</th>
            <th className="px-3 py-1.5 font-medium text-left whitespace-nowrap" onClick={() => toggle('ticker')}>Ticker{caret('ticker')}</th>
            {hasCap && (
              <th className="px-3 py-1.5 font-medium text-right whitespace-nowrap" onClick={() => toggle('cap')}
                title="company.market_cap_eur as stored today — full cap, not free-float. This is the numerator of the Weight beside it: cap ÷ the total of this column.">
                Mkt cap €bn{caret('cap')}
              </th>
            )}
            <th className="px-3 py-1.5 font-medium text-right whitespace-nowrap" onClick={() => toggle('weight')}>Weight{caret('weight')}</th>
            <th className="px-3 py-1.5 font-medium text-left whitespace-nowrap" onClick={() => toggle('ccy')}>Ccy{caret('ccy')}</th>
            {data.years.map((y) => (
              <th key={y} className="px-3 py-1.5 font-medium text-right whitespace-nowrap" onClick={() => toggle(y)}>{y}{caret(y)}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={`${r.isin}-${i}`} className="border-b border-neutral-800/20 hover:bg-overlay/[0.02]">
              <td className="px-3 py-1.5 text-fg-soft sticky left-0 bg-card z-10 max-w-0">
                <span className="block truncate" title={r.name}>{r.name}</span>
              </td>
              <td className="px-3 py-1.5 font-mono text-[11px] text-fg-subtle whitespace-nowrap">{r.exchange ?? '—'}</td>
              <td className="px-3 py-1.5 font-mono text-[11px] whitespace-nowrap">
                {r.ticker
                  ? <a href={guruFocusUrl(r.ticker, r.exchange)} target="_blank" rel="noopener noreferrer"
                      className="text-accent-400 hover:underline" title="Open the GuruFocus page">{r.ticker} ↗</a>
                  : '—'}
              </td>
              {hasCap && <td className="px-3 py-1.5 text-right font-mono text-fg-muted whitespace-nowrap">{capBn(r.market_cap_eur)}</td>}
              {/* ⚠ TWO DECIMALS, MATCHING THE SERVER. `weight_pct` is rounded to 2 there, and
                  printing 1 here made cap ÷ Σcap fail to reproduce the number beside it — on the
                  one table whose purpose is that the division can be checked. */}
              <td className="px-3 py-1.5 text-right font-mono text-fg-muted whitespace-nowrap">{r.weight_pct.toFixed(2)}%</td>
              <td className="px-3 py-1.5 font-mono text-[11px] text-fg-subtle whitespace-nowrap">{r.currency ?? '—'}</td>
              {r.status === 'unsubscribed' ? (
                // Can't fetch it — exchange outside the GuruFocus subscription.
                <td colSpan={data.years.length} className="px-3 py-1.5 text-warn-300"
                  title={`No ${noun}: ${r.ticker ?? ''}@${r.exchange ?? '?'} is on an exchange outside our GuruFocus subscription.`}>
                  Unsubscribed
                </td>
              ) : r.status === 'no_data' ? (
                <td colSpan={data.years.length} className="px-3 py-1.5">
                  {ingest[r.isin]?.busy ? (
                    <span className="text-[11px] text-fg-faint">fetching…</span>
                  ) : onFetch ? (
                    <span className="inline-flex items-center gap-2">
                      <button type="button" onClick={() => void fetchOne(r.isin, r.name)}
                        title="Fetch this company's financials from GuruFocus."
                        className="cursor-pointer text-[11px] px-2 py-0.5 rounded-lg border border-accent-600/40 text-accent-400 hover:bg-overlay/5">
                        Fetch financials
                      </button>
                      {ingest[r.isin]?.msg && (
                        <span className="text-[10px] text-warn-300" title={ingest[r.isin]?.msg}>
                          {ingest[r.isin]?.msg}
                        </span>
                      )}
                    </span>
                  ) : (
                    <span className="text-[11px] text-fg-faint">no {noun} ingested</span>
                  )}
                </td>
              ) : (
                data.years.map((y) => (
                  <td key={y} className="px-3 py-1.5 text-right font-mono text-fg-soft whitespace-nowrap">{fmt(r.revenue[y])}</td>
                ))
              )}
            </tr>
          ))}
        </tbody>
        <tfoot>
          {/* Sum of the shown companies' weights — under 100% because cash / bonds / any holding
              we can't price aren't listed. */}
          <tr className="border-t border-neutral-800/40 bg-page font-semibold text-fg-strong">
            <td className="px-3 py-1.5 sticky left-0 bg-page z-10">Total</td>
            <td className="px-3 py-1.5" />
            <td className="px-3 py-1.5" />
            {/* ⚠ THE DENOMINATOR, SPELLED OUT. Without it the Weight column is a set of numbers to
                take on trust; with it, every row is cap ÷ this. */}
            {hasCap && (
              <td className="px-3 py-1.5 text-right font-mono whitespace-nowrap"
                title="The sum every weight in this column was divided by.">
                {capBn(rows.reduce((a, r) => a + (r.market_cap_eur ?? 0), 0))}
              </td>
            )}
            <td className="px-3 py-1.5 text-right font-mono whitespace-nowrap">
              {rows.reduce((a, r) => a + r.weight_pct, 0).toFixed(2)}%
            </td>
            <td className="px-3 py-1.5" />
            {data.years.map((y) => <td key={y} className="px-3 py-1.5" />)}
          </tr>
        </tfoot>
      </table>
    </div>
  );
}

/** The lines as the chart drew them, one row per period. */
function PlottedTable({ points, seriesLabel, benchLabel, fmt, isIndex }: {
  points: PlottedPoint[]; seriesLabel: string; benchLabel?: string | null;
  fmt: (v: number | null | undefined) => string; isIndex: boolean;
}) {
  const hasTrend = points.some((p) => p.trend != null);
  const hasBench = points.some((p) => p.bench != null);
  return (
    <div className="overflow-auto rounded-lg border border-neutral-800/40">
      <table className="w-full text-xs">
        <thead className="bg-page">
          <tr className="text-fg-faint text-[10px] uppercase tracking-wide border-b border-neutral-800/40">
            <th className="px-3 py-1.5 font-medium text-left w-full">Period</th>
            {/* ⚠ The head is tinted to its line; the VALUES stay in text ink. A number wearing a
                series colour is the "text wears text tokens" rule broken. */}
            {/* ⚠ SAME HAZARD ON THIS SIDE WHEN IT IS A BLEND, and it needs saying here too — the
                benchmark column that carries the other half of the warning may not be present. */}
            <th className="px-3 py-1.5 font-medium text-right whitespace-nowrap"
              style={{ color: chartTheme.accent }}
              title={isIndex
                ? 'A blended growth INDEX, not a currency amount — each holding rebased to 100 at '
                  + 'its first period, then weight-averaged. Revenues in different currencies '
                  + 'cannot be summed, so no level here belongs to anyone.'
                : undefined}>
              {seriesLabel}
            </th>
            {hasTrend && <th className="px-3 py-1.5 font-medium text-right whitespace-nowrap"
              style={{ color: chartTheme.warn }}>Trend</th>}
            {/* ⚠ THE WARNING LIVES ON THE HEAD NOW, BUT IT STILL HAS TO LIVE SOMEWHERE. This
                column is scaled to meet the line beside it at the first shared period, so it
                carries that line's units and reads exactly like an absolute figure — "AEX revenue
                23.9B" — which it is not. Only its SHAPE is meaningful. */}
            {hasBench && <th className="px-3 py-1.5 font-medium text-right whitespace-nowrap"
              style={{ color: chartTheme.pos }}
              title={`Not ${benchLabel}'s own figures. ${benchLabel}'s blended series, multiplied by a single constant so it meets the ${seriesLabel} column at the first period both cover — on a log axis that is a vertical shift, so the growth rate is untouched and the level is not a quantity of anything.${isIndex ? ' The other column is itself a growth index (each holding rebased to 100 at its first period, then weight-averaged), not a currency amount.' : ''}`}>
              {benchLabel} (rebased)
            </th>}
          </tr>
        </thead>
        <tbody>
          {points.map((p) => (
            <tr key={p.year} className="border-b border-neutral-800/20 hover:bg-overlay/[0.02]">
              <td className="px-3 py-1.5 font-mono text-fg-soft">{xToPeriod(p.year)}</td>
              <td className="px-3 py-1.5 text-right font-mono text-fg-soft whitespace-nowrap">{fmt(p.value)}</td>
              {hasTrend && <td className="px-3 py-1.5 text-right font-mono text-fg-muted whitespace-nowrap">{fmt(p.trend)}</td>}
              {hasBench && <td className="px-3 py-1.5 text-right font-mono text-fg-soft whitespace-nowrap">{fmt(p.bench)}</td>}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default function HoldingsRevenueModal({
  target, metric = 'revenue', unit = 'millions', noun = 'revenue', portfolioName, onClose,
  plotted, seriesLabel, benchLabel, benchTarget, isIndex = false,
}: {
  target: Target;
  metric?: string;
  unit?: 'millions' | 'per_share' | 'percent' | 'shares';
  noun?: string;
  portfolioName?: string | null;
  onClose: () => void;
  /** The chart's own points. Absent ⇒ the modal is the holdings table alone, as it was. */
  plotted?: PlottedPoint[];
  seriesLabel?: string;
  benchLabel?: string | null;
  /** Set when a benchmark is active — lets the modal load the INDEX's constituents on demand. */
  benchTarget?: { universe: string; cadence: 'annual' | 'quarterly' } | null;
  /** True when the plotted line is a blended index rather than one company's reported figures. */
  isIndex?: boolean;
}) {
  // millions/shares → compact B/T/M; per_share → a plain per-share figure; percent → a % ratio.
  const fmtM = (v: number | null | undefined) => {
    if (v == null) return '—';
    if (unit === 'percent') return `${v.toFixed(1)}%`;
    if (unit === 'per_share') return v.toFixed(2);
    const a = Math.abs(v);
    if (a >= 1e6) return `${(v / 1e6).toFixed(2)}T`;
    if (a >= 1e3) return `${(v / 1e3).toFixed(1)}B`;
    return `${v.toFixed(0)}M`;
  };
  // ⚠ A BLENDED INDEX IS NOT CURRENCY. Rendering 214.3 as "214M" claims a unit it does not have —
  // the same reason the blend endpoint returns a null `currency`.
  const fmtPlot = (v: number | null | undefined) => (
    v == null ? '—' : isIndex ? v.toFixed(1) : fmtM(v));
  const [data, setData] = useState<Resp | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [reloadKey, setReloadKey] = useState(0);

  const load = async (body: Target): Promise<Resp> => {
    const r = await apiFetch(`${API_URL}/api/earnings/portfolio-revenue-matrix?metric=${encodeURIComponent(metric)}`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const b = await r.json().catch(() => null);
    if (!r.ok) throw new Error(b?.detail ?? `HTTP ${r.status}`);
    return b as Resp;
  };

  useEffect(() => {
    let alive = true;
    void (async () => {
      setData(null); setErr(null);
      try {
        const b = await load(target);
        if (alive) setData(b);
      } catch (e) {
        if (alive) setErr(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { alive = false; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [target, metric, reloadKey]);

  /**
   * The index's constituents — every one, with its weight and its reported figures, so the line
   * can be checked by hand.
   *
   * ⚠ IT LOADS WITH THE MODAL, NOT BEHIND A BUTTON, AND THAT ONLY BECAME REASONABLE ONCE THE READ
   * WAS FIXED. It used to be one metric read per company: the S&P's 489 constituents took **64.5
   * s**, which is why it was gated. Prefetched it is one chunked, paged query — **0.19 s**
   * measured — so hiding the table now costs a click and buys nothing.
   */
  const [bench, setBench] = useState<Resp | null>(null);
  const [benchErr, setBenchErr] = useState<string | null>(null);
  const benchKey = benchTarget ? `${benchTarget.universe}|${benchTarget.cadence}` : '';
  useEffect(() => {
    let alive = true;
    void (async () => {
      setBench(null); setBenchErr(null);
      if (!benchTarget) return;
      try {
        const b = await load(benchTarget);
        if (alive) setBench(b);
      } catch (e) {
        console.warn('[bb:bench] constituent matrix:', e);
        if (alive) setBenchErr(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { alive = false; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [benchKey, metric]);

  /** Fetch a `no_data` holding's financials, then reload so its figures appear. Throws the stated
   *  reason on anything else, which the row renders. Admin-only endpoint. */
  const fetchRevenue = async (isin: string, name: string) => {
    const r = await apiFetch(`${API_URL}/api/earnings/fundamental-coverage/ingest`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ isin, name }),
    });
    const j = (await r.json().catch(() => null)) as { status?: string; detail?: string } | null;
    if (r.ok && j?.status === 'ingested') {
      setReloadKey((k) => k + 1);
      return;
    }
    throw new Error(j?.detail ?? j?.status ?? `HTTP ${r.status}`);
  };

  const section = 'text-[11px] uppercase tracking-wide text-fg-muted';
  /**
   * ⚠ A ONE-ROW MATRIX IS THE PLOTTED TABLE AGAIN. On a single company the line IS that company's
   * reported figures, so the "as reported" table repeats every number above it and adds a 100.00%
   * weight column and a Total row over one row — noise that makes the modal look like it holds two
   * findings when it holds one. It stays the moment there is a second row to compare against.
   */
  const solo = !!plotted && (data?.rows.length ?? 0) === 1;
  const only = solo ? data!.rows[0] : null;

  return (
    <div className="fixed inset-0 z-[60] flex items-center justify-center bg-scrim/60 p-4"
      onClick={onClose} role="presentation">
      <div className="bg-card rounded-xl border border-neutral-800/40 shadow-xl w-[88vw] h-[84vh] flex flex-col"
        onClick={(e) => e.stopPropagation()} role="dialog" aria-modal="true">
        <div className="flex items-baseline gap-3 px-6 py-4 border-b border-neutral-800/40">
          <h2 className="text-fg-strong font-medium">{seriesLabel ?? noun} — everything behind the chart</h2>
          {portfolioName && <span className="text-sm text-fg-soft truncate max-w-[24ch]" title={portfolioName}>{portfolioName}</span>}
          {/* ⚠ THE PROVENANCE SURVIVES THE TABLE IT LIVED IN. Dropping the one-row matrix would
              otherwise take the GuruFocus listing + reporting currency with it, and those are how
              a reader checks the figures against the source. */}
          {only ? (
            <span className="text-[11px] text-fg-faint font-mono">
              {only.exchange ?? '—'}
              {only.ticker && <>
                {' '}
                <a href={guruFocusUrl(only.ticker, only.exchange)} target="_blank" rel="noopener noreferrer"
                  className="text-accent-400 hover:underline" title="Open the GuruFocus page">{only.ticker} ↗</a>
              </>}
              {only.currency && ` · ${only.currency}`}
            </span>
          ) : data && <span className="text-[11px] text-fg-faint">{data.rows.length} companies</span>}
          {benchLabel && <span className="text-[11px]" style={{ color: chartTheme.pos }}>vs {benchLabel}</span>}
          <button type="button" onClick={onClose} className="ml-auto text-fg-muted hover:text-fg-strong px-2">✕</button>
        </div>

        <div className="flex-1 overflow-auto px-6 py-4 space-y-5">
          {/* 1 — the lines themselves. First, because they are what was clicked. */}
          {plotted && plotted.length > 0 && (
            <div className="space-y-1.5">
              <h3 className={section}>Plotted series</h3>
              <PlottedTable points={plotted} seriesLabel={seriesLabel ?? noun} benchLabel={benchLabel}
                fmt={fmtPlot} isIndex={isIndex} />
            </div>
          )}

          {/* 2 — the reported figures the portfolio line was built from. Dropped on a single
              company, where it is the table above with one row — see `solo`. */}
          <div className="space-y-1.5">
            {!solo && (
              <h3 className={section}>
                {portfolioName ? `${portfolioName} — ` : ''}{noun} by period, as reported
              </h3>
            )}
            {err && <p className="text-xs text-neg-300">{err}</p>}
            {!data && !err && <p className="text-xs text-fg-subtle">Loading…</p>}
            {data && data.rows.length === 0 && !err && (
              <p className="text-xs text-fg-subtle">No held company has {noun} ingested.</p>
            )}
            {data && data.rows.length > 0 && !solo && (
              <MatrixTable data={data} fmt={fmtM} noun={noun} onFetch={fetchRevenue} />
            )}
          </div>

          {/* 3 — the same, for the index, on demand. */}
          {benchTarget && (
            <div className="space-y-1.5">
              <h3 className={section}>{benchLabel} constituents — {noun} by period, as reported</h3>
              {!bench && !benchErr && (
                <p className="text-xs text-fg-subtle">Loading {benchLabel} constituents…</p>
              )}
              {benchErr && <p className="text-xs text-neg-300">{benchErr}</p>}
              {bench && (
                <>
                  {/* ⚠ EVERY CAVEAT IS STILL HERE — IT MOVED TO THE `title`, IT DID NOT GO. Each is
                      a thing a reader would otherwise assume, and each is false: the weight is
                      CURRENT full cap (not backed out to the start of the window the way the price
                      index does, and not float-adjusted or 15%-capped like the published AEX); it
                      spans every constituent while the line renormalises over the covered ones
                      period by period; and a constituent with no stored cap is not in the index at
                      all — systematically the names GuruFocus does not cover. */}
                  <p className="text-[10px] text-fg-faint">
                    {bench.rows.length} constituents ·{' '}
                    <span className="underline decoration-dotted underline-offset-2"
                      title="company.market_cap_eur as stored today. Full cap — not free-float, and not capped per constituent the way the published index is. Not backed out to the start of the window either, so a company that has since grown carries its post-growth weight over its whole history.">
                      current full market cap
                    </span>{' '}
                    · {bench.rows.filter((r) => r.status === 'ok').length} with {noun} feed the line,
                    renormalised each period
                  </p>
                  {/* ⚠ THE DROPPED NAMES STAY IN THE OPEN. Absent from the table and absent from a
                      note, a missing constituent reads as a weight the index really has. */}
                  {bench.weight_basis && bench.weight_basis.excluded.length > 0 && (
                    <p className="text-[10px] text-warn-300"
                      title={`Not in the index at any weight. The remaining ${bench.weight_basis.weighted} are renormalised to 100%, so every weight shown is larger than that constituent's share of the real index.`}>
                      {bench.weight_basis.excluded.length}/{bench.weight_basis.members} excluded,
                      weights renormalised:{' '}
                      {bench.weight_basis.excluded
                        .map((x) => `${x.name ?? '?'} (${x.reason})`).join(' · ')}
                    </p>
                  )}
                  <MatrixTable data={bench} fmt={fmtM} noun={noun} />
                </>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
