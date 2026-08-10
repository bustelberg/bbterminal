'use client';

/**
 * Every instrument behind the correlation matrix, with its price series.
 *
 * ⚠ THE SERIES IS THE ONE THE MATRIX CONSUMED, not a re-fetch. There IS an existing per-ISIN
 * price endpoint (`/api/asset-pipeline/price-series/isin/{isin}`) and using it here would have
 * been less code — but it returns MONTHLY closes on its own split-adjustment path, so the chart
 * under a correlation of DAILY returns would be a different series with a different shape, and
 * any disagreement between the table and the matrix would be unexplainable. The endpoint ships
 * the exact daily EUR levels it correlated; this only draws them.
 *
 * ⚠ SEPARATE <section> FROM THE MATRIX, deliberately. The matrix's section carries
 * `isolation: isolate` because its sticky diagonal headers run a z-index ladder that climbs with
 * the portfolio count, and it scrolls horizontally. An expandable chart row inside that
 * container would inherit both.
 */

import { useMemo, useState } from 'react';

import LwLineChart from './LwLineChart';
import {
  compareInstruments, seriesPoints, sourceLabel, sparkPath, windowReturnPct, ytdStart,
  type InstrumentSort, type SeriesBlock,
} from './correlationSeries';
import type { PortfolioCorrelationMatrix } from '../../lib/types/api';

type Instrument = NonNullable<PortfolioCorrelationMatrix['instruments']>[number];

/** Below this median daily traded value a listing is flagged THIN — mirrors the backend's
 *  `THIN_ADV_EUR`, which is the EUR 250k/day bar `add_portfolio_isins.py` warns at. */
const THIN_ADV_EUR = 250_000;

const SPARK_W = 120;
const SPARK_H = 24;

const eur0 = new Intl.NumberFormat('en-GB', { maximumFractionDigits: 0 });
const eur2 = new Intl.NumberFormat('en-GB', { minimumFractionDigits: 2, maximumFractionDigits: 2 });

function StateBadge({ row }: { row: Instrument }) {
  if (row.state === 'direct') {
    return <span className="text-fg-faint">listed</span>;
  }
  if (row.state === 'lookthrough') {
    return (
      <span className="text-accent-400"
        title={`No price of its own — this is a certificate wrapping ${row.linked_label ?? 'another model'}. `
          + 'The series charted here is that model\'s own EUR curve, which is what entered the '
          + 'correlation for this leg. Indexed to 100, not a price.'}>
        looks through
      </span>
    );
  }
  return (
    <span className="text-warn-500"
      title={row.linked_label
        ? `Wraps ${row.linked_label}, which could not be priced either (its own coverage is `
          + 'below the floor), so this leg contributed nothing.'
        : 'No price series anywhere — an in-house or structured fund with no listing. Its weight '
          + 'counts against its portfolios\' coverage, which is why a book can be refused a figure.'}>
      unpriced
    </span>
  );
}

export default function CorrelationInstruments(
  { data, win }: { data: PortfolioCorrelationMatrix; win: 'ytd' | 'trailing_12m' },
) {
  const [sort, setSort] = useState<InstrumentSort>('holdings');
  const [desc, setDesc] = useState(true);
  const [open, setOpen] = useState<string | null>(null);
  const [q, setQ] = useState('');
  const [thinOnly, setThinOnly] = useState(false);

  const block: SeriesBlock = useMemo(
    () => ({ dates: data.series?.dates ?? [], values: data.series?.values ?? {} }),
    [data.series],
  );

  // ⚠ THE WINDOW IS SLICED HERE, NOT REFETCHED. The payload always carries the trailing year;
  // YTD is a left-trim of it. Switching the matrix's window must not re-run the price load.
  const from = win === 'ytd' ? ytdStart(data.as_of) : undefined;

  const rows = useMemo(() => {
    const all = data.instruments ?? [];
    const needle = q.trim().toLowerCase();
    const pointsOf = (r: Instrument) => seriesPoints(block, r.series_key, from);
    const returnOf = (r: Instrument) => windowReturnPct(pointsOf(r));
    return all
      .filter((r) => !thinOnly
        || (r.med_adv_eur !== null && r.med_adv_eur !== undefined && r.med_adv_eur < THIN_ADV_EUR))
      .filter((r) => !needle
        || (r.name ?? '').toLowerCase().includes(needle)
        || (r.asset_name ?? '').toLowerCase().includes(needle)
        || (r.symbol ?? '').toLowerCase().includes(needle)
        || r.isin.toLowerCase().includes(needle))
      .slice()
      .sort((a, b) => compareInstruments(a, b, sort, desc, returnOf));
  }, [data.instruments, block, from, sort, desc, q, thinOnly]);

  const counts = useMemo(() => {
    const all = data.instruments ?? [];
    return {
      total: all.length,
      direct: all.filter((r) => r.state === 'direct').length,
      look: all.filter((r) => r.state === 'lookthrough').length,
      unpriced: all.filter((r) => r.state === 'unpriced').length,
      thin: all.filter((r) => r.med_adv_eur !== null && r.med_adv_eur !== undefined
        && r.med_adv_eur < THIN_ADV_EUR).length,
    };
  }, [data.instruments]);

  const head = (key: InstrumentSort, label: string, extra = '') => (
    <th
      className={`px-3 py-2 font-medium cursor-pointer select-none hover:text-fg ${extra}`}
      onClick={() => {
        if (sort === key) setDesc(!desc);
        else { setSort(key); setDesc(true); }
      }}
    >
      {label}
      {sort === key && <span className="text-accent-400">{desc ? ' ↓' : ' ↑'}</span>}
    </th>
  );

  if (!(data.instruments ?? []).length) return null;

  return (
    <section className="bg-card border border-neutral-800/40 rounded-xl p-5 space-y-3">
      <div className="flex flex-wrap items-baseline gap-x-3 gap-y-1">
        <h3 className="text-fg-strong font-medium">Instruments behind the matrix</h3>
        <span className="text-fg-muted text-sm">
          {counts.total} instruments · {counts.direct} listed · {counts.look} looked through
          {counts.unpriced > 0 && <> · <span className="text-warn-500">{counts.unpriced} unpriced</span></>}
        </span>
      </div>

      <p className="text-fg-muted text-sm max-w-4xl">
        Every position the {data.portfolio_ids.length} models hold, and the exact daily EUR series
        the correlations were computed from — not a re-fetch, so a chart here cannot disagree with
        a cell above.{' '}
        <span className="text-fg-subtle">
          Prices are EUR at each date&apos;s own rate. A row that <em>looks through</em> is a
          certificate that IS another model: it has no price of its own, so the series shown is
          that model&apos;s curve, indexed to 100.
        </span>
      </p>

      {/* ⚠ SAID ONCE, IN PROSE, BECAUSE THE COLUMN CANNOT SAY IT. Every priced row reads
          "yfinance", so the Source column proves the rule but never states WHY it matters: this
          app holds two price worlds, and a reader who knows /benchmarks is GuruFocus-priced has
          no way to tell which one a correlation came from. The two differ in adjustment
          convention and FX, so the answer is not a detail. */}
      <p className="text-fg-subtle text-sm max-w-4xl">
        <span className="text-fg-muted">Where the numbers come from:</span> every price here is
        <span className="text-fg"> yfinance</span> (<code className="text-xs">asset_price</code>,
        joined to the AIRS books by ISIN). GuruFocus never enters this path — it prices the
        /benchmarks index and the momentum engine, which live in the company world, and the two
        vendors differ in adjustment convention and FX. The EUR conversion is a
        <span className="text-fg"> second</span> vendor: ECB for its published currencies, Yahoo
        for TWD alone. A EUR-quoted holding needs no conversion and so names only one.
      </p>

      <div className="flex flex-wrap items-center gap-2">
        <input
          value={q}
          onChange={(e) => setQ(e.target.value)}
          placeholder="Search name, ticker or ISIN…"
          className="bg-page border border-neutral-700 rounded-lg px-3 py-1.5 text-sm w-64
                     focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none"
        />
        {counts.thin > 0 && (
          <button
            type="button"
            onClick={() => setThinOnly(!thinOnly)}
            className={`px-3 py-1.5 rounded-lg text-sm border ${thinOnly
              ? 'bg-warn-500/15 border-warn-500/40 text-warn-500'
              : 'border-neutral-700 text-fg-muted hover:bg-overlay/5'}`}
            title={`Median daily traded value under EUR ${eur0.format(THIN_ADV_EUR)}. A near-untraded `
              + 'listing still produces a full year of bars, so nothing else flags it — but its '
              + 'closes are stale against the real market, and a correlation of daily returns is '
              + 'the statistic that damages most.'}
          >
            {thinOnly ? '✓ ' : ''}{counts.thin} thin {counts.thin === 1 ? 'listing' : 'listings'}
          </button>
        )}
        <span className="text-fg-faint text-sm">
          {rows.length === counts.total ? null : `${rows.length} shown · `}
          window: {win === 'ytd' ? 'YTD' : 'trailing 12m'}
        </span>
      </div>

      <div className="overflow-auto rounded-lg border border-neutral-800/40 max-h-[70vh]">
        <table className="w-full text-sm">
          <thead className="sticky top-0 bg-elevated text-fg-muted text-left z-10">
            <tr className="border-b border-neutral-800/40">
              {head('name', 'Instrument')}
              <th className="px-3 py-2 font-medium">Ticker</th>
              <th className="px-3 py-2 font-medium">Ccy</th>
              <th className="px-3 py-2 font-medium">Priced</th>
              <th className="px-3 py-2 font-medium">Source</th>
              {head('holdings', 'Models', 'text-right')}
              {head('weight', 'Σ weight', 'text-right')}
              {head('liquidity', 'Med. ADV (€)', 'text-right')}
              {head('return', 'Return', 'text-right')}
              <th className="px-3 py-2 font-medium">Series</th>
              <th className="px-3 py-2 font-medium text-right">Obs</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => {
              const pts = seriesPoints(block, r.series_key, from);
              const ret = windowReturnPct(pts);
              const thin = r.med_adv_eur !== null && r.med_adv_eur !== undefined
                && r.med_adv_eur < THIN_ADV_EUR;
              const isOpen = open === r.isin;
              return [
                <tr
                  key={r.isin}
                  className="border-b border-neutral-800/20 hover:bg-overlay/[0.02] cursor-pointer"
                  onClick={() => setOpen(isOpen ? null : r.isin)}
                >
                  <td className="px-3 py-2 max-w-[22rem]">
                    <span className="text-fg-faint mr-1.5">{isOpen ? '▾' : '▸'}</span>
                    <span className="text-fg" title={r.asset_name && r.asset_name !== r.name
                      ? `${r.name ?? ''} — our name for it: ${r.asset_name}` : (r.name ?? '')}>
                      {r.name ?? r.isin}
                    </span>
                    <span className="text-fg-faint font-mono text-xs ml-2">{r.isin}</span>
                  </td>
                  <td className="px-3 py-2 font-mono text-xs">
                    {r.symbol ?? <span className="text-fg-faint">—</span>}
                  </td>
                  <td className="px-3 py-2 text-fg-muted">{r.currency ?? '—'}</td>
                  <td className="px-3 py-2"><StateBadge row={r} /></td>
                  <td className="px-3 py-2 whitespace-nowrap">
                    <span className={r.price_source ? 'text-fg-muted' : 'text-fg-faint'}
                      title={sourceLabel(r).title}>
                      {sourceLabel(r).short}
                    </span>
                  </td>
                  <td className="px-3 py-2 text-right font-mono">{r.in_portfolios}</td>
                  <td className="px-3 py-2 text-right font-mono text-fg-muted">
                    {eur2.format(r.weight_pct_sum ?? 0)}%
                  </td>
                  <td className={`px-3 py-2 text-right font-mono ${thin ? 'text-warn-500' : 'text-fg-muted'}`}>
                    {r.med_adv_eur === null || r.med_adv_eur === undefined
                      ? <span className="text-fg-faint" title="No liquidity figure recorded for this listing.">—</span>
                      : <span title={thin ? 'THIN — under EUR 250k/day. Its closes are stale against the real market.' : undefined}>
                        {eur0.format(r.med_adv_eur)}{thin ? ' ⚠' : ''}
                      </span>}
                  </td>
                  <td className={`px-3 py-2 text-right font-mono ${ret === null ? '' : ret >= 0 ? 'text-pos-500' : 'text-neg-500'}`}>
                    {ret === null
                      ? <span className="text-fg-faint" title="No two observations in this window to compare — not a 0% return.">—</span>
                      : `${ret >= 0 ? '+' : ''}${ret.toFixed(2)}%`}
                  </td>
                  <td className="px-3 py-2">
                    {pts.length > 1 ? (
                      <svg width={SPARK_W} height={SPARK_H} className="block"
                        viewBox={`0 0 ${SPARK_W} ${SPARK_H}`} aria-hidden>
                        <path d={sparkPath(pts, SPARK_W, SPARK_H)} fill="none" strokeWidth={1.25}
                          className={ret === null || ret >= 0 ? 'stroke-pos-500' : 'stroke-neg-500'} />
                      </svg>
                    ) : (
                      <span className="text-fg-faint text-xs">no series</span>
                    )}
                  </td>
                  <td className="px-3 py-2 text-right font-mono text-fg-muted">{r.observations}</td>
                </tr>,
                isOpen ? (
                  <tr key={`${r.isin}-detail`} className="bg-inset border-b border-neutral-800/40">
                    <td colSpan={11} className="px-4 py-4">
                      {pts.length > 1 ? (
                        <div className="space-y-2">
                          <div className="flex flex-wrap items-baseline gap-x-4 gap-y-1 text-sm">
                            <span className="text-fg-strong">{r.name ?? r.isin}</span>
                            <span className="text-fg-muted">
                              {r.unit === 'index'
                                ? `indexed to 100 · via ${r.linked_label ?? 'the wrapped model'}`
                                : `EUR · ${r.symbol ?? ''} ${r.currency ? `(quoted ${r.currency})` : ''}`}
                            </span>
                            <span className="text-fg-faint">
                              {r.first_date} → {r.last_date} · {pts.length} points
                            </span>
                            {thin && (
                              <span className="text-warn-500">
                                ⚠ thin listing — median €{eur0.format(r.med_adv_eur as number)}/day
                              </span>
                            )}
                          </div>
                          <LwLineChart
                            data={pts}
                            unit={r.unit === 'index' ? '' : '€'}
                            scale="linear"
                          />
                        </div>
                      ) : (
                        <p className="text-fg-muted text-sm">
                          No price series for this instrument, so it contributed nothing to any
                          correlation — its weight counts against its portfolios&apos; coverage.
                          {r.linked_label && <> It wraps <span className="text-fg">{r.linked_label}</span>,
                            which could not be priced either.</>}
                        </p>
                      )}
                    </td>
                  </tr>
                ) : null,
              ];
            })}
          </tbody>
        </table>
      </div>
    </section>
  );
}
