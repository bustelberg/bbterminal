'use client';

import { useState, useEffect, useCallback, useMemo } from 'react';
import { trackedFetch } from '../../lib/loading';
import { API_URL } from '../../lib/apiUrl';
import { useStaticUniverses } from '../../lib/hooks/apiData';
import type { Column } from '../../lib/tableExport';
import TableDownloadButton from './TableDownloadButton';
import LoadingDots from './LoadingDots';
import InfoTip from './universe/InfoTip';

/**
 * /timezone — every exchange in a chosen universe, with its regular trading
 * hours converted to Amsterdam wall-clock. Because we trade at the *previous
 * day's close*, the Amsterdam closing time tells us by when each market's last
 * price is final in our own day; a summer (CEST) / winter (CET) toggle shows
 * the daylight-saving variant since the offset shifts between seasons.
 */

type AmsTime = { time: string; day_offset: number };
type Hours = {
  timezone: string;
  local_open: string;
  local_close: string;
  lunch_start: string | null;
  lunch_end: string | null;
  trading_week: string;
  observes_dst: boolean;
  amsterdam_winter: { open: AmsTime; close: AmsTime };
  amsterdam_summer: { open: AmsTime; close: AmsTime };
};
type ExchangeRow = {
  exchange_code: string;
  exchange_name: string | null;
  currency: string | null;
  country: string | null;
  company_count: number;
  hours: Hours | null;
};
type Season = 'summer' | 'winter';

const ALL = '__ALL__';

/** Render an Amsterdam time with a day-rollover marker (−1d / +1d). */
function AmsCell({ t, dim = false }: { t: AmsTime; dim?: boolean }) {
  return (
    <span className={dim ? 'text-fg-faint' : 'text-fg-strong font-medium'}>
      <span className="font-mono">{t.time}</span>
      {t.day_offset !== 0 && (
        <sup className="ml-0.5 text-[9px] text-warn-400" title={t.day_offset < 0 ? 'Previous Amsterdam day' : 'Next Amsterdam day'}>
          {t.day_offset > 0 ? `+${t.day_offset}d` : `${t.day_offset}d`}
        </sup>
      )}
    </span>
  );
}

export default function TimezoneExplorer() {
  const { data: universes } = useStaticUniverses();
  const [universe, setUniverse] = useState<string>(ALL);
  const [season, setSeason] = useState<Season>(
    // Default to whichever season Amsterdam is in right now (Apr–Oct ≈ CEST).
    (() => {
      const m = new Date().getMonth(); // 0=Jan
      return m >= 3 && m <= 9 ? 'summer' : 'winter';
    })(),
  );
  const [rows, setRows] = useState<ExchangeRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [filter, setFilter] = useState('');

  const load = useCallback((u: string) => {
    setLoading(true);
    setError(null);
    const qs = u === ALL ? '' : `?universe=${encodeURIComponent(u)}`;
    trackedFetch('Loading trading hours', `${API_URL}/api/timezone/exchanges${qs}`)
      .then(async (r) => {
        if (!r.ok) throw new Error(`Couldn't load exchanges (${r.status})`);
        return r.json();
      })
      .then((d: { exchanges: ExchangeRow[] }) => setRows(Array.isArray(d.exchanges) ? d.exchanges : []))
      .catch((e: unknown) => setError(e instanceof Error ? e.message : "Couldn't load exchanges"))
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => { load(universe); }, [load, universe]);

  const filtered = useMemo(() => {
    const f = filter.trim().toLowerCase();
    if (!f) return rows;
    return rows.filter(
      (r) =>
        r.exchange_code.toLowerCase().includes(f) ||
        (r.exchange_name || '').toLowerCase().includes(f) ||
        (r.country || '').toLowerCase().includes(f) ||
        (r.hours?.timezone || '').toLowerCase().includes(f),
    );
  }, [rows, filter]);

  const other: Season = season === 'summer' ? 'winter' : 'summer';
  const totalCompanies = useMemo(() => rows.reduce((s, r) => s + r.company_count, 0), [rows]);

  const exportColumns = useMemo<Column<ExchangeRow>[]>(() => [
    { key: 'exchange_code', header: 'Exchange', accessor: (r) => r.exchange_code },
    { key: 'exchange_name', header: 'Name', accessor: (r) => r.exchange_name ?? '' },
    { key: 'country', header: 'Country', accessor: (r) => r.country ?? '' },
    { key: 'company_count', header: 'Companies', accessor: (r) => r.company_count },
    { key: 'timezone', header: 'Timezone', accessor: (r) => r.hours?.timezone ?? '' },
    { key: 'local', header: 'Local session', accessor: (r) => (r.hours ? `${r.hours.local_open}–${r.hours.local_close}` : '') },
    { key: 'ams_open_w', header: 'Ams open (CET)', accessor: (r) => r.hours?.amsterdam_winter.open.time ?? '' },
    { key: 'ams_close_w', header: 'Ams close (CET)', accessor: (r) => r.hours?.amsterdam_winter.close.time ?? '' },
    { key: 'ams_open_s', header: 'Ams open (CEST)', accessor: (r) => r.hours?.amsterdam_summer.open.time ?? '' },
    { key: 'ams_close_s', header: 'Ams close (CEST)', accessor: (r) => r.hours?.amsterdam_summer.close.time ?? '' },
  ], []);

  return (
    <div className="flex-1 min-h-0 flex flex-col">
      <div className="px-8 py-5 border-b border-neutral-800/40">
        <h1 className="text-xl font-semibold text-fg-strong">Trading Hours</h1>
        <p className="text-sm text-fg-subtle mt-1 max-w-3xl">
          Every exchange in the selected universe, with its regular trading session converted to{' '}
          <span className="text-fg-soft">Amsterdam</span> time. We trade at the <em>previous day&apos;s close</em>, so the
          Amsterdam <span className="text-fg-soft">close</span> is when that price is final in our day. Hours shift with
          daylight-saving — toggle the season to see the variant.
        </p>
      </div>

      <div className="px-8 py-4 flex items-center gap-3 flex-wrap">
        <label className="text-xs text-fg-subtle">Universe</label>
        <select
          value={universe}
          onChange={(e) => setUniverse(e.target.value)}
          className="px-3 py-1.5 bg-page border border-neutral-700 rounded-lg text-sm text-fg focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none"
        >
          <option value={ALL}>All companies (whole DB)</option>
          {(universes ?? []).map((u) => (
            <option key={u.template_key} value={u.template_key}>{u.label ?? u.template_key}</option>
          ))}
        </select>

        <div className="inline-flex rounded-lg border border-neutral-700 overflow-hidden" title="Which Amsterdam season to show in the main columns. Both are always in the export.">
          {(['winter', 'summer'] as Season[]).map((s) => (
            <button
              key={s}
              onClick={() => setSeason(s)}
              className={`px-3 py-1.5 text-xs font-medium transition-colors ${
                season === s ? 'bg-accent-600 text-fg-strong' : 'text-fg-muted hover:bg-overlay/5'
              }`}
            >
              {s === 'winter' ? 'Winter (CET)' : 'Summer (CEST)'}
            </button>
          ))}
        </div>

        <input
          type="text"
          placeholder="Filter exchange / country / tz…"
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          className="px-3 py-1.5 bg-page border border-neutral-700 rounded-lg text-sm text-fg placeholder-fg-faint focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none w-64"
        />

        <div className="ml-auto flex items-center gap-3">
          <span className="text-xs text-fg-subtle">
            {rows.length} exchange{rows.length === 1 ? '' : 's'} · {totalCompanies} companies
          </span>
          <TableDownloadButton
            rows={filtered}
            columns={exportColumns}
            filename={`trading_hours_${universe === ALL ? 'all' : universe}`}
            title={`Download ${filtered.length} exchanges as CSV / XLSX`}
          />
        </div>
      </div>

      <div className="flex-1 min-h-0 overflow-auto px-8 pb-6">
        <div className="rounded-xl border border-neutral-800/40 bg-card overflow-hidden">
          {error ? (
            <div className="px-5 py-4 text-sm text-neg-300 flex items-center gap-3">
              <span>{error}</span>
              <button onClick={() => load(universe)} className="px-2 py-1 rounded-md text-xs bg-overlay/5 hover:bg-overlay/10 text-fg-soft">Retry</button>
            </div>
          ) : loading && rows.length === 0 ? (
            <div className="px-5 py-10 text-center text-fg-subtle text-sm"><LoadingDots label="Loading" /></div>
          ) : (
            <table className="w-full text-sm">
              <thead className="sticky top-0 z-10 bg-card">
                <tr className="text-left text-xs text-fg-subtle border-b border-neutral-800/40">
                  <th className="px-4 py-2.5 font-medium">Exchange</th>
                  <th className="px-3 py-2.5 font-medium">Country</th>
                  <th className="px-3 py-2.5 font-medium text-right w-16">#</th>
                  <th className="px-3 py-2.5 font-medium">Local session</th>
                  <th className="px-3 py-2.5 font-medium">
                    Opens (Ams)
                    <span className="ml-1 text-fg-faint normal-case">{season === 'winter' ? 'CET' : 'CEST'}</span>
                  </th>
                  <th className="px-3 py-2.5 font-medium">
                    <span className="inline-flex items-center gap-1">
                      Closes (Ams)
                      <span className="text-fg-faint normal-case">{season === 'winter' ? 'CET' : 'CEST'}</span>
                      <InfoTip text="Amsterdam wall-clock time the market closes. Because we trade the previous day's close, this is when yesterday's final price is locked in relative to our day. The faint second line is the other DST season." />
                    </span>
                  </th>
                </tr>
              </thead>
              <tbody>
                {filtered.map((r) => (
                  <tr key={r.exchange_code} className="border-b border-neutral-800/20 hover:bg-overlay/[0.02]">
                    <td className="px-4 py-2.5">
                      <span className="font-mono font-medium text-fg-strong">{r.exchange_code}</span>
                      {r.exchange_name && <span className="ml-2 text-fg-muted text-xs">{r.exchange_name}</span>}
                      {r.currency && <span className="ml-2 text-fg-faint text-[10px] font-mono">{r.currency}</span>}
                    </td>
                    <td className="px-3 py-2.5 text-fg-muted">{r.country || '—'}</td>
                    <td className="px-3 py-2.5 text-right font-mono text-fg-muted">{r.company_count}</td>
                    {r.hours ? (
                      <>
                        <td className="px-3 py-2.5 text-fg-muted">
                          <span className="font-mono">{r.hours.local_open}–{r.hours.local_close}</span>
                          <span className="ml-2 text-fg-faint text-[10px]" title={r.hours.timezone}>
                            {r.hours.timezone.split('/').pop()?.replace('_', ' ')}
                          </span>
                          {r.hours.lunch_start && (
                            <span className="ml-2 text-fg-faint text-[10px]" title="Lunch break (no trading)">
                              lunch {r.hours.lunch_start}–{r.hours.lunch_end}
                            </span>
                          )}
                          {r.hours.trading_week !== 'Mon–Fri' && (
                            <span className="ml-2 px-1.5 py-0.5 rounded text-[9px] font-medium bg-warn-500/15 text-warn-300 border border-warn-500/25" title="Trades on a non-Mon–Fri week">
                              {r.hours.trading_week}
                            </span>
                          )}
                          {!r.hours.observes_dst && (
                            <span className="ml-2 text-fg-faint text-[10px]" title="This exchange's timezone does not observe daylight-saving, so its Amsterdam time shifts by an hour between our seasons.">
                              no DST
                            </span>
                          )}
                        </td>
                        <td className="px-3 py-2.5">
                          <div><AmsCell t={r.hours[`amsterdam_${season}`].open} /></div>
                          <div className="text-[10px]"><AmsCell t={r.hours[`amsterdam_${other}`].open} dim /> <span className="text-fg-faint">{other === 'winter' ? 'CET' : 'CEST'}</span></div>
                        </td>
                        <td className="px-3 py-2.5">
                          <div><AmsCell t={r.hours[`amsterdam_${season}`].close} /></div>
                          <div className="text-[10px]"><AmsCell t={r.hours[`amsterdam_${other}`].close} dim /> <span className="text-fg-faint">{other === 'winter' ? 'CET' : 'CEST'}</span></div>
                        </td>
                      </>
                    ) : (
                      <td colSpan={3} className="px-3 py-2.5 text-fg-faint text-xs italic">No trading-hours data for this exchange</td>
                    )}
                  </tr>
                ))}
                {!loading && filtered.length === 0 && (
                  <tr><td colSpan={6} className="px-5 py-10 text-center text-fg-subtle text-sm">
                    {rows.length === 0 ? 'No exchanges found for this universe.' : 'No exchanges match your filter.'}
                  </td></tr>
                )}
              </tbody>
            </table>
          )}
        </div>
      </div>
    </div>
  );
}
