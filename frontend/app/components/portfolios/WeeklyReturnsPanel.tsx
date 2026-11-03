'use client';

import { useEffect, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { startJob } from '../../../lib/stores/jobs';
import { invalidateReadCache } from '../../../lib/readCache';
import { useLang } from '../../../lib/i18n';
import { priceSegments, type WeeklyReturns, type WeeklyInstrument } from './weeklyReturns';

const quote = (value: number | null | undefined) => value == null ? '—'
  : value.toLocaleString('nl-NL', { maximumFractionDigits: 4 });
const day = (value: string | null) => value ? new Date(`${value}T12:00:00`).toLocaleDateString('nl-NL', { day: 'numeric', month: 'short', year: '2-digit' }) : '—';

function PriceChart({ instrument, data }: { instrument: WeeklyInstrument; data: WeeklyReturns }) {
  const segments = priceSegments(instrument.weeks);
  const prices = segments.flat().map((point) => point.price);
  if (!prices.length) return null;
  const low = Math.min(...prices), high = Math.max(...prices);
  const x = (index: number) => 80 + index / Math.max(1, data.periods.length - 1) * 670;
  const y = (price: number) => high === low ? 70 : 20 + (high - price) / (high - low) * 100;
  return <section className="rounded-lg border border-neutral-800/40 bg-elevated p-3">
    <div className="flex flex-wrap justify-between gap-2 text-xs">
      <span className="font-medium text-fg-strong">{instrument.name}</span>
      <span className="text-fg-subtle">AIRS · Huidige koers · {instrument.currency}</span>
    </div>
    <svg viewBox="0 0 800 150" role="img" aria-label={`${instrument.name} · AIRS prices`} className="mt-2 w-full max-h-44 text-accent-400">
      <text x="0" y="24" fill="currentColor" fontSize="11">{quote(high)}</text>
      <text x="0" y="123" fill="currentColor" fontSize="11">{quote(low)}</text>
      <line x1="80" x2="750" y1="120" y2="120" stroke="currentColor" opacity=".15" />
      {segments.map((segment, i) => <g key={i}>
        <polyline points={segment.map((point) => `${x(point.index)},${y(point.price)}`).join(' ')} fill="none" stroke="currentColor" strokeWidth="2" />
        {segment.map((point) => <circle key={point.index} cx={x(point.index)} cy={y(point.price)} r="3" fill="currentColor">
          <title>{day(data.periods[point.index].as_of_date)} · {quote(point.price)} {instrument.currency}</title>
        </circle>)}
      </g>)}
      <text x="80" y="145" fill="currentColor" fontSize="10">{day(data.periods[0]?.as_of_date)}</text>
      <text x="750" y="145" textAnchor="end" fill="currentColor" fontSize="10">{day(data.periods.at(-1)?.as_of_date ?? null)}</text>
    </svg>
  </section>;
}

export default function WeeklyReturnsPanel({ portefeuille }: { portefeuille?: string | null }) {
  const [lang] = useLang();
  const nl = lang === 'nl';
  const [weeks, setWeeks] = useState(12);
  const [revision, setRevision] = useState(0);
  const [result, setResult] = useState<{ key: string; data?: WeeklyReturns; error?: string } | null>(null);
  const [busy, setBusy] = useState(false);
  const [jobError, setJobError] = useState<string | null>(null);
  const [currentOnly, setCurrentOnly] = useState(false);
  const [selected, setSelected] = useState<string | null>(null);
  const key = `${portefeuille}|${weeks}|${revision}`;
  const data = result?.key === key ? result.data : undefined;
  useEffect(() => {
    if (!portefeuille) return;
    const controller = new AbortController();
    let active = true;
    void (async () => {
      try {
        const response = await apiFetch(`${API_URL}/api/airs/accounts/${encodeURIComponent(portefeuille)}/weekly-returns?weeks=${weeks}`, { signal: controller.signal });
        const payload = await response.json();
        if (!response.ok) throw new Error(payload.detail ?? 'Could not load weekly prices');
        if (active) setResult({ key, data: payload });
      } catch (error) {
        if (active) setResult({ key, error: String(error instanceof Error ? error.message : error) });
      }
    })();
    return () => { active = false; controller.abort(); };
  }, [key, portefeuille, weeks]);

  async function loadHistory() {
    if (!portefeuille || busy) return;
    setBusy(true); setJobError(null);
    try {
      const { done } = await startJob(`${API_URL}/api/airs/accounts/${encodeURIComponent(portefeuille)}/weekly-returns/job?weeks=${weeks}`,
        nl ? 'Wekelijkse AIRS-koersen laden' : 'Loading weekly AIRS prices');
      const job = await done;
      invalidateReadCache('weekly AIRS snapshots saved');
      setRevision((value) => value + 1);
      if (job.status !== 'done') setJobError(nl ? 'Niet alle weken konden worden geladen. Open Details voor de uitleg.' : 'Some weeks could not be loaded. Open Details for the explanation.');
    } catch (error) { setJobError(error instanceof Error ? error.message : String(error)); }
    finally { setBusy(false); }
  }

  if (!portefeuille) return <p className="text-sm text-fg-muted">{nl ? 'Koppel een AIRS-portefeuille om de wekelijkse koersen te zien.' : 'Link an AIRS portfolio to see weekly prices.'}</p>;
  const instruments = data?.instruments.filter((item) => !currentOnly || item.held === true) ?? [];
  const columns = data?.periods.map((period, index) => ({ period, index })).reverse() ?? [];
  const chosen = instruments.find((item) => item.id === selected) ?? instruments[0];
  const labels: Record<string, string> = nl ? {
    added: 'Toegevoegd', reentered: 'Opnieuw gekocht', sold: 'Verkocht', not_held: 'Niet in bezit',
    missing_snapshot: 'Geen weekopname', missing_previous: 'Vorige week ontbreekt', missing_price: 'Geen koers',
  } : { added: 'Added', reentered: 'Bought again', sold: 'Sold', not_held: 'Not held',
    missing_snapshot: 'No snapshot', missing_previous: 'Previous week missing', missing_price: 'No price' };
  return <div className="space-y-4">
    <div className="flex flex-wrap items-center justify-between gap-3">
      <div><h3 className="text-sm font-semibold text-fg-strong">{nl ? 'Wekelijkse koersveranderingen' : 'Weekly price changes'}</h3>
        <p className="mt-1 max-w-3xl text-xs text-fg-subtle">{nl ? 'AIRS Huidige koers, in de genoteerde valuta. Extra aankopen en verkopen veranderen dit percentage niet. Dividend, coupons en valutaveranderingen zijn uitgesloten. Een aandelensplitsing kan een sprong geven.' : 'AIRS Huidige koers, in the quoted currency. Buying or selling more does not change this percentage. Dividends, coupons and currency changes are excluded. Stock splits can cause jumps.'}</p>
      </div>
      <div className="flex items-center gap-2 text-xs">
        <select aria-label={nl ? 'Aantal weken' : 'Number of weeks'} value={weeks} disabled={busy} onChange={(event) => setWeeks(Number(event.target.value))} className="rounded-md border border-neutral-800/40 bg-elevated px-2 py-1.5 text-fg">
          {[12, 26, 52].map((value) => <option key={value} value={value}>{value} {nl ? 'weken' : 'weeks'}</option>)}
        </select>
        <button type="button" disabled={busy} onClick={() => void loadHistory()} className="rounded-md border border-neutral-800/40 bg-elevated px-3 py-1.5 font-medium text-fg-muted hover:text-accent-300 disabled:opacity-50">{busy ? (nl ? 'Geschiedenis laden…' : 'Loading history…') : (nl ? 'Geschiedenis laden' : 'Load history')}</button>
      </div>
    </div>
    {jobError && <p role="alert" className="text-xs text-neg-300">{jobError}</p>}
    {result?.key === key && result.error && <p role="alert" className="text-sm text-neg-300">{result.error}</p>}
    {!data && !(result?.key === key && result.error) && <p role="status" className="text-sm text-fg-muted">{nl ? 'Opgeslagen koersen laden…' : 'Loading saved prices…'}</p>}
    {data && <>
      <div className="flex flex-wrap justify-between gap-2 text-xs text-fg-subtle">
        <label className="flex items-center gap-2"><input type="checkbox" checked={currentOnly} onChange={(event) => setCurrentOnly(event.target.checked)} />{nl ? 'Alleen huidige posities' : 'Current holdings only'}</label>
        <span>{data.missing_weeks ? (nl ? `${data.missing_weeks} weken ontbreken · laad de geschiedenis om deze aan te vullen` : `${data.missing_weeks} weeks missing · load history to fill them`) : (nl ? 'Dagelijkse AIRS-opnamen bewaren automatisch de nieuwste koers van elke week.' : 'Daily AIRS snapshots automatically retain each week’s latest prices.')}</span>
      </div>
      {chosen && <PriceChart instrument={chosen} data={data} />}
      <div className="overflow-auto rounded-lg border border-neutral-800/40">
        <table className="w-full border-collapse text-xs"><thead><tr className="bg-elevated text-fg-subtle">
          <th className="sticky left-0 z-10 min-w-52 bg-elevated p-3 text-left">{nl ? 'Instrument' : 'Instrument'}</th>
          {columns.map(({ period }) => <th key={period.week_end} className="min-w-20 p-2 text-right font-medium" title={`${nl ? 'Koersen van' : 'Quotes dated'} ${day(period.as_of_date)} · ${nl ? 'Vergeleken met' : 'Compared with'} ${day(period.from_date)}`}>
            <span className="block">{period.partial ? (nl ? 'Deze week' : 'This week') : day(period.week_end)}</span>
            <span className="text-[10px] font-normal">{day(period.as_of_date)}</span>
          </th>)}
        </tr></thead><tbody>{instruments.map((instrument) => <tr key={instrument.id} className="border-t border-neutral-800/30">
          <td className="sticky left-0 z-10 bg-card p-3"><button type="button" onClick={() => setSelected(instrument.id)} className={`text-left font-medium hover:text-accent-300 ${chosen?.id === instrument.id ? 'text-accent-300' : 'text-fg'}`}>{instrument.name}</button>
            <span className="mt-0.5 block text-[10px] text-fg-subtle">{instrument.isin ?? (nl ? 'AIRS-naam · geen ISIN' : 'AIRS name · no ISIN')} · {instrument.currency}{instrument.held === false ? (nl ? ' · Niet meer in bezit' : ' · No longer held') : ''}</span>
          </td>
          {columns.map(({ period, index }) => {
            const cell = instrument.weeks[index];
            return <td key={period.week_end} className={`p-2 text-right tabular-nums ${cell.return_pct == null ? 'text-fg-subtle' : cell.return_pct >= 0 ? 'bg-pos-500/5 text-pos-300' : 'bg-neg-500/5 text-neg-300'}`} title={`${labels[cell.status] ?? ''} ${day(period.from_date)}: ${quote(cell.from_price)} → ${day(period.as_of_date)}: ${quote(cell.price)} ${instrument.currency}`}>
              {cell.return_pct == null ? <span className="text-[10px]">{labels[cell.status] ?? '—'}</span> : `${cell.return_pct >= 0 ? '+' : ''}${cell.return_pct.toFixed(2)}%`}
            </td>;
          })}
        </tr>)}</tbody></table>
      </div>
      {!instruments.length && <p className="text-sm text-fg-muted">{currentOnly && data.instruments.length
        ? (nl ? 'Huidige posities zijn voor de laatste week niet beschikbaar. Toon alle instrumenten om de opgeslagen geschiedenis te zien.' : 'Current holdings are unavailable for the latest week. Show all instruments to see saved history.')
        : (nl ? 'Nog geen wekelijkse koersen opgeslagen. Laad de geschiedenis om te beginnen.' : 'No weekly prices saved yet. Load history to get started.')}</p>}
      <p className="text-[11px] text-fg-subtle">{nl ? 'Een rendement vereist twee opeenvolgende weekopnamen van hetzelfde instrument. Ontbrekende weken blijven leeg. Aankopen en verkopen tussen twee opnamen zijn niet altijd zichtbaar.' : 'A return requires two consecutive weekly snapshots of the same instrument. Missing weeks stay blank. Trades between snapshots may not be visible.'}</p>
    </>}
  </div>;
}
