'use client';

import { useEffect, useId, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { startJob } from '../../../lib/stores/jobs';
import { invalidateReadCache } from '../../../lib/readCache';
import { useLang } from '../../../lib/i18n';
import { instrumentLabel, searchInstruments, weeklyBars, type WeeklyReturns, type InstrumentOption } from './weeklyReturns';

const day = (value: string | null, nl: boolean) => value
  ? new Date(`${value}T12:00:00`).toLocaleDateString(nl ? 'nl-NL' : 'en-GB', { day: 'numeric', month: 'short', year: 'numeric' }) : '—';
const percent = (value: number) => `${value > 0 ? '+' : ''}${value.toFixed(2)}%`;

function ReturnChart({ data, nl }: { data: WeeklyReturns; nl: boolean }) {
  const bars = weeklyBars(data.selected?.weeks ?? []);
  const limit = Math.max(1, ...bars.map((value) => Math.abs(value ?? 0))) * 1.1;
  const left = 65, right = 925, top = 25, bottom = 260;
  const mid = (top + bottom) / 2;
  const step = (right - left) / Math.max(1, bars.length);
  const width = Math.min(18, step * .65);
  const y = (value: number) => mid - value / limit * (bottom - top) / 2;
  const labels: Record<string, string> = nl ? {
    added: 'Toegevoegd', reentered: 'Opnieuw gekocht', sold: 'Verkocht', not_held: 'Niet in bezit',
    missing_snapshot: 'Geen koers', missing_previous: 'Vorige koers ontbreekt', missing_price: 'Geen koers',
  } : { added: 'Added', reentered: 'Bought again', sold: 'Sold', not_held: 'Not held',
    missing_snapshot: 'No price', missing_previous: 'Previous price missing', missing_price: 'No price' };
  return <svg viewBox="0 0 960 320" role="img" aria-label={`${data.selected?.name ?? ''} · ${nl ? 'Wekelijkse koersveranderingen vanaf 1 januari 2026' : 'Weekly price changes from 1 January 2026'}`} className="w-full min-w-0 text-fg-subtle">
    {[-limit, 0, limit].map((value) => <g key={value}>
      <line x1={left} x2={right} y1={y(value)} y2={y(value)} stroke="currentColor" opacity={value === 0 ? .4 : .12} />
      <text x={left - 9} y={y(value) + 4} textAnchor="end" fill="currentColor" fontSize="11">{value.toFixed(1)}%</text>
    </g>)}
    {bars.map((value, index) => {
      const period = data.periods[index];
      const x = left + step * (index + .5);
      const cell = data.selected?.weeks[index];
      const title = `${day(period.target_date, nl)} · ${value == null ? (labels[cell?.status ?? ''] ?? '—') : percent(value)}\n${day(period.from_date, nl)} → ${day(period.as_of_date, nl)}`;
      return value == null ? <circle key={period.target_date} cx={x} cy={mid} r="1.5" fill="currentColor" opacity=".3"><title>{title}</title></circle>
        : <rect key={period.target_date} x={x - width / 2} y={value >= 0 ? y(value) : mid}
            width={width} height={Math.max(1, Math.abs(y(value) - mid))} rx="1"
            className={value > 0 ? 'fill-current text-pos-300' : value < 0 ? 'fill-current text-neg-300' : 'fill-current text-fg-subtle'}>
          <title>{title}</title>
        </rect>;
    })}
    <text x={left} y="292" fill="currentColor" fontSize="11">{nl ? '1 jan 2026' : '1 Jan 2026'}</text>
    {data.periods.map((period, index) => {
      if (!index || index > bars.length - 3 || period.target_date.slice(0, 7) === data.periods[index - 1].target_date.slice(0, 7)) return null;
      const label = new Date(`${period.target_date}T12:00:00`).toLocaleDateString(nl ? 'nl-NL' : 'en-GB', { month: 'short' });
      return <text key={period.target_date} x={left + step * (index + .5)} y="292" textAnchor="middle" fill="currentColor" fontSize="11">{label}</text>;
    })}
    <text x={right} y="292" textAnchor="end" fill="currentColor" fontSize="11">{day(data.through, nl)}</text>
    {!bars.some((value) => value != null) && <text x="495" y="105" textAnchor="middle" fill="currentColor" fontSize="13">{nl ? 'Nog geen wekelijkse rendementen beschikbaar' : 'No weekly returns available yet'}</text>}
  </svg>;
}

export default function WeeklyReturnsPanel({ portefeuille }: { portefeuille?: string | null }) {
  const [lang] = useLang();
  const nl = lang === 'nl';
  const listId = useId();
  const [selection, setSelection] = useState<{ account: string; id: string } | null>(null);
  const selectedId = selection && selection.account === portefeuille ? selection.id : null;
  const [query, setQuery] = useState<string | null>(null);
  const [revision, setRevision] = useState(0);
  const [result, setResult] = useState<{ account: string; key: string; data?: WeeklyReturns; error?: string } | null>(null);
  const [busy, setBusy] = useState(false);
  const [notice, setNotice] = useState<{ id: string; text: string } | null>(null);
  const key = `${portefeuille}|${selectedId ?? ''}|${revision}`;
  const options = result && result.account === portefeuille ? result.data?.options ?? [] : [];
  const chosen = options.find((item) => item.id === selectedId) ?? (selectedId ? undefined : options[0]);
  const data = result?.key === key && !result.error ? result.data : undefined;

  useEffect(() => {
    if (!portefeuille) return;
    const controller = new AbortController();
    let active = true;
    const url = `${API_URL}/api/airs/accounts/${encodeURIComponent(portefeuille)}/weekly-returns${selectedId ? `?instrument_id=${encodeURIComponent(selectedId)}` : ''}`;
    void (async () => {
      try {
        const response = await apiFetch(url, { signal: controller.signal });
        const payload = await response.json();
        if (!response.ok) throw new Error(payload.detail ?? 'Could not load weekly returns');
        if (active) setResult({ account: portefeuille, key, data: payload });
      } catch (error) {
        if (active) setResult((previous) => ({ account: portefeuille, key,
          data: previous?.account === portefeuille ? previous.data : undefined,
          error: error instanceof Error ? error.message : String(error) }));
      }
    })();
    return () => { active = false; controller.abort(); };
  }, [key, portefeuille, selectedId]);

  function choose(instrument: InstrumentOption) {
    if (portefeuille) setSelection({ account: portefeuille, id: instrument.id });
    setQuery(null);
  }

  async function loadHistory() {
    if (!portefeuille || !chosen || busy) return;
    const instrument = chosen;
    choose(instrument);
    setBusy(true); setNotice(null);
    try {
      const { done } = await startJob(`${API_URL}/api/airs/accounts/${encodeURIComponent(portefeuille)}/weekly-returns/job?instrument_id=${encodeURIComponent(instrument.id)}`,
        `${instrument.name} · AIRS`);
      const job = await done;
      invalidateReadCache('selected AIRS instrument history saved');
      setRevision((value) => value + 1);
      if (job.status !== 'done') setNotice({ id: instrument.id, text: nl ? 'Niet alles kon worden geladen. Zie Details.' : 'Some history could not be loaded. See Details.' });
    } catch (error) { setNotice({ id: instrument.id, text: error instanceof Error ? error.message : String(error) }); }
    finally { setBusy(false); }
  }

  if (!portefeuille) return <p className="text-sm text-fg-muted">{nl ? 'Koppel een AIRS-portefeuille om rendementen te zien.' : 'Link an AIRS portfolio to see returns.'}</p>;
  return <div className="space-y-4">
    <div className="flex flex-wrap items-center gap-3">
      <input list={listId} aria-label={nl ? 'Instrument zoeken' : 'Search instruments'} autoComplete="off"
        value={query ?? chosen?.name ?? ''} placeholder={nl ? 'Zoek een instrument…' : 'Search an instrument…'}
        title={chosen ? instrumentLabel(chosen) : undefined}
        onFocus={(event) => event.currentTarget.select()}
        onChange={(event) => { const value = event.target.value; setQuery(value); const exact = options.find((option) => instrumentLabel(option) === value); if (exact) choose(exact); }}
        onBlur={() => setQuery(null)}
        onKeyDown={(event) => { if (event.key === 'Enter' && query) { const found = searchInstruments(options, query); if (found.length === 1) { event.preventDefault(); choose(found[0]); } } }}
        className="min-w-56 flex-1 rounded-lg border border-neutral-800/40 bg-elevated px-3 py-2 text-sm text-fg outline-none focus:border-accent-500/60" />
      <datalist id={listId}>{options.map((instrument) => <option key={instrument.id} value={instrumentLabel(instrument)} />)}</datalist>
      <button type="button" disabled={busy || !chosen || query !== null} onClick={() => void loadHistory()}
        className="rounded-lg border border-neutral-800/40 bg-elevated px-3 py-2 text-xs font-medium text-fg-muted hover:text-accent-300 disabled:opacity-50">
        {busy ? (nl ? 'Laden…' : 'Loading…') : (nl ? 'Ontbrekende geschiedenis laden' : 'Load missing history')}
      </button>
    </div>
    {result?.key === key && result.error && <p role="alert" className="text-xs text-neg-300">{result.error}</p>}
    {notice && notice.id === chosen?.id && <p role="alert" className="text-xs text-neg-300">{notice.text}</p>}
    {data ? <ReturnChart data={data} nl={nl} /> : !(result?.key === key && result.error)
      ? <p role="status" className="py-16 text-center text-xs text-fg-muted">{nl ? 'Rendementen laden…' : 'Loading returns…'}</p> : null}
  </div>;
}
