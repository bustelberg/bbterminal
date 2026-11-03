'use client';

import { useCallback, useEffect, useId, useMemo, useRef, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { startJob } from '../../../lib/stores/jobs';
import { invalidateReadCache } from '../../../lib/readCache';
import { useLang } from '../../../lib/i18n';
import { instrumentLabel, searchInstruments, weeklyBars, type WeeklyReturns, type InstrumentOption } from './weeklyReturns';

const day = (value: string | null, nl: boolean) => value
  ? new Date(`${value}T12:00:00`).toLocaleDateString(nl ? 'nl-NL' : 'en-GB', { day: 'numeric', month: 'short', year: 'numeric' }) : '—';
const percent = (value: number) => `${value > 0 ? '+' : ''}${value.toFixed(2)}%`;
const weekStart = (target: string) => {
  const start = new Date(`${target}T12:00:00Z`);
  start.setUTCDate(start.getUTCDate() - (start.getUTCDay() + 6) % 7);
  return start.toISOString().slice(0, 10);
};

function InstrumentPicker({ options, selected, onSelect, nl }: {
  options: InstrumentOption[]; selected?: InstrumentOption; onSelect: (instrument: InstrumentOption) => void; nl: boolean;
}) {
  const listId = useId();
  const input = useRef<HTMLInputElement>(null);
  const list = useRef<HTMLDivElement>(null);
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState('');
  const [active, setActive] = useState(0);
  const filtered = searchInstruments(options, query);
  useEffect(() => {
    if (open) list.current?.querySelector(`[data-index="${active}"]`)?.scrollIntoView({ block: 'nearest' });
  }, [active, open]);
  function select(instrument: InstrumentOption) {
    onSelect(instrument);
    input.current?.focus();
    setOpen(false);
    setQuery('');
  }
  function show() { if (!open) { setQuery(''); setActive(0); setOpen(true); } }
  return <div className="relative w-72 max-w-full" onBlur={(event) => {
    if (!event.currentTarget.contains(event.relatedTarget)) { setOpen(false); setQuery(''); }
  }}>
    <div className="relative flex items-center rounded-lg border border-neutral-800/40 bg-elevated focus-within:border-accent-500/60">
      <input ref={input} role="combobox" aria-label={nl ? 'Instrument zoeken' : 'Search instruments'}
        aria-expanded={open} aria-controls={listId} aria-autocomplete="list"
        aria-activedescendant={open && filtered[active] ? `${listId}-${active}` : undefined}
        autoComplete="off" value={open ? query : selected?.name ?? ''}
        placeholder={open ? (nl ? 'Zoek op naam of ISIN…' : 'Search name or ISIN…') : (nl ? 'Kies een instrument' : 'Choose an instrument')}
        title={selected ? instrumentLabel(selected) : undefined}
        onFocus={show} onClick={show}
        onChange={(event) => { setQuery(event.target.value); setActive(0); setOpen(true); }}
        onKeyDown={(event) => {
          if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
            event.preventDefault();
            if (!open) show();
            else setActive((index) => Math.max(0, Math.min(filtered.length - 1, index + (event.key === 'ArrowDown' ? 1 : -1))));
          } else if (event.key === 'Enter' && open) {
            event.preventDefault(); if (filtered[active]) select(filtered[active]);
          } else if (event.key === 'Escape') { event.preventDefault(); setOpen(false); setQuery(''); }
        }}
        className="w-full min-w-0 truncate bg-transparent py-2 pl-3 pr-9 text-sm text-fg outline-none" />
      <svg aria-hidden="true" viewBox="0 0 16 16" className="pointer-events-none absolute right-3 h-4 w-4 text-fg-muted" fill="none" stroke="currentColor" strokeWidth="1.5"><path d="m4 6 4 4 4-4" /></svg>
    </div>
    {open && <div ref={list} id={listId} role="listbox" aria-label={nl ? 'Instrumenten' : 'Instruments'}
      className="absolute left-0 top-full z-50 mt-1 max-h-64 w-full overflow-y-auto overscroll-contain rounded-lg border border-neutral-800/40 bg-elevated p-1 shadow-xl">
      {filtered.length ? filtered.map((instrument, index) => <button key={instrument.id} type="button"
        id={`${listId}-${index}`} data-index={index} role="option" aria-selected={instrument.id === selected?.id} tabIndex={-1}
        onClick={() => select(instrument)}
        className={`block w-full rounded-md px-3 py-2 text-left hover:bg-accent-500/10 ${active === index ? 'bg-accent-500/10' : ''}`}>
        <span className={`block text-sm ${instrument.id === selected?.id ? 'text-accent-300' : 'text-fg'}`}>{instrument.name}</span>
        <span className="block text-xs text-fg-muted">{[instrument.isin, instrument.currency].filter(Boolean).join(' · ')}</span>
      </button>) : <p className="px-3 py-3 text-xs text-fg-muted">{nl ? 'Geen instrumenten gevonden' : 'No instruments found'}</p>}
    </div>}
  </div>;
}

type LinkedWeeks = {
  hovered: string | null; onHover: (date: string | null) => void;
  window: [string, string] | null; onWindow: (window: [string, string] | null) => void;
};

function ReturnChart({ data, nl, scale, linked }: { data: WeeklyReturns; nl: boolean; scale: number; linked: LinkedWeeks }) {
  const drag = useRef<{ start: string; previous: [string, string] | null } | null>(null);
  const { hovered, onHover, window, onWindow } = linked;
  const found = data.periods.findIndex((period) => period.target_date === hovered);
  const active = found < 0 ? null : found;
  const chart = useMemo(() => {
    const bars = weeklyBars(data.selected?.weeks ?? []);
    const limit = scale;
    const left = 65, right = 925, top = 25, bottom = 260;
    const mid = (top + bottom) / 2;
    const step = (right - left) / Math.max(1, bars.length);
    const width = Math.min(18, step * .65);
    const y = (value: number) => mid - value / limit * (bottom - top) / 2;
    const weekLabel = (target: string) => {
      const monday = weekStart(target);
      return `${day(monday < data.start_date ? data.start_date : monday, nl)} – ${day(target, nl)}`;
    };
    const labels: Record<string, string> = nl ? {
      added: 'Toegevoegd', reentered: 'Opnieuw gekocht', sold: 'Verkocht', not_held: 'Niet in bezit',
      missing_snapshot: 'Geen koers', missing_previous: 'Vorige koers ontbreekt', missing_price: 'Geen koers',
    } : { added: 'Added', reentered: 'Bought again', sold: 'Sold', not_held: 'Not held',
      missing_snapshot: 'No price', missing_previous: 'Previous price missing', missing_price: 'No price' };
    const content = <>
      {[-limit, 0, limit].map((value) => <g key={value}>
        <line x1={left} x2={right} y1={y(value)} y2={y(value)} stroke="currentColor" opacity={value === 0 ? .4 : .12} />
        <text x={left - 9} y={y(value) + 4} textAnchor="end" fill="currentColor" fontSize="11">{value.toFixed(1)}%</text>
      </g>)}
      {bars.map((value, index) => {
        const period = data.periods[index];
        const x = left + step * (index + .5);
        const cell = data.selected?.weeks[index];
        const title = `${weekLabel(period.target_date)} · ${value == null ? (labels[cell?.status ?? ''] ?? '—') : percent(value)}`;
        return <g key={period.target_date} tabIndex={0} aria-label={title}
          onFocus={() => onHover(period.target_date)} onBlur={() => onHover(null)}
          className="outline-none">
          {value == null ? <circle cx={x} cy={mid} r="1.5" fill="currentColor" opacity=".3" />
          : <rect x={x - width / 2} y={value >= 0 ? y(value) : mid}
              width={width} height={Math.max(1, Math.abs(y(value) - mid))} rx="1"
              className={value > 0 ? 'fill-current text-pos-300' : value < 0 ? 'fill-current text-neg-300' : 'fill-current text-fg-subtle'}>
          </rect>}
          <rect x={x - step / 2} y={top} width={step} height={bottom - top} fill="transparent" />
        </g>;
      })}
      <text x={left} y="292" fill="currentColor" fontSize="11">{nl ? '1 jan 2026' : '1 Jan 2026'}</text>
      {data.periods.map((period, index) => {
        if (!index || index > bars.length - 3 || period.target_date.slice(0, 7) === data.periods[index - 1].target_date.slice(0, 7)) return null;
        const label = new Date(`${period.target_date}T12:00:00`).toLocaleDateString(nl ? 'nl-NL' : 'en-GB', { month: 'short' });
        return <text key={period.target_date} x={left + step * (index + .5)} y="292" textAnchor="middle" fill="currentColor" fontSize="11">{label}</text>;
      })}
      <text x={right} y="292" textAnchor="end" fill="currentColor" fontSize="11">{day(data.through, nl)}</text>
      {!bars.some((value) => value != null) && <text x="495" y="105" textAnchor="middle" fill="currentColor" fontSize="13">{nl ? 'Nog geen wekelijkse rendementen beschikbaar' : 'No weekly returns available yet'}</text>}
    </>;
    return { content, bars, labels, weekLabels: data.periods.map((period) => weekLabel(period.target_date)), left, right, step, top, bottom, mid, width, limit };
  }, [data, nl, scale, onHover]);
  const { bars, labels, left, right, step, top, bottom, mid, width, limit } = chart;
  const activePeriod = active === null ? undefined : data.periods[active];
  const activeValue = active === null ? null : bars[active];
  const x = left + step * ((active ?? 0) + .5);
  const tooltipX = Math.max(left, Math.min(right - 540, x - 270));
  const barY = mid - (activeValue ?? 0) / limit * (bottom - top) / 2;
  function dateAt(svg: SVGSVGElement, clientX: number, clientY: number, clamp = false) {
    const matrix = svg.getScreenCTM();
    if (!matrix || !data.periods.length) return null;
    const point = svg.createSVGPoint(); point.x = clientX; point.y = clientY;
    const position = point.matrixTransform(matrix.inverse());
    if (!clamp && (position.x < left || position.x > right || position.y < top || position.y > bottom)) return null;
    return data.periods[Math.max(0, Math.min(data.periods.length - 1, Math.floor((position.x - left) / step)))].target_date;
  }
  function extend(date: string) {
    if (drag.current) onWindow(date < drag.current.start ? [date, drag.current.start] : [drag.current.start, date]);
  }
  const selectedIndices = window ? data.periods.flatMap((period, index) =>
    period.target_date >= window[0] && period.target_date <= window[1] ? [index] : []) : [];
  return <svg viewBox="0 0 960 320" role="img" aria-label={`${data.selected?.name ?? ''} · ${nl ? 'Wekelijkse koersveranderingen vanaf 1 januari 2026' : 'Weekly price changes from 1 January 2026'}`} className="w-full min-w-0 touch-none select-none text-fg-subtle"
    onPointerLeave={() => { if (!drag.current) onHover(null); }}
    onPointerDown={(event) => {
      if (event.button !== 0) return;
      const date = dateAt(event.currentTarget, event.clientX, event.clientY);
      if (!date) return;
      event.preventDefault(); event.currentTarget.setPointerCapture(event.pointerId);
      drag.current = { start: date, previous: window }; onHover(date); onWindow([date, date]);
    }}
    onPointerMove={(event) => {
      const date = dateAt(event.currentTarget, event.clientX, event.clientY, !!drag.current);
      onHover(date); if (date) extend(date);
    }}
    onPointerUp={(event) => {
      if (!drag.current) return;
      const date = dateAt(event.currentTarget, event.clientX, event.clientY, true);
      if (date) extend(date);
      drag.current = null;
      event.currentTarget.releasePointerCapture(event.pointerId);
      onHover(dateAt(event.currentTarget, event.clientX, event.clientY));
    }}
    onPointerCancel={() => { if (drag.current) onWindow(drag.current.previous); drag.current = null; onHover(null); }}
    onLostPointerCapture={() => { drag.current = null; }}>
    {chart.content}
    {!!selectedIndices.length && <rect pointerEvents="none" x={left + selectedIndices[0] * step} y={top}
      width={selectedIndices.length * step} height={bottom - top}
      className="fill-accent-500/15 stroke-accent-500" strokeWidth="2" />}
    {activePeriod && <g pointerEvents="none">
      <rect x={x - step / 2} y={top} width={step} height={bottom - top} fill="currentColor" opacity=".08" />
      {activeValue != null && <rect x={x - width / 2} y={Math.min(mid, barY)} width={width}
        height={Math.max(1, Math.abs(barY - mid))} rx="1" fill="none" stroke="currentColor" strokeWidth="2"
        className={activeValue > 0 ? 'text-pos-300' : activeValue < 0 ? 'text-neg-300' : ''} />}
    </g>}
    {activePeriod && <g role="tooltip" pointerEvents="none">
      <rect x={tooltipX} y="2" width="540" height="100" rx="8" className="fill-elevated stroke-current" strokeOpacity=".25" />
      <text x={tooltipX + 20} y="34" fill="currentColor" fontSize="24">{chart.weekLabels[active!]}</text>
      <text x={tooltipX + 20} y="82" fontSize={activeValue == null ? 24 : 40} fontWeight="600"
        className={`fill-current ${activeValue != null && activeValue > 0 ? 'text-pos-300' : activeValue != null && activeValue < 0 ? 'text-neg-300' : ''}`}>
        {activeValue == null ? (labels[data.selected?.weeks[active!]?.status ?? ''] ?? '—') : percent(activeValue)}
      </text>
    </g>}
  </svg>;
}

function InstrumentReturnSlot({ portefeuille, scale, onData, linked }: {
  portefeuille: string; scale: number; onData: (data: WeeklyReturns | null) => void; linked: LinkedWeeks;
}) {
  const [lang] = useLang();
  const nl = lang === 'nl';
  const [selection, setSelection] = useState<{ account: string; id: string } | null>(null);
  const selectedId = selection && selection.account === portefeuille ? selection.id : null;
  const [revision, setRevision] = useState(0);
  const [result, setResult] = useState<{ account: string; key: string; data?: WeeklyReturns; error?: string } | null>(null);
  const [busy, setBusy] = useState(false);
  const [notice, setNotice] = useState<{ id: string; text: string } | null>(null);
  const key = `${portefeuille}|${selectedId ?? ''}|${revision}`;
  const options = result && result.account === portefeuille ? result.data?.options ?? [] : [];
  const chosen = options.find((item) => item.id === selectedId);
  const data = selectedId && result?.key === key && !result.error ? result.data : undefined;

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
        if (active) {
          setResult({ account: portefeuille, key, data: payload });
          onData(selectedId ? payload : null);
        }
      } catch (error) {
        if (active) setResult((previous) => ({ account: portefeuille, key,
          data: previous?.account === portefeuille ? previous.data : undefined,
          error: error instanceof Error ? error.message : String(error) }));
      }
    })();
    return () => { active = false; controller.abort(); };
  }, [key, portefeuille, selectedId, onData]);

  function choose(instrument: InstrumentOption) {
    if (instrument.id === selectedId) return;
    onData(null);
    if (portefeuille) setSelection({ account: portefeuille, id: instrument.id });
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
  return <div className="flex h-full min-h-0 min-w-0 flex-col rounded-xl border border-neutral-800/30 bg-elevated/30 p-3">
    <div className="flex h-20 shrink-0 flex-col items-start gap-2">
      <InstrumentPicker options={options} selected={chosen} onSelect={choose} nl={nl} />
      <button type="button" disabled={busy || !chosen} onClick={() => void loadHistory()}
        className="rounded-lg border border-neutral-800/40 bg-elevated px-3 py-2 text-xs font-medium text-fg-muted hover:text-accent-300 disabled:opacity-50">
        {busy ? (nl ? 'Laden…' : 'Loading…') : (nl ? 'Ontbrekende geschiedenis laden' : 'Load missing history')}
      </button>
    </div>
    <div className="h-9 shrink-0 overflow-y-auto">
      {result?.key === key && result.error && <p role="alert" className="text-xs text-neg-300">{result.error}</p>}
      {notice && notice.id === chosen?.id && <p role="alert" className="text-xs text-neg-300">{notice.text}</p>}
    </div>
    <div className="flex min-h-0 flex-1 items-center [&>svg]:h-full [&>svg]:w-full">
      {data ? <ReturnChart data={data} nl={nl} scale={scale} linked={linked} /> : <svg viewBox="0 0 960 320" className="h-full w-full text-fg-subtle" role="img"
        aria-label={nl ? 'Lege rendementsgrafiek' : 'Empty return chart'}>
        <line x1="65" x2="925" y1="142.5" y2="142.5" stroke="currentColor" opacity=".4" />
        <text x="56" y="146.5" textAnchor="end" fill="currentColor" fontSize="11">0%</text>
        <text x="495" y="110" textAnchor="middle" fill="currentColor" fontSize="16">
          {selectedId ? (nl ? 'Rendementen laden…' : 'Loading returns…') : (nl ? 'Kies een instrument' : 'Choose an instrument')}
        </text>
        <text x="65" y="292" fill="currentColor" fontSize="11">{nl ? '1 jan 2026' : '1 Jan 2026'}</text>
      </svg>}
    </div>
  </div>;
}

export default function WeeklyReturnsPanel({ portefeuille }: { portefeuille?: string | null }) {
  const [lang] = useLang();
  const [first, setFirst] = useState<WeeklyReturns | null>(null);
  const [second, setSecond] = useState<WeeklyReturns | null>(null);
  const [hovered, onHover] = useState<string | null>(null);
  const [window, setWindow] = useState<[string, string] | null>(null);
  const onWindow = useCallback((next: [string, string] | null) => {
    setWindow((previous) => previous?.[0] === next?.[0] && previous?.[1] === next?.[1] ? previous : next);
  }, []);
  const linked = { hovered, onHover, window, onWindow };
  const onFirst = useCallback((data: WeeklyReturns | null) => setFirst(data), []);
  const onSecond = useCallback((data: WeeklyReturns | null) => setSecond(data), []);
  const scale = Math.max(1, ...[first, second].flatMap((data) =>
    weeklyBars(data?.selected?.weeks ?? []).map((value) => Math.abs(value ?? 0)))) * 1.1;
  if (!portefeuille) return <p className="text-sm text-fg-muted">{lang === 'nl' ? 'Koppel een AIRS-portefeuille om rendementen te zien.' : 'Link an AIRS portfolio to see returns.'}</p>;
  return <div className="flex h-full min-h-0 flex-col gap-2">
    <div className="flex h-6 shrink-0 items-center justify-between gap-2 text-xs text-fg-muted">
      <span>{window ? `${day(weekStart(window[0]) < '2026-01-01' ? '2026-01-01' : weekStart(window[0]), lang === 'nl')} – ${day(window[1], lang === 'nl')}` : (lang === 'nl' ? 'Sleep om weken te vergelijken' : 'Drag to compare weeks')}</span>
      {window && <button type="button" className="text-accent-300 hover:underline" onClick={() => onWindow(null)}>{lang === 'nl' ? 'Selectie wissen' : 'Clear selection'}</button>}
    </div>
    <div className="grid min-h-0 flex-1 grid-cols-2 gap-3">
      <InstrumentReturnSlot portefeuille={portefeuille} scale={scale} onData={onFirst} linked={linked} />
      <InstrumentReturnSlot portefeuille={portefeuille} scale={scale} onData={onSecond} linked={linked} />
    </div>
  </div>;
}
