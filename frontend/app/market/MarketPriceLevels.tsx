'use client';

import { useState, type FormEvent } from 'react';
import { useLang } from '../../lib/i18n';
import { parseDrawdown, priceLevel, type MarketKey } from './priceLevels';
import type { usePriceLevels } from './usePriceLevels';

export default function MarketPriceLevels({ market, name, peak, latest, currency, levels }: {
  market: MarketKey; name: string; peak: number; latest: number; currency: string;
  levels: ReturnType<typeof usePriceLevels>;
}) {
  const [lang] = useLang();
  const nl = lang === 'nl';
  const [input, setInput] = useState('');
  const [error, setError] = useState('');
  const rows = levels.rows.filter(row => row.market === market).sort((a, b) => a.drawdown_pct - b.drawdown_pct);
  const money = (value: number) => value.toLocaleString(nl ? 'nl-NL' : 'en-GB', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  const percentage = (value: number) => value.toLocaleString(nl ? 'nl-NL' : 'en-GB', { maximumFractionDigits: 2 });
  async function add(event: FormEvent) {
    event.preventDefault();
    const value = parseDrawdown(input);
    if (value == null) { setError(nl ? 'Vul een percentage tussen 0,01 en 99,99 in.' : 'Enter a percentage between 0.01 and 99.99.'); return; }
    if (rows.some(row => row.drawdown_pct === value)) { setError(nl ? 'Dit niveau is al opgeslagen.' : 'This level is already saved.'); return; }
    setError('');
    if (await levels.update(market, value)) setInput('');
    else setError(nl ? 'Opslaan is niet gelukt. Probeer opnieuw.' : 'Could not save this level. Please try again.');
  }
  return <section className="rounded-xl border border-neutral-800/40 bg-card p-4 sm:p-5" aria-label={`${name} ${nl ? 'koersniveaus' : 'price levels'}`}>
    <div className="flex flex-wrap items-start justify-between gap-4">
      <div><h3 className="text-sm font-semibold text-fg-strong">{nl ? 'Mijn koersniveaus' : 'My price levels'}</h3>
        <p className="mt-1 text-xs text-fg-muted">{nl ? 'Stel een daling vanaf de hoogste stand (ATH) in. Bij een nieuwe top bewegen de niveaus mee.' : 'Set a drop from the all-time high (ATH). Levels move with a new high.'}</p></div>
      <form onSubmit={event => void add(event)} className="flex items-end gap-2">
        <label className="space-y-1 text-xs text-fg-muted" htmlFor={`${market}-drawdown`}>
          <span className="block">{nl ? 'Onder ATH (%)' : 'Below ATH (%)'}</span>
          <input id={`${market}-drawdown`} type="number" min="0.01" max="99.99" step="0.01" required
            inputMode="decimal" value={input} onChange={event => { setInput(event.target.value); setError(''); }} placeholder="10"
            disabled={!levels.ready || levels.saving}
            className="w-28 rounded-lg border border-neutral-700 bg-elevated px-3 py-2 text-sm text-fg-strong disabled:opacity-50" />
        </label>
        <button type="submit" disabled={!levels.ready || levels.saving}
          className="rounded-lg bg-accent-600 px-3 py-2 text-sm font-medium text-white disabled:opacity-50">{nl ? 'Toevoegen' : 'Add'}</button>
      </form>
    </div>
    {levels.error ? <p role="alert" className="mt-3 text-xs text-neg-400">{nl ? 'Uw koersniveaus konden niet worden geladen.' : 'Could not load your price levels.'}
      <button type="button" onClick={levels.retry} className="ml-2 underline">{nl ? 'Opnieuw proberen' : 'Retry'}</button></p>
      : !levels.ready ? <p role="status" className="mt-3 text-xs text-fg-muted">{nl ? 'Koersniveaus laden…' : 'Loading price levels…'}</p>
        : !rows.length ? <p className="mt-3 text-xs text-fg-muted">{nl ? 'Voeg een niveau toe, bijvoorbeeld 10% onder ATH.' : 'Add a level, for example 10% below ATH.'}</p> : null}
    {!!rows.length && <div className="mt-4 overflow-x-auto"><table className="w-full text-left text-sm">
      <thead className="text-xs text-fg-muted"><tr><th className="pb-2 font-medium">{nl ? 'Onder ATH' : 'Below ATH'}</th>
        <th className="pb-2 font-medium">{nl ? 'Koersniveau' : 'Target price'} · {currency}</th><th className="pb-2 font-medium">Status</th><th><span className="sr-only">{nl ? 'Verwijderen' : 'Remove'}</span></th></tr></thead>
      <tbody>{rows.map(row => {
        const level = priceLevel(peak, latest, row.drawdown_pct);
        return <tr key={row.drawdown_pct} className="border-t border-neutral-800/50">
          <td className="py-3 tabular-nums">−{percentage(row.drawdown_pct)}%</td>
          <td className="py-3 font-mono tabular-nums">{level ? money(level.target) : '—'}</td>
          <td className="py-3"><span className={`inline-flex rounded-md px-2 py-1 text-xs font-medium ${level?.hit ? 'bg-pos-500/10 text-pos-400' : 'bg-overlay/5 text-fg-muted'}`}>
            {level == null ? '—' : level.hit ? (nl ? 'Nu bereikt' : 'Hit now') : (nl ? 'Niet bereikt' : 'Not hit')}</span></td>
          <td className="py-3 text-right"><button type="button" disabled={levels.saving}
            aria-label={`${nl ? 'Verwijder' : 'Remove'} ${percentage(row.drawdown_pct)}% · ${name}`}
            className="text-xs text-fg-muted hover:text-neg-400 disabled:opacity-50" onClick={() => {
              setError(''); void levels.update(market, row.drawdown_pct, true).then(ok => {
                if (!ok) setError(nl ? 'Verwijderen is niet gelukt. Probeer opnieuw.' : 'Could not remove this level. Please try again.');
              });
            }}>{nl ? 'Verwijderen' : 'Remove'}</button></td>
        </tr>;
      })}</tbody></table>
      <p className="mt-2 text-xs text-fg-faint">{nl ? 'Status op basis van de laatst getoonde koers. Een eerder geraakt niveau kan nu weer hoger staan.' : 'Status uses the latest price shown. A level hit earlier may no longer be hit now.'}</p></div>}
    {error && <p role="alert" className="mt-3 text-xs text-neg-400">{error}</p>}
  </section>;
}
