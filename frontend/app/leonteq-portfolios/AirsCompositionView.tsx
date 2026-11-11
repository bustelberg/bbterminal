'use client';

import { useEffect, useRef, useState } from 'react';
import { apiFetch } from '../../lib/apiFetch';
import { API_URL } from '../../lib/apiUrl';
import { useLang } from '../../lib/i18n';
import { refreshRequest } from './refreshProgress';

type Position = {
  id?: number | null;
  Fonds: string | null; Percentage: number | null; ISINCode: string | null; valuta: string | null;
  Beleggingscategorie: string | null; Beleggingssector: string | null; regio: string | null; afmCategorie: string | null;
};
export type AirsComposition = {
  portfolio: string; portfolio_id: number; datum: string | null; dates: string[]; rows: Position[];
  cached_at?: string; retrieved_at?: string; no_fixed_model?: boolean;
  snapshot_saved?: boolean;
  already_at_target?: boolean;
  cache_upgrade_available?: boolean;
};

async function read(id: number, datum = '', refresh = false, name?: string, nl = false): Promise<AirsComposition> {
  const params = new URLSearchParams();
  if (datum) params.set('datum', datum);
  if (refresh) params.set('refresh', 'true');
  const url = `${API_URL}/api/leonteq/airs/model-portfolios/${id}/composition${params.size ? `?${params}` : ''}`;
  const init = { cache: 'no-store' as const, noReadCache: true };
  const response = refresh ? await refreshRequest(url, init, `AIRS · ${name ?? id}`, name, nl) : await apiFetch(url, init);
  if (!response.ok) throw new Error(response.status === 409 ? 'busy' : 'failed');
  return await response.json() as AirsComposition;
}

export default function AirsCompositionView({ id, name, cache, onBack }: {
  id: number; name: string; cache: Map<string, AirsComposition>; onBack: () => void;
}) {
  const [lang] = useLang();
  const nl = lang === 'nl';
  const [data, setData] = useState<AirsComposition | null>(() => cache.get(`${id}:latest`) ?? null);
  const [busy, setBusy] = useState(() => !cache.has(`${id}:latest`));
  const [datum, setDatum] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [editing, setEditing] = useState<{ row: Position; value: string } | null>(null);
  const [saving, setSaving] = useState(false);
  const [editError, setEditError] = useState<string | null>(null);
  const [saved, setSaved] = useState(false);
  const generation = useRef(0);
  const alive = useRef(true);
  useEffect(() => {
    alive.current = true;
    const request = ++generation.current;
    if (!cache.has(`${id}:latest`)) {
      void read(id).then((result) => {
        cache.set(`${id}:latest`, result);
        if (alive.current && request === generation.current) setData(result);
      }).catch((cause: Error) => {
        if (alive.current && request === generation.current) setError(cause.message);
      }).finally(() => {
        if (alive.current && request === generation.current) setBusy(false);
      });
    }
    return () => { alive.current = false; };
  }, [id, cache]);

  async function load(nextDate = datum, refresh = false) {
    setEditing(null); setEditError(null); setSaved(false);
    const request = ++generation.current;
    const key = `${id}:${nextDate || 'latest'}`;
    const saved = cache.get(key);
    setDatum(nextDate); setError(null);
    setData(saved ?? (nextDate === datum ? data : null));
    if (saved && !refresh) { setBusy(false); return; }
    setBusy(true);
    try {
      const result = await read(id, nextDate, refresh, name, nl);
      cache.set(key, result);
      if (alive.current && request === generation.current) setData(result);
    } catch (cause) {
      if (alive.current && request === generation.current) setError(cause instanceof Error ? cause.message : 'failed');
    } finally {
      if (alive.current && request === generation.current) setBusy(false);
    }
  }

  async function savePercentage() {
    if (!editing || !data?.datum || !editing.row.id || saving) return;
    const value = editing.value.trim().replace(',', '.');
    const percentage = Number(value);
    if (!/^\d+(?:\.\d{1,6})?$/.test(value) || !Number.isFinite(percentage) || percentage < 0 || percentage > 100) {
      setEditError(nl ? 'Vul een percentage van 0 tot 100 in, met maximaal 6 decimalen.' : 'Enter a percentage from 0 to 100 with at most 6 decimal places.');
      return;
    }
    setSaving(true); setEditError(null); setSaved(false);
    try {
      const response = await apiFetch(`${API_URL}/api/leonteq/airs/model-portfolios/${id}/positions/${editing.row.id}/percentage`, {
        method: 'PATCH', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ datum: data.datum, expected_percentage: editing.row.Percentage, percentage }),
      });
      if (!response.ok) {
        const failure = await response.json().catch(() => null) as { detail?: string } | null;
        const code = failure?.detail;
        if (code === 'percentage_changed' || code === 'row_changed') throw new Error(nl
          ? 'Deze rij is in AIRS gewijzigd. Vernieuw de samenstelling en probeer opnieuw.'
          : 'This row changed in AIRS. Refresh the composition and try again.');
        if (code === 'save_unverified') throw new Error(nl
          ? 'De wijziging kan zijn opgeslagen, maar kon niet worden bevestigd. Vernieuw eerst de samenstelling.'
          : 'The change may have been saved but could not be confirmed. Refresh the composition before trying again.');
        throw new Error(nl ? 'AIRS heeft de wijziging niet opgeslagen. Controleer de waarde en probeer opnieuw.' : 'AIRS did not save the change. Check the value and try again.');
      }
      const result = await response.json() as AirsComposition;
      for (const key of cache.keys()) if (key.startsWith(`${id}:`)) cache.delete(key);
      cache.set(`${id}:${result.datum}`, result);
      if (alive.current) { setData(result); setDatum(result.datum ?? ''); setEditing(null); setSaved(true); }
    } catch (cause) {
      // Discard local copies after an uncertain update so reopening forces a read.
      for (const key of cache.keys()) if (key.startsWith(`${id}:`)) cache.delete(key);
      if (alive.current) setEditError(cause instanceof Error ? cause.message : (nl ? 'Opslaan mislukt.' : 'Saving failed.'));
    } finally {
      if (alive.current) setSaving(false);
    }
  }

  const dates = Array.from(new Set([...(data?.dates ?? []), ...(data?.datum ? [data.datum] : []), ...(datum ? [datum] : [])])).sort().reverse();
  const timestamp = data?.cached_at ?? data?.retrieved_at;
  return <section className="space-y-4" aria-labelledby="airs-composition-heading" aria-busy={busy}>
    <button type="button" disabled={saving} onClick={onBack} className="text-sm text-accent-300 hover:underline disabled:opacity-50">← {nl ? 'Alle modelportefeuilles' : 'All model portfolios'}</button>
    <div className="flex flex-wrap items-start justify-between gap-4">
      <div><h2 id="airs-composition-heading" className="text-xl font-semibold">{name} · {nl ? 'Samenstelling' : 'Composition'}</h2>
        {data?.datum && <p className="mt-1 text-sm text-fg-muted">{nl ? 'Samenstellingsdatum' : 'Composition date'}: {data.datum}</p>}
        {timestamp && <p className="mt-1 text-xs text-fg-muted">{data?.cached_at ? (nl ? 'Opgeslagen op' : 'Saved at') : (nl ? 'Opgehaald op' : 'Retrieved at')} {new Date(timestamp).toLocaleString(nl ? 'nl-NL' : 'en-GB')}</p>}
      </div>
      <div className="flex flex-wrap items-center gap-3">
        {dates.length > 0 && <label className="flex items-center gap-2 text-sm text-fg-muted">Datum
          <select value={datum} disabled={busy || saving} onChange={(event) => void load(event.target.value)} className="rounded-lg border border-neutral-800/40 bg-elevated px-3 py-2 text-fg">
            <option value="">{nl ? 'Nieuwste beschikbare' : 'Latest available'}</option>
            {dates.map((date) => <option key={date} value={date}>{date}</option>)}
          </select>
        </label>}
        <button type="button" disabled={busy || saving} onClick={() => void load(datum, true)} className="rounded-lg border border-neutral-800/40 bg-elevated px-3 py-2 text-sm disabled:opacity-50">{nl ? 'Vernieuwen uit AIRS' : 'Refresh from AIRS'}</button>
      </div>
    </div>
    <div role="status" aria-live="polite" className="text-sm text-fg-muted">{busy ? (nl ? 'Samenstelling laden; beschikbare gegevens blijven zichtbaar…' : 'Loading composition; available data remains visible…') : null}</div>
    {editing && <p className="text-sm text-fg-muted">{nl ? 'Opslaan wijzigt alleen dit percentage in AIRS voor' : 'Saving changes only this percentage in AIRS for'} {data?.datum}. {nl ? 'Andere gewichten worden niet automatisch aangepast.' : 'Other weights are not adjusted automatically.'}</p>}
    {editError && <p role="alert" className="text-sm text-neg-300">{editError}</p>}
    {saved && <p role="status" className="text-sm text-pos-300">{nl ? 'Percentage opgeslagen en gecontroleerd in AIRS.' : 'Percentage saved and verified in AIRS.'}</p>}
    {data?.snapshot_saved === false && <p className="text-sm text-fg-muted">{nl ? 'Opgeslagen in AIRS. De lokale cache wordt bij de volgende keer openen opnieuw geladen.' : 'Saved in AIRS. The local cache will reload on the next opening.'}</p>}
    {data?.cache_upgrade_available && <p className="text-sm text-fg-muted">{nl ? 'Opgeslagen posities worden direct getoond. Kies Vernieuwen uit AIRS voor de volledige bewerkbare samenstelling.' : 'Saved holdings are shown immediately. Select Refresh from AIRS for the complete editable composition.'}</p>}
    {error && <p role="alert" className="text-sm text-neg-300">{error === 'busy' ? (nl ? 'AIRS is bezig met een andere taak. Probeer het zo opnieuw.' : 'AIRS is busy with another task. Try again shortly.')
      : (nl ? 'De samenstelling kon niet worden geladen. Probeer opnieuw.' : 'The composition could not be loaded. Try again.')}</p>}
    {data?.no_fixed_model ? <p className="rounded-xl border border-neutral-800/40 bg-elevated p-5 text-sm text-fg-muted">{nl ? 'Dit model heeft in AIRS geen vaste samenstelling (type normaal of meervoudig).' : 'This AIRS model has no fixed composition (type normaal or meervoudig).'}</p>
      : data && <div className="overflow-x-auto rounded-xl border border-neutral-800/40">
        <table className="w-full whitespace-nowrap text-sm">
          <caption className="sr-only">Overzicht · {data.portfolio} · {data.datum ?? ''}</caption>
          <thead className="bg-elevated text-xs text-fg-muted"><tr>{['Fonds', 'Percentage', 'ISIN-code', 'Valuta', 'Categorie', 'Sector', 'Regio', 'Afmcategorie'].map((label, index) => <th scope="col" key={label} className={`px-4 py-3 font-medium ${index === 1 ? 'text-right' : 'text-left'}`}>{label}</th>)}</tr></thead>
          <tbody className="divide-y divide-neutral-800/30">
            {data.rows.map((row, index) => <tr key={`${row.ISINCode ?? row.Fonds}-${index}`} className="hover:bg-elevated/40">
              <th scope="row" className="px-4 py-3 text-left font-medium">{row.Fonds ?? '—'}</th>
              <td className="px-4 py-3 text-right tabular-nums">{editing && editing.row.id === row.id && row.id != null
                ? <form className="flex items-center justify-end gap-2" onSubmit={(event) => { event.preventDefault(); void savePercentage(); }}>
                  <input autoFocus inputMode="decimal" aria-label={`${nl ? 'Percentage voor' : 'Percentage for'} ${row.Fonds}`} value={editing.value} disabled={saving}
                    onChange={(event) => { const value = event.target.value; setEditing((current) => current ? { ...current, value } : null); }} className="w-24 rounded border border-neutral-800/40 bg-elevated px-2 py-1 text-right" />
                  <button type="submit" disabled={saving} className="rounded bg-accent-600 px-2 py-1 text-xs text-white disabled:opacity-50">{saving ? (nl ? 'Opslaan…' : 'Saving…') : (nl ? 'Opslaan in AIRS' : 'Save to AIRS')}</button>
                  <button type="button" disabled={saving} onClick={() => { setEditing(null); setEditError(null); }} className="text-xs text-fg-muted">{nl ? 'Annuleren' : 'Cancel'}</button>
                </form>
                : <div className="flex items-center justify-end gap-2"><span>{row.Percentage == null ? '—' : `${row.Percentage.toLocaleString(nl ? 'nl-NL' : 'en-GB', { maximumFractionDigits: 6 })}%`}</span>
                  {row.id != null && row.Percentage != null && data.datum && !data.cache_upgrade_available && <button type="button" disabled={busy || saving} aria-label={`${nl ? 'Percentage wijzigen voor' : 'Edit percentage for'} ${row.Fonds}`}
                    onClick={() => { setEditing({ row, value: String(row.Percentage) }); setEditError(null); setSaved(false); }} className="rounded p-1 text-fg-muted hover:bg-overlay/5 hover:text-accent-300 disabled:opacity-50">
                    <svg aria-hidden="true" viewBox="0 0 20 20" className="h-4 w-4" fill="none" stroke="currentColor" strokeWidth="1.5"><path d="m12.5 3.5 4 4M3 17l4.5-1 10-10a2.8 2.8 0 0 0-4-4l-10 10L3 17Z" /></svg>
                  </button>}
                </div>}</td>
              {[row.ISINCode, row.valuta, row.Beleggingscategorie, row.Beleggingssector, row.regio, row.afmCategorie].map((value, column) => <td key={column} className="px-4 py-3">{value || '—'}</td>)}
            </tr>)}
            {!data.rows.length && <tr><td colSpan={8} className="px-4 py-8 text-center text-fg-muted">{nl ? 'Geen posities beschikbaar voor deze datum.' : 'No positions available for this date.'}</td></tr>}
          </tbody>
        </table>
      </div>}
  </section>;
}
