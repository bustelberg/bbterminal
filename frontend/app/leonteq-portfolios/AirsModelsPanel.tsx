'use client';

import { useEffect, useRef, useState } from 'react';
import { apiFetch } from '../../lib/apiFetch';
import { API_URL } from '../../lib/apiUrl';
import { useLang } from '../../lib/i18n';
import { startJob } from '../../lib/stores/jobs';
import AirsCompositionView, { type AirsComposition } from './AirsCompositionView';

type Model = {
  id: number; name: string; omschrijving: string | null;
  portfolio_type: string | null; fixed_datum: string | null; scanned_at: string | null;
};

export default function AirsModelsPanel({ kind = 'fixed' }: { kind?: 'fixed' | 'dynamic' }) {
  const [lang] = useLang();
  const nl = lang === 'nl';
  const [rows, setRows] = useState<Model[]>([]);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [query, setQuery] = useState('');
  const [selected, setSelected] = useState<Model | null>(null);
  const compositions = useRef(new Map<string, AirsComposition>());
  const inFlight = useRef(false);
  const alive = useRef(true);

  useEffect(() => {
    alive.current = true;
    let active = true;
    const snapshots = kind === 'dynamic' ? Promise.resolve(null) : apiFetch(`${API_URL}/api/leonteq/airs/model-portfolios/compositions`, { cache: 'no-store', noReadCache: true })
      .then(async (response) => response.ok ? await response.json() as { compositions: AirsComposition[] } : null)
      .catch(() => null);
    const models = apiFetch(`${API_URL}/api/airs/model-portfolios`).then(async (response) => {
      if (!response.ok) throw new Error('read failed');
      return await response.json() as Model[];
    });
    void Promise.all([models, snapshots]).then(([rows, saved]) => {
      if (!active) return;
      for (const snapshot of saved?.compositions ?? []) {
        compositions.current.set(`${snapshot.portfolio_id}:latest`, snapshot);
        if (snapshot.datum) compositions.current.set(`${snapshot.portfolio_id}:${snapshot.datum}`, snapshot);
      }
      setRows(rows);
    }).catch(() => {
      if (active) setError(nl ? 'De opgeslagen AIRS-modelportefeuilles konden niet worden geladen.' : 'The saved AIRS model portfolios could not be loaded.');
    }).finally(() => { if (active) setLoading(false); });
    return () => { active = false; alive.current = false; };
  }, [nl, kind]);

  async function refresh() {
    if (inFlight.current) return;
    inFlight.current = true;
    setRefreshing(true); setError(null);
    try {
      const { done } = await startJob(`${API_URL}/api/leonteq/airs/model-portfolios/refresh/job`, 'AIRS · Model portefeuilles');
      const job = await done;
      if (job.status !== 'done') throw new Error(job.summary ?? 'refresh failed');
      const response = await apiFetch(`${API_URL}/api/airs/model-portfolios`, { noReadCache: true });
      if (!response.ok) throw new Error('read failed');
      const data = await response.json() as Model[];
      if (alive.current) setRows(data);
    } catch {
      if (alive.current) setError(nl ? 'Vernieuwen uit AIRS is niet gelukt. De opgeslagen lijst blijft beschikbaar. Zie Details bij de taak.' : 'Refreshing from AIRS failed. The saved list remains available. See the job details.');
    } finally {
      inFlight.current = false;
      if (alive.current) setRefreshing(false);
    }
  }

  const typedRows = rows.filter((row) => kind === 'fixed' ? /^fixed\b/i.test(row.portfolio_type ?? '') : row.portfolio_type?.trim().toLowerCase() === 'normaal');
  const filtered = typedRows.filter((row) => `${row.name} ${row.omschrijving ?? ''} ${row.portfolio_type ?? ''}`.toLocaleLowerCase().includes(query.trim().toLocaleLowerCase()));
  const scannedAt = rows.reduce<string | null>((latest, row) => row.scanned_at && (!latest || row.scanned_at > latest) ? row.scanned_at : latest, null);
  if (selected) return <AirsCompositionView key={selected.id} id={selected.id} name={selected.name} cache={compositions.current} onBack={() => setSelected(null)} />;
  return <section className="space-y-4" aria-labelledby={`airs-${kind}-heading`} aria-busy={loading || refreshing}>
    <div className="flex flex-wrap items-center justify-between gap-4">
      <div>
        <h2 id={`airs-${kind}-heading`} className="text-lg font-semibold">AIRS {kind === 'fixed' ? 'Fixed' : 'Dynamic'} <span className="text-sm font-normal text-fg-muted">({typedRows.length})</span></h2>
        {scannedAt && <p className="mt-1 text-xs text-fg-muted">{nl ? 'Opgehaald op' : 'Retrieved at'} {new Date(scannedAt).toLocaleString(nl ? 'nl-NL' : 'en-GB')}</p>}
      </div>
      <button type="button" disabled={refreshing || loading} onClick={() => void refresh()} className="rounded-lg bg-accent-600 px-4 py-2 text-sm font-medium text-white hover:bg-accent-500 disabled:opacity-50">
        {refreshing ? (nl ? 'Vernieuwen…' : 'Refreshing…') : (nl ? 'Vernieuwen uit AIRS' : 'Refresh from AIRS')}
      </button>
    </div>
    <input type="search" value={query} onChange={(event) => setQuery(event.target.value)} aria-label={nl ? 'Modelportefeuilles zoeken' : 'Search model portfolios'}
      placeholder={nl ? 'Zoek op portefeuille, omschrijving of type…' : 'Search portfolio, description or type…'} className="w-full max-w-md rounded-lg border border-neutral-800/40 bg-elevated px-3 py-2 text-sm" />
    <div role="status" aria-live="polite" className="text-sm text-fg-muted">
      {loading ? (nl ? 'Opgeslagen modelportefeuilles laden…' : 'Loading saved model portfolios…')
        : refreshing ? (nl ? 'Modelportefeuilles ophalen uit AIRS; de opgeslagen lijst blijft zichtbaar…' : 'Reading model portfolios from AIRS; the saved list remains visible…') : null}
    </div>
    {error && <p role="alert" className="text-sm text-neg-300">{error}</p>}
    <div className="overflow-x-auto rounded-xl border border-neutral-800/40">
      <table className="w-full text-sm">
        <caption className="sr-only">Overzicht Modelportefeuilles · AIRS</caption>
        <thead className="bg-elevated text-xs text-fg-muted"><tr>{['Portefeuille', 'Omschrijving', 'Type', 'Datum'].map((label) => <th key={label} scope="col" className="px-4 py-3 text-left font-medium">{label}</th>)}</tr></thead>
        <tbody className="divide-y divide-neutral-800/30">
          {filtered.map((row) => <tr key={row.id} className="hover:bg-elevated/40">
            <th scope="row" className="px-4 py-3 text-left font-medium">{kind === 'dynamic' ? row.name : <button type="button" onClick={() => setSelected(row)} className="text-accent-300 hover:underline" aria-label={`${row.name} · ${nl ? 'Samenstelling bekijken' : 'View composition'}`}>{row.name}</button>}</th>
            <td className="px-4 py-3">{row.omschrijving ?? '—'}</td>
            <td className="whitespace-nowrap px-4 py-3">{row.portfolio_type ?? '—'}</td>
            <td className="whitespace-nowrap px-4 py-3 tabular-nums">{row.fixed_datum ?? '—'}</td>
          </tr>)}
          {!loading && !filtered.length && <tr><td colSpan={4} className="px-4 py-8 text-center text-fg-muted">{rows.length ? (nl ? 'Geen overeenkomende modelportefeuilles.' : 'No matching model portfolios.')
            : (nl ? 'Nog geen modelportefeuilles opgeslagen. Kies Vernieuwen uit AIRS.' : 'No saved model portfolios yet. Select Refresh from AIRS.')}</td></tr>}
        </tbody>
      </table>
    </div>
  </section>;
}
