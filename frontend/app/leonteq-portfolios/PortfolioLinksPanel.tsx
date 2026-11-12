'use client';

import { useEffect, useRef, useState } from 'react';
import { API_URL } from '../../lib/apiUrl';
import { apiFetch } from '../../lib/apiFetch';
import { useLang } from '../../lib/i18n';
import SearchablePortfolioPicker from './SearchablePortfolioPicker';
import ConnectionHoldingsView, { type ConnectionHoldingsCache } from './ConnectionHoldingsView';
import type { HoldingRule } from './matchHoldings';
import { reloadWithProgress } from './refreshProgress';
import HoldingsConnectionStatus from './HoldingsConnectionStatus';

type Link = { leonteq_name: string; airs_portfolio_id: number; airs_name: string | null; updated_at: string };
type Options = {
  leonteq_portfolios: { name: string; sponsor: string | null }[];
  airs_portfolios: { id: number; name: string; omschrijving: string | null; portfolio_type: string }[];
  links: Link[];
};
const endpoint = `${API_URL}/api/leonteq/portfolio-links`;

async function readOptions(kind: 'fixed' | 'dynamic'): Promise<Options> {
  const response = await apiFetch(kind === 'dynamic' ? `${endpoint}?kind=dynamic` : endpoint, { cache: 'no-store', noReadCache: true });
  if (!response.ok) throw new Error('read_failed');
  return await response.json() as Options;
}

function LinksPanel({ kind, active }: { kind: 'fixed' | 'dynamic'; active: boolean }) {
  const [lang] = useLang();
  const nl = lang === 'nl';
  const [data, setData] = useState<Options | null>(null);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState(false);
  const [source, setSource] = useState('');
  const [target, setTarget] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [selected, setSelected] = useState<Link | null>(null);
  const holdingsCache = useRef<ConnectionHoldingsCache>(new Map());
  const rulesCache = useRef(new Map<string, HoldingRule[]>());
  const alive = useRef(true);
  const inFlight = useRef(false);

  useEffect(() => {
    if (!active) return;
    alive.current = true;
    const cache = holdingsCache.current, savedRules = rulesCache.current;
    let current = true;
    void readOptions(kind).then((options) => {
      if (current) { setData(options); setError(null); if (!inFlight.current) setBusy(false); }
    })
      .catch(() => { if (current) setError('load'); })
      .finally(() => { if (current) setLoading(false); });
    return () => {
      current = false; alive.current = false;
      // Rebalance may add/edit AIRS positions while Connections is hidden.
      cache.clear(); savedRules.clear();
    };
  }, [kind, active]);

  async function reload() {
    if (inFlight.current) return;
    inFlight.current = true; setLoading(true); setError(null); setNotice(null);
    try {
      const options = await reloadWithProgress(nl ? 'Koppelingen · Keuzelijsten vernieuwen' : 'Connections · Reload options',
        () => readOptions(kind), nl ? 'Opgeslagen Leonteq- en AIRS-portefeuilles en koppelingen laden…' : 'Loading saved Leonteq and AIRS portfolios and connections…',
        nl ? 'Portefeuilles en koppelingen bijgewerkt.' : 'Portfolio options and connections updated.');
      if (alive.current) { holdingsCache.current.clear(); rulesCache.current.clear(); setData(options); }
    } catch {
      if (alive.current) setError('load');
    } finally {
      inFlight.current = false;
      if (alive.current) setLoading(false);
    }
  }

  async function change(method: 'PUT' | 'DELETE', leonteqName: string, airsId: number) {
    if (inFlight.current) return;
    inFlight.current = true; setBusy(true); setError(null); setNotice(null);
    try {
      const response = await apiFetch(endpoint, { method, headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ leonteq_name: leonteqName, airs_portfolio_id: airsId, ...(kind === 'dynamic' ? { kind } : {}) }) });
      if (!response.ok) throw new Error('save_failed');
      const saved = await response.json() as Link;
      if (alive.current) {
        setData((current) => current ? { ...current, links: [
          ...current.links.filter((link) => link.leonteq_name !== leonteqName || link.airs_portfolio_id !== airsId),
          ...(method === 'PUT' ? [saved] : []),
        ].sort((a, b) => a.leonteq_name.localeCompare(b.leonteq_name)) } : current);
        setNotice(method === 'PUT' ? 'connected' : 'disconnected');
        if (method === 'PUT') { setSource(''); setTarget(''); }
      }
    } catch {
      if (alive.current) setError('save');
    } finally {
      inFlight.current = false;
      if (alive.current) setBusy(false);
    }
  }

  const alreadyLinked = data?.links.some((link) => link.leonteq_name === source && link.airs_portfolio_id === Number(target));
  const connectedSources = new Set(data?.links.map((link) => link.leonteq_name));
  const connectedTargets = new Set(data?.links.map((link) => link.airs_portfolio_id));
  const sourceOptions = (data?.leonteq_portfolios ?? []).filter((row) => !connectedSources.has(row.name));
  const targetOptions = (data?.airs_portfolios ?? []).filter((row) => !connectedTargets.has(row.id));
  const valid = sourceOptions.some((row) => row.name === source)
    && targetOptions.some((row) => row.id === Number(target));
  if (!active) return null;
  if (selected) return <ConnectionHoldingsView key={JSON.stringify([selected.leonteq_name, selected.airs_portfolio_id])}
    link={selected} kind={kind} cache={holdingsCache.current} rulesCache={rulesCache.current} onBack={() => setSelected(null)} />;
  return <section className="space-y-5" aria-labelledby="portfolio-connections-heading" aria-busy={loading || busy}>
    <div className="flex flex-wrap items-start justify-between gap-4">
      <div>
        <h2 id="portfolio-connections-heading" className="text-lg font-semibold">{nl ? 'Portefeuilles koppelen' : 'Connect portfolios'}</h2>
        <p className="mt-1 text-sm text-fg-muted">{nl ? 'Leonteq → AIRS' : 'Leonteq → AIRS'}</p>
      </div>
      <button type="button" disabled={loading || busy} onClick={() => void reload()} className="rounded-lg border border-neutral-800/40 bg-elevated px-3 py-2 text-sm disabled:opacity-50">{nl ? 'Keuzelijsten vernieuwen' : 'Reload options'}</button>
    </div>
    <form onSubmit={(event) => { event.preventDefault(); if (valid && !alreadyLinked) void change('PUT', source, Number(target)); }}
      className="flex flex-wrap items-end gap-4 rounded-xl border border-neutral-800/40 bg-elevated/40 p-4">
      <div className="min-w-0 flex-[1_1_20rem]">
        <SearchablePortfolioPicker label={nl ? 'Leonteq-portefeuille' : 'Leonteq portfolio'}
          placeholder={nl ? 'Zoek een Leonteq-portefeuille…' : 'Search Leonteq portfolios…'}
          options={sourceOptions.map((row) => ({ value: row.name, label: row.name, description: row.sponsor }))}
          value={source} onSelect={(value) => { setSource(value); setNotice(null); }} disabled={loading || busy} nl={nl} />
      </div>
      <div className="min-w-0 flex-[1_1_20rem]">
        <SearchablePortfolioPicker label={nl ? kind === 'fixed' ? 'Vaste AIRS-portefeuille' : 'Dynamische AIRS-portefeuille' : kind === 'fixed' ? 'Fixed AIRS portfolio' : 'Dynamic AIRS portfolio'}
          placeholder={nl ? 'Zoek op AIRS-naam of omschrijving…' : 'Search AIRS name or description…'}
          options={targetOptions.map((row) => ({ value: String(row.id), label: row.name, description: row.omschrijving }))}
          value={target} onSelect={(value) => { setTarget(value); setNotice(null); }} disabled={loading || busy} nl={nl} />
      </div>
      <button type="submit" disabled={loading || busy || !valid || alreadyLinked} className="rounded-lg bg-accent-600 px-4 py-2 text-sm font-medium text-white disabled:opacity-50">{busy ? (nl ? 'Opslaan…' : 'Saving…') : (nl ? 'Koppeling opslaan' : 'Save connection')}</button>
    </form>
    {loading && <p role="status" className="text-sm text-fg-muted">{nl ? 'Opgeslagen portefeuilles en koppelingen laden…' : 'Loading saved portfolios and connections…'}</p>}
    {!loading && data && (!data.leonteq_portfolios.length || !data.airs_portfolios.length) && <p className="text-sm text-fg-muted">{nl ? 'Laad eerst de portefeuilles in de tabbladen Leonteq en AIRS. Vernieuw daarna de keuzelijsten.' : 'Load the portfolios in the Leonteq and AIRS tabs first, then reload these options.'}</p>}
    {alreadyLinked && <p className="text-sm text-fg-muted">{nl ? 'Deze koppeling bestaat al.' : 'This connection already exists.'}</p>}
    {error && <p role="alert" className="text-sm text-neg-300">{error === 'load'
      ? (nl ? 'Koppelingen konden niet worden geladen. Controleer de verbinding en probeer opnieuw.' : 'Connections could not be loaded. Check the connection and retry.')
      : (nl ? 'De wijziging kon niet worden opgeslagen. Vernieuw de keuzelijsten en probeer opnieuw.' : 'The change could not be saved. Reload the options and retry.')}</p>}
    {notice && <p role="status" className="text-sm text-pos-300">{notice === 'connected' ? (nl ? 'Koppeling opgeslagen.' : 'Connection saved.') : (nl ? 'Koppeling verwijderd.' : 'Connection removed.')}</p>}
    <div className="overflow-x-auto rounded-xl border border-neutral-800/40">
      <table className="w-full text-sm"><caption className="sr-only">{nl ? 'Opgeslagen portefeuille-koppelingen' : 'Saved portfolio connections'}</caption>
        <thead className="bg-elevated text-xs text-fg-muted"><tr>{[nl ? 'Leonteq-portefeuille' : 'Leonteq portfolio', nl ? kind === 'fixed' ? 'Vaste AIRS-portefeuille' : 'Dynamische AIRS-portefeuille' : kind === 'fixed' ? 'Fixed AIRS portfolio' : 'Dynamic AIRS portfolio', nl ? 'Posities gekoppeld' : 'Holdings connected', nl ? 'Actie' : 'Action'].map((label) => <th scope="col" key={label} className="px-4 py-3 text-left font-medium">{label}</th>)}</tr></thead>
        <tbody className="divide-y divide-neutral-800/30">{data?.links.map((link) => <tr key={JSON.stringify([link.leonteq_name, link.airs_portfolio_id])}
          onClick={() => { if (!busy && !loading) setSelected(link); }} className="cursor-pointer hover:bg-elevated/40">
          <th scope="row" className="px-4 py-3 text-left font-medium"><button type="button" disabled={loading || busy} onClick={() => setSelected(link)} className="text-accent-300 hover:underline"
            aria-label={`${nl ? 'Posities bekijken voor' : 'View holdings for'} ${link.leonteq_name} · ${link.airs_name ?? link.airs_portfolio_id}`}>{link.leonteq_name}</button></th>
          <td className="px-4 py-3">{link.airs_name ?? `AIRS #${link.airs_portfolio_id}`}</td>
          <td className="px-4 py-3"><HoldingsConnectionStatus link={link} nl={nl} cache={holdingsCache.current} rulesCache={rulesCache.current} /></td>
          <td className="px-4 py-3"><button type="button" disabled={loading || busy} onClick={(event) => { event.stopPropagation(); void change('DELETE', link.leonteq_name, link.airs_portfolio_id); }}
            aria-label={`${nl ? 'Koppeling verwijderen voor' : 'Remove connection for'} ${link.leonteq_name} · ${link.airs_name ?? link.airs_portfolio_id}`} className="text-fg-muted hover:text-neg-300 disabled:opacity-50">{nl ? 'Ontkoppelen' : 'Disconnect'}</button></td>
        </tr>)}
          {!loading && !data?.links.length && <tr><td colSpan={4} className="px-4 py-8 text-center text-fg-muted">{nl ? 'Nog geen koppelingen opgeslagen.' : 'No connections saved yet.'}</td></tr>}
        </tbody>
      </table>
    </div>
  </section>;
}

export default function PortfolioLinksPanel({ active = true }: { active?: boolean }) {
  const [kind, setKind] = useState<'fixed' | 'dynamic'>('fixed');
  return <div className="space-y-4">
    <div className="inline-flex max-w-full overflow-x-auto rounded-lg border border-neutral-800/40 text-xs" aria-label="Connection type">
      {(['fixed', 'dynamic'] as const).map((value) => <button key={value} type="button" aria-pressed={kind === value} onClick={() => setKind(value)} className={`shrink-0 px-3 py-1.5 font-medium transition-colors ${kind === value ? 'bg-accent-600 text-white' : 'text-fg-subtle hover:bg-overlay/5'}`}>{value === 'fixed' ? 'Leonteq to AIRS Fixed' : 'Leonteq to AIRS Dynamic'}</button>)}
    </div>
    <LinksPanel key={kind} kind={kind} active={active} />
  </div>;
}
