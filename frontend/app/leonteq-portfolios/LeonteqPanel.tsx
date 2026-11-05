'use client';

import { useEffect, useRef, useState } from 'react';
import { apiFetch } from '../../lib/apiFetch';
import { API_URL } from '../../lib/apiUrl';
import { useLang } from '../../lib/i18n';
import CompositionTable, { type CompositionResult } from './CompositionTable';

type Portfolio = {
  name: string; sponsor: string | null; start_date: string | null;
  value: string | null; change: string | null; aum: string | null;
  itd: string | null; ytd: string | null; last_7_days_path: string | null;
};
type LoginResult = { success: boolean; status: string; portfolios?: Portfolio[]; retrieved_at?: string; cached?: boolean; snapshot_saved?: boolean; refresh_error?: string };
const performanceColor = (value: string | null) => !value ? 'text-fg-muted'
  : value.startsWith('-') ? 'text-neg-300' : Number.parseFloat(value) > 0 ? 'text-pos-300' : 'text-fg';

const messages: Record<string, [string, string]> = {
  success: ['Login successful. AMC Indices portfolios loaded.', 'Inloggen gelukt. AMC Indices-portefeuilles geladen.'],
  portfolios_failed: ['Login succeeded, but the AMC Indices table could not be read. Try loading again.', 'Inloggen gelukt, maar de AMC Indices-tabel kon niet worden gelezen. Probeer opnieuw te laden.'],
  composition_failed: ['The current composition could not be read. Try again.', 'De huidige samenstelling kon niet worden gelezen. Probeer opnieuw.'],
  portfolio_not_found: ['This portfolio is no longer in the AMC Indices list. Reload the portfolios.', 'Deze portefeuille staat niet meer in de AMC Indices-lijst. Laad de portefeuilles opnieuw.'],
  missing_credentials: ['Fill LEONTEQ_USER and LEONTEQ_PASSWORD in backend/.env, then restart the backend.', 'Vul LEONTEQ_USER en LEONTEQ_PASSWORD in backend/.env in en herstart de backend.'],
  login_rejected: ['LYNQS rejected the login. Check your credentials or account status.', 'LYNQS heeft de aanmelding geweigerd. Controleer je inloggegevens of accountstatus.'],
  verification_required: ['LYNQS requires an additional verification step. Complete login in the LYNQS portal.', 'LYNQS vraagt om extra verificatie. Rond het inloggen af in het LYNQS-portaal.'],
  timeout: ['Login could not be confirmed in time. LYNQS may require verification or an interactive login.', 'Inloggen kon niet op tijd worden bevestigd. Mogelijk vraagt LYNQS om verificatie of interactief inloggen.'],
  session_failed: ['LYNQS did not retain the session when opening the indices page.', 'LYNQS heeft de sessie niet behouden bij het openen van de indicespagina.'],
  browser_error: ['The login browser could not complete the check. Verify the backend browser installation and portal connectivity.', 'De inlogbrowser kon de controle niet afronden. Controleer de browserinstallatie op de backend en de verbinding met het portaal.'],
  browser_unavailable: ['The backend login browser is not installed.', 'De inlogbrowser op de backend is niet geïnstalleerd.'],
  busy: ['A login check is already running. Try again shortly.', 'Er wordt al een aanmelding gecontroleerd. Probeer het zo opnieuw.'],
  request_failed: ['The login check failed. Check the backend connection and try again.', 'De inlogcontrole is mislukt. Controleer de verbinding met de backend en probeer opnieuw.'],
  forbidden: ['Administrator access is required to check the Leonteq login.', 'Beheerderstoegang is vereist om de Leonteq-aanmelding te controleren.'],
};

export default function LeonteqPanel() {
  const [lang] = useLang();
  const nl = lang === 'nl';
  const [busy, setBusy] = useState(false);
  const [result, setResult] = useState<LoginResult | null>(null);
  const [listError, setListError] = useState<string | null>(null);
  const listRequest = useRef(0);
  const [query, setQuery] = useState('');
  const [selected, setSelected] = useState<string | null>(null);
  const [composition, setComposition] = useState<CompositionResult | null>(null);
  const [compositionBusy, setCompositionBusy] = useState(false);
  const [compositionError, setCompositionError] = useState<string | null>(null);
  const compositionCache = useRef(new Map<string, CompositionResult>());
  const compositionRequest = useRef(0);
  const portfolios = result?.portfolios ?? [];
  const filtered = portfolios.filter((row) => `${row.name} ${row.sponsor ?? ''}`.toLocaleLowerCase().includes(query.trim().toLocaleLowerCase()));
  const inFlight = useRef(false);

  useEffect(() => {
    let active = true;
    const request = listRequest.current;
    // This reads saved data only: opening the page never waits for LYNQS.
    void apiFetch(`${API_URL}/api/leonteq/portfolios/snapshot`, { cache: 'no-store' })
      .then(async (response) => response.ok ? await response.json() as LoginResult : null)
      .then((data) => {
        if (active && listRequest.current === request && data?.success) setResult(data);
      }).catch(() => { /* The Load portfolios button remains available after a failed saved read. */ });
    return () => { active = false; };
  }, []);

  function closeComposition() {
    compositionRequest.current += 1;
    setSelected(null);
    setComposition(null);
    setCompositionError(null);
    setCompositionBusy(false);
  }

  async function openComposition(name: string, refresh = false) {
    const request = ++compositionRequest.current;
    const saved = compositionCache.current.get(name);
    setSelected(name);
    setComposition(saved ? { ...saved, cached: true } : null);
    setCompositionError(null);
    if (saved && !refresh) {
      setCompositionBusy(false);
      return;
    }
    setCompositionBusy(true);
    try {
      const response = await apiFetch(`${API_URL}/api/leonteq/portfolios/composition`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ name, refresh }),
      });
      const data = response.status === 403 ? { success: false, status: 'forbidden' }
        : response.ok ? await response.json() as CompositionResult : { success: false, status: 'composition_failed' };
      if (data.success) compositionCache.current.set(name, data);
      if (request === compositionRequest.current) {
        if (data.success) setComposition(data);
        else if (!saved || data.status === 'forbidden') setComposition(data);
        setCompositionError(data.success ? data.refresh_error ?? null : data.status);
      }
    } catch {
      if (request === compositionRequest.current) setCompositionError('request_failed');
    } finally {
      if (request === compositionRequest.current) setCompositionBusy(false);
    }
  }

  async function checkLogin(refresh = false) {
    if (inFlight.current) return;
    inFlight.current = true;
    listRequest.current += 1;
    closeComposition();
    setBusy(true);
    setListError(null);
    try {
      const response = await apiFetch(`${API_URL}/api/leonteq/portfolios/login`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ refresh }),
      });
      if (response.status === 403) {
        setResult({ success: false, status: 'forbidden' });
      } else {
        if (!response.ok) throw new Error('Login check failed');
        const data = await response.json() as LoginResult;
        if (data.success || !result?.success) setResult(data);
        setListError(data.success ? data.refresh_error ?? null : data.status);
      }
    } catch {
      setListError('request_failed');
    } finally {
      inFlight.current = false;
      setBusy(false);
    }
  }

  return <div className="space-y-6">
    <section className="space-y-4 rounded-xl border border-neutral-800/40 bg-elevated p-5" aria-busy={busy}>
      <h2 className="font-medium">{nl ? 'LYNQS-portefeuilles' : 'LYNQS portfolios'}</h2>
      <div className="flex flex-wrap items-center gap-4">
        <button type="button" disabled={busy} onClick={() => void checkLogin(Boolean(result?.success))}
          className="rounded-lg bg-accent-600 px-4 py-2 text-sm font-medium text-white hover:bg-accent-500 disabled:opacity-50">
          {busy ? (result?.success ? (nl ? 'Vernieuwen…' : 'Refreshing…') : (nl ? 'Bezig met inloggen…' : 'Logging in…'))
            : result?.success ? (nl ? 'Vernieuwen uit LYNQS' : 'Refresh from LYNQS') : (nl ? 'Portefeuilles laden' : 'Load portfolios')}
        </button>
        <a href="https://www.lynqs.com/eportal/#/amc/indices" target="_blank" rel="noopener noreferrer"
          className="text-sm text-accent-300 hover:underline">{nl ? 'LYNQS openen' : 'Open LYNQS'}</a>
      </div>
      <div role="status" aria-live="polite" className="text-sm">
        {busy ? <p className="text-fg-muted">{result?.success
          ? (nl ? 'De opgeslagen lijst blijft zichtbaar terwijl LYNQS wordt bijgewerkt…' : 'The saved list remains visible while fetching the latest data from LYNQS…')
          : (nl ? 'Verbinding maken en de indicespagina controleren…' : 'Connecting and checking access to the indices page…')}</p>
          : result ? <div className={`rounded-lg border p-3 ${result.success ? 'border-pos-300/30 text-pos-300' : 'border-neg-300/30 text-neg-300'}`}>
            <p className="font-semibold">{result.cached ? (nl ? 'Opgeslagen portefeuilles' : 'Saved portfolios') : result.success ? (nl ? 'Inloggen gelukt' : 'Login successful') : (nl ? 'Inloggen niet gelukt' : 'Login unsuccessful')}</p>
            <p className="mt-1">{result.cached
              ? (nl ? 'Direct geladen uit de opgeslagen lijst. Vernieuwen haalt de nieuwste gegevens uit LYNQS op.' : 'Loaded directly from the saved list. Refresh retrieves the latest data from LYNQS.')
              : (messages[result.status] ?? messages.request_failed)[nl ? 1 : 0]}</p>
          </div> : <p className="text-fg-muted">{nl ? 'Nog niet gecontroleerd.' : 'Not checked yet.'}</p>}
      </div>
      {listError && <p role="alert" className="text-sm text-neg-300">{(messages[listError] ?? messages.request_failed)[nl ? 1 : 0]}</p>}
      {result?.snapshot_saved === false && <p className="text-sm text-fg-muted">{nl ? 'Deze lijst kon niet blijvend worden opgeslagen.' : 'This list could not be saved permanently.'}</p>}
    </section>
    {selected && <section className="space-y-4" aria-labelledby="composition-heading" aria-busy={compositionBusy}>
      <button type="button" onClick={closeComposition} className="text-sm text-accent-300 hover:underline">← {nl ? 'Alle portefeuilles' : 'All portfolios'}</button>
      <div className="flex flex-wrap items-center justify-between gap-3">
        <h2 id="composition-heading" className="text-xl font-semibold">{selected} · {nl ? 'Samenstelling' : 'Composition'}</h2>
        <button type="button" disabled={compositionBusy} onClick={() => void openComposition(selected, true)} className="rounded-lg border border-neutral-800/40 bg-elevated px-3 py-2 text-sm disabled:opacity-50">{nl ? 'Vernieuwen' : 'Refresh'}</button>
      </div>
      <div role="status" aria-live="polite">
        {compositionBusy && <p className="text-sm text-fg-muted">{composition?.success
          ? (nl ? 'Nieuwste posities ophalen; de opgeslagen samenstelling blijft zichtbaar…' : 'Fetching latest positions; the saved composition remains visible…')
          : (nl ? 'Huidige posities laden uit LYNQS…' : 'Loading current positions from LYNQS…')}</p>}
        {compositionError && <p className="text-sm text-neg-300">{(messages[compositionError] ?? messages.composition_failed)[nl ? 1 : 0]}</p>}
        {composition?.snapshot_saved === false && <p className="text-sm text-fg-muted">{nl ? 'De samenstelling kon niet blijvend worden opgeslagen. Opnieuw openen op deze pagina blijft direct beschikbaar.' : 'The composition could not be saved permanently. Reopening it on this page is still immediate.'}</p>}
      </div>
      {composition?.success && <CompositionTable data={composition} nl={nl} />}
    </section>}
    {result?.success && !selected && <section className="space-y-4" aria-labelledby="indices-heading">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h2 id="indices-heading" className="text-lg font-semibold">AMC Indices <span className="text-sm font-normal text-fg-muted">({portfolios.length})</span></h2>
          {result.retrieved_at && <p className="mt-1 text-xs text-fg-muted">{nl ? 'Opgehaald op' : 'Retrieved at'} {new Date(result.retrieved_at).toLocaleString(nl ? 'nl-NL' : 'en-GB')}</p>}
        </div>
        <input type="search" value={query} onChange={(event) => setQuery(event.target.value)}
          aria-label={nl ? 'Zoeken op indexnaam of sponsor' : 'Search index name or sponsor'}
          placeholder={nl ? 'Zoek op indexnaam of sponsor…' : 'Search index name or sponsor…'}
          className="w-72 max-w-full rounded-lg border border-neutral-800/40 bg-elevated px-3 py-2 text-sm" />
      </div>
      <div className="overflow-x-auto rounded-xl border border-neutral-800/40">
        <table className="w-full whitespace-nowrap text-sm">
          <caption className="sr-only">{nl ? 'Leonteq AMC Indices-portefeuilles' : 'Leonteq AMC Indices portfolios'}</caption>
          <thead className="bg-elevated text-xs text-fg-muted"><tr>
            {[nl ? 'Indexnaam' : 'Index name', 'Sponsor', nl ? 'Startdatum' : 'Index start date', nl ? 'Waarde' : 'Value', 'AuM', nl ? 'Sinds start' : 'Since inception', 'YTD', nl ? 'Laatste 7 dagen' : 'Last 7 days'].map((label, index) =>
              <th key={label} scope="col" className={`px-4 py-3 font-medium ${index < 3 ? 'text-left' : 'text-right'}`}>{label}</th>)}
          </tr></thead>
          <tbody className="divide-y divide-neutral-800/30">
            {filtered.map((row) => <tr key={`${row.name}-${row.sponsor}-${row.start_date}`} className="hover:bg-elevated/50">
              <th scope="row" className="px-4 py-3 text-left font-medium"><button type="button" onClick={() => void openComposition(row.name)} className="text-accent-300 hover:underline" aria-label={`${row.name} · ${nl ? 'Samenstelling bekijken' : 'View composition'}`}>{row.name}</button></th>
              <td className="px-4 py-3 text-fg-muted">{row.sponsor ?? '—'}</td>
              <td className="px-4 py-3">{row.start_date ?? '—'}</td>
              <td className="px-4 py-3 text-right tabular-nums"><div>{row.value ?? '—'}</div><div className={`mt-1 text-xs ${performanceColor(row.change)}`}>{row.change ?? '—'}</div></td>
              <td className="px-4 py-3 text-right tabular-nums">{row.aum ?? '—'}</td>
              <td className={`px-4 py-3 text-right tabular-nums ${performanceColor(row.itd)}`}>{row.itd ?? '—'}</td>
              <td className={`px-4 py-3 text-right tabular-nums ${performanceColor(row.ytd)}`}>{row.ytd ?? '—'}</td>
              <td className="px-4 py-3">{row.last_7_days_path && /^[ML\d\s.,eE+\-]+$/.test(row.last_7_days_path)
                ? <svg viewBox="0 0 120 40" className="ml-auto h-10 w-28 text-accent-300" role="img" aria-label={`${row.name} · ${nl ? 'laatste 7 dagen' : 'last 7 days'}`}><path d={row.last_7_days_path} fill="none" stroke="currentColor" strokeWidth="2" /></svg>
                : <span className="block text-right text-fg-muted">—</span>}</td>
            </tr>)}
            {!filtered.length && <tr><td colSpan={8} className="px-4 py-8 text-center text-fg-muted">{nl ? 'Geen portefeuilles gevonden.' : 'No portfolios found.'}</td></tr>}
          </tbody>
        </table>
      </div>
    </section>}
  </div>;
}
