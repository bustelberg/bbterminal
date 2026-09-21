'use client';

import { useEffect, useMemo, useState } from 'react';
import { API_URL } from '../../../lib/apiUrl';
import { apiFetch } from '../../../lib/apiFetch';
import { useLang } from '../../../lib/i18n';
import CompanyPicker, { type AssetPick } from '../research/CompanyPicker';
import { INFO_ICON } from '../../../lib/infoIcon';

type Entry = {
  id: number; meeting_date: string; company_name: string; isin: string; decision: string;
  notes: string; conviction: number; portfolio_weight: number | null;
  review_on: string | null;
};
type Article = { id?: number; subject?: string; subtitle?: string; publish_time?: string; link?: string };
type YahooReturn = { available: boolean; return_pct?: number; start_date?: string; as_of?: string; retrieved_at?: string; message?: string };

const card = 'rounded-xl border border-neutral-800/60 bg-card p-5';
const decisionKeys = ['Buy', 'Hold', 'Reduce', 'Sell', 'Watch'] as const;
const legacyDecisionKeys: Record<string, typeof decisionKeys[number]> = {
  Kopen: 'Buy', Aanhouden: 'Hold', Verlagen: 'Reduce', Verkopen: 'Sell', Volgen: 'Watch',
};
const decisionLabels: Record<typeof decisionKeys[number], { nl: string; en: string }> = {
  Buy: { nl: 'Kopen', en: 'Buy' }, Hold: { nl: 'Aanhouden', en: 'Hold' },
  Reduce: { nl: 'Verlagen', en: 'Reduce' }, Sell: { nl: 'Verkopen', en: 'Sell' },
  Watch: { nl: 'Volgen', en: 'Watch' },
};

export default function LogDashboard() {
  const [lang] = useLang();
  const nl = lang === 'nl';
  const decisionLabel = (decision: string) => {
    const key = legacyDecisionKeys[decision] ?? decision;
    return decisionLabels[key as keyof typeof decisionLabels]?.[nl ? 'nl' : 'en'] ?? decision;
  };
  const today = new Date().toISOString().slice(0, 10);
  const formatDate = (value: string | null | undefined) => {
    if (!value) return '—';
    const [year, month, day] = value.slice(0, 10).split('-').map(Number);
    if (!year || !month || !day) return value;
    const months = nl
      ? ['jan', 'feb', 'mrt', 'apr', 'mei', 'jun', 'jul', 'aug', 'sept', 'okt', 'nov', 'dec']
      : ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    return `${day} ${months[month - 1]} ${year}`;
  };
  const [entries, setEntries] = useState<Entry[]>([]);
  const [company, setCompany] = useState<AssetPick | null>(null);
  const [articles, setArticles] = useState<Article[]>([]);
  const [newsMessage, setNewsMessage] = useState('');
  const [saving, setSaving] = useState(false);
  const [openTarget, setOpenTarget] = useState<Entry | null>(null);
  const [yahooReturns, setYahooReturns] = useState<Record<number, YahooReturn>>({});
  const [deleteTarget, setDeleteTarget] = useState<Entry | null>(null);
  const [deleting, setDeleting] = useState(false);
  const [form, setForm] = useState({ decision: 'Hold', notes: '', conviction: '3', portfolio_weight: '', review_on: new Date().toISOString().slice(0, 10) });

  async function loadEntries() {
    const r = await apiFetch(`${API_URL}/api/log-dashboard/entries`);
    if (r.ok) setEntries(await r.json());
  }
  useEffect(() => { void loadEntries(); }, []);

  useEffect(() => {
    if (!entries.length) { setYahooReturns({}); return; }
    const ctrl = new AbortController();
    void (async () => {
      const dated = entries.filter((entry) => entry.review_on);
      const results = await Promise.all(dated.map(async (entry) => {
        const r = await apiFetch(`${API_URL}/api/log-dashboard/yahoo-return/${encodeURIComponent(entry.isin)}?since=${encodeURIComponent(entry.review_on!.slice(0, 10))}`, { signal: ctrl.signal });
        const body = await r.json().catch(() => null);
        return [entry.id, r.ok ? body as YahooReturn : { available: false }] as const;
      }));
      if (!ctrl.signal.aborted) setYahooReturns(Object.fromEntries(results));
    })().catch(() => { if (!ctrl.signal.aborted) setYahooReturns({}); });
    return () => ctrl.abort();
  }, [entries]);

  useEffect(() => {
    const ticker = company?.yahoo_symbol?.replace(/\..*$/, '');
    if (!ticker) { setArticles([]); setNewsMessage(nl ? 'Kies een bedrijf om het nieuws te zien.' : 'Choose a company to see its news.'); return; }
    const ctrl = new AbortController();
    setNewsMessage(nl ? 'Nieuws laden…' : 'Loading news…');
    void (async () => {
      const r = await apiFetch(`${API_URL}/api/log-dashboard/news/${encodeURIComponent(ticker)}`, { signal: ctrl.signal });
      if (ctrl.signal.aborted) return;
      const body = await r.json().catch(() => ({}));
      if (!r.ok) { setArticles([]); setNewsMessage((body.detail as string) || (nl ? 'Geen nieuws beschikbaar voor dit bedrijf.' : 'No news is available for this company.')); return; }
      setArticles(body.articles || []);
      setNewsMessage(body.message || (body.articles?.length ? '' : (nl ? 'Geen recente artikelen voor dit aandeel.' : 'No recent articles for this company.')));
    })();
    return () => ctrl.abort();
  }, [company, nl]);

  const reviewDue = useMemo(() => entries.filter((e) => e.review_on && e.review_on <= new Date().toISOString().slice(0, 10)), [entries]);
  const openYahooReturn = openTarget ? yahooReturns[openTarget.id] : null;
  async function save() {
    if (!company) return;
    setSaving(true);
    try {
      const r = await apiFetch(`${API_URL}/api/log-dashboard/entries`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({
        company_name: company.name || company.isin, isin: company.isin, decision: form.decision, notes: form.notes,
        conviction: Number(form.conviction), portfolio_weight: form.portfolio_weight ? Number(form.portfolio_weight) : null,
        review_on: form.review_on || null,
      }) });
      if (!r.ok) throw new Error((await r.json().catch(() => ({}))).detail || 'Could not save the decision.');
      setForm({ decision: 'Hold', notes: '', conviction: '3', portfolio_weight: '', review_on: today });
      await loadEntries();
    } finally { setSaving(false); }
  }
  async function deleteEntry() {
    if (!deleteTarget) return;
    setDeleting(true);
    try {
      const r = await apiFetch(`${API_URL}/api/log-dashboard/entries/${deleteTarget.id}`, { method: 'DELETE' });
      if (!r.ok) throw new Error('Could not delete the decision.');
      setDeleteTarget(null);
      await loadEntries();
    } finally { setDeleting(false); }
  }

  return <main className="min-h-screen bg-page px-5 py-8 text-fg sm:px-8 lg:px-10">
    <div className="mx-auto max-w-7xl space-y-6">
      <header><h1 className="text-2xl font-semibold text-fg-strong">{nl ? 'BC-logboek' : 'BC logbook'}</h1><p className="mt-1 text-sm text-fg-muted">{nl ? 'Leg besluiten vast, volg actiepunten op en evalueer de beleggingscase door de tijd.' : 'Record decisions, follow up on actions and review the investment case over time.'}</p></header>
      <div className="grid gap-6 xl:grid-cols-[minmax(0,1.05fr)_minmax(0,0.95fr)]">
        <section className={`${card} xl:h-[clamp(420px,calc(100vh-14rem),560px)] xl:overflow-y-auto`}>
          <h2 className="text-base font-semibold text-fg-strong">{nl ? 'Nieuw besluit' : 'New decision'}</h2>
          <div className="mt-4"><CompanyPicker label={nl ? 'Bedrijf' : 'Company'} value={company} onPick={setCompany} /></div>
          <div className="mt-4 grid gap-3 sm:grid-cols-2">
            <label className="text-sm text-fg-soft">{nl ? 'Besluit' : 'Decision'}<select value={form.decision} onChange={e => setForm({ ...form, decision: e.target.value })} className="mt-1 w-full rounded-lg border border-neutral-700 bg-page px-3 py-2 text-fg">{decisionKeys.map(key => <option key={key} value={key}>{decisionLabel(key)}</option>)}</select></label>
            <label className="text-sm text-fg-soft">{nl ? 'Convictie (1–5)' : 'Conviction (1–5)'}<select value={form.conviction} onChange={e => setForm({ ...form, conviction: e.target.value })} className="mt-1 w-full rounded-lg border border-neutral-700 bg-page px-3 py-2 text-fg">{[1,2,3,4,5].map(n => <option key={n}>{n}</option>)}</select></label>
            <label className="text-sm text-fg-soft">{nl ? 'Portefeuilleweging (%)' : 'Portfolio weight (%)'}<input value={form.portfolio_weight} onChange={e => setForm({ ...form, portfolio_weight: e.target.value })} inputMode="decimal" className="mt-1 w-full rounded-lg border border-neutral-700 bg-page px-3 py-2 text-fg" /></label>
            <label className="text-sm text-fg-soft">{nl ? 'Evalueren op' : 'Review on'}<input type="date" value={form.review_on} onChange={e => setForm({ ...form, review_on: e.target.value })} className="mt-1 w-full rounded-lg border border-neutral-700 bg-page px-3 py-2 text-fg" /></label>
          </div>
          <label className="mt-3 block text-sm text-fg-soft">{nl ? 'Besluit en notulen' : 'Decision and minutes'}<textarea value={form.notes} onChange={e => setForm({ ...form, notes: e.target.value })} rows={4} className="mt-1 w-full rounded-lg border border-neutral-700 bg-page px-3 py-2 text-fg" /></label>
          <button type="button" disabled={!company || saving} onClick={() => void save()} className="mt-4 rounded-lg bg-accent-600 px-4 py-2 text-sm font-medium text-white disabled:opacity-50">{saving ? (nl ? 'Opslaan…' : 'Saving…') : (nl ? 'Besluit opslaan' : 'Save decision')}</button>
        </section>
        <section className={`${card} flex h-full min-h-0 flex-col overflow-hidden xl:h-[clamp(420px,calc(100vh-14rem),560px)]`}><div className="flex items-baseline justify-between gap-3"><h2 className="text-base font-semibold text-fg-strong">{nl ? 'Nieuws' : 'News'}</h2></div><div className="mt-4 min-h-0 flex-1 space-y-3 overflow-y-auto pr-1">{articles.map((a, i) => <a key={a.id || i} href={a.link} target="_blank" rel="noreferrer" className="block rounded-lg border border-neutral-800/60 p-3 hover:bg-overlay/[0.04]"><p className="text-sm font-medium text-fg-strong">{a.subject}</p>{a.subtitle && <p className="mt-1 text-xs text-fg-muted">{a.subtitle}</p>}<p className="mt-2 text-xs text-fg-faint">{formatDate(a.publish_time)}</p></a>)}{newsMessage && <p className="py-8 text-center text-sm text-fg-muted">{newsMessage}</p>}</div></section>
      </div>
      <section className={card}>
        <div className="flex items-baseline justify-between">
          <h2 className="text-base font-semibold text-fg-strong">{nl ? 'Besluitlog' : 'Decision log'}</h2>
          {reviewDue.length > 0 && <span className="text-sm text-warn-400">{reviewDue.length} {nl ? 'evaluatie' : `review${reviewDue.length === 1 ? '' : 's'}`} {nl ? 'open' : 'due'}</span>}
        </div>
        <div className="mt-4 overflow-x-auto">
          <table className="w-full min-w-[740px] text-left text-sm [&_tbody_button]:rounded-lg [&_tbody_button]:border [&_tbody_button]:border-neutral-700 [&_tbody_button]:bg-card [&_tbody_button]:px-2.5 [&_tbody_button]:py-1.5 [&_tbody_button]:font-medium [&_tbody_button]:transition-colors [&_tbody_button:hover]:border-neutral-500 [&_tbody_button:hover]:bg-overlay/[0.05]">
            <thead className="border-b border-neutral-800 text-fg-faint"><tr><th className="pb-2 font-medium">{nl ? 'Bedrijf' : 'Company'}</th><th className="pb-2 font-medium">{nl ? 'Besluit' : 'Decision'}</th><th className="pb-2 font-medium">{nl ? 'Convictie' : 'Conviction'}</th><th className="pb-2 font-medium">{nl ? 'Weging' : 'Weight'}</th><th className="pb-2 font-medium">{nl ? 'Evaluatie' : 'Review'}</th><th className="pb-2 font-medium">{nl ? 'Yahoo-rendement' : 'Yahoo return'}</th><th className="pb-2 font-medium">{nl ? 'Notities' : 'Notes'}</th><th className="pb-2 font-medium"><span className="sr-only">{nl ? 'Acties' : 'Actions'}</span></th></tr></thead>
            <tbody>{entries.map(e => { const result = yahooReturns[e.id]; return <tr key={e.id} className="border-b border-neutral-800/50 align-top"><td className="py-3"><div className="font-medium text-fg-strong">{e.company_name}</div><div className="font-mono text-xs text-fg-faint">{e.isin}</div></td><td className="py-3">{decisionLabel(e.decision)}</td><td className="py-3">{e.conviction}/5</td><td className="py-3">{e.portfolio_weight == null ? '—' : `${e.portfolio_weight}%`}</td><td className="py-3">{formatDate(e.review_on)}</td><td className={`py-3 font-medium ${result?.available && (result.return_pct ?? 0) >= 0 ? 'text-pos-400' : result?.available ? 'text-neg-400' : 'text-fg-muted'}`}>{!e.review_on ? '—' : !result ? '…' : result.available ? <span>{(result.return_pct ?? 0) >= 0 ? '+' : ''}{result.return_pct}% <span className={`ml-1 ${INFO_ICON}`} title={`${nl ? 'Yahoo-koersrendement vanaf' : 'Yahoo price return from'} ${formatDate(result.start_date)} ${nl ? 'tot en met' : 'through'} ${formatDate(result.as_of)}. ${nl ? 'Opgehaald:' : 'Retrieved:'} ${result.retrieved_at ? new Date(result.retrieved_at).toLocaleString(nl ? 'nl-NL' : 'en-GB') : '—'}`}>i</span></span> : '—'}</td><td className="max-w-sm py-3 text-fg-muted"><p className="line-clamp-2 whitespace-pre-wrap">{e.notes || '—'}</p></td><td className="py-3 text-right"><div className="flex justify-end gap-3"><button type="button" onClick={() => setOpenTarget(e)} className="text-xs text-fg-subtle hover:text-fg">{nl ? 'Openen' : 'Open'}</button><button type="button" onClick={() => setDeleteTarget(e)} className="text-xs text-fg-subtle hover:text-neg-400">{nl ? 'Verwijderen' : 'Delete'}</button></div></td></tr>})}{!entries.length && <tr><td colSpan={8} className="py-8 text-center text-fg-muted">{nl ? 'Nog geen besluiten vastgelegd.' : 'No decisions recorded yet.'}</td></tr>}</tbody>
          </table>
        </div>
      </section>
      {openTarget && <div className="fixed inset-0 z-[10001] flex items-center justify-center bg-scrim/70 p-4" role="dialog" aria-modal="true" aria-labelledby="open-log-title">
        <div className="w-full max-w-2xl rounded-xl border border-neutral-700 bg-popover p-5 shadow-xl">
          <h2 id="open-log-title" className="text-lg font-semibold text-fg-strong">{openTarget.company_name}</h2>
          <dl className="mt-5 grid grid-cols-2 gap-x-6 gap-y-4 rounded-lg border border-neutral-800/60 bg-card p-4 text-sm sm:grid-cols-3">
            <div><dt className="text-xs text-fg-faint">ISIN</dt><dd className="mt-1 font-mono text-fg-soft">{openTarget.isin}</dd></div>
            <div><dt className="text-xs text-fg-faint">{nl ? 'Besluit' : 'Decision'}</dt><dd className="mt-1 text-fg-soft">{decisionLabel(openTarget.decision)}</dd></div>
            <div><dt className="text-xs text-fg-faint">{nl ? 'Convictie' : 'Conviction'}</dt><dd className="mt-1 text-fg-soft">{openTarget.conviction}/5</dd></div>
            <div><dt className="text-xs text-fg-faint">{nl ? 'Portefeuilleweging' : 'Portfolio weight'}</dt><dd className="mt-1 text-fg-soft">{openTarget.portfolio_weight == null ? '—' : `${openTarget.portfolio_weight}%`}</dd></div>
            <div><dt className="text-xs text-fg-faint">{nl ? 'Vastgelegd op' : 'Recorded on'}</dt><dd className="mt-1 text-fg-soft">{formatDate(openTarget.meeting_date)}</dd></div>
            <div><dt className="text-xs text-fg-faint">{nl ? 'Evalueren op' : 'Review on'}</dt><dd className="mt-1 text-fg-soft">{formatDate(openTarget.review_on)}</dd></div>
            <div><dt className="text-xs text-fg-faint">{nl ? 'Yahoo-rendement' : 'Yahoo return'}</dt><dd className={`mt-1 font-medium ${openYahooReturn?.available && (openYahooReturn.return_pct ?? 0) >= 0 ? 'text-pos-400' : openYahooReturn?.available ? 'text-neg-400' : 'text-fg-soft'}`}>{!openTarget.review_on ? '—' : !openYahooReturn ? (nl ? 'Laden…' : 'Loading…') : openYahooReturn.available ? `${(openYahooReturn.return_pct ?? 0) >= 0 ? '+' : ''}${openYahooReturn.return_pct}%` : '—'}{openYahooReturn?.available && <span className="mt-0.5 block text-[11px] font-normal text-fg-faint">{formatDate(openYahooReturn.start_date)} – {formatDate(openYahooReturn.as_of)}</span>}</dd></div>
          </dl>
          <p className="mt-2 text-xs text-fg-faint">{nl ? 'Gebaseerd op Yahoo-slotkoersen; exclusief dividenden.' : 'Based on Yahoo closing prices; excluding dividends.'}</p>
          <div className="mt-5"><h3 className="text-sm font-medium text-fg-soft">{nl ? 'Notities' : 'Notes'}</h3><p className="mt-2 max-h-[45vh] overflow-y-auto whitespace-pre-wrap rounded-lg border border-neutral-800/60 bg-card p-4 text-sm leading-6 text-fg-soft">{openTarget.notes || '—'}</p></div>
          <div className="mt-5 flex justify-end"><button type="button" onClick={() => setOpenTarget(null)} className="rounded-lg bg-accent-600 px-3 py-2 text-sm font-medium text-white">{nl ? 'Sluiten' : 'Close'}</button></div>
        </div>
      </div>}
      {deleteTarget && <div className="fixed inset-0 z-[10001] flex items-center justify-center bg-scrim/70 p-4" role="dialog" aria-modal="true" aria-labelledby="delete-log-title"><div className="w-full max-w-md rounded-xl border border-neutral-700 bg-popover p-5 shadow-xl"><h2 id="delete-log-title" className="text-lg font-semibold text-fg-strong">{nl ? 'Besluit verwijderen?' : 'Delete decision?'}</h2><p className="mt-2 text-sm text-fg-muted">{nl ? <>Hiermee verwijder je het besluit voor <span className="font-medium text-fg-soft">{deleteTarget.company_name}</span> definitief uit het gedeelde BC-logboek.</> : <>This permanently removes the {deleteTarget.decision.toLowerCase()} decision for <span className="font-medium text-fg-soft">{deleteTarget.company_name}</span> from the shared BC logbook.</>}</p><div className="mt-5 flex justify-end gap-3"><button type="button" disabled={deleting} onClick={() => setDeleteTarget(null)} className="rounded-lg px-3 py-2 text-sm text-fg-muted hover:text-fg">{nl ? 'Annuleren' : 'Cancel'}</button><button type="button" disabled={deleting} onClick={() => void deleteEntry()} className="rounded-lg bg-neg-600 px-3 py-2 text-sm font-medium text-white hover:bg-neg-500 disabled:opacity-50">{deleting ? (nl ? 'Verwijderen…' : 'Deleting…') : (nl ? 'Besluit verwijderen' : 'Delete decision')}</button></div></div></div>}
    </div>
  </main>;
}
