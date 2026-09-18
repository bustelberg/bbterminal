'use client';

import { useEffect, useMemo, useState } from 'react';
import { API_URL } from '../../../lib/apiUrl';
import { apiFetch } from '../../../lib/apiFetch';
import CompanyPicker, { type AssetPick } from '../research/CompanyPicker';

type Entry = {
  id: number; meeting_date: string; company_name: string; isin: string; decision: string;
  notes: string; actions: string; conviction: number; portfolio_weight: number | null;
  flag: 'none' | 'yellow' | 'red'; review_on: string | null;
};
type Article = { id?: number; subject?: string; subtitle?: string; publish_time?: string; link?: string };

const card = 'rounded-xl border border-neutral-800/60 bg-card p-5';

export default function LogDashboard() {
  const [entries, setEntries] = useState<Entry[]>([]);
  const [company, setCompany] = useState<AssetPick | null>(null);
  const [articles, setArticles] = useState<Article[]>([]);
  const [newsMessage, setNewsMessage] = useState('Choose a US company to see its GuruFocus news.');
  const [saving, setSaving] = useState(false);
  const [form, setForm] = useState({ decision: 'Hold', notes: '', actions: '', conviction: '3', portfolio_weight: '', flag: 'none', review_on: '' });

  async function loadEntries() {
    const r = await apiFetch(`${API_URL}/api/log-dashboard/entries`);
    if (r.ok) setEntries(await r.json());
  }
  useEffect(() => { void loadEntries(); }, []);

  useEffect(() => {
    const ticker = company?.yahoo_symbol?.replace(/\..*$/, '');
    if (!ticker) { setArticles([]); setNewsMessage('Choose a US company to see its GuruFocus news.'); return; }
    const ctrl = new AbortController();
    setNewsMessage('Loading GuruFocus news…');
    void (async () => {
      const r = await apiFetch(`${API_URL}/api/log-dashboard/news/${encodeURIComponent(ticker)}`, { signal: ctrl.signal });
      if (ctrl.signal.aborted) return;
      const body = await r.json().catch(() => ({}));
      if (!r.ok) { setArticles([]); setNewsMessage((body.detail as string) || 'No GuruFocus news is available for this company.'); return; }
      setArticles(body.articles || []);
      setNewsMessage(body.message || (body.articles?.length ? '' : 'No GuruFocus articles were published for this ticker in the available period.'));
    })();
    return () => ctrl.abort();
  }, [company]);

  const reviewDue = useMemo(() => entries.filter((e) => e.review_on && e.review_on <= new Date().toISOString().slice(0, 10)), [entries]);
  async function save() {
    if (!company) return;
    setSaving(true);
    try {
      const r = await apiFetch(`${API_URL}/api/log-dashboard/entries`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({
        company_name: company.name || company.isin, isin: company.isin, decision: form.decision, notes: form.notes, actions: form.actions,
        conviction: Number(form.conviction), portfolio_weight: form.portfolio_weight ? Number(form.portfolio_weight) : null,
        flag: form.flag, review_on: form.review_on || null,
      }) });
      if (!r.ok) throw new Error((await r.json().catch(() => ({}))).detail || 'Could not save the decision.');
      setForm({ decision: 'Hold', notes: '', actions: '', conviction: '3', portfolio_weight: '', flag: 'none', review_on: '' });
      await loadEntries();
    } finally { setSaving(false); }
  }

  return <main className="min-h-screen bg-page px-5 py-8 text-fg sm:px-8 lg:px-10">
    <div className="mx-auto max-w-7xl space-y-6">
      <header><h1 className="text-2xl font-semibold text-fg-strong">BC logbook</h1><p className="mt-1 text-sm text-fg-muted">Record decisions, follow up on actions and review the investment case over time.</p></header>
      <div className="grid gap-6 xl:grid-cols-[minmax(0,1.05fr)_minmax(0,0.95fr)]">
        <section className={`${card} xl:h-[clamp(420px,calc(100vh-14rem),560px)] xl:overflow-y-auto`}>
          <h2 className="text-base font-semibold text-fg-strong">New decision</h2>
          <div className="mt-4"><CompanyPicker label="Company" value={company} onPick={setCompany} /></div>
          <div className="mt-4 grid gap-3 sm:grid-cols-2">
            <label className="text-sm text-fg-soft">Decision<select value={form.decision} onChange={e => setForm({ ...form, decision: e.target.value })} className="mt-1 w-full rounded-lg border border-neutral-700 bg-page px-3 py-2 text-fg"><option>Buy</option><option>Hold</option><option>Reduce</option><option>Sell</option><option>Watch</option></select></label>
            <label className="text-sm text-fg-soft">Conviction (1–5)<select value={form.conviction} onChange={e => setForm({ ...form, conviction: e.target.value })} className="mt-1 w-full rounded-lg border border-neutral-700 bg-page px-3 py-2 text-fg">{[1,2,3,4,5].map(n => <option key={n}>{n}</option>)}</select></label>
            <label className="text-sm text-fg-soft">Portfolio weight (%)<input value={form.portfolio_weight} onChange={e => setForm({ ...form, portfolio_weight: e.target.value })} inputMode="decimal" className="mt-1 w-full rounded-lg border border-neutral-700 bg-page px-3 py-2 text-fg" /></label>
            <label className="text-sm text-fg-soft">Flag<select value={form.flag} onChange={e => setForm({ ...form, flag: e.target.value })} className="mt-1 w-full rounded-lg border border-neutral-700 bg-page px-3 py-2 text-fg"><option value="none">None</option><option value="yellow">Yellow flag</option><option value="red">Red flag</option></select></label>
          </div>
          <label className="mt-3 block text-sm text-fg-soft">Decision and minutes<textarea value={form.notes} onChange={e => setForm({ ...form, notes: e.target.value })} rows={4} className="mt-1 w-full rounded-lg border border-neutral-700 bg-page px-3 py-2 text-fg" /></label>
          <div className="mt-3 grid gap-3 sm:grid-cols-2"><label className="text-sm text-fg-soft">Action points<textarea value={form.actions} onChange={e => setForm({ ...form, actions: e.target.value })} rows={2} className="mt-1 w-full rounded-lg border border-neutral-700 bg-page px-3 py-2 text-fg" /></label><label className="text-sm text-fg-soft">Review on<input type="date" value={form.review_on} onChange={e => setForm({ ...form, review_on: e.target.value })} className="mt-1 w-full rounded-lg border border-neutral-700 bg-page px-3 py-2 text-fg" /></label></div>
          <button type="button" disabled={!company || saving} onClick={() => void save()} className="mt-4 rounded-lg bg-accent-600 px-4 py-2 text-sm font-medium text-white disabled:opacity-50">{saving ? 'Saving…' : 'Save decision'}</button>
        </section>
        <section className={`${card} flex h-full min-h-0 flex-col overflow-hidden xl:h-[clamp(420px,calc(100vh-14rem),560px)]`}><div className="flex items-baseline justify-between gap-3"><h2 className="text-base font-semibold text-fg-strong">News</h2><span className="text-xs text-fg-faint">GuruFocus · US coverage</span></div><div className="mt-4 min-h-0 flex-1 space-y-3 overflow-y-auto pr-1">{articles.map((a, i) => <a key={a.id || i} href={a.link} target="_blank" rel="noreferrer" className="block rounded-lg border border-neutral-800/60 p-3 hover:bg-overlay/[0.04]"><p className="text-sm font-medium text-fg-strong">{a.subject}</p>{a.subtitle && <p className="mt-1 text-xs text-fg-muted">{a.subtitle}</p>}<p className="mt-2 text-xs text-fg-faint">{a.publish_time}</p></a>)}{newsMessage && <p className="py-8 text-center text-sm text-fg-muted">{newsMessage}</p>}</div></section>
      </div>
      <section className={card}><div className="flex items-baseline justify-between"><h2 className="text-base font-semibold text-fg-strong">Decision log</h2>{reviewDue.length > 0 && <span className="text-sm text-warn-400">{reviewDue.length} review{reviewDue.length === 1 ? '' : 's'} due</span>}</div><div className="mt-4 overflow-x-auto"><table className="w-full min-w-[760px] text-left text-sm"><thead className="border-b border-neutral-800 text-fg-faint"><tr><th className="pb-2 font-medium">Company</th><th className="pb-2 font-medium">Decision</th><th className="pb-2 font-medium">Conviction</th><th className="pb-2 font-medium">Weight</th><th className="pb-2 font-medium">Flag</th><th className="pb-2 font-medium">Review</th><th className="pb-2 font-medium">Notes</th></tr></thead><tbody>{entries.map(e => <tr key={e.id} className="border-b border-neutral-800/50 align-top"><td className="py-3"><div className="font-medium text-fg-strong">{e.company_name}</div><div className="font-mono text-xs text-fg-faint">{e.isin}</div></td><td className="py-3">{e.decision}</td><td className="py-3">{e.conviction}/5</td><td className="py-3">{e.portfolio_weight == null ? '—' : `${e.portfolio_weight}%`}</td><td className={`py-3 ${e.flag === 'red' ? 'text-neg-400' : e.flag === 'yellow' ? 'text-warn-400' : 'text-fg-muted'}`}>{e.flag === 'none' ? '—' : e.flag}</td><td className="py-3">{e.review_on || '—'}</td><td className="max-w-sm whitespace-pre-wrap py-3 text-fg-muted">{e.notes || e.actions || '—'}</td></tr>)}{!entries.length && <tr><td colSpan={7} className="py-8 text-center text-fg-muted">No decisions recorded yet.</td></tr>}</tbody></table></div></section>
    </div>
  </main>;
}
