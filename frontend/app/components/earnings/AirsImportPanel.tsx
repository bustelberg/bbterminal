'use client';

import { useEffect, useMemo, useState } from 'react';

import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import Spinner from '../Spinner';
import CompanyPicker from './CompanyPicker';
import type { Company } from './types';

type Candidate = { company_id: number; name: string | null; ticker: string | null; exchange: string | null; score: number };
type MatchRow = {
  holding_name: string;
  currency: string | null;
  current_value_eur: number | null;
  weight: number | null;
  match: Candidate | null;
  candidates: Candidate[];
};
type MatchResp = { portfolio_name: string; as_of_date: string | null; rows: MatchRow[] };
type AirsItem = { portfolio_name: string; as_of_date: string; holdings: number };

type Review = {
  holding_name: string;
  value: number;            // current_value_eur — basis for renormalization
  score: number | null;     // fuzzy score when the company is the auto-match; null once manually changed
  companyId: number | null; // resolved company, or null (unmatched)
  dropped: boolean;
};

/** "Import from AIRS" flow for the earnings Portfolio Manager: pick a stored
 * AIRS Vermogensoverzicht, fuzzy-match each Fonds to a company, let the user
 * confirm / change / drop each row, then hand the confirmed (value-weighted,
 * renormalized-to-100%) members back to the parent create form. */
export default function AirsImportPanel({
  companies,
  onImport,
  onCancel,
}: {
  companies: Company[];
  onImport: (name: string, rows: { company_id: number; ticker: string; name: string; weight: number }[]) => void;
  onCancel: () => void;
}) {
  const [list, setList] = useState<AirsItem[] | null>(null);
  const [portfolio, setPortfolio] = useState('');
  const [loading, setLoading] = useState(false);
  const [resp, setResp] = useState<MatchResp | null>(null);
  const [reviews, setReviews] = useState<Review[]>([]);
  const [err, setErr] = useState<string | null>(null);

  const byId = useMemo(() => {
    const m = new Map<number, Company>();
    companies.forEach((c) => m.set(c.company_id, c));
    return m;
  }, [companies]);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const r = await apiFetch(`${API_URL}/api/earnings/airs-portfolios`);
        if (cancelled) return;
        if (r.ok) setList(await r.json() as AirsItem[]);
        else setErr(`Failed to load AIRS portfolios (${r.status})`);
      } catch (e) { if (!cancelled) setErr(e instanceof Error ? e.message : String(e)); }
    })();
    return () => { cancelled = true; };
  }, []);

  const loadMatch = async (name: string) => {
    setPortfolio(name);
    setErr(null);
    if (!name) { setResp(null); setReviews([]); return; }
    setLoading(true);
    try {
      const r = await apiFetch(`${API_URL}/api/earnings/airs-portfolios/${encodeURIComponent(name)}/match`);
      if (!r.ok) { setErr(`Match failed (${r.status})`); return; }
      const data = await r.json() as MatchResp;
      setResp(data);
      setReviews(data.rows.map((row) => ({
        holding_name: row.holding_name,
        value: row.current_value_eur ?? 0,
        score: row.match?.score ?? null,
        companyId: row.match?.company_id ?? null,
        dropped: false,
      })));
    } catch (e) { setErr(e instanceof Error ? e.message : String(e)); }
    finally { setLoading(false); }
  };

  const setRow = (i: number, patch: Partial<Review>) =>
    setReviews((prev) => prev.map((r, idx) => (idx === i ? { ...r, ...patch } : r)));

  const kept = reviews.filter((r) => !r.dropped && r.companyId != null);
  const totalValue = kept.reduce((s, r) => s + (r.value || 0), 0);
  const unmatched = reviews.filter((r) => !r.dropped && r.companyId == null).length;

  const doImport = () => {
    if (kept.length === 0) return;
    const rows = kept.map((r) => {
      const c = byId.get(r.companyId!);
      const w = totalValue > 0 ? (r.value / totalValue) * 100 : 100 / kept.length;
      return {
        company_id: r.companyId!,
        ticker: c?.gurufocus_ticker ?? '',
        name: c?.company_name ?? c?.gurufocus_ticker ?? String(r.companyId),
        weight: Math.round(w * 100) / 100,
      };
    });
    const name = resp
      ? `AIRS: ${resp.portfolio_name}${resp.as_of_date ? ` (${resp.as_of_date})` : ''}`
      : 'AIRS portfolio';
    onImport(name, rows);
  };

  const scoreColor = (s: number) => (s >= 90 ? 'text-pos-300' : s >= 75 ? 'text-warn-300' : 'text-neg-300');

  return (
    <div className="p-5 space-y-3">
      <div className="flex items-center justify-between">
        <h3 className="text-fg-soft text-sm font-medium">Import from AIRS</h3>
        <button onClick={onCancel} className="text-xs text-fg-subtle hover:text-fg-strong transition-colors">← Back to manual</button>
      </div>

      {err && <div className="text-sm text-neg-300 bg-neg-500/10 border border-neg-500/20 rounded-lg px-3 py-2">{err}</div>}

      <div className="flex items-center gap-2">
        <span className="text-fg-faint text-xs">Portfolio</span>
        <select
          value={portfolio}
          onChange={(e) => void loadMatch(e.target.value)}
          className="flex-1 bg-page border border-neutral-700 rounded-lg px-3 py-2 text-sm text-fg-strong outline-none focus:border-accent-500"
        >
          <option value="">{list ? 'Select an AIRS portfolio…' : 'Loading…'}</option>
          {(list ?? []).map((p) => (
            <option key={p.portfolio_name} value={p.portfolio_name}>
              {p.portfolio_name} — {p.holdings} holdings (as of {p.as_of_date})
            </option>
          ))}
        </select>
        {loading && <Spinner size={14} />}
      </div>

      {resp && reviews.length > 0 && (
        <>
          <div className="text-fg-faint text-xs">
            {kept.length} matched · {unmatched} unmatched · {reviews.filter((r) => r.dropped).length} dropped — weights value-weighted, renormalized to 100%.
          </div>
          <div className="space-y-1.5 max-h-[26rem] overflow-y-auto pr-1">
            {reviews.map((r, i) => {
              const sel = r.companyId != null ? byId.get(r.companyId) ?? null : null;
              const pct = totalValue > 0 && !r.dropped && r.companyId != null ? (r.value / totalValue) * 100 : null;
              return (
                <div
                  key={`${r.holding_name}-${i}`}
                  className={`rounded-lg border px-3 py-2 ${r.dropped ? 'border-neutral-800/30 opacity-50' : r.companyId == null ? 'border-neg-500/30 bg-neg-500/[0.04]' : 'border-neutral-800/40'}`}
                >
                  <div className="flex items-center gap-2">
                    <span className="text-fg-soft text-sm flex-1 min-w-0 truncate" title={r.holding_name}>{r.holding_name}</span>
                    <span className="text-fg-faint text-xs font-mono shrink-0">
                      {r.value ? `€${Math.round(r.value).toLocaleString()}` : '—'}
                      {pct != null && <span className="text-fg-muted"> · {pct.toFixed(1)}%</span>}
                    </span>
                    <button
                      onClick={() => setRow(i, { dropped: !r.dropped })}
                      className={`text-xs px-2 py-0.5 rounded transition-colors shrink-0 ${r.dropped ? 'text-accent-400 hover:text-accent-300' : 'text-fg-faint hover:text-neg-400'}`}
                    >
                      {r.dropped ? 'restore' : 'drop'}
                    </button>
                  </div>
                  {!r.dropped && (
                    <div className="flex items-center gap-2 mt-1.5">
                      <span className="text-fg-faint text-[11px] shrink-0">→</span>
                      <CompanyPicker
                        companies={companies}
                        selected={sel}
                        onSelect={(c) => setRow(i, { companyId: c.company_id, score: null })}
                        className="flex-1 min-w-0"
                      />
                      {r.companyId != null ? (
                        <span className="text-[11px] shrink-0 w-24 text-right">
                          {r.score != null
                            ? <span className={scoreColor(r.score)}>match {r.score.toFixed(0)}%</span>
                            : <span className="text-accent-300">manual</span>}
                        </span>
                      ) : (
                        <span className="text-[11px] text-neg-300 shrink-0 w-24 text-right">no match</span>
                      )}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
          <div className="flex items-center gap-3 pt-1">
            <button
              onClick={doImport}
              disabled={kept.length === 0}
              className="px-4 py-2 rounded-lg text-sm font-medium bg-accent-600 hover:bg-accent-500 text-fg-strong disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
            >
              Import {kept.length} {kept.length === 1 ? 'holding' : 'holdings'} → form
            </button>
            {unmatched > 0 && (
              <span className="text-warn-300 text-xs">{unmatched} unmatched row{unmatched === 1 ? '' : 's'} will be skipped — pick a company or drop them.</span>
            )}
          </div>
        </>
      )}

      {resp && reviews.length === 0 && !loading && (
        <p className="text-fg-subtle text-sm py-3">No holdings stored for this portfolio yet.</p>
      )}
    </div>
  );
}
