'use client';

import { useCallback, useEffect, useState } from 'react';
import { apiFetch } from '../../lib/apiFetch';
import { API_URL } from '../../lib/apiUrl';

type Execution = {
  execution_id: number; isin: string; yahoo_symbol: string | null; name: string | null;
  exchange: string | null; currency: string | null; med_adv_eur: number | null;
  first_date: string | null; years: number | null; wrapper: string | null;
  is_leveraged: boolean; is_default: boolean;
};
type Asset = {
  analysis_id: number; symbol: string; asset_class: string | null; label: string | null;
  sector: string | null; currency: string | null; first_date: string | null; years: number | null;
  executions: number; price_rows: number; volume_rows: number; price_from: string | null; price_to: string | null;
  executions_list: Execution[];
};

const adv = (v: number | null) =>
  v == null ? '—' : v >= 1e9 ? `€${(v / 1e9).toFixed(2)}B` : v >= 1e6 ? `€${(v / 1e6).toFixed(1)}M` : `€${(v / 1e3).toFixed(0)}k`;

/** Browse what the pipeline has stored: analysis assets (dedup'd) with their
 * execution instruments nested and price coverage. Read-only. */
export default function AssetPipelineCatalog({ reloadSignal }: { reloadSignal?: number }) {
  const [assets, setAssets] = useState<Asset[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [open, setOpen] = useState<Set<number>>(new Set());

  const load = useCallback(async () => {
    setLoading(true); setError(null);
    try {
      const r = await apiFetch(`${API_URL}/api/asset-pipeline/assets`);
      const body = await r.json().catch(() => null);
      if (!r.ok) setError(body?.detail ?? `HTTP ${r.status}`);
      else setAssets((body?.assets ?? []) as Asset[]);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, []);
  useEffect(() => { void load(); }, [load, reloadSignal]);

  const toggle = (id: number) => setOpen((s) => {
    const n = new Set(s);
    if (n.has(id)) n.delete(id); else n.add(id);
    return n;
  });

  return (
    <section className="bg-card border border-neutral-800/40 rounded-xl p-5 space-y-3">
      <div className="flex items-center justify-between gap-3">
        <h3 className="text-sm font-semibold text-fg-strong">Stored assets{assets ? ` (${assets.length})` : ''}</h3>
        <button type="button" onClick={() => void load()} disabled={loading}
          className="text-xs px-3 py-1.5 rounded-lg border border-neutral-700 text-fg-muted hover:text-accent-300 hover:border-accent-500/50 disabled:opacity-40 transition-colors">
          {loading ? 'Loading…' : 'Refresh'}
        </button>
      </div>

      {error && <div className="bg-neg-500/10 border border-neg-500/20 rounded-lg px-3 py-2 text-xs text-neg-300">{error}</div>}
      {assets && assets.length === 0 && <div className="text-xs text-fg-subtle">Nothing ingested yet — run a batch above.</div>}

      {assets && assets.length > 0 && (
        <div className="overflow-auto rounded-lg border border-neutral-800/40 max-w-5xl">
          <table className="w-full text-xs">
            <thead className="bg-card">
              <tr className="text-fg-faint text-[10px] uppercase tracking-wide border-b border-neutral-800/40">
                <th className="px-3 py-1.5 text-left font-medium">Analysis symbol</th>
                <th className="px-3 py-1.5 text-left font-medium">Class</th>
                <th className="px-3 py-1.5 text-left font-medium">Sector</th>
                <th className="px-3 py-1.5 text-right font-medium" title="Tradeable executions mapped to this asset">Exec.</th>
                <th className="px-3 py-1.5 text-right font-medium">Price rows</th>
                <th className="px-3 py-1.5 text-right font-medium" title="Rows with a stored volume value — aligned 1:1 with price rows (a no-trade day is volume 0, still counted)">Vol rows</th>
                <th className="px-3 py-1.5 text-right font-medium">Coverage</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-neutral-800/20">
              {assets.map((a) => (
                <FragmentRow key={a.analysis_id} a={a} isOpen={open.has(a.analysis_id)} onToggle={() => toggle(a.analysis_id)} />
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}

function FragmentRow({ a, isOpen, onToggle }: { a: Asset; isOpen: boolean; onToggle: () => void }) {
  return (
    <>
      <tr className="hover:bg-overlay/[0.02] cursor-pointer" onClick={onToggle}>
        <td className="px-3 py-1.5 font-mono text-fg whitespace-nowrap">
          <span className="text-fg-faint mr-1">{isOpen ? '▾' : '▸'}</span>{a.symbol}
          <span className="ml-2 text-fg-subtle font-sans inline-block max-w-[260px] truncate align-bottom" title={a.label ?? ''}>{a.label ?? ''}</span>
        </td>
        <td className="px-3 py-1.5 text-fg-subtle whitespace-nowrap">{a.asset_class ?? '—'}</td>
        <td className="px-3 py-1.5 text-fg-subtle whitespace-nowrap">
          <span className="inline-block max-w-[160px] truncate align-bottom" title={a.sector ?? ''}>{a.sector ?? '—'}</span>
        </td>
        <td className="px-3 py-1.5 text-right font-mono text-fg-muted whitespace-nowrap">{a.executions}</td>
        <td className="px-3 py-1.5 text-right font-mono text-fg whitespace-nowrap">{a.price_rows.toLocaleString()}</td>
        <td className={`px-3 py-1.5 text-right font-mono whitespace-nowrap ${a.volume_rows > 0 ? 'text-fg-muted' : 'text-fg-faint'}`}>{a.volume_rows.toLocaleString()}</td>
        <td className="px-3 py-1.5 text-right font-mono text-fg-subtle whitespace-nowrap">{a.price_from ?? '—'} → {a.price_to ?? '—'}</td>
      </tr>
      {isOpen && (
        <tr className="bg-inset/40">
          <td colSpan={7} className="px-3 py-2">
            <div className="text-[10px] uppercase tracking-wide text-fg-faint mb-1">Executions (trade one of these)</div>
            <div className="space-y-1">
              {a.executions_list.length === 0 && <div className="text-[11px] text-fg-faint">No executions (native symbol).</div>}
              {a.executions_list.map((e) => (
                <div key={e.execution_id} className="flex flex-wrap items-center gap-x-3 gap-y-0.5 text-[11px] font-mono">
                  <span className="text-fg-soft w-16 shrink-0">{e.yahoo_symbol ?? '—'}</span>
                  <span className="text-fg-faint w-32 shrink-0">{e.isin}</span>
                  <span className="text-fg-subtle">{e.exchange ?? '—'} · {e.currency ?? '—'}</span>
                  <span className="text-fg-muted">{adv(e.med_adv_eur)} ADV</span>
                  {e.is_default && <span className="text-[9px] uppercase px-1 py-0.5 rounded bg-pos-500/20 text-pos-300 border border-pos-500/30">default</span>}
                  {e.wrapper && <span className="text-[9px] uppercase px-1 py-0.5 rounded bg-accent-500/15 text-accent-300 border border-accent-500/20">{e.wrapper}</span>}
                  {e.is_leveraged && <span className="text-[9px] uppercase px-1 py-0.5 rounded bg-warn-500/15 text-warn-300 border border-warn-500/20">leveraged</span>}
                </div>
              ))}
            </div>
          </td>
        </tr>
      )}
    </>
  );
}
