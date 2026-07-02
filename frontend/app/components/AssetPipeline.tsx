'use client';

import { useMemo, useState } from 'react';
import { apiFetch } from '../../lib/apiFetch';
import { API_URL } from '../../lib/apiUrl';
import LoadingDots from './LoadingDots';
import AssetPipelineBatch from './AssetPipelineBatch';
import AssetPipelineCatalog from './AssetPipelineCatalog';

// --- Types (mirror the /api/asset-pipeline/resolve dict; endpoint is untyped) ---
type Candidate = {
  symbol: string;
  currency: string | null;
  exchange: string | null;
  med_adv_eur: number;
  first_date: string | null;
  years: number;
  quote_type: string | null;
  name: string | null;
  eligible?: boolean;
};
type Candle = {
  date: string;
  open: number | null; high: number | null; low: number | null; close: number | null; volume: number | null;
};
type Ibkr = {
  status: string; message: string;
  isin?: string; candidates?: unknown[]; chosen?: unknown;
} | null;
type Result = {
  input: string;
  id_type: string;
  asset_class: string | null;
  wrapper: string | null;
  candidates: Candidate[];
  /** What you'd TRADE — the resolved tradeable listing (an ETF for a BTC ETP). */
  execution: Candidate | null;
  /** What you BACKTEST — the underlying's long series when the input is a
   * single-underlying wrapper, else the same as execution. */
  analysis: Candidate | null;
  underlying: { symbol: string; label: string } | null;
  reason: string;
  /** Why analysis != execution (the underlying swap), or null. */
  analysis_note: string | null;
  sector: string | null;
  /** Candles OF THE ANALYSIS instrument. */
  candles: { oldest: Candle[]; newest: Candle[] } | null;
  ibkr: Ibkr;
};

const ISIN_RE = /\b[A-Z]{2}[A-Z0-9]{9}\d\b/g;

/** Median daily traded value, EUR — compact. */
function adv(v: number): string {
  if (v >= 1e9) return `€${(v / 1e9).toFixed(2)}B`;
  if (v >= 1e6) return `€${(v / 1e6).toFixed(1)}M`;
  if (v >= 1e3) return `€${(v / 1e3).toFixed(0)}k`;
  return `€${v.toFixed(0)}`;
}
const num = (v: number | null, d = 2) =>
  v == null ? '—' : v.toLocaleString(undefined, { minimumFractionDigits: d, maximumFractionDigits: d });
const vol = (v: number | null) => (v == null ? '—' : v.toLocaleString());

function Panel({ label, right, children }: { label: string; right?: React.ReactNode; children: React.ReactNode }) {
  return (
    <section className="bg-card border border-neutral-800/40 rounded-xl p-4">
      <div className="flex items-center justify-between gap-3 mb-3 min-h-6">
        <h3 className="text-[11px] uppercase tracking-wider text-fg-muted font-medium">{label}</h3>
        {right}
      </div>
      {children}
    </section>
  );
}

function Field({ k, children }: { k: string; children: React.ReactNode }) {
  return (
    <>
      <dt className="text-fg-faint">{k}</dt>
      <dd className="font-mono text-fg-soft truncate">{children}</dd>
    </>
  );
}

const CHIP_TONES: Record<string, string> = {
  neutral: 'bg-inset text-fg-soft border-neutral-800/40',
  accent: 'bg-accent-500/10 text-accent-300 border-accent-500/20',
  warn: 'bg-warn-500/10 text-warn-300 border-warn-500/20',
};
function Chip({ children, tone = 'neutral', title }: { children: React.ReactNode; tone?: keyof typeof CHIP_TONES; title?: string }) {
  return (
    <span title={title} className={`text-[10px] uppercase tracking-wide px-1.5 py-0.5 rounded border ${CHIP_TONES[tone]} ${title ? 'cursor-help' : ''}`}>
      {children}
    </span>
  );
}

/** One instrument (analysis or execution) as a labelled block: symbol, name,
 * and an aligned key/value field grid + chips. */
function InstrumentBlock({ role, inst, children }: { role: string; inst: Candidate; children?: React.ReactNode }) {
  return (
    <div className="min-w-0">
      <div className="text-[10px] uppercase tracking-wider text-fg-faint mb-1">{role}</div>
      <div className="text-lg font-mono font-semibold text-fg-strong truncate">{inst.symbol}</div>
      <div className="text-sm text-fg-soft truncate" title={inst.name ?? ''}>{inst.name ?? '—'}</div>
      <dl className="mt-2 grid grid-cols-[auto_1fr] gap-x-3 gap-y-0.5 text-xs">
        <Field k="Exch">{inst.exchange ?? '—'} · {inst.currency ?? '—'}</Field>
        <Field k="History">{inst.first_date ?? '—'} · {inst.years}y</Field>
        <Field k="Liquidity">{inst.med_adv_eur ? `${adv(inst.med_adv_eur)}/d` : '—'}</Field>
      </dl>
      {children && <div className="mt-2 flex flex-wrap gap-1.5">{children}</div>}
    </div>
  );
}

function CandleTable({ title, rows }: { title: string; rows: Candle[] }) {
  return (
    <div>
      <div className="text-[11px] uppercase tracking-wide text-fg-faint mb-1">{title}</div>
      <div className="overflow-auto rounded-lg border border-neutral-800/40 inline-block max-w-full">
        <table className="text-xs">
          <thead className="bg-card">
            <tr className="text-fg-faint text-[10px] uppercase tracking-wide border-b border-neutral-800/40">
              <th className="px-2 py-1.5 text-left font-medium whitespace-nowrap">Date</th>
              <th className="px-2 py-1.5 text-right font-medium whitespace-nowrap">Open</th>
              <th className="px-2 py-1.5 text-right font-medium whitespace-nowrap">High</th>
              <th className="px-2 py-1.5 text-right font-medium whitespace-nowrap">Low</th>
              <th className="px-2 py-1.5 text-right font-medium whitespace-nowrap">Close</th>
              <th className="px-2 py-1.5 text-right font-medium whitespace-nowrap">Volume</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-neutral-800/20">
            {rows.map((r) => (
              <tr key={r.date} className="font-mono">
                <td className="px-2 py-1.5 text-fg-soft whitespace-nowrap">{r.date}</td>
                <td className="px-2 py-1.5 text-right text-fg-muted whitespace-nowrap">{num(r.open)}</td>
                <td className="px-2 py-1.5 text-right text-fg-muted whitespace-nowrap">{num(r.high)}</td>
                <td className="px-2 py-1.5 text-right text-fg-muted whitespace-nowrap">{num(r.low)}</td>
                <td className="px-2 py-1.5 text-right text-fg whitespace-nowrap">{num(r.close)}</td>
                <td className="px-2 py-1.5 text-right text-fg-subtle whitespace-nowrap">{vol(r.volume)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export default function AssetPipeline() {
  const [isins, setIsins] = useState<string[]>([]);
  const [fileName, setFileName] = useState<string | null>(null);
  const [identifier, setIdentifier] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<Result | null>(null);
  const [catalogReload, setCatalogReload] = useState(0);
  const [storing, setStoring] = useState(false);
  const [stored, setStored] = useState<{ rows: number; analysis: string; stored_fields: string[] } | null>(null);
  const [storeErr, setStoreErr] = useState<string | null>(null);

  const onFile = async (file: File) => {
    const text = await file.text();
    const found = Array.from(text.matchAll(ISIN_RE), (m) => m[0]);
    // Fallback: if no ISIN pattern matched, take the first column of each line.
    const list = found.length
      ? found
      : text.split(/\r?\n/).map((l) => l.split(/[,;\t]/)[0].trim()).filter(Boolean);
    setIsins([...new Set(list)]);
    setFileName(file.name);
  };

  const run = async (idRaw: string) => {
    const id = idRaw.trim();
    if (!id) return;
    setIdentifier(id);
    setLoading(true);
    setError(null);
    setResult(null);
    setStored(null);
    setStoreErr(null);
    try {
      const r = await apiFetch(`${API_URL}/api/asset-pipeline/resolve?identifier=${encodeURIComponent(id)}`);
      const body = await r.json().catch(() => null);
      if (!r.ok) {
        setError(body?.detail ?? `HTTP ${r.status}`);
      } else {
        setResult(body as Result);
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  };

  const random = () => {
    if (!isins.length) return;
    void run(isins[Math.floor(Math.random() * isins.length)]);
  };

  const storeAsset = async () => {
    if (!result || storing) return;
    setStoring(true); setStoreErr(null);
    try {
      const r = await apiFetch(`${API_URL}/api/asset-pipeline/store`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ identifier: result.input }),
      });
      const body = await r.json().catch(() => null);
      if (!r.ok) setStoreErr(body?.detail ?? `HTTP ${r.status}`);
      else { setStored(body); setCatalogReload((x) => x + 1); }
    } catch (e) {
      setStoreErr(e instanceof Error ? e.message : String(e));
    } finally {
      setStoring(false);
    }
  };

  const analysis = result?.analysis ?? null;
  const execution = result?.execution ?? null;
  const ranked = useMemo(
    () => [...(result?.candidates ?? [])].sort((a, b) => b.med_adv_eur - a.med_adv_eur),
    [result],
  );

  return (
    <div className="min-h-screen bg-page text-fg">
      <div className="px-8 py-5 border-b border-neutral-800/40">
        <h1 className="text-xl font-semibold text-fg-strong">Asset Pipeline</h1>
        <p className="text-sm text-fg-subtle mt-1">ISIN → analysis (backtest) + execution (trade). Yahoo-sourced.</p>
      </div>

      <div className="px-8 py-6 space-y-4">
        {/* Resolve */}
        <Panel
          label="Resolve"
          right={isins.length > 0 ? (
            <div className="flex items-center gap-2">
              <select value="" onChange={(e) => { if (e.target.value) void run(e.target.value); }}
                className="bg-page border border-neutral-700 rounded-lg px-2 py-1 text-xs text-fg focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 max-w-[200px]">
                <option value="">From CSV ({isins.length})…</option>
                {isins.map((i) => <option key={i} value={i}>{i}</option>)}
              </select>
              <button type="button" onClick={random} className="text-xs px-2.5 py-1 rounded-lg border border-neutral-700 text-fg-muted hover:text-accent-300 hover:border-accent-500/50 transition-colors">Random</button>
            </div>
          ) : (
            <label className="text-xs px-2.5 py-1 rounded-lg border border-neutral-700 text-fg-muted hover:text-accent-300 hover:border-accent-500/50 cursor-pointer transition-colors">
              Upload CSV
              <input type="file" accept=".csv,text/csv,text/plain" className="hidden" onChange={(e) => { const f = e.target.files?.[0]; if (f) void onFile(f); }} />
            </label>
          )}
        >
          <div className="flex items-center gap-2">
            <input value={identifier} onChange={(e) => setIdentifier(e.target.value)}
              onKeyDown={(e) => { if (e.key === 'Enter') void run(identifier); }}
              placeholder="ISIN or symbol — US0378331005 · BTC-USD · GC=F · EURUSD=X"
              className="flex-1 bg-page border border-neutral-700 rounded-lg px-3 py-2 text-sm font-mono text-fg focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30" />
            <button type="button" onClick={() => void run(identifier)} disabled={loading || !identifier.trim()}
              className="text-sm px-4 py-2 rounded-lg bg-accent-600 hover:bg-accent-500 text-white disabled:opacity-40 disabled:cursor-not-allowed transition-colors">
              {loading ? 'Resolving…' : 'Resolve'}
            </button>
          </div>
          {fileName && <div className="mt-2 text-[11px] text-fg-faint font-mono">{fileName} · {isins.length} ISINs</div>}
        </Panel>

        <AssetPipelineBatch isins={isins} onIngested={() => setCatalogReload((x) => x + 1)} />
        <AssetPipelineCatalog reloadSignal={catalogReload} />

        {loading && <div className="bg-card border border-neutral-800/40 rounded-xl px-5 py-4"><LoadingDots label="Resolving via Yahoo" /></div>}
        {error && <div className="bg-neg-500/10 border border-neg-500/20 rounded-lg px-4 py-3 text-sm text-neg-300">{error}</div>}
        {result && !loading && !analysis && (
          <div className="bg-card border border-neutral-800/40 rounded-xl px-4 py-3 text-sm text-fg-subtle">No analysis instrument. {result.reason}</div>
        )}

        {result && !loading && analysis && (
          <>
            {/* Result: analysis vs execution */}
            <Panel
              label="Result"
              right={
                <div className="flex items-center gap-2">
                  {stored && <span className="text-[11px] text-pos-400 font-mono">✓ {stored.rows.toLocaleString()} rows · {stored.stored_fields.join('+')}</span>}
                  {storeErr && <span className="text-[11px] text-neg-300">{storeErr}</span>}
                  <button type="button" onClick={() => void storeAsset()} disabled={storing}
                    title="Persist the analysis instrument's daily close + volume"
                    className="text-xs px-3 py-1.5 rounded-lg bg-accent-600 hover:bg-accent-500 text-white disabled:opacity-40 disabled:cursor-not-allowed transition-colors">
                    {storing ? 'Storing…' : 'Store close+volume'}
                  </button>
                </div>
              }
            >
              <div className="grid gap-5 sm:grid-cols-2">
                <InstrumentBlock role="Analysis · backtest" inst={analysis}>
                  <Chip tone="accent">{result.asset_class ?? '—'}</Chip>
                  {result.sector && result.sector !== result.asset_class && <Chip>{result.sector}</Chip>}
                </InstrumentBlock>
                {execution ? (
                  <InstrumentBlock role="Execution · trade" inst={execution}>
                    {result.wrapper && execution.symbol !== analysis.symbol && <Chip tone="accent">{result.wrapper} → {analysis.symbol}</Chip>}
                    {result.ibkr && <Chip tone={result.ibkr.status === 'stub' ? 'warn' : 'neutral'} title={result.ibkr.message}>IBKR · {result.ibkr.status}</Chip>}
                  </InstrumentBlock>
                ) : (
                  <div className="min-w-0">
                    <div className="text-[10px] uppercase tracking-wider text-fg-faint mb-1">Execution · trade</div>
                    <div className="text-sm text-fg-subtle">Native symbol — no tradeable listing.</div>
                  </div>
                )}
              </div>
              {result.analysis_note && (
                <div className="mt-3 text-xs text-warn-300 bg-warn-500/10 border border-warn-500/20 rounded-lg px-3 py-1.5">{result.analysis_note}</div>
              )}
            </Panel>

            {/* Ranking */}
            {result.candidates.length > 0 && (
              <Panel label={`Ranking · ${result.candidates.length} listings`}>
                <div className="overflow-auto rounded-lg border border-neutral-800/40 max-w-3xl">
                  <table className="w-full text-xs">
                    <thead className="bg-card">
                      <tr className="text-fg-faint text-[10px] uppercase tracking-wide border-b border-neutral-800/40">
                        <th className="px-3 py-1.5 text-left font-medium">Symbol</th>
                        <th className="px-3 py-1.5 text-left font-medium">Exchange</th>
                        <th className="px-3 py-1.5 text-left font-medium">Ccy</th>
                        <th className="px-3 py-1.5 text-right font-medium" title="Median daily traded value in EUR (liquidity)">€ ADV</th>
                        <th className="px-3 py-1.5 text-right font-medium">Since</th>
                        <th className="px-3 py-1.5 text-right font-medium">Years</th>
                        <th className="px-3 py-1.5 text-center font-medium" title="Meets the minimum-history floor">Elig.</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-neutral-800/20">
                      {ranked.map((c) => {
                        const isChosen = execution?.symbol === c.symbol;
                        return (
                          <tr key={c.symbol} className={isChosen ? 'bg-pos-500/10' : (c.med_adv_eur < (execution?.med_adv_eur ?? 0) * 0.01 ? 'opacity-50' : '')}>
                            <td className="px-3 py-1.5 font-mono text-fg whitespace-nowrap">
                              {c.symbol}{isChosen && <span className="ml-1.5 text-[9px] uppercase tracking-wide px-1 py-0.5 rounded bg-pos-500/20 text-pos-300 border border-pos-500/30">trade</span>}
                            </td>
                            <td className="px-3 py-1.5 text-fg-subtle whitespace-nowrap">
                              <span className="inline-block max-w-[160px] truncate align-bottom" title={c.exchange ?? ''}>{c.exchange ?? '—'}</span>
                            </td>
                            <td className="px-3 py-1.5 font-mono text-fg-muted whitespace-nowrap">{c.currency ?? '—'}</td>
                            <td className="px-3 py-1.5 text-right font-mono text-fg whitespace-nowrap">{adv(c.med_adv_eur)}</td>
                            <td className="px-3 py-1.5 text-right font-mono text-fg-subtle whitespace-nowrap">{c.first_date ?? '—'}</td>
                            <td className="px-3 py-1.5 text-right font-mono text-fg-muted whitespace-nowrap">{c.years}</td>
                            <td className="px-3 py-1.5 text-center">{c.eligible ? <span className="text-pos-400">✓</span> : <span className="text-fg-faint">·</span>}</td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
                <p className="text-[11px] text-fg-faint mt-2">{result.reason}</p>
              </Panel>
            )}

            {/* Candles */}
            {result.candles && (
              <Panel label={`Candles · ${analysis.symbol}`}>
                <div className="flex flex-wrap gap-4">
                  <CandleTable title="Oldest 5" rows={result.candles.oldest} />
                  <CandleTable title="Newest 5" rows={result.candles.newest} />
                </div>
              </Panel>
            )}
          </>
        )}
      </div>
    </div>
  );
}
