'use client';

import { useCallback, useMemo, useState } from 'react';
import { API_URL } from '../../../lib/apiUrl';
import { apiFetch } from '../../../lib/apiFetch';
import { dialog } from '../../../lib/dialog';
import { invalidateStaticUniverses, useStaticUniverses, type UniverseTemplate } from '../../../lib/hooks/apiData';
import LoadingDots from '../LoadingDots';

type MemberRow = {
  company_id: number;
  company_name: string | null;
  ticker: string | null;
  exchange: string | null;
  isin: string | null;
};

type CompareResult = {
  universe_label: string;
  target_month: string | null;
  universe_member_count: number;
  csv_isin_count: number;
  matched_count: number;
  unmatched_count: number;
  intersection: MemberRow[];
  in_universe_not_in_csv: MemberRow[];
  csv_isins_not_in_universe: string[];
};

/** Parse a CSV/XLSX file into a header row + data rows (array-of-arrays).
 * Lazy-loads SheetJS (already a dependency) so it ships only on this page. */
async function parseSpreadsheet(file: File): Promise<{ headers: string[]; rows: string[][] }> {
  const XLSX = await import('xlsx');
  const buf = await file.arrayBuffer();
  const wb = XLSX.read(buf, { type: 'array' });
  const ws = wb.Sheets[wb.SheetNames[0]];
  if (!ws) return { headers: [], rows: [] };
  const aoa = XLSX.utils.sheet_to_json(ws, { header: 1, blankrows: false, raw: false }) as unknown[][];
  if (aoa.length === 0) return { headers: [], rows: [] };
  const headers = (aoa[0] ?? []).map((c) => String(c ?? '').trim());
  const rows = aoa.slice(1).map((r) => headers.map((_, i) => String(r[i] ?? '').trim()));
  return { headers, rows };
}

export default function IsinComparer() {
  const { data: universes, loading: universesLoading } = useStaticUniverses();

  const [fileName, setFileName] = useState<string | null>(null);
  const [headers, setHeaders] = useState<string[]>([]);
  const [rows, setRows] = useState<string[][]>([]);
  const [colIdx, setColIdx] = useState<number | null>(null);
  const [universeLabel, setUniverseLabel] = useState('');
  const [parsing, setParsing] = useState(false);
  const [running, setRunning] = useState(false);
  const [pruning, setPruning] = useState(false);
  const [pruneMsg, setPruneMsg] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<CompareResult | null>(null);
  // Freshly-refetched universe list after a prune — the cached
  // `useStaticUniverses` value (and its member counts) is stale until the next
  // page mount, so we refetch here and render the dropdown from this when set.
  const [universesFresh, setUniversesFresh] = useState<UniverseTemplate[] | null>(null);
  const universeOptions = universesFresh ?? universes ?? [];

  const onFile = useCallback(async (file: File | null) => {
    if (!file) return;
    setParsing(true);
    setError(null);
    setResult(null);
    try {
      const { headers: h, rows: r } = await parseSpreadsheet(file);
      if (h.length === 0) {
        setError('Could not read any columns from that file.');
        setHeaders([]); setRows([]); setColIdx(null);
        return;
      }
      setFileName(file.name);
      setHeaders(h);
      setRows(r);
      // Auto-pick a column that looks like ISINs (header contains "isin"),
      // else the first column.
      const guess = h.findIndex((x) => x.toLowerCase().includes('isin'));
      setColIdx(guess >= 0 ? guess : 0);
    } catch (e) {
      setError(`Failed to parse file: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setParsing(false);
    }
  }, []);

  // Cleaned, de-duplicated ISIN list from the chosen column (drops empty cells).
  const { isins, rawCount } = useMemo(() => {
    if (colIdx == null) return { isins: [] as string[], rawCount: 0 };
    const vals = rows.map((r) => (r[colIdx] ?? '').trim().toUpperCase()).filter((v) => v.length > 0);
    return { isins: Array.from(new Set(vals)), rawCount: vals.length };
  }, [rows, colIdx]);

  const runCompare = useCallback(async () => {
    if (!universeLabel || isins.length === 0) return;
    setRunning(true);
    setError(null);
    try {
      const res = await apiFetch(`${API_URL}/api/isin-compare`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ universe_label: universeLabel, isins }),
      });
      if (!res.ok) {
        const d = await res.json().catch(() => ({}));
        throw new Error(typeof d.detail === 'string' ? d.detail : `HTTP ${res.status}`);
      }
      setResult(await res.json());
      setPruneMsg(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
    setRunning(false);
  }, [universeLabel, isins]);

  // Prune the selected universe down to the intersection: permanently drop the
  // members not in the CSV. Confirms first, then re-runs the compare so the
  // tables reflect the pruned universe.
  const onPrune = useCallback(async () => {
    if (!result || result.unmatched_count === 0) return;
    const n = result.unmatched_count;
    const ok = await dialog.confirm(
      `Permanently remove ${n} compan${n === 1 ? 'y' : 'ies'} (the ones not in your CSV) from "${result.universe_label}"?\n\n`
      + `This edits the universe's membership and can't be undone here — the ${result.matched_count} matched will remain.`,
      { destructive: true, confirmLabel: `Drop ${n}` },
    );
    if (!ok) return;
    setPruning(true);
    setError(null);
    setPruneMsg(null);
    try {
      const res = await apiFetch(`${API_URL}/api/isin-compare/prune`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          universe_label: result.universe_label,
          drop_company_ids: result.in_universe_not_in_csv.map((m) => m.company_id),
        }),
      });
      if (!res.ok) {
        const d = await res.json().catch(() => ({}));
        throw new Error(typeof d.detail === 'string' ? d.detail : `HTTP ${res.status}`);
      }
      const out = await res.json();
      // Refresh the dropdown's member counts (the cached list is now stale) and
      // drop the shared cache so other pages refetch fresh on their next mount.
      invalidateStaticUniverses();
      try {
        const ures = await apiFetch(`${API_URL}/api/static-universes`);
        if (ures.ok) setUniversesFresh(await ures.json());
      } catch { /* non-fatal — the result summary still shows the live count */ }
      await runCompare();
      setPruneMsg(`Dropped ${out.dropped} — ${out.remaining_member_count} members remain in ${result.universe_label}.`);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
    setPruning(false);
  }, [result, runCompare]);

  return (
    <div className="flex flex-col h-full">
      <div className="px-8 py-5 border-b border-neutral-800/60">
        <h1 className="text-lg font-semibold text-fg-strong">ISIN ↔ Universe Compare</h1>
        <p className="text-xs text-fg-subtle mt-0.5">
          Upload a CSV, pick the column of ISINs, and compare it against a universe — see which
          members are in your list (intersection) and which universe members are missing from it.
        </p>
      </div>

      <div className="flex-1 overflow-auto px-8 py-5 space-y-5">
        {error && (
          <div className="bg-neg-500/10 border border-neg-500/20 rounded-lg px-4 py-2.5 text-sm text-neg-400">{error}</div>
        )}

        {/* Step 1 — file + column */}
        <div className="bg-card rounded-xl border border-neutral-800/40 p-5">
          <h3 className="text-fg-muted text-xs font-medium uppercase tracking-wider mb-3">1 · Upload &amp; pick the ISIN column</h3>
          <div className="flex flex-wrap items-end gap-3">
            <label className="px-4 py-2 rounded-lg text-sm font-medium border border-accent-500 text-accent-400 hover:bg-accent-600/10 transition-colors cursor-pointer">
              {parsing ? <LoadingDots label="Parsing" /> : 'Choose CSV…'}
              <input
                type="file"
                accept=".csv,text/csv,.xlsx,.xls"
                className="hidden"
                onChange={(e) => onFile(e.target.files?.[0] ?? null)}
              />
            </label>
            {fileName && <span className="text-xs text-fg-subtle font-mono">{fileName} · {rows.length} rows</span>}
            {headers.length > 0 && (
              <div>
                <label className="text-fg-subtle text-xs block mb-1">ISIN column</label>
                <select
                  value={colIdx ?? ''}
                  onChange={(e) => setColIdx(e.target.value === '' ? null : Number(e.target.value))}
                  className="bg-page border border-neutral-700 rounded-lg px-3 py-2 text-fg-strong text-sm max-w-xs focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none"
                >
                  {headers.map((h, i) => (
                    <option key={i} value={i}>{h || `Column ${i + 1}`}</option>
                  ))}
                </select>
              </div>
            )}
          </div>
          {colIdx != null && rows.length > 0 && (
            <p className="text-xs text-fg-subtle mt-3">
              <span className="font-mono text-fg-strong">{isins.length}</span> distinct non-empty ISINs
              {rawCount - isins.length > 0 && <> ({rawCount - isins.length} duplicate{rawCount - isins.length === 1 ? '' : 's'} / blanks dropped)</>}.
            </p>
          )}
        </div>

        {/* Step 2 — universe + run */}
        <div className="bg-card rounded-xl border border-neutral-800/40 p-5">
          <h3 className="text-fg-muted text-xs font-medium uppercase tracking-wider mb-3">2 · Compare against a universe</h3>
          <div className="flex flex-wrap items-end gap-3">
            <div className="flex-1 min-w-64">
              <label className="text-fg-subtle text-xs block mb-1">Universe (frozen snapshots)</label>
              <select
                value={universeLabel}
                onChange={(e) => setUniverseLabel(e.target.value)}
                className="w-full bg-page border border-neutral-700 rounded-lg px-3 py-2 text-fg-strong text-sm focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none"
              >
                <option value="">{universesLoading ? 'Loading…' : 'Select a universe…'}</option>
                {universeOptions.map((u) => (
                  <option key={u.template_key} value={u.template_key}>
                    {u.label || u.template_key} ({u.latest_membership_count} members)
                  </option>
                ))}
              </select>
            </div>
            <button
              onClick={runCompare}
              disabled={!universeLabel || isins.length === 0 || running}
              className="px-4 py-2 rounded-lg text-sm font-medium bg-accent-600 hover:bg-accent-500 text-fg-strong transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
            >
              {running ? <LoadingDots /> : 'Compare'}
            </button>
          </div>
        </div>

        {result && result.unmatched_count > 0 && (
          <div className="bg-card rounded-xl border border-warn-500/30 p-5 flex items-center justify-between gap-3 flex-wrap">
            <div className="min-w-0">
              <h3 className="text-sm font-medium text-fg-strong">Prune universe to the intersection</h3>
              <p className="text-xs text-fg-subtle mt-0.5">
                Permanently remove the {result.unmatched_count} member{result.unmatched_count === 1 ? '' : 's'} not in your CSV
                from <span className="font-mono text-fg">{result.universe_label}</span>, keeping the {result.matched_count} matched.
              </p>
              {pruneMsg && <p className="text-xs text-pos-400 mt-1">{pruneMsg}</p>}
            </div>
            <button
              onClick={onPrune}
              disabled={pruning}
              className="px-4 py-2 rounded-lg text-sm font-medium border border-neg-500/50 text-neg-300 hover:bg-neg-500/10 transition-colors disabled:opacity-40 disabled:cursor-not-allowed shrink-0"
            >
              {pruning ? <LoadingDots /> : `Drop ${result.unmatched_count} from universe`}
            </button>
          </div>
        )}

        {result && <ResultView result={result} />}
      </div>
    </div>
  );
}

function ResultView({ result: r }: { result: CompareResult }) {
  return (
    <>
      <div className="bg-card rounded-xl border border-neutral-800/40 p-5">
        <div className="flex flex-wrap gap-x-6 gap-y-1.5 text-sm">
          <span className="text-fg-subtle text-xs uppercase tracking-wider self-center">{r.universe_label}{r.target_month ? ` · ${r.target_month}` : ''}</span>
          <span className="text-fg-muted">Universe <span className="font-mono text-fg-strong">{r.universe_member_count}</span></span>
          <span className="text-fg-muted">CSV ISINs <span className="font-mono text-fg-strong">{r.csv_isin_count}</span></span>
          <span className="text-fg-muted">Intersection <span className="font-mono text-pos-400">{r.matched_count}</span></span>
          <span className="text-fg-muted">In universe, not in CSV <span className="font-mono text-warn-400">{r.unmatched_count}</span></span>
          <span className="text-fg-muted">CSV not in universe <span className="font-mono text-fg">{r.csv_isins_not_in_universe.length}</span></span>
        </div>
      </div>

      <MemberTable
        title={`Intersection — in the universe AND in your CSV (${r.matched_count})`}
        rows={r.intersection}
        accent="pos"
      />
      <MemberTable
        title={`In the universe but NOT in your CSV (${r.unmatched_count})`}
        rows={r.in_universe_not_in_csv}
        accent="warn"
      />

      {r.csv_isins_not_in_universe.length > 0 && (
        <div className="bg-card rounded-xl border border-neutral-800/40 p-5">
          <h3 className="text-fg-muted text-xs font-medium uppercase tracking-wider mb-3">
            CSV ISINs not matched to any universe member ({r.csv_isins_not_in_universe.length})
          </h3>
          <div className="flex flex-wrap gap-1.5">
            {r.csv_isins_not_in_universe.map((isin) => (
              <span key={isin} className="font-mono text-xs px-2 py-0.5 rounded bg-inset border border-neutral-800/40 text-fg-muted">{isin}</span>
            ))}
          </div>
        </div>
      )}
    </>
  );
}

function MemberTable({ title, rows, accent }: { title: string; rows: MemberRow[]; accent: 'pos' | 'warn' }) {
  const dot = accent === 'pos' ? 'bg-pos-500' : 'bg-warn-500';
  return (
    <div className="bg-card rounded-xl border border-neutral-800/40 p-5">
      <h3 className="text-fg-muted text-xs font-medium uppercase tracking-wider mb-3 flex items-center gap-2">
        <span className={`inline-block w-2 h-2 rounded-full ${dot}`} />{title}
      </h3>
      {rows.length === 0 ? (
        <p className="text-sm text-fg-subtle">None.</p>
      ) : (
        <div className="overflow-auto max-h-[60vh]">
          <table className="w-full text-sm">
            <thead className="sticky top-0 bg-card">
              <tr className="text-fg-subtle text-xs border-b border-neutral-800/60">
                <th className="text-left font-medium py-2 pr-2">Ticker</th>
                <th className="text-left font-medium py-2 px-2">ISIN</th>
                <th className="text-left font-medium py-2 px-2">Company</th>
                <th className="text-left font-medium py-2 px-2">Exchange</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((m) => (
                <tr key={m.company_id} className="border-b border-neutral-800/30 hover:bg-overlay/[0.02]">
                  <td className="py-2 pr-2 font-mono text-fg-strong whitespace-nowrap">{m.ticker || '—'}</td>
                  <td className="py-2 px-2 font-mono text-fg-muted whitespace-nowrap">{m.isin || <span className="text-fg-faint">no ISIN</span>}</td>
                  <td className="py-2 px-2 text-fg truncate max-w-[260px]">{m.company_name || '—'}</td>
                  <td className="py-2 px-2 text-fg-subtle whitespace-nowrap">{m.exchange || '—'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
