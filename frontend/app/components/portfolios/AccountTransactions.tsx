'use client';

import { useCallback, useEffect, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import type { AirsAccountTransactions } from '../../../lib/types/api';

/**
 * WHAT THIS BOOK BOUGHT AND SOLD — the AIRS Transacties report, beside the positions it produced.
 *
 * The three reports already on this screen say what a book HOLDS (Vermogensoverzicht), what it
 * EARNED (Mutaties) and what its strategy ASKS FOR (Model). None of them says what it DID: a
 * position that appeared mid-year, one that was sold out entirely, and a weight that drifted purely
 * because the market moved are indistinguishable from the outside.
 *
 * ⚠ THE COLUMNS ARE RENDERED FROM THE PAYLOAD, NOT DECLARED HERE, AND THAT IS DELIBERATE. No column
 * of the TRANS report has ever been measured — `rapport_types=TRANS` was probed in 2026-07 and
 * confirmed only to return an XLS. Every other AIRS table in this app names its columns because
 * somebody read the sheet first. Hard-coding a guess would put "Bedrag" in a column headed
 * "Bedrag eur" — one word apart, and the wrong one carries no FX leg, so it renders as a plausible
 * number rather than an error. This table shows the report AS the report until the sheet has been
 * seen; then the columns that matter get promoted, formatted and explained like every other.
 *
 * ⚠ ALIGNMENT COMES FROM THE SERVER'S `kinds`, WHICH IS A DTYPE, NOT A MEANING. A column pandas
 * read as text stays left-aligned and unformatted even if it is named like an amount — that is a
 * visible fact about the export, and hiding it behind a number formatter is how a column that only
 * sometimes parses gets trusted.
 *
 * ⚠ AN EMPTY TABLE MUST NAME ITS OWN CAUSE. Three very different things produce zero rows — the
 * book did not trade, AIRS has no such report for it, or we could not ask — and a bare "no
 * transactions" asserts the first. `note` carries the reason and is always shown when present.
 */

/** Loaded lazily: nothing is fetched until the section is opened, because the first fetch of an
 *  account goes out to AIRS behind a headless session and takes seconds. */
export default function AccountTransactions({ portefeuille }: { portefeuille: string }) {
  const [open, setOpen] = useState(false);
  const [data, setData] = useState<AirsAccountTransactions | null>(null);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  const load = useCallback(async (refresh: boolean) => {
    setLoading(true);
    setErr(null);
    const t0 = performance.now();
    try {
      const r = await apiFetch(`${API_URL}/api/airs/accounts/${encodeURIComponent(portefeuille)}`
        + `/transactions${refresh ? '?refresh=true' : ''}`);
      if (!r.ok) throw new Error(`${r.status} ${await r.text()}`);
      const j: AirsAccountTransactions = await r.json();
      console.warn(`[AIRS transactions] ${portefeuille}: ${j.rows?.length ?? 0} row(s) `
        + `from ${j.source} in ${Math.round(performance.now() - t0)}ms`, j.note ?? '');
      setData(j);
    } catch (e) {
      // ⚠ THE DETAIL GOES TO THE CONSOLE, ONE SHORT LINE TO THE UI. A stack trace in a table cell
      // is not a message a reader can act on.
      console.warn(`[AIRS transactions] ${portefeuille} failed`, e);
      setErr('Could not load transactions — see the console for the full error.');
    } finally {
      setLoading(false);
    }
  }, [portefeuille]);

  // Fetch ONCE, on first open. Re-opening a section already loaded costs nothing; the explicit
  // Refresh is the only thing that goes back to AIRS.
  useEffect(() => {
    if (open && !data && !loading && !err) void load(false);
  }, [open, data, loading, err, load]);

  const rows = data?.rows ?? [];
  const columns = data?.columns ?? [];
  const kinds = data?.kinds ?? {};

  return (
    <div className="space-y-2">
      <button type="button" onClick={() => setOpen((v) => !v)}
        aria-expanded={open}
        className="w-full flex items-center gap-2 text-left text-[12px] px-2 py-1.5 rounded-lg border border-neutral-800/40 bg-card hover:bg-overlay/5 transition-colors">
        <span className={`text-[9px] text-fg-faint transition-transform ${open ? 'rotate-90' : ''}`}>▶</span>
        <span className="font-medium text-fg-strong">Transactions</span>
        <span className="text-fg-faint">
          {/* ⚠ BEFORE THE FIRST OPEN WE DO NOT KNOW THE COUNT, AND SAYING SO BEATS SHOWING "0".
              The count costs an AIRS download; a 0 rendered while nothing has been asked is a
              claim that this book never traded. */}
          {loading ? 'loading…'
            : err && !data ? <span className="text-neg-400">could not load</span>
              : !data ? 'what this book bought and sold'
                : `${rows.length} row${rows.length === 1 ? '' : 's'}`}
        </span>
        {data && (
          <span className="ml-auto flex items-center gap-2 text-[11px] text-fg-faint">
            <span>{data.datum_van} → {data.datum_tot}</span>
            {/* ⚠ A CACHED ANSWER SHOWN AS FRESH IS HOW A STALE FIGURE GETS TRUSTED. Same rule the
                model-portfolio positions follow. */}
            {data.cached_at && <span title={`Stored ${data.cached_at}`}>cached</span>}
          </span>
        )}
      </button>

      {open && (
        <div className="space-y-2">
          {/* The reason behind whatever the table shows — including behind zero rows, where it is
              the difference between "did not trade", "AIRS has no such report" and "could not
              ask". Amber, because every one of those is something to notice rather than an error. */}
          {data?.note && (
            <p className="text-[11px] text-warn-500 px-1">{data.note}</p>
          )}
          {/* ⚠ THE ACTION IS OFFERED ON THE FAILURE PATH TOO. The auto-load fires once and then
              stops (a loop against a failing AIRS session helps nobody), so without this a
              transient failure could only be cleared by collapsing the whole account row. */}
          {(data || err) && (
            <div className="flex items-center gap-2 text-[11px] px-1">
              <button type="button" disabled={loading} onClick={() => void load(true)}
                title="Re-download this book's Transacties from AIRS. Seconds, behind the shared AIRS session — the stored snapshot is served until it lands."
                className="px-2 py-1 rounded-md border border-neutral-800/40 text-fg-subtle hover:bg-overlay/5 disabled:opacity-50 transition-colors">
                {loading ? 'Loading…' : data ? 'Refresh from AIRS' : 'Try again'}
              </button>
              {/* ⚠ SAID ON SCREEN, NOT ONLY IN THE CODE. A reader looking at unfamiliar column
                  headings deserves to know they are AIRS's own and not ours — otherwise a missing
                  column reads as a bug in this page. */}
              {data && (
                <span className="text-fg-faint">
                  AIRS’s own columns, shown as the report gives them.
                </span>
              )}
            </div>
          )}
          {loading && !data && <p className="text-[12px] text-fg-subtle px-1">Loading transactions…</p>}
          {err && <p className="text-[12px] text-neg-400 px-1">{err}</p>}
          {data && !columns.length && !data.note && (
            <p className="text-[12px] text-fg-subtle px-1">
              This book has no transactions in this period.
            </p>
          )}
          {!!columns.length && (
            // A dense table scrolls inside its own box so the page never scrolls sideways.
            <div className="overflow-x-auto rounded-lg border border-neutral-800/40">
              <table className="w-full text-xs whitespace-nowrap">
                <thead className="bg-card [&_th]:bg-card">
                  <tr className="text-fg-faint text-[11px] uppercase tracking-wide border-b border-neutral-800/40">
                    <th className="px-3 py-1.5 font-medium text-right w-10">#</th>
                    {columns.map((c) => (
                      <th key={c}
                        className={`px-3 py-1.5 font-medium ${kinds[c] === 'number' ? 'text-right' : 'text-left'}`}
                        title={`AIRS column “${c}” — ${kinds[c] ?? 'text'}`}>
                        {c}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody className="divide-y divide-neutral-800/20">
                  {rows.map((r, i) => (
                    <tr key={i} className="hover:bg-overlay/[0.02]">
                      <td className="px-3 py-1.5 text-right font-mono text-[11px] text-fg-faint tabular-nums">
                        {i + 1}
                      </td>
                      {columns.map((c) => {
                        const v = r[c];
                        const kind = kinds[c] ?? 'text';
                        return (
                          <td key={c}
                            className={`px-3 py-1.5 ${kind === 'text'
                              ? 'text-fg-soft'
                              : 'text-right font-mono tabular-nums text-fg'}`}>
                            {/* ⚠ A BLANK IS A BLANK, NEVER A 0. The server already turns an empty
                                Excel cell into null rather than the truthy string "nan"; printing
                                a 0 here would put a number where the report has none. */}
                            {v == null ? <span className="text-fg-faint">—</span>
                              : kind === 'number' ? fmtNum(v as number)
                                : String(v)}
                          </td>
                        );
                      })}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

/** A number as the sheet reports it — thousands separated, up to 6 decimals, NOTHING assumed about
 *  units. ⚠ No currency symbol and no rounding to 2dp: we do not know which columns are amounts,
 *  which are quantities and which are prices, and a € in front of a share count would be a claim.
 *  `maximumFractionDigits: 6` because a quantity or an FX rate carries more than two and truncating
 *  one silently changes the value on screen. */
function fmtNum(v: number): string {
  return v.toLocaleString('en-US', { maximumFractionDigits: 6 });
}
