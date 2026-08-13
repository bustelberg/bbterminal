'use client';

import { useEffect, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { chartTheme } from '../../../lib/chartTheme';
import { cashConversionOf, type CashConversionInputs, type CashConversionRow } from './cashConversionData';
import { RatioInputsTable, type InputsLine } from './RatioInputsTable';
import { type BenchTarget } from './benchSeries';
import { type Target } from './HoldingsRevenueModal';

/** The base inputs behind Cash conversion — TWO rows per company (Free Cash Flow, Net Income),
 * each in the company's own reporting currency (millions).
 *
 * ⚠ A VISIBLE LOSS WITH A BLANK CONVERSION IS THE POINT. The ratio does not apply to negative
 * earnings, so the row shows both amounts and no percentage — the reader can see exactly why.
 * Same columns / Unsubscribed / Fetch behaviour as the other drill-downs. Self-fetches (so Fetch
 * can reload). Mirrors {@link ./SbcOcfInputsModal}. */

/**
 * ⚠ THE TABLE IS `RatioInputsTable`, SHARED BY EVERY RATIO CARD. What is left in this file is the
 * fetch, the prose, and the card's own two constants — the lines it lists and the figure it
 * derives. Eleven near-identical copies of that table used to exist, which is why the benchmark
 * only ever got built into one of them, and why adding the cap/weight lines was a ten-file edit.
 *
 * ⚠ THE BENCHMARK IS THE SAME ENDPOINT AND THE SAME TABLE — `{holdings|portfolio_id}` swapped for
 * `{universe}`. One component renders both, so the book's rows and the index's cannot come to
 * format a figure or hide a status differently on the one screen built for comparing them.
 *
 * ⚠ THE DERIVED LINE CALLS THE CARD'S OWN FUNCTION, and `RatioInputsTable` feeds that same function
 * to `periodDenoms` — which is what makes the `weight` line under each company sum to exactly 100%
 * of the line the chart drew.
 */

const LINES: InputsLine<CashConversionRow>[] = [
  { label: 'Free cash flow', of: (r, y) => r.fcf[y] },
  { label: 'Net income', of: (r, y) => r.net_income[y], muted: true },
];

export default function CashConversionInputsModal({ target, portfolioName, benchTarget, benchLabel, onClose }: {
  target: Target; portfolioName?: string | null; onClose: () => void;
  /** The index the chart is drawn against, when one is ticked. Same endpoint, same table. */
  benchTarget?: BenchTarget | null;
  benchLabel?: string | null;
}) {
  const [data, setData] = useState<CashConversionInputs | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [reloadKey, setReloadKey] = useState(0);

  const load = async (body: Target | BenchTarget): Promise<CashConversionInputs> => {
    const r = await apiFetch(`${API_URL}/api/earnings/cash-conversion-inputs`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const b = await r.json().catch(() => null);
    if (!r.ok) throw new Error(b?.detail ?? `HTTP ${r.status}`);
    return b as CashConversionInputs;
  };

  useEffect(() => {
    let alive = true;
    void (async () => {
      setData(null); setErr(null);
      try {
        const b = await load(target);
        if (alive) setData(b);
      } catch (e) {
        if (alive) setErr(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { alive = false; };
     
  }, [target, reloadKey]);

  /** The index's constituents. Silent on failure: it is an addition to a modal that works. */
  const [bench, setBench] = useState<CashConversionInputs | null>(null);
  const [benchErr, setBenchErr] = useState<string | null>(null);
  const benchKey = benchTarget ? `${benchTarget.universe}|${benchTarget.cadence}` : '';
  useEffect(() => {
    let alive = true;
    void (async () => {
      setBench(null); setBenchErr(null);
      if (!benchTarget) return;
      try {
        const b = await load(benchTarget);
        if (alive) setBench(b);
      } catch (e) {
        console.warn('[bb:bench] cash-conversion-inputs constituents:', e);
        if (alive) setBenchErr(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { alive = false; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [benchKey]);

  /** Fetch a `no_data` holding's financials, then reload. Throws the stated reason otherwise, which
   *  the row renders — a fetch that loaded financials carrying none of these lines is a real
   *  answer, not a failure. */
  const fetchFinancials = async (isin: string, name: string) => {
    const r = await apiFetch(`${API_URL}/api/earnings/fundamental-coverage/ingest`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ isin, name }),
    });
    const j = (await r.json().catch(() => null)) as { status?: string; detail?: string } | null;
    if (r.ok && j?.status === 'ingested') { setReloadKey((k) => k + 1); return; }
    throw new Error(j?.detail ?? j?.status ?? `HTTP ${r.status}`);
  };

  const section = 'text-[12px] uppercase tracking-wide text-fg-muted';
  const derived = { label: 'FCF / Net Income', kind: 'ratio' as const, of: (r: CashConversionRow, y: string) => cashConversionOf(r.fcf[y], r.net_income[y]) };

  return (
    <div className="fixed inset-0 z-[60] flex items-center justify-center bg-scrim/60 p-4"
      onClick={onClose} role="presentation">
      <div className="bg-card rounded-xl border border-neutral-800/40 shadow-xl w-[88vw] h-[84vh] flex flex-col"
        onClick={(e) => e.stopPropagation()} role="dialog" aria-modal="true">
        <div className="flex items-baseline gap-3 px-6 py-4 border-b border-neutral-800/40">
          <h2 className="text-fg-strong font-medium">Holdings — FCF / Net Income inputs by year</h2>
          {portfolioName && <span className="text-sm text-fg-soft truncate max-w-[24ch]" title={portfolioName}>{portfolioName}</span>}
          {data && <span className="text-[12px] text-fg-faint">{data.rows.length} companies</span>}
          {benchLabel && <span className="text-[12px]" style={{ color: chartTheme.pos }}>vs {benchLabel}</span>}
          <button type="button" onClick={onClose} className="ml-auto text-fg-muted hover:text-fg-strong px-2">✕</button>
        </div>

        <div className="flex-1 overflow-auto px-6 py-4 space-y-5">
          <p className="text-[12px] text-fg-faint">Free cash flow and net income (millions, native currency). Conversion = FCF ÷ Net Income. Above 100% is normal — depreciation ahead of capex. A loss has no conversion, so the percentage is blank there.</p>

          <div className="space-y-1.5">
            <h3 className={section}>{portfolioName ? `${portfolioName} — ` : ''}inputs by year</h3>
            {err && <p className="text-xs text-neg-300">{err}</p>}
            {!data && !err && <p className="text-xs text-fg-subtle">Loading…</p>}
            {data && data.rows.length === 0 && !err && <p className="text-xs text-fg-subtle">No held company has these figures ingested.</p>}
            {data && data.rows.length > 0 && (
              <RatioInputsTable data={data} lines={LINES} derived={derived} onFetch={fetchFinancials} />
            )}
          </div>

          {benchTarget && (
            <div className="space-y-1.5">
              <h3 className={section}>{benchLabel} constituents — inputs by year</h3>
              {!bench && !benchErr && <p className="text-xs text-fg-subtle">Loading {benchLabel} constituents…</p>}
              {benchErr && <p className="text-xs text-neg-300">{benchErr}</p>}
              {bench && (
                <>
                  {/* ⚠ THE COVERAGE GAP IS STATED. Only constituents with these lines ingested feed
                      the benchmark line, so a table longer than the contributing set is not a
                      mismatch — it IS the gap, and the `weight` line renormalises over what is
                      left, period by period. */}
                  <p className="text-[11px] text-fg-faint">
                    {bench.rows.length} constituents ·{' '}
                    {bench.rows.filter((r) => r.status === 'ok').length} with figures feed the line,
                    renormalised each period
                  </p>
                  <RatioInputsTable data={bench} lines={LINES} derived={derived} />
                </>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
