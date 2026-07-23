'use client';

import { useEffect, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { type Basket } from './PerformanceModal';

/**
 * What a portfolio-level fundamentals view can and cannot reach — BY WEIGHT.
 *
 * ⚠ COVERAGE IS THE FIRST ANSWER, NOT A FOOTNOTE. Every holding that cannot be reached is weight
 * that would silently drop out of any blend, and a blended figure over half a book presented as
 * the book's is the same fabrication the AIRS return coverage floor already refuses.
 *
 * ⚠ A COUNT WOULD LIE. Nine covered minnows and one uncovered giant is not 90% coverage.
 */
type Row = {
  isin: string | null; name: string | null; weight_pct: number; reason: string;
  company_name: string | null;
};
type Coverage = {
  holdings: number; covered_pct: number;
  by_reason_pct: Record<string, number>; rows: Row[];
};

// ⚠ `unsubscribed` and `no_company` are NOT synonyms: one is a purchase decision, the other a
// five-minute ingest. They are worded so the difference survives being skim-read.
const REASON: Record<string, { label: string; note: string; tone: string }> = {
  covered: { label: 'covered', tone: 'text-pos-400',
    note: 'a company row exists and its fundamentals can be fetched.' },
  unsubscribed: { label: 'no GuruFocus subscription', tone: 'text-warn-400',
    note: 'a real company, on an exchange outside our GuruFocus subscription (India, UK, Ireland, Russia, Africa, LatAm, AU/NZ). The data exists and we cannot buy it — the only gap here a purchase would fix.' },
  no_company: { label: 'company not ingested', tone: 'text-warn-300',
    note: 'an equity we hold no company row for. A gap in our own ingest, not in the subscription — fixable by adding it.' },
  no_metrics: { label: 'fundamentals not ingested', tone: 'text-warn-300',
    note: 'the company IS in our database and no fundamentals have been fetched for it. A third remedy again: not a purchase, not adding the company — running the earnings ingest. Measured 2026-07-23: 2,776 company rows, seven with any annual metric.' },
  fund: { label: 'fund (holds companies, is not one)', tone: 'text-fg-muted',
    note: 'an ETF or fund has no income statement of its own. Looking through to its constituents is a different feature, not a gap in this one.' },
  not_equity: { label: 'not an equity', tone: 'text-fg-muted',
    note: 'a bond, future, FX or crypto line. A coupon is not an earnings stream — the question does not apply.' },
  cash: { label: 'cash', tone: 'text-fg-faint', note: 'no ISIN, nothing to look up.' },
};

export default function FundamentalCoverage({ basket, portfolioId }: {
  basket?: Basket; portfolioId?: number;
}) {
  const [cov, setCov] = useState<Coverage | null>(null);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    let alive = true;
    const body = basket
      ? { holdings: basket.holdings.map((h) => ({ isin: h.isin, name: h.name, weight: h.weight })) }
      : { portfolio_id: portfolioId };
    void (async () => {
      try {
        const r = await apiFetch(`${API_URL}/api/earnings/fundamental-coverage`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(body),
        });
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        if (alive) setCov((await r.json()) as Coverage);
      } catch (e) {
        if (alive) setErr(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { alive = false; };
  }, [basket, portfolioId]);

  if (err) return <p className="text-xs text-neg-300 py-8 text-center">{err}</p>;
  if (!cov) return <p className="text-xs text-fg-subtle py-8 text-center">Checking coverage…</p>;

  const excluded = cov.rows.filter((r) => r.reason !== 'covered');
  return (
    <div className="space-y-4">
      {/* ⚠ The covered-weight headline + reason bar were removed on request. What remains is the
          EXCLUSIONS table, which is the load-bearing half: a reader can see a chart is blended
          over less than the whole book only if the missing holdings are named. The share of
          weight is still stated there, so nothing that qualifies a blended figure is lost. */}
      {excluded.length > 0 && (
        <div className="space-y-1">
          <h4 className="text-xs font-semibold text-fg-strong">
            Not included ({(100 - cov.covered_pct).toFixed(1)}% of weight)
          </h4>
          <div className="overflow-auto rounded-lg border border-neutral-800/40">
            <table className="w-auto text-xs whitespace-nowrap">
              <thead className="bg-card">
                <tr className="text-fg-faint text-[10px] uppercase tracking-wide border-b border-neutral-800/40">
                  <th className="px-3 py-1.5 font-medium text-left">Instrument</th>
                  <th className="px-3 py-1.5 font-medium text-right">Weight</th>
                  <th className="px-3 py-1.5 font-medium text-left">Why not</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-neutral-800/20">
                {excluded.map((r) => (
                  <tr key={`${r.isin ?? r.name}`} className="hover:bg-overlay/[0.02]">
                    <td className="px-3 py-1.5 text-fg-soft">
                      <span className="inline-block max-w-[28ch] truncate align-bottom"
                        title={r.name ?? ''}>{r.name ?? '—'}</span>
                      {r.isin && <span className="text-fg-faint font-mono text-[10px] ml-2">{r.isin}</span>}
                    </td>
                    <td className="px-3 py-1.5 text-right font-mono text-fg-subtle">
                      {r.weight_pct.toFixed(2)}%
                    </td>
                    <td className={`px-3 py-1.5 ${REASON[r.reason]?.tone ?? 'text-fg-muted'}`}
                      title={REASON[r.reason]?.note}>
                      {REASON[r.reason]?.label ?? r.reason}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
