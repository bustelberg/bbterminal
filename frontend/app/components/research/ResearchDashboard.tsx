'use client';

import { useCallback, useMemo, useRef, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { startJob } from '../../../lib/stores/jobs';
import CompanyPicker, { type AssetPick } from './CompanyPicker';
import OwnerEarningsModal from '../portfolios/OwnerEarningsModal';
import { BLEND_METRICS } from '../portfolios/LongEquityTab';

/**
 * Two companies in ONE Fundamental view — both drawn on the same chart, on every chart.
 *
 *  It is the dialog from /management-dashboard, WITH COMPANY B IN THE BENCHMARK SLOT. Every Long
 * Equity card already draws a second line on a shared y-domain — same axis, same legend, same hover
 * order, same coverage floor — computed by running the card's OWN helper (`marginByYear`,
 * `debtRatioByYear`, …) over a second row set. A company is a one-holding book to those same
 * endpoints, so supplying B as that second series turns all fourteen charts into comparisons with
 * no new chart code, no new blend rule and no new endpoint.
 *
 * That is also what makes it trustworthy: the two lines cannot be computed differently, because
 * there is only one computation. A bespoke "compare" pipeline would be a second definition of every
 * ratio on the tab, and the first divergence would be invisible — two lines on one axis look
 * comparable whether or not they are.
 *
 *  Side-by-side panels came first and were worse. Two independent dialogs meant two y-domains, two
 * legends and two scroll positions; reading a 3pp margin gap off two charts a screen apart is
 * eyeballing, not comparing. One chart with both lines answers it directly.
 *
 *  The cost: one comparison line per chart. Choosing company B means not showing the index on
 * that chart. The tab's own selector still offers ACWI/SP500/AEX, so B can be swapped back out for
 * a market without leaving the page.
 */
export default function ResearchDashboard() {
  const [a, setA] = useState<AssetPick | null>(null);
  const [b, setB] = useState<AssetPick | null>(null);
  const comparison = useMemo(() => b ? { isin: b.isin, name: b.name ?? b.isin } : null, [b]);
  const [preparing, setPreparing] = useState<string | null>(null);
  const pending = useRef(new Map<string, Promise<void>>());
  const selectionVersion = useRef({ primary: 0, compare: 0 });

  const ensureFundamentals = useCallback((company: AssetPick): Promise<void> => {
    const alreadyPending = pending.current.get(company.isin);
    if (alreadyPending) return alreadyPending;

    const run = (async () => {
      try {
        const metrics = await apiFetch(
          `${API_URL}/api/earnings/by-isin/${encodeURIComponent(company.isin)}`
            + `/metrics?cadence=annual&metric_keys=${encodeURIComponent(BLEND_METRICS.join(','))}`,
        );
        let missing = metrics.status === 404;
        if (metrics.ok) {
          const payload = await metrics.json().catch(() => null) as {
            metrics?: { metric_code?: string }[];
          } | null;
          missing = !(payload?.metrics ?? []).some((row) => {
            const code = row.metric_code ?? '';
            return code === 'annuals__Cashflow Statement__Free Cash Flow'
              || code === 'annuals__cashflow_statement__Free Cash Flow'
              || code === 'annuals__Per Share Data__Free Cash Flow per Share'
              || code === 'annuals__per_share_data__Free Cash Flow per Share'
              || code === 'annuals__per_share_data_array__Free Cash Flow per Share'
              || code === 'annuals__Per Share Data__EPS without NRI'
              || code === 'annuals__per_share_data__EPS without NRI'
              || code === 'annuals__per_share_data_array__EPS without NRI'
              || code === 'annuals__Income Statement__Revenue'
              || code === 'annuals__income_statement__Revenue';
          });
        }
        if (!missing) return;

        const name = company.name ?? company.isin;
        const { done } = await startJob(
          `${API_URL}/api/airs/basket/fundamentals/ingest/job`
            + '?force=true&only_due=true&feeds=all&prices=true',
          `${name} fundamentals`,
          {
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              holdings: [{ isin: company.isin }],
              label: name,
            }),
          },
        );
        await done;
      } catch {
        // The selected company remains usable even if its optional fill cannot start or fails.
      } finally {
        pending.current.delete(company.isin);
      }
    })();
    pending.current.set(company.isin, run);
    return run;
  }, []);

  const selectPrimary = async (company: AssetPick | null) => {
    const version = ++selectionVersion.current.primary;
    if (!company) {
      setA(null);
      return;
    }
    const name = company.name ?? company.isin;
    setPreparing(name);
    await ensureFundamentals(company);
    if (version !== selectionVersion.current.primary) return;
    setA(company);
    setPreparing(null);
  };

  const selectCompare = async (company: AssetPick | null) => {
    const version = ++selectionVersion.current.compare;
    if (!company) {
      setB(null);
      return;
    }
    const name = company.name ?? company.isin;
    setPreparing(name);
    await ensureFundamentals(company);
    if (version !== selectionVersion.current.compare) return;
    setB(company);
    setPreparing(null);
  };

  if (!a) {
    return (
      <div className="p-6 min-w-0">
        <div className="mx-auto max-w-md space-y-4 pt-10">
          <h1 className="text-lg font-semibold text-center text-fg-strong">Research Dashboard</h1>
          <CompanyPicker label="Company" value={null} onPick={selectPrimary} />
          {preparing && (
            <p className="text-center text-xs text-fg-subtle">Preparing {preparing}…</p>
          )}
        </div>
      </div>
    );
  }

  return (
    <div className="p-6 space-y-4 min-w-0">
      <h1 className="text-lg font-semibold text-fg-strong">Research Dashboard</h1>

      <div className="grid gap-4 md:grid-cols-2 max-w-3xl mx-auto">
        <CompanyPicker label="Company" value={a} onPick={selectPrimary} />
        <CompanyPicker label="Compare with (optional)" value={b} onPick={selectCompare} />
      </div>

      {preparing && <p className="text-center text-xs text-fg-subtle">Preparing {preparing}…</p>}
      <OwnerEarningsModal
        embedded
        isin={a.isin}
        name={a.name ?? a.isin}
        compare={comparison}
        onClose={() => {
          selectionVersion.current.primary++;
          selectionVersion.current.compare++;
          setA(null); setB(null); setPreparing(null);
        }}
      />
    </div>
  );
}
