'use client';

import { useState } from 'react';
import CompanyPicker, { type AssetPick } from './CompanyPicker';
import OwnerEarningsModal from '../portfolios/OwnerEarningsModal';

/**
 * Two companies in ONE Fundamental view — both drawn on the same chart, on every chart.
 *
 * ⚠⚠ IT IS THE DIALOG FROM /management-dashboard, WITH COMPANY B IN THE BENCHMARK SLOT. Every Long
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
 * ⚠ SIDE-BY-SIDE PANELS CAME FIRST AND WERE WORSE. Two independent dialogs meant two y-domains, two
 * legends and two scroll positions; reading a 3pp margin gap off two charts a screen apart is
 * eyeballing, not comparing. One chart with both lines answers it directly.
 *
 * ⚠ THE COST: ONE COMPARISON LINE PER CHART. Choosing company B means not showing the index on
 * that chart. The tab's own selector still offers ACWI/SP500/AEX, so B can be swapped back out for
 * a market without leaving the page.
 */
export default function ResearchDashboard() {
  const [a, setA] = useState<AssetPick | null>(null);
  const [b, setB] = useState<AssetPick | null>(null);

  return (
    <div className="p-6 space-y-4 min-w-0">
      <div>
        <h1 className="text-lg font-semibold text-fg-strong">Research Dashboard</h1>
        <p className="text-sm text-fg-subtle mt-0.5">
          Two companies from the asset pipeline, on the same charts. This is the Fundamental view
          the Management Dashboard opens — the second company takes the benchmark line, so every
          chart compares them directly rather than side by side.
        </p>
      </div>

      <div className="grid gap-4 md:grid-cols-2 max-w-3xl">
        <CompanyPicker label="Company A — the subject" value={a} onPick={setA} />
        <CompanyPicker label="Company B — drawn beside it" value={b} onPick={setB} />
      </div>

      {a ? (
        /* ⚠ KEYED ON BOTH ISINs so changing either company REMOUNTS the tab. Its cadence, its
           selected comparison and every card's fetch are per-pair state; carried across a switch
           they would describe the previous pair under the new names. */
        <OwnerEarningsModal
          key={`${a.isin}-${b?.isin ?? 'none'}`}
          embedded
          isin={a.isin}
          name={a.name ?? a.isin}
          compare={b ? { isin: b.isin, name: b.name ?? b.isin } : null}
          /* Closing an embedded card means "clear the subject" — there is no dialog to dismiss,
             and a ✕ that did nothing would be worse than no ✕. */
          onClose={() => setA(null)}
        />
      ) : (
        <div className="bg-card border border-neutral-800/40 rounded-xl p-4">
          <p className="py-16 text-center text-xs text-fg-faint">
            Pick company A to open the Fundamental view. Company B is optional — without it this is
            the ordinary single-company view, measured against an index.
          </p>
        </div>
      )}
    </div>
  );
}
