'use client';

/**
 * CONCENTRATION — the Risk panel's sixth view.
 *
 *     C₁₀ = Σᵢ₌₁¹⁰ w₍ᵢ₎        HHI = Σ wᵢ²        N_eff = 1 / HHI
 *
 * ⚠⚠ ON COMPANIES, NOT ON LINES — the same folding Active share uses. Alphabet A + Alphabet C is
 * ONE position, and counting two would understate concentration exactly at the top, where the ten
 * largest are decided. Two views of the same book disagreeing about how many positions it holds
 * would be worse than either. (The code says `issuer` and still should — see the ⚠ in
 * `ActiveSharePanel` for why the screen says company and the fold does not.)
 *
 * ⚠⚠ BOTH DENOMINATORS ARE ON SCREEN. "Of the stock sleeve" is what compares across books and is
 * the panel's convention everywhere else; "of the whole book" is what is true in absolute terms for
 * a book carrying 30% cash. The choice genuinely changes the number, so making it silently would be
 * picking a side of a real question on the reader's behalf.
 *
 * ⚠ HHI IS THE BETTER MEASURE AND C₁₀ IS THE ONE PEOPLE ASK FOR, so both are here with N_eff given
 * the most prominent tile. A cut at exactly ten is arbitrary: two books with identical C₁₀ can be an
 * even ten-name portfolio and one dominated by its top three. N_eff has no cut-off and reads in
 * units anybody can hold in their head.
 */
import { useEffect, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { chartTheme } from '../../../lib/chartTheme';
import { AspectCard } from '../../../lib/tipCard';
import InfoTip from '../InfoTip';
import { v } from '../../../lib/dynamicValue';
import { traceError } from '../../../lib/debugTrace';
import { withWorked, subNum } from './workedFormula';
import type { PortfolioConcentration } from '../../../lib/types/api';
import type { ActiveShareHolding } from './ActiveSharePanel';

/** ⚠ TWO DECIMALS ON EVERY NON-INTEGER, ACROSS ALL SEVEN VIEWS. One decimal read as false
 *  precision on a figure the reader is asked to check against a table that carries two: "79.5%"
 *  beside rows summing to 79.53 invites the arithmetic to be redone and found wrong. Counts
 *  (issuers, observations, lines, periods) stay integers — they ARE integers. */
const pct2 = (n: number | null | undefined) => (n == null ? '—' : `${n.toFixed(2)}%`);

function Tile({ label, value, sub, tone, info }: {
  label: string; value: string; sub?: string; tone?: string; info?: React.ReactNode;
}) {
  return (
    <div className="rounded-lg border border-neutral-800/40 bg-elevated px-3 py-2 min-w-[8rem]">
      <div className="text-[9px] uppercase tracking-wider text-fg-faint flex items-center gap-1">
        {label}{info}
      </div>
      <div className={`font-mono text-xl tabular-nums ${tone ?? 'text-fg-strong'}`}>{value}</div>
      {sub && <div className="text-[10px] text-fg-faint">{sub}</div>}
    </div>
  );
}

export default function ConcentrationView({ holdings, benchmark }: {
  holdings: ActiveShareHolding[];
  benchmark: string;
}) {
  const [data, setData] = useState<PortfolioConcentration | null>(null);
  const [error, setError] = useState<string | null>(null);

  const key = `${benchmark}|${holdings.length}`
    + `|${holdings.reduce((s, h) => s + h.weight_pct, 0).toFixed(4)}`;

  useEffect(() => {
    let cancelled = false;
    setData(null);
    void (async () => {
      try {
        const r = await apiFetch(
          `${API_URL}/api/airs/portfolio/concentration?benchmark=${encodeURIComponent(benchmark)}`,
          { method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ holdings }) });
        const b = await r.json().catch(() => null);
        if (cancelled) return;
        if (!r.ok) { setError(b?.detail ?? `HTTP ${r.status}`); return; }
        setError(null);
        setData(b as PortfolioConcentration);
      } catch (e) {
        traceError('concentration', 'the concentration could not be computed', e);
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { cancelled = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key]);

  const rows = data?.top ?? [];
  const widest = Math.max(1, ...rows.map((r) => Math.max(r.weight_pct, r.benchmark_pct ?? 0)));

  return (
    <div className="space-y-3">
      {error && <p className="text-xs text-neg-300">{error}</p>}
      {!data && !error && <p className="text-xs text-fg-subtle">Computing concentration…</p>}
      {data && !data.available && <p className="text-xs text-fg-muted">{data.reason}</p>}

      {data?.available && (
        <>
          <div className="flex flex-wrap gap-2">
            <Tile label="Effective positions"
              value={data.effective_positions == null ? '—' : data.effective_positions.toFixed(2)}
              sub={`of ${data.issuers} companies held`}
              info={<InfoTip className="ml-0.5" content={<AspectCard
                what="How many equally-sized positions this book behaves like."
                where={`1 ÷ HHI, over ${v(data.issuers)} companies.`}
                worked={data.hhi == null || data.effective_positions == null ? '' : withWorked(
                  String.raw`HHI = \sum_i w_i^2\qquad N_{\text{eff}} = \dfrac{1}{HHI}`,
                  String.raw`\dfrac{1}{${subNum(data.hhi, 4)}} = ${data.effective_positions.toFixed(2)}`)}
                legend={[
                  { sym: 'w_i', is: 'company i’s weight in the sleeve, as a fraction of 1' },
                  { sym: String.raw`HHI`, is: 'the sum of those weights SQUARED — squaring is what makes a big position count for more than its size' },
                  { sym: String.raw`N_{\text{eff}}`, is: 'the answer: how many EQUALLY-sized positions would concentrate the book this much' },
                ]}
                how={'⚠ THE BETTER NUMBER, and the reason it leads. A cut at exactly ten is '
                  + 'arbitrary — two books with the same C₁₀ can be an even ten-name portfolio and '
                  + 'one dominated by its top three. This has no cut-off. Forty names of which '
                  + 'five dominate reads far below forty.'} />} />} />
            <Tile label="Top 10" value={pct2(data.top10_pct)}
              sub={`${pct2(data.top10_of_book_pct)} of the whole book`}
              info={<InfoTip className="ml-0.5" content={<AspectCard
                what="The share of the stock sleeve in its ten largest companies."
                where={`Σ of the ten biggest of ${v(data.issuers)}, sorted descending.`}
                worked={data.top10_pct == null ? '' : withWorked(
                  String.raw`C_{10} = \sum_{i=1}^{10} w_{(i)}`,
                  String.raw`C_1 = ${subNum(data.top1_pct ?? 0, 2)}\%`
                  + String.raw`\quad C_3 = ${subNum(data.top3_pct ?? 0, 2)}\%`
                  + String.raw`\quad C_5 = ${subNum(data.top5_pct ?? 0, 2)}\%`
                  + String.raw`\quad C_{10} = ${subNum(data.top10_pct, 2)}\%`
                  + String.raw`\quad C_{20} = ${subNum(data.top20_pct ?? 0, 2)}\%`)}
                legend={[
                  { sym: String.raw`w_{(i)}`, is: 'the weights sorted largest first — the bracket is what makes (i) a RANK rather than a name' },
                  { sym: String.raw`C_{10}`, is: 'the answer: how much of the sleeve sits in its ten largest companies' },
                ]}
                how={'⚠⚠ TWO DENOMINATORS, BOTH TRUE. The headline is of the STOCK SLEEVE, which is '
                  + 'what compares across books; the line beneath is of the whole book including '
                  + `cash and funds (the sleeve is ${pct2(data.stocks_pct)} of it). A book that is `
                  + '30% cash really is less concentrated in absolute terms.'} />} />} />
            <Tile label="Largest position" value={pct2(data.top1_pct)} tone="text-fg-strong"
              sub={rows[0]?.name}
              info={<InfoTip className="ml-0.5" content={<AspectCard
                what="The single biggest company, as a share of the sleeve."
                where={rows[0] ? `${rows[0].name} — the index holds ${pct2(rows[0].benchmark_pct)}.` : undefined}
                how={'⚠ A BIG POSITION IS NOT AUTOMATICALLY A BIG BET. Apple at 6% against an index '
                  + 'holding 5% is a 1pp bet; the same 6% in a name the index does not hold is a '
                  + '6pp one. The table below carries both.'} />} />} />
            <Tile label={`${data.benchmark} effective`}
              value={data.benchmark_effective_positions == null ? '—'
                : data.benchmark_effective_positions.toFixed(2)}
              tone="text-fg-muted"
              sub={`of ${data.benchmark_issuers} · top 10 ${pct2(data.benchmark_top10_pct)}`}
              info={<InfoTip className="ml-0.5" content={<AspectCard
                what="The index's own effective position count, on the same measure."
                where={`1 ÷ HHI over ${v(data.benchmark_issuers)} priced constituents.`}
                how={'For scale. ⚠ A cap-weighted index is far more concentrated than its member '
                  + 'count suggests, so this is usually a small fraction of it — which is the '
                  + 'honest comparison, not the raw count.'} />} />} />
          </div>

          {data.benchmark_covered_pct != null && data.benchmark_covered_pct < 99.5 && (
            <p className="text-[11px] text-fg-faint">
              {`Priced ${data.benchmark_covered_pct.toFixed(2)}% of ${data.benchmark}'s members — `}
              the missing weight redistributes over the rest, so the index reads slightly more
              concentrated than it is.
            </p>
          )}

          <div>
            <div className="text-[10px] uppercase tracking-wider text-fg-faint mb-1">
              Largest companies, with the index&apos;s weight in each
            </div>
            {/* ⚠ ITS OWN SCROLL — twenty rows must not stretch the fixed dialog. */}
            <div className="overflow-auto">
              <table className="w-full text-[11px]">
                <thead>
                  <tr className="text-fg-faint [&>th]:py-1 [&>th]:font-medium">
                    <th className="text-left w-6">#</th>
                    <th className="text-left">Company</th>
                    <th className="text-right">Weight</th>
                    <th className="text-right">Cumulative</th>
                    <th className="text-right">{data.benchmark}</th>
                    <th className="w-28" />
                  </tr>
                </thead>
                <tbody>
                  {rows.map((r) => (
                    <tr key={r.name}
                      className="[&>td]:py-1 [&>td]:border-t [&>td]:border-neutral-800/20">
                      <td className="text-fg-faint tabular-nums">{r.rank}</td>
                      <td className="text-fg-soft truncate max-w-[16rem]">{r.name}</td>
                      <td className="text-right font-mono tabular-nums text-fg">
                        {pct2(r.weight_pct)}
                      </td>
                      <td className="text-right font-mono tabular-nums text-fg-muted">
                        {pct2(r.cumulative_pct)}
                      </td>
                      <td className="text-right font-mono tabular-nums text-fg-muted">
                        {(r.benchmark_pct ?? 0) > 0 ? pct2(r.benchmark_pct) : '—'}
                      </td>
                      {/* ⚠ TWO BARS ON ONE SCALE, book over index — so "big position" and "big bet"
                          are visually different things rather than the same one. */}
                      <td className="pr-1">
                        <span className="block h-1.5 rounded-sm" style={{
                          width: `${(r.weight_pct / widest) * 100}%`,
                          background: chartTheme.accent,
                        }} />
                        <span className="block h-1 rounded-sm mt-0.5" style={{
                          width: `${((r.benchmark_pct ?? 0) / widest) * 100}%`,
                          background: chartTheme.pos,
                        }} />
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          <p className="text-[11px] text-fg-faint leading-relaxed">
            Folded onto ISSUERS, not lines — two share classes of one company are a single position,
            which is what stops the ten largest being decided by an identifier.
            {(data.unresolved ?? 0) > 0
              && ` ${data.unresolved} holding${data.unresolved === 1 ? '' : 's'} could not be `
                + 'matched to a company name and each counts as its own company.'}
          </p>
        </>
      )}
    </div>
  );
}
