'use client';

/**
 * CONCENTRATION — the Risk panel's sixth view.
 *
 *     C₁₀ = Σᵢ₌₁¹⁰ w₍ᵢ₎        HHI = Σ wᵢ²        N_eff = 1 / HHI
 *
 * ⚠⚠ ON ISSUERS, NOT ON LINES — the same folding Active share uses. Alphabet A + Alphabet C is ONE
 * position, and counting two would understate concentration exactly at the top, where the ten
 * largest are decided. Two views of the same book disagreeing about how many positions it holds
 * would be worse than either.
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
import { traceError } from '../../../lib/debugTrace';
import { withWorked, subNum } from './workedFormula';
import type { PortfolioConcentration } from '../../../lib/types/api';
import type { ActiveShareHolding } from './ActiveSharePanel';

const pct1 = (v: number | null | undefined) => (v == null ? '—' : `${v.toFixed(1)}%`);
const pct2 = (v: number | null | undefined) => (v == null ? '—' : `${v.toFixed(2)}%`);

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
              value={data.effective_positions == null ? '—' : data.effective_positions.toFixed(1)}
              sub={`of ${data.issuers} issuers held`}
              info={<InfoTip className="ml-0.5" content={<AspectCard
                what="How many equally-sized positions this book behaves like."
                where={`1 ÷ HHI, over ${data.issuers} issuers.`}
                worked={data.hhi == null || data.effective_positions == null ? '' : withWorked(
                  'HHI = Σ wᵢ²  (weights as fractions),  N_eff = 1 ÷ HHI',
                  `1 ÷ ${subNum(data.hhi, 4)} = ${data.effective_positions.toFixed(1)}`)}
                how={'⚠ THE BETTER NUMBER, and the reason it leads. A cut at exactly ten is '
                  + 'arbitrary — two books with the same C₁₀ can be an even ten-name portfolio and '
                  + 'one dominated by its top three. This has no cut-off. Forty names of which '
                  + 'five dominate reads far below forty.'} />} />} />
            <Tile label="Top 10" value={pct1(data.top10_pct)}
              sub={`${pct1(data.top10_of_book_pct)} of the whole book`}
              info={<InfoTip className="ml-0.5" content={<AspectCard
                what="The share of the stock sleeve in its ten largest issuers."
                where={`Σ of the ten biggest of ${data.issuers}, sorted descending.`}
                worked={data.top10_pct == null ? '' : withWorked(
                  'C₁₀ = Σᵢ₌₁¹⁰ w₍ᵢ₎',
                  `C₁ ${subNum(data.top1_pct ?? 0, 1)}%`
                  + ` · C₃ ${subNum(data.top3_pct ?? 0, 1)}%`
                  + ` · C₅ ${subNum(data.top5_pct ?? 0, 1)}%`
                  + ` · C₁₀ ${subNum(data.top10_pct, 1)}%`
                  + ` · C₂₀ ${subNum(data.top20_pct ?? 0, 1)}%`)}
                how={'⚠⚠ TWO DENOMINATORS, BOTH TRUE. The headline is of the STOCK SLEEVE, which is '
                  + 'what compares across books; the line beneath is of the whole book including '
                  + `cash and funds (the sleeve is ${pct1(data.stocks_pct)} of it). A book that is `
                  + '30% cash really is less concentrated in absolute terms.'} />} />} />
            <Tile label="Largest position" value={pct1(data.top1_pct)} tone="text-fg-strong"
              sub={rows[0]?.name}
              info={<InfoTip className="ml-0.5" content={<AspectCard
                what="The single biggest issuer, as a share of the sleeve."
                where={rows[0] ? `${rows[0].name} — the index holds ${pct2(rows[0].benchmark_pct)}.` : undefined}
                how={'⚠ A BIG POSITION IS NOT AUTOMATICALLY A BIG BET. Apple at 6% against an index '
                  + 'holding 5% is a 1pp bet; the same 6% in a name the index does not hold is a '
                  + '6pp one. The table below carries both.'} />} />} />
            <Tile label={`${data.benchmark} effective`}
              value={data.benchmark_effective_positions == null ? '—'
                : data.benchmark_effective_positions.toFixed(0)}
              tone="text-fg-muted"
              sub={`of ${data.benchmark_issuers} · top 10 ${pct1(data.benchmark_top10_pct)}`}
              info={<InfoTip className="ml-0.5" content={<AspectCard
                what="The index's own effective position count, on the same measure."
                where={`1 ÷ HHI over ${data.benchmark_issuers} priced constituents.`}
                how={'For scale. ⚠ A cap-weighted index is far more concentrated than its member '
                  + 'count suggests, so this is usually a small fraction of it — which is the '
                  + 'honest comparison, not the raw count.'} />} />} />
          </div>

          {data.benchmark_covered_pct != null && data.benchmark_covered_pct < 99.5 && (
            <p className="text-[11px] text-fg-faint">
              {`Priced ${data.benchmark_covered_pct.toFixed(1)}% of ${data.benchmark}'s members — `}
              the missing weight redistributes over the rest, so the index reads slightly more
              concentrated than it is.
            </p>
          )}

          <div>
            <div className="text-[10px] uppercase tracking-wider text-fg-faint mb-1">
              Largest issuers, with the index&apos;s weight in each
            </div>
            {/* ⚠ ITS OWN SCROLL — twenty rows must not stretch the fixed dialog. */}
            <div className="overflow-auto">
              <table className="w-full text-[11px]">
                <thead>
                  <tr className="text-fg-faint [&>th]:py-1 [&>th]:font-medium">
                    <th className="text-left w-6">#</th>
                    <th className="text-left">Issuer</th>
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
                        {pct1(r.cumulative_pct)}
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
                + 'matched to a company name and each counts as its own issuer.'}
          </p>
        </>
      )}
    </div>
  );
}
