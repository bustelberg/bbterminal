'use client';

/**
 * EFFECTIVE POSITIONS — the Risk panel's seventh view. `Eᵢ = qᵢ · Pᵢ · Xᵢ`.
 *
 * ⚠⚠ WE DO NOT COMPUTE THAT PRODUCT, AND THE PANEL SAYS SO. `airs_holding` carries a quantity, but
 * it also carries `current_value_eur` — AIRS's own valuation, already in euros, already struck on
 * its own date. That is the number on the client's statement. Re-deriving it from our close and our
 * FX rate would produce a second figure disagreeing with the statement on most rows, and nothing on
 * screen could say which was right. `Eᵢ` here IS that valuation, folded per issuer.
 *
 * ⚠⚠ THE WEIGHTS ARE THE SAME ONES ACTIVE SHARE AND CONCENTRATION USE — `build_issuer_weights`,
 * built once and read by all three. That is the whole architectural point of this view: three
 * panels showing three sets of weights for one portfolio would make every number on all of them
 * unfalsifiable.
 *
 * ⚠ THE CURRENCY IS THE LISTING'S — the exposure actually borne. Hold Nestlé on SIX in CHF and the
 * euro value moves with CHF/EUR; that is a fact about the position. "Nestlé earns worldwide so the
 * real exposure is diversified" is true and is a different, softer claim that cannot be measured
 * from here.
 */
import { useEffect, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { chartTheme } from '../../../lib/chartTheme';
import { AspectCard } from '../../../lib/tipCard';
import InfoTip from '../InfoTip';
import { v } from '../../../lib/dynamicValue';
import { traceError } from '../../../lib/debugTrace';
import type { PortfolioExposure } from '../../../lib/types/api';
import type { ActiveShareHolding } from './ActiveSharePanel';

/** ⚠ TWO DECIMALS ON EVERY NON-INTEGER, ACROSS ALL SEVEN VIEWS. One decimal read as false
 *  precision on a figure the reader is asked to check against a table that carries two: "79.5%"
 *  beside rows summing to 79.53 invites the arithmetic to be redone and found wrong. Counts
 *  (issuers, observations, lines, periods) stay integers — they ARE integers. */
const pct2 = (n: number | null | undefined) => (n == null ? '—' : `${n.toFixed(2)}%`);
/** ⚠ NO DECIMALS ON A POSITION VALUE. Cents on a six-figure holding are noise that costs the
 *  reader the digits that matter, and AIRS's own valuation is not precise to the cent anyway. */
const eur0 = (n: number | null | undefined) =>
  (n == null ? '—' : `€${Math.round(n).toLocaleString('nl-NL')}`);

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

export default function ExposureView({ holdings, benchmark }: {
  holdings: ActiveShareHolding[];
  benchmark: string;
}) {
  const [data, setData] = useState<PortfolioExposure | null>(null);
  const [error, setError] = useState<string | null>(null);

  const key = `${benchmark}|${holdings.length}`
    + `|${holdings.reduce((s, h) => s + h.weight_pct, 0).toFixed(4)}`;

  useEffect(() => {
    let cancelled = false;
    setData(null);
    void (async () => {
      try {
        const r = await apiFetch(
          `${API_URL}/api/airs/portfolio/exposure?benchmark=${encodeURIComponent(benchmark)}`,
          { method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ holdings }) });
        const b = await r.json().catch(() => null);
        if (cancelled) return;
        if (!r.ok) { setError(b?.detail ?? `HTTP ${r.status}`); return; }
        setError(null);
        setData(b as PortfolioExposure);
      } catch (e) {
        traceError('exposure', 'the exposure could not be computed', e);
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { cancelled = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key]);

  const rows = data?.positions ?? [];
  const ccys = data?.currencies ?? [];
  const widest = Math.max(1, ...ccys.map((c) => c.weight_pct));

  return (
    <div className="space-y-3">
      {error && <p className="text-xs text-neg-300">{error}</p>}
      {!data && !error && <p className="text-xs text-fg-subtle">Computing exposure…</p>}
      {data && !data.available && <p className="text-xs text-fg-muted">{data.reason}</p>}

      {data?.available && (
        <>
          <div className="flex flex-wrap gap-2">
            <Tile label="Stock sleeve" value={data.has_values ? eur0(data.sleeve_eur) : pct2(100)}
              sub={data.has_values ? `of ${eur0(data.book_eur)} in the book` : 'weights only'}
              info={<InfoTip className="ml-0.5" content={<AspectCard
                what="The euros in individual stocks — the sleeve every view in this panel measures."
                where={'AIRS\'s own `current_value_eur` per holding, summed. ⚠ NOT a q·P·X of ours: '
                  + 'AIRS values the book, and that is the number on the client\'s statement.'}
                how={'⚠ TRADE DATE vs SETTLEMENT DATE IS AIRS\'S CONVENTION and it exposes no flag '
                  + 'saying which it used, so a book with a very recent trade may differ from a '
                  + 'trade-date view by that trade\'s value. Stated rather than assumed away.'} />} />} />
            <Tile label="Companies" value={`${data.issuers}`}
              sub={(data.folded_lines ?? 0) > 0
                ? `${data.lines} lines, ${data.folded_lines} folded` : `${data.lines} lines`}
              info={<InfoTip className="ml-0.5" content={<AspectCard
                what="Distinct companies held, after folding share classes and dual listings."
                where={`${v(data.lines)} holdings on the table → ${v(data.issuers)} companies here.`}
                how={'⚠ THE ONE-LINE ANSWER TO "WHY DOES THIS COUNT DIFFERENTLY FROM THE HOLDINGS '
                  + 'TABLE". Alphabet A + Alphabet C is one position. The same fold feeds Active '
                  + 'share and Concentration, so all three agree by construction.'} />} />} />
            <Tile label="Currencies" value={`${ccys.length}`}
              sub={ccys[0] ? `${ccys[0].currency} ${pct2(ccys[0].weight_pct)} largest` : undefined}
              info={<InfoTip className="ml-0.5" content={<AspectCard
                what="How many currencies the sleeve's value actually sits in."
                where="The LISTING's currency, from the holding or our grid mapping of the ISIN."
                how={'⚠ THE EXPOSURE YOU BEAR, not the one the company earns in. Nestlé on SIX is '
                  + 'CHF exposure whatever its revenue mix — that is a fact about the position. '
                  + 'The economic argument is true and is a different, softer claim.'} />} />} />
            <Tile label="Other" value={data.has_values ? eur0(data.other_eur) : '—'}
              tone="text-fg-muted"
              sub={`funds, cash, bonds — ${pct2(100 - (data.stocks_pct ?? 0))} of the book`}
              info={<InfoTip className="ml-0.5" content={<AspectCard
                what="Everything outside the stock sleeve."
                where="Funds, cash, bonds, and any line without a usable ISIN."
                how={'⚠ EVERY OTHER VIEW IN THIS PANEL EXCLUDES THIS AND RENORMALISES. The figure '
                  + 'is here so the renormalisation is never invisible.'} />} />} />
          </div>

          {(data.currency_unknown_pct ?? 0) > 0.05 && (
            <p className="text-[11px] text-warn-300">
              {`${pct2(data.currency_unknown_pct)} of the sleeve has no currency we could assign. `}
              It is reported separately rather than folded into EUR — that default would make the
              book look more domestic than it is.
            </p>
          )}

          <div>
            <div className="text-[10px] uppercase tracking-wider text-fg-faint mb-1">
              Currency exposure
            </div>
            <table className="w-full text-[11px]">
              <tbody>
                {ccys.map((c) => (
                  <tr key={c.currency} className="[&>td]:py-1">
                    <td className="text-fg-soft w-12 font-mono">{c.currency}</td>
                    <td className="text-right font-mono tabular-nums text-fg w-16">
                      {pct2(c.weight_pct)}
                    </td>
                    <td className="text-right font-mono tabular-nums text-fg-muted w-28">
                      {data.has_values ? eur0(c.value_eur) : ''}
                    </td>
                    <td className="text-fg-faint w-20 text-right pr-2">
                      {c.issuers} {c.issuers === 1 ? 'company' : 'companies'}
                    </td>
                    <td>
                      <span className="block h-2 rounded-sm" style={{
                        width: `${(c.weight_pct / widest) * 100}%`,
                        background: chartTheme.accent,
                      }} />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div>
            <div className="text-[10px] uppercase tracking-wider text-fg-faint mb-1">
              Effective position per company
            </div>
            <div className="overflow-auto">
              <table className="w-full text-[11px]">
                <thead>
                  <tr className="text-fg-faint [&>th]:py-1 [&>th]:font-medium">
                    <th className="text-left">Company</th>
                    <th className="text-right">Weight</th>
                    <th className="text-right">Value</th>
                    <th className="text-left pl-3">Currency</th>
                  </tr>
                </thead>
                <tbody>
                  {rows.map((r) => (
                    <tr key={r.name}
                      className="[&>td]:py-1 [&>td]:border-t [&>td]:border-neutral-800/20">
                      <td className="text-fg-soft truncate max-w-[18rem]">
                        {r.name}
                        {/* ⚠ NAMED, because the fold is the thing that makes this count differ. */}
                        {(r.lines ?? 1) > 1 && (
                          <span className="ml-1.5 text-[10px] text-fg-faint">
                            {r.lines} lines
                          </span>
                        )}
                      </td>
                      <td className="text-right font-mono tabular-nums text-fg">
                        {pct2(r.weight_pct)}
                      </td>
                      <td className="text-right font-mono tabular-nums text-fg-muted">
                        {data.has_values ? eur0(r.value_eur) : '—'}
                      </td>
                      {/* ⚠⚠ ONE ISSUER, TWO CURRENCIES IS A REAL STATE and the fold would hide it:
                          a dual-listed company is a single position and two FX exposures. */}
                      <td className={`pl-3 font-mono ${
                        (r.currencies ?? []).length > 1 ? 'text-warn-300' : 'text-fg-faint'}`}>
                        {(r.currencies ?? []).join(' + ') || '—'}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          <p className="text-[11px] text-fg-faint leading-relaxed">
            Eᵢ is AIRS&apos;s own EUR valuation of the position, not a quantity × price × FX of
            ours — it is the figure on the client&apos;s statement, and a second derivation would
            disagree with it on most rows with no way to say which was right. The weights here are
            the same ones Active share and Concentration read, folded once.
          </p>
        </>
      )}
    </div>
  );
}
