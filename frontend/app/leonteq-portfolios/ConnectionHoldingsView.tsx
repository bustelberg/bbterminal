'use client';

import { Fragment, useEffect, useRef, useState } from 'react';
import { apiFetch } from '../../lib/apiFetch';
import { API_URL } from '../../lib/apiUrl';
import { useLang } from '../../lib/i18n';
import type { CompositionResult } from './CompositionTable';
import type { AirsComposition } from './AirsCompositionView';
import { holdingKey, matchHoldings, soldRuleKey, type Holding, type HoldingRule } from './matchHoldings';
import SearchablePortfolioPicker from './SearchablePortfolioPicker';
import { refreshRequest } from './refreshProgress';

export type PortfolioConnection = { leonteq_name: string; airs_portfolio_id: number; airs_name: string | null };
type Side = 'leonteq' | 'airs';
type Snapshot = {
  rows: Holding[];
  datum?: string | null; timestamp?: string; cached?: boolean; warning?: boolean; noFixed?: boolean;
};
const sides: Side[] = ['leonteq', 'airs'];
const formatWeight = (value: string | null | undefined) => {
  if (value == null || !value.trim()) return '—';
  const number = Number(value.trim().replace(/%$/, '').trim().replace(',', '.'));
  return Number.isFinite(number) ? `${number.toFixed(2)}%` : '—';
};
const formatDate = (value: string, withTime = false) => {
  const date = new Date(value.length === 10 ? `${value}T12:00:00` : value);
  const parts = new Intl.DateTimeFormat('nl-NL', {
    timeZone: 'Europe/Amsterdam', day: 'numeric', month: 'short', year: 'numeric',
    ...(withTime ? { hour: '2-digit', minute: '2-digit', hourCycle: 'h23' as const } : {}),
  }).formatToParts(date);
  const part = (type: Intl.DateTimeFormatPartTypes) => parts.find((item) => item.type === type)?.value ?? '';
  return `${part('day')} ${part('month').replace(/\.$/, '')} ${part('year')}${withTime ? ` ${part('hour')}:${part('minute')}` : ''}`;
};
// Old AIRS snapshots include accrued-interest lines. Never reuse those after this upgrade.
export const connectionSnapshotKey = (link: PortfolioConnection, side: Side) => side === 'leonteq'
  ? `leonteq:${link.leonteq_name}` : `airs:security-v2:${link.airs_portfolio_id}`;
const keyFor = connectionSnapshotKey;

export async function readConnectionHoldings(link: PortfolioConnection, side: Side, refresh: boolean, nl = false, savedOnly = false): Promise<Snapshot> {
  const fetcher = refresh ? (url: string, init: Parameters<typeof apiFetch>[1]) => refreshRequest(url, init,
    `${side === 'leonteq' ? 'Leonteq' : 'AIRS'} · ${side === 'leonteq' ? link.leonteq_name : link.airs_name ?? link.airs_portfolio_id}`,
    side === 'leonteq' ? link.leonteq_name : link.airs_name ?? undefined, nl) : apiFetch;
  if (side === 'leonteq') {
    const response = await fetcher(`${API_URL}/api/leonteq/portfolios/composition`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: link.leonteq_name, refresh, ...(savedOnly ? { saved_only: true } : {}) }),
    });
    if (!response.ok) throw new Error('failed');
    const data = await response.json() as CompositionResult;
    if (!data.success || !data.groups) throw new Error('failed');
    return { rows: data.groups.flatMap((group) => group.positions.map((position) => ({
      isin: position.isin, name: position.name, weight: position.weight,
    }))), timestamp: data.retrieved_at, cached: data.cached, warning: Boolean(data.refresh_error) };
  }
  const response = await fetcher(`${API_URL}/api/leonteq/airs/model-portfolios/${link.airs_portfolio_id}/composition${refresh ? '?refresh=true' : savedOnly ? '?saved_only=true' : ''}`, { cache: 'no-store', noReadCache: true });
  if (!response.ok) throw new Error('failed');
  const data = await response.json() as AirsComposition;
  return { rows: data.rows.map((position) => ({ isin: position.ISINCode, name: position.Fonds,
    weight: position.Percentage == null ? null : `${position.Percentage}%`,
  })), datum: data.datum, timestamp: data.cached_at ?? data.retrieved_at,
    cached: Boolean(data.cached_at), noFixed: data.no_fixed_model };
}

const read = readConnectionHoldings;

export default function ConnectionHoldingsView({ link, cache, rulesCache, onBack }: {
  link: PortfolioConnection; cache: Map<string, Snapshot>; rulesCache: Map<string, HoldingRule[]>; onBack: () => void;
}) {
  const [lang] = useLang();
  const nl = lang === 'nl';
  const [data, setData] = useState<Partial<Record<Side, Snapshot>>>(() => ({
    leonteq: cache.get(keyFor(link, 'leonteq')), airs: cache.get(keyFor(link, 'airs')),
  }));
  const [loading, setLoading] = useState<Record<Side, boolean>>(() => ({
    leonteq: !cache.has(keyFor(link, 'leonteq')), airs: !cache.has(keyFor(link, 'airs')),
  }));
  const [errors, setErrors] = useState<Partial<Record<Side, boolean>>>({});
  const version = useRef(0);
  const alive = useRef(true);
  const rulesKey = JSON.stringify([link.leonteq_name, link.airs_portfolio_id]);
  const [rules, setRules] = useState<HoldingRule[] | null>(() => rulesCache.get(rulesKey) ?? null);
  const [rulesLoading, setRulesLoading] = useState(() => !rulesCache.has(rulesKey));
  const [rulesSaving, setRulesSaving] = useState(false);
  const [rulesError, setRulesError] = useState(false);
  const [leftChoice, setLeftChoice] = useState('');
  const [rightChoice, setRightChoice] = useState('');
  const savingRule = useRef(false);
  const rulesUrl = `${API_URL}/api/leonteq/portfolio-links/holdings`;
  useEffect(() => {
    let active = true;
    if (!rulesCache.has(rulesKey)) {
      const params = new URLSearchParams({ leonteq_name: link.leonteq_name, airs_portfolio_id: String(link.airs_portfolio_id) });
      void apiFetch(`${API_URL}/api/leonteq/portfolio-links/holdings?${params}`, { cache: 'no-store', noReadCache: true })
        .then(async (response) => { if (!response.ok) throw new Error('failed'); return await response.json() as { rules: HoldingRule[] }; })
        .then((result) => { if (active) { rulesCache.set(rulesKey, result.rules); setRules(result.rules); } })
        .catch(() => { if (active) setRulesError(true); })
        .finally(() => { if (active) setRulesLoading(false); });
    }
    return () => { active = false; };
  }, [link.leonteq_name, link.airs_portfolio_id, rulesCache, rulesKey]);

  async function saveRule(left: string, right: string | null) {
    if (savingRule.current) return;
    savingRule.current = true; setRulesSaving(true); setRulesError(false);
    try {
      const response = await apiFetch(rulesUrl, { method: 'PUT', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ leonteq_name: link.leonteq_name, airs_portfolio_id: link.airs_portfolio_id, leonteq_key: left, airs_key: right }) });
      if (!response.ok) throw new Error('failed');
      const result = await response.json() as { rules: HoldingRule[] };
      rulesCache.set(rulesKey, result.rules);
      if (alive.current) { setRules(result.rules); setLeftChoice(''); setRightChoice(''); }
    } catch { if (alive.current) setRulesError(true); }
    finally { savingRule.current = false; if (alive.current) setRulesSaving(false); }
  }
  useEffect(() => {
    alive.current = true;
    let active = true;
    const request = ++version.current;
    for (const side of sides) {
      if (cache.has(keyFor(link, side))) continue;
      void read(link, side, false).then((result) => {
        if (active && request === version.current) {
          cache.set(keyFor(link, side), result);
          setData((current) => ({ ...current, [side]: result }));
        }
      }).catch(() => {
        if (active && request === version.current) setErrors((current) => ({ ...current, [side]: true }));
      }).finally(() => {
        if (active && request === version.current) setLoading((current) => ({ ...current, [side]: false }));
      });
    }
    return () => { active = false; alive.current = false; };
  }, [link, cache]);

  function refresh() {
    const request = ++version.current;
    setLoading({ leonteq: true, airs: true }); setErrors({});
    setRulesLoading(true); setRulesError(false);
    const params = new URLSearchParams({ leonteq_name: link.leonteq_name, airs_portfolio_id: String(link.airs_portfolio_id) });
    void apiFetch(`${rulesUrl}?${params}`, { cache: 'no-store', noReadCache: true })
      .then(async (response) => { if (!response.ok) throw new Error('failed'); return await response.json() as { rules: HoldingRule[] }; })
      .then((result) => { if (alive.current) { rulesCache.set(rulesKey, result.rules); setRules(result.rules); } })
      .catch(() => { if (alive.current) setRulesError(true); })
      .finally(() => { if (alive.current) setRulesLoading(false); });
    for (const side of sides) {
      void read(link, side, true, nl).then((result) => {
        if (alive.current && request === version.current) {
          cache.set(keyFor(link, side), result);
          setData((current) => ({ ...current, [side]: result }));
        }
      }).catch(() => {
        if (alive.current && request === version.current) setErrors((current) => ({ ...current, [side]: true }));
      }).finally(() => {
        if (alive.current && request === version.current) setLoading((current) => ({ ...current, [side]: false }));
      });
    }
  }

  const comparison = rules && data.leonteq && data.airs && !data.airs.noFixed
    ? matchHoldings(data.leonteq.rows, data.airs.rows, rules) : null;
  const controlsBusy = rulesLoading || rulesSaving || loading.leonteq || loading.airs;
  function choices(rows: Holding[]) {
    return Array.from(new Map(rows.map((row) => [holdingKey(row), {
      value: holdingKey(row), label: row.name ?? row.isin ?? '—', description: row.isin ?? (nl ? 'Geen ISIN' : 'No ISIN'),
    }])).values());
  }
  function leftoverTable(rows: Holding[], side: Side) {
    const headers = side === 'leonteq' ? ['ISIN', nl ? 'Naam' : 'Name', nl ? 'Gewicht' : 'Weight'] : ['Fonds', 'Percentage', 'ISIN-code', nl ? 'Actie' : 'Action'];
    return <div className="overflow-x-auto rounded-xl border border-neutral-800/40">
      <table className="w-full text-sm"><caption className="sr-only">{side === 'leonteq' ? 'Leonteq' : 'AIRS'} · {nl ? 'Niet gekoppelde posities' : 'Unmatched holdings'}</caption>
        <thead className="bg-elevated text-xs text-fg-muted"><tr>{headers.map((label, column) => <th key={label} scope="col" className={`px-3 py-3 font-medium ${(side === 'leonteq' ? column === 2 : column === 1) ? 'text-right' : 'text-left'}`}>{label}</th>)}</tr></thead>
        <tbody className="divide-y divide-neutral-800/30">{rows.map((row, index) => <tr key={`${row.isin ?? row.name}-${index}`}>
          {(side === 'leonteq' ? [row.isin, row.name, formatWeight(row.weight)] : [row.name, formatWeight(row.weight), row.isin]).map((value, column) => <td key={column} className={`px-3 py-3 ${(side === 'leonteq' ? column === 2 : column === 1) ? 'text-right tabular-nums' : ''}`}>{value ?? '—'}</td>)}
          {side === 'airs' && <td className="px-3 py-3">{holdingKey(row).startsWith('isin:') && !data.leonteq?.rows.some((source) => holdingKey(source) === holdingKey(row)) && <button type="button" disabled={controlsBusy} onClick={() => void saveRule(soldRuleKey(holdingKey(row)), holdingKey(row))} className="whitespace-nowrap text-accent-300 hover:underline disabled:opacity-50" aria-label={`Mark ${row.name} sold in Leonteq`}>{nl ? 'Verkocht in Leonteq' : 'Sold in Leonteq'}</button>}</td>}
        </tr>)}
          {!rows.length && <tr><td colSpan={headers.length} className="px-3 py-6 text-center text-fg-muted">{nl ? 'Geen overgebleven posities.' : 'No unmatched holdings.'}</td></tr>}
        </tbody>
      </table>
    </div>;
  }

  return <section className="space-y-4 [&_thead_th]:text-center [&_thead_th]:align-middle" aria-labelledby="linked-holdings-heading">
    <button type="button" disabled={rulesSaving} onClick={onBack} className="text-sm text-accent-300 hover:underline disabled:opacity-50">← {nl ? 'Alle koppelingen' : 'All connections'}</button>
    <div className="flex flex-wrap items-center justify-between gap-3">
      <h2 id="linked-holdings-heading" className="text-lg font-semibold">{nl ? 'Portefeuilleposities' : 'Portfolio holdings'}</h2>
      <button type="button" disabled={controlsBusy} onClick={refresh} className="rounded-lg border border-neutral-800/40 bg-elevated px-3 py-2 text-sm disabled:opacity-50">{nl ? 'Beide vernieuwen' : 'Refresh both'}</button>
    </div>
    {rulesLoading && <p role="status" className="text-sm text-fg-muted">{nl ? 'Opgeslagen rij-koppelingen laden…' : 'Loading saved row connections…'}</p>}
    {rulesError && <p role="alert" className="text-sm text-neg-300">{nl ? 'De rij-koppelingen konden niet worden geladen of opgeslagen. Kies Beide vernieuwen en probeer opnieuw.' : 'Row connections could not be loaded or saved. Select Refresh both and retry.'}</p>}
    <div className="grid min-w-0 grid-cols-1 items-start gap-4 lg:grid-cols-2">
      {sides.map((side) => {
        const snapshot = data[side];
        const name = side === 'leonteq' ? link.leonteq_name : link.airs_name ?? `AIRS #${link.airs_portfolio_id}`;
        const headers = side === 'leonteq' ? ['ISIN', nl ? 'Naam' : 'Name', nl ? 'Gewicht' : 'Weight'] : ['Fonds', 'Percentage', 'ISIN-code'];
        return <section key={side} className="min-w-0 space-y-3" aria-label={`${side === 'leonteq' ? 'Leonteq' : 'AIRS'} · ${name}`} aria-busy={loading[side]}>
          <div className="min-h-32 rounded-lg bg-elevated px-4 py-3">
            <p className="text-xs font-medium text-fg-muted">{side === 'leonteq' ? 'Leonteq' : 'AIRS'}</p>
            <h3 className="mt-1 font-semibold">{name}</h3>
            {snapshot?.datum && <p className="mt-1 text-sm text-fg-muted">{nl ? 'Samenstellingsdatum' : 'Composition date'}: {formatDate(snapshot.datum)}</p>}
            {snapshot?.timestamp && <p className="mt-1 text-sm text-fg-muted">{snapshot.cached ? (nl ? 'Opgeslagen op' : 'Saved at') : (nl ? 'Opgehaald op' : 'Retrieved at')} {formatDate(snapshot.timestamp, true)}</p>}
          </div>
          {loading[side] && <p role="status" className="text-sm text-fg-muted">{nl ? 'Posities laden…' : 'Loading holdings…'}</p>}
          {(errors[side] || snapshot?.warning) && <p role="alert" className="text-sm text-neg-300">{nl ? 'Deze posities konden niet worden vernieuwd. Eventuele opgeslagen gegevens blijven zichtbaar.' : 'These holdings could not be refreshed. Any saved data remains visible.'}</p>}
          {snapshot?.noFixed ? <p className="text-sm text-fg-muted">{nl ? 'Dit AIRS-model heeft geen vaste samenstelling.' : 'This AIRS model has no fixed composition.'}</p>
            : snapshot && !comparison && <div className="overflow-x-auto rounded-xl border border-neutral-800/40">
              <table className="w-full text-sm"><caption className="sr-only">{side === 'leonteq' ? 'Leonteq' : 'AIRS'} · {name}</caption>
                <thead className="bg-elevated text-xs text-fg-muted"><tr>{headers.map((label, column) => <th key={label} scope="col" className={`px-3 py-3 font-medium ${(side === 'leonteq' ? column === 2 : column === 1) ? 'text-right' : 'text-left'}`}>{label}</th>)}</tr></thead>
                <tbody className="divide-y divide-neutral-800/30">{snapshot.rows.map((row, index) => <tr key={`${row.isin ?? row.name}-${index}`} className="hover:bg-elevated/40">
                  {(side === 'leonteq' ? [row.isin, row.name, formatWeight(row.weight)] : [row.name, formatWeight(row.weight), row.isin]).map((value, column) => <td key={column} className={`px-3 py-3 ${(side === 'leonteq' ? column === 2 : column === 1) ? 'text-right tabular-nums' : column === (side === 'leonteq' ? 0 : 2) ? 'whitespace-nowrap' : ''}`}>{value ?? '—'}</td>)}
                </tr>)}
                  {!snapshot.rows.length && <tr><td colSpan={3} className="px-3 py-8 text-center text-fg-muted">{nl ? 'Geen posities beschikbaar.' : 'No holdings available.'}</td></tr>}
                </tbody>
              </table>
            </div>}
        </section>;
      })}
    </div>
    {comparison && <>
      <section className="space-y-3" aria-labelledby="matched-holdings-heading">
        <h3 id="matched-holdings-heading" className="font-semibold">{nl ? 'Gekoppelde posities' : 'Connected holdings'} <span className="text-sm font-normal text-fg-muted">({comparison.matched.length})</span></h3>
        <div className="overflow-x-auto rounded-xl border border-neutral-800/40">
          <table className="w-full text-sm">
            <caption className="sr-only">{nl ? 'Gekoppelde posities' : 'Connected holdings'}</caption>
            <thead className="bg-elevated text-xs text-fg-muted">
              <tr><td aria-hidden="true" /><th colSpan={2} scope="colgroup" className="border-l border-neutral-800/30 px-3 py-2 font-medium">Leonteq</th><th colSpan={2} scope="colgroup" className="border-l border-neutral-800/30 px-3 py-2 font-medium">AIRS</th><td aria-hidden="true" /></tr>
              <tr><th scope="col" className="px-3 py-3 font-medium">ISIN</th><th scope="col" className="border-l border-neutral-800/30 px-3 py-3 font-medium">{nl ? 'Naam' : 'Name'}</th><th scope="col" className="px-3 py-3 font-medium">{nl ? 'Gewicht' : 'Weight'}</th><th scope="col" className="border-l border-neutral-800/30 px-3 py-3 font-medium">Fonds</th><th scope="col" className="px-3 py-3 font-medium">Percentage</th><th scope="col" className="px-3 py-3 font-medium">{nl ? 'Actie' : 'Action'}</th></tr>
            </thead>
            <tbody>{comparison.matched.map((match) => <Fragment key={match.leonteqKey}>
              {Array.from({ length: Math.max(match.leonteq.length, match.airs.length) }, (_, index) => <tr key={index} className={`${index === 0 ? 'border-t border-neutral-800/30' : ''} hover:bg-elevated/40`}>
                {index === 0 && <th scope="rowgroup" rowSpan={Math.max(match.leonteq.length, match.airs.length)} className="px-3 py-3 text-left align-top font-medium"><div>{match.isin}</div><div className="mt-1 text-xs font-normal text-fg-muted">{match.cash ? (nl ? 'Automatisch opgeteld' : 'Automatically combined') : match.manual ? (nl ? 'Handmatig gekoppeld' : 'Manually connected') : match.company ? (nl ? 'Bedrijfsnaam · verschillende ISINs' : 'Company name · different ISINs') : 'ISIN'}</div></th>}
                <td className="border-l border-neutral-800/30 px-3 py-3">{match.leonteq[index]?.name ?? '—'}</td>
                <td className="px-3 py-3 text-right tabular-nums">{formatWeight(match.leonteq[index]?.weight)}</td>
                <td className="border-l border-neutral-800/30 px-3 py-3">{match.airs[index]?.name ?? '—'}</td>
                <td className="px-3 py-3 text-right tabular-nums">{formatWeight(match.airs[index]?.weight)}</td>
                {index === 0 && <td rowSpan={Math.max(match.leonteq.length, match.airs.length)} className="px-3 py-3 text-center align-middle">{match.cash ? (nl ? 'Automatisch' : 'Automatic') : <button type="button" disabled={controlsBusy}
                  aria-label={`${nl ? 'Posities ontkoppelen voor' : 'Disconnect holdings for'} ${match.leonteq[0].name ?? match.isin}`}
                  onClick={() => void saveRule(match.leonteqKey, null)} className="text-fg-muted hover:text-neg-300 disabled:opacity-50">{nl ? 'Ontkoppelen' : 'Disconnect'}</button>}</td>}
              </tr>)}
            </Fragment>)}
              {!comparison.matched.length && <tr><td colSpan={6} className="px-3 py-8 text-center text-fg-muted">{nl ? 'Geen gekoppelde posities.' : 'No connected holdings.'}</td></tr>}
            </tbody>
          </table>
        </div>
      </section>
      {comparison.sold.length > 0 && <section className="space-y-3" aria-label="Sold in Leonteq">
        <h3 className="font-semibold">{nl ? 'Verkocht in Leonteq' : 'Sold in Leonteq'}</h3>
        <div className="overflow-x-auto rounded-xl border border-neutral-800/40"><table className="w-full text-sm">
          <thead className="bg-elevated text-xs text-fg-muted"><tr>{['Fonds', 'ISIN', 'AIRS', nl ? 'Doel' : 'Target', nl ? 'Actie' : 'Action'].map((label) => <th key={label} className="px-3 py-3">{label}</th>)}</tr></thead>
          <tbody>{comparison.sold.flatMap((sold) => sold.rows.map((row, index) => <tr key={`${sold.airsKey}-${index}`} className="border-t border-neutral-800/30">
            <th className="px-3 py-3 text-left font-medium">{row.name}{sold.conflict && <p role="alert" className="text-xs text-neg-300">{nl ? 'Weer aanwezig in Leonteq. Markering ongedaan maken.' : 'Present in Leonteq again. Undo the sold marking.'}</p>}</th>
            <td className="px-3 py-3">{row.isin}</td><td className="px-3 py-3">{formatWeight(row.weight)}</td><td className="px-3 py-3">{sold.conflict ? '—' : '0.00%'}</td>
            <td className="px-3 py-3 text-center"><button type="button" disabled={controlsBusy} onClick={() => void saveRule(soldRuleKey(sold.airsKey), null)} className="text-accent-300 hover:underline disabled:opacity-50">{nl ? 'Ongedaan maken' : 'Undo'}</button></td>
          </tr>))}</tbody>
        </table></div>
      </section>}
      <section className="space-y-3" aria-labelledby="unmatched-holdings-heading">
        <h3 id="unmatched-holdings-heading" className="font-semibold">{nl ? 'Overgebleven posities' : 'Unmatched holdings'}</h3>
        <form className="flex flex-wrap items-end gap-3 rounded-xl border border-neutral-800/40 bg-elevated/40 p-4" onSubmit={(event) => {
          event.preventDefault();
          if (comparison.leonteqOnly.some((row) => holdingKey(row) === leftChoice) && comparison.airsOnly.some((row) => holdingKey(row) === rightChoice)) void saveRule(leftChoice, rightChoice);
        }}>
          <div className="min-w-0 flex-[1_1_16rem]"><SearchablePortfolioPicker label={nl ? 'Leonteq-positie' : 'Leonteq holding'} placeholder={nl ? 'Zoek een positie…' : 'Search a holding…'} options={choices(comparison.leonteqOnly)} value={leftChoice} onSelect={setLeftChoice} disabled={controlsBusy} nl={nl} /></div>
          <div className="min-w-0 flex-[1_1_16rem]"><SearchablePortfolioPicker label={nl ? 'AIRS-positie' : 'AIRS holding'} placeholder={nl ? 'Zoek een positie…' : 'Search a holding…'} options={choices(comparison.airsOnly)} value={rightChoice} onSelect={setRightChoice} disabled={controlsBusy} nl={nl} /></div>
          <button type="submit" disabled={controlsBusy || !comparison.leonteqOnly.some((row) => holdingKey(row) === leftChoice) || !comparison.airsOnly.some((row) => holdingKey(row) === rightChoice)} className="rounded-lg bg-accent-600 px-3 py-2 text-sm font-medium text-white disabled:opacity-50">{nl ? 'Rijen koppelen' : 'Connect rows'}</button>
        </form>
        <div className="grid min-w-0 grid-cols-1 items-start gap-4 lg:grid-cols-2">
          {sides.map((side) => {
            const rows = side === 'leonteq' ? comparison.leonteqOnly : comparison.airsOnly;
            return <section key={side} className="min-w-0 space-y-2" aria-label={`${side === 'leonteq' ? 'Leonteq' : 'AIRS'} · ${nl ? 'Overgebleven posities' : 'Unmatched holdings'}`}>
              <h4 className="text-sm font-medium">{side === 'leonteq' ? 'Leonteq' : 'AIRS'} <span className="text-fg-muted">({rows.length})</span></h4>
              {leftoverTable(rows, side)}
            </section>;
          })}
        </div>
        <p className="text-xs text-fg-muted">{nl ? 'Gelijke ISINs worden automatisch gekoppeld, tenzij je ze ontkoppelt. Je kunt overgebleven posities handmatig koppelen, ook cash. Gewichten blijven ongewijzigd.' : 'Matching ISINs connect automatically unless you disconnect them. You can manually connect leftover holdings, including cash. Weights stay unchanged.'}</p>
      </section>
    </>}
  </section>;
}

export type ConnectionHoldingsCache = Map<string, Snapshot>;
