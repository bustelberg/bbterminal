'use client';

import { useEffect, useRef, useState } from 'react';
import { apiFetch } from '../../lib/apiFetch';
import { API_URL } from '../../lib/apiUrl';
import { ExpectedJobFailure, startLocalJob } from '../../lib/stores/jobs';
import type { AirsComposition } from './AirsCompositionView';
import type { CompositionResult } from './CompositionTable';
import { cashComparison } from './cashComparison';

type Plan = { total: number; displayed_total: number; row_id: number; cash_name: string; current: number; target: number; datum: string; balanced: boolean; composition: AirsComposition };
const pct = (value: number) => `${Number(value).toFixed(2)}%`;
const button = 'rounded-lg border border-neutral-800/40 px-3 py-2 text-sm disabled:opacity-50';

export default function CashBalancePanel({ portfolioId, composition, leonteq, dummy, disabled, onBusyChange, onComposition }: {
  portfolioId: number; composition: AirsComposition; dummy: boolean; disabled: boolean;
  leonteq?: CompositionResult;
  onBusyChange: (busy: boolean) => void; onComposition: (composition: AirsComposition) => void;
}) {
  const [plan, setPlan] = useState<Plan | null>(null);
  const [working, setWorking] = useState<'read' | 'execute' | null>(null);
  const [message, setMessage] = useState('');
  const [latest, setLatest] = useState<{ base: AirsComposition; value: AirsComposition } | null>(null);
  const comparison = cashComparison(leonteq, latest?.base === composition ? latest.value : composition);
  const pending = useRef(false), alive = useRef(true);
  useEffect(() => { alive.current = true; return () => { alive.current = false; }; }, []);
  useEffect(() => { setPlan(null); }, [composition, dummy, portfolioId]);

  function run(execute: boolean) {
    if (pending.current || disabled || (execute && !plan)) return;
    pending.current = true; setWorking(execute ? 'execute' : 'read'); onBusyChange(true); setMessage('');
    startLocalJob(`${execute ? dummy ? 'Simulate cash adjustment' : 'Balance AIRS cash' : 'Read latest AIRS total'}`, 'leonteq.rebalance.cash', async (_signal, report) => {
      try {
        if (execute && dummy) {
          const result = `Simulated: ${plan!.cash_name} ${pct(plan!.current)} → ${pct(plan!.target)}. No AIRS changes.`;
          if (alive.current) setMessage(result);
          return result;
        }
        report({ done: 0, total: 1, message: execute ? 'Rechecking the latest AIRS total, adjusting cash and verifying 100%…' : 'Reading the latest AIRS composition and Totaal footer…' });
        const response = await apiFetch(`${API_URL}/api/leonteq/airs/model-portfolios/${portfolioId}/cash-balance`, {
          method: execute ? 'POST' : 'GET', cache: 'no-store', noReadCache: true,
          ...(execute ? { headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ datum: plan!.datum, row_id: plan!.row_id, total: plan!.total, current: plan!.current, target: plan!.target }) } : {}),
        });
        const result = await response.json();
        if (!response.ok) throw new Error(typeof result.detail === 'string' ? result.detail : 'Could not verify cash. Read the latest total again.');
        if (alive.current) {
          setLatest({ base: composition, value: result.composition });
          if (execute) { onComposition(result.composition); setPlan(null); setMessage('Cash saved and verified. AIRS total: 100.00%.'); }
          else setPlan(result);
        }
        return execute ? 'Cash saved and verified. AIRS total: 100.00%.' : `Latest AIRS total: ${pct(result.displayed_total)}.`;
      } catch (cause) {
        const error = cause instanceof Error ? cause.message : 'Could not verify cash. Read the latest total again.';
        if (alive.current) { setPlan(null); setMessage(error); }
        throw new ExpectedJobFailure(error);
      } finally {
        pending.current = false;
        if (alive.current) { setWorking(null); onBusyChange(false); }
      }
    }, { cancellable: false });
  }

  return <section aria-label="Balance cash" className="space-y-3 rounded-xl border border-neutral-800/40 bg-elevated/40 p-4">
    <h3 className="font-semibold">Balance cash to 100%</h3>
    <p className="text-sm text-fg-muted">Read the latest AIRS total after your changes, then adjust Liquiditeiten / cash so the portfolio adds up to 100%.</p>
    <div className="flex flex-wrap items-start gap-6 text-sm" aria-label="Cash comparison">
      <div><div className="text-fg-muted">AIRS cash</div><strong className="tabular-nums">{comparison.airs == null ? 'Unavailable' : pct(comparison.airs)}</strong></div>
      <div><div className="text-fg-muted">Leonteq cash</div><strong className="tabular-nums">{comparison.leonteq == null ? 'Unavailable' : pct(comparison.leonteq)}</strong></div>
      <div><div className="text-fg-muted">Difference (AIRS − Leonteq)</div><strong className="tabular-nums">{comparison.difference == null ? 'Unavailable' : `${comparison.difference > 0 ? '+' : ''}${comparison.difference.toFixed(2)}pp`}</strong></div>
    </div>
    <p className="text-xs text-fg-muted">Cash currencies are combined. Leonteq uses the fetched composition; Read latest total updates AIRS cash.</p>
    <button className={button} disabled={disabled || !!working} onClick={() => run(false)}>{working === 'read' ? 'Working…' : 'Read latest total'}</button>
    {plan && <div className="flex flex-wrap items-center gap-4 text-sm">
      <span>AIRS total: <strong>{pct(plan.displayed_total)}</strong></span>
      <span>{plan.cash_name}: {pct(plan.current)} → <strong>{pct(plan.target)}</strong></span>
      <span>Total after: <strong>100.00%</strong></span>
      {plan.balanced ? <span className="text-pos-300">Already balanced</span> : <button className={`${button} bg-accent-600 text-white`} disabled={disabled || !!working} onClick={() => run(true)}>{working === 'execute' ? 'Working…' : 'Execute'}</button>}
    </div>}
    {message && <p role="status" className="text-sm text-fg-muted">{message}</p>}
  </section>;
}
