'use client';

import { useCallback, useRef, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { type Target } from './HoldingsRevenueModal';
import {
  badgeFor, hasMetric, planIngest, summarize,
  type Attempt, type CoverageRow, type MatrixRow, type Plan, type Tone,
} from './bulkIngest';

/**
 * "This portfolio has no <metric>" → fetch the GuruFocus financials for every holding that could
 * have it, one at a time, then list each holding as present or not.
 *
 * ⚠ SEQUENTIAL, ALWAYS. Hammering GuruFocus in parallel is how a bulk run gets throttled, and a
 * throttled fetch comes back looking like an absence rather than an error (the same failure mode
 * the asset-pipeline resolver guards against). The run shows its progress and can be stopped.
 *
 * ⚠ THE LIST OUTLIVES THE EMPTY STATE. Reloading the charts is a button, not an automatic
 * consequence of finishing: if some holdings load and others don't, auto-reloading would replace
 * the list naming the absent ones with a chart that looks complete.
 *
 * The card column is narrow, so every row is ONE line — name truncates, badge is fixed-width, and
 * the reason lives in the `title`. A wrapped sentence in a 4-up grid is what made this ugly.
 */

const TONE: Record<Tone, string> = {
  ok: 'text-pos-400', warn: 'text-warn-300', muted: 'text-fg-muted', pending: 'text-fg-faint',
};

type Phase = 'idle' | 'planning' | 'running' | 'done';

export default function HoldingsIngestPanel({ target, metric, noun, onIngested }: {
  target: Target;
  metric: string;          // the `metric` param of portfolio-revenue-matrix (cfg.benchmarkMetric)
  noun: string;            // 'dividend/share' — for the sentences
  onIngested?: () => void; // reload the tab's metrics (the caller's charts)
}) {
  const [phase, setPhase] = useState<Phase>('idle');
  const [plan, setPlan] = useState<Plan | null>(null);
  const [at, setAt] = useState(0);                       // 1-based position in the queue
  const [attempts, setAttempts] = useState<Record<string, Attempt>>({});
  const [present, setPresent] = useState<Set<string>>(new Set());  // keys that HAVE it now
  const [err, setErr] = useState<string | null>(null);
  const stop = useRef(false);

  const post = useCallback(async (path: string, body: unknown) => {
    const r = await apiFetch(`${API_URL}${path}`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body),
    });
    const j = await r.json().catch(() => null);
    if (!r.ok) throw new Error((j as { detail?: string })?.detail ?? `HTTP ${r.status}`);
    return j;
  }, []);

  /** Which holdings have the card's metric right now — what every badge is taken from. */
  const probe = useCallback(async (): Promise<MatrixRow[]> => {
    const j = await post(`/api/earnings/portfolio-revenue-matrix?metric=${encodeURIComponent(metric)}`, target);
    return ((j as { rows?: MatrixRow[] })?.rows ?? []);
  }, [post, metric, target]);

  const run = useCallback(async () => {
    stop.current = false;
    setPhase('planning'); setErr(null); setAttempts({}); setPresent(new Set()); setAt(0);
    let p: Plan;
    try {
      // The work-list is coverage (it lists holdings with NO company row — the ones that most need
      // this); the matrix only says who already has the metric. See `bulkIngest`.
      const [cov, matrix] = await Promise.all([
        post('/api/earnings/fundamental-coverage', target) as Promise<{ rows?: CoverageRow[] }>,
        probe(),
      ]);
      p = planIngest(cov?.rows ?? [], matrix);
      setPlan(p);
      setPresent(new Set(matrix.filter(hasMetric).map((r) => r.isin)));
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
      setPhase('idle');
      return;
    }
    if (p.queue.length === 0) { setPhase('done'); return; }

    setPhase('running');
    let n = 0;
    for (const q of p.queue) {
      if (stop.current) break;
      setAt(++n);
      try {
        const j = await post('/api/earnings/fundamental-coverage/ingest',
          { isin: q.isin, name: q.name }) as { status?: string; detail?: string };
        setAttempts((s) => ({ ...s, [q.key]: { key: q.key, status: j?.status, detail: j?.detail } }));
      } catch (e) {
        const detail = e instanceof Error ? e.message : String(e);
        setAttempts((s) => ({ ...s, [q.key]: { key: q.key, status: 'error', detail } }));
      }
    }
    // ⚠ Re-probe: "ingested" is not "this card has data now". A non-payer fetches perfectly and
    // still has no dividend/share — only the metric itself can say whether the run helped.
    try {
      const after = await probe();
      setPresent(new Set(after.filter(hasMetric).map((r) => r.isin)));
    } catch { /* the badges fall back to the per-holding ingest answers */ }
    setPhase('done');
  }, [post, probe, target]);

  if (phase === 'idle') {
    return (
      <div className="py-16 flex flex-col items-center gap-3 text-center px-4">
        <p className="text-[12px] text-fg-faint">No {noun} ingested for this portfolio.</p>
        {err && <p className="text-xs text-neg-300 max-w-[30ch] break-words">{err}</p>}
        <button type="button" onClick={run}
          title={`Fetch every holding's financials from GuruFocus, one at a time, then list which ones have ${noun}.`}
          className="text-xs px-3 py-1 rounded-lg border border-accent-600/40 text-accent-400 hover:bg-overlay/5">
          Fetch financials for all holdings
        </button>
      </div>
    );
  }

  const counts = plan ? summarize(plan.rows, (k) => present.has(k)) : null;

  return (
    <div className="rounded-lg border border-neutral-800/40 bg-inset p-3 space-y-2 min-w-0">
      <div className="flex items-center gap-2 min-w-0">
        <span className="text-[12px] text-fg-soft truncate min-w-0">
          {phase === 'planning' ? 'Checking holdings…'
            : phase === 'running' ? `Fetching ${at} / ${plan?.queue.length ?? 0}`
              : counts ? `${counts.present} of ${counts.total} have ${noun}` : ''}
        </span>
        {phase === 'running' ? (
          <button type="button" onClick={() => { stop.current = true; }}
            className="ml-auto shrink-0 text-[12px] px-2 py-0.5 rounded-lg border border-neutral-700 text-fg-soft hover:bg-overlay/5">
            Stop
          </button>
        ) : phase === 'done' ? (
          <span className="ml-auto shrink-0 flex items-center gap-1.5">
            {counts && counts.present > 0 && onIngested && (
              <button type="button" onClick={onIngested} title="Reload the charts."
                className="text-[12px] px-2 py-0.5 rounded-lg border border-accent-600/40 text-accent-400 hover:bg-overlay/5">
                Reload
              </button>
            )}
            <button type="button" onClick={run} title="Retry the holdings that came back empty."
              className="text-[12px] px-2 py-0.5 rounded-lg border border-neutral-700 text-fg-soft hover:bg-overlay/5">
              Again
            </button>
          </span>
        ) : null}
      </div>

      {err && <p className="text-[12px] text-neg-300 break-words">{err}</p>}

      {/* One line per holding: present, or not — `unsubscribed` called out because it is the one
          absence an ingest can never fix. Everything else that explains an absence is the title. */}
      {plan && (
        <ul className="max-h-56 overflow-auto space-y-0.5">
          {plan.rows.map((row) => {
            const b = badgeFor(row, attempts[row.key], present.has(row.key));
            return (
              <li key={row.key} className="flex items-baseline gap-2 min-w-0 text-[12px]"
                title={b.note ? `${row.name} — ${b.note}` : row.name}>
                <span className="flex-1 min-w-0 truncate text-fg-soft">{row.name}</span>
                <span className={`shrink-0 font-mono ${TONE[b.tone]}`}>{b.label}</span>
              </li>
            );
          })}
        </ul>
      )}
    </div>
  );
}
