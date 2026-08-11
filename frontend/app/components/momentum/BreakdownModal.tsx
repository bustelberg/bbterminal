'use client';

/**
 * "Why this company, on this day" — the full arithmetic behind one pick, as a modal.
 *
 * Extracted from `schedule/DailyHoldingsSection` (2026-08-02) so the /schedule "Current portfolio"
 * table opens the SAME screen from a clicked company name. It sits beside `signalBreakdown.tsx`
 * (the renderer) for the same reason that file gives: this shows the arithmetic behind a
 * selection, and a second copy is a second explanation of one number.
 */

import { useEffect, useState } from 'react';
import { createPortal } from 'react-dom';
import Spinner from '../Spinner';
import { API_URL } from '../../../lib/apiUrl';
import { runSSE } from '../../../lib/stream';
import { BreakdownView, CategoryMathBreakdown, type BreakdownData } from './signalBreakdown';

/** Which company, on which day, we are explaining. */
export type BreakdownTarget = {
  companyId: number;
  date: string;
  name: string;
  ticker: string | null;
};

/**
 * ⚠ IT RENDERS THE SHARED `BreakdownView`, NOT A SECOND EXPLANATION. Everything on screen —
 * raw signal, universe min/max, the 0-100 normalisation, the weight, the category blend, the final
 * score — comes from `POST /api/momentum/signal-breakdown`, the same endpoint and the same
 * components the /backtest ticker timeline uses. Two renderers for one number is two answers to
 * "why", and they drift the moment a pillar is added.
 *
 * ⚠ IT RE-DERIVES THE SCORE AT THAT CUTOFF RATHER THAN READING THE ROW. That is deliberate and it
 * is also the one thing that can disagree: the row's score came out of the walk (possibly from
 * cache), this is computed live now. A mismatch is not a bug in either — it is the same
 * late-arriving-price effect the "vs stored" column shows, and the header states the cutoff so the
 * two can be compared rather than confused.
 *
 * `config` must be the config the pick was MADE with (universe, signal + category weights) — the
 * snapshot's own, not the strategy's current one, which may have been edited since.
 */
export default function BreakdownModal({ target, config, onClose }: {
  target: BreakdownTarget;
  config: Record<string, unknown>;
  onClose: () => void;
}) {
  const [state, setState] = useState<
    { s: 'loading'; msg: string } | { s: 'error'; msg: string } | { s: 'ok'; data: BreakdownData }
  >({ s: 'loading', msg: 'Starting…' });
  const [mounted, setMounted] = useState(false);
  // createPortal needs `document`, undefined during SSR — defer until after first
  // client mount. Same canonical pattern (and same lint exemption) as `CellInfoTip`.
  // eslint-disable-next-line react-hooks/set-state-in-effect
  useEffect(() => setMounted(true), []);

  // Escape closes, as everywhere else in the app.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose(); };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onClose]);

  useEffect(() => {
    const ac = new AbortController();
    void (async () => {
      try {
        await runSSE(
          `${API_URL}/api/momentum/signal-breakdown`,
          {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              company_id: target.companyId,
              as_of_date: target.date,
              universe_label: config.universe_label,
              index_universe: config.index_universe,
              signal_weights: config.signal_weights,
              category_weights: config.category_weights,
            }),
          },
          (raw) => {
            const evt = raw as { type?: string; message?: string; data?: BreakdownData };
            if (evt.type === 'progress' && evt.message) setState({ s: 'loading', msg: evt.message });
            // ⚠ `result`, matching the endpoint (`routers/momentum/signals.py`) and the ticker
            // timeline's consumer. A wrong event name here fails SILENTLY — the stream completes,
            // nothing sets state, and the modal spins forever with no error.
            else if (evt.type === 'result' && evt.data) setState({ s: 'ok', data: evt.data });
            else if (evt.type === 'error') throw new Error(evt.message ?? 'Breakdown failed');
          },
          ac.signal,
        );
      } catch (e) {
        if (ac.signal.aborted) return;
        console.warn('[breakdown] failed', e);
        setState({ s: 'error', msg: e instanceof Error ? e.message : String(e) });
      }
    })();
    return () => { ac.abort(); };
  }, [target, config]);

  // ⚠ PORTALLED TO `document.body`, NOT RENDERED IN PLACE. This modal is opened from a row inside
  // `CollapsibleCard`, whose root is `overflow-hidden` — and the app's frosted chrome applies
  // `backdrop-filter`, which makes an ancestor the containing block for `position: fixed`. Either
  // one traps the overlay inside the card: it renders clipped, scrolled with the table, and sized
  // against the wrong box instead of the viewport.
  if (!mounted) return null;
  return createPortal((
    <div className="fixed inset-0 z-[70] flex items-center justify-center bg-scrim/60 p-4"
      onClick={onClose} role="presentation">
      <div className="bg-card rounded-xl border border-neutral-800/40 shadow-xl w-[80vw] h-[85vh] flex flex-col"
        onClick={(e) => e.stopPropagation()} role="dialog" aria-modal="true">
        <div className="flex items-baseline gap-3 px-6 py-4 border-b border-neutral-800/40">
          <h2 className="text-fg-strong font-medium">Why this was picked</h2>
          <span className="text-sm text-fg-soft truncate max-w-[26ch]" title={target.name}>{target.name}</span>
          {target.ticker && <span className="text-[12px] font-mono text-fg-faint">{target.ticker}</span>}
          {/* The cutoff, always — the whole breakdown is "as of" this date and means nothing without it. */}
          <span className="text-[12px] font-mono text-accent-300">as of {target.date}</span>
          <button type="button" onClick={onClose}
            className="ml-auto text-fg-muted hover:text-fg-strong px-2">✕</button>
        </div>
        <div className="flex-1 overflow-auto px-6 py-4 space-y-4">
          {state.s === 'loading' && (
            <p className="text-xs text-fg-subtle flex items-center gap-2">
              <Spinner className="h-3 w-3" />{state.msg}
            </p>
          )}
          {state.s === 'error' && (
            <div className="bg-neg-500/10 border border-neg-500/20 rounded-lg px-3 py-2 text-xs text-neg-300">
              {state.msg}
            </div>
          )}
          {state.s === 'ok' && (
            <>
              <BreakdownView data={state.data} />
              <CategoryMathBreakdown data={state.data} />
            </>
          )}
        </div>
      </div>
    </div>
  ), document.body);
}
