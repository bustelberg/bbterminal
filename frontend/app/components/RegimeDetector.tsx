'use client';

import { useEffect, useMemo, useRef, useState } from 'react';

import { API_URL } from '../../lib/apiUrl';
import { runSSE } from '../../lib/stream';
import { useUniverseTemplates, useStaticUniverses } from '../../lib/hooks/apiData';
import type { BacktestResult } from '../../lib/stores/momentum';
import MarketHealthCard from './momentum/MarketHealthCard';
import ExposureReturnsBreakdown from './momentum/ExposureReturnsBreakdown';
import RegimeBenchmarkSim from './momentum/RegimeBenchmarkSim';

/**
 * /regime-detector — a standalone research tool for the market-health
 * signal, decoupled from running a strategy.
 *
 * It drives the existing backtest endpoint in a "signals-only" config
 * (momentum selection with top-1 holdings so the payload stays tiny, but
 * breadth is still measured over the WHOLE eligible universe; regime_floor=0
 * so `market_health` + its component breakdown are computed every month).
 * The result feeds `MarketHealthCard` with `showComponents`, so you see the
 * composite health, its three sub-signals, and a selectable benchmark on a
 * shared time axis — to judge whether the indicator anticipates crises
 * before wiring it deeper into the strategy.
 */

const today = () => new Date().toISOString().slice(0, 10);

type Progress = { pct: number; message: string };

export default function RegimeDetector() {
  const { data: templates } = useUniverseTemplates();
  const { data: statics } = useStaticUniverses();

  // Both template-managed (ACWI) and frozen snapshots are valid universes
  // for the signals-only run — `index_universe` resolves either.
  const universes = useMemo(
    () => [...(templates ?? []), ...(statics ?? [])],
    [templates, statics],
  );

  const [universe, setUniverse] = useState<string>('');
  // Default to a range that's inside typical FX + price coverage. The
  // `fx_rate` table only extends FORWARD from when backtests first
  // populated it (~2017) — it never backfills earlier dates — so starting
  // at the universe's 2002 membership floor drops every non-EUR company
  // (no FX to convert) and yields an empty, signal-less run. Match the
  // backtest's default start; the user can pull it back if their DB has
  // older FX + price history.
  const [startDate, setStartDate] = useState<string>('2017-01-01');
  const [endDate, setEndDate] = useState<string>(today());

  const [running, setRunning] = useState(false);
  const [progress, setProgress] = useState<Progress | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [warnings, setWarnings] = useState<string[]>([]);
  const [result, setResult] = useState<BacktestResult | null>(null);
  const [fromCache, setFromCache] = useState(false);
  // Raw SSE event trace — reveals exactly what the backend sent (did a
  // `result` event arrive? what warnings?) so a silent failure is visible.
  const [eventLog, setEventLog] = useState<{ type: string; msg: string }[]>([]);
  // Off by default → repeat runs of the same window are served from the
  // cache (instant). Tick it to recompute fresh (e.g. after the signal
  // definition changes, since the cache would otherwise serve a stale one).
  const [forceRecompute, setForceRecompute] = useState(false);

  const abortRef = useRef<AbortController | null>(null);

  // Default the universe to the first available template once it loads.
  // The start date is intentionally NOT pulled to the template's earliest
  // membership month — that floor (≈2002) is usually before the DB's FX /
  // price coverage and produces an empty run (see startDate note above).
  useEffect(() => {
    if (universe || universes.length === 0) return;
    setUniverse(universes[0].template_key);
  }, [universes, universe]);

  async function run() {
    abortRef.current?.abort();
    const ctrl = new AbortController();
    abortRef.current = ctrl;
    setRunning(true);
    setError(null);
    setWarnings([]);
    setResult(null);
    setFromCache(false);
    setEventLog([]);
    setProgress({ pct: 0, message: 'Starting…' });

    const body = {
      start_date: startDate,
      end_date: endDate,
      index_universe: universe,
      selection_mode: 'momentum',
      // Tiny selection → small payload; breadth is over the full universe.
      top_n_sectors: 1,
      top_n_per_sector: 1,
      rebalance_frequency: 'monthly',
      // Triggers per-period market_health + component + RSI computation.
      regime_floor: 0,
      // Cache by default (keyed by config + UTC day) so the same window
      // replays instantly; the toggle forces a fresh compute when needed.
      force_recompute: forceRecompute,
    };

    try {
      await runSSE(
        `${API_URL}/api/momentum/backtest`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(body),
        },
        (raw) => {
          const d = raw as { type: string; pct?: number; message?: string; data?: BacktestResult; from_cache?: boolean };
          // Trace every non-progress event (progress is too chatty) so a
          // silently-dropped `result` is obvious. For `result`, note the
          // record count so we can see if the payload arrived but was empty.
          if (d.type !== 'progress') {
            const note =
              d.type === 'result'
                ? `${d.data?.monthly_records?.length ?? 0} records${d.from_cache ? ' (cache)' : ''}`
                : (d.message ?? '');
            setEventLog((l) => (l.length < 60 ? [...l, { type: d.type, msg: note }] : l));
          }
          if (d.type === 'progress') {
            setProgress({ pct: d.pct ?? 0, message: d.message ?? '' });
          } else if (d.type === 'result') {
            if (d.data) setResult(d.data);
            if (d.from_cache) setFromCache(true);
          } else if (d.type === 'warning') {
            const m = d.message ?? '';
            if (m) setWarnings((w) => (w.length < 12 ? [...w, m] : w));
          } else if (d.type === 'error') {
            setError(d.message ?? 'Unknown error');
          }
        },
        ctrl.signal,
      );
    } catch (e) {
      if (!ctrl.signal.aborted) setError(e instanceof Error ? e.message : String(e));
    } finally {
      setRunning(false);
    }
  }

  function cancel() {
    abortRef.current?.abort();
    setRunning(false);
  }

  return (
    <div className="px-8 py-5 space-y-5">
      <div>
        <h1 className="text-fg-strong text-xl font-semibold">Regime Detector</h1>
        <p className="text-fg-muted text-sm mt-1 max-w-3xl">
          Compute the composite market-health signal over a universe and compare it against a benchmark —
          to judge whether it sags ahead of crises before integrating it into the strategy. Health is the average of three
          absolute breadth measures (fraction above 200-MA, fraction with positive 6-month momentum, and average closeness to 52-week highs).
        </p>
      </div>

      <div className="bg-card rounded-xl border border-neutral-800/40 p-5 space-y-4">
        <div className="grid grid-cols-1 md:grid-cols-4 gap-3">
          <label className="text-xs text-fg-subtle block">
            Universe
            <select
              value={universe}
              onChange={(e) => setUniverse(e.target.value)}
              className="mt-1 w-full bg-page border border-neutral-700 rounded-lg px-3 py-2 text-sm text-fg focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none"
            >
              {universes.length === 0 && <option value="">(loading…)</option>}
              {universes.map((u) => (
                <option key={u.template_key} value={u.template_key}>
                  {u.label ?? u.template_key} ({u.latest_membership_count})
                </option>
              ))}
            </select>
          </label>
          <label className="text-xs text-fg-subtle block">
            Start
            <input
              type="date"
              value={startDate}
              onChange={(e) => setStartDate(e.target.value)}
              className="mt-1 w-full bg-page border border-neutral-700 rounded-lg px-3 py-2 text-sm text-fg font-mono focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none"
            />
          </label>
          <label className="text-xs text-fg-subtle block">
            End
            <input
              type="date"
              value={endDate}
              onChange={(e) => setEndDate(e.target.value)}
              className="mt-1 w-full bg-page border border-neutral-700 rounded-lg px-3 py-2 text-sm text-fg font-mono focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none"
            />
          </label>
          <div className="flex items-end gap-2">
            {running ? (
              <button
                type="button"
                onClick={cancel}
                className="w-full bg-neg-500/10 border border-neg-500/30 text-neg-300 rounded-lg px-4 py-2 text-sm hover:bg-neg-500/20"
              >
                Cancel
              </button>
            ) : (
              <button
                type="button"
                onClick={run}
                disabled={!universe}
                className="w-full bg-accent-600 hover:bg-accent-500 disabled:opacity-40 disabled:cursor-not-allowed text-white rounded-lg px-4 py-2 text-sm"
              >
                Compute signal
              </button>
            )}
          </div>
        </div>

        <label className="flex items-center gap-2 text-[11px] text-fg-muted cursor-pointer w-fit">
          <input
            type="checkbox"
            checked={forceRecompute}
            onChange={(e) => setForceRecompute(e.target.checked)}
            className="accent-accent-500 w-3.5 h-3.5 cursor-pointer"
          />
          Force recompute (ignore cache)
        </label>

        {running && progress && (
          <div className="space-y-1.5">
            <div className="h-1.5 bg-page rounded-full overflow-hidden">
              <div className="h-full bg-accent-500 transition-all" style={{ width: `${progress.pct}%` }} />
            </div>
            <p className="text-[11px] text-fg-faint font-mono truncate">{progress.message}</p>
          </div>
        )}

        {error && (
          <div className="bg-neg-500/10 border border-neg-500/20 rounded-lg px-3 py-2 text-sm text-neg-300">
            {error}
          </div>
        )}
      </div>

      {warnings.length > 0 && (
        <div className="bg-warn-500/5 border border-warn-500/20 rounded-lg px-3 py-2 text-[11px] text-warn-300/90 space-y-0.5">
          {warnings.map((w, i) => (
            <div key={i} className="font-mono truncate">{w}</div>
          ))}
        </div>
      )}

      {result && (() => {
        const recs = result.monthly_records ?? [];
        const nHealth = recs.filter((r) => r.market_health != null).length;
        const nRsi = recs.filter((r) => r.universe_rsi != null).length;
        const firstEmpty = recs.find((r) => r.empty_reason)?.empty_reason;
        return (
          <div className="text-[12px] bg-card rounded-xl border border-neutral-800/40 px-4 py-3 space-y-1.5">
            <div className="font-mono text-fg-soft flex items-center gap-2">
              <span>{recs.length} periods · {nHealth} with health · {nRsi} with RSI</span>
              {fromCache && (
                <span className="text-[10px] font-sans px-1.5 py-0.5 rounded bg-accent-500/15 text-accent-300 border border-accent-500/30">cached</span>
              )}
            </div>
            {nHealth < 2 && (
              <div className="text-warn-300">
                The run completed but produced no per-period signals to chart — the universe likely has no DB price coverage for this window,
                or the start date precedes the universe snapshot. Try a narrower, more recent range (e.g. 2015→today).
                {firstEmpty ? ` First empty period: ${firstEmpty}` : ''}
              </div>
            )}
          </div>
        );
      })()}

      {result && <MarketHealthCard result={result} showComponents defaultCollapsed={false} />}

      {result && <ExposureReturnsBreakdown result={result} defaultCollapsed={false} />}

      {result && <RegimeBenchmarkSim result={result} defaultCollapsed={false} />}

      {eventLog.length > 0 && (
        <details className="bg-card rounded-xl border border-neutral-800/40 px-4 py-2">
          <summary className="text-[11px] text-fg-muted cursor-pointer select-none">
            Event log ({eventLog.length}) — {eventLog.some((e) => e.type === 'result') ? 'result received' : 'NO result event'}
          </summary>
          <div className="mt-2 max-h-48 overflow-auto font-mono text-[10px] space-y-0.5">
            {eventLog.map((e, i) => (
              <div key={i} className={e.type === 'result' ? 'text-accent-300' : e.type === 'error' ? 'text-neg-300' : 'text-fg-faint'}>
                {e.type}{e.msg ? ` · ${e.msg}` : ''}
              </div>
            ))}
          </div>
        </details>
      )}

      {!result && !running && !error && (
        <div className="text-sm text-fg-subtle">
          Pick a universe and date range, then <span className="text-fg-soft">Compute signal</span>.
          The run loads prices and builds monthly breadth across the whole universe — it can take a while (it always recomputes fresh).
        </div>
      )}
    </div>
  );
}
