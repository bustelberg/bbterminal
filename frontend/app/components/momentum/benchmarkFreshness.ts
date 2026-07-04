import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import type { BenchmarkPrice } from './types';

/** Status of a benchmark load/refresh, for a narrated inline line. */
export type BenchStatus = { msg: string; tone: 'info' | 'ok' | 'warn' } | null;

/** Calendar days from `a` to `b` (both "YYYY-MM-DD"); positive when b is later. */
const daysBetween = (a: string, b: string) => Math.round((Date.parse(b) - Date.parse(a)) / 86400000);

async function fetchPrices(benchmarkId: number): Promise<BenchmarkPrice[]> {
  // Widen the window so any later comparison series can still overlap.
  const resp = await apiFetch(
    `${API_URL}/api/benchmarks/${benchmarkId}/prices?start_date=1990-01-01&end_date=2099-12-31`,
  );
  if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
  return (await resp.json()) as BenchmarkPrice[];
}

/** Load a benchmark's prices, AUTO-REFRESHING from GuruFocus when they're stale
 * vs `referenceDate` (the date the strategy's line reaches). Narrates progress
 * via `onStatus` — loading / stale-refreshing / refreshed / up-to-date / failed —
 * so a benchmark is never silently overlaid on stale data. Returns the
 * (possibly refreshed) price series; a `>4` calendar-day tolerance absorbs
 * weekend/holiday skew so a benign 1-day gap doesn't trigger a needless refresh.
 */
export async function loadFreshBenchmarkPrices(
  benchmarkId: number,
  label: string,
  referenceDate: string | null,
  onStatus: (s: BenchStatus) => void,
): Promise<BenchmarkPrice[]> {
  onStatus({ msg: `Loading ${label}…`, tone: 'info' });
  let prices = await fetchPrices(benchmarkId);
  const latest = prices.length ? String(prices[prices.length - 1].target_date).slice(0, 10) : null;
  const stale = !!referenceDate && (!latest || daysBetween(latest, referenceDate) > 4);
  if (stale) {
    onStatus({
      msg: `${label} prices are stale (last close ${latest ?? 'none'}, comparison runs to ${referenceDate}) — refreshing from GuruFocus…`,
      tone: 'warn',
    });
    try {
      const r = await apiFetch(`${API_URL}/api/benchmarks/${benchmarkId}/refresh`, { method: 'POST' });
      if (r.ok) {
        const body = await r.json().catch(() => ({} as { prices_loaded?: number }));
        prices = await fetchPrices(benchmarkId);
        const newLatest = prices.length ? String(prices[prices.length - 1].target_date).slice(0, 10) : null;
        onStatus({
          msg: `${label} refreshed — ${body?.prices_loaded ?? 0} new bar(s), now current through ${newLatest ?? '—'}.`,
          tone: 'ok',
        });
      } else {
        onStatus({ msg: `${label}: refresh failed (HTTP ${r.status}) — showing the last data on file (${latest ?? 'none'}).`, tone: 'warn' });
      }
    } catch {
      onStatus({ msg: `${label}: couldn't refresh — showing the last data on file (${latest ?? 'none'}).`, tone: 'warn' });
    }
  } else {
    onStatus({ msg: `${label} is up to date (through ${latest ?? '—'}).`, tone: 'ok' });
  }
  return prices;
}
