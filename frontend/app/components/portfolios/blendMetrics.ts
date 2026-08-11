import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { runSSE } from '../../../lib/stream';

/**
 * Loading the blended metric suite for a PORTFOLIO, with per-company progress.
 *
 * ⚠ ONE LOADER, TWO CALLERS. `FundamentalCharts` and `LongEquityTab` both open on the same blend
 * and each had its own copy of the fetch. Two implementations of "load the portfolio's metrics" is
 * one more than the number of ways to load them, and they had already begun to differ (one mapped
 * 404 to an empty suite, the other to a friendly note).
 *
 * ⚠ THE STREAM IS AN IMPROVEMENT, NEVER A DEPENDENCY. Any failure of the SSE path — an old backend
 * with no `/stream` route, a proxy that buffers, a malformed frame — falls back to the plain POST,
 * which is the endpoint that was always there and which has real status codes. So the worst case
 * is the spinner we had before, not a broken modal; and 404 handling lives in exactly one place
 * (the fallback), rather than being re-derived from a thrown "HTTP 404" string.
 */

export type BlendHolding = { isin: string; weight: number; name?: string };

/** A portfolio to blend: an explicit basket, or a model portfolio resolved server-side. */
export type BlendTarget = {
  basket?: { holdings: BlendHolding[] };
  portfolioId?: number;
  /** 'quarterly' rolls every metric to TRAILING TWELVE MONTHS server-side. Omitted = annual.
   *  ⚠ The PORTFOLIO path needs this explicitly: the derived cards carry the cadence in the body
   *  they already POST, but the blend has its own builder — leave it out and a book's growth cards
   *  quietly stay on fiscal years while the nine cards beside them switch. */
  cadence?: 'annual' | 'quarterly';
};

/** How far the blend has got. `total` is the number of COVERED holdings — the ones with
 *  fundamentals to read — which is smaller than the book and is the number actually being waited
 *  on. `name` is the holding just finished. */
export type BlendProgress = { done: number; total: number; name?: string | null };

export type BlendResult<T> =
  | { kind: 'ready'; data: T }
  | { kind: 'none' }                      // 404 — nothing in this book has fundamentals
  | { kind: 'error'; message: string };

const BASE = `${API_URL}/api/earnings/fundamental-blend-metrics`;

/** The request body both endpoints take. A basket sends its holdings; a saved model portfolio
 *  sends its id and is expanded server-side (the frontend never holds its membership). */
export function blendBody(t: BlendTarget): string {
  const cadence = t.cadence ?? 'annual';
  return JSON.stringify(t.basket
    ? { holdings: t.basket.holdings.map((h) => ({ isin: h.isin, name: h.name, weight: h.weight })),
      cadence }
    : { portfolio_id: t.portfolioId, cadence });
}

async function viaPost<T>(body: string, signal?: AbortSignal): Promise<BlendResult<T>> {
  const r = await apiFetch(BASE, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body, signal,
  });
  if (r.status === 404) return { kind: 'none' };
  const b = await r.json().catch(() => null);
  if (!r.ok) return { kind: 'error', message: (b?.detail as string) ?? `HTTP ${r.status}` };
  return { kind: 'ready', data: b as T };
}

/**
 * Load the blend, reporting progress as each holding's metrics arrive.
 *
 * `onProgress` fires only on the streaming path; a fallback run reports nothing and the caller
 * keeps whatever it last showed. Never throws for a load failure — the outcome is in the result —
 * but an abort propagates, because a cancelled request has no outcome to report.
 */
export async function loadBlendMetrics<T>(
  target: BlendTarget,
  onProgress: (p: BlendProgress) => void,
  signal?: AbortSignal,
): Promise<BlendResult<T>> {
  const body = blendBody(target);
  let result: BlendResult<T> | null = null;
  try {
    await runSSE(`${BASE}/stream`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body,
    }, (raw) => {
      const e = raw as { type?: string; done?: number; total?: number; name?: string | null;
        payload?: T; detail?: string };
      if (e.type === 'progress' && typeof e.done === 'number' && typeof e.total === 'number') {
        onProgress({ done: e.done, total: e.total, name: e.name });
      } else if (e.type === 'result' && e.payload) {
        result = { kind: 'ready', data: e.payload };
      } else if (e.type === 'error') {
        // ⚠ An error INSIDE the stream, after the headers went out. It cannot be a status code,
        // so it arrives as a frame — and it is a real answer, not a reason to retry the whole
        // blend over the slower path.
        result = { kind: 'error', message: e.detail ?? 'blend failed' };
      }
    }, signal);
  } catch (e) {
    if (signal?.aborted) throw e;
    result = null;                       // the stream itself failed — fall back below
  }
  if (result) return result;
  // ⚠ ALSO REACHED WHEN THE STREAM ENDED WITHOUT A RESULT — a truncated body reads as a clean
  // close, and treating "no frames" as success would render an empty suite as though the book had
  // no fundamentals.
  return viaPost<T>(body, signal);
}

/** "Loading… (3 of 41 companies)", or plain "Loading…" before the first frame. */
export function blendLoadingLabel(p: BlendProgress | null): string {
  if (!p || !p.total) return 'Loading…';
  return `Loading… (${p.done} of ${p.total} companies)`;
}
