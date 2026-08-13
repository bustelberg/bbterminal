'use client';

/**
 * A running commentary of what the app is doing, in the browser console.
 *
 * WHY IT IS ALWAYS ON, INCLUDING IN PRODUCTION
 *   The bugs that cost us the most this year were not crashes — they were CONFIDENT WRONG
 *   NUMBERS, and every one of them was invisible until someone happened to compare two screens:
 *   a portfolio reading +55.20% in production and +36.64% locally (a truncated read), a holding
 *   priced off the wrong vendor, an "already current" price that was never fetched. None of them
 *   raised anything. A stack trace would not have helped; a record of WHAT WAS READ, HOW MUCH
 *   CAME BACK, and WHICH BRANCH WAS TAKEN would have answered each in seconds.
 *
 *   So this is not a developer-only aid to be stripped at build time. When something looks wrong
 *   in production the console IS the diagnostic, and it has to already be there — asking a user
 *   to reproduce with a debug flag on is asking them to reproduce a number they cannot reproduce.
 *
 * ⚠ IT NEVER READS A RESPONSE BODY. Cloning a response to count rows doubles the memory of every
 *   payload on the page (ACWI alone is ~1,700 members) and, worse, a clone that is never consumed
 *   leaks. Size comes from `content-length`, which the server already sent. Shape, where it
 *   matters, is logged by the caller — which knows what it asked for.
 *
 * ⚠ AN EMPTY RESULT IS A FIRST-CLASS EVENT, NOT A QUIET SUCCESS. `traceEmpty` exists because a
 *   fresh or partially-migrated database is the state this page is most likely to meet on its
 *   first production deploy, and "0 rows" rendered as a blank panel is indistinguishable from a
 *   broken one. Every panel says what it found, what it therefore drew, and why.
 *
 * Silence it with `localStorage.setItem('bb.debug', '0')` (and `removeItem` to restore) — for a
 * screen-share or a demo, not as the default. Nothing here throws: a tracer that can break the
 * page it is tracing is worse than no tracer.
 */

const PREFIX = 'bb';

function enabled(): boolean {
  try {
    return typeof window !== 'undefined' && window.localStorage.getItem('bb.debug') !== '0';
  } catch {
    return true;      // private mode / blocked storage — trace rather than go silent
  }
}

/** `1,234 ms` / `812 ms` — a duration a human compares at a glance. */
const ms = (n: number) => `${Math.round(n).toLocaleString('en-US')} ms`;

/** `content-length` as something readable, or null when the server did not send one
 *  (chunked responses, SSE). Never derived from the body — see the header note. */
function sizeOf(resp: Response): string | null {
  const raw = resp.headers.get('content-length');
  if (!raw) return null;
  const n = Number(raw);
  if (!Number.isFinite(n)) return null;
  return n >= 1024 * 1024 ? `${(n / 1024 / 1024).toFixed(1)} MB`
    : n >= 1024 ? `${Math.round(n / 1024)} kB`
      : `${n} B`;
}

/** The path, without the origin — the origin is the same for every call and eats the line. */
function shortUrl(url: string): string {
  try {
    const u = new URL(url, typeof window !== 'undefined' ? window.location.origin : 'http://x');
    return u.pathname + (u.search || '');
  } catch {
    return url;
  }
}

/** One step in a panel's own story: what it is about to do, or what it decided. */
export function trace(scope: string, message: string, data?: unknown): void {
  if (!enabled()) return;
  try {
    if (data === undefined) console.log(`%c[${PREFIX}:${scope}]%c ${message}`, 'color:#3b82c9', '');
    else console.log(`%c[${PREFIX}:${scope}]%c ${message}`, 'color:#3b82c9', '', data);
  } catch { /* a tracer must never break its caller */ }
}

/**
 * A state the page is DEGRADED into, not a failure — the empty database, the missing pairing, the
 * benchmark with no universe. Warn level, because it changes what the reader is looking at.
 *
 * `why` is not optional on purpose: "0 rows" is not information, "0 rows — nothing has been
 * scanned into `airs_model_portfolio` yet, press Scan AIRS" is.
 */
export function traceEmpty(scope: string, what: string, why: string): void {
  if (!enabled()) return;
  try {
    console.warn(`%c[${PREFIX}:${scope}]%c ${what} — ${why}`, 'color:#c0891a', '');
  } catch { /* never break the caller */ }
}

/** Something went wrong. The UI gets one short line; the console gets everything. */
export function traceError(scope: string, message: string, err?: unknown): void {
  try {
    console.error(`%c[${PREFIX}:${scope}]%c ${message}`, 'color:#c33', '', err ?? '');
  } catch { /* never break the caller */ }
}

/** Wrap a panel's whole load so its steps sit under one collapsed heading rather than
 *  interleaving with two other panels loading at the same time. Returns whatever `fn` returns
 *  and re-raises whatever it throws — grouping must not change control flow. */
export async function traceScope<T>(scope: string, label: string, fn: () => Promise<T>): Promise<T> {
  if (!enabled()) return fn();
  const t0 = performance.now();
  try {
    console.groupCollapsed(`%c[${PREFIX}:${scope}]%c ${label}`, 'color:#3b82c9;font-weight:600', '');
  } catch { /* ignore */ }
  try {
    return await fn();
  } finally {
    try {
      console.log(`%c[${PREFIX}:${scope}]%c finished in ${ms(performance.now() - t0)}`,
        'color:#3b82c9', '');
      console.groupEnd();
    } catch { /* ignore */ }
  }
}

/**
 * Every backend call, logged from ONE place.
 *
 * `apiFetch` is the single chokepoint for the whole app, so instrumenting it covers every panel
 * on every page for free — and, more to the point, covers the calls nobody thought to instrument.
 * A 401 after a token expiry, a 502 from the gateway, a request that takes 40 seconds: all of
 * them show up here without a single line in the caller.
 */
export function traceRequest(
  method: string, url: string,
): (resp: Response | null, err?: unknown, cancelled?: boolean) => void {
  const t0 = performance.now();
  const path = shortUrl(url);
  return (resp, err, cancelled) => {
    if (!enabled()) return;
    try {
      if (err || !resp) {
        /**
         * ⚠⚠ A REQUEST WE CANCELLED OURSELVES IS NOT A FAILURE, AND CALLING IT ONE IS WORSE THAN
         * SAYING NOTHING. Every long-lived read on this app ends by being aborted — an SSE stream
         * when the tab is hidden or the page unmounts, a fetch superseded by a newer selection —
         * so the normal, healthy lifecycle was painting red `FAILED` lines in the console:
         *
         *     [bb:api] GET /api/schedule/stream — FAILED after 7,801 ms AbortError: signal is
         *     aborted without reason
         *
         * Nothing failed there; the page closed a stream it had deliberately opened. A console
         * that cries wolf on its own teardown is a console people stop reading, which costs the
         * genuine 502 sitting two lines below it.
         *
         * ⚠ THE CALLER TELLS US, we do not sniff the error. `AbortError` alone is ambiguous — it is
         * also what an `AbortSignal.timeout()` raises, and a timeout IS a failure. `apiFetch` knows
         * whether ITS signal was the one that fired.
         */
        if (cancelled) {
          console.log(`%c[${PREFIX}:api]%c ${method} ${path} — cancelled after ${ms(performance.now() - t0)}`,
            'color:#6b7280', '');
          return;
        }
        console.error(`%c[${PREFIX}:api]%c ${method} ${path} — FAILED after ${ms(performance.now() - t0)}`,
          'color:#c33', '', err ?? '');
        return;
      }
      const size = sizeOf(resp);
      const line = `${method} ${path} → ${resp.status} in ${ms(performance.now() - t0)}`
        + (size ? ` · ${size}` : '');
      // A 401 is the one status worth shouting about: it renders as an empty panel, and the cause
      // (an expired session) is nowhere near the symptom.
      if (resp.status === 401 || resp.status === 403) {
        console.warn(`%c[${PREFIX}:api]%c ${line} — not authorised; the panel will look empty`,
          'color:#c0891a', '');
      } else if (!resp.ok) {
        console.error(`%c[${PREFIX}:api]%c ${line}`, 'color:#c33', '');
      } else {
        console.log(`%c[${PREFIX}:api]%c ${line}`, 'color:#6b7280', '');
      }
    } catch { /* never break the caller */ }
  };
}

/** How many rows a payload actually carried, and — when it is zero — that this is worth noticing.
 *  Call it from the panel, which is the only place that knows what "empty" would mean. */
export function traceRows(scope: string, what: string, rows: unknown[] | null | undefined,
                          emptyMeans?: string): void {
  const n = Array.isArray(rows) ? rows.length : 0;
  if (n === 0 && emptyMeans) traceEmpty(scope, `0 ${what}`, emptyMeans);
  else trace(scope, `${n.toLocaleString('en-US')} ${what}`);
}
