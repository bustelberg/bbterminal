import { useEffect, useState } from 'react';
import { API_URL } from '../apiUrl';
import { runSSE } from '../stream';

type Frame = { topic: string; payload: unknown };

/**
 * Subscribe to a multiplexed snapshot SSE stream (`{topic, payload}` frames from
 * `routers/_sse_stream.py`). Returns the latest payload per topic + connection
 * status.
 *
 * Lifecycle:
 *  - Opens on mount and whenever the tab becomes visible; closes when hidden —
 *    so a backgrounded page holds NO connection and makes no requests.
 *  - Reconnects with exponential backoff on error, and immediately on a clean
 *    end (the server caps a stream at ~1h to avoid leaks).
 *  - After `maxFailures` consecutive connection failures it sets `failed=true`
 *    so the caller can fall back to polling; `failed` clears on the next frame.
 *
 * Auth + JWT are handled by `runSSE` → `apiFetch` (never a raw EventSource,
 * which can't send the Authorization header).
 */
export function useEventStream(
  path: string | null,
  opts?: { maxFailures?: number },
): { data: Record<string, unknown>; connected: boolean; failed: boolean } {
  const maxFailures = opts?.maxFailures ?? 3;
  const [data, setData] = useState<Record<string, unknown>>({});
  const [connected, setConnected] = useState(false);
  const [failed, setFailed] = useState(false);

  useEffect(() => {
    if (!path) return;
    let stopped = false;
    let abort: AbortController | null = null;
    let retryTimer: number | undefined;
    let failures = 0;

    const scheduleReconnect = (ms: number) => {
      window.clearTimeout(retryTimer);
      retryTimer = window.setTimeout(() => void connect(), ms);
    };

    const connect = async () => {
      if (stopped || document.hidden) return;
      // ⚠ HELD LOCALLY AS WELL AS IN `abort`. The shared `abort` is reassigned by the NEXT
      // connect, so a catch block reading it could ask "was I aborted?" of a controller belonging
      // to a later attempt. `ctrl` is this attempt's own.
      const ctrl = new AbortController();
      abort = ctrl;
      try {
        await runSSE(
          `${API_URL}${path}`,
          { method: 'GET' },
          (evt) => {
            const f = evt as Frame;
            if (f && typeof f === 'object' && typeof f.topic === 'string') {
              failures = 0;
              setConnected(true);
              setFailed(false); // no-op if already false
              setData((prev) => ({ ...prev, [f.topic]: f.payload }));
            }
          },
          ctrl.signal,
        );
        // Clean end (server closed at its cap) — reconnect promptly.
        setConnected(false);
        if (!stopped && !document.hidden) scheduleReconnect(500);
      } catch {
        setConnected(false);
        /**
         * ⚠⚠ AN ABORT WE CAUSED IS NOT A CONNECTION FAILURE, AND COUNTING IT HAS A CONSEQUENCE
         * BEYOND THE CONSOLE. `failures` is what trips `failed=true`, which tells every caller to
         * abandon the stream and fall back to POLLING — so a page that was hidden and shown a few
         * times could talk itself into permanent polling while the stream was working perfectly.
         * The visibility handler resets the counter, which is why this has not bitten yet; that is
         * a coincidence of ordering, not a design.
         *
         * `ctrl.signal.aborted` rather than the error's name: only OUR controller firing means we
         * asked for this. Anything else — a dropped connection, a 502 from the gateway — is a real
         * failure and must still count toward the fallback.
         */
        if (ctrl.signal.aborted) {
          if (!stopped && !document.hidden) scheduleReconnect(500);
          return;
        }
        failures += 1;
        if (failures >= maxFailures) setFailed(true);
        if (!stopped && !document.hidden) scheduleReconnect(Math.min(30000, 1000 * 2 ** failures));
      }
    };

    const onVisibility = () => {
      if (document.hidden) {
        abort?.abort();
        window.clearTimeout(retryTimer);
        setConnected(false);
      } else {
        failures = 0;
        void connect();
      }
    };

    if (!document.hidden) void connect();
    document.addEventListener('visibilitychange', onVisibility);
    return () => {
      stopped = true;
      abort?.abort();
      window.clearTimeout(retryTimer);
      document.removeEventListener('visibilitychange', onVisibility);
    };
  }, [path, maxFailures]);

  return { data, connected, failed };
}
