'use client';

import { useState, useEffect, useCallback, useImperativeHandle, useRef, forwardRef } from 'react';

import { API_URL } from '../../lib/apiUrl';
import { apiFetch } from '../../lib/apiFetch';
const LIMIT = 20000;

type Usage = { usa: number; europe: number; asia: number; month: string };

export type ApiUsageBadgeHandle = {
  addSessionCalls: (region: string, count: number) => void;
  refresh: () => void;
};

function ApiInfoTip() {
  const [show, setShow] = useState(false);
  const [pos, setPos] = useState({ top: 0, left: 0 });
  const iconRef = useRef<HTMLSpanElement>(null);

  const tipWidth = 288; // w-72
  const margin = 8;

  const handleEnter = () => {
    if (iconRef.current) {
      const rect = iconRef.current.getBoundingClientRect();
      const centerX = rect.left + rect.width / 2;
      const clampedLeft = Math.max(margin + tipWidth / 2, Math.min(centerX, window.innerWidth - margin - tipWidth / 2));
      setPos({ top: rect.bottom + 8, left: clampedLeft });
    }
    setShow(true);
  };

  return (
    <span className="relative cursor-help" onMouseEnter={handleEnter} onMouseLeave={() => setShow(false)}>
      <span ref={iconRef} className="inline-flex items-center justify-center w-4 h-4 rounded-full border border-neutral-600 text-fg-subtle text-[10px] leading-none hover:border-accent-400 hover:text-accent-400 transition-colors">i</span>
      {show && (
        <span
          className="fixed w-72 px-3 py-2 bg-popover border border-neutral-700 rounded-lg text-xs text-fg-soft leading-relaxed z-[9999] shadow-xl pointer-events-none"
          style={{ top: pos.top, left: pos.left, transform: 'translate(-50%, 0)' }}
        >
          Estimated monthly GuruFocus API usage (20k requests per region). May not be fully accurate — check actual usage at the Bustelberg GuruFocus account (API Token tab). Resets each month at midnight EST.
        </span>
      )}
    </span>
  );
}

const ApiUsageBadge = forwardRef<ApiUsageBadgeHandle>(function ApiUsageBadge(_props, ref) {
  const [usage, setUsage] = useState<Usage | null>(null);
  const [session, setSession] = useState({ usa: 0, europe: 0, asia: 0 });

  const fetchUsage = useCallback(() => {
    apiFetch(`${API_URL}/api/usage`)
      .then((r) => r.json())
      .then((data) => setUsage(data))
      .catch(() => {});
  }, []);

  useImperativeHandle(ref, () => ({
    addSessionCalls(region: string, count: number) {
      setSession((prev) => ({
        ...prev,
        [region]: (prev[region as keyof typeof prev] ?? 0) + count,
      }));
    },
    refresh: fetchUsage,
  }), [fetchUsage]);

  useEffect(() => {
    fetchUsage();
    const interval = setInterval(fetchUsage, 60_000);
    return () => clearInterval(interval);
  }, [fetchUsage]);

  if (!usage) return null;

  const usaPct = (usage.usa / LIMIT) * 100;
  const eurPct = (usage.europe / LIMIT) * 100;
  const asiaPct = ((usage.asia ?? 0) / LIMIT) * 100;

  const barColor = (pct: number) =>
    pct >= 90 ? 'bg-neg-500' : pct >= 70 ? 'bg-warn-500' : 'bg-accent-500';

  return (
    <div className="flex items-center gap-4 px-3 py-2 bg-card rounded-lg border border-neutral-800/40 text-xs">
      <span className="text-fg-subtle font-medium">API</span>
      <ApiInfoTip />
      <div className="flex items-center gap-1.5">
        <span className="text-fg-muted">USA</span>
        <div className="w-20 h-1.5 bg-neutral-800 rounded-full overflow-hidden">
          <div className={`h-full rounded-full transition-all ${barColor(usaPct)}`} style={{ width: `${Math.min(usaPct, 100)}%` }} />
        </div>
        <span className="text-fg-muted font-mono">{usage.usa.toLocaleString()}/{(LIMIT / 1000)}k</span>
        {session.usa > 0 && <span className="text-accent-400 font-mono">+{session.usa}</span>}
      </div>
      <div className="flex items-center gap-1.5">
        <span className="text-fg-muted">EU</span>
        <div className="w-20 h-1.5 bg-neutral-800 rounded-full overflow-hidden">
          <div className={`h-full rounded-full transition-all ${barColor(eurPct)}`} style={{ width: `${Math.min(eurPct, 100)}%` }} />
        </div>
        <span className="text-fg-muted font-mono">{usage.europe.toLocaleString()}/{(LIMIT / 1000)}k</span>
        {session.europe > 0 && <span className="text-accent-400 font-mono">+{session.europe}</span>}
      </div>
      <div className="flex items-center gap-1.5">
        <span className="text-fg-muted">Asia</span>
        <div className="w-20 h-1.5 bg-neutral-800 rounded-full overflow-hidden">
          <div className={`h-full rounded-full transition-all ${barColor(asiaPct)}`} style={{ width: `${Math.min(asiaPct, 100)}%` }} />
        </div>
        <span className="text-fg-muted font-mono">{(usage.asia ?? 0).toLocaleString()}/{(LIMIT / 1000)}k</span>
        {session.asia > 0 && <span className="text-accent-400 font-mono">+{session.asia}</span>}
      </div>
    </div>
  );
});

export default ApiUsageBadge;
