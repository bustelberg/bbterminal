'use client';

import { useCallback, useEffect, useState } from 'react';
import { API_URL } from '../../lib/apiUrl';
import { apiFetch } from '../../lib/apiFetch';

// ── Shape of GET /api/admin/network-diagnostics ────────────────────────
type Verdict = 'ok' | 'blocked' | 'degraded' | 'unreachable';

type SourceResult = {
  name: string;
  category: 'critical' | 'important' | 'optional';
  purpose: string;
  domain: string | null;
  url: string | null;
  resolved_ip: string | null;
  dns_error: string | null;
  status_code: number | null;
  latency_ms: number | null;
  server: string | null;
  cdn_headers: Record<string, string>;
  used_target?: string | null;
  verdict: Verdict;
  reason: string;
};

type Diagnostics = {
  observed_at: string;
  egress: { ip: string | null; source: string | null; error: string | null };
  gurufocus_circuit: {
    curl_cffi_available: boolean;
    circuit_open: boolean;
    circuit_seconds_remaining: number;
    proxy_configured: boolean;
    preferred_target: string | null;
    ladder: string[];
  };
  sources: SourceResult[];
  summary: Record<string, number>;
};

const VERDICT_META: Record<Verdict, { label: string; dot: string; chip: string }> = {
  ok: { label: 'Reachable', dot: 'bg-pos-500', chip: 'bg-pos-500/10 text-pos-300 border-pos-500/25' },
  degraded: { label: 'Degraded', dot: 'bg-warn-500', chip: 'bg-warn-500/15 text-warn-300 border-warn-500/30' },
  blocked: { label: 'Blocked', dot: 'bg-neg-500', chip: 'bg-neg-500/15 text-neg-300 border-neg-500/30' },
  unreachable: { label: 'Unreachable', dot: 'bg-neg-500', chip: 'bg-neg-500/15 text-neg-300 border-neg-500/30' },
};

const CATEGORY_LABEL: Record<SourceResult['category'], string> = {
  critical: 'Critical — the terminal can’t function without these',
  important: 'Important — degraded features if down',
  optional: 'Optional — single page / background job',
};

function VerdictChip({ verdict }: { verdict: Verdict }) {
  const m = VERDICT_META[verdict];
  return (
    <span className={`inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-[11px] font-medium border ${m.chip}`}>
      <span className={`w-1.5 h-1.5 rounded-full ${m.dot}`} />
      {m.label}
    </span>
  );
}

export default function NetworkDiagnostics() {
  const [data, setData] = useState<Diagnostics | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const run = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const resp = await apiFetch(`${API_URL}/api/admin/network-diagnostics`);
      if (!resp.ok) {
        const body = await resp.text().catch(() => '');
        throw new Error(`HTTP ${resp.status}${body ? ` — ${body.slice(0, 200)}` : ''}`);
      }
      setData((await resp.json()) as Diagnostics);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void run();
  }, [run]);

  const circuit = data?.gurufocus_circuit;
  const grouped = (['critical', 'important', 'optional'] as const).map((cat) => ({
    cat,
    rows: (data?.sources ?? []).filter((s) => s.category === cat),
  }));

  return (
    <div className="px-8 py-5 max-w-6xl">
      <div className="flex items-start justify-between gap-4 mb-5">
        <div>
          <h1 className="text-xl font-semibold tracking-tight text-fg-strong">Network</h1>
          <p className="text-sm text-fg-muted mt-1">
            Reachability of the external services this site depends on — and whether our egress IP is being
            blocked by Cloudflare. Probes run server-side from the backend (Railway), so they reflect what the
            ingest pipeline actually sees.
          </p>
        </div>
        <button
          type="button"
          onClick={() => void run()}
          disabled={loading}
          className="shrink-0 bg-accent-600 hover:bg-accent-500 disabled:opacity-50 text-white text-sm font-medium px-4 py-2 rounded-lg transition-colors"
        >
          {loading ? 'Checking…' : 'Re-run checks'}
        </button>
      </div>

      {error && (
        <div className="bg-neg-500/10 border border-neg-500/20 rounded-lg px-4 py-3 text-sm text-neg-300 mb-5">
          Couldn’t run diagnostics: {error}
        </div>
      )}

      {/* Our IP + GuruFocus circuit-breaker state — the two "why" headline cards. */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-5">
        <div className="bg-card rounded-xl border border-neutral-800/40 p-5">
          <div className="text-xs uppercase tracking-wider text-fg-subtle mb-1">Our egress IP</div>
          <div className="font-mono text-2xl text-fg-strong">
            {data?.egress.ip ?? (loading ? '…' : '—')}
          </div>
          <div className="text-xs text-fg-muted mt-1">
            {data?.egress.error
              ? data.egress.error
              : data?.egress.source
                ? `What external services see when we connect (via ${data.egress.source}).`
                : 'The address remote hosts see when this backend connects out.'}
          </div>
        </div>

        <div className="bg-card rounded-xl border border-neutral-800/40 p-5">
          <div className="text-xs uppercase tracking-wider text-fg-subtle mb-1">GuruFocus Cloudflare status</div>
          {circuit ? (
            circuit.circuit_open ? (
              <>
                <div className="text-2xl font-semibold text-neg-300">
                  Blocked · retry in {circuit.circuit_seconds_remaining}s
                </div>
                <div className="text-xs text-fg-muted mt-1">
                  The circuit breaker is OPEN — too many Cloudflare blocks, so calls are suppressed.{' '}
                  {circuit.proxy_configured
                    ? 'A proxy is configured.'
                    : 'Set GURUFOCUS_PROXY to a residential proxy to bypass.'}
                </div>
              </>
            ) : (
              <>
                <div className="text-2xl font-semibold text-pos-300">Clear</div>
                <div className="text-xs text-fg-muted mt-1">
                  No active block. Impersonating <span className="font-mono">{circuit.preferred_target ?? '—'}</span>
                  {circuit.proxy_configured ? ' through a proxy.' : ' directly.'}
                  {!circuit.curl_cffi_available && ' ⚠ curl_cffi missing — prod calls will be blocked.'}
                </div>
              </>
            )
          ) : (
            <div className="text-2xl font-semibold text-fg-faint">{loading ? '…' : '—'}</div>
          )}
        </div>
      </div>

      {/* Per-source table, grouped by criticality. */}
      <div className="bg-card rounded-xl border border-neutral-800/40 overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="text-fg-subtle text-xs border-b border-neutral-800/40">
              <th className="text-left px-5 py-2.5 font-medium">Service</th>
              <th className="text-left px-3 py-2.5 font-medium">Domain → IP</th>
              <th className="text-left px-3 py-2.5 font-medium">Status</th>
              <th className="text-right px-3 py-2.5 font-medium">Latency</th>
              <th className="text-left px-5 py-2.5 font-medium">Why</th>
            </tr>
          </thead>
          <tbody>
            {grouped.map(({ cat, rows }) =>
              rows.length === 0 ? null : (
                <tr key={`hdr-${cat}`}>
                  <td colSpan={5} className="bg-inset px-5 py-1.5 text-[11px] uppercase tracking-wider text-fg-subtle border-b border-neutral-800/40">
                    {CATEGORY_LABEL[cat]}
                  </td>
                </tr>
              ),
            )}
            {grouped.flatMap(({ rows }) =>
              rows.map((s) => (
                <tr key={s.name} className="border-b border-neutral-800/20 align-top">
                  <td className="px-5 py-3">
                    <div className="font-medium text-fg-strong">{s.name}</div>
                    <div className="text-xs text-fg-muted">{s.purpose}</div>
                  </td>
                  <td className="px-3 py-3 font-mono text-xs">
                    <div className="text-fg-soft">{s.domain ?? '—'}</div>
                    <div className="text-fg-subtle">
                      {s.resolved_ip ?? (s.dns_error ? 'DNS failed' : '—')}
                      {s.status_code != null && <span className="ml-2 text-fg-faint">HTTP {s.status_code}</span>}
                    </div>
                  </td>
                  <td className="px-3 py-3">
                    <VerdictChip verdict={s.verdict} />
                    {Object.keys(s.cdn_headers).length > 0 && (
                      <div className="text-[10px] text-fg-faint font-mono mt-1">
                        {Object.entries(s.cdn_headers)
                          .map(([k, v]) => `${k}=${v}`)
                          .join(' · ')}
                      </div>
                    )}
                  </td>
                  <td className="px-3 py-3 text-right font-mono text-xs text-fg-muted whitespace-nowrap">
                    {s.latency_ms != null ? `${s.latency_ms} ms` : '—'}
                  </td>
                  <td className="px-5 py-3 text-xs text-fg-soft max-w-md">{s.reason}</td>
                </tr>
              )),
            )}
            {!data && !loading && (
              <tr>
                <td colSpan={5} className="px-5 py-8 text-center text-sm text-fg-subtle">
                  No results yet.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {data && (
        <div className="text-xs text-fg-faint mt-3">
          Checked {new Date(data.observed_at).toLocaleString()} ·{' '}
          {Object.entries(data.summary)
            .map(([k, v]) => `${v} ${k}`)
            .join(' · ')}
        </div>
      )}
    </div>
  );
}
