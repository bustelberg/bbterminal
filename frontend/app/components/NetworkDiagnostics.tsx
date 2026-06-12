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

type GuruMethod = 'curl' | 'plain';

type Diagnostics = {
  observed_at: string;
  guru_method?: GuruMethod;
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

/**
 * Build a ready-to-send support message for GuruFocus, populated from the live
 * diagnostics. The crux: their team whitelisted our IP *for the User API* (their
 * application-level access control), but the block we hit is at Cloudflare's edge
 * (bot management / IP reputation) — a separate layer the User-API allowlist
 * doesn't touch. The cf-ray / server=cloudflare headers are the proof, and the
 * concrete ask is a Cloudflare IP Access Rule. We only have cf-ray when a real
 * probe ran (the circuit breaker short-circuits without a network call when
 * open), so the message degrades gracefully and tells them how to capture one.
 */
function buildSupportMessage(d: Diagnostics, guru: SourceResult): string {
  const egressIp = d.egress.ip ?? '(unknown — please ask us to re-check)';
  const domain = guru.domain ?? 'api.gurufocus.com';
  const cfIp = guru.resolved_ip ?? '(unresolved)';
  const path = guru.url ?? `https://${domain}/public/user/<API_KEY>/stock/{TICKER}/price`;
  const cfRay = guru.cdn_headers['cf-ray'];
  const server = guru.cdn_headers['server'];
  const cfMitigated = guru.cdn_headers['cf-mitigated'];
  const status = guru.status_code;
  const target = guru.used_target ?? d.gurufocus_circuit.preferred_target ?? null;
  const observed = new Date(d.observed_at).toISOString();

  const evidence: string[] = [
    `- ${domain} resolves to ${cfIp}, which is a Cloudflare edge IP.`,
  ];
  if (status != null) {
    evidence.push(
      `- Our request received HTTP ${status} — a Cloudflare challenge/block page, not a GuruFocus application response.`,
    );
  }
  const cfHeaderBits = [
    cfRay ? `cf-ray=${cfRay}` : null,
    server ? `server=${server}` : null,
    cfMitigated ? `cf-mitigated=${cfMitigated}` : null,
  ].filter(Boolean);
  if (cfHeaderBits.length) {
    evidence.push(
      `- The response carried Cloudflare edge headers (${cfHeaderBits.join(', ')}), confirming the block is at the Cloudflare layer rather than your API.`,
    );
  }
  if (target) {
    evidence.push(
      `- We send a genuine desktop-browser TLS fingerprint (impersonating ${target}) and are still blocked across multiple browser fingerprints — this points to an IP-reputation / bot-management block, not a TLS or User-Agent issue.`,
    );
  }

  const noRayNote = cfRay
    ? ''
    : `\n\nNote: no cf-ray ID was captured in this snapshot${
        d.gurufocus_circuit.circuit_open
          ? ' (our client briefly suppressed calls after repeated blocks)'
          : ''
      }. If you need a cf-ray to locate the blocked requests in your Cloudflare logs, let us know and we'll re-run the check to capture one.`;

  return `Subject: Cloudflare is blocking our whitelisted server IP on ${domain} (User API)

Hi GuruFocus Support,

Our server's automated calls to the GuruFocus User API are being blocked by Cloudflare, even though our egress IP was already whitelisted "for the User API."

Account / request details
- Server egress IP to allow: ${egressIp}
- Endpoint we call: GET ${path}
- API key: present (masked here for security)

What we observe (measured server-side from our backend)
${evidence.join('\n')}${noRayNote}

Why the existing whitelist didn't resolve it
The IP was whitelisted for the User API — your application-level access control. The block we hit happens earlier, at Cloudflare's edge (bot management / IP Access Rules), before the request ever reaches the User API. These are two separate layers, so a User-API allowlist does not stop the Cloudflare challenge.

What we're asking
Please allowlist our egress IP ${egressIp} at the Cloudflare layer for ${domain} — for example a Cloudflare IP Access Rule set to "Allow", or a WAF / Bot Management skip rule — so automated API traffic from this IP is no longer challenged.

Measured at: ${observed}

Thank you,`;
}

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
  const [showSupport, setShowSupport] = useState(false);
  const [draft, setDraft] = useState('');
  const [copied, setCopied] = useState(false);
  const [guruMethod, setGuruMethod] = useState<GuruMethod>('curl');

  const run = useCallback(async (method?: GuruMethod) => {
    const m = method ?? guruMethod;
    setLoading(true);
    setError(null);
    try {
      const resp = await apiFetch(`${API_URL}/api/admin/network-diagnostics?guru_method=${m}`);
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
  }, [guruMethod]);

  // Fires on mount and whenever the probe method changes (toggling guruMethod
  // recreates `run`, which re-triggers this effect → automatic re-probe).
  useEffect(() => {
    void run();
  }, [run]);

  const circuit = data?.gurufocus_circuit;
  const guru = data?.sources.find((s) => s.name === 'GuruFocus API');
  const guruBlocked = guru?.verdict === 'blocked';
  const supportMessage = data && guru ? buildSupportMessage(data, guru) : '';

  // Refresh the editable draft whenever a fresh probe regenerates the message
  // (a re-run = new data = new message; intentionally discards manual edits).
  useEffect(() => {
    setDraft(supportMessage);
  }, [supportMessage]);

  const copyDraft = useCallback(async () => {
    try {
      await navigator.clipboard.writeText(draft);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      /* clipboard blocked — the textarea is still selectable for manual copy */
    }
  }, [draft]);

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
          <div className="flex items-center justify-between gap-2 mb-2">
            <div className="text-xs uppercase tracking-wider text-fg-subtle">GuruFocus reachability</div>
            {/* Probe-method switch. "Plain requests" is the proof-of-fix test:
                if a fingerprint-less request ever returns 200, GuruFocus has
                stopped bot-challenging this IP and curl_cffi is redundant. */}
            <div className="flex items-center gap-0.5 rounded-lg border border-neutral-700 p-0.5 shrink-0">
              {(['curl', 'plain'] as const).map((m) => (
                <button
                  key={m}
                  type="button"
                  onClick={() => setGuruMethod(m)}
                  disabled={loading}
                  title={
                    m === 'curl'
                      ? 'Probe via the curl_cffi browser-impersonation ladder — exactly what the ingest pipeline uses.'
                      : 'Probe with a plain requests.get — no impersonation, no proxy, bypasses the circuit breaker. Succeeds only if GuruFocus has stopped bot-challenging this IP.'
                  }
                  className={`px-2 py-1 rounded-md text-[11px] font-medium transition-colors disabled:opacity-50 ${guruMethod === m ? 'bg-accent-600 text-white' : 'text-fg-muted hover:text-fg-strong hover:bg-overlay/5'}`}
                >
                  {m === 'curl' ? 'Browser (curl_cffi)' : 'Plain requests'}
                </button>
              ))}
            </div>
          </div>
          {circuit && guru ? (
            <>
              {/* Headline = the LIVE probe verdict (the source of truth), not
                  just the breaker state. The breaker only trips after 5
                  consecutive blocks, so it can read "not tripped" while every
                  individual call is still being blocked — that mismatch is
                  what made the old card misleading. */}
              {guruBlocked ? (
                <div className="text-2xl font-semibold text-neg-300">Blocked by Cloudflare</div>
              ) : guru.verdict === 'ok' ? (
                <div className="text-2xl font-semibold text-pos-300">Reachable</div>
              ) : (
                <div className="text-2xl font-semibold text-warn-300">Degraded</div>
              )}
              <div className="text-xs text-fg-muted mt-1 space-y-1">
                {guruMethod === 'plain' ? (
                  <>
                    <div>
                      Probed with a plain <span className="font-mono">requests.get</span> — no browser
                      impersonation, no proxy, circuit breaker bypassed.
                    </div>
                    <div>
                      This only succeeds once GuruFocus stops bot-challenging this IP — i.e. it&rsquo;s the
                      &ldquo;proper API&rdquo; test.
                    </div>
                  </>
                ) : (
                  <>
                    <div>
                      Impersonating <span className="font-mono">{circuit.preferred_target ?? '—'}</span>
                      {circuit.proxy_configured ? ' through a proxy.' : ' directly (no proxy).'}
                      {!circuit.curl_cffi_available && ' ⚠ curl_cffi missing — prod calls will be blocked.'}
                    </div>
                    <div>
                      {circuit.circuit_open
                        ? `Circuit breaker OPEN — calls suppressed for ${circuit.circuit_seconds_remaining}s.`
                        : guruBlocked
                          ? 'Circuit breaker not yet tripped (opens after 5 consecutive blocks), but live calls are being blocked right now.'
                          : 'Circuit breaker closed.'}
                    </div>
                  </>
                )}
              </div>
            </>
          ) : (
            <div className="text-2xl font-semibold text-fg-faint">{loading ? '…' : '—'}</div>
          )}
        </div>
      </div>

      {/* GuruFocus support-message drafter — only when GuruFocus isn't healthy.
          Gives the user a copy-pasteable message that tells GuruFocus support
          the block is at Cloudflare's edge (not the User API they whitelisted)
          and the exact fix to request. */}
      {guru && guru.verdict !== 'ok' && (
        <div className="bg-card rounded-xl border border-neutral-800/40 p-5 mb-5">
          <div className="flex items-start justify-between gap-4">
            <div>
              <div className="text-sm font-semibold text-fg-strong">Message for GuruFocus support</div>
              <div className="text-xs text-fg-muted mt-1 max-w-2xl">
                A ready-to-send message explaining that the block is at Cloudflare&rsquo;s edge — not the
                User-API allowlist they already applied — with the live evidence and the exact Cloudflare
                fix to request. Review, add your sign-off, then copy.
              </div>
            </div>
            <div className="flex gap-2 shrink-0">
              <button
                type="button"
                onClick={() => setShowSupport((v) => !v)}
                className="text-sm font-medium px-3 py-2 rounded-lg hover:bg-overlay/5 text-fg-soft transition-colors"
              >
                {showSupport ? 'Hide' : 'Draft message'}
              </button>
              {showSupport && (
                <button
                  type="button"
                  onClick={() => void copyDraft()}
                  className="bg-accent-600 hover:bg-accent-500 text-white text-sm font-medium px-4 py-2 rounded-lg transition-colors"
                >
                  {copied ? 'Copied!' : 'Copy'}
                </button>
              )}
            </div>
          </div>
          {showSupport && (
            <textarea
              value={draft}
              onChange={(e) => setDraft(e.target.value)}
              rows={22}
              spellCheck={false}
              className="mt-3 w-full bg-page border border-neutral-700 rounded-lg p-3 font-mono text-xs text-fg leading-relaxed focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 focus:outline-none resize-y"
            />
          )}
        </div>
      )}

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
