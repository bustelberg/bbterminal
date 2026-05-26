'use client';

import { useEffect, useMemo, useState } from 'react';
import { createClient } from '../../lib/supabase/client';

import { API_URL } from '../../lib/apiUrl';

/** Catalog entry for one endpoint. Each entry produces a card on the
 * page; the user fills in path/query params, hits Try, and sees the
 * response inline. New endpoints land in one place — no per-endpoint
 * component sprawl. */
type Param = {
  name: string;
  type: 'integer' | 'string';
  default?: string | number;
  required?: boolean;
  /** Short hint shown under the input. */
  hint?: string;
};

type Endpoint = {
  id: string;
  group: string;
  method: 'GET' | 'POST' | 'PUT' | 'DELETE' | 'PATCH';
  path: string; // may contain {placeholders} matched against pathParams
  desc: string;
  pathParams?: Param[];
  queryParams?: Param[];
  bodyExample?: object; // when set, body editor renders preseeded with this JSON
};

const ENDPOINTS: Endpoint[] = [
  // ─── Admin: health + monitoring ──────────────────────────────────
  {
    id: 'admin-health',
    group: 'Admin · health',
    method: 'GET',
    path: '/api/admin/health',
    desc: 'Composite system health. Returns is_healthy + is_healthy_strict booleans, the list of underlying checks, and any problems found. Use the strict flag as a pre-trade gate.',
  },
  {
    id: 'admin-data-freshness',
    group: 'Admin · health',
    method: 'GET',
    path: '/api/admin/data-freshness',
    desc: 'Per-source freshness: close_price + volume max target_date (with trading-day-age), the latest current-picks snapshot, the most recent pipeline run.',
  },
  {
    id: 'admin-sanity-check',
    group: 'Admin · health',
    method: 'GET',
    path: '/api/admin/sanity-check',
    desc: 'Coarse table counts (company / ingest_run / current_picks_snapshot / …) + recent-run status distribution + latest snapshot summary. For eyeball "is everything basically wired up?" checks.',
  },
  {
    id: 'admin-egress-ip',
    group: 'Admin · health',
    method: 'GET',
    path: '/api/admin/egress-ip',
    desc: 'The public IP this backend currently appears to egress from (queried via ifconfig.me with fallbacks). Hit it a few times across deploys/restarts to see if Railway is giving you a stable IP — if yes, paste it into the AirSPMS allowlist. If it rotates, you either need Railway\'s Static Outbound IP add-on or a small static-IP proxy in front.',
  },
  {
    id: 'admin-gurufocus-probe',
    group: 'Admin · health',
    method: 'GET',
    path: '/api/admin/gurufocus-probe',
    desc: 'One-shot diagnostic: hits a single GuruFocus URL through the same impersonation ladder the ingest pipeline uses, and returns the FULL response (status, all headers, body excerpt, attempted profiles). Use to confirm whether a 403 is actually Cloudflare (look for cf-ray / server=cloudflare in the response headers) vs some other 403 (revoked key, vendor 403, etc.). Compare local vs prod to confirm IP-block hypothesis.',
    queryParams: [
      { name: 'symbol', type: 'string', default: 'AAPL', hint: 'GuruFocus symbol, e.g. "AAPL" or "XAMS:ABN"' },
      { name: 'endpoint', type: 'string', default: 'price', hint: 'GuruFocus endpoint: price | financials | analyst_estimate | forward_pe_ratio' },
    ],
  },

  // ─── Admin: portfolio ────────────────────────────────────────────
  {
    id: 'admin-portfolio-latest',
    group: 'Admin · portfolio',
    method: 'GET',
    path: '/api/admin/portfolio/latest',
    desc: 'Latest scheduled-strategy snapshot, IBKR-ready. Returns target weights, exchange codes, currencies, sides, prices, and strategy metadata.',
  },
  {
    id: 'admin-portfolio-by-id',
    group: 'Admin · portfolio',
    method: 'GET',
    path: '/api/admin/portfolio/{snapshot_id}',
    desc: 'Specific snapshot by id, same shape as /latest.',
    pathParams: [
      { name: 'snapshot_id', type: 'integer', required: true, hint: 'snapshot_id from /portfolio/latest or /runs/latest' },
    ],
  },

  // ─── Admin: schedules ───────────────────────────────────────────
  {
    id: 'admin-schedules-list',
    group: 'Admin · schedules',
    method: 'GET',
    path: '/api/admin/schedules',
    desc: 'Every scheduled strategy + its full latest portfolio. The intended one-shot for an external buyer: list of strategies, next rebalancing date (next_due_at), and full IBKR-ready holdings (ticker / exchange / currency / target_weight / entry_price_local).',
    queryParams: [
      { name: 'enabled_only', type: 'string', default: 'true', hint: '"true" hides paused strategies; "false" returns everything' },
    ],
  },
  {
    id: 'admin-schedule-by-id',
    group: 'Admin · schedules',
    method: 'GET',
    path: '/api/admin/schedules/{strategy_id}',
    desc: 'One scheduled strategy with its latest portfolio + next_due_at. Same shape as one entry from the list endpoint.',
    pathParams: [
      { name: 'strategy_id', type: 'integer', required: true, hint: 'id from the list endpoint' },
    ],
  },

  // ─── Admin: pipeline runs ────────────────────────────────────────
  {
    id: 'admin-runs-latest',
    group: 'Admin · pipeline',
    method: 'GET',
    path: '/api/admin/runs/latest',
    desc: 'Most recent pipeline run (any status) + most recent SUCCESSFUL run. The latter is useful when the latest run errored — you can still see when things last worked.',
  },
  {
    id: 'admin-pipeline-runs',
    group: 'Admin · pipeline',
    method: 'GET',
    path: '/api/admin/pipeline-runs',
    desc: 'Recent pipeline runs (newest first), compact per-row summary.',
    queryParams: [
      { name: 'limit', type: 'integer', default: 20, hint: '1–100' },
    ],
  },

  // ─── Auth ────────────────────────────────────────────────────────
  {
    id: 'auth-me',
    group: 'Auth',
    method: 'GET',
    path: '/api/auth/me',
    desc: "Caller's user info + role. Hit this first to confirm the Bearer token works.",
  },

  // ─── Health (no auth required) ───────────────────────────────────
  {
    id: 'system-health',
    group: 'System',
    method: 'GET',
    path: '/api/health',
    desc: 'Backend liveness probe. Returns quickly with no auth required.',
  },
];

type CallResult =
  | { status: 'idle' }
  | { status: 'running' }
  | { status: 'done'; statusCode: number; durationMs: number; body: unknown; headers: Record<string, string> }
  | { status: 'error'; message: string };

/** Substitute {placeholders} in a path with the corresponding values
 * from the params map. Returns null + the missing param name when a
 * required placeholder isn't filled in. */
function substitutePath(
  path: string,
  values: Record<string, string>,
): { path: string; missing: string | null } {
  let missing: string | null = null;
  const out = path.replace(/\{([^}]+)\}/g, (_m, name: string) => {
    const v = values[name];
    if (v === undefined || v === '') {
      missing = name;
      return `{${name}}`;
    }
    return encodeURIComponent(v);
  });
  return { path: out, missing };
}

function buildQueryString(params: Param[] | undefined, values: Record<string, string>): string {
  if (!params || params.length === 0) return '';
  const usp = new URLSearchParams();
  for (const p of params) {
    const v = values[p.name];
    if (v !== undefined && v !== '') usp.append(p.name, v);
  }
  const s = usp.toString();
  return s ? `?${s}` : '';
}

function buildCurl(method: string, fullUrl: string, hasToken: boolean, body?: string): string {
  const tokenPart = hasToken ? ' -H "Authorization: Bearer $TOKEN"' : '';
  const bodyPart = body ? ` -H "Content-Type: application/json" -d ${JSON.stringify(body)}` : '';
  return `curl -fsS -X ${method} "${fullUrl}"${tokenPart}${bodyPart}`;
}

function EndpointCard({ ep, token }: { ep: Endpoint; token: string | null }) {
  const [pathVals, setPathVals] = useState<Record<string, string>>(() =>
    Object.fromEntries((ep.pathParams ?? []).map((p) => [p.name, String(p.default ?? '')])),
  );
  const [queryVals, setQueryVals] = useState<Record<string, string>>(() =>
    Object.fromEntries((ep.queryParams ?? []).map((p) => [p.name, String(p.default ?? '')])),
  );
  const [body, setBody] = useState<string>(() =>
    ep.bodyExample ? JSON.stringify(ep.bodyExample, null, 2) : '',
  );
  const [result, setResult] = useState<CallResult>({ status: 'idle' });
  const [copyOk, setCopyOk] = useState(false);

  const { path: resolvedPath, missing } = substitutePath(ep.path, pathVals);
  const qs = buildQueryString(ep.queryParams, queryVals);
  const fullUrl = `${API_URL}${resolvedPath}${qs}`;
  const needsAuth = ep.path.startsWith('/api/admin/') || ep.path.startsWith('/api/auth/');

  const onTry = async () => {
    if (missing) {
      setResult({ status: 'error', message: `Missing required path param: ${missing}` });
      return;
    }
    if (needsAuth && !token) {
      setResult({ status: 'error', message: 'No Supabase access token — sign in first.' });
      return;
    }
    setResult({ status: 'running' });
    const t0 = performance.now();
    try {
      const init: RequestInit = {
        method: ep.method,
        headers: {
          ...(needsAuth && token ? { Authorization: `Bearer ${token}` } : {}),
          ...(body && ['POST', 'PUT', 'PATCH'].includes(ep.method)
            ? { 'Content-Type': 'application/json' }
            : {}),
        },
        ...(body && ['POST', 'PUT', 'PATCH'].includes(ep.method) ? { body } : {}),
      };
      const r = await fetch(fullUrl, init);
      const dur = Math.round(performance.now() - t0);
      let parsed: unknown;
      const text = await r.text();
      try {
        parsed = text.length > 0 ? JSON.parse(text) : null;
      } catch {
        parsed = text;
      }
      const headerObj: Record<string, string> = {};
      r.headers.forEach((v, k) => { headerObj[k] = v; });
      setResult({ status: 'done', statusCode: r.status, durationMs: dur, body: parsed, headers: headerObj });
    } catch (e) {
      setResult({ status: 'error', message: e instanceof Error ? e.message : String(e) });
    }
  };

  const onCopyCurl = async () => {
    const curl = buildCurl(ep.method, fullUrl, needsAuth, body || undefined);
    try {
      await navigator.clipboard.writeText(curl);
      setCopyOk(true);
      setTimeout(() => setCopyOk(false), 1500);
    } catch {
      // Some browsers block clipboard outside HTTPS — fall back to a
      // visible textarea the user can copy manually.
      const ta = document.createElement('textarea');
      ta.value = curl;
      document.body.appendChild(ta);
      ta.select();
      try { document.execCommand('copy'); setCopyOk(true); setTimeout(() => setCopyOk(false), 1500); } catch {}
      document.body.removeChild(ta);
    }
  };

  const methodColor =
    ep.method === 'GET' ? 'bg-emerald-500/15 text-emerald-300 border-emerald-500/30'
    : ep.method === 'POST' ? 'bg-indigo-500/15 text-indigo-300 border-indigo-500/30'
    : ep.method === 'PUT' ? 'bg-amber-500/15 text-amber-300 border-amber-500/30'
    : ep.method === 'DELETE' ? 'bg-rose-500/15 text-rose-300 border-rose-500/30'
    : 'bg-gray-500/15 text-gray-300 border-gray-500/30';

  return (
    <div className="bg-[#151821] border border-gray-800/40 rounded-xl">
      <div className="px-5 py-3 border-b border-gray-800/40 flex items-center gap-3 flex-wrap">
        <span className={`inline-flex items-center text-[10px] uppercase tracking-wider px-2 py-0.5 rounded border font-mono ${methodColor}`}>
          {ep.method}
        </span>
        <span className="font-mono text-sm text-gray-200">{ep.path}</span>
        {needsAuth && <span className="text-[10px] uppercase tracking-wider text-amber-400">admin</span>}
        <div className="ml-auto flex items-center gap-2">
          <button
            type="button"
            onClick={onCopyCurl}
            className="text-xs px-3 py-1 rounded-lg border border-gray-700 hover:border-gray-500 text-gray-300 transition-colors"
            title="Copy curl command (uses $TOKEN env var)"
          >
            {copyOk ? '✓ Copied' : 'Copy as curl'}
          </button>
          <button
            type="button"
            onClick={onTry}
            disabled={result.status === 'running'}
            className="text-xs px-3 py-1 rounded-lg bg-indigo-600 hover:bg-indigo-500 disabled:opacity-50 text-white transition-colors"
          >
            {result.status === 'running' ? 'Running…' : 'Try it'}
          </button>
        </div>
      </div>

      <div className="px-5 py-3 text-xs text-gray-400">{ep.desc}</div>

      {(ep.pathParams || ep.queryParams || ep.bodyExample) && (
        <div className="px-5 pb-3 space-y-2">
          {ep.pathParams && ep.pathParams.length > 0 && (
            <ParamGroup
              label="Path params"
              params={ep.pathParams}
              values={pathVals}
              onChange={setPathVals}
            />
          )}
          {ep.queryParams && ep.queryParams.length > 0 && (
            <ParamGroup
              label="Query params"
              params={ep.queryParams}
              values={queryVals}
              onChange={setQueryVals}
            />
          )}
          {ep.bodyExample && (
            <div>
              <div className="text-[10px] uppercase tracking-wider text-gray-500 mb-1">Request body (JSON)</div>
              <textarea
                value={body}
                onChange={(e) => setBody(e.target.value)}
                spellCheck={false}
                rows={6}
                className="w-full bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 font-mono text-[11px] text-gray-200 focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 focus:outline-none"
              />
            </div>
          )}
        </div>
      )}

      {/* Resolved URL — clickable to copy. Helpful when the user wants
          to grab the exact URL the Try button would hit. */}
      <div className="px-5 pb-3 text-[10px] text-gray-500 font-mono break-all">
        <span className="text-gray-600">URL: </span>{fullUrl}
      </div>

      {result.status !== 'idle' && (
        <ResponsePanel result={result} />
      )}
    </div>
  );
}

function ParamGroup({
  label,
  params,
  values,
  onChange,
}: {
  label: string;
  params: Param[];
  values: Record<string, string>;
  onChange: (next: Record<string, string>) => void;
}) {
  return (
    <div>
      <div className="text-[10px] uppercase tracking-wider text-gray-500 mb-1">{label}</div>
      <div className="grid gap-2 sm:grid-cols-2 md:grid-cols-3">
        {params.map((p) => (
          <label key={p.name} className="block">
            <div className="text-[11px] text-gray-400 mb-0.5">
              {p.name}
              {p.required && <span className="text-rose-400"> *</span>}
              <span className="text-gray-600 ml-1">{p.type}</span>
            </div>
            <input
              type={p.type === 'integer' ? 'number' : 'text'}
              value={values[p.name] ?? ''}
              onChange={(e) => onChange({ ...values, [p.name]: e.target.value })}
              placeholder={p.default != null ? String(p.default) : ''}
              className="w-full bg-[#0f1117] border border-gray-700 rounded-lg px-2.5 py-1 text-xs text-gray-200 font-mono focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 focus:outline-none"
            />
            {p.hint && <div className="text-[10px] text-gray-600 mt-0.5">{p.hint}</div>}
          </label>
        ))}
      </div>
    </div>
  );
}

function ResponsePanel({ result }: { result: CallResult }) {
  if (result.status === 'idle' || result.status === 'running') return null;
  if (result.status === 'error') {
    return (
      <div className="px-5 pb-4">
        <div className="bg-rose-500/10 border border-rose-500/20 rounded-lg px-3 py-2 text-xs text-rose-300 font-mono whitespace-pre-wrap">
          {result.message}
        </div>
      </div>
    );
  }
  const statusColor =
    result.statusCode >= 200 && result.statusCode < 300 ? 'text-emerald-400'
    : result.statusCode >= 400 ? 'text-rose-400'
    : 'text-amber-400';
  const pretty = (() => {
    if (result.body === null || result.body === undefined) return '(empty body)';
    if (typeof result.body === 'string') return result.body;
    try {
      return JSON.stringify(result.body, null, 2);
    } catch {
      return String(result.body);
    }
  })();
  return (
    <div className="px-5 pb-4 space-y-2">
      <div className="flex items-center gap-3 text-xs">
        <span className={`font-mono font-medium ${statusColor}`}>{result.statusCode}</span>
        <span className="text-gray-500 font-mono">{result.durationMs}ms</span>
        <span className="text-gray-600 font-mono truncate">
          {result.headers['content-type'] ?? ''}
        </span>
      </div>
      <pre className="bg-[#0f1117] border border-gray-800/60 rounded-lg px-3 py-2 text-[11px] font-mono text-gray-200 whitespace-pre-wrap max-h-96 overflow-auto">
        {pretty}
      </pre>
    </div>
  );
}

export default function ApiExplorer() {
  const [token, setToken] = useState<string | null>(null);
  const [email, setEmail] = useState<string | null>(null);
  const [role, setRole] = useState<string | null>(null);

  useEffect(() => {
    const supabase = createClient();
    let cancelled = false;
    (async () => {
      const { data: { session } } = await supabase.auth.getSession();
      if (cancelled) return;
      setToken(session?.access_token ?? null);
      const { data: { user } } = await supabase.auth.getUser();
      if (cancelled) return;
      setEmail(user?.email ?? null);
      const meta = (user?.app_metadata ?? {}) as { role?: string };
      setRole(meta.role ?? 'user');
    })();
    // Refresh on auth-state changes so a token-refresh while the user is
    // on this page doesn't leave Try buttons firing stale tokens.
    const { data: { subscription } } = supabase.auth.onAuthStateChange((_event, session) => {
      setToken(session?.access_token ?? null);
    });
    return () => { cancelled = true; subscription.unsubscribe(); };
  }, []);

  const grouped = useMemo(() => {
    const out = new Map<string, Endpoint[]>();
    for (const ep of ENDPOINTS) {
      const arr = out.get(ep.group) ?? [];
      arr.push(ep);
      out.set(ep.group, arr);
    }
    return Array.from(out.entries());
  }, []);

  return (
    <div className="min-h-screen bg-[#0f1117] text-gray-200">
      <div className="px-8 py-5 border-b border-gray-800/40">
        <h1 className="text-xl font-semibold text-white">API</h1>
        <p className="text-sm text-gray-500 mt-1">
          Interactive endpoint explorer. Each card hits the real backend with your current Supabase session — same auth your IBKR script would use. Copy any request as a curl command for scripts.
        </p>
      </div>

      <div className="px-8 py-6 space-y-6 max-w-5xl">
        {/* Auth status banner — admin endpoints need a real session. */}
        <div className="bg-[#151821] border border-gray-800/40 rounded-xl px-5 py-3 flex items-center gap-3 flex-wrap">
          <span className="text-xs text-gray-500">Auth</span>
          {token ? (
            <>
              <span className="text-xs text-emerald-400">✓ Signed in</span>
              <span className="text-xs text-gray-400 font-mono">{email}</span>
              <span className="text-[10px] uppercase tracking-wider text-indigo-400">{role ?? 'user'}</span>
              <span className="text-[10px] text-gray-600 font-mono ml-auto">token: {token.slice(0, 12)}…{token.slice(-8)}</span>
            </>
          ) : (
            <span className="text-xs text-rose-400">
              Not signed in — admin endpoints will return 401. Sign in via the sidebar first.
            </span>
          )}
        </div>

        {grouped.map(([group, eps]) => (
          <section key={group} className="space-y-3">
            <h2 className="text-xs uppercase tracking-wider text-gray-500 px-1">{group}</h2>
            <div className="space-y-3">
              {eps.map((ep) => (
                <EndpointCard key={ep.id} ep={ep} token={token} />
              ))}
            </div>
          </section>
        ))}

        <div className="text-[11px] text-gray-500 leading-relaxed pt-3 border-t border-gray-800/40">
          <p>
            The Try button uses your current Supabase access token directly. Tokens expire ~1h after sign-in;
            if you see 401s after the page has been open for a while, navigate away and back to refresh.
          </p>
          <p className="mt-1">
            For external scripts (IBKR rebalancer, monitoring crons), hit{' '}
            <span className="font-mono text-gray-400">{`POST ${process.env.NEXT_PUBLIC_SUPABASE_URL ?? 'https://<supabase>'}/auth/v1/token?grant_type=password`}</span>{' '}
            with <span className="font-mono text-gray-400">{`{email, password}`}</span> to mint a token, then use it as
            <span className="font-mono text-gray-400"> Authorization: Bearer …</span> on the admin endpoints. See CLAUDE.md for full curl examples.
          </p>
        </div>
      </div>
    </div>
  );
}
