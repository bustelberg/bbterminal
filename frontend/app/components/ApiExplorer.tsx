'use client';

import { useEffect, useMemo, useState } from 'react';
import { createClient } from '../../lib/supabase/client';

import { API_URL } from '../../lib/apiUrl';

/** Catalog entry for one endpoint card. New endpoints land in one
 * place — auto-discovered from FastAPI's `/openapi.json` on mount, so
 * adding a new `@router.get(...)` on the backend is enough. The
 * MANUAL_OVERRIDES map below lets us enrich descriptions / set
 * defaults / hide endpoints when the auto-derived metadata isn't
 * quite right. */
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

// ─── OpenAPI → Endpoint transform ──────────────────────────────────

type OpenApiParam = {
  name: string;
  in: 'path' | 'query' | 'header';
  required?: boolean;
  description?: string;
  schema?: { type?: string; default?: unknown };
};

type OpenApiOperation = {
  tags?: string[];
  summary?: string;
  description?: string;
  operationId?: string;
  parameters?: OpenApiParam[];
  requestBody?: unknown;
};

type OpenApiSpec = {
  paths: Record<string, Partial<Record<'get' | 'post' | 'put' | 'delete' | 'patch', OpenApiOperation>>>;
};

/** Per-endpoint enrichment merged onto the auto-derived card. Keys are
 * `METHOD path`. Use this for: tighter descriptions when the docstring
 * is too long or too short; default values for params that don't have
 * one in the schema; sample request bodies for POST endpoints; hide
 * endpoints we never want to show in the UI. */
const MANUAL_OVERRIDES: Record<string, Partial<Endpoint> & { hide?: boolean }> = {
  'GET /api/admin/gurufocus-probe': {
    queryParams: [
      { name: 'symbol', type: 'string', default: 'AAPL', hint: 'GuruFocus symbol, e.g. "AAPL" or "XAMS:ABN"' },
      { name: 'endpoint', type: 'string', default: 'price', hint: 'price | financials | analyst_estimate | forward_pe_ratio' },
    ],
  },
  // X-Cron-Secret protected — not callable from the UI.
  'POST /api/ingest/scheduled-refresh/cron': { hide: true },
  'POST /api/momentum/current-picks/cron': { hide: true },
};

/** Display allow-list — scopes this explorer to the EXTERNAL admin API
 * (the IBKR buy flow), the only endpoints meant to be called from outside
 * the web app. The other ~129 endpoints are the app's own internal API
 * (every page calls them); they stay fully functional but are hidden here
 * so /api reads as the external-API console. To surface another endpoint,
 * add its `METHOD path`. An EMPTY set falls back to showing everything. */
const VISIBLE_ENDPOINTS = new Set<string>([
  'GET /api/admin/schedules',
  'GET /api/admin/schedules/{strategy_id}',
  'GET /api/admin/health',
]);

/** Title-case a tag for the group header (e.g. "admin" → "Admin",
 * "index-universe" → "Index Universe"). Falls back to the raw tag. */
function _formatGroup(tag: string): string {
  return tag
    .split(/[-_ ]+/)
    .filter(Boolean)
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join(' ') || tag;
}

function _mapOpenApiParam(p: OpenApiParam): Param {
  const t = p.schema?.type;
  return {
    name: p.name,
    type: t === 'integer' || t === 'number' ? 'integer' : 'string',
    default: p.schema?.default as string | number | undefined,
    required: p.required,
    hint: p.description,
  };
}

function _endpointFromOperation(
  path: string,
  method: string,
  op: OpenApiOperation,
): Endpoint {
  const tag = (op.tags && op.tags[0]) || 'other';
  const group = _formatGroup(tag);
  const id = op.operationId || `${method.toLowerCase()}-${path.replace(/[^\w]+/g, '-')}`;
  const pathParams: Param[] = [];
  const queryParams: Param[] = [];
  for (const p of op.parameters ?? []) {
    // The auth middleware injects `authorization: Header(...)` into
    // every protected route — that's not a user-facing param, skip.
    if (p.name === 'authorization' && p.in === 'header') continue;
    const mapped = _mapOpenApiParam(p);
    if (p.in === 'path') pathParams.push(mapped);
    else if (p.in === 'query') queryParams.push(mapped);
  }
  return {
    id,
    group,
    method: method.toUpperCase() as Endpoint['method'],
    path,
    desc: op.description?.trim() || op.summary?.trim() || '',
    pathParams: pathParams.length > 0 ? pathParams : undefined,
    queryParams: queryParams.length > 0 ? queryParams : undefined,
  };
}

/** Walk an OpenAPI spec → Endpoint[], applying MANUAL_OVERRIDES and
 * filtering out hidden endpoints. Path order within a group is
 * alphabetical; group order matches the GROUP_ORDER constant below
 * (Admin first, System last). */
function endpointsFromOpenApi(spec: OpenApiSpec): Endpoint[] {
  const out: Endpoint[] = [];
  for (const [path, ops] of Object.entries(spec.paths)) {
    for (const [method, op] of Object.entries(ops)) {
      if (!op) continue;
      const ep = _endpointFromOperation(path, method, op);
      const overrideKey = `${ep.method} ${ep.path}`;
      // Scope to the external-API allow-list (when non-empty).
      if (VISIBLE_ENDPOINTS.size > 0 && !VISIBLE_ENDPOINTS.has(overrideKey)) continue;
      const override = MANUAL_OVERRIDES[overrideKey];
      if (override?.hide) continue;
      out.push({ ...ep, ...override });
    }
  }
  return out;
}

/** Pinned group order so the dashboard reads top-down by use-case
 * (admin first because that's the dominant audience for this page). */
const GROUP_ORDER = [
  'Admin', 'Auth', 'Companies', 'Earnings', 'Momentum',
  'Universe', 'Index Universe', 'Universe Templates',
  'Schedule', 'Ingest', 'Fx', 'Fees', 'Benchmarks',
  'Indicators', 'Longequity', 'Leonteq', 'Airs', 'System', 'Other',
];

function _groupRank(g: string): number {
  const i = GROUP_ORDER.indexOf(g);
  return i < 0 ? GROUP_ORDER.length : i;
}


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
          // Every /api/* endpoint now requires a valid JWT, so always attach
          // the session token when we have one (not just admin endpoints).
          ...(token ? { Authorization: `Bearer ${token}` } : {}),
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
    ep.method === 'GET' ? 'bg-pos-500/15 text-pos-300 border-pos-500/30'
    : ep.method === 'POST' ? 'bg-accent-500/15 text-accent-300 border-accent-500/30'
    : ep.method === 'PUT' ? 'bg-warn-500/15 text-warn-300 border-warn-500/30'
    : ep.method === 'DELETE' ? 'bg-neg-500/15 text-neg-300 border-neg-500/30'
    : 'bg-neutral-500/15 text-fg-soft border-neutral-500/30';

  return (
    <div className="bg-card border border-neutral-800/40 rounded-xl">
      <div className="px-5 py-3 border-b border-neutral-800/40 flex items-center gap-3 flex-wrap">
        <span className={`inline-flex items-center text-[10px] uppercase tracking-wider px-2 py-0.5 rounded border font-mono ${methodColor}`}>
          {ep.method}
        </span>
        <span className="font-mono text-sm text-fg">{ep.path}</span>
        {needsAuth && <span className="text-[10px] uppercase tracking-wider text-warn-400">admin</span>}
        <div className="ml-auto flex items-center gap-2">
          <button
            type="button"
            onClick={onCopyCurl}
            className="text-xs px-3 py-1 rounded-lg border border-neutral-700 hover:border-neutral-500 text-fg-soft transition-colors"
            title="Copy curl command (uses $TOKEN env var)"
          >
            {copyOk ? '✓ Copied' : 'Copy as curl'}
          </button>
          <button
            type="button"
            onClick={onTry}
            disabled={result.status === 'running'}
            className="text-xs px-3 py-1 rounded-lg bg-accent-600 hover:bg-accent-500 disabled:opacity-50 text-fg-strong transition-colors"
          >
            {result.status === 'running' ? 'Running…' : 'Try it'}
          </button>
        </div>
      </div>

      <div className="px-5 py-3 text-xs text-fg-muted">{ep.desc}</div>

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
              <div className="text-[10px] uppercase tracking-wider text-fg-subtle mb-1">Request body (JSON)</div>
              <textarea
                value={body}
                onChange={(e) => setBody(e.target.value)}
                spellCheck={false}
                rows={6}
                className="w-full bg-page border border-neutral-700 rounded-lg px-3 py-2 font-mono text-[11px] text-fg focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 focus:outline-none"
              />
            </div>
          )}
        </div>
      )}

      {/* Resolved URL — clickable to copy. Helpful when the user wants
          to grab the exact URL the Try button would hit. */}
      <div className="px-5 pb-3 text-[10px] text-fg-subtle font-mono break-all">
        <span className="text-fg-faint">URL: </span>{fullUrl}
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
      <div className="text-[10px] uppercase tracking-wider text-fg-subtle mb-1">{label}</div>
      <div className="grid gap-2 sm:grid-cols-2 md:grid-cols-3">
        {params.map((p) => (
          <label key={p.name} className="block">
            <div className="text-[11px] text-fg-muted mb-0.5">
              {p.name}
              {p.required && <span className="text-neg-400"> *</span>}
              <span className="text-fg-faint ml-1">{p.type}</span>
            </div>
            <input
              type={p.type === 'integer' ? 'number' : 'text'}
              value={values[p.name] ?? ''}
              onChange={(e) => onChange({ ...values, [p.name]: e.target.value })}
              placeholder={p.default != null ? String(p.default) : ''}
              className="w-full bg-page border border-neutral-700 rounded-lg px-2.5 py-1 text-xs text-fg font-mono focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 focus:outline-none"
            />
            {p.hint && <div className="text-[10px] text-fg-faint mt-0.5">{p.hint}</div>}
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
        <div className="bg-neg-500/10 border border-neg-500/20 rounded-lg px-3 py-2 text-xs text-neg-300 font-mono whitespace-pre-wrap">
          {result.message}
        </div>
      </div>
    );
  }
  const statusColor =
    result.statusCode >= 200 && result.statusCode < 300 ? 'text-pos-400'
    : result.statusCode >= 400 ? 'text-neg-400'
    : 'text-warn-400';
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
        <span className="text-fg-subtle font-mono">{result.durationMs}ms</span>
        <span className="text-fg-faint font-mono truncate">
          {result.headers['content-type'] ?? ''}
        </span>
      </div>
      <pre className="bg-page border border-neutral-800/60 rounded-lg px-3 py-2 text-[11px] font-mono text-fg whitespace-pre-wrap max-h-96 overflow-auto">
        {pretty}
      </pre>
    </div>
  );
}

export default function ApiExplorer() {
  const [token, setToken] = useState<string | null>(null);
  const [email, setEmail] = useState<string | null>(null);
  const [role, setRole] = useState<string | null>(null);
  const [endpoints, setEndpoints] = useState<Endpoint[] | null>(null);
  const [specError, setSpecError] = useState<string | null>(null);

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

  // Fetch the FastAPI OpenAPI spec on mount → transform → display.
  // Auto-discovery means new `@router.get(...)` definitions on the
  // backend show up as cards here without any per-endpoint UI work.
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const resp = await fetch(`${API_URL}/openapi.json`);
        if (!resp.ok) throw new Error(`/openapi.json returned ${resp.status}`);
        const spec = (await resp.json()) as OpenApiSpec;
        if (cancelled) return;
        setEndpoints(endpointsFromOpenApi(spec));
      } catch (e) {
        if (!cancelled) setSpecError(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { cancelled = true; };
  }, []);

  const grouped = useMemo(() => {
    if (!endpoints) return [];
    const out = new Map<string, Endpoint[]>();
    for (const ep of endpoints) {
      const arr = out.get(ep.group) ?? [];
      arr.push(ep);
      out.set(ep.group, arr);
    }
    // Sort endpoints inside each group by path for predictable order.
    for (const arr of out.values()) {
      arr.sort((a, b) => a.path.localeCompare(b.path) || a.method.localeCompare(b.method));
    }
    // Sort groups by GROUP_ORDER ranking.
    return Array.from(out.entries()).sort((a, b) => _groupRank(a[0]) - _groupRank(b[0]));
  }, [endpoints]);

  return (
    <div className="min-h-screen bg-page text-fg">
      <div className="px-8 py-5 border-b border-neutral-800/40">
        <h1 className="text-xl font-semibold text-fg-strong">API</h1>
        <p className="text-sm text-fg-subtle mt-1">
          Interactive endpoint explorer. Each card hits the real backend with your current Supabase session — same auth your IBKR script would use. Copy any request as a curl command for scripts.
        </p>
      </div>

      <div className="px-8 py-6 space-y-6 max-w-5xl">
        {/* Auth status banner — admin endpoints need a real session. */}
        <div className="bg-card border border-neutral-800/40 rounded-xl px-5 py-3 flex items-center gap-3 flex-wrap">
          <span className="text-xs text-fg-subtle">Auth</span>
          {token ? (
            <>
              <span className="text-xs text-pos-400">✓ Signed in</span>
              <span className="text-xs text-fg-muted font-mono">{email}</span>
              <span className="text-[10px] uppercase tracking-wider text-accent-400">{role ?? 'user'}</span>
              <span className="text-[10px] text-fg-faint font-mono ml-auto">token: {token.slice(0, 12)}…{token.slice(-8)}</span>
            </>
          ) : (
            <span className="text-xs text-neg-400">
              Not signed in — admin endpoints will return 401. Sign in via the sidebar first.
            </span>
          )}
        </div>

        {/* Loading / error state for the openapi.json fetch. The cards
            stream in once the spec lands. */}
        {endpoints === null && !specError && (
          <div className="bg-card border border-neutral-800/40 rounded-xl px-5 py-4 text-sm text-fg-subtle">
            Loading endpoint catalog from <span className="font-mono text-fg-muted">{API_URL}/openapi.json</span>…
          </div>
        )}
        {specError && (
          <div className="bg-neg-500/10 border border-neg-500/20 rounded-xl px-5 py-4 text-sm text-neg-300">
            Couldn&apos;t load the endpoint catalog: <span className="font-mono">{specError}</span>. The backend may be down or unreachable.
          </div>
        )}

        {grouped.map(([group, eps]) => (
          <section key={group} className="space-y-3">
            <h2 className="text-xs uppercase tracking-wider text-fg-subtle px-1 flex items-baseline gap-2">
              <span>{group}</span>
              <span className="text-[10px] text-fg-faint font-mono normal-case">{eps.length} endpoint{eps.length === 1 ? '' : 's'}</span>
            </h2>
            <div className="space-y-3">
              {eps.map((ep) => (
                <EndpointCard key={ep.id} ep={ep} token={token} />
              ))}
            </div>
          </section>
        ))}

        <div className="text-[11px] text-fg-subtle leading-relaxed pt-3 border-t border-neutral-800/40">
          <p>
            The Try button uses your current Supabase access token directly. Tokens expire ~1h after sign-in;
            if you see 401s after the page has been open for a while, navigate away and back to refresh.
          </p>
          <p className="mt-1">
            For external scripts (IBKR rebalancer, monitoring crons), hit{' '}
            <span className="font-mono text-fg-muted">{`POST ${process.env.NEXT_PUBLIC_SUPABASE_URL ?? 'https://<supabase>'}/auth/v1/token?grant_type=password`}</span>{' '}
            with <span className="font-mono text-fg-muted">{`{email, password}`}</span> to mint a token, then use it as
            <span className="font-mono text-fg-muted"> Authorization: Bearer …</span> on the admin endpoints. See CLAUDE.md for full curl examples.
          </p>
        </div>
      </div>
    </div>
  );
}
