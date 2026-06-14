'use client';

import { useState } from 'react';

// Runtime values from the build's NEXT_PUBLIC_* env vars. On Vercel
// these get the actual deployed URLs; in local dev they fall back to
// the localhost defaults. Either way the rendered docs match the
// environment the admin is actually viewing — paste the env example
// into a sibling repo's `.env` and the URLs are already correct.
//
// SUPABASE_ANON_KEY is PUBLIC by design (it's already embedded in the
// JS bundle the user just loaded), so showing it inline doesn't leak
// anything that isn't already in the browser.
const RUNTIME = {
  apiUrl: process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000',
  supabaseUrl: process.env.NEXT_PUBLIC_SUPABASE_URL || 'http://127.0.0.1:54321',
  supabaseAnonKey:
    process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ||
    '<run `npx supabase status` and paste the publishable key here>',
};

/** Code block with a clipboard-copy button on hover. The actual code
 * lives here as a string rather than an external file because:
 *   - the snippet IS the docs the user is reading
 *   - we want copy-to-clipboard to give exactly what's rendered
 *   - keeping it inline means edits don't require synchronizing two
 *     copies (the rendered markdown and a separate raw asset). */
function CodeBlock({ code, lang }: { code: string; lang?: string }) {
  const [copied, setCopied] = useState(false);
  const onCopy = async () => {
    try {
      await navigator.clipboard.writeText(code);
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    } catch {
      // Some browsers block clipboard outside HTTPS — fall back to the
      // textarea trick. Same dance as the API explorer page.
      const ta = document.createElement('textarea');
      ta.value = code;
      document.body.appendChild(ta);
      ta.select();
      try { document.execCommand('copy'); setCopied(true); setTimeout(() => setCopied(false), 1500); } catch {}
      document.body.removeChild(ta);
    }
  };
  return (
    <div className="relative group">
      <button
        type="button"
        onClick={onCopy}
        className="absolute top-2 right-2 text-[10px] uppercase tracking-wider px-2 py-1 rounded border border-neutral-700 bg-card text-fg-muted hover:text-fg hover:border-neutral-500 transition-colors opacity-0 group-hover:opacity-100 focus:opacity-100"
      >
        {copied ? '✓ Copied' : 'Copy'}
      </button>
      {lang && (
        <span className="absolute top-2 left-2 text-[9px] uppercase tracking-wider text-fg-faint font-mono pointer-events-none">
          {lang}
        </span>
      )}
      <pre className="bg-page border border-neutral-800/60 rounded-lg px-4 py-3 pt-7 text-[11.5px] font-mono text-fg overflow-auto leading-relaxed">
{code}
      </pre>
    </div>
  );
}

function Section({
  id,
  title,
  children,
}: {
  id: string;
  title: string;
  children: React.ReactNode;
}) {
  return (
    <section id={id} className="scroll-mt-8 space-y-3">
      <h2 className="text-lg font-semibold text-fg-strong border-b border-neutral-800/40 pb-2">
        <a href={`#${id}`} className="hover:text-accent-300 transition-colors">
          {title}
        </a>
      </h2>
      <div className="space-y-3 text-sm text-fg-soft leading-relaxed">{children}</div>
    </section>
  );
}

// ─── Inline source for the Python client ─────────────────────────────
//
// Kept as a string literal so:
//   1. Copy button gives a byte-identical paste
//   2. Edits to the docs are a single file change
//   3. No build-time asset import to wire up
const PYTHON_CLIENT_SOURCE = String.raw`"""Minimal admin-API client for bbterminal.

Drop into your IBKR rebalancer repo. Reads creds from env vars by
default so no secrets in source.
"""
from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from typing import Any

import requests


class BBTerminalError(Exception):
    """Wrapped HTTP error with the response body for actionable messages."""


@dataclass
class BBTerminalClient:
    base_url: str                # e.g. "https://bbterminal-api.railway.app"
    supabase_url: str            # e.g. "https://abc.supabase.co"
    supabase_anon_key: str
    email: str
    password: str
    timeout: int = 30
    # Refresh ~60 s before expiry so we don't race the boundary.
    refresh_buffer_seconds: int = 60

    _access_token: str | None = field(default=None, init=False, repr=False)
    _refresh_token: str | None = field(default=None, init=False, repr=False)
    _expires_at: float | None = field(default=None, init=False, repr=False)

    # ─── Auth lifecycle ──────────────────────────────────────────

    def _login(self) -> None:
        """Trade email/password for a fresh access + refresh token pair."""
        r = requests.post(
            f"{self.supabase_url}/auth/v1/token",
            params={"grant_type": "password"},
            headers={
                "apikey": self.supabase_anon_key,
                "Content-Type": "application/json",
            },
            json={"email": self.email, "password": self.password},
            timeout=self.timeout,
        )
        if r.status_code >= 400:
            raise BBTerminalError(f"Login failed: {r.status_code} {r.text[:200]}")
        self._apply_token_response(r.json())

    def _refresh(self) -> None:
        """Refresh-token swap. Falls back to full login if the refresh
        token is revoked or expired."""
        if not self._refresh_token:
            self._login()
            return
        r = requests.post(
            f"{self.supabase_url}/auth/v1/token",
            params={"grant_type": "refresh_token"},
            headers={
                "apikey": self.supabase_anon_key,
                "Content-Type": "application/json",
            },
            json={"refresh_token": self._refresh_token},
            timeout=self.timeout,
        )
        if r.status_code >= 400:
            self._login()
            return
        self._apply_token_response(r.json())

    def _apply_token_response(self, data: dict) -> None:
        self._access_token = data["access_token"]
        self._refresh_token = data.get("refresh_token", self._refresh_token)
        # Prefer absolute "expires_at" (unix seconds); fall back to offset.
        self._expires_at = data.get("expires_at") or (
            time.time() + data.get("expires_in", 3600)
        )

    def _ensure_token(self) -> str:
        """Return a valid access token, refreshing transparently when
        the current one is missing or near expiry."""
        if not self._access_token or not self._expires_at:
            self._login()
        elif time.time() >= self._expires_at - self.refresh_buffer_seconds:
            self._refresh()
        assert self._access_token is not None
        return self._access_token

    # ─── HTTP plumbing ───────────────────────────────────────────

    def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        """Authenticated request with one auto-retry on 401 (token
        race / sudden revocation)."""
        url = f"{self.base_url}{path}"
        last_status = None
        for attempt in range(2):
            token = self._ensure_token()
            headers = {**(kwargs.pop("headers", {}) or {}),
                       "Authorization": f"Bearer {token}"}
            r = requests.request(method, url, headers=headers,
                                 timeout=self.timeout, **kwargs)
            last_status = r.status_code
            if r.status_code == 401 and attempt == 0:
                # Force re-login next loop iteration.
                self._access_token = None
                self._expires_at = None
                continue
            if r.status_code >= 400:
                raise BBTerminalError(
                    f"{method} {path} -> {r.status_code} {r.text[:300]}"
                )
            if r.headers.get("content-type", "").startswith("application/json"):
                return r.json()
            return r.text
        raise BBTerminalError(f"Auth retry exhausted (last status {last_status})")

    # ─── Convenience wrappers per admin endpoint ─────────────────

    def whoami(self) -> dict:
        """Quick startup sanity check — returns {id, email, role}.
        Bail fast if role != 'admin'."""
        return self._request("GET", "/api/auth/me")

    def schedules(self, enabled_only: bool = True) -> list[dict]:
        """List every scheduled strategy + its next rebalance date
        (lightweight; no holdings). Use it to find the strategy_id to
        pass to schedule()."""
        return self._request("GET", "/api/admin/schedules",
                             params={"enabled_only": str(enabled_only).lower()})

    def schedule(self, strategy_id: int) -> dict:
        """One strategy's CURRENT holdings — order-ready. Each holding
        carries ticker, exchange, country, currency, company_name, side,
        target_weight, score, entry_price_local, entry_price_eur. Also
        returns as_of_date / latest_price_date — gate on those (or
        health()) so you never trade on stale data."""
        return self._request("GET", f"/api/admin/schedules/{strategy_id}")

    def health(self) -> dict:
        """Composite go/no-go. Gate trades on is_healthy_strict."""
        return self._request("GET", "/api/admin/health")

    # ─── Factory ─────────────────────────────────────────────────

    @classmethod
    def from_env(cls) -> "BBTerminalClient":
        """Construct from env vars. Useful so the IBKR script never
        embeds the password literally — keep it in .env / your
        secrets manager."""
        required = (
            "BBTERMINAL_URL",
            "SUPABASE_URL",
            "SUPABASE_ANON_KEY",
            "BBTERMINAL_EMAIL",
            "BBTERMINAL_PASSWORD",
        )
        missing = [k for k in required if not os.environ.get(k)]
        if missing:
            raise BBTerminalError(f"Missing env vars: {', '.join(missing)}")
        return cls(
            base_url=os.environ["BBTERMINAL_URL"].rstrip("/"),
            supabase_url=os.environ["SUPABASE_URL"].rstrip("/"),
            supabase_anon_key=os.environ["SUPABASE_ANON_KEY"],
            email=os.environ["BBTERMINAL_EMAIL"],
            password=os.environ["BBTERMINAL_PASSWORD"],
        )
`;

/** Most of the docs include URL/key snippets that should reflect the
 * environment the page was deployed into. We build those strings at
 * module load time so React never has to re-derive them; if the
 * deployment env changes, a new build will produce a new bundle with
 * fresh values. */
const ENV_EXAMPLE = `BBTERMINAL_URL=${RUNTIME.apiUrl}
SUPABASE_URL=${RUNTIME.supabaseUrl}
SUPABASE_ANON_KEY=${RUNTIME.supabaseAnonKey}
BBTERMINAL_EMAIL=you@example.com
BBTERMINAL_PASSWORD=hunter2
`;

const CURL_LOGIN = `SUPABASE_URL="${RUNTIME.supabaseUrl}"
SUPABASE_ANON_KEY="${RUNTIME.supabaseAnonKey}"
EMAIL="you@example.com"
PASSWORD="<your-password>"

TOKEN=$(curl -fsS -X POST \\
  "$SUPABASE_URL/auth/v1/token?grant_type=password" \\
  -H "apikey: $SUPABASE_ANON_KEY" \\
  -H "Content-Type: application/json" \\
  -d "{\\"email\\":\\"$EMAIL\\",\\"password\\":\\"$PASSWORD\\"}" \\
  | jq -r .access_token)

curl -fsS "${RUNTIME.apiUrl}/api/admin/health" \\
  -H "Authorization: Bearer $TOKEN" | jq
`;

const POWERSHELL_LOGIN = `$env:SUPABASE_URL = "${RUNTIME.supabaseUrl}"
$env:SUPABASE_ANON_KEY = "${RUNTIME.supabaseAnonKey}"

$body = @{ email = "you@example.com"; password = "<your-password>" } | ConvertTo-Json
$response = Invoke-RestMethod -Method Post \`
  -Uri "$env:SUPABASE_URL/auth/v1/token?grant_type=password" \`
  -Headers @{ "apikey" = $env:SUPABASE_ANON_KEY; "Content-Type" = "application/json" } \`
  -Body $body

$env:TOKEN = $response.access_token

Invoke-RestMethod -Uri "${RUNTIME.apiUrl}/api/admin/health" \`
  -Headers @{ "Authorization" = "Bearer $env:TOKEN" } | ConvertTo-Json -Depth 10
`;

const USAGE_EXAMPLE = `from bbterminal_client import BBTerminalClient, BBTerminalError

bb = BBTerminalClient.from_env()

# Pre-trade safety gate — bail out if the data is stale or the
# pipeline hasn't run successfully recently.
health = bb.health()
if not health["is_healthy_strict"]:
    raise SystemExit(f"Refusing to trade: {health['problems']}")

# 1. List strategies + their next rebalance dates, pick one.
for s in bb.schedules():
    print(f"  #{s['strategy_id']}  {s['name']}  next rebalance {s['next_rebalance_at']}")

strategy_id = bb.schedules()[0]["strategy_id"]

# 2. Pull that strategy's current holdings — order-ready.
strat = bb.schedule(strategy_id)
print(f"{strat['name']} — {strat['holdings_count']} holdings as of {strat['as_of_date']}")

for h in strat["holdings"]:
    print(
        f"  {h['side'].upper():5}  {h['exchange']:6} {h['ticker']:10}  "
        f"{h['country']}  {h['currency']}  weight={h['target_weight']:.4f}  "
        f"@ {h['entry_price_local']}"
    )
`;

type TocLink = { id: string; label: string };

const TOC: TocLink[] = [
  { id: 'overview', label: 'Overview' },
  { id: 'tiers', label: 'Access tiers' },
  { id: 'quickstart', label: 'Quick start (curl)' },
  { id: 'python-client', label: 'Python client' },
  { id: 'env-vars', label: 'Environment variables' },
  { id: 'usage', label: 'Usage example' },
  { id: 'design', label: 'Why this design' },
  { id: 'extensions', label: 'Optional extensions' },
  { id: 'powershell', label: 'PowerShell (one-off)' },
  { id: 'reference', label: 'Endpoint reference' },
];

export default function Documentation() {
  // Best-effort environment-name guess from the URL hostname. Just a
  // visual cue so the user can tell at a glance whether they're
  // copying dev or prod values. The actual substitution into code
  // blocks doesn't depend on this — it always uses the NEXT_PUBLIC_*
  // vars baked into this build.
  const envLabel = (() => {
    const h = RUNTIME.apiUrl.toLowerCase();
    if (h.includes('localhost') || h.includes('127.0.0.1')) return 'local dev';
    if (h.includes('railway.app') || h.includes('vercel.app')) return 'production';
    return 'this deployment';
  })();

  return (
    <div className="min-h-screen bg-page text-fg">
      <div className="px-8 py-5 border-b border-neutral-800/40">
        <h1 className="text-xl font-semibold text-fg-strong">Documentation</h1>
        <p className="text-sm text-fg-subtle mt-1">
          How to authenticate to the BBTerminal API from your own scripts and which endpoints your account can call.
          Same flow the web app and the IBKR rebalancer use.
        </p>
        <div className="mt-3 inline-flex items-center gap-2 text-xs bg-card border border-neutral-800/40 rounded-lg px-3 py-1.5">
          <span className="text-fg-subtle">URLs in code blocks are filled in from</span>
          <span className="font-mono text-accent-300">{envLabel}</span>
          <span className="text-fg-faint">·</span>
          <span className="font-mono text-fg-muted truncate max-w-[400px]">{RUNTIME.apiUrl}</span>
        </div>
      </div>

      <div className="px-8 py-6 grid gap-8 lg:grid-cols-[200px_1fr] max-w-6xl">
        {/* Sticky TOC */}
        <nav className="lg:sticky lg:top-6 self-start text-xs space-y-1">
          <div className="text-[10px] uppercase tracking-wider text-fg-subtle mb-2">Contents</div>
          {TOC.map((t) => (
            <a
              key={t.id}
              href={`#${t.id}`}
              className="block text-fg-muted hover:text-accent-300 transition-colors py-0.5"
            >
              {t.label}
            </a>
          ))}
        </nav>

        <article className="space-y-10 min-w-0">

          <Section id="overview" title="Overview">
            <p>
              <strong>Every</strong> <code className="text-warn-300">/api/*</code> endpoint requires a Supabase JWT — there is no
              unauthenticated read access. The auth flow is the same for every caller (the web app, an external script, the
              IBKR rebalancer): you exchange your <strong>email + password</strong> for an
              {' '}<code className="text-warn-300">access_token</code>, then pass it as
              {' '}<code className="text-warn-300">Authorization: Bearer …</code> on each call.
            </p>
            <p>
              Tokens are minted by Supabase Auth directly, not by this API — you POST to
              {' '}<code className="text-warn-300">$SUPABASE_URL/auth/v1/token?grant_type=password</code> with the public
              {' '}<code className="text-warn-300">anon</code> key as the <code className="text-warn-300">apikey</code> header.
              Tokens last about 1&nbsp;hour; after that you either re-authenticate or use the returned
              {' '}<code className="text-warn-300">refresh_token</code> (<code className="text-warn-300">grant_type=refresh_token</code>).
            </p>
            <p>
              <strong>What you can call depends on your account&apos;s role</strong> — see <a href="#tiers" className="text-accent-400 hover:underline">Access tiers</a> below.
              The token mechanism is identical regardless of role; only the set of allowed paths differs.
            </p>
            <p>
              For one-off probing, the <a href="/api" className="text-accent-400 hover:underline">/api page</a> in this app does
              all of that for you with Try buttons and copy-as-curl. For an automated script, use the Python client below.
            </p>
          </Section>

          <Section id="tiers" title="Access tiers">
            <p>
              The API gate (<code className="text-warn-300">enforce_api_auth</code>) checks your JWT on every request and
              authorizes by role. A missing/invalid token is <code className="text-warn-300">401</code>; an authenticated
              user reaching beyond their tier is <code className="text-warn-300">403</code>.
            </p>
            <div className="overflow-auto border border-neutral-800/40 rounded-lg">
              <table className="w-full text-xs">
                <thead className="text-fg-subtle text-[10px] uppercase">
                  <tr className="border-b border-neutral-800/40 bg-card">
                    <th className="text-left px-3 py-2 font-medium">Tier</th>
                    <th className="text-left px-3 py-2 font-medium">Token</th>
                    <th className="text-left px-3 py-2 font-medium">Can call</th>
                  </tr>
                </thead>
                <tbody>
                  <tr className="border-b border-neutral-800/20">
                    <td className="px-3 py-2 font-mono text-fg">anonymous</td>
                    <td className="px-3 py-2 text-fg-muted">none</td>
                    <td className="px-3 py-2 text-fg-muted">
                      Only the public tier: <code className="text-warn-300">/api/health</code>, <code className="text-warn-300">/api/hello</code>,
                      and the <code className="text-warn-300">*/cron</code> endpoints (which gate on <code className="text-warn-300">X-Cron-Secret</code>).
                      Everything else → <code className="text-warn-300">401</code>.
                    </td>
                  </tr>
                  <tr className="border-b border-neutral-800/20">
                    <td className="px-3 py-2 font-mono text-fg">user</td>
                    <td className="px-3 py-2 text-fg-muted">any valid JWT</td>
                    <td className="px-3 py-2 text-fg-muted">
                      Reads under <code className="text-warn-300">/api/companies</code>, <code className="text-warn-300">/api/earnings</code>,
                      {' '}<code className="text-warn-300">/api/airs</code>, <code className="text-warn-300">/api/usage</code>; plus the writes
                      {' '}<code className="text-warn-300">POST /api/portfolios/parse</code> and <code className="text-warn-300">POST /api/earnings/&#123;id&#125;/refresh</code>.
                      Anything else → <code className="text-warn-300">403</code>.
                    </td>
                  </tr>
                  <tr className="border-b border-neutral-800/20">
                    <td className="px-3 py-2 font-mono text-fg">admin</td>
                    <td className="px-3 py-2 text-fg-muted"><code className="text-warn-300">app_metadata.role == &apos;admin&apos;</code></td>
                    <td className="px-3 py-2 text-fg-muted">
                      Every <code className="text-warn-300">/api/*</code> endpoint, including the <code className="text-warn-300">/api/admin/*</code> surface
                      documented below.
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
            <p className="text-xs text-fg-subtle">
              <code className="text-warn-300">/api/auth/*</code> is self-authenticating — those endpoints verify your token
              themselves (e.g. <code className="text-warn-300">GET /api/auth/me</code> returns your id, email, and role; use it to
              check which tier you&apos;re in). SSE endpoints need the token as the <code className="text-warn-300">Authorization</code> header,
              so a browser <code className="text-warn-300">EventSource</code> won&apos;t work — read the stream with an HTTP client that sets headers.
            </p>
          </Section>

          <Section id="quickstart" title="Quick start (curl)">
            <p>
              The minimal end-to-end shell flow — exchange password for token, call an endpoint.
            </p>
            <CodeBlock lang="bash" code={CURL_LOGIN} />
            <p className="text-xs text-fg-subtle">
              Replace <code className="text-warn-300">SUPABASE_URL</code> and <code className="text-warn-300">SUPABASE_ANON_KEY</code> with values from{' '}
              <code className="text-warn-300">npx supabase status</code> (local) or your prod env vars.
              Tokens expire after ~1&nbsp;hour; re-run the login command or use <code className="text-warn-300">grant_type=refresh_token</code>.
            </p>
          </Section>

          <Section id="python-client" title="Python client">
            <p>
              Drop <code className="text-warn-300">bbterminal_client.py</code> into your Python repo. One class, sync API,
              handles login + token refresh transparently. Only depends on <code className="text-warn-300">requests</code>.
            </p>
            <CodeBlock lang="python" code={PYTHON_CLIENT_SOURCE} />
          </Section>

          <Section id="env-vars" title="Environment variables">
            <p>
              <code className="text-warn-300">BBTerminalClient.from_env()</code> reads five variables. Keep them in a
              {' '}<code className="text-warn-300">.env</code> file (gitignored) or your secrets manager — never commit the password.
            </p>
            <p className="text-xs text-warn-300/80">
              The URL + anon key below are auto-filled from this deployment — paste straight into a sibling repo&apos;s
              {' '}<code className="text-warn-300">.env</code>.
            </p>
            <CodeBlock lang="env" code={ENV_EXAMPLE} />
            <p className="text-xs text-fg-subtle">
              On the script side, load these with{' '}
              <code className="text-warn-300">python-dotenv</code> (<code className="text-warn-300">from dotenv import load_dotenv; load_dotenv()</code>)
              before constructing the client. The anon key is safe to commit on its own — it&apos;s the same value already shipped in the
              browser JS bundle — but committing the password obviously is not.
            </p>
          </Section>

          <Section id="usage" title="Usage example">
            <p>
              A pre-trade safety gate followed by pulling the target portfolio. Adapt the inner loop to call IBKR&apos;s API.
            </p>
            <CodeBlock lang="python" code={USAGE_EXAMPLE} />
          </Section>

          <Section id="design" title="Why this design">
            <ul className="list-disc list-outside space-y-1.5 pl-5 text-sm">
              <li>
                <strong>One file, one class.</strong> No package boilerplate. Easy to vendor and review.
              </li>
              <li>
                <strong>Sync, not async.</strong> Your rebalancer is probably a sleep-loop or cron — async adds complexity
                without benefit at single-request cadence.
              </li>
              <li>
                <strong>Transparent token lifecycle.</strong> You never think about Bearer tokens. The client refreshes
                ~60&nbsp;s before expiry and retries on 401 once (handles the rare race where the token expired
                between the freshness check and the actual call).
              </li>
              <li>
                <strong>No Supabase SDK dependency.</strong> Just <code className="text-warn-300">requests</code>.
                Adding <code className="text-warn-300">supabase-py</code> would pull in{' '}
                <code className="text-warn-300">gotrue</code>, <code className="text-warn-300">postgrest</code>,{' '}
                <code className="text-warn-300">realtime</code>, etc. that you don&apos;t need for this single-purpose client.
              </li>
              <li>
                <strong><code className="text-warn-300">from_env()</code> factory</strong> keeps the password out of source.
                The constructor still accepts explicit args if you&apos;d rather inject from a secrets manager.
              </li>
            </ul>
          </Section>

          <Section id="extensions" title="Optional extensions">
            <ul className="list-disc list-outside space-y-1.5 pl-5 text-sm">
              <li>
                <strong>Disk-cached token</strong> — so a short-running script doesn&apos;t re-login on every cold start.
                Serialize <code className="text-warn-300">{`{access_token, refresh_token, expires_at}`}</code> to{' '}
                <code className="text-warn-300">~/.cache/bbterminal-token.json</code> after{' '}
                <code className="text-warn-300">_apply_token_response</code> and read it in{' '}
                <code className="text-warn-300">__init__</code>. Use file mode <code className="text-warn-300">0o600</code>.
              </li>
              <li>
                <strong>Retry on 5xx with backoff</strong> — if Railway has a transient blip. Add{' '}
                <code className="text-warn-300">tenacity</code> and decorate <code className="text-warn-300">_request</code>.
              </li>
              <li>
                <strong>Typed responses</strong> — generate Pydantic models from the response shapes if you want
                IDE autocomplete in your IBKR code.
              </li>
            </ul>
          </Section>

          <Section id="powershell" title="PowerShell (one-off)">
            <p>
              For a quick interactive call from PowerShell without leaving a script behind.
            </p>
            <CodeBlock lang="powershell" code={POWERSHELL_LOGIN} />
          </Section>

          <Section id="reference" title="Endpoint reference">
            <p>
              Every admin endpoint at a glance. The{' '}
              <a href="/api" className="text-accent-400 hover:underline">/api page</a> lets you call each one
              interactively; the <code className="text-warn-300">BBTerminalClient</code> above exposes one method per row.
            </p>
            <div className="overflow-auto border border-neutral-800/40 rounded-lg">
              <table className="w-full text-xs">
                <thead className="text-fg-subtle text-[10px] uppercase">
                  <tr className="border-b border-neutral-800/40 bg-card">
                    <th className="text-left px-3 py-2 font-medium">Method</th>
                    <th className="text-left px-3 py-2 font-medium">Path</th>
                    <th className="text-left px-3 py-2 font-medium">Python</th>
                    <th className="text-left px-3 py-2 font-medium">Purpose</th>
                  </tr>
                </thead>
                <tbody>
                  {[
                    ['GET', '/api/admin/schedules', 'bb.schedules()', 'List strategies + each one’s next rebalance date (lightweight).'],
                    ['GET', '/api/admin/schedules/{id}', 'bb.schedule(id)', 'One strategy’s current holdings — order-ready (ticker, exchange, country, currency, weight, side) + as_of_date.'],
                    ['GET', '/api/admin/health', 'bb.health()', 'Composite go/no-go. Gate trades on is_healthy_strict.'],
                    ['GET', '/api/auth/me', 'bb.whoami()', 'Caller identity + role. Bail fast if role != "admin".'],
                  ].map(([method, path, py, desc]) => (
                    <tr key={path} className="border-b border-neutral-800/20">
                      <td className="px-3 py-2"><span className="text-pos-300 font-mono">{method}</span></td>
                      <td className="px-3 py-2 font-mono text-fg">{path}</td>
                      <td className="px-3 py-2 font-mono text-fg-muted">{py}</td>
                      <td className="px-3 py-2 text-fg-muted">{desc}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <p className="text-xs text-fg-subtle">
              Full response shapes are documented in the backend source and visible on the{' '}
              <a href="/api" className="text-accent-400 hover:underline">/api page</a> — click Try it on any
              endpoint to see the live JSON.
            </p>
          </Section>

        </article>
      </div>
    </div>
  );
}
