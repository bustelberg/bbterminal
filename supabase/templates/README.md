# Auth email templates

Each `.html` file here is **exactly** what goes in the Supabase dashboard — select all, paste,
save. Nothing else belongs in them.

> ⚠ **NO COMMENTS IN THE `.html` FILES.** They were written with a long `<!-- ... -->` block
> explaining the reasoning, which is the house style everywhere else in this repo and is wrong
> here: these files are pasted verbatim into a dashboard field and then **mailed to people**. An
> HTML comment does not render, but it is still shipped — every recipient's mail client holds a
> copy, "view source" shows it, and what it explained was our own incident history. The
> explanation lives in this file, which nobody emails.

| File | Dashboard template (Authentication → Emails) | `type` in the link |
|---|---|---|
| `confirmation.html` | **Confirm signup** | `signup` |
| `magic_link.html` | Magic Link | `magiclink` |
| `recovery.html` | Reset Password | `recovery` |

`config.toml` wires the same three files into the **local** stack, so local and production send the
same mail. After changing one: `npx supabase stop && npx supabase start`.

## Why the link is not `{{ .ConfirmationURL }}`

That is Supabase's default, and it is what account creation was still using in production on
2026-08-19 when a new user got **"Auth session missing"** after choosing a password. It expands to:

```
{SUPABASE_URL}/auth/v1/verify?token=…&type=…&redirect_to=…
```

It comes back as `?code=`, which is **PKCE**: `createBrowserClient` stored a code verifier in a
cookie when the link was *requested*, and only that browser can complete the exchange. Ask on a
laptop, open the mail on a phone, and it cannot work — and locally there is only ever one browser,
which is why this survived. `token_hash` + `verifyOtp` carries no such per-browser state, so the
link works from any device.

### ⚠⚠ What this change does NOT fix: mail scanners

**A single-use token is spent by anything that fetches its URL**, and corporate mail security
(Microsoft Defender Safe Links, Proofpoint, Mimecast) fetches every link in every message before
the recipient sees it. `/auth/v1/verify` and our own `/auth/confirm` are both plain GETs, so
changing the template moves **who** spends the token, not **whether** a scanner can. Production,
2026-08-19: a client got the signup mail and was told the link had already been used.

That is fixed in the app, not in this template: `frontend/app/auth/confirm/page.tsx` is a **page
with a button**, not a route handler. It reads the token out of the URL and does nothing with it;
`verifyOtp` runs on the press. A scanner issues the GET, renders no JS, presses nothing, and the
token is still there when the person arrives. ⚠ `detectSessionInUrl: false` on that page's Supabase
client is part of it — the default processes `?code=` on load and would hand the protection back.

So the two changes fix two different failures, and both are needed:

| failure | fixed by |
|---|---|
| link opened on a different device from the one that requested it | this template (`token_hash`, no PKCE verifier) |
| "link already used" — a mail scanner got there first | the confirm **page** (nothing is spent on GET) |

Verified against the local stack rather than taken from the docs: `generate_link` returns
`action_link = …/auth/v1/verify?token=…` (the URL scanners eat), and `verify_otp({token_hash, type})`
returns a live session for `signup`, `magiclink` **and** `recovery` with no code verifier anywhere.

## Two things that are easy to get wrong

⚠ **`Confirm signup` is the one the signup flow sends.** `signInWithOtp({ shouldCreateUser: true })`
sends *Confirm signup* to a NEW address and *Magic Link* only to an existing one — so fixing only
the Magic Link template leaves the exact case that was broken still broken.

⚠ **`type` is pinned per template, not `{{ .Email_Action_Type }}`.** The variable is the documented
way and it does work, but each template already knows what it is, and a variable that fails to
render leaves `type=` empty — which the route can only refuse. One fewer thing between a person and
their account.

## The dashboard settings these depend on

**Dashboard → your project → Authentication → URL Configuration**
(`https://supabase.com/dashboard/project/<project-ref>/auth/url-configuration`)

* **Site URL** = `https://bbterminal.vercel.app` — no trailing slash, no path. This is what
  `{{ .SiteURL }}` in every link above expands to, so it literally builds the URL that gets mailed.
* **Redirect URLs** must contain `https://bbterminal.vercel.app/auth/confirm`. The login page
  passes `emailRedirectTo: ${window.location.origin}/auth/confirm`, and ⚠ if that exact URL is not
  allow-listed Supabase does not error — it silently falls back to the Site URL, dropping the
  `/auth/confirm` path, so the token lands on `/` and the route never runs. Add an entry for every
  origin the app is opened from (a custom domain, `http://localhost:3000/auth/confirm` if the local
  frontend is ever pointed at the hosted project).

⚠ **Vercel preview deploys get their own hostname, and `{{ .SiteURL }}` is fixed** — so an email
link always lands on production, whichever deployment requested it. A wildcard Redirect URL lets a
preview finish a sign-in it started; it cannot change where the email points.

**How to check it worked** — request a signup link in production and look at the raw `href` in the
email. It should be `https://bbterminal.vercel.app/auth/confirm?token_hash=…&type=signup`. If it
still reads `…supabase.co/auth/v1/verify?token=…`, the template did not save.
