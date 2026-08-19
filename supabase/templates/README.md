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

Two things follow, and **neither can happen in development**, which is why this survived:

1. **That URL consumes the one-time token when it is fetched.** Corporate mail security (Outlook
   Safe Links and friends) fetches every link in a message before the human sees it, so the token
   is spent and the real click gets `?error=access_denied&error_code=otp_expired`. A local Mailpit
   inbox has no scanner.
2. **It comes back as `?code=`, which is PKCE.** `createBrowserClient` stored a code verifier in a
   cookie when the link was *requested*, and only that browser can complete the exchange. Ask on a
   laptop, open the mail on a phone, and it cannot work. Locally there is only ever one browser.

`token_hash` + `verifyOtp` — what `frontend/app/auth/confirm/route.ts` does — has neither problem:
nothing is consumed until our own route runs, and there is no per-browser state to carry.

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
