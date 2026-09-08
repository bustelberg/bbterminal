'use client';

import { useCallback, useEffect, useState } from 'react';
import { createClient } from '../../../lib/supabase/client';
import { describeMfaError } from '../../../lib/mfaError';
import { useSecurityCopy } from '../../components/account/securityCopy';
import {
  CODE_LENGTH, type Factor, groupSecret, isCompleteCode, normaliseCode, suggestName, svgOnly,
  unverifiedIds, verifiedFactors, verifyOrder,
} from '../../components/account/mfaFactors';

/**
 * Manage the authenticators on your own account — enrol, confirm, list, remove.
 *
 * ⚠⚠ THIS PAGE TALKS TO SUPABASE DIRECTLY AND NEVER TO OUR BACKEND. `supabase.auth.mfa.*` goes to
 * `NEXT_PUBLIC_SUPABASE_URL`, not `API_URL`, so it needs no entry in `_auth_middleware.py` — which
 * makes it the one exception to `userAllowedPaths`' standing warning that adding a page there is
 * half the job. Worth stating, because the absence of the second half looks like an omission.
 *
 * ⚠ EVERY SIGNED-IN USER, NOT JUST ADMINS. These are the reader's own credentials; an admin cannot
 * enrol somebody else's phone and there is nothing here to gate.
 *
 * ⚠ IT DOES NOT ENFORCE ANYTHING. Nothing reads `aal` yet — `proxy.ts` and `verify_token` are
 * steps 4 and 5 — so a user who enrols a factor can still sign in with a password alone. The page
 * says so rather than implying a protection that is not switched on: a security screen that
 * overstates what it did is worse than one that admits the gap.
 */
export default function AccountSecurityPage() {
  const copy = useSecurityCopy();
  const [supabase] = useState(() => createClient());

  const [factors, setFactors] = useState<Factor[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  // Enrolment in flight: the factor GoTrue created, plus what it wants us to show.
  const [pending, setPending] = useState<
    { id: string; qr: string; secret: string } | null>(null);
  const [name, setName] = useState('');
  const [code, setCode] = useState('');
  const [busy, setBusy] = useState(false);
  const [copiedSecret, setCopiedSecret] = useState(false);

  // Removal in flight: which factor, and the code proving it may go.
  const [removing, setRemoving] = useState<Factor | null>(null);
  const [removeCode, setRemoveCode] = useState('');

  const load = useCallback(async () => {
    const { data, error: e } = await supabase.auth.mfa.listFactors();
    if (e) {
      console.warn('[mfa] could not list factors:', e);
      setError(describeMfaError({ message: e.message }));
      setFactors([]);
      return;
    }
    setFactors((data?.all ?? []) as Factor[]);
  }, [supabase]);

  useEffect(() => { void load(); }, [load]);

  async function startEnrol() {
    setError(null);
    setNotice(null);
    setBusy(true);
    try {
      // ⚠⚠ SWEEP THE ABANDONED ONES FIRST. See `unverifiedIds`: every enrolment nobody finished
      // left a real factor behind that counts against `max_enrolled_factors`, so without this a
      // handful of closed tabs eventually make "Add" fail with "too many factors" on a page
      // showing none. Failures here are ignored on purpose — a leftover we cannot clear is not a
      // reason to block the enrolment the reader actually asked for.
      for (const id of unverifiedIds(factors ?? [])) {
        await supabase.auth.mfa.unenroll({ factorId: id }).catch(() => undefined);
      }
      const friendlyName = name.trim() || suggestName(factors ?? []);
      const { data, error: e } = await supabase.auth.mfa.enroll({
        factorType: 'totp',
        friendlyName,
      });
      if (e) {
        console.warn('[mfa] enroll failed:', e);
        setError(describeMfaError({ message: e.message }));
        return;
      }
      setName(friendlyName);
      setPending({ id: data.id, qr: data.totp.qr_code, secret: data.totp.secret });
      setCode('');
    } finally {
      setBusy(false);
    }
  }

  async function confirmEnrol() {
    if (!pending || !isCompleteCode(code)) return;
    setError(null);
    setBusy(true);
    try {
      const { error: e } = await supabase.auth.mfa.challengeAndVerify({
        factorId: pending.id,
        code: normaliseCode(code),
      });
      if (e) {
        console.warn('[mfa] verify failed:', e);
        setError(describeMfaError({ message: e.message }));
        return;
      }
      setPending(null);
      setCode('');
      setName('');
      setNotice(copy.enrolled);
      await load();
    } finally {
      setBusy(false);
    }
  }

  async function cancelEnrol() {
    const id = pending?.id;
    setPending(null);
    setCode('');
    setError(null);
    // ⚠ REMOVE THE HALF-MADE FACTOR ON THE WAY OUT rather than leaving it for the next sweep. It
    // is the same cleanup, done at the moment we know it is abandoned instead of guessing later.
    if (id) await supabase.auth.mfa.unenroll({ factorId: id }).catch(() => undefined);
    await load();
  }

  async function confirmRemove() {
    if (!removing || !isCompleteCode(removeCode)) return;
    setError(null);
    setBusy(true);
    try {
      // ⚠⚠ PROVE POSSESSION BEFORE TAKING THE PROTECTION OFF. Without a code, anyone holding a
      // session could strip two-factor from the account — which is exactly what a stolen session
      // would do first. `verifyOrder` puts the OTHER authenticators first so a lost device can
      // still be removed using the spare, which is what having a spare is for.
      let verified = false;
      for (const f of verifyOrder(factors ?? [], removing.id)) {
        const { error: e } = await supabase.auth.mfa.challengeAndVerify({
          factorId: f.id,
          code: normaliseCode(removeCode),
        });
        if (!e) { verified = true; break; }
        console.warn('[mfa] verify against', f.id, 'failed:', e);
      }
      if (!verified) {
        setError(describeMfaError({ message: 'invalid code' }));
        return;
      }
      const { error: e } = await supabase.auth.mfa.unenroll({ factorId: removing.id });
      if (e) {
        console.warn('[mfa] unenroll failed:', e);
        setError(describeMfaError({ message: e.message }));
        return;
      }
      setRemoving(null);
      setRemoveCode('');
      setNotice(copy.removed);
      await load();
    } finally {
      setBusy(false);
    }
  }

  const active = verifiedFactors(factors ?? []);
  const dateFmt = (iso: string) =>
    new Date(iso).toLocaleDateString(copy.lang === 'nl' ? 'nl-NL' : 'en-GB',
      { year: 'numeric', month: 'short', day: 'numeric' });

  return (
    <div className="p-6 max-w-2xl mx-auto space-y-5">
      <header>
        <h1 className="text-lg font-semibold tracking-tight text-fg-strong">{copy.title}</h1>
        <p className="mt-1 text-sm text-fg-subtle leading-relaxed">{copy.intro}</p>
      </header>

      {/* ⚠ THE POLICY, AT THE TOP, IN ONE SENTENCE. It used to say the opposite — that enrolling
          changed nothing — which was true for exactly as long as the gate was unbuilt. A security
          page that is out of date about its own rules is worse than one that says nothing. */}
      <p className="rounded-lg border border-accent-300/60 bg-accent-200/50 px-3.5 py-3 text-xs
                    leading-relaxed text-accent-400">
        {copy.policy}
      </p>

      {error && (
        <p role="alert" className="rounded-lg border border-neg-200 bg-neg-100 px-3.5 py-3
                                   text-xs leading-relaxed text-neg-400">
          {error}
        </p>
      )}
      {notice && (
        <p className="rounded-lg border border-pos-300/40 bg-pos-300/10 px-3.5 py-3
                      text-xs leading-relaxed text-pos-400">
          {notice}
        </p>
      )}

      {factors === null ? (
        <p className="text-sm text-fg-subtle">{copy.loading}</p>
      ) : (
        <div className="bg-card border border-neutral-800/40 rounded-xl p-5 space-y-4">
          {active.length === 0 ? (
            <div>
              <p className="text-sm font-medium text-fg-strong">{copy.noneTitle}</p>
              <p className="mt-1 text-xs text-fg-subtle">{copy.noneBody}</p>
            </div>
          ) : (
            <ul className="divide-y divide-neutral-800/30">
              {active.map((f) => (
                <li key={f.id} className="flex items-center gap-3 py-2.5 first:pt-0">
                  <div className="flex-1 min-w-0">
                    <div className="text-sm text-fg-strong truncate">
                      {f.friendly_name || copy.active}
                    </div>
                    <div className="text-[11px] text-fg-faint">{copy.addedOn(dateFmt(f.created_at))}</div>
                  </div>
                  <span className="text-[10px] uppercase tracking-wider text-pos-400 shrink-0">
                    {copy.active}
                  </span>
                  <button
                    type="button"
                    onClick={() => { setRemoving(f); setRemoveCode(''); setError(null); }}
                    className="text-xs px-2.5 py-1 rounded-lg border border-neutral-700 text-fg-muted
                               hover:text-neg-400 hover:border-neg-300 transition-colors"
                  >
                    {copy.remove}
                  </button>
                </li>
              ))}
            </ul>
          )}

          {!pending && !removing && (
            <div className="space-y-3 pt-1">
              <div>
                <label htmlFor="factor-name"
                  className="block text-xs font-medium text-fg-muted mb-1.5">
                  {copy.nameLabel}
                </label>
                <input
                  id="factor-name"
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                  placeholder={copy.namePlaceholder}
                  className="w-full bg-inset border border-neutral-700 rounded-lg px-3.5 py-2.5
                             text-sm text-fg-strong placeholder-fg-faint transition-colors
                             focus:outline-none focus:border-accent-500 focus:ring-2
                             focus:ring-accent-500/25 focus:bg-card"
                />
              </div>
              <button
                type="button"
                onClick={() => void startEnrol()}
                disabled={busy}
                className="bg-accent-600 hover:bg-accent-500 disabled:opacity-50 text-white
                           text-sm font-medium rounded-lg px-4 py-2.5 transition-colors"
              >
                {active.length === 0 ? copy.addFirst : copy.add}
              </button>
            </div>
          )}

          {pending && (
            <div className="space-y-4 pt-1">
              <div>
                <p className="text-sm font-medium text-fg-strong">{copy.scanTitle}</p>
                <p className="mt-1 text-xs text-fg-subtle">{copy.scanBody}</p>
              </div>

              {/* ⚠ THE QR IS AN SVG STRING FROM GOTRUE, injected as markup because that is the
                  form Supabase returns it in. The trust boundary is the same one that issues our
                  sessions — if that server were hostile, an <svg> would be the least of it — and
                  the alternative is a QR library for one image. */}
              <div
                className="bg-white rounded-lg p-3 w-fit border border-neutral-800/30
                           [&_svg]:w-44 [&_svg]:h-44"
                dangerouslySetInnerHTML={{ __html: svgOnly(pending.qr) }}
              />

              <details className="text-xs">
                <summary className="cursor-pointer text-accent-400 hover:text-accent-500">
                  {copy.cannotScan}
                </summary>
                <div className="mt-2 space-y-1.5">
                  <p className="text-fg-subtle">{copy.secretLabel}</p>
                  <div className="flex items-center gap-2">
                    <code className="flex-1 font-mono text-[11px] bg-inset border
                                     border-neutral-800/40 rounded px-2 py-1.5 break-all
                                     text-fg-strong">
                      {groupSecret(pending.secret)}
                    </code>
                    <button
                      type="button"
                      onClick={() => {
                        void navigator.clipboard?.writeText(pending.secret)
                          .then(() => setCopiedSecret(true))
                          // ⚠ Clipboard access can be refused (permissions, insecure origin). The
                          // key is on screen either way, so this is a convenience, not a failure.
                          .catch((e) => console.warn('[mfa] clipboard refused:', e));
                      }}
                      className="text-xs px-2.5 py-1.5 rounded-lg border border-neutral-700
                                 text-fg-muted hover:bg-overlay/[0.04] transition-colors shrink-0"
                    >
                      {copiedSecret ? copy.copied : copy.copySecret}
                    </button>
                  </div>
                </div>
              </details>

              <div>
                <label htmlFor="totp-code"
                  className="block text-xs font-medium text-fg-muted mb-1.5">
                  {copy.codeLabel}
                </label>
                <input
                  id="totp-code"
                  value={code}
                  onChange={(e) => setCode(normaliseCode(e.target.value))}
                  // ⚠ `inputMode` + `autoComplete="one-time-code"`: a numeric keypad on a phone,
                  // and iOS/Android offer the code from the authenticator directly.
                  inputMode="numeric"
                  autoComplete="one-time-code"
                  autoFocus
                  maxLength={CODE_LENGTH}
                  placeholder="000000"
                  className="w-36 bg-inset border border-neutral-700 rounded-lg px-3.5 py-2.5
                             font-mono tracking-[0.3em] text-sm text-fg-strong
                             placeholder-fg-faint transition-colors focus:outline-none
                             focus:border-accent-500 focus:ring-2 focus:ring-accent-500/25
                             focus:bg-card"
                />
                <p className="mt-1.5 text-[11px] text-fg-faint">{copy.codeHint}</p>
              </div>

              <div className="flex gap-2">
                <button
                  type="button"
                  onClick={() => void confirmEnrol()}
                  disabled={busy || !isCompleteCode(code)}
                  className="bg-accent-600 hover:bg-accent-500 disabled:opacity-50
                             disabled:cursor-not-allowed text-white text-sm font-medium
                             rounded-lg px-4 py-2.5 transition-colors"
                >
                  {busy ? copy.confirming : copy.confirm}
                </button>
                <button
                  type="button"
                  onClick={() => void cancelEnrol()}
                  className="text-sm text-fg-muted hover:text-fg-strong px-3 py-2.5
                             transition-colors"
                >
                  {copy.cancel}
                </button>
              </div>
            </div>
          )}

          {removing && (
            <div className="space-y-3 pt-1 border-t border-neutral-800/30">
              <p className="text-sm font-medium text-fg-strong">
                {copy.removeTitle(removing.friendly_name || copy.active)}
              </p>
              <p className="text-xs text-fg-subtle leading-relaxed">
                {active.length > 1 ? copy.removeBodyOther : copy.removeBody}
              </p>
              <input
                value={removeCode}
                onChange={(e) => setRemoveCode(normaliseCode(e.target.value))}
                inputMode="numeric"
                autoComplete="one-time-code"
                autoFocus
                maxLength={CODE_LENGTH}
                placeholder="000000"
                aria-label={copy.codeLabel}
                className="w-36 bg-inset border border-neutral-700 rounded-lg px-3.5 py-2.5
                           font-mono tracking-[0.3em] text-sm text-fg-strong placeholder-fg-faint
                           transition-colors focus:outline-none focus:border-accent-500
                           focus:ring-2 focus:ring-accent-500/25 focus:bg-card"
              />
              <div className="flex gap-2">
                <button
                  type="button"
                  onClick={() => void confirmRemove()}
                  disabled={busy || !isCompleteCode(removeCode)}
                  className="bg-neg-600 hover:bg-neg-500 disabled:opacity-50
                             disabled:cursor-not-allowed text-white text-sm font-medium
                             rounded-lg px-4 py-2.5 transition-colors"
                >
                  {busy ? copy.removing : copy.removeConfirm}
                </button>
                <button
                  type="button"
                  onClick={() => { setRemoving(null); setRemoveCode(''); setError(null); }}
                  className="text-sm text-fg-muted hover:text-fg-strong px-3 py-2.5
                             transition-colors"
                >
                  {copy.cancel}
                </button>
              </div>
            </div>
          )}
        </div>
      )}

      {/* ⚠ ON THE PAGE, NOT IN A RUNBOOK. Supabase TOTP ships no backup codes, so the only
          recovery is a second authenticator or an admin deleting the factor — and the moment to
          learn that is while you still have the phone. */}
      <div className="bg-card-alt border border-neutral-800/30 rounded-xl p-4">
        <p className="text-xs font-medium text-fg-strong">{copy.recoveryTitle}</p>
        <p className="mt-1 text-xs text-fg-subtle leading-relaxed">{copy.recoveryBody}</p>
      </div>
    </div>
  );
}
