'use client';

import { Suspense, useEffect, useState } from 'react';
import { useRouter, useSearchParams } from 'next/navigation';
import { createClient } from '../../lib/supabase/client';
import { describeMfaError } from '../../lib/mfaError';
import { safeNext } from '../../lib/mfaGate';
import { serverSkewSeconds } from '../../lib/totp';
import AuthShell, {
  AuthNotice, authButtonClass, authFieldClass, authLabelClass,
} from '../components/auth/AuthShell';
import { useSecurityCopy } from '../components/account/securityCopy';
import {
  CODE_LENGTH, type Factor, isCompleteCode, normaliseCode, verifiedFactors,
} from '../components/account/mfaFactors';

/**
 * The second-factor gate. `proxy.ts` sends every session here that has a verified factor and has
 * not used it yet (`currentLevel aal1`, `nextLevel aal2` — see `lib/mfaGate`).
 *
 * ⚠⚠ IT WEARS `AuthShell`, THE SAME FRAME AS /login AND /auth/confirm, because it belongs to the
 * same sequence: password, code, in. Rendered as an ordinary page inside the app chrome it would
 * sit beside a nav rail full of links that all bounce straight back here.
 *
 * ⚠ THERE IS AN ESCAPE HATCH AND IT IS NOT DECORATION. Somebody who cannot produce a code — phone
 * flat, wrong device, travelling — is otherwise stuck on a screen with no exit and a session they
 * cannot use, which is the state people resolve by clearing site data or asking for a password
 * reset. Signing out is the honest way off this page.
 */
export default function MfaPage() {
  return (
    <Suspense fallback={null}>
      <MfaChallenge />
    </Suspense>
  );
}

function MfaChallenge() {
  const router = useRouter();
  const params = useSearchParams();
  const copy = useSecurityCopy();
  const [supabase] = useState(() => createClient());

  const [factors, setFactors] = useState<Factor[] | null>(null);
  const [factorId, setFactorId] = useState<string>('');
  const [code, setCode] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const next = safeNext(params.get('next'))

  /**
   * ⚠ THE SAME PRE-EMPTIVE CHECK AS THE ENROLMENT PAGE, for the same reason: GoTrue's acceptance
   * window is about 30 seconds and is not configurable, so a machine a minute out rejects every
   * code the reader can possibly produce. Here it matters more than on enrolment — somebody stuck
   * at this gate cannot reach ANY page to find out why.
   */
  const [clockSkew, setClockSkew] = useState<number | null>(null)
  useEffect(() => {
    let alive = true
    void (async () => {
      const s = await serverSkewSeconds(
        `${process.env.NEXT_PUBLIC_SUPABASE_URL ?? ''}/auth/v1/health`)
      if (alive && s != null && Math.abs(s) >= 10) setClockSkew(s)
    })()
    return () => { alive = false }
  }, []);

  useEffect(() => {
    let alive = true;
    void (async () => {
      const { data, error: e } = await supabase.auth.mfa.listFactors();
      if (!alive) return;
      if (e) {
        console.warn('[mfa] could not list factors at the gate:', e);
        setError(describeMfaError({ message: e.message }));
        setFactors([]);
        return;
      }
      const verified = verifiedFactors((data?.all ?? []) as Factor[]);
      setFactors(verified);
      // ⚠ ONE FACTOR IS THE NORMAL CASE, so it is preselected and the reader never sees a chooser
      // with a single option — a control that asks a question with one answer.
      if (verified.length > 0) setFactorId(verified[0].id);
    })();
    return () => { alive = false; };
  }, [supabase]);

  /**
   * ⚠⚠ NO FACTORS MEANS LEAVE, and this is the one path that must never dead-end. `proxy.ts` only
   * sends people here when GoTrue says they have one — but the two reads are moments apart, and
   * removing your last authenticator in another tab lands exactly in the gap. Without this the
   * reader is on a challenge page for a factor that does not exist, and the gate has stopped
   * firing so nothing will ever redirect them away either.
   */
  useEffect(() => {
    if (factors && factors.length === 0) router.replace(next);
  }, [factors, next, router]);

  async function verify() {
    if (!factorId || !isCompleteCode(code)) return;
    setError(null);
    setBusy(true);
    try {
      const { error: e } = await supabase.auth.mfa.challengeAndVerify({
        factorId,
        code: normaliseCode(code),
      });
      if (e) {
        console.warn('[mfa] challenge failed:', e);
        setError(describeMfaError({ message: e.message }));
        return;
      }
      // ⚠ A HARD NAVIGATION, NOT `router.push`. Verifying issues a NEW session at `aal2`, and the
      // middleware reads the cookie on the next server request — a client-side transition would
      // arrive at the destination with the old cookie still in flight and be bounced straight back
      // here. The same reason the retired account switcher reloaded.
      window.location.href = next;
    } finally {
      setBusy(false);
    }
  }

  async function signOut() {
    await supabase.auth.signOut();
    router.push('/login');
    router.refresh();
  }

  const multiple = (factors?.length ?? 0) > 1;

  return (
    <AuthShell
      title={copy.challenge.title}
      subtitle={copy.challenge.body}
      footer={
        <button
          type="button"
          onClick={() => void signOut()}
          className="text-accent-400 hover:text-accent-500 underline underline-offset-2
                     transition-colors"
        >
          {copy.challenge.signOut}
        </button>
      }
    >
      <form
        onSubmit={(e) => { e.preventDefault(); void verify(); }}
        className="space-y-4"
      >
        {multiple && (
          <div>
            <label htmlFor="factor" className={authLabelClass}>{copy.challenge.pick}</label>
            <select
              id="factor"
              value={factorId}
              onChange={(e) => setFactorId(e.target.value)}
              className={authFieldClass}
            >
              {factors?.map((f) => (
                <option key={f.id} value={f.id}>{f.friendly_name || copy.active}</option>
              ))}
            </select>
          </div>
        )}

        <div>
          <label htmlFor="code" className={authLabelClass}>{copy.codeLabel}</label>
          <input
            id="code"
            value={code}
            onChange={(e) => setCode(normaliseCode(e.target.value))}
            inputMode="numeric"
            autoComplete="one-time-code"
            autoFocus
            maxLength={CODE_LENGTH}
            placeholder="000000"
            className={`${authFieldClass} font-mono tracking-[0.3em]`}
          />
          <p className="mt-1.5 text-xs text-fg-faint">{copy.codeHint}</p>
        </div>

        {/* ⚠ ABOVE the error: when the clock is out every code fails, so this is the cause and
            the rejection below it is only the symptom. */}
        {clockSkew != null && (
          <AuthNotice kind="error">
            {copy.clockWarning(Math.abs(clockSkew), clockSkew > 0)}
          </AuthNotice>
        )}

        {error && <AuthNotice kind="error">{error}</AuthNotice>}

        <button
          type="submit"
          disabled={busy || !isCompleteCode(code) || !factorId}
          className={authButtonClass}
        >
          {busy ? copy.challenge.verifying : copy.challenge.verify}
        </button>
      </form>
    </AuthShell>
  );
}
