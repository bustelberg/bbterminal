'use client';

import { useCallback, useEffect, useState } from 'react';
import { createClient } from '../../lib/supabase/client';
import { dialog } from '../../lib/dialog';

import LoadingDots from '../components/LoadingDots';
import { API_URL } from '../../lib/apiUrl';

/**
 * ⚠⚠ THERE IS NO PASSWORD FIELD HERE AND THERE WILL NOT BE ONE, hash included. Asked for
 * (2026-09-08) and declined in `routers/auth.py::_user_detail`: a bcrypt hash is not information
 * about a person, it is an offline cracking target, and putting one on a screen puts it in
 * screenshots and in the DOM of a page anybody can shoulder-read. `has_password` answers the
 * question that actually has an action behind it — an invited user who never chose one signs in
 * by link, and that is worth seeing.
 *
 * ⚠ THE DETAIL FIELDS ARE NULLABLE ON PURPOSE. They come from a direct-Postgres read that needs
 * `SUPABASE_DB_URL`; without it they are `null`, which is a statement about US. `0` would be a
 * statement about the ACCOUNT — "no authenticators" — and rendering "unknown" as "off" is the one
 * direction a two-factor column must never be wrong in.
 */
type User = {
  id: string;
  email: string | null;
  role: 'admin' | 'user';
  created_at: string;
  last_sign_in_at: string;
  mfa_verified: number | null;
  /** Abandoned enrolments. ⚠ Counted apart from `mfa_verified` — a pending factor protects nothing. */
  mfa_pending: number | null;
  mfa_since: string | null;
  has_password: boolean | null;
  email_confirmed: boolean | null;
  banned_until: string | null;
  /** Live sessions. Answers "are they signed in right now", and drops to 0 after a Reset 2FA. */
  sessions: number | null;
};

export default function UsersPage() {
  const [users, setUsers] = useState<User[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  /** ⚠ THE CALLER'S OWN id. The reset endpoint refuses self-service (see its docstring), so
   *  without this the page would draw a button that always 403s — the one thing the house rule
   *  about admin controls says never to do. */
  const [meId, setMeId] = useState<string | null>(null);

  const [newEmail, setNewEmail] = useState('');
  const [newPassword, setNewPassword] = useState('');
  const [newRole, setNewRole] = useState<'user' | 'admin'>('user');
  const [creating, setCreating] = useState(false);

  const authHeader = useCallback(async (): Promise<Record<string, string> | null> => {
    const supabase = createClient();
    const { data: { session } } = await supabase.auth.getSession();
    if (!session) return null;
    return { Authorization: `Bearer ${session.access_token}` };
  }, []);

  useEffect(() => {
    void (async () => {
      const supabase = createClient();
      const { data: { user } } = await supabase.auth.getUser();
      setMeId(user?.id ?? null);
    })();
  }, []);

  const refresh = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const headers = await authHeader();
      if (!headers) {
        setError('Not signed in.');
        return;
      }
      const r = await fetch(`${API_URL}/api/auth/users`, { headers });
      if (!r.ok) {
        const body = await r.text();
        setError(`${r.status}: ${body}`);
        return;
      }
      const data = await r.json();
      setUsers(data.users ?? []);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, [authHeader]);

  useEffect(() => { refresh(); }, [refresh]);

  async function createUser(e: React.FormEvent) {
    e.preventDefault();
    if (!newEmail || !newPassword) return;
    setCreating(true);
    try {
      const headers = await authHeader();
      if (!headers) {
        await dialog.alert('Not signed in.');
        return;
      }
      const r = await fetch(`${API_URL}/api/auth/users`, {
        method: 'POST',
        headers: { ...headers, 'Content-Type': 'application/json' },
        body: JSON.stringify({ email: newEmail, password: newPassword, role: newRole }),
      });
      if (!r.ok) {
        const body = await r.text();
        await dialog.alert(`Could not create user:\n${r.status}: ${body}`, { title: 'Create failed' });
        return;
      }
      setNewEmail('');
      setNewPassword('');
      setNewRole('user');
      await refresh();
    } catch (e) {
      await dialog.alert(`Error: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setCreating(false);
    }
  }

  async function setRole(u: User, role: 'user' | 'admin') {
    if (u.role === role) return;
    const ok = await dialog.confirm(`Set ${u.email} to ${role}?`);
    if (!ok) return;
    try {
      const headers = await authHeader();
      if (!headers) return;
      const r = await fetch(`${API_URL}/api/auth/users/${u.id}/role`, {
        method: 'PATCH',
        headers: { ...headers, 'Content-Type': 'application/json' },
        body: JSON.stringify({ role }),
      });
      if (!r.ok) {
        const body = await r.text();
        await dialog.alert(`Update failed:\n${r.status}: ${body}`);
        return;
      }
      await refresh();
    } catch (e) {
      await dialog.alert(`Error: ${e instanceof Error ? e.message : String(e)}`);
    }
  }

  /**
   * Clear another person's authenticators after they lose their phone.
   *
   * ⚠⚠ IT IS THE WHOLE RECOVERY STORY, because Supabase TOTP has no backup codes and two-factor
   * is mandatory: without a way to do this, a lost phone is a permanent lockout that only hand-
   * written SQL against production could undo.
   *
   * ⚠ THE CONFIRMATION NAMES BOTH CONSEQUENCES. Removing the factor is half of it — the endpoint
   * also evicts their sessions, so anyone signed in on that account is thrown out. Somebody
   * pressing this to help a colleague should know it will also end that colleague's live session
   * on their laptop, and it is the point rather than a side effect: a phone stolen WITH the app
   * open is exactly what this is for.
   */
  async function resetMfa(u: User) {
    const ok = await dialog.confirm(
      `Remove every authenticator on ${u.email}?

`
      + 'They will be signed out everywhere and must set up two-factor again on their next '
      + 'sign-in. Use this when they have lost the device.',
    );
    if (!ok) return;
    try {
      const headers = await authHeader();
      if (!headers) return;
      const r = await fetch(`${API_URL}/api/auth/users/${u.id}/mfa/reset`, {
        method: 'POST',
        headers,
      });
      const body = await r.json().catch(() => null);
      if (!r.ok) {
        await dialog.alert(`Reset failed:
${r.status}: ${body?.detail ?? ''}`);
        return;
      }
      // ⚠ REPORT WHAT ACTUALLY HAPPENED, including the case where there was nothing to remove —
      // "done" over a no-op sends somebody away believing a problem is fixed.
      const n = body?.factors_removed ?? 0;
      await dialog.alert(
        n === 0
          ? `${u.email} had no authenticators — nothing to reset.`
          : `Removed ${n} authenticator${n === 1 ? '' : 's'} for ${u.email}.`
            + (body?.sessions_cleared
              ? ' They have been signed out everywhere.'
              : ' ⚠ Their existing sessions could NOT be cleared — a device already signed in '
                + 'keeps working until it expires.'),
      );
      await refresh();
    } catch (e) {
      await dialog.alert(`Error: ${e instanceof Error ? e.message : String(e)}`);
    }
  }

  async function removeUser(u: User) {
    const ok = await dialog.confirm(`Delete ${u.email}? This is permanent.`);
    if (!ok) return;
    try {
      const headers = await authHeader();
      if (!headers) return;
      const r = await fetch(`${API_URL}/api/auth/users/${u.id}`, {
        method: 'DELETE',
        headers,
      });
      if (!r.ok) {
        const body = await r.text();
        await dialog.alert(`Delete failed:\n${r.status}: ${body}`);
        return;
      }
      await refresh();
    } catch (e) {
      await dialog.alert(`Error: ${e instanceof Error ? e.message : String(e)}`);
    }
  }

  return (
    <div className="px-8 py-5 space-y-6 max-w-4xl">
      <div>
        <h1 className="text-xl font-semibold text-fg-strong">Users</h1>
        <p className="text-sm text-fg-muted mt-1">
          Admins see every page; regular users only see Welcome and Earnings.
        </p>
      </div>

      {/* Invite form */}
      <form
        onSubmit={createUser}
        className="bg-card rounded-xl border border-neutral-800/40 p-4 space-y-3"
      >
        <div className="text-sm font-medium text-fg-strong">Add user</div>
        <div className="grid grid-cols-1 md:grid-cols-[1fr_1fr_auto_auto] gap-3 items-end">
          <div>
            <label className="block text-xs text-fg-muted mb-1">Email</label>
            <input
              type="email"
              required
              value={newEmail}
              onChange={(e) => setNewEmail(e.target.value)}
              placeholder="someone@example.com"
              className="w-full bg-page border border-neutral-700 rounded-lg px-3 py-2 text-sm text-fg-strong focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none"
            />
          </div>
          <div>
            <label className="block text-xs text-fg-muted mb-1">Initial password</label>
            <input
              type="text"
              required
              minLength={8}
              value={newPassword}
              onChange={(e) => setNewPassword(e.target.value)}
              placeholder="min 8 chars"
              className="w-full bg-page border border-neutral-700 rounded-lg px-3 py-2 text-sm text-fg-strong font-mono focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none"
            />
          </div>
          <div>
            <label className="block text-xs text-fg-muted mb-1">Role</label>
            <select
              value={newRole}
              onChange={(e) => setNewRole(e.target.value as 'user' | 'admin')}
              className="bg-page border border-neutral-700 rounded-lg px-3 py-2 text-sm text-fg-strong focus:border-accent-500 outline-none"
            >
              <option value="user">User</option>
              <option value="admin">Admin</option>
            </select>
          </div>
          <button
            type="submit"
            disabled={creating || !newEmail || !newPassword}
            className="px-4 py-2 rounded-lg text-sm font-medium bg-accent-600 hover:bg-accent-500 text-fg-strong transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {creating ? 'Adding...' : 'Add user'}
          </button>
        </div>
      </form>

      {/* User list */}
      <div className="bg-card rounded-xl border border-neutral-800/40 overflow-hidden">
        <div className="px-5 py-3 border-b border-neutral-800/40 flex items-center justify-between">
          <span className="text-sm font-medium text-fg-strong">All users</span>
          <button
            onClick={refresh}
            className="text-xs text-fg-muted hover:text-fg-strong"
            disabled={loading}
          >
            {loading ? <LoadingDots label="Loading" /> : 'Refresh'}
          </button>
        </div>
        {error && (
          <div className="px-5 py-3 text-sm text-neg-400 bg-neg-500/10 border-b border-neg-500/20">
            {error}
          </div>
        )}
        <table className="w-full text-sm">
          <thead>
            <tr className="text-fg-subtle text-xs border-b border-neutral-800/40">
              <th className="text-left px-5 py-2.5 font-medium">Email</th>
              <th className="text-left px-3 py-2.5 font-medium">Role</th>
              <th className="text-left px-3 py-2.5 font-medium" title="Authenticator apps enrolled and verified. Two-factor is required to use the app.">
                2FA
              </th>
              <th className="text-left px-3 py-2.5 font-medium" title="Live sessions. Signing out, a Reset 2FA, or the 30-day timebox each drop this to 0.">
                Sessions
              </th>
              <th className="text-left px-3 py-2.5 font-medium">Created</th>
              <th className="text-left px-3 py-2.5 font-medium">Last sign-in</th>
              <th className="text-right px-5 py-2.5 font-medium">Actions</th>
            </tr>
          </thead>
          <tbody>
            {users.map((u) => (
              <tr key={u.id} className="border-b border-neutral-800/30 hover:bg-overlay/[0.02]">
                <td className="px-5 py-2 text-fg font-mono">
                  <span>{u.email ?? '—'}</span>
                  {/* ⚠ EXCEPTIONS ONLY, NOT COLUMNS. Every one of these is false for a healthy
                      account, so a column would be four mostly-empty cells on every row; as
                      badges they appear exactly when there is something to notice. */}
                  {u.email_confirmed === false && (
                    <span className="ml-2 text-[10px] uppercase tracking-wider text-warn-300"
                      title="Never confirmed their email address.">unconfirmed</span>
                  )}
                  {u.has_password === false && (
                    <span className="ml-2 text-[10px] uppercase tracking-wider text-warn-300"
                      title="No password set — this account can only sign in with an emailed link.">
                      link only
                    </span>
                  )}
                  {u.banned_until && (
                    <span className="ml-2 text-[10px] uppercase tracking-wider text-neg-400"
                      title={`Banned until ${u.banned_until} (set in the Supabase dashboard, not here).`}>
                      banned
                    </span>
                  )}
                </td>
                <td className="px-3 py-2">
                  <span
                    className={`inline-block px-2 py-0.5 text-[11px] font-medium rounded-md ${
                      u.role === 'admin'
                        ? 'bg-accent-500/15 text-accent-300 border border-accent-500/30'
                        : 'bg-neutral-700/30 text-fg-muted border border-neutral-700/50'
                    }`}
                  >
                    {u.role}
                  </span>
                </td>
                <td className="px-3 py-2">
                  {u.mfa_verified == null ? (
                    // ⚠ NOT "off". We could not read it — see the type's note.
                    <span className="text-xs text-fg-faint" title="Needs SUPABASE_DB_URL on the backend to read.">unknown</span>
                  ) : u.mfa_verified > 0 ? (
                    <span className="text-xs text-pos-400"
                      title={u.mfa_since ? `Since ${u.mfa_since.slice(0, 10)}` : undefined}>
                      on{u.mfa_verified > 1 ? ` (${u.mfa_verified})` : ''}
                    </span>
                  ) : (
                    <span className="text-xs text-warn-300"
                      title="No authenticator. They will be sent to /account/security and cannot use the app until they enrol.">
                      off
                      {/* ⚠ A PENDING FACTOR IS NOT PROTECTION — it is an abandoned enrolment, and
                          saying so is the difference between "they are half done" and "they gave
                          up". Shown beside `off`, never folded into the count. */}
                      {!!u.mfa_pending && (
                        <span className="text-fg-faint"
                          title="Started an enrolment and never finished it.">
                          {` · ${u.mfa_pending} pending`}
                        </span>
                      )}
                    </span>
                  )}
                </td>
                <td className="px-3 py-2 font-mono text-xs">
                  {u.sessions == null
                    ? <span className="text-fg-faint">—</span>
                    : <span className={u.sessions > 0 ? 'text-fg' : 'text-fg-faint'}>{u.sessions}</span>}
                </td>
                <td className="px-3 py-2 text-fg-subtle font-mono text-xs">
                  {u.created_at ? u.created_at.slice(0, 10) : '—'}
                </td>
                <td className="px-3 py-2 text-fg-subtle font-mono text-xs">
                  {u.last_sign_in_at ? u.last_sign_in_at.slice(0, 10) : 'never'}
                </td>
                <td className="px-5 py-2 text-right">
                  <div className="inline-flex gap-2">
                    {u.role === 'user' ? (
                      <button
                        onClick={() => setRole(u, 'admin')}
                        className="text-xs text-accent-400 hover:text-accent-300"
                      >
                        Promote
                      </button>
                    ) : (
                      <button
                        onClick={() => setRole(u, 'user')}
                        className="text-xs text-fg-muted hover:text-warn-400"
                      >
                        Demote
                      </button>
                    )}
                    {/* ⚠ NOT ON YOUR OWN ROW. The endpoint refuses it — /account/security is
                        where you manage your own, and it asks for a current code first. */}
                    {u.id !== meId && (
                      <button
                        onClick={() => resetMfa(u)}
                        className="text-xs text-fg-muted hover:text-warn-400"
                        title="Clear their authenticators and sign them out — for a lost device."
                      >
                        Reset 2FA
                      </button>
                    )}
                    <button
                      onClick={() => removeUser(u)}
                      className="text-xs text-fg-subtle hover:text-neg-400"
                    >
                      Delete
                    </button>
                  </div>
                </td>
              </tr>
            ))}
            {!loading && users.length === 0 && (
              <tr>
                <td colSpan={7} className="px-5 py-8 text-center text-sm text-fg-subtle">
                  No users yet.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
