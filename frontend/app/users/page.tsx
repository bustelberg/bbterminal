'use client';

import { useCallback, useEffect, useState } from 'react';
import { createClient } from '../../lib/supabase/client';
import { dialog } from '../../lib/dialog';

import LoadingDots from '../components/LoadingDots';
import { API_URL } from '../../lib/apiUrl';

type User = {
  id: string;
  email: string | null;
  role: 'admin' | 'user';
  created_at: string;
  last_sign_in_at: string;
};

export default function UsersPage() {
  const [users, setUsers] = useState<User[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

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
              <th className="text-left px-3 py-2.5 font-medium">Created</th>
              <th className="text-left px-3 py-2.5 font-medium">Last sign-in</th>
              <th className="text-right px-5 py-2.5 font-medium">Actions</th>
            </tr>
          </thead>
          <tbody>
            {users.map((u) => (
              <tr key={u.id} className="border-b border-neutral-800/30 hover:bg-overlay/[0.02]">
                <td className="px-5 py-2 text-fg font-mono">{u.email ?? '—'}</td>
                <td className="px-3 py-2">
                  <span
                    className={`inline-block px-2 py-0.5 text-[10px] font-medium rounded-md ${
                      u.role === 'admin'
                        ? 'bg-accent-500/15 text-accent-300 border border-accent-500/30'
                        : 'bg-neutral-700/30 text-fg-muted border border-neutral-700/50'
                    }`}
                  >
                    {u.role}
                  </span>
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
                <td colSpan={5} className="px-5 py-8 text-center text-sm text-fg-subtle">
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
