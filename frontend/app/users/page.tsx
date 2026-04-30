'use client';

import { useCallback, useEffect, useState } from 'react';
import { createClient } from '../../lib/supabase/client';
import { dialog } from '../../lib/dialog';

const API_URL = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000';

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
        <h1 className="text-xl font-semibold text-white">Users</h1>
        <p className="text-sm text-gray-400 mt-1">
          Admins see every page; regular users only see Welcome and Earnings.
        </p>
      </div>

      {/* Invite form */}
      <form
        onSubmit={createUser}
        className="bg-[#151821] rounded-xl border border-gray-800/40 p-4 space-y-3"
      >
        <div className="text-sm font-medium text-white">Add user</div>
        <div className="grid grid-cols-1 md:grid-cols-[1fr_1fr_auto_auto] gap-3 items-end">
          <div>
            <label className="block text-xs text-gray-400 mb-1">Email</label>
            <input
              type="email"
              required
              value={newEmail}
              onChange={(e) => setNewEmail(e.target.value)}
              placeholder="someone@example.com"
              className="w-full bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-sm text-white focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
            />
          </div>
          <div>
            <label className="block text-xs text-gray-400 mb-1">Initial password</label>
            <input
              type="text"
              required
              minLength={8}
              value={newPassword}
              onChange={(e) => setNewPassword(e.target.value)}
              placeholder="min 8 chars"
              className="w-full bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-sm text-white font-mono focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
            />
          </div>
          <div>
            <label className="block text-xs text-gray-400 mb-1">Role</label>
            <select
              value={newRole}
              onChange={(e) => setNewRole(e.target.value as 'user' | 'admin')}
              className="bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-sm text-white focus:border-indigo-500 outline-none"
            >
              <option value="user">User</option>
              <option value="admin">Admin</option>
            </select>
          </div>
          <button
            type="submit"
            disabled={creating || !newEmail || !newPassword}
            className="px-4 py-2 rounded-lg text-sm font-medium bg-indigo-600 hover:bg-indigo-500 text-white transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {creating ? 'Adding...' : 'Add user'}
          </button>
        </div>
      </form>

      {/* User list */}
      <div className="bg-[#151821] rounded-xl border border-gray-800/40 overflow-hidden">
        <div className="px-5 py-3 border-b border-gray-800/40 flex items-center justify-between">
          <span className="text-sm font-medium text-white">All users</span>
          <button
            onClick={refresh}
            className="text-xs text-gray-400 hover:text-white"
            disabled={loading}
          >
            {loading ? 'Loading…' : 'Refresh'}
          </button>
        </div>
        {error && (
          <div className="px-5 py-3 text-sm text-rose-400 bg-rose-500/10 border-b border-rose-500/20">
            {error}
          </div>
        )}
        <table className="w-full text-sm">
          <thead>
            <tr className="text-gray-500 text-xs border-b border-gray-800/40">
              <th className="text-left px-5 py-2.5 font-medium">Email</th>
              <th className="text-left px-3 py-2.5 font-medium">Role</th>
              <th className="text-left px-3 py-2.5 font-medium">Created</th>
              <th className="text-left px-3 py-2.5 font-medium">Last sign-in</th>
              <th className="text-right px-5 py-2.5 font-medium">Actions</th>
            </tr>
          </thead>
          <tbody>
            {users.map((u) => (
              <tr key={u.id} className="border-b border-gray-800/30 hover:bg-white/[0.02]">
                <td className="px-5 py-2 text-gray-200 font-mono">{u.email ?? '—'}</td>
                <td className="px-3 py-2">
                  <span
                    className={`inline-block px-2 py-0.5 text-[10px] font-medium rounded-md ${
                      u.role === 'admin'
                        ? 'bg-indigo-500/15 text-indigo-300 border border-indigo-500/30'
                        : 'bg-gray-700/30 text-gray-400 border border-gray-700/50'
                    }`}
                  >
                    {u.role}
                  </span>
                </td>
                <td className="px-3 py-2 text-gray-500 font-mono text-xs">
                  {u.created_at ? u.created_at.slice(0, 10) : '—'}
                </td>
                <td className="px-3 py-2 text-gray-500 font-mono text-xs">
                  {u.last_sign_in_at ? u.last_sign_in_at.slice(0, 10) : 'never'}
                </td>
                <td className="px-5 py-2 text-right">
                  <div className="inline-flex gap-2">
                    {u.role === 'user' ? (
                      <button
                        onClick={() => setRole(u, 'admin')}
                        className="text-xs text-indigo-400 hover:text-indigo-300"
                      >
                        Promote
                      </button>
                    ) : (
                      <button
                        onClick={() => setRole(u, 'user')}
                        className="text-xs text-gray-400 hover:text-amber-400"
                      >
                        Demote
                      </button>
                    )}
                    <button
                      onClick={() => removeUser(u)}
                      className="text-xs text-gray-500 hover:text-rose-400"
                    >
                      Delete
                    </button>
                  </div>
                </td>
              </tr>
            ))}
            {!loading && users.length === 0 && (
              <tr>
                <td colSpan={5} className="px-5 py-8 text-center text-sm text-gray-500">
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
