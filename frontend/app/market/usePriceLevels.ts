'use client';

import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { createClient } from '../../lib/supabase/client';
import type { MarketKey, PriceLevelRow } from './priceLevels';

export function usePriceLevels() {
  const client = useMemo(() => createClient(), []);
  const [userId, setUserId] = useState<string | null>(null);
  const [stored, setStored] = useState<{ owner: string; rows: PriceLevelRow[] } | null>(null);
  const [error, setError] = useState(false);
  const [saving, setSaving] = useState(false);
  const [reload, setReload] = useState(0);
  const writing = useRef(false);
  useEffect(() => {
    let alive = true;
    let authChanged = false;
    void client.auth.getUser().then(({ data, error }) => {
      if (alive && !authChanged) { setUserId(data.user?.id ?? null); if (error) setError(true); }
    }).catch(() => { if (alive && !authChanged) setError(true); });
    const { data } = client.auth.onAuthStateChange((_event, session) => {
      authChanged = true;
      if (alive) setUserId(session?.user.id ?? null);
    });
    return () => { alive = false; data.subscription.unsubscribe(); };
  }, [client, reload]);
  useEffect(() => {
    if (!userId) return;
    let alive = true;
    setError(false);
    void (async () => {
      const result = await client.from('market_price_level').select('market,drawdown_pct').eq('user_id', userId);
      if (!alive) return;
      if (result.error) { setError(true); return; }
      setStored({ owner: userId, rows: (result.data ?? []).map(row => ({
        market: row.market as MarketKey, drawdown_pct: Number(row.drawdown_pct),
      })) });
    })().catch(() => { if (alive) setError(true); });
    return () => { alive = false; };
  }, [client, userId, reload]);
  const ready = !!userId && stored?.owner === userId;
  const rows = ready ? stored.rows : [];
  const update = useCallback(async (market: MarketKey, drawdown: number, remove = false) => {
    if (!userId || !ready || writing.current) return false;
    const owner = userId;
    writing.current = true;
    setSaving(true);
    try {
      const result = remove
        ? await client.from('market_price_level').delete().eq('user_id', owner).eq('market', market).eq('drawdown_pct', drawdown)
        : await client.from('market_price_level').insert({ user_id: owner, market, drawdown_pct: drawdown });
      if (result.error) return false;
      setStored(current => current?.owner !== owner ? current : { owner, rows: remove
        ? current.rows.filter(row => row.market !== market || row.drawdown_pct !== drawdown)
        : [...current.rows, { market, drawdown_pct: drawdown }] });
      return true;
    } catch { return false; }
    finally { writing.current = false; setSaving(false); }
  }, [client, userId, ready]);
  return { rows, ready, saving, error, update, retry: () => setReload(n => n + 1) };
}
