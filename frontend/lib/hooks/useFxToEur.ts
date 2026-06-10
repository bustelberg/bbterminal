'use client';

import { useEffect, useMemo, useState } from 'react';
import { API_URL } from '../apiUrl';
import { apiFetch } from '../apiFetch';

type FxRate = { date: string; rate: number };

export type FxConverter = {
  /** Convert a native-currency value (as of `date`) to EUR. */
  toEur: (value: number, date: string) => number;
  /** False while the FX history is still loading (converter is identity until then). */
  ready: boolean;
  /** True when the currency is already EUR (or unknown) — no conversion needed. */
  isEur: boolean;
};

const IDENTITY = (v: number) => v;

/** Returns a converter from a company's native reporting currency to EUR.
 *
 * ECB rates are stored as "units of <currency> per 1 EUR", so EUR = native /
 * rate. For each value we use the latest rate on/before its date. EUR (or an
 * unknown/empty currency) → identity. Fetches `/api/fx/history/{currency}`
 * once per currency (DB-cached, fast); while loading, `ready` is false and the
 * converter is identity so the chart still renders (it re-renders to true EUR
 * once rates arrive). Loaded rates are tagged with their currency so a stale
 * fetch from a previous currency is never applied. */
export function useFxToEur(currency: string | null | undefined): FxConverter {
  const cur = (currency ?? '').toUpperCase();
  const isEur = cur === '' || cur === 'EUR';
  const [loaded, setLoaded] = useState<{ cur: string; rates: FxRate[] } | null>(null);

  useEffect(() => {
    if (isEur) return;
    let cancelled = false;
    apiFetch(`${API_URL}/api/fx/history/${cur}`)
      .then((r) => (r.ok ? r.json() : null))
      .then((d) => {
        if (!cancelled && Array.isArray(d?.rates)) setLoaded({ cur, rates: d.rates as FxRate[] });
      })
      .catch(() => { /* leave unloaded → identity fallback */ });
    return () => { cancelled = true; };
  }, [cur, isEur]);

  return useMemo<FxConverter>(() => {
    if (isEur) return { toEur: IDENTITY, ready: true, isEur: true };
    // Ignore rates left over from a previous currency.
    const rates = loaded && loaded.cur === cur ? loaded.rates : null;
    if (!rates || rates.length === 0) return { toEur: IDENTITY, ready: false, isEur: false };
    const sorted = [...rates].sort((a, b) => a.date.localeCompare(b.date));
    const toEur = (value: number, date: string) => {
      // Last rate with rate.date <= date (fall back to the earliest when the
      // value predates FX history).
      let lo = 0;
      let hi = sorted.length - 1;
      let idx = 0;
      while (lo <= hi) {
        const mid = (lo + hi) >> 1;
        if (sorted[mid].date <= date) { idx = mid; lo = mid + 1; }
        else hi = mid - 1;
      }
      const rate = sorted[idx].rate;
      return rate ? value / rate : value;
    };
    return { toEur, ready: true, isEur: false };
  }, [loaded, cur, isEur]);
}
