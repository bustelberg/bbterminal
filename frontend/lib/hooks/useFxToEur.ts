'use client';

import { useEffect, useMemo, useRef, useState } from 'react';
import { API_URL } from '../apiUrl';
import { apiFetch } from '../apiFetch';

type FxRate = { date: string; rate: number };

/** Build a date-aware EUR converter from a currency's daily rate history
 * (ECB convention: units per 1 EUR, so EUR = value / rate). Uses the latest
 * rate on/before the given date. */
function buildConverter(rates: FxRate[]): (value: number, date: string) => number {
  const sorted = [...rates].sort((a, b) => a.date.localeCompare(b.date));
  return (value: number, date: string) => {
    let lo = 0;
    let hi = sorted.length - 1;
    let idx = 0;
    while (lo <= hi) {
      const mid = (lo + hi) >> 1;
      if (sorted[mid].date <= date) { idx = mid; lo = mid + 1; }
      else hi = mid - 1;
    }
    const rate = sorted[idx]?.rate;
    return rate ? value / rate : value;
  };
}

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

/** Date-aware EUR converters for a SET of currencies (e.g. every distinct
 * currency in a holdings table). Fetches `/api/fx/history/{currency}` once per
 * non-EUR currency (DB-cached) and returns `Map<currency, toEur(value, date)>`.
 * A currency that's still loading (or has no history) is simply absent from the
 * map, so callers fall back to identity / a stored value. */
export function useFxConverters(
  currencies: (string | null | undefined)[],
): Map<string, (value: number, date: string) => number> {
  const wanted = useMemo(
    () => Array.from(new Set(
      currencies.map((c) => (c ?? '').toUpperCase()).filter((c) => c && c !== 'EUR'),
    )).sort(),
    [currencies],
  );
  const key = wanted.join(',');
  const [ratesByCcy, setRatesByCcy] = useState<Record<string, FxRate[]>>({});
  const fetchedRef = useRef<Set<string>>(new Set());

  useEffect(() => {
    let cancelled = false;
    for (const cur of key ? key.split(',') : []) {
      if (fetchedRef.current.has(cur)) continue;
      fetchedRef.current.add(cur);
      apiFetch(`${API_URL}/api/fx/history/${cur}`)
        .then((r) => (r.ok ? r.json() : null))
        .then((d) => {
          if (!cancelled && Array.isArray(d?.rates)) {
            setRatesByCcy((prev) => ({ ...prev, [cur]: d.rates as FxRate[] }));
          }
        })
        .catch(() => { fetchedRef.current.delete(cur); });
    }
    return () => { cancelled = true; };
  }, [key]);

  return useMemo(() => {
    const m = new Map<string, (value: number, date: string) => number>();
    for (const [cur, rates] of Object.entries(ratesByCcy)) {
      if (rates && rates.length > 0) m.set(cur, buildConverter(rates));
    }
    return m;
  }, [ratesByCcy]);
}
