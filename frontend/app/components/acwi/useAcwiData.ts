/**
 * `useAcwiData` — all data fetching + business-logic derivations for the
 * `/acwi` reconstruction-diagnostics view (everything BELOW the canonical
 * universe card).
 *
 * Lifted out of `AcwiUniverse.tsx` so the god-component stops owning three
 * coupled fetch domains (iShares holdings, MSCI announcements + per-detail
 * SSE stream, net additions) plus the derived sector/country/timeline/
 * detail-summary memos. The presentational table components consume the
 * returned raw + derived data and own only their local search/sort UI
 * state.
 *
 * The mount effect preserves the original orchestration exactly: load
 * holdings + announcements in parallel, then — if any constituent change
 * is missing its detail — kick off the module-scoped SSE detail stream
 * (which survives navigation) and reload announcements + net additions as
 * it completes; otherwise load net additions immediately.
 */
import { useState, useEffect, useMemo, useCallback } from 'react';

import { acwiFetchStore, startAcwiFetchDetails } from '../../../lib/stores/acwi';
import { trackedFetch } from '../../../lib/loading';
import { API_URL } from '../../../lib/apiUrl';
import {
  FEASIBLE_GF_EXCHANGES,
  type Announcement,
  type Detail,
  type DetailSummaryGroups,
  type Holding,
  type NetAddition,
  type TimelineRow,
} from './types';

export type UseAcwiDataResult = ReturnType<typeof useAcwiData>;

export function useAcwiData() {
  const [holdings, setHoldings] = useState<Holding[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [announcements, setAnnouncements] = useState<Announcement[]>([]);
  const [annLoading, setAnnLoading] = useState(true);
  const [annError, setAnnError] = useState<string | null>(null);
  // Manual detail fetches (keyed by href)
  const [manualDetails, setManualDetails] = useState<Record<string, Detail>>({});
  // fetch-all-details state lives in a module-scoped store so the SSE stream
  // keeps running when the user navigates away.
  const fetchProgress = acwiFetchStore.use((s) => s.progress);
  const fetching = acwiFetchStore.use((s) => s.fetching);
  const fetchSummary = acwiFetchStore.use((s) => s.summary);

  // Net additions
  const [netAdditions, setNetAdditions] = useState<NetAddition[]>([]);
  const [netAdditionsLoading, setNetAdditionsLoading] = useState(false);
  const [netAdditionsStats, setNetAdditionsStats] = useState<{ total: number; matched: number } | null>(null);

  const loadNetAdditions = useCallback(async () => {
    setNetAdditionsLoading(true);
    try {
      const res = await trackedFetch('Loading ACWI net additions', `${API_URL}/api/acwi/net-additions`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      setNetAdditions(data.net_additions);
      setNetAdditionsStats({ total: data.total, matched: data.matched });
    } catch {
      // silently fail — net additions are non-critical
    }
    setNetAdditionsLoading(false);
  }, []);

  const loadAnnouncements = useCallback(async () => {
    try {
      const res = await trackedFetch('Loading MSCI announcements', `${API_URL}/api/acwi/announcements`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      setAnnouncements(data.announcements);
      return data.announcements as Announcement[];
    } catch (e) {
      setAnnError(e instanceof Error ? e.message : 'Failed to load announcements');
      return [] as Announcement[];
    }
  }, []);

  useEffect(() => {
    (async () => {
      try {
        const res = await trackedFetch('Loading ACWI holdings', `${API_URL}/api/acwi/holdings`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        setHoldings(data.holdings);
      } catch (e) {
        setError(e instanceof Error ? e.message : 'Failed to load');
      }
      setLoading(false);
    })();
    (async () => {
      const anns = await loadAnnouncements();
      setAnnLoading(false);

      // Check if any constituent changes are missing details
      const needsFetch = anns.some(
        (a: Announcement) => a.is_constituent_change && a.href && !a.detail
      );
      if (needsFetch && !acwiFetchStore.get().fetching) {
        // Auto-trigger SSE fetch for uncached details — store owns the stream
        // so it keeps running when the user navigates away.
        startAcwiFetchDetails(() => {
          loadAnnouncements();
          loadNetAdditions();
        });
      } else {
        // All details already cached — load net additions immediately
        loadNetAdditions();
      }
    })();
  }, [loadAnnouncements, loadNetAdditions]);

  const fetchDetail = useCallback(async (href: string) => {
    setManualDetails(prev => ({ ...prev, [href]: { standard: null, effective_date: null, loading: true } }));
    try {
      const res = await trackedFetch(
        'Loading announcement detail',
        `${API_URL}/api/acwi/announcement-detail?url=${encodeURIComponent(href)}`,
      );
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      setManualDetails(prev => ({ ...prev, [href]: { ...data, loading: false } }));
    } catch (e) {
      setManualDetails(prev => ({ ...prev, [href]: { standard: null, effective_date: null, loading: false, error: e instanceof Error ? e.message : 'Failed' } }));
    }
  }, []);

  // Get detail for an announcement: from inline (server-cached) or manual fetch
  const getDetail = useCallback((a: Announcement): Detail | undefined => {
    return a.detail || manualDetails[a.href];
  }, [manualDetails]);

  const feasibleHoldings = useMemo(() => {
    return holdings.filter(h =>
      h.gf_exchange === null || FEASIBLE_GF_EXCHANGES.has(h.gf_exchange)
    );
  }, [holdings]);

  const additionTimeline = useMemo<TimelineRow[]>(() => {
    if (!feasibleHoldings.length) return [];
    const tickerToHolding = new Map<string, Holding>();
    for (const h of feasibleHoldings) tickerToHolding.set(h.Ticker, h);

    const rows = netAdditions
      .filter(na => na.matched && na.matched_ticker && na.effective_date && tickerToHolding.has(na.matched_ticker))
      .map(na => {
        const h = tickerToHolding.get(na.matched_ticker!)!;
        const ts = new Date(na.effective_date!).getTime();
        return {
          effective_date: na.effective_date!,
          ts: isNaN(ts) ? 0 : ts,
          ticker: na.matched_ticker!,
          name: h.Name,
          country: h.Location,
          cc: na.country,
          sector: h.Sector,
          gf_exchange: h.gf_exchange,
          gurufocus_url: h.gurufocus_url,
          href: na.href,
        };
      });

    // Dedupe by ticker, keep the earliest effective_date (first-time addition)
    const firstByTicker = new Map<string, TimelineRow>();
    for (const r of rows) {
      const prev = firstByTicker.get(r.ticker);
      if (!prev || r.ts < prev.ts) firstByTicker.set(r.ticker, r);
    }

    return Array.from(firstByTicker.values()).sort((a, b) => b.ts - a.ts);
  }, [feasibleHoldings, netAdditions]);

  const sectorBreakdown = useMemo(() => {
    const map: Record<string, { count: number; weight: number }> = {};
    for (const h of holdings) {
      const s = h.Sector || 'Unknown';
      if (!map[s]) map[s] = { count: 0, weight: 0 };
      map[s].count++;
      map[s].weight += parseFloat(h['Weight (%)']) || 0;
    }
    return Object.entries(map)
      .sort((a, b) => b[1].weight - a[1].weight);
  }, [holdings]);

  const countryBreakdown = useMemo(() => {
    const map: Record<string, { count: number; weight: number }> = {};
    for (const h of holdings) {
      const c = h.Location || 'Unknown';
      if (!map[c]) map[c] = { count: 0, weight: 0 };
      map[c].count++;
      map[c].weight += parseFloat(h['Weight (%)']) || 0;
    }
    return Object.entries(map)
      .sort((a, b) => b[1].weight - a[1].weight);
  }, [holdings]);

  // Summary of details grouped by action
  const detailSummary = useMemo<DetailSummaryGroups>(() => {
    const groups: DetailSummaryGroups = {
      ADDED: [],
      DELETED: [],
      'ADDED+DELETED': [],
      '-': [],
      'N/A': [],
    };
    for (const a of announcements) {
      if (!a.is_constituent_change) continue;
      const d = getDetail(a);
      if (!d || d.loading) continue;
      const action = d.standard || 'N/A';
      const key = action in groups ? action : 'N/A';
      groups[key].push({ announcement: a, detail: d });
    }
    return groups;
    // `getDetail` already closes over `manualDetails`, so listing both is
    // a redundant dep that ESLint flags. Drop `manualDetails`.
  }, [announcements, getDetail]);

  const otherCountryCoded = useMemo(() => {
    return announcements.filter(a => a.is_other_country_coded);
  }, [announcements]);

  const hasFetchedDetails = useMemo(() => {
    return announcements.some(a => a.detail) || Object.values(manualDetails).some(d => !d.loading);
  }, [announcements, manualDetails]);

  return {
    // raw holdings
    holdings,
    loading,
    error,
    // announcements
    announcements,
    annLoading,
    annError,
    getDetail,
    fetchDetail,
    // SSE detail stream
    fetchProgress,
    fetching,
    fetchSummary,
    // net additions
    netAdditions,
    netAdditionsLoading,
    netAdditionsStats,
    // derived
    feasibleHoldings,
    additionTimeline,
    sectorBreakdown,
    countryBreakdown,
    detailSummary,
    otherCountryCoded,
    hasFetchedDetails,
  };
}
