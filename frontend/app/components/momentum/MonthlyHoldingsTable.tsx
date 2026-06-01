'use client';

import { Fragment, memo, useEffect, useMemo, useRef, useState } from 'react';
import dynamic from 'next/dynamic';
import type { BacktestResult, Holding, PeriodRecord } from '../../../lib/stores/momentum';
import { API_URL } from '../../../lib/apiUrl';
import type { Column } from '../../../lib/tableExport';
import TableDownloadButton from '../TableDownloadButton';
import CellInfoTip from './CellInfoTip';
import CollapsibleCard from './CollapsibleCard';
// Heavy modal: SSE wiring, breakdown-fetch handlers, ~900 lines. Only
// mounts when a user clicks a row. next/dynamic puts it in a separate
// chunk that ships only when the modal is actually opened.
const TickerTimelineModal = dynamic(() => import('./TickerTimelineModal'), { ssr: false });
import { computeNetStats, parenPct } from './feeStats';
import { useClickOutside } from '../../../lib/hooks/useClickOutside';
import { useExchangeFeeMap } from '../../../lib/hooks/apiData';
import { EXCHANGE_NAMES, displayExchange, fmtPct, fmtPrice, guruFocusUrl } from './utils';

type HeldCompany = { company_id: number; ticker: string; company_name: string };

/** Subset of the active backtest's selection config that the per-ticker
 * timeline modal forwards to the signal-breakdown endpoint. Lets the
 * recompute see the same universe + weights the user actually ran. */
export type ScoringConfig = {
  universe_label: string | null;
  index_universe: string | null;
  signal_weights: Record<string, number>;
  category_weights: Record<string, number>;
};

type Props = {
  result: BacktestResult;
  categories: string[];
  exchangeByCompany: Map<number, string>;
  scoringConfig: ScoringConfig;
  /** Optional "go-live" date (YYYY-MM-DD). The one period whose window
   * contains it is split into two rows — the part of the period before
   * go-live and the part from go-live to the period end — with returns
   * recomputed from the daily curve. Used by /schedule. */
  markerDate?: string;
};

/** "Monthly Portfolios" card: one row per rebalance month, expandable to
 * show that month's holdings with per-stock returns and FX details. Owns
 * its own expansion state and the per-month turnover memo (which is only
 * read here). The parent feeds it the active backtest result; whenever
 * that changes — new run, loaded saved run, etc. — the table resets its
 * expansion automatically.
 */
function MonthlyHoldingsTableInner({ result, categories, exchangeByCompany, scoringConfig, markerDate }: Props) {
  const [expandedMonth, setExpandedMonth] = useState<string | null>(null);
  // company_id whose timeline modal is open, or null for closed.
  const [timelineCompanyId, setTimelineCompanyId] = useState<number | null>(null);
  // Per-company close price (local + EUR) at the go-live date, fetched
  // on demand so the split sub-period rows can show each holding's
  // entry/exit prices for ITS dates rather than the full month's.
  const [goLivePrices, setGoLivePrices] = useState<
    Record<string, { price_local: number; price_eur?: number; target_date: string }> | null
  >(null);
  // Re-price of the trailing OPEN period to the latest available close, so
  // the last row isn't frozen at the backtest's run-time as-of date when
  // newer price data exists. { date, prices } or null.
  const [openReprice, setOpenReprice] = useState<
    { date: string; prices: Record<string, { price_local: number; price_eur?: number; target_date: string }> } | null
  >(null);

  // Every distinct company ever held during this backtest — one entry per
  // company_id, regardless of how many months it appeared in. Drives the
  // header search box; an exact ticker match wins over a fuzzy name hit.
  const heldCompanies = useMemo<HeldCompany[]>(() => {
    const seen = new Map<number, HeldCompany>();
    for (const r of result.monthly_records) {
      for (const h of r.holdings) {
        if (!seen.has(h.company_id)) {
          seen.set(h.company_id, {
            company_id: h.company_id,
            ticker: h.ticker ?? '',
            company_name: h.company_name ?? '',
          });
        }
      }
    }
    return Array.from(seen.values()).sort((a, b) => a.ticker.localeCompare(b.ticker));
  }, [result]);

  // Per-exchange fees (bps) for the (net) parentheticals in the Return /
  // Cumulative columns. Fetched once on mount; null when the user hasn't
  // configured any non-zero fees, in which case every parenPct(...) call
  // below renders as the empty string (no parens, no visual noise).
  const feesByExchange = useExchangeFeeMap();

  // Net per-period + cumulative returns for the active result, keyed by
  // the same `r.date` the outer rows render. `computeNetStats` returns
  // null when fees aren't configured OR when the parent passed an empty
  // exchangeByCompany (e.g. ScheduleRunDetail) — both fall through to
  // gross-only display below.
  const netStats = useMemo(() => {
    if (!feesByExchange || exchangeByCompany.size === 0) return null;
    return computeNetStats(result.monthly_records, feesByExchange, exchangeByCompany, result.daily_records);
  }, [feesByExchange, exchangeByCompany, result.monthly_records, result.daily_records]);

  const netByDate = useMemo<Map<string, { portRet: number; cumRet: number }>>(() => {
    const m = new Map<string, { portRet: number; cumRet: number }>();
    if (!netStats) return m;
    for (let i = 0; i < netStats.dates.length; i++) {
      m.set(netStats.dates[i], {
        portRet: netStats.period_returns[i],
        cumRet: (netStats.cum_factors[i] - 1) * 100,
      });
    }
    return m;
  }, [netStats]);

  // One-way turnover per month: % of current holdings that weren't held
  // last month. First month has no prior portfolio → null.
  const turnoverByDate = useMemo<Record<string, number | null>>(() => {
    const map: Record<string, number | null> = {};
    let prevIds: Set<number> | null = null;
    for (const r of result.monthly_records) {
      const currIds = new Set(r.holdings.map((h) => h.company_id));
      if (prevIds === null || currIds.size === 0) {
        map[r.date] = null;
      } else {
        let added = 0;
        for (const id of currIds) if (!prevIds.has(id)) added += 1;
        map[r.date] = (added / currIds.size) * 100;
      }
      prevIds = currIds;
    }
    return map;
  }, [result]);

  // Fetch the go-live-date close prices for the holdings of the period
  // that contains the marker, so the two split sub-rows can re-price each
  // holding at the split boundary. Read-only; runs only when a marker is set.
  useEffect(() => {
    // Find the cids of the period that contains the marker (if any).
    const records = result.monthly_records;
    let cids: number[] = [];
    if (markerDate) {
      for (let i = 0; i < records.length; i++) {
        const start = records[i].date.slice(0, 10);
        const end = i + 1 < records.length ? records[i + 1].date.slice(0, 10) : (records[i].as_of_date ?? null);
        if (markerDate > start && (end == null || markerDate < end)) {
          cids = records[i].holdings.map((h) => h.company_id).filter((c): c is number => c != null);
          break;
        }
      }
    }
    if (cids.length === 0) {
      // eslint-disable-next-line react-hooks/set-state-in-effect
      setGoLivePrices(null);
      return;
    }
    // Stale prices from a previous marker can't be mis-applied — they're
    // keyed by company_id, and a different period has different cids — so
    // we just overwrite on resolve without an interim reset.
    let cancelled = false;
    fetch(`${API_URL}/api/momentum/prices-at?as_of=${markerDate}&company_ids=${cids.join(',')}`)
      .then((r) => (r.ok ? r.json() : null))
      .then((d: { prices?: Record<string, { price_local: number; price_eur?: number; target_date: string }> } | null) => {
        if (!cancelled && d?.prices) setGoLivePrices(d.prices);
      })
      .catch(() => { /* split rows fall back to full-period prices */ });
    return () => { cancelled = true; };
  }, [markerDate, result]);

  // Re-price the trailing OPEN period to its freshest *common* date — the
  // most recent date with a close for EVERY held name (the engine's
  // definition, shown in the row tooltip). NOT the global latest: if 23 of
  // 24 names only have data through the 26th and one has the 28th, the
  // honest "as of" is the 26th, since the portfolio return needs all names
  // priced on the same day. Only fires when that common date is genuinely
  // newer than the backtest's saved as-of.
  type PriceMap = Record<string, { price_local: number; price_eur?: number; target_date: string }>;
  useEffect(() => {
    const records = result.monthly_records;
    const last = records[records.length - 1];
    if (!last || !last.is_open || last.holdings.length === 0) {
      // eslint-disable-next-line react-hooks/set-state-in-effect
      setOpenReprice(null);
      return;
    }
    const cids = last.holdings.map((h) => h.company_id).filter((c): c is number => c != null);
    if (cids.length === 0) {
      setOpenReprice(null);
      return;
    }
    const savedAsOf = last.as_of_date?.slice(0, 10);
    let cancelled = false;
    (async () => {
      try {
        const ld = await fetch(`${API_URL}/api/data/latest-price-date`).then((r) => (r.ok ? r.json() : null));
        const globalLatest: string | undefined = ld?.date;
        if (!globalLatest || (savedAsOf && globalLatest <= savedAsOf)) return;
        // Each name's latest close on/before today — its own freshest mark.
        const pr: { prices?: PriceMap } | null = await fetch(
          `${API_URL}/api/momentum/prices-at?as_of=${globalLatest}&company_ids=${cids.join(',')}`,
        ).then((r) => (r.ok ? r.json() : null));
        const dates = Object.values(pr?.prices ?? {}).map((p) => p.target_date.slice(0, 10));
        if (dates.length === 0 || !pr?.prices) return;
        // Header date = the freshest mark present in the portfolio. Each
        // holding is shown at ITS OWN latest below; a name with no newer
        // trade keeps its existing (older) close. Only re-price when at
        // least one name has data newer than the backtest's saved as-of.
        const maxHeld = dates.reduce((mx, d) => (d > mx ? d : mx), dates[0]);
        if (savedAsOf && maxHeld <= savedAsOf) return;
        if (!cancelled) setOpenReprice({ date: maxHeld, prices: pr.prices });
      } catch {
        /* leave the open period at its backtest as-of date */
      }
    })();
    return () => { cancelled = true; };
  }, [result]);

  // Records with the trailing open period re-priced to the latest close
  // (when newer data than the backtest's as-of exists). Re-prices each
  // open holding, then recomputes the period return (long weight-mean −
  // short weight-mean, matching the engine) + the chained cumulative.
  const repricedRecords = useMemo<PeriodRecord[]>(() => {
    const recs = result.monthly_records;
    if (!openReprice || recs.length === 0) return recs;
    const lastIdx = recs.length - 1;
    const last = recs[lastIdx];
    if (!last.is_open) return recs;
    const newHoldings = last.holdings.map((h) => {
      const p = openReprice.prices[String(h.company_id)];
      if (!p) return h;
      const exitEur = p.price_eur ?? h.exit_price_eur;
      const entryEur = h.entry_price_eur;
      return {
        ...h,
        exit_price_local: p.price_local,
        exit_price_eur: exitEur,
        exit_date: p.target_date.slice(0, 10),
        forward_return_pct: exitEur != null && entryEur ? (exitEur / entryEur - 1) * 100 : h.forward_return_pct,
      };
    });
    const wmean = (arr: Holding[]): number | null => {
      let w = 0, r = 0;
      for (const h of arr) {
        if (h.forward_return_pct == null) continue;
        const wt = h.weight ?? 1;
        w += wt; r += wt * h.forward_return_pct;
      }
      return w > 0 ? r / w : null;
    };
    const longRet = wmean(newHoldings.filter((h) => h.side !== 'short'));
    const shorts = newHoldings.filter((h) => h.side === 'short');
    const shortRet = shorts.length ? wmean(shorts) : null;
    const portRet = longRet != null ? (shortRet != null ? longRet - shortRet : longRet) : last.portfolio_return_pct;
    const prevCum = lastIdx >= 1 ? (recs[lastIdx - 1].cumulative_return_pct ?? 0) : 0;
    const cum = portRet != null ? ((1 + prevCum / 100) * (1 + portRet / 100) - 1) * 100 : last.cumulative_return_pct;
    const reLast: PeriodRecord = {
      ...last,
      holdings: newHoldings,
      portfolio_return_pct: portRet,
      cumulative_return_pct: cum,
      as_of_date: openReprice.date,
    };
    return [...recs.slice(0, lastIdx), reLast];
  }, [result, openReprice]);

  // Display rows = the period records, except the ONE period whose window
  // contains the go-live date is split into two: the slice before go-live
  // and the slice from go-live to the period end. Per-period + cumulative
  // returns (strategy and universe) are recomputed from the daily curve so
  // each slice reads like a standalone period. Holdings are identical for
  // both slices (no rebalance happens at go-live).
  const displayRows = useMemo<{
    row: PeriodRecord;
    key: string;
    label: string | null;
    turnoverDate: string | null;
    net: boolean;
  }[]>(() => {
    const records = repricedRecords;
    const passthrough = () =>
      records.map((r) => ({ row: r, key: r.date, label: null, turnoverDate: r.date, net: true }));
    if (!markerDate) return passthrough();

    const stratSeries = (result.daily_records ?? []).map((d) => ({ date: d.date.slice(0, 10), cum: d.cumulative_return_pct }));
    const uniSeries = (result.universe_daily_records ?? []).map((d) => ({ date: d.date.slice(0, 10), cum: d.cumulative_return_pct }));
    if (stratSeries.length === 0) return passthrough();

    const cumAt = (series: { date: string; cum: number }[], date: string): number | null => {
      let v: number | null = null;
      for (const p of series) { if (p.date <= date) v = p.cum; else break; }
      return v;
    };
    const rel = (a: number | null, b: number | null): number | null =>
      a != null && b != null ? ((1 + a / 100) / (1 + b / 100) - 1) * 100 : null;

    const out: { row: PeriodRecord; key: string; label: string | null; turnoverDate: string | null; net: boolean }[] = [];
    for (let i = 0; i < records.length; i++) {
      const r = records[i];
      const start = r.date.slice(0, 10);
      const end = i + 1 < records.length ? records[i + 1].date.slice(0, 10) : (r.as_of_date ?? null);
      const inside = markerDate > start && (end == null || markerDate < end);
      if (!inside) { out.push({ row: r, key: r.date, label: null, turnoverDate: r.date, net: true }); continue; }

      const cStart = cumAt(stratSeries, start);
      const cGo = cumAt(stratSeries, markerDate);
      const cEnd = end ? cumAt(stratSeries, end) : stratSeries[stratSeries.length - 1].cum;
      const uStart = cumAt(uniSeries, start);
      const uGo = cumAt(uniSeries, markerDate);
      const uEnd = end ? cumAt(uniSeries, end) : (uniSeries.length ? uniSeries[uniSeries.length - 1].cum : null);

      // Re-price each holding at the go-live boundary so the expanded
      // detail shows entry/exit prices for the sub-period's own dates.
      // `pre` ends at go-live (exit = go-live price); `post` starts at
      // go-live (entry = go-live price). Falls back to the original holding
      // when the go-live price isn't (yet) available for a cid.
      const repricePre = (h: Holding): Holding => {
        const p = goLivePrices?.[String(h.company_id)];
        if (!p) return h;
        const exitEur = p.price_eur ?? h.exit_price_eur;
        const entryEur = h.entry_price_eur;
        return {
          ...h,
          exit_price_local: p.price_local,
          exit_price_eur: exitEur,
          exit_date: markerDate,
          forward_return_pct: exitEur != null && entryEur ? (exitEur / entryEur - 1) * 100 : h.forward_return_pct,
        };
      };
      const repricePost = (h: Holding): Holding => {
        const p = goLivePrices?.[String(h.company_id)];
        if (!p) return h;
        const entryEur = p.price_eur ?? h.entry_price_eur;
        const exitEur = h.exit_price_eur;
        return {
          ...h,
          entry_price_local: p.price_local,
          entry_price_eur: entryEur,
          entry_date: markerDate,
          forward_return_pct: entryEur != null && exitEur ? (exitEur / entryEur - 1) * 100 : h.forward_return_pct,
        };
      };

      const pre: PeriodRecord = {
        ...r,
        holdings: r.holdings.map(repricePre),
        portfolio_return_pct: rel(cGo, cStart),
        universe_return_pct: rel(uGo, uStart),
        cumulative_return_pct: cGo ?? r.cumulative_return_pct,
        universe_cumulative_return_pct: uGo,
        is_open: false,
        as_of_date: undefined,
      };
      const post: PeriodRecord = {
        ...r,
        holdings: r.holdings.map(repricePost),
        portfolio_return_pct: rel(cEnd, cGo),
        universe_return_pct: rel(uEnd, uGo),
        cumulative_return_pct: cEnd ?? r.cumulative_return_pct,
        universe_cumulative_return_pct: uEnd,
        is_open: r.is_open,
        as_of_date: r.as_of_date,
      };
      out.push({ row: pre, key: `${r.date}__pre`, label: `${start} → ${markerDate} · pre go-live`, turnoverDate: r.date, net: false });
      out.push({ row: post, key: `${r.date}__post`, label: `${markerDate} → ${end ?? 'now'} · go-live →`, turnoverDate: null, net: false });
    }
    return out;
  }, [repricedRecords, result, markerDate, goLivePrices]);

  // When the active result changes (new run / loaded saved run) collapse
  // any open month so the user starts at a clean view. React 19's
  // recommended pattern for "reset state when a prop changes" is to
  // track the prior value and reset during render — no effect needed.
  // https://react.dev/reference/react/useState#storing-information-from-previous-renders
  const [lastResult, setLastResult] = useState(result);
  if (result !== lastResult) {
    setLastResult(result);
    setExpandedMonth(null);
  }

  // Flatten the nested period × holdings tree into one row per holding.
  // Each row carries its period label so a spreadsheet user can group/
  // pivot without needing the visual indentation. Empty periods are
  // skipped — they contribute nothing to a holdings export.
  type FlatHolding = {
    period: string;
    is_open: boolean;
    side: string;
    ticker: string;
    exchange: string;
    company_name: string;
    sector: string;
    category_scores: Record<string, number | null>;
    score: number;
    entry_price_local: number | null;
    exit_price_local: number | null;
    currency: string;
    entry_price_eur: number | null;
    exit_price_eur: number | null;
    entry_date: string;
    exit_date: string;
    forward_return_pct: number | null;
  };
  const flatHoldings = useMemo<FlatHolding[]>(() => {
    const out: FlatHolding[] = [];
    for (const r of result.monthly_records) {
      for (const h of r.holdings) {
        out.push({
          period: r.date,
          is_open: !!r.is_open,
          side: h.side ?? 'long',
          ticker: h.ticker ?? '',
          exchange: exchangeByCompany.get(h.company_id) ?? '',
          company_name: h.company_name ?? '',
          sector: h.sector ?? '',
          category_scores: h.category_scores ?? {},
          score: h.score,
          entry_price_local: h.entry_price_local ?? null,
          exit_price_local: h.exit_price_local ?? null,
          currency: h.currency ?? '',
          entry_price_eur: h.entry_price_eur ?? null,
          exit_price_eur: h.exit_price_eur ?? null,
          entry_date: h.entry_date ?? '',
          exit_date: h.exit_date ?? '',
          forward_return_pct: h.forward_return_pct ?? null,
        });
      }
    }
    return out;
  }, [result, exchangeByCompany]);

  // Category-score columns are dynamic (price/volume usually; some
  // strategies add more). Spread them between the fixed Score and
  // price columns so the export mirrors the on-screen layout.
  const exportColumns = useMemo<Column<FlatHolding>[]>(() => {
    const cols: Column<FlatHolding>[] = [
      { key: 'period', header: 'Period', accessor: (r) => r.period },
      { key: 'side', header: 'Side', accessor: (r) => r.side },
      { key: 'ticker', header: 'Ticker', accessor: (r) => r.ticker },
      { key: 'exchange', header: 'Exchange', accessor: (r) => r.exchange },
      { key: 'company_name', header: 'Company', accessor: (r) => r.company_name },
      { key: 'sector', header: 'Sector', accessor: (r) => r.sector },
    ];
    for (const cat of categories) {
      cols.push({
        key: `score_${cat}`,
        header: cat === 'price' ? 'Price score' : cat === 'volume' ? 'Volume score' : `${cat} score`,
        accessor: (r) => r.category_scores[cat] ?? null,
      });
    }
    cols.push(
      { key: 'total_score', header: 'Total score', accessor: (r) => r.score },
      { key: 'currency', header: 'Currency', accessor: (r) => r.currency },
      { key: 'entry_price_local', header: 'Start (local)', accessor: (r) => r.entry_price_local },
      { key: 'exit_price_local', header: 'End (local)', accessor: (r) => r.exit_price_local },
      { key: 'entry_price_eur', header: 'Start (EUR)', accessor: (r) => r.entry_price_eur },
      { key: 'exit_price_eur', header: 'End (EUR)', accessor: (r) => r.exit_price_eur },
      { key: 'entry_date', header: 'Entry date', accessor: (r) => r.entry_date },
      { key: 'exit_date', header: 'Exit date', accessor: (r) => r.exit_date },
      { key: 'return_pct', header: 'Return (%)', accessor: (r) => r.forward_return_pct },
      { key: 'gurufocus_url', header: 'GuruFocus URL', accessor: (r) => r.ticker ? guruFocusUrl(r.ticker, r.exchange) : '' },
    );
    return cols;
  }, [categories]);

  return (
    <>
    <CollapsibleCard
      title="Portfolios"
      rightSlot={
        <div className="flex items-center gap-2">
          <CompanySearch
            companies={heldCompanies}
            onPick={(cid) => setTimelineCompanyId(cid)}
          />
          <TableDownloadButton
            rows={flatHoldings}
            columns={exportColumns}
            filename="portfolio_holdings"
            title={`Download ${flatHoldings.length} holdings (${result.monthly_records.length} periods) as CSV / XLSX`}
          />
        </div>
      }
    >
      {/* Fill the viewport from where the card sits. `calc(100vh-12rem)`
          leaves ~12rem (192px) for the page header chrome + the card's own
          header bar, so when the user expands this card it spans the rest
          of the screen instead of capping at a fixed 500px window. The
          sticky thead inside this container keeps column labels pinned
          while the user scrolls through tall portfolios. */}
      <div className="max-h-[calc(100vh-12rem)] overflow-auto border-t border-gray-800/40">
        <table className="w-full text-sm">
          <thead className="sticky top-0 bg-[#151821] z-20">
            <tr className="text-gray-500 text-xs border-b border-gray-800/40">
              <th className="text-left px-5 py-2.5 font-medium">
                Period<CellInfoTip>Rebalance period start. The strategy enters this period&apos;s portfolio at the first trading day and holds until the next rebalance. Format is YYYY-MM for monthly+ cadences and YYYY-MM-DD for daily/weekly.</CellInfoTip>
              </th>
              <th className="text-right px-3 py-2.5 font-medium">
                Holdings<CellInfoTip>Number of stocks in the portfolio for this period (equal-weighted). Long-only: top_n_sectors × top_n_per_sector. Long-short: same on each side, so total is up to 2×.</CellInfoTip>
              </th>
              <th className="text-right px-3 py-2.5 font-medium">
                Return<CellInfoTip>Equal-weighted portfolio return for this period in EUR. Long-only: mean of holdings&apos; (exit ÷ entry) − 1. Long-short: long-side mean minus short-side mean.</CellInfoTip>
              </th>
              <th className="text-right px-3 py-2.5 font-medium">
                Universe<CellInfoTip>&quot;What if you held the entire eligible universe equal-weighted?&quot; — the no-skill baseline. Same entry→exit window as the strategy. Compare against Return to see whether the picks added value vs. just being in the market.</CellInfoTip>
              </th>
              <th className="text-right px-3 py-2.5 font-medium">
                Alpha<CellInfoTip>Return minus Universe for this period. Positive = the picks beat the equal-weighted universe over the same window; negative = the universe would have done better. &quot;—&quot; when either side has no return for the period.</CellInfoTip>
              </th>
              <th className="text-right px-3 py-2.5 font-medium">
                Turnover<CellInfoTip>Percentage of this period&apos;s holdings that weren&apos;t held in the previous period. 0% means the strategy held the same portfolio; 100% means it replaced everything.</CellInfoTip>
              </th>
              <th className="text-right px-3 py-2.5 font-medium">
                Cumulative<CellInfoTip>Cumulative return through the end of this period, since the backtest start: chain-linked product of all prior period returns.</CellInfoTip>
              </th>
              <th className="text-right px-5 py-2.5 font-medium">
                Universe cum<CellInfoTip>Cumulative universe-baseline return — chain-linked from the per-period &quot;Universe&quot; column. Compare against Cumulative: the difference is the strategy&apos;s alpha vs. holding everything.</CellInfoTip>
              </th>
            </tr>
          </thead>
          <tbody>
            {displayRows.map(({ row: r, key: rowKey, label: rowLabel, turnoverDate: rowTurnover, net: showNet }) => (
              <Fragment key={rowKey}>
                <tr
                  className={`border-b border-gray-800/20 hover:bg-white/[0.02] cursor-pointer transition-colors ${rowLabel ? 'border-l-2 border-l-rose-500/60' : ''}`}
                  onClick={() => setExpandedMonth(expandedMonth === rowKey ? null : rowKey)}
                >
                  <td className="px-5 py-2.5 text-gray-300 font-mono">
                    <span className="text-gray-600 mr-2">{expandedMonth === rowKey ? '▾' : '▸'}</span>
                    {rowLabel ? <span className="text-rose-300/90 text-xs">{rowLabel}</span> : r.date}
                    {r.is_open && (
                      <span
                        className="ml-2 inline-flex items-center text-[10px] uppercase tracking-wider px-1.5 py-0.5 rounded bg-amber-500/15 text-amber-300 border border-amber-500/30"
                        title={
                          r.as_of_date
                            ? `Open period — valued through ${r.as_of_date}. Each held name uses its own latest available close; names with no newer trade keep their last price, so some end dates may be earlier.`
                            : 'Open period — return reflects the partial window from the last rebalance through today'
                        }
                      >
                        open · {r.as_of_date ? `as of ${r.as_of_date}` : 'YTD'}
                      </span>
                    )}
                  </td>
                  <td className="text-right px-3 py-2.5 text-gray-400 font-mono">{r.holdings.length}</td>
                  <td className={`text-right px-3 py-2.5 font-mono ${r.portfolio_return_pct != null ? (r.portfolio_return_pct >= 0 ? 'text-emerald-400' : 'text-rose-400') : 'text-gray-600'}`}>
                    {fmtPct(r.portfolio_return_pct)}
                    <span className="text-gray-500">{parenPct(showNet ? netByDate.get(r.date)?.portRet : undefined)}</span>
                  </td>
                  <td
                    className="text-right px-3 py-2.5 font-mono text-gray-500"
                    title={
                      r.universe_constituents != null
                        ? `Equal-weight across ${r.universe_constituents} eligible companies for this period`
                        : undefined
                    }
                  >
                    {r.universe_return_pct != null ? fmtPct(r.universe_return_pct) : '—'}
                  </td>
                  {(() => {
                    // Per-period alpha — render only when BOTH legs are
                    // available. Render with explicit sign + emerald/rose
                    // tint so the column reads as a verdict, not a raw
                    // number.
                    const alpha =
                      r.portfolio_return_pct != null && r.universe_return_pct != null
                        ? r.portfolio_return_pct - r.universe_return_pct
                        : null;
                    return (
                      <td
                        className={`text-right px-3 py-2.5 font-mono font-medium ${
                          alpha == null
                            ? 'text-gray-600'
                            : alpha >= 0
                            ? 'text-emerald-400'
                            : 'text-rose-400'
                        }`}
                        title={
                          alpha != null
                            ? 'Strategy return minus universe return for this period.'
                            : 'Alpha unavailable — either the strategy had no return or no eligible universe baseline for this period.'
                        }
                      >
                        {alpha == null ? '—' : `${alpha >= 0 ? '+' : ''}${alpha.toFixed(2)}%`}
                      </td>
                    );
                  })()}
                  <td className="text-right px-3 py-2.5 font-mono text-gray-400">
                    {rowTurnover == null
                      ? '0.0%'
                      : turnoverByDate[rowTurnover] != null ? `${turnoverByDate[rowTurnover]!.toFixed(1)}%` : '—'}
                  </td>
                  <td className={`text-right px-3 py-2.5 font-mono ${r.cumulative_return_pct >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                    {fmtPct(r.cumulative_return_pct)}
                    <span className="text-gray-500">{parenPct(showNet ? netByDate.get(r.date)?.cumRet : undefined)}</span>
                  </td>
                  <td className="text-right px-5 py-2.5 font-mono text-gray-500">
                    {r.universe_cumulative_return_pct != null ? fmtPct(r.universe_cumulative_return_pct) : '—'}
                  </td>
                </tr>
                {expandedMonth === rowKey && r.holdings.length > 0 && (
                  <tr key={`${rowKey}-detail`}>
                    <td colSpan={8} className="bg-[#0f1117] px-5 py-3">
                      <table className="w-full text-xs">
                        <thead>
                          <tr className="text-gray-600">
                            <th className="text-left py-1 font-medium">
                              Side<CellInfoTip>Direction of the position. Long-only backtests are all &quot;Long&quot;. Long-short backtests group longs at the top and shorts at the bottom of each portfolio.</CellInfoTip>
                            </th>
                            <th className="text-left py-1 font-medium">
                              Ticker<CellInfoTip>The stock&apos;s ticker on its primary exchange. Click to open in GuruFocus.</CellInfoTip>
                            </th>
                            <th className="text-left py-1 font-medium">
                              Exchange<CellInfoTip>GuruFocus exchange code (e.g. NYSE, NASDAQ, HKSE, XTER). US-listed names use the bare ticker on GuruFocus; everything else is referenced as `EXCHANGE:TICKER`.</CellInfoTip>
                            </th>
                            <th className="text-left py-1 font-medium">
                              Company<CellInfoTip>Issuer name. Click to open in GuruFocus.</CellInfoTip>
                            </th>
                            <th className="text-left py-1 font-medium">
                              Sector<CellInfoTip>GICS sector. Selection picks top sectors then top stocks within each.</CellInfoTip>
                            </th>
                            <th className="text-right py-1 font-medium whitespace-nowrap">
                              Sector Rank<CellInfoTip>This sector&apos;s position among the top sectors picked this period (1 = highest-scoring sector by average company score). Range: 1..top_n_sectors.</CellInfoTip>
                            </th>
                            <th className="text-right py-1 font-medium whitespace-nowrap">
                              Company Rank<CellInfoTip>This company&apos;s position within its sector (1 = top-scoring stock in the sector). Range: 1..top_n_per_sector.</CellInfoTip>
                            </th>
                            {categories.map((cat) => (
                              <th key={cat} className="text-right py-1 font-medium">
                                {cat === 'price' ? 'Price' : cat === 'volume' ? 'Vol' : cat}
                                <CellInfoTip>
                                  {cat === 'price'
                                    ? 'Composite 0–100 score across the price-momentum signals, min-max normalized within the universe at this date.'
                                    : cat === 'volume'
                                    ? 'Composite 0–100 score across the volume signals, min-max normalized within the universe at this date.'
                                    : `${cat} category score, 0–100 normalized across the universe.`}
                                </CellInfoTip>
                              </th>
                            ))}
                            <th className="text-right py-1 font-medium">
                              Total<CellInfoTip>Weighted combination of the category scores. Selection ranks by this.</CellInfoTip>
                            </th>
                            <th className="text-right py-1 font-medium pl-4">
                              Start (local)<CellInfoTip>Entry price in local currency at the first trading day of this period.</CellInfoTip>
                            </th>
                            <th className="text-right py-1 font-medium">
                              End (local)<CellInfoTip>Exit price in local currency at the first trading day of the next period.</CellInfoTip>
                            </th>
                            <th className="text-right py-1 font-medium pl-4">
                              Start (€)<CellInfoTip>Entry price converted to EUR using the day&apos;s ECB FX rate.</CellInfoTip>
                            </th>
                            <th className="text-right py-1 font-medium">
                              End (€)<CellInfoTip>Exit price converted to EUR using the day&apos;s ECB FX rate.</CellInfoTip>
                            </th>
                            <th className="text-right py-1 font-medium pl-4">
                              Return<CellInfoTip>Per-stock price return in EUR over this period: (End € ÷ Start €) − 1. For shorts the period contribution is the negation of this.</CellInfoTip>
                            </th>
                          </tr>
                        </thead>
                        <tbody>
                          {[...r.holdings]
                            .sort((a, b) => {
                              // Longs first (top half), shorts second (bottom).
                              const sideA = a.side === 'short' ? 1 : 0;
                              const sideB = b.side === 'short' ? 1 : 0;
                              if (sideA !== sideB) return sideA - sideB;
                              // Prefer the engine's selection ranks when
                              // present so the table reflects "1st best
                              // sector → 1st best stock → 2nd best stock…".
                              // Fall back to (sector alpha, score desc) for
                              // legacy snapshots persisted before ranks
                              // existed AND for sector-ETF mode where ranks
                              // aren't computed.
                              const aHas = a.sector_rank != null && a.company_rank != null;
                              const bHas = b.sector_rank != null && b.company_rank != null;
                              if (aHas && bHas) {
                                const sr = a.sector_rank! - b.sector_rank!;
                                if (sr !== 0) return sr;
                                return a.company_rank! - b.company_rank!;
                              }
                              if (aHas !== bHas) return aHas ? -1 : 1;
                              const sec = a.sector.localeCompare(b.sector);
                              return sec !== 0 ? sec : b.score - a.score;
                            })
                            .map((h) => {
                              const exchRaw = exchangeByCompany.get(h.company_id) ?? '';
                              const exch = displayExchange(exchRaw, h.ticker);
                              const href = guruFocusUrl(h.ticker, exchRaw);
                              const isShort = h.side === 'short';
                              return (
                                <tr key={`${h.side ?? 'long'}-${h.company_id}`} className={`border-t border-gray-800/20 ${isShort ? 'bg-rose-500/[0.04]' : ''}`}>
                                  <td className="py-1.5 pr-2 whitespace-nowrap">
                                    <span
                                      className={`inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-medium ${
                                        isShort
                                          ? 'bg-rose-500/15 text-rose-300 border border-rose-500/30'
                                          : 'bg-emerald-500/10 text-emerald-300 border border-emerald-500/25'
                                      }`}
                                    >
                                      {isShort ? 'Short' : 'Long'}
                                    </span>
                                  </td>
                                  <td className="py-1.5 font-mono whitespace-nowrap">
                                    <button
                                      type="button"
                                      onClick={() => setTimelineCompanyId(h.company_id)}
                                      className="mr-1.5 inline-flex w-3.5 h-3.5 items-center justify-center text-gray-500 hover:text-indigo-300 transition-colors align-middle"
                                      title={`Show ${h.ticker} holding history across the backtest`}
                                      aria-label={`Show ${h.ticker} timeline`}
                                    >
                                      <svg viewBox="0 0 16 16" fill="currentColor" className="w-3 h-3">
                                        <path d="M2 13h12v1H2v-1zm0-3h2v2H2v-2zm3-2h2v4H5V8zm3-3h2v7H8V5zm3-2h2v9h-2V3z" />
                                      </svg>
                                    </button>
                                    <a
                                      href={href}
                                      target="_blank"
                                      rel="noopener noreferrer"
                                      className="text-indigo-400 hover:text-indigo-300 hover:underline"
                                    >
                                      {h.ticker}
                                    </a>
                                    {exch && (
                                      <span
                                        className="ml-1 text-[10px] text-gray-500"
                                        title={EXCHANGE_NAMES[exch.toUpperCase()] ?? exch}
                                      >
                                        ({exch})
                                      </span>
                                    )}
                                  </td>
                                  <td
                                    className="py-1.5 text-gray-400 font-mono whitespace-nowrap"
                                    title={exch ? (EXCHANGE_NAMES[exch.toUpperCase()] ?? exch) : ''}
                                  >
                                    {exch || '—'}
                                  </td>
                                  <td className="py-1.5 truncate max-w-[200px]">
                                    <a
                                      href={href}
                                      target="_blank"
                                      rel="noopener noreferrer"
                                      className="text-gray-300 hover:text-indigo-300 hover:underline"
                                    >
                                      {h.company_name}
                                    </a>
                                  </td>
                                  <td className="py-1.5 text-gray-500">{h.sector}</td>
                                  <td className="text-right py-1.5 font-mono">
                                    {h.sector_rank != null ? (
                                      <span className="text-indigo-300">{h.sector_rank}</span>
                                    ) : (
                                      <span className="text-gray-600 text-[10px]">—</span>
                                    )}
                                  </td>
                                  <td className="text-right py-1.5 font-mono">
                                    {h.company_rank != null ? (
                                      <span className="text-gray-200">{h.company_rank}</span>
                                    ) : (
                                      <span className="text-gray-600 text-[10px]">—</span>
                                    )}
                                  </td>
                                  {categories.map((cat) => (
                                    <td key={cat} className="text-right py-1.5 text-gray-400 font-mono">
                                      {h.category_scores?.[cat] != null ? h.category_scores[cat]!.toFixed(0) : '—'}
                                    </td>
                                  ))}
                                  <td className="text-right py-1.5 text-white font-mono font-medium">{h.score.toFixed(1)}</td>
                                  <td className="text-right py-1.5 text-gray-400 font-mono pl-4">
                                    {fmtPrice(h.entry_price_local)}
                                    {h.currency && <span className="text-gray-600 text-[10px] ml-1">{h.currency}</span>}
                                    {h.entry_date && (
                                      <CellInfoTip>
                                        <div className="text-gray-400">Trading date</div>
                                        <div className="font-mono text-gray-200">{h.entry_date}</div>
                                      </CellInfoTip>
                                    )}
                                  </td>
                                  <td className="text-right py-1.5 text-gray-400 font-mono">
                                    {fmtPrice(h.exit_price_local)}
                                    {h.exit_date && (
                                      <CellInfoTip>
                                        <div className="text-gray-400">Trading date</div>
                                        <div className="font-mono text-gray-200">{h.exit_date}</div>
                                      </CellInfoTip>
                                    )}
                                  </td>
                                  <td className="text-right py-1.5 text-gray-400 font-mono pl-4">
                                    {fmtPrice(h.entry_price_eur)}
                                    {(h.entry_date || (h.entry_price_eur != null && h.entry_price_local)) && (
                                      <CellInfoTip>
                                        {h.entry_date && (
                                          <>
                                            <div className="text-gray-400">Trading date</div>
                                            <div className="font-mono text-gray-200 mb-1">{h.entry_date}</div>
                                          </>
                                        )}
                                        {h.entry_price_eur != null && h.entry_price_local && h.entry_price_local > 0 && (
                                          <>
                                            <div className="text-gray-400">FX rate</div>
                                            <div className="font-mono text-gray-200">
                                              1 {h.currency ?? 'LCL'} = {(h.entry_price_eur / h.entry_price_local).toFixed(4)} EUR
                                            </div>
                                          </>
                                        )}
                                      </CellInfoTip>
                                    )}
                                  </td>
                                  <td className="text-right py-1.5 text-gray-400 font-mono">
                                    {fmtPrice(h.exit_price_eur)}
                                    {(h.exit_date || (h.exit_price_eur != null && h.exit_price_local)) && (
                                      <CellInfoTip>
                                        {h.exit_date && (
                                          <>
                                            <div className="text-gray-400">Trading date</div>
                                            <div className="font-mono text-gray-200 mb-1">{h.exit_date}</div>
                                          </>
                                        )}
                                        {h.exit_price_eur != null && h.exit_price_local && h.exit_price_local > 0 && (
                                          <>
                                            <div className="text-gray-400">FX rate</div>
                                            <div className="font-mono text-gray-200">
                                              1 {h.currency ?? 'LCL'} = {(h.exit_price_eur / h.exit_price_local).toFixed(4)} EUR
                                            </div>
                                          </>
                                        )}
                                      </CellInfoTip>
                                    )}
                                  </td>
                                  <td className={`text-right py-1.5 font-mono pl-4 ${h.forward_return_pct != null ? (h.forward_return_pct >= 0 ? 'text-emerald-400' : 'text-rose-400') : 'text-gray-600'}`}>
                                    {fmtPct(h.forward_return_pct)}
                                  </td>
                                </tr>
                              );
                            })}
                        </tbody>
                      </table>
                    </td>
                  </tr>
                )}
                {expandedMonth === rowKey && r.holdings.length === 0 && (
                  <tr key={`${rowKey}-empty`}>
                    <td colSpan={4} className="bg-[#0f1117] px-5 py-4">
                      <div className="text-xs text-gray-500">
                        {r.empty_reason || 'No holdings for this period (unknown reason)'}
                      </div>
                    </td>
                  </tr>
                )}
              </Fragment>
            ))}
          </tbody>
        </table>
      </div>
    </CollapsibleCard>
    {timelineCompanyId !== null && (
      <TickerTimelineModal
        result={result}
        companyId={timelineCompanyId}
        exchangeByCompany={exchangeByCompany}
        scoringConfig={scoringConfig}
        onClose={() => setTimelineCompanyId(null)}
      />
    )}
    </>
  );
}

/** React.memo barrier — `MomentumBacktester` re-renders the whole tree
 * on lots of unrelated state changes (axis chip toggles, sweep
 * inputs, run-time timer). This table renders ~200 monthly rows and
 * its expanded per-month holdings detail, so re-rendering it for an
 * unrelated parent state change is the most expensive needless work
 * on the /backtest page. Default shallow-compare is sufficient
 * because the caller already useMemo()s `scoringConfig` and the
 * `result` / `categories` / `exchangeByCompany` references only
 * change when their underlying data actually changes. */
const MonthlyHoldingsTable = memo(MonthlyHoldingsTableInner);
export default MonthlyHoldingsTable;

/** Search box in the Portfolios card header. Filters the set of companies
 * ever held during this backtest by ticker prefix / name substring and lets
 * the user open the same TickerTimelineModal that a row click would. Lives
 * inside the CollapsibleCard's clickable header, so every interactive
 * element stops click + key propagation to avoid toggling the card. */
function CompanySearch({
  companies,
  onPick,
}: {
  companies: HeldCompany[];
  onPick: (companyId: number) => void;
}) {
  const [query, setQuery] = useState('');
  const [open, setOpen] = useState(false);
  const [activeIdx, setActiveIdx] = useState(0);
  const containerRef = useRef<HTMLDivElement>(null);

  const matches = useMemo(() => {
    const q = query.trim().toLowerCase();
    if (!q) return [];
    // Rank: exact ticker > ticker prefix > ticker contains > name contains.
    type Scored = HeldCompany & { rank: number };
    const scored: Scored[] = [];
    for (const c of companies) {
      const t = c.ticker.toLowerCase();
      const n = c.company_name.toLowerCase();
      let rank = -1;
      if (t === q) rank = 0;
      else if (t.startsWith(q)) rank = 1;
      else if (t.includes(q)) rank = 2;
      else if (n.includes(q)) rank = 3;
      if (rank >= 0) scored.push({ ...c, rank });
    }
    scored.sort((a, b) => a.rank - b.rank || a.ticker.localeCompare(b.ticker));
    return scored.slice(0, 30);
  }, [query, companies]);

  // Reset highlighted index whenever the filter set changes. Same
  // "track prior value, reset during render" pattern the parent uses
  // above so the React 19 lint stays clean.
  const [lastQuery, setLastQuery] = useState(query);
  if (query !== lastQuery) {
    setLastQuery(query);
    setActiveIdx(0);
  }

  useClickOutside(containerRef, () => setOpen(false));

  const choose = (cid: number) => {
    onPick(cid);
    setQuery('');
    setOpen(false);
  };

  return (
    <div
      ref={containerRef}
      className="relative"
      onClick={(e) => e.stopPropagation()}
      onKeyDown={(e) => e.stopPropagation()}
    >
      <input
        type="text"
        value={query}
        onChange={(e) => {
          setQuery(e.target.value);
          setOpen(true);
        }}
        onFocus={() => setOpen(true)}
        onKeyDown={(e) => {
          // Stop bubbling so the CollapsibleCard header doesn't treat
          // Space/Enter as a toggle, then handle list navigation locally.
          e.stopPropagation();
          if (e.key === 'ArrowDown') {
            e.preventDefault();
            setActiveIdx((i) => Math.min(matches.length - 1, i + 1));
          } else if (e.key === 'ArrowUp') {
            e.preventDefault();
            setActiveIdx((i) => Math.max(0, i - 1));
          } else if (e.key === 'Enter') {
            if (matches[activeIdx]) {
              e.preventDefault();
              choose(matches[activeIdx].company_id);
            }
          } else if (e.key === 'Escape') {
            setOpen(false);
            setQuery('');
          }
        }}
        placeholder={`Search ${companies.length} stocks…`}
        className="bg-[#0f1117] border border-gray-700 rounded-lg px-2.5 py-1 text-xs text-gray-200 placeholder-gray-500 focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 focus:outline-none w-48"
      />
      {open && query.trim() && (
        <div className="absolute right-0 top-full mt-1 w-72 max-h-80 overflow-y-auto bg-[#1e2130] border border-gray-700 rounded-lg shadow-2xl z-30">
          {matches.length === 0 ? (
            <div className="px-3 py-2 text-xs text-gray-500">No matches in this backtest</div>
          ) : (
            matches.map((c, i) => (
              <button
                key={c.company_id}
                type="button"
                onMouseEnter={() => setActiveIdx(i)}
                onClick={() => choose(c.company_id)}
                className={`w-full text-left px-3 py-1.5 border-b border-gray-800/30 last:border-b-0 ${
                  i === activeIdx ? 'bg-white/[0.05]' : 'hover:bg-white/[0.03]'
                }`}
              >
                <div className="font-mono text-xs text-gray-200">{c.ticker || '—'}</div>
                <div className="text-[10px] text-gray-500 truncate">{c.company_name || '—'}</div>
              </button>
            ))
          )}
        </div>
      )}
    </div>
  );
}
