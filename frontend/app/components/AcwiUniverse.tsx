'use client';

import AcwiCanonicalView from './AcwiCanonicalView';
import LoadingDots from './LoadingDots';
import { useAcwiData } from './acwi/useAcwiData';
import FetchProgressBanner from './acwi/FetchProgressBanner';
import BreakdownCards from './acwi/BreakdownCards';
import ConstituentChangesSummary from './acwi/ConstituentChangesSummary';
import NetAdditionsTable from './acwi/NetAdditionsTable';
import AnnouncementsTable from './acwi/AnnouncementsTable';
import OtherCountryCodedTable from './acwi/OtherCountryCodedTable';
import HoldingsTable from './acwi/HoldingsTable';
import FeasibleUniverseTable from './acwi/FeasibleUniverseTable';
import AdditionTimelineTable from './acwi/AdditionTimelineTable';

// This page was decomposed (2026-06-04) into `app/components/acwi/`:
// all fetching + business-logic derivations live in `useAcwiData`, the
// shared shapes in `types.ts`, formatters in `format.ts`, and each render
// section is its own presentational component owning only its local
// search/sort UI state. When extending /acwi, add/extend a section
// component or the hook — don't regrow this orchestrator.
//
// The canonical (template-managed) universe — what backtests and the
// scheduled pipeline actually use — sits at the top via AcwiCanonicalView.
// Everything below is iShares/MSCI reconstruction diagnostics driven by
// the hook.

export default function AcwiUniverse() {
  const {
    holdings,
    loading,
    error,
    announcements,
    annLoading,
    annError,
    getDetail,
    fetchDetail,
    fetching,
    netAdditions,
    netAdditionsLoading,
    netAdditionsStats,
    feasibleHoldings,
    additionTimeline,
    sectorBreakdown,
    countryBreakdown,
    detailSummary,
    otherCountryCoded,
    hasFetchedDetails,
  } = useAcwiData();

  return (
    <div className="p-8 space-y-6 max-w-[1400px] mx-auto">
      <div>
        <h1 className="text-2xl font-semibold text-white">MSCI ACWI Universe</h1>
        <p className="text-gray-400 text-sm mt-1">
          The canonical universe (template-managed) sits at the top — that&apos;s what backtests and the scheduled pipeline use.
          Below: live iShares fund holdings + MSCI announcement explorer for diagnosing the reconstruction.
        </p>
      </div>

      <AcwiCanonicalView />

      {error && (
        <div className="bg-rose-500/10 border border-rose-500/20 rounded-lg px-4 py-3 text-rose-400 text-sm">
          {error}
        </div>
      )}

      <FetchProgressBanner />

      {loading ? (
        <div className="text-gray-400 text-sm"><LoadingDots label="Loading holdings" /></div>
      ) : (
        <>
          <BreakdownCards sectorBreakdown={sectorBreakdown} countryBreakdown={countryBreakdown} />

          {hasFetchedDetails && (
            <ConstituentChangesSummary detailSummary={detailSummary} fetching={fetching} />
          )}

          <NetAdditionsTable
            netAdditions={netAdditions}
            netAdditionsLoading={netAdditionsLoading}
            netAdditionsStats={netAdditionsStats}
            fetching={fetching}
          />

          <AnnouncementsTable
            announcements={announcements}
            annLoading={annLoading}
            annError={annError}
            getDetail={getDetail}
            fetchDetail={fetchDetail}
            fetching={fetching}
          />

          <OtherCountryCodedTable otherCountryCoded={otherCountryCoded} />

          <HoldingsTable holdings={holdings} />

          <FeasibleUniverseTable feasibleHoldings={feasibleHoldings} />

          <AdditionTimelineTable additionTimeline={additionTimeline} />
        </>
      )}
    </div>
  );
}
