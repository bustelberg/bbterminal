import { describe, expect, it } from 'vitest';

import {
  ETF_SECTOR_ALLOCATION_ISIN,
  hasEtfSectorAllocation,
} from './PortfolioAnalysisModal';

describe('ETF sector-allocation gate', () => {
  it('offers look-through for each ETF the backend marks as supported', () => {
    expect(hasEtfSectorAllocation({
      isin: ETF_SECTOR_ALLOCATION_ISIN,
      is_fund: true,
      sector_allocation_available: true,
    })).toBe(true);
    expect(hasEtfSectorAllocation({
      isin: 'IE00B6R52259',
      is_fund: true,
      sector_allocation_available: true,
    })).toBe(true);
    expect(hasEtfSectorAllocation({
      isin: 'IE000MEQP5U8',
      is_fund: true,
      sector_allocation_available: true,
    })).toBe(true);
  });

  it('does not put the button on stocks, internal portfolios, or unavailable funds', () => {
    expect(hasEtfSectorAllocation({
      isin: ETF_SECTOR_ALLOCATION_ISIN,
      is_fund: false,
      sector_allocation_available: true,
    })).toBe(false);
    expect(hasEtfSectorAllocation({
      isin: 'CH1593776334',
      is_fund: true,
      sector_allocation_available: false,
    })).toBe(false);
    expect(hasEtfSectorAllocation({ is_fund: true, sector_allocation_available: true })).toBe(false);
  });
});
