import { describe, expect, it } from 'vitest';

import { computeFeeWaterfall, DEFAULT_FEE_CONFIG, type FeeConfig } from './feeModel';
import type { DailyRecord, Holding, PeriodRecord } from '../../../lib/stores/momentum';

const ZERO: FeeConfig = {
  leonteq_annual_bps: 0,
  transaction_bps: 0,
  bustelberg_mgmt_bps: 0,
  bustelberg_perf_pct: 0,
};

function holding(cid: number, entry: string, exit: string): Holding {
  return {
    company_id: cid,
    ticker: `T${cid}`,
    company_name: `C${cid}`,
    sector: 'Tech',
    score: 1,
    category_scores: {},
    weight: 0.5,
    forward_return_pct: 0,
    entry_price_eur: 100,
    exit_price_eur: 100,
    entry_date: entry,
    exit_date: exit,
    side: 'long',
  };
}

/** A closed period whose holdings carry the given exit date. */
function period(date: string, cids: number[], entry: string, exit: string): PeriodRecord {
  return {
    date,
    holdings: cids.map((c) => holding(c, entry, exit)),
    portfolio_return_pct: 0,
    cumulative_return_pct: 0,
  };
}

/** Daily curve from startISO for `days` days, each day multiplying the
 * factor by `dailyFactor`. cumulative_return_pct is (factor − 1) × 100. */
function dailyCurve(startISO: string, days: number, dailyFactor: number): DailyRecord[] {
  const out: DailyRecord[] = [];
  let f = 1;
  const start = Date.parse(startISO);
  for (let i = 0; i < days; i++) {
    const d = new Date(start + i * 86400 * 1000).toISOString().slice(0, 10);
    out.push({ date: d, cumulative_return_pct: (f - 1) * 100 });
    f *= dailyFactor;
  }
  return out;
}

// Two full calendar years 2024-01-01 .. 2025-12-31, steady growth.
const DAYS = 731; // 2024 (366, leap) + 2025 (365)
const DAILY_GROWTH = Math.pow(1.44, 1 / (DAYS - 1)); // ~ +44% total
const DAILY = dailyCurve('2024-01-01', DAYS, DAILY_GROWTH);
// Two yearly periods so trade-counting + exit dates line up with the curve.
const MONTHLY: PeriodRecord[] = [
  period('2024-01', [1, 2], '2024-01-01', '2024-12-31'),
  period('2025-01', [1, 2], '2025-01-01', '2025-12-31'),
];

describe('computeFeeWaterfall', () => {
  it('zero fees → every layer equals gross, nothing accrued', () => {
    const w = computeFeeWaterfall(MONTHLY, DAILY, ZERO, { grossTotalReturnPct: 44 })!;
    expect(w).not.toBeNull();
    expect(w.gross_return_pct).toBeCloseTo(44, 6);
    expect(w.after_leonteq_pct).toBeCloseTo(44, 6);
    expect(w.after_bustelberg_pct).toBeCloseTo(44, 6);
    expect(w.bustelberg_accrued_pct).toBeCloseTo(0, 9);
  });

  it('layers are monotonic: gross ≥ after-Leonteq ≥ after-Bustelberg', () => {
    const w = computeFeeWaterfall(MONTHLY, DAILY, DEFAULT_FEE_CONFIG, { grossTotalReturnPct: 44 })!;
    expect(w.gross_return_pct).toBeGreaterThan(w.after_leonteq_pct);
    expect(w.after_leonteq_pct).toBeGreaterThan(w.after_bustelberg_pct);
    // Drags are positive percentage points.
    expect(w.leonteq_drag_pp).toBeGreaterThan(0);
    expect(w.bustelberg_drag_pp).toBeGreaterThan(0);
  });

  it('anchors the gross row to the authoritative total return', () => {
    const w = computeFeeWaterfall(MONTHLY, DAILY, DEFAULT_FEE_CONFIG, { grossTotalReturnPct: 44 })!;
    expect(w.gross_return_pct).toBeCloseTo(44, 6);
    // net-to-client headline equals after-Bustelberg.
    expect(w.net.total_return_pct).toBeCloseTo(w.after_bustelberg_pct, 6);
  });

  it('accrued = management + performance, both positive on a rising book', () => {
    const w = computeFeeWaterfall(MONTHLY, DAILY, DEFAULT_FEE_CONFIG, { grossTotalReturnPct: 44 })!;
    expect(w.bustelberg_mgmt_pct).toBeGreaterThan(0);
    expect(w.bustelberg_perf_pct).toBeGreaterThan(0);
    expect(w.bustelberg_accrued_pct).toBeCloseTo(
      w.bustelberg_mgmt_pct + w.bustelberg_perf_pct,
      6,
    );
    // ~2 years of 100bps mgmt on a book around 1.0–1.4 → roughly 2%.
    expect(w.bustelberg_mgmt_pct).toBeGreaterThan(1.5);
    expect(w.bustelberg_mgmt_pct).toBeLessThan(3.5);
  });

  it('transaction-only: buy + sell of the whole book ≈ 2× txn bps', () => {
    const txnOnly: FeeConfig = { ...ZERO, transaction_bps: 10 };
    // Single closed period → entrants = N (open), departures = N (close)
    // → leg fraction 2 → 20 bps drag on the whole book.
    const single = [period('2024-01', [1, 2], '2024-01-01', '2024-12-31')];
    const oneYear = dailyCurve('2024-01-01', 366, Math.pow(1.10, 1 / 365));
    const w = computeFeeWaterfall(single, oneYear, txnOnly, { grossTotalReturnPct: 10 })!;
    // gross 1.10, after 20bps: 1.10 × 0.998 → +9.78%.
    expect(w.after_leonteq_pct).toBeCloseTo((1.10 * 0.998 - 1) * 100, 4);
  });

  it('high-water mark: a flat second year accrues no new performance fee', () => {
    // Year 1 rises to +20%, year 2 dead flat. Performance fee should come
    // entirely from year 1; year 2 adds management fee only.
    const y1 = dailyCurve('2024-01-01', 366, Math.pow(1.20, 1 / 365));
    const lastY1 = y1[y1.length - 1].cumulative_return_pct; // ~+20%
    const flatFactor = 1 + lastY1 / 100;
    const y2: DailyRecord[] = [];
    for (let i = 0; i < 365; i++) {
      const d = new Date(Date.parse('2025-01-01') + i * 86400 * 1000).toISOString().slice(0, 10);
      y2.push({ date: d, cumulative_return_pct: (flatFactor - 1) * 100 });
    }
    const daily = [...y1, ...y2];
    const perfOnly: FeeConfig = { ...ZERO, bustelberg_perf_pct: 10 };
    const w = computeFeeWaterfall(MONTHLY, daily, perfOnly, { grossTotalReturnPct: lastY1 })!;
    // HWM perf = 10% of the year-1 gain only (≈ 0.10 × 0.20 = 2% of capital).
    expect(w.bustelberg_perf_pct).toBeGreaterThan(1.5);
    expect(w.bustelberg_perf_pct).toBeLessThan(2.5);
    // No management fee configured here.
    expect(w.bustelberg_mgmt_pct).toBeCloseTo(0, 9);
  });

  it('Bustelberg drag reconciles with accrued (no double-counting across crystallizations)', () => {
    // Regression for the cumulative-vs-incremental drag bug: the drop
    // from after-Leonteq to after-Bustelberg must equal the fees Bustelberg
    // accrued, plus only a small lost-compounding gap — NOT a multiple of it.
    const w = computeFeeWaterfall(MONTHLY, DAILY, DEFAULT_FEE_CONFIG, { grossTotalReturnPct: 44 })!;
    const drag = w.after_leonteq_pct - w.after_bustelberg_pct;
    expect(drag).toBeCloseTo(w.bustelberg_drag_pp, 6);
    // Client shortfall ≥ fees accrued (lost compounding), but the gap is tiny.
    expect(drag).toBeGreaterThanOrEqual(w.bustelberg_accrued_pct - 0.01);
    expect(drag - w.bustelberg_accrued_pct).toBeLessThan(1.0);
  });

  it('per-year breakdown reconciles each row: gross − fees = net', () => {
    const w = computeFeeWaterfall(MONTHLY, DAILY, DEFAULT_FEE_CONFIG, { grossTotalReturnPct: 44 })!;
    expect(w.breakdown.length).toBeGreaterThanOrEqual(1);
    for (const b of w.breakdown) {
      const recon = b.gross_return_pct - b.transaction_pct - b.leonteq_annual_pct - b.mgmt_pct - b.perf_pct;
      expect(recon).toBeCloseTo(b.net_return_pct, 6);
    }
  });

  it('Leonteq drag splits into transaction + annual', () => {
    const w = computeFeeWaterfall(MONTHLY, DAILY, DEFAULT_FEE_CONFIG, { grossTotalReturnPct: 44 })!;
    expect(w.transaction_drag_pp + w.leonteq_annual_drag_pp).toBeCloseTo(w.leonteq_drag_pp, 6);
  });

  it('regression: ~1y run with two crystallizations does not over-drag Bustelberg', () => {
    // Mirrors the reported case: ~1 year, gross ~26%, default fees. The
    // bug made after-Bustelberg crater ~15pp below after-Leonteq while
    // only ~3% was accrued. After the fix the drag must track the accrual.
    const days = 366;
    const daily = dailyCurve('2025-01-01', days, Math.pow(1.2626, 1 / (days - 1)));
    const monthly = [period('2025-01', [1, 2], '2025-01-01', '2025-12-31')];
    const w = computeFeeWaterfall(monthly, daily, DEFAULT_FEE_CONFIG, { grossTotalReturnPct: 26.26 })!;
    expect(w.gross_return_pct).toBeCloseTo(26.26, 4);
    expect(w.bustelberg_accrued_pct).toBeGreaterThan(0);
    // The Bustelberg drag must be in the same ballpark as what it accrued
    // (≤ ~1pp of lost compounding), nowhere near 15pp.
    expect(w.bustelberg_drag_pp).toBeLessThan(w.bustelberg_accrued_pct + 1.0);
    expect(w.bustelberg_drag_pp).toBeLessThan(5);
    // breakdown has one row per crystallization (year-end + final).
    expect(w.breakdown.length).toBeGreaterThanOrEqual(1);
  });

  it('pro-rates the annual fee for a short (sub-year) run', () => {
    // Half-year run → 35bps Leonteq annual should deduct ~17.5bps.
    const half = dailyCurve('2024-01-01', 183, 1); // flat, ~0.5y
    const single = [period('2024-01', [1, 2], '2024-01-01', '2024-07-02')];
    const leonteqOnly: FeeConfig = { ...ZERO, leonteq_annual_bps: 35 };
    const w = computeFeeWaterfall(single, half, leonteqOnly, { grossTotalReturnPct: 0 })!;
    // Flat book, ~0.5y of 35bps → ~ −0.175%.
    expect(w.after_leonteq_pct).toBeLessThan(0);
    expect(w.after_leonteq_pct).toBeGreaterThan(-0.25);
    expect(w.after_leonteq_pct).toBeLessThan(-0.10);
  });
});
