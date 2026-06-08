-- Switch the Bustelberg management fee from a yearly rate (100 bps/yr) to a
-- monthly rate (10 bps/month). The column itself is unchanged (still
-- `bustelberg_mgmt_bps` on the single-row `fee_config`); only its UNIT and
-- default change. The client-side fee waterfall now accrues this fee at the
-- monthly rate × months in each crystallization window (see
-- frontend/app/components/momentum/feeModel.ts).
--
-- 10 bps/month ≈ 1.2%/yr, vs the old 100 bps/yr (1%/yr).

ALTER TABLE public.fee_config ALTER COLUMN bustelberg_mgmt_bps SET DEFAULT 10;

-- Migrate the existing single config row off the old yearly default. Guard on
-- the old value so a hand-customized rate isn't clobbered.
UPDATE public.fee_config
   SET bustelberg_mgmt_bps = 10, updated_at = now()
 WHERE id = 1 AND bustelberg_mgmt_bps = 100;
