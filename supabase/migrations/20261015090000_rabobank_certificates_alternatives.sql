-- Rabobank Certificates are perpetual bank-capital instruments, not ordinary
-- equities or bonds.  Pin the instrument-level class so every AIRS portfolio
-- consistently presents this exact ISIN under Alternatives.
INSERT INTO public.asset_bucket_override (isin, bucket, updated_at)
VALUES ('XS1002121454', 'Alternatives', now())
ON CONFLICT (isin) DO UPDATE
SET bucket = EXCLUDED.bucket,
    updated_at = EXCLUDED.updated_at;
