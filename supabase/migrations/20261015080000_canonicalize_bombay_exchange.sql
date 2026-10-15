-- ACWI maps iShares' "Bse Ltd" to BOM, the canonical GuruFocus / IBKR code.
-- The original seed used BSE, which made Bombay constituents fail the DB join
-- and therefore disappear from ACWI sync.  The code is a foreign-key target,
-- so create the canonical parent first, migrate dependants, then remove BSE.
DO $$
DECLARE
  bse_id integer;
  bom_id integer;
BEGIN
  -- The initial schema seeded explicit IDs but left this sequence at 1.  This
  -- migration may need to insert BOM, so make its generated ID safe first.
  PERFORM setval(
    'public.exchange_id_seq',
    (SELECT max(exchange_id) FROM public.gurufocus_exchange),
    true
  );

  SELECT exchange_id INTO bse_id
  FROM public.gurufocus_exchange
  WHERE exchange_code = 'BSE';

  SELECT exchange_id INTO bom_id
  FROM public.gurufocus_exchange
  WHERE exchange_code = 'BOM';

  IF bse_id IS NULL AND bom_id IS NULL THEN
    INSERT INTO public.gurufocus_exchange
      (exchange_code, exchange_name, is_us, country_code, currency_code)
    VALUES ('BOM', 'Bombay Stock Exchange', false, 'IN', 'INR');
  ELSIF bse_id IS NOT NULL THEN
    -- A partially repaired database may contain both values.  Keep BOM as the
    -- canonical parent and merge dependent rows without assuming either fee
    -- row exists.
    IF bom_id IS NULL THEN
      INSERT INTO public.gurufocus_exchange
        (exchange_code, exchange_name, is_us, country_code, currency_code)
      VALUES ('BOM', 'Bombay Stock Exchange', false, 'IN', 'INR')
      RETURNING exchange_id INTO bom_id;
    END IF;

    UPDATE public.company
    SET exchange_id = bom_id
    WHERE exchange_id = bse_id;

    IF EXISTS (SELECT 1 FROM public.exchange_fee WHERE exchange_code = 'BSE') THEN
      IF EXISTS (SELECT 1 FROM public.exchange_fee WHERE exchange_code = 'BOM') THEN
        DELETE FROM public.exchange_fee WHERE exchange_code = 'BSE';
      ELSE
        UPDATE public.exchange_fee
        SET exchange_code = 'BOM'
        WHERE exchange_code = 'BSE';
      END IF;
    END IF;

    DELETE FROM public.gurufocus_exchange WHERE exchange_id = bse_id;
  END IF;
END $$;
