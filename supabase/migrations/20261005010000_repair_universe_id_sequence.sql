-- A restored/seeded database can contain universe rows whose IDs were inserted
-- explicitly while the serial sequence remained at its initial value. The next
-- template-created universe would then collide with an existing primary key.
SELECT setval(
  'public.universe_universe_id_seq',
  COALESCE((SELECT MAX(universe_id) FROM public.universe), 1),
  true
);
