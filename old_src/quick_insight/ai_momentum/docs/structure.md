orchestrate.py
│
├── 1. get_universe()
│        → universe_df  (sector, company_name, ticker, exchange)
│
├── 2. fetch all signals in PARALLEL (thread pool, one task per signal source)
│        │
│        ├── get_universe_price_stats(universe_df)       → price_df
│        ├── get_universe_smart_money(universe_df)       → smart_money_df
│        └── get_universe_narrative(universe_df)         → narrative_df  (future)
│
├── 3. join on (primary_ticker, primary_exchange)
│        → company_signals_df  (one row per company, all raw signals)
│
├── 4. score_companies(company_signals_df, weights)
│        → company_scores_df   (adds price_score, smart_money_score per company)
│
├── 5. aggregate_sectors(company_scores_df)
│        → sector_df           (mean scores per sector)
│
├── 6. score_sectors(sector_df, sector_weights)
│        → sector_scorecard    (one final_score per sector, ranked)
│
└── 7. top_n_sectors(sector_scorecard, n=4)
         → passing_sectors