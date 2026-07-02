"""Asset-pipeline prototype.

ISIN (or native symbol) -> longest + most-liquid Yahoo listing (the ANALYSIS
instrument to backtest on) + oldest/newest candles, with a seam for the IBKR
European-tradeable step (the EXECUTION instrument). Deliberately decoupled from
the GuruFocus ingest pipeline. Yahoo is an unofficial API — best-effort only."""
