// Display labels for the asset-pipeline's lowercase `asset_class` + `sector`.
// The stored values stay lowercase (filtering/logic depends on them) — these only
// prettify the UI: equity → Equity, etf → ETF, crypto → Crypto, …

const CLASS_LABEL: Record<string, string> = {
  equity: 'Equity', etf: 'ETF', crypto: 'Crypto', commodity: 'Commodity',
  fx: 'FX', index: 'Index', bond: 'Bond',
};

export const classLabel = (c?: string | null): string =>
  !c ? '' : (CLASS_LABEL[c.toLowerCase()] ?? c.charAt(0).toUpperCase() + c.slice(1));

// Title-case each word (Yahoo sectors are already proper). But a crypto/commodity/
// ETF row whose "sector" falls back to the asset class is lowercase — route those
// through the class map so `etf → ETF` / `fx → FX`, not "Etf"/"Fx".
export const sectorLabel = (s?: string | null): string => {
  if (!s) return '';
  const asClass = CLASS_LABEL[s.toLowerCase()];
  if (asClass) return asClass;
  return s.replace(/\w\S*/g, (w) => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase());
};
