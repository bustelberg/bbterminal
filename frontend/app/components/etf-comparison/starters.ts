// Exact fund ISINs and the healthy Yahoo listings already used by asset_execution.
// These are editable comparison defaults, not the portfolio's purchase dates or target weights.
export const ETF_STARTERS = [
  { ticker: 'IWMO.L', isin: 'IE00BP3QZ825', name: 'iShares MSCI World Momentum' },
  { ticker: 'XDWU.DE', isin: 'IE00BM67HQ30', name: 'Xtrackers MSCI World Utilities' },
  { ticker: 'WSML.L', isin: 'IE00BF4RFH31', name: 'iShares MSCI World Small Cap' },
  { ticker: 'XUSE.AS', isin: 'IE000R4ZNTN3', name: 'iShares MSCI World ex-USA' },
  { ticker: 'MWEQ.L', isin: 'IE000OEF25S1', name: 'Invesco MSCI World Equal Weight' },
] as const;

export function starterEntries(start: string) {
  return ETF_STARTERS.map((etf, index) => ({ id: index + 1, ticker: String(etf.ticker), start }));
}
