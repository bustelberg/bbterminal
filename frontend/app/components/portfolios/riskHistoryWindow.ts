export const RISK_HISTORY_START = '2021-01-01';
export const riskHistoryYears = () => Math.max(1, new Date().getFullYear() - 2021 + 1);
export const riskHistoryQuery = () => `start_date=${RISK_HISTORY_START}&years=${riskHistoryYears()}&data_window=fixed-2021-benchmark-repair-v2`;
