export const RISK_HISTORY_START = '2021-01-01';
// Daily jobs own history refreshes; opening a risk view must remain a stored-data read.
export const AUTOMATIC_RISK_REPAIRS = false;
export const riskHistoryYears = () => Math.max(1, new Date().getFullYear() - 2021 + 1);
export const riskHistoryQuery = () => `start_date=${RISK_HISTORY_START}&years=${riskHistoryYears()}&data_window=fixed-2021-benchmark-repair-v2`;
