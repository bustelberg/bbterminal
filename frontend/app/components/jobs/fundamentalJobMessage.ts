/**
 * Make a fundamentals job receipt suitable for the UI.
 *
 * The server now emits plain-English failures, but this also cleans receipts from an older
 * backend that may still be finishing a job during a frontend deployment.
 */
export function fundamentalJobMessage(message: string | null | undefined): string {
  if (!message) return '';
  const cleaned = message
    .replace(/^(?:[A-Za-z][\w.]*Error):\s*/, '')
    .replace(/:\s*(?:fin|est|ind):\s*/gi, ': ')
    .replace(/no change \([\d,]+ rows already stored\)/gi, 'up to date')
    .replace(/\s+[—–]\s+/g, '. ')
    .replace(/\s+\|\s+/g, '; ')
    .replace(/\.\s*;\s*/g, '; ')
    .replace(/\s{2,}/g, ' ')
    .trim();

  const failed = cleaned.match(/^Could not refresh (\d+) compan(?:y|ies)\. /i);
  const details = failed ? cleaned.slice(failed[0].length) : cleaned;
  const emptyStatements = [...details.matchAll(
    /(?:^|; )([^;]+?): GuruFocus did not provide financial statements\.(?: Please try again later\.)?/gi,
  )];
  if (failed && emptyStatements.length) {
    const count = Number(failed[1]);
    const names = emptyStatements.map((match) => match[1].trim());
    const named = names.length === 1
      ? names[0]
      : `${names.slice(0, -1).join(', ')} and ${names.at(-1)}`;
    const affected = count > names.length ? `${count} companies, including ${named}` : named;
    return `GuruFocus did not provide financial statements for ${affected}. Please try again later.`;
  }

  // The backend receipt is deliberately complete for an operator, but a reader needs the outcome:
  // what refreshed and which names, if any, need attention. Counts of database rows, retained
  // observations, API calls and coverage diagnostics do not help them decide what to do next.
  const refreshed = cleaned.match(/\b(\d+)\s+compan(?:y|ies)\s+(?:refetched|loaded)(?:,\s*(\d+)\s+failed)?/i);
  if (refreshed) {
    const count = Number(refreshed[1]);
    const failedCount = Number(refreshed[2] ?? 0);
    const failureSegment = cleaned.match(/\bfailures:\s*(.+?)(?=\s*·\s*|$)/i)?.[1] ?? '';
    const unavailableSegment = cleaned.match(/\bunavailable:\s*(.+?)(?=\s*·\s*|$)/i)?.[1] ?? '';
    const names = [...failureSegment.split('|'), ...unavailableSegment.split('|')]
      .map((entry) => entry.trim().split(':')[0].trim())
      .filter(Boolean);
    const uniqueNames = [...new Set(names)];
    const refreshedText = `${count} ${count === 1 ? 'company' : 'companies'} refreshed.`;
    if (uniqueNames.length) {
      return `${refreshedText} Could not refresh: ${uniqueNames.join(', ')}.`;
    }
    if (failedCount) {
      return `${refreshedText} ${failedCount} ${failedCount === 1 ? 'company could' : 'companies could'} not be refreshed.`;
    }
    return refreshedText;
  }
  return cleaned;
}
