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
  return cleaned;
}
