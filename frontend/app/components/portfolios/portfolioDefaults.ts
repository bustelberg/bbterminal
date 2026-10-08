/** Defaults that depend on a portfolio's product family, not on the modal that opens it. */

/**
 * Toppenberg portfolios are chiefly wrappers around linked certificates. Starting expanded gives
 * their readers the companies they are actually evaluating, while leaving the checkbox available
 * to inspect the traded wrappers instead.
 */
export function defaultLookThroughCertificates(name: string | null | undefined,
  bookPortfolio?: string | null): boolean {
  return [name, bookPortfolio].some((value) => {
    const trimmed = value?.trim() ?? '';
    return /^toppenberg\b/i.test(trimmed) || /^tops_/i.test(trimmed);
  });
}
