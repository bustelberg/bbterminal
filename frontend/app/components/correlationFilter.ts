/** Project an NxN correlation matrix onto a subset of its portfolios.
 *
 * ⚠ BOTH AXES, THROUGH THE SAME INDEX LIST. `matrix` is NxN over the same index space as
 * `labels`; filtering the ROWS alone leaves rows of the original width, so every cell after the
 * first dropped column is read from the wrong portfolio. That result is still rectangular, still
 * renders as a heatmap, and is silently wrong — there is no error, just a matrix about pairs that
 * were never paired.
 *
 * Extracted from the component so it is testable: this is the one piece of the filter that can
 * fail without looking like it failed. (The profiles it is filtered BY live in
 * `portfolioVariants` — they are a /portfolios concept, not a correlation one.)
 */
export function sliceMatrix<T>(
  matrix: (T | null)[][],
  keep: number[],
): (T | null)[][] {
  return keep.map((i) => keep.map((j) => matrix[i]?.[j] ?? null));
}
