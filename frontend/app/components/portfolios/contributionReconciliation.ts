/** A disclosed difference, never a measured investment return or a change to AIRS data. */
export function contributionReconciliation(
  contributions: (number | null | undefined)[], reported: number | null | undefined,
  comparable: boolean,
) {
  const known = contributions.filter((value): value is number => typeof value === 'number' && Number.isFinite(value));
  if (!comparable || typeof reported !== 'number' || !Number.isFinite(reported) || !known.length) return null;
  // Sum the two-decimal contributions users actually see, in integer hundredths of a point.
  const displayedPoints = (value: number) => Math.round(Number(value.toFixed(2)) * 100);
  const calculatedPoints = known.reduce((total, value) => total + displayedPoints(value), 0);
  const reportedPoints = displayedPoints(reported);
  return { calculated: calculatedPoints / 100, reported: reportedPoints / 100,
    missing: (reportedPoints - calculatedPoints) / 100 };
}
