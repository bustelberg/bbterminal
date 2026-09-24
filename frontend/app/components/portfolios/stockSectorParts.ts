/** Pure grouping for the Holdings table's Individual stocks section. */
export type SectorHolding = { sector?: string | null };

export type StockSectorPart<T> = {
  key: string;
  label: string;
  rows: T[];
};

const sectorName = (value: string | null | undefined) => {
  const name = value?.trim();
  return !name || name === 'Unclassified' ? 'Unclassified' : name;
};

/**
 * Group operating-company rows by their resolved sector. Sector order is stable and alphabetical;
 * unclassified rows stay last. The caller remains responsible for sorting rows within each group.
 */
export function stockSectorParts<T extends SectorHolding>(rows: readonly T[]): StockSectorPart<T>[] {
  const grouped = new Map<string, T[]>();
  for (const row of rows) {
    const label = sectorName(row.sector);
    grouped.set(label, [...(grouped.get(label) ?? []), row]);
  }
  return [...grouped.entries()]
    .sort(([a], [b]) => {
      if (a === 'Unclassified') return 1;
      if (b === 'Unclassified') return -1;
      return a.localeCompare(b);
    })
    .map(([label, sectorRows]) => ({
      key: label.toLocaleLowerCase('en-US').replace(/[^a-z0-9]+/g, '-'),
      label,
      rows: sectorRows,
    }));
}
