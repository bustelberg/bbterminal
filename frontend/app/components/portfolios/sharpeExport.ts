import type { Column } from '../../../lib/tableExport';

type Period = { start: string; end: string };
type Block = { bucket: string; average_sharpe?: number | null; period?: Period | null;
  instruments: Array<{ name: string; overall?: (Period & { sharpe?: number | null }) | null }> };
export type SharpeExportRow = { category: string; instrument: string; sharpe: number | null; start: string | null; end: string | null };
const score = (value?: number | null) => value != null && Number.isFinite(value) ? Number(value.toFixed(2)) : null;

export function sharpeExportRows(blocks: Block[], formatDate: (value: string) => string): SharpeExportRow[] {
  return blocks.flatMap(block => {
    const category = block.bucket === 'Equity' ? 'Stocks' : block.bucket;
    return [...block.instruments.map(instrument => ({ category, instrument: instrument.name,
      sharpe: score(instrument.overall?.sharpe), start: instrument.overall ? formatDate(instrument.overall.start) : null,
      end: instrument.overall ? formatDate(instrument.overall.end) : null })),
    { category, instrument: 'Average instrument Sharpe', sharpe: score(block.average_sharpe),
      start: block.period ? formatDate(block.period.start) : null, end: block.period ? formatDate(block.period.end) : null }];
  });
}

export const sharpeExportColumns: Column<SharpeExportRow>[] = [
  { key: 'category', header: 'Category', accessor: row => row.category },
  { key: 'instrument', header: 'Instrument', accessor: row => row.instrument },
  { key: 'sharpe', header: 'Sharpe', accessor: row => row.sharpe },
  { key: 'start', header: 'Start', accessor: row => row.start },
  { key: 'end', header: 'End', accessor: row => row.end },
];
