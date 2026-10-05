import * as XLSX from 'xlsx';

export type CalculationClient = {
  portfolio: string;
  name: string;
  cash: number;
};

export type CalculationSource = {
  name: string;
  sheetName: string;
  rows: { portfolio: string; trade: number }[];
};

export type CalculationExport = {
  headers: string[];
  rows: (string | number)[][];
  sourceCount: number;
};

function portfolioKey(portfolio: string): string {
  return portfolio.trim();
}

function sourceHeaders(files: CalculationSource[]): string[] {
  const seen = new Map<string, number>();
  return files.map((file) => {
    const base = `Purchase / sale: ${file.name} — ${file.sheetName}`;
    const n = (seen.get(base) ?? 0) + 1;
    seen.set(base, n);
    return n === 1 ? base : `${base} (${n})`;
  });
}

/**
 * The same source aggregation shown by the expandable client calculation.
 * A positive source value is a purchase (cash out); a negative value is a
 * sale (cash in). Keeping the signed values makes the Excel formula auditable.
 */
export function calculationExportData(
  clients: CalculationClient[], files: CalculationSource[],
): CalculationExport {
  const sourceCount = files.length;
  return {
    headers: [
      'Portefeuille',
      'Naam',
      'Liquid cash',
      ...sourceHeaders(files),
      'Total purchases / sales',
      'Projected cash',
    ],
    rows: clients.map((client) => [
      client.portfolio,
      client.name,
      client.cash,
      ...files.map((file) => file.rows
        .filter((row) => portfolioKey(row.portfolio) === portfolioKey(client.portfolio))
        .reduce((sum, row) => sum + row.trade, 0)),
    ]),
    sourceCount,
  };
}

function columnName(index: number): string {
  return XLSX.utils.encode_col(index);
}

function todayStamp(): string {
  const now = new Date();
  return `${now.getFullYear()}-${`${now.getMonth() + 1}`.padStart(2, '0')}-${`${now.getDate()}`.padStart(2, '0')}`;
}

/** Download the visible client calculation as an Excel workbook. */
export function downloadCalculationXlsx(clients: CalculationClient[], files: CalculationSource[]): void {
  const { headers, rows, sourceCount } = calculationExportData(clients, files);
  const worksheet = XLSX.utils.aoa_to_sheet([headers, ...rows]);
  const firstTradeColumn = 3;
  const totalTradeColumn = firstTradeColumn + sourceCount;
  const projectedCashColumn = totalTradeColumn + 1;

  rows.forEach((row, index) => {
    const excelRow = index + 2;
    const totalCell = `${columnName(totalTradeColumn)}${excelRow}`;
    const projectedCell = `${columnName(projectedCashColumn)}${excelRow}`;
    const firstTradeCell = `${columnName(firstTradeColumn)}${excelRow}`;
    const lastTradeCell = `${columnName(totalTradeColumn - 1)}${excelRow}`;
    const totalTrades = row.slice(firstTradeColumn).reduce<number>((sum, value) => (
      sum + (typeof value === 'number' ? value : 0)
    ), 0);
    // A portfolio can be calculated from liquid cash alone when every uploaded
    // source filtered it out. SUM of an empty range would be malformed, so use 0.
    worksheet[totalCell] = sourceCount
      ? { t: 'n', v: totalTrades, f: `SUM(${firstTradeCell}:${lastTradeCell})`, z: '€ #,##0.00;[Red]-€ #,##0.00' }
      : { t: 'n', v: 0, z: '€ #,##0.00;[Red]-€ #,##0.00' };
    worksheet[projectedCell] = {
      t: 'n',
      v: Number(row[2]) - totalTrades,
      f: `${columnName(2)}${excelRow}-${totalCell}`,
      z: '€ #,##0.00;[Red]-€ #,##0.00',
    };
    // Keep source and starting-cash cells numeric, so downloaded data remains
    // sortable and the formulas stay editable in Excel.
    [2, ...Array.from({ length: sourceCount }, (_, i) => firstTradeColumn + i)].forEach((column) => {
      const cell = worksheet[`${columnName(column)}${excelRow}`];
      if (cell) cell.z = '€ #,##0.00;[Red]-€ #,##0.00';
    });
  });

  worksheet['!cols'] = headers.map((header, index) => ({
    wch: index < 2 ? Math.max(14, Math.min(32, header.length + 2)) : Math.max(16, Math.min(42, header.length + 2)),
  }));
  worksheet['!autofilter'] = { ref: `A1:${columnName(projectedCashColumn)}${rows.length + 1}` };
  worksheet['!freeze'] = { xSplit: 2, ySplit: 1 };

  const workbook = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(workbook, worksheet, 'Cash calculation');
  XLSX.writeFile(workbook, `debet-calculation_${todayStamp()}.xlsx`);
}
