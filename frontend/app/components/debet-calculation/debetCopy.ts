'use client';

import { useLang } from '../../../lib/i18n';

const EN = {
  locale: 'en-GB',
  cashControl: 'Cash control',
  title: 'Debet calculations',
  intro: 'Upload one or more order files to combine each client\'s purchases and sales with their liquid cash position. A positive purchase value reduces cash; a negative value is a sale and increases it.',
  dropFiles: 'Drop Excel files here, or click to upload',
  acceptedFiles: '.xlsx, .xlsm, or .xls · one tab per file',
  uploadedFiles: (count: number) => `Uploaded files (${count})`,
  localProcessing: 'All processing happens locally in this browser.',
  clearAll: 'Clear all',
  removeFile: (name: string) => `Remove ${name}`,
  useHiddenRows: 'Use hidden rows',
  clientCalculation: 'Client calculation',
  searchLabel: 'Search portfolio or name',
  searchPlaceholder: 'Search portfolio or name',
  downloadExcel: 'Download Excel',
  portfolio: 'Portfolio',
  name: 'Name',
  liquidCash: 'Liquid cash',
  purchasesSales: 'Purchases / sales',
  projectedCash: 'Projected cash',
  calculationDetails: 'Calculation details',
  showCalculation: 'Show calculation',
  hideCalculation: 'Hide calculation',
  calculation: 'Calculation',
  startingLiquidCash: 'Starting liquid cash',
  buySellFrom: (source: string) => `Buy / sell from ${source}`,
  foundAt: (locations: string) => `Found at ${locations}`,
  wrongTabCount: (name: string, count: number) => `${name} has ${count} tabs; exactly one tab is required.`,
  emptyFile: (name: string) => `${name} is empty.`,
  missingColumns: (name: string, columns: string) => `${name} is missing column(s): ${columns}.`,
  invalidValues: (name: string, row: number) => `${name}, row ${row}: cash and purchase value must both be numbers.`,
  noClientRows: (name: string) => `${name} has no client rows.`,
  invalidFile: (name: string) => `${name} is not an Excel file. Upload .xlsx, .xlsm, or .xls files.`,
  unreadableFile: 'The Excel file could not be read.',
};

const NL: typeof EN = {
  locale: 'nl-NL',
  cashControl: 'Kasbeheer',
  title: 'Debetberekeningen',
  intro: 'Upload één of meer orderbestanden om de aan- en verkopen van iedere cliënt te combineren met de liquide middelen. Een positieve aankoopwaarde verlaagt de liquide middelen; een negatieve waarde is een verkoop en verhoogt deze.',
  dropFiles: 'Sleep Excel-bestanden hierheen, of klik om te uploaden',
  acceptedFiles: '.xlsx, .xlsm of .xls · één tabblad per bestand',
  uploadedFiles: (count) => `Geüploade bestanden (${count})`,
  localProcessing: 'Alle verwerking gebeurt lokaal in deze browser.',
  clearAll: 'Alles wissen',
  removeFile: (name) => `${name} verwijderen`,
  useHiddenRows: 'Verborgen rijen gebruiken',
  clientCalculation: 'Cliëntberekening',
  searchLabel: 'Zoek portefeuille of naam',
  searchPlaceholder: 'Zoek portefeuille of naam',
  downloadExcel: 'Excel downloaden',
  portfolio: 'Portefeuille',
  name: 'Naam',
  liquidCash: 'Liquide middelen',
  purchasesSales: 'Aan- / verkopen',
  projectedCash: 'Verwachte liquide middelen',
  calculationDetails: 'Berekeningsdetails',
  showCalculation: 'Berekening tonen',
  hideCalculation: 'Berekening verbergen',
  calculation: 'Berekening',
  startingLiquidCash: 'Liquide middelen bij aanvang',
  buySellFrom: (source) => `Aan- / verkoop uit ${source}`,
  foundAt: (locations) => `Gevonden in ${locations}`,
  wrongTabCount: (name, count) => `${name} heeft ${count} tabbladen; precies één tabblad is vereist.`,
  emptyFile: (name) => `${name} is leeg.`,
  missingColumns: (name, columns) => `${name} mist kolom(men): ${columns}.`,
  invalidValues: (name, row) => `${name}, rij ${row}: liquide middelen en aankoopwaarde moeten beide getallen zijn.`,
  noClientRows: (name) => `${name} heeft geen cliëntregels.`,
  invalidFile: (name) => `${name} is geen Excel-bestand. Upload .xlsx-, .xlsm- of .xls-bestanden.`,
  unreadableFile: 'Het Excel-bestand kon niet worden gelezen.',
};

export type DebetCopy = typeof EN;

export function useDebetCopy(): DebetCopy {
  const [lang] = useLang();
  return lang === 'nl' ? NL : EN;
}
