// Small file writers for coverage harness artifacts.

import { appendFileSync, writeFileSync } from 'node:fs';

export function csvEscape(value) {
  if (value === null || value === undefined) return '';
  const text = String(value);
  if (/[",\n\r]/.test(text)) return `"${text.replace(/"/g, '""')}"`;
  return text;
}

export function writeCsv(path, rows, columns) {
  const lines = [
    columns.join(','),
    ...rows.map((row) => columns.map((col) => csvEscape(row[col])).join(',')),
  ];
  writeFileSync(path, `${lines.join('\n')}\n`);
}

export function appendJsonl(path, record) {
  appendFileSync(path, `${JSON.stringify(record)}\n`);
}
