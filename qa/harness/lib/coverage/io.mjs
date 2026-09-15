// Small file writers for coverage harness artifacts.

import { appendFileSync, readFileSync, writeFileSync } from 'node:fs';

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

export function readJsonl(path) {
  const text = readFileSync(path, 'utf8').replace(/\r\n/g, '\n').replace(/\r/g, '\n').trim();
  if (!text) return [];
  return text.split('\n').map((line, idx) => {
    try {
      return JSON.parse(line);
    } catch (e) {
      throw new Error(`Could not parse JSONL ${path}:${idx + 1}: ${String((e && e.message) || e)}`);
    }
  });
}

function parseCsvLine(line) {
  const cells = [];
  let cell = '';
  let quoted = false;
  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    if (quoted) {
      if (ch === '"' && line[i + 1] === '"') {
        cell += '"';
        i += 1;
      } else if (ch === '"') {
        quoted = false;
      } else {
        cell += ch;
      }
    } else if (ch === '"') {
      quoted = true;
    } else if (ch === ',') {
      cells.push(cell);
      cell = '';
    } else {
      cell += ch;
    }
  }
  cells.push(cell);
  return cells;
}

export function readCsv(path) {
  const text = readFileSync(path, 'utf8').replace(/\r\n/g, '\n').replace(/\r/g, '\n');
  const lines = text.split('\n').filter((line) => line.length > 0);
  if (!lines.length) return [];
  const header = parseCsvLine(lines[0]);
  return lines.slice(1).map((line) => {
    const cells = parseCsvLine(line);
    return Object.fromEntries(header.map((name, idx) => [name, cells[idx] ?? '']));
  });
}
