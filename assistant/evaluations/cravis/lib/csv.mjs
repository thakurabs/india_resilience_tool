function safeCell(value) {
  if (value == null) return '';
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) return '';
    return String(value);
  }
  if (typeof value === 'boolean') return value ? 'true' : 'false';
  const text = String(value);
  return /^\s*[=+\-@]/.test(text) ? `'${text}` : text;
}

function escapeCell(value) {
  const text = safeCell(value);
  return /[",\r\n]/.test(text) ? `"${text.replaceAll('"', '""')}"` : text;
}

export function rowsToCsv(rows, columns = null) {
  const orderedColumns = columns ?? [...new Set(rows.flatMap((row) => Object.keys(row)))].sort();
  const lines = [orderedColumns.map(escapeCell).join(',')];
  for (const row of rows) lines.push(orderedColumns.map((column) => escapeCell(row[column])).join(','));
  return `${lines.join('\r\n')}\r\n`;
}

