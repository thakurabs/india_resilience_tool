// Upload the formula-injection CSV, then CLICK every download candidate
// (coordinate-view .csv/.xlsx/.zip links + Table-view .csv/.xlsx) and capture
// whatever downloads. For each captured file, scan for the =cmd payload and
// report whether it is escaped (leading apostrophe) or written raw.
//
//   node qa/harness/export-capture.mjs

import { chromium } from 'playwright';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { mkdirSync, readFileSync, existsSync } from 'node:fs';
import { execSync } from 'node:child_process';
import { AUTH_STATE, APP_URL } from './lib/evidence.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const fx = join(__dirname, '..', 'charters', 'us10-coordinates-panel', 'fixtures', 'adversarial', 'v05_formula_injection.csv');
const OUT = join(__dirname, '..', 'runs', 'export-capture');
mkdirSync(OUT, { recursive: true });

const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({ storageState: AUTH_STATE, viewport: { width: 1440, height: 900 }, acceptDownloads: true });
const page = await context.newPage();

const saved = [];

/** Click a locator and, if it triggers a download within `ms`, save + return it. */
async function clickForDownload(label, locator, ms = 6000) {
  try {
    if (await locator.count() === 0) { console.log(`  - ${label}: (not present)`); return; }
    const dlP = page.waitForEvent('download', { timeout: ms }).catch(() => null);
    await locator.first().click({ timeout: 4000 }).catch(() => {});
    const dl = await dlP;
    if (!dl) { console.log(`  - ${label}: click did NOT trigger a download`); return; }
    const fn = `${label.replace(/\W+/g, '_')}__${dl.suggestedFilename()}`;
    const p = join(OUT, fn);
    await dl.saveAs(p);
    saved.push({ label, file: p, suggested: dl.suggestedFilename() });
    console.log(`  - ${label}: DOWNLOADED -> ${fn}`);
  } catch (e) {
    console.log(`  - ${label}: error ${String(e && e.message || e)}`);
  }
}

await page.goto(APP_URL, { waitUntil: 'domcontentloaded' });
await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => {});
await page.waitForTimeout(1200);
await page.getByRole('button', { name: /Coordinate Panel/i }).click();
await page.waitForTimeout(600);
await page.getByRole('button', { name: /^Upload Coordinates$/i }).click();
await page.waitForTimeout(500);
await page.locator('input[type="file"]').first().setInputFiles(fx);
await page.waitForTimeout(500);
await page.getByRole('button', { name: /^Upload$/i }).first().click().catch(() => {});
await page.waitForTimeout(2500);
console.log('Uploaded v05_formula_injection.csv. Clicking download candidates:\n');

// Coordinate-view "sample/supported format" links.
await clickForDownload('coord_csv', page.getByRole('link', { name: /Comma separated file \(\.csv\)/i }));
await clickForDownload('coord_xlsx', page.getByRole('link', { name: /Spreadsheet \(\.xlsx\)/i }));
await clickForDownload('coord_zip', page.getByRole('link', { name: /Zipped shapefile \(\.zip\)/i }));

// Table view.
await page.getByRole('button', { name: /^Table$/i }).click().catch(() => {});
await page.waitForTimeout(1800);
await page.screenshot({ path: join(OUT, 'table-view.png'), fullPage: true });
await clickForDownload('table_csv', page.getByRole('link', { name: /Comma separated file \(\.csv\)/i }));
await clickForDownload('table_xlsx', page.getByRole('link', { name: /Spreadsheet \(\.xlsx\)/i }));
// Some tables expose a generic Download/Export button:
await clickForDownload('table_download_btn', page.getByRole('button', { name: /download|export/i }));

console.log('\n================ PAYLOAD SCAN ================');
const PAYLOAD = /=cmd|@SUM\(1\+9\)|\+1\+1|-2\+3/;
for (const s of saved) {
  console.log(`\nFILE: ${s.suggested}  (${s.label})`);
  const buf = readFileSync(s.file);
  if (s.suggested.endsWith('.xlsx') || s.suggested.endsWith('.zip')) {
    // xlsx/zip: unzip and grep the shared-strings / sheet xml.
    try {
      const listing = execSync(`unzip -l "${s.file}"`, { encoding: 'utf8' });
      const hit = execSync(`unzip -p "${s.file}" 2>/dev/null | grep -aoE "'?=cmd[^<\"]{0,20}|'?\\+1\\+1|'?@SUM\\(1\\+9\\)" | head -20 || true`, { encoding: 'utf8', shell: '/bin/bash' });
      console.log('  (zip archive) payload strings found in contents:');
      console.log(hit.trim() ? hit.trim().split('\n').map((l) => '    ' + JSON.stringify(l)).join('\n') : '    (none)');
    } catch (e) { console.log('  xlsx/zip scan error:', String(e.message)); }
  } else {
    const txt = buf.toString('utf8');
    const lines = txt.split(/\r?\n/).filter((l) => PAYLOAD.test(l));
    if (!lines.length) { console.log('  payload NOT present in this file'); continue; }
    for (const l of lines) {
      // Escaped if the =cmd cell is prefixed with a single quote or a tab/space guard.
      const cell = (l.match(/("?)('?=cmd[^",]*)/) || [])[2] || l;
      const escaped = /^'/.test(cell.replace(/^"/, '')) || /^\t|^ /.test(cell.replace(/^"/, ''));
      console.log(`    RAW LINE: ${JSON.stringify(l.slice(0, 100))}`);
      console.log(`    -> starts-with-formula-char & UNescaped: ${!escaped ? 'YES (VULNERABLE)' : 'no (guarded)'}`);
    }
  }
}
if (!saved.length) console.log('No downloads were produced by any candidate.');
console.log(`\nEvidence in: ${OUT}`);
await context.close();
await browser.close();
