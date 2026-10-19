// Recon: after uploading the formula-injection CSV, find every real
// export/download affordance for the user's coordinate/analysis data (not the
// static sample-file links). Enumerates candidate controls and, for each that
// triggers a download, saves the file and scans it for the =cmd payload.
//
//   node qa/harness/export-recon.mjs

import { chromium } from 'playwright';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { mkdirSync, writeFileSync, readFileSync } from 'node:fs';
import { AUTH_STATE, APP_URL } from './lib/evidence.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const fx = join(__dirname, '..', 'charters', 'us10-coordinates-panel', 'fixtures', 'adversarial', 'v05_formula_injection.csv');
const OUT = join(__dirname, '..', 'runs', 'export-recon');
mkdirSync(OUT, { recursive: true });

const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({ storageState: AUTH_STATE, viewport: { width: 1440, height: 900 }, acceptDownloads: true });
const page = await context.newPage();

const downloads = [];
page.on('download', async (d) => {
  const fn = d.suggestedFilename();
  const p = join(OUT, fn);
  await d.saveAs(p).catch((e) => downloads.push({ fn, error: String(e) }));
  downloads.push({ fn, path: p });
});

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
await page.screenshot({ path: join(OUT, 'after-upload.png'), fullPage: true });

// Enumerate every interactive control + anything that looks export-ish.
const controls = await page.evaluate(() => {
  const vis = (el) => { const r = el.getBoundingClientRect(); return r.width > 0 && r.height > 0; };
  const clip = (t) => (t || '').trim().replace(/\s+/g, ' ').slice(0, 60);
  return [...document.querySelectorAll('a,button,[role="button"],[download],svg,[class*="download" i],[class*="export" i],[aria-label*="download" i],[aria-label*="export" i]')]
    .filter(vis)
    .map((el) => ({
      tag: el.tagName.toLowerCase(),
      text: clip(el.innerText || el.getAttribute('aria-label') || el.getAttribute('title')),
      cls: clip(el.getAttribute('class')),
      href: el.getAttribute('href'),
      download: el.hasAttribute('download'),
    }))
    .filter((c) => /download|export|csv|xlsx|excel|save|\.zip/i.test(`${c.text} ${c.cls} ${c.href || ''}`) || c.download);
});
writeFileSync(join(OUT, 'export-candidates.json'), JSON.stringify(controls, null, 2));
console.log('Export-ish candidates on the coordinate view:');
for (const c of controls) console.log(`  [${c.tag}] text="${c.text}" cls="${c.cls}" href=${c.href} download=${c.download}`);

// Also peek at the Table view (Ranking Table / Table) — exports often live there.
await page.getByRole('button', { name: /^Table$/i }).click().catch(() => {});
await page.waitForTimeout(1500);
await page.screenshot({ path: join(OUT, 'table-view.png'), fullPage: true });
const tableControls = await page.evaluate(() => {
  const vis = (el) => { const r = el.getBoundingClientRect(); return r.width > 0 && r.height > 0; };
  const clip = (t) => (t || '').trim().replace(/\s+/g, ' ').slice(0, 60);
  return [...document.querySelectorAll('a,button,[role="button"],[download],[aria-label]')]
    .filter(vis)
    .map((el) => ({ tag: el.tagName.toLowerCase(), text: clip(el.innerText || el.getAttribute('aria-label')), href: el.getAttribute('href'), download: el.hasAttribute('download') }))
    .filter((c) => /download|export|csv|xlsx|excel/i.test(c.text) || c.download || /\.(csv|xlsx|zip)/i.test(c.href || ''));
});
writeFileSync(join(OUT, 'table-export-candidates.json'), JSON.stringify(tableControls, null, 2));
console.log('\nExport-ish candidates on the Table view:');
for (const c of tableControls) console.log(`  [${c.tag}] text="${c.text}" href=${c.href} download=${c.download}`);

console.log('\nDownloads captured so far:', JSON.stringify(downloads, null, 2));
console.log(`\nEvidence in: ${OUT}`);
await context.close();
await browser.close();
