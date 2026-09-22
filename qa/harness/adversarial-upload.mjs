// Adversarial fuzz of the US 10 "Upload Coordinates" validation.
//
// For each garbage fixture: fresh authed context -> open Coordinate Panel ->
// Upload Coordinates -> drive the file input -> click Upload -> capture the
// panel text (before/after), any toast, upload network status, console/http
// errors, and any JS dialog (XSS canary). Each fixture runs in its own context
// so accept/reject state does not bleed between cases.
//
//   node qa/harness/adversarial-upload.mjs
//
// Evidence + results.json land in qa/runs/<ts>_us10-adversarial-upload/.

import { chromium } from 'playwright';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { writeFileSync } from 'node:fs';
import { statSync } from 'node:fs';
import { AUTH_STATE, APP_URL, createRun, attachCollectors, finalize } from './lib/evidence.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const fx = (f) => join(__dirname, '..', 'charters', 'us10-coordinates-panel', 'fixtures', 'adversarial', f);

// file, why it is garbage, and what a *correct* validator should do.
const CASES = [
  ['v01_outofrange.csv', 'lat 999 / long -500 / 1e9', 'reject-or-flag'],
  ['v02_nonnumeric.csv', 'abc/#$% in numeric cells', 'reject'],
  ['v03_empty_coords.csv', 'empty lat/long cells', 'reject'],
  ['v04_outofindia.csv', 'London / Pacific / Null Island (out of coverage)', 'reject-or-flag'],
  ['v05_formula_injection.csv', 'CSV formula injection in custom_name', 'sanitize'],
  ['v06_longname.csv', '5000-char name + absurd coord precision', 'reject-or-truncate'],
  ['v07_unicode.csv', 'emoji / <script> / RTL override in name', 'sanitize'],
  ['v08_weird_numbers.csv', 'dup ids / padded / negative-zero', 'accept-or-flag'],
  ['f01_over1mb.csv', '1.6 MB (UI says max 1 MB)', 'reject-size'],
  ['f02_empty.csv', '0-byte file', 'reject'],
  ['f03_header_only.csv', 'header, no rows', 'reject'],
  ['f04_malformed.csv', 'unclosed quote / ragged cols / embedded newline', 'reject'],
  ['f05_binary_as.csv', 'PNG bytes renamed .csv', 'reject'],
  ['f06_bogus_shapefile.zip', 'valid zip, no .shp/.shx/.dbf', 'reject'],
  ['f07_csv_named.xlsx', 'CSV bytes renamed .xlsx', 'reject'],
];

const ERROR_RE = /invalid|unsupported|not supported|error|failed|unable|exceed|too large|too big|maximum|max size|size limit|wrong|incorrect|provided sample|is required|valid coordinates|out of|not within|coverage/i;
const ACCEPT_RE = /uploaded successfully|added to|point\s*\d|coordinates? (uploaded|added|imported)|successfully/i;

async function panelText(page) {
  return page.evaluate(() => {
    const host = [...document.querySelectorAll('div,section,aside')]
      .find((el) => /Coordinate Panel/i.test(el.textContent || ''));
    return (host || document.body).innerText.replace(/\s+/g, ' ').trim();
  });
}

async function grabToast(page, ms = 3500) {
  const end = Date.now() + ms;
  let seen = '';
  while (Date.now() < end) {
    const body = await page.evaluate(() => document.body.innerText.replace(/\s+/g, ' '));
    const m = body.match(/(Invalid file format[^.]*\.|Unsupported file format|Unable to upload[^.]*\.|[^.]*could not be resolved[^.]*\.|[^.]*max(imum)?[^.]*1\s*MB[^.]*|[^.]*file size[^.]*\.|[^.]*too large[^.]*\.)/i);
    if (m) { seen = m[0].trim(); break; }
    await page.waitForTimeout(120);
  }
  return seen;
}

async function runCase(run, file, why, expect) {
  const rec = { file, why, expect, sizeBytes: statSync(fx(file)).size };
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({
    storageState: AUTH_STATE,
    viewport: { width: 1440, height: 900 },
  });
  const page = await context.newPage();
  attachCollectors(page, run);

  const net = [];
  const dialogs = [];
  page.on('requestfinished', async (req) => {
    const u = req.url();
    if (/upload|coordinate|import|geo\//i.test(u) && req.method() !== 'GET') {
      const r = await req.response();
      net.push({ url: u.slice(-80), method: req.method(), status: r ? r.status() : null });
    }
  });
  page.on('dialog', async (d) => { dialogs.push({ type: d.type(), message: d.message() }); await d.dismiss().catch(() => {}); });

  try {
    await page.goto(APP_URL, { waitUntil: 'domcontentloaded' });
    await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => {});
    await page.waitForTimeout(1200);
    await page.getByRole('button', { name: /Coordinate Panel/i }).click();
    await page.waitForTimeout(800);
    await page.getByRole('button', { name: /^Upload Coordinates$/i }).click();
    await page.waitForTimeout(600);

    rec.panelBefore = (await panelText(page)).slice(0, 500);

    const input = page.locator('input[type="file"]');
    if (await input.count() === 0) throw new Error('no input[type=file] found');
    await input.first().setInputFiles(fx(file));
    await page.waitForTimeout(600);
    await page.getByRole('button', { name: /^Upload$/i }).first().click().catch(() => {});

    rec.toast = await grabToast(page, 4000);
    await page.waitForTimeout(800);
    rec.panelAfter = (await panelText(page)).slice(0, 700);
    await page.screenshot({ path: join(run.dir, `adv-${file.replace(/\W+/g, '_')}.png`) }).catch(() => {});

    const both = `${rec.toast} || ${rec.panelAfter}`;
    rec.rejected = ERROR_RE.test(both);
    rec.acceptedSignal = ACCEPT_RE.test(rec.panelAfter);
    // Injection canaries
    rec.scriptReflectedRaw = /<script>/i.test(await page.content());
    rec.formulaReflected = /=cmd|SUM\(1\+9\)/i.test(rec.panelAfter);
    rec.dialogs = dialogs;
    rec.upload = net;
    rec.verdict = rec.rejected && !rec.acceptedSignal ? 'REJECTED'
      : rec.acceptedSignal && !rec.rejected ? 'ACCEPTED'
      : rec.rejected && rec.acceptedSignal ? 'MIXED'
      : 'NO-VISIBLE-FEEDBACK';
    rec.error = null;
  } catch (e) {
    rec.error = String(e && e.message || e);
    rec.verdict = 'DRIVE-ERROR';
  } finally {
    await context.close();
    await browser.close();
  }
  return rec;
}

const run = createRun('us10-adversarial-upload');
const results = [];
for (const [file, why, expect] of CASES) {
  process.stdout.write(`${file.padEnd(28)} `);
  const rec = await runCase(run, file, why, expect);
  results.push(rec);
  const flags = [];
  if (rec.dialogs?.length) flags.push('JS-DIALOG!');
  if (rec.scriptReflectedRaw) flags.push('SCRIPT-IN-DOM');
  console.log(`${rec.verdict}  net=${JSON.stringify(rec.upload?.map(n => n.status) || [])} toast="${(rec.toast || '').slice(0, 60)}" ${flags.join(' ')}`);
}
run.adversarial = results;
writeFileSync(join(run.dir, 'adversarial-results.json'), JSON.stringify(results, null, 2));
finalize(run);
console.log(`\nrun dir: ${run.dir}`);
