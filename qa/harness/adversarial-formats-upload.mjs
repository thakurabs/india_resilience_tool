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
const fx = (f) => join(__dirname, '..', 'charters', 'us10-coordinates-panel', 'fixtures', 'adversarial_formats', f);

// XLSX + shapefile-ZIP cases, built by mutating the app's OWN valid samples.
const CASES = [
  // --- .xlsx parser ---
  ['x01_xlsx_formula.xlsx', 'XLSX: formula injection in custom_name cell', 'sanitize'],
  ['x02_xlsx_outofrange.xlsx', 'XLSX: lat 999 / long -500', 'reject-or-flag'],
  ['x03_xlsx_nonnumeric.xlsx', 'XLSX: "abc" in a coordinate cell', 'reject'],
  ['x04_xlsx_docschema.xlsx', 'XLSX: documented schema Latitude/Longitude/Label', 'observe'],
  ['x05_xlsx_empty_coords.xlsx', 'XLSX: empty coordinate cells', 'reject'],
  // --- .zip shapefile parser ---
  ['z01_shp_baseline.zip', 'ZIP: app\'s OWN valid sample shapefile (control)', 'accept'],
  ['z02_shp_formula_dbf.zip', 'ZIP: formula injection in .dbf attribute', 'sanitize'],
  ['z03_shp_missing_shp.zip', 'ZIP: missing required .shp component', 'reject'],
  ['z04_shp_only_dbf.zip', 'ZIP: only .dbf, no geometry', 'reject'],
  ['z05_shp_longname_dbf.zip', 'ZIP: 50-char custom_name in .dbf', 'accept-or-flag'],
  ['z06_shp_plus_junk.zip', 'ZIP: valid shapefile + 200 KB junk member', 'accept-or-flag'],
  ['z07_shp_outofindia.zip', 'ZIP: valid shapefile, points in London/Pacific/Null Island', 'reject-or-flag'],
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

const run = createRun('us10-adversarial-formats');
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
