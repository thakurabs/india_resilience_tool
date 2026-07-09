// Investigation A: Save Analysis -> My Analysis. After saving a portfolio that
// contains the formula-injection site names, does reopening / the saved
// analysis "Actions" (⋮) menu expose a download / share / export that carries
// the user-supplied custom_name (the injection vector)?
//
//   node qa/harness/saved-analysis-probe.mjs
//
// Evidence -> qa/runs/saved-analysis-probe/.

import { chromium } from 'playwright';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { mkdirSync, writeFileSync, readFileSync } from 'node:fs';
import { execSync } from 'node:child_process';
import { AUTH_STATE, APP_URL } from './lib/evidence.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const fx = join(__dirname, '..', 'charters', 'us10-coordinates-panel', 'fixtures', 'adversarial', 'v05_formula_injection.csv');
const OUT = join(__dirname, '..', 'runs', 'saved-analysis-probe');
mkdirSync(OUT, { recursive: true });
const PAYLOAD_RE = /=cmd|@SUM\(1\+9\)|\+1\+1|-2\+3/;
const NAME = `INJ-A ${new Date().toISOString().slice(11, 19)}`;

const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({ storageState: AUTH_STATE, viewport: { width: 1440, height: 1200 }, acceptDownloads: true });
const page = await context.newPage();

const saved = [];
page.on('download', async (d) => {
  const fn = `${Date.now()}__${d.suggestedFilename()}`;
  const p = join(OUT, fn);
  try { await d.saveAs(p); saved.push({ suggested: d.suggestedFilename(), file: p }); console.log(`    [download] ${d.suggestedFilename()}`); }
  catch (e) { console.log(`    [download] err ${e.message}`); }
});
const shot = (t) => page.screenshot({ path: join(OUT, `${t}.png`), fullPage: true }).catch(() => {});
async function pickFirst(field) {
  const t = page.locator(`button[aria-label="${field}"]`);
  if (!(await t.count()) || !(await t.first().isEnabled().catch(() => false))) return;
  await t.first().click().catch(() => {}); await page.waitForTimeout(600);
  const o = page.locator('[role="option"]'); if (await o.count()) await o.first().click().catch(() => {});
  await page.waitForTimeout(700);
}

await page.goto(APP_URL, { waitUntil: 'domcontentloaded' });
await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => {});
await page.waitForTimeout(1200);
await page.getByRole('button', { name: /Coordinate Panel/i }).click(); await page.waitForTimeout(500);
await page.getByRole('button', { name: /^Upload Coordinates$/i }).click(); await page.waitForTimeout(400);
await page.locator('input[type="file"]').first().setInputFiles(fx); await page.waitForTimeout(400);
await page.getByRole('button', { name: /^Upload$/i }).first().click().catch(() => {}); await page.waitForTimeout(2500);
await page.getByText(/Select Resilience Filters/i).first().click().catch(() => {}); await page.waitForTimeout(900);
for (const f of ['Risk Domain', 'Metric', 'Scenario', 'Period', 'Statistic']) await pickFirst(f);
await page.getByText(/Select Resilience Filters/i).first().click().catch(() => {}); await page.waitForTimeout(900);

// ---- Save Analysis ----
console.log('-- Save Analysis --');
await page.getByRole('button', { name: /^Save Analysis$/i }).first().click().catch((e) => console.log('save click err', e.message));
await page.waitForTimeout(1200);
await shot('A1-save-dialog');
// The modal's Analysis Name input PREFILLS a placeholder derived from the first
// site's injected custom_name ("=cmd|' /C calc'!A1 - Nampally - ..."). Note that
// default and overwrite it with a unique name so we can find the entry later.
const nameInput = page.locator('input[placeholder^="=cmd"], input[placeholder*="Nampally"]');
const defaultName = (await nameInput.count()) ? await nameInput.first().getAttribute('placeholder').catch(() => null) : null;
console.log(`  DEFAULT analysis-name (from injected custom_name): ${JSON.stringify(defaultName)}`);
if (await nameInput.count()) { await nameInput.first().fill(NAME).catch(() => {}); console.log(`  overrode with unique name "${NAME}"`); }
await page.waitForTimeout(300);
// Confirm via the modal's Save Analysis button (the enabled dialog one).
const confirm = page.getByRole('button', { name: /^Save Analysis$/i });
const cnt = await confirm.count();
await confirm.nth(cnt - 1).click().catch((e) => console.log('confirm err', e.message));
await page.waitForTimeout(1800);
await shot('A2-after-save');

// Reliable way to open the My Analysis panel: Add the portfolio to analysis —
// that auto-opens the panel where the Saved Analysis list + Actions menus live.
await page.getByRole('button', { name: /^Add to Analysis$/i }).first().click().catch(() => {});
await page.waitForTimeout(2000);

// ---- Open My Analysis, find the saved analysis, open its Actions (⋮) ----
console.log('\n-- Saved analysis Actions menu --');
// Open the My Analysis panel (top-right collapsible) and wait for the list.
if (await page.getByText(/Saved Analysis/i).count() === 0) {
  await page.getByText(/^My Analysis$/i).first().click().catch(() => {});
  await page.getByText(/Saved Analysis/i).first().waitFor({ state: 'visible', timeout: 6000 }).catch(() => console.log('  Saved Analysis list did not appear'));
  await page.waitForTimeout(800);
}
await shot('A3-my-analysis');
// Enumerate the saved-analysis entries so we can see the stored NAME (which the
// modal prefilled from the injected custom_name).
const savedNames = await page.evaluate(() => {
  const vis = (el) => { const r = el.getBoundingClientRect(); return r.width > 0 && r.height > 0; };
  return [...document.querySelectorAll('button')].filter(vis)
    .map((b) => (b.getAttribute('aria-label') || b.innerText || '').trim())
    .filter((t) => /^Actions for |^=cmd|Single District|sites_|Analysis -/i.test(t)).slice(0, 20);
});
console.log('  saved-analysis entries:', JSON.stringify(savedNames, null, 2));
const actions = page.getByRole('button', { name: new RegExp(`Actions for ${NAME.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}`, 'i') });
console.log(`  Actions button for new analysis count=${await actions.count()}`);
let actionsBtn = actions;
if (await actions.count() === 0) {
  // fall back to the first "Actions for ..." button
  actionsBtn = page.getByRole('button', { name: /^Actions for /i });
  console.log(`  fallback Actions buttons count=${await actionsBtn.count()}`);
}
if (await actionsBtn.count()) {
  await actionsBtn.first().click().catch((e) => console.log('actions err', e.message));
  await page.waitForTimeout(1000);
  await shot('A4-actions-menu');
  const menu = await page.evaluate(() => {
    const vis = (el) => { const r = el.getBoundingClientRect(); return r.width > 0 && r.height > 0; };
    return [...document.querySelectorAll('[role="menuitem"],button,a,li')].filter(vis)
      .map((el) => (el.innerText || '').trim()).filter((t) => t && t.length < 40 && /download|share|export|open|view|rename|delete|duplicate|copy|report/i.test(t));
  });
  console.log('  menu items:', JSON.stringify([...new Set(menu)]));
  // Click any download/share/export/report option and capture.
  for (const nm of [/download/i, /export/i, /report/i, /share/i, /open|view/i]) {
    const mi = page.getByRole('menuitem', { name: nm });
    const bt = page.getByRole('button', { name: nm });
    for (const loc of [mi, bt]) {
      if (await loc.count()) { console.log(`  clicking ${nm}`); await loc.first().click({ timeout: 4000 }).catch(() => {}); await page.waitForTimeout(2500); await shot(`A5-clicked-${nm.source.replace(/\W+/g, '')}`); }
    }
  }
}

// ---- scan any downloads for the payload ----
console.log('\n================ PAYLOAD SCAN ================');
if (!saved.length) console.log('No downloads captured in the Save/Actions flow.');
for (const s of saved) {
  console.log(`\nFILE: ${s.suggested}`);
  if (/\.(xlsx|zip)$/i.test(s.suggested)) {
    const raw = execSync(`unzip -p "${s.file}" 2>/dev/null | grep -aoE "cmd|SUM\\(1|Nampally|Ibrahimpatnam|Serilingampally|Vijayawada|'?=cmd|'?\\+1\\+1|'?-2\\+3" | sort | uniq -c || true`, { encoding: 'utf8', shell: '/bin/bash' });
    console.log('  name/payload tokens in archive:\n' + (raw.trim() ? raw.trim().split('\n').map((l) => '    ' + l.trim()).join('\n') : '    (none)'));
  } else {
    const txt = readFileSync(s.file, 'utf8');
    const lines = txt.split(/\r?\n/).filter((l) => PAYLOAD_RE.test(l) || /Nampally|Ibrahimpatnam/i.test(l));
    console.log(lines.length ? lines.slice(0, 10).map((l) => '    ' + JSON.stringify(l.slice(0, 100))).join('\n') : '    (no site refs)');
  }
}
console.log(`\nEvidence in: ${OUT}`);
await context.close();
await browser.close();
