// DETONATOR TEST (Investigation B, real flow): does the stored formula-injection
// site name reach a DOWNLOADED report file unescaped?
//
// Flow discovered via recon:
//   upload v05 -> open "Select Resilience Filters" -> cascade-select
//   Risk Domain > Metric > Scenario > Period > Statistic (this resolves the
//   uploaded points to risk values and ENABLES "Add to Analysis") ->
//   Add to Analysis -> My Analysis panel -> Compare Portfolio ->
//   Download Reports -> capture file(s) -> scan for =cmd / @SUM / +1+1 / -2+3
//   and whether each is guarded by a leading apostrophe.
//
//   node qa/harness/portfolio-detonator.mjs
//
// Evidence -> qa/runs/portfolio-detonator/.

import { chromium } from 'playwright';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { mkdirSync, writeFileSync, readFileSync } from 'node:fs';
import { execSync } from 'node:child_process';
import { AUTH_STATE, APP_URL } from './lib/evidence.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const fx = join(__dirname, '..', 'charters', 'us10-coordinates-panel', 'fixtures', 'adversarial', 'v05_formula_injection.csv');
const OUT = join(__dirname, '..', 'runs', 'portfolio-detonator');
mkdirSync(OUT, { recursive: true });
const PAYLOAD_RE = /=cmd|@SUM\(1\+9\)|\+1\+1|-2\+3/;

const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({ storageState: AUTH_STATE, viewport: { width: 1440, height: 1200 }, acceptDownloads: true });
const page = await context.newPage();

const saved = [];
page.on('download', async (d) => {
  const fn = `${Date.now()}__${d.suggestedFilename()}`;
  const p = join(OUT, fn);
  try { await d.saveAs(p); saved.push({ suggested: d.suggestedFilename(), file: p }); console.log(`    [download] saved ${d.suggestedFilename()}`); }
  catch (e) { console.log(`    [download] save error ${e.message}`); }
});

const shot = (tag) => page.screenshot({ path: join(OUT, `${tag}.png`), fullPage: true }).catch(() => {});

/** Open the listbox for a field (by exact aria-label) and click its first option. */
async function pickFirst(fieldLabel) {
  const trigger = page.locator(`button[aria-label="${fieldLabel}"]`);
  if (await trigger.count() === 0) { console.log(`  ${fieldLabel}: trigger absent`); return false; }
  if (!(await trigger.first().isEnabled().catch(() => false))) { console.log(`  ${fieldLabel}: trigger DISABLED`); return false; }
  await trigger.first().click({ timeout: 5000 }).catch((e) => console.log(`  ${fieldLabel}: open err ${e.message}`));
  await page.waitForTimeout(700);
  const opt = page.locator('[role="option"]');
  const n = await opt.count();
  if (n === 0) { console.log(`  ${fieldLabel}: no options appeared`); await page.keyboard.press('Escape').catch(() => {}); return false; }
  const chosen = (await opt.first().innerText().catch(() => '')).trim().slice(0, 40);
  await opt.first().click({ timeout: 5000 }).catch((e) => console.log(`  ${fieldLabel}: pick err ${e.message}`));
  await page.waitForTimeout(900);
  console.log(`  ${fieldLabel}: picked "${chosen}" (of ${n})`);
  return true;
}

async function enabled(name) {
  const loc = page.getByRole('button', { name });
  return (await loc.count()) && (await loc.first().isEnabled().catch(() => false));
}

// ---- upload v05 ----
await page.goto(APP_URL, { waitUntil: 'domcontentloaded' });
await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => {});
await page.waitForTimeout(1200);
await page.getByRole('button', { name: /Coordinate Panel/i }).click(); await page.waitForTimeout(500);
await page.getByRole('button', { name: /^Upload Coordinates$/i }).click(); await page.waitForTimeout(400);
await page.locator('input[type="file"]').first().setInputFiles(fx); await page.waitForTimeout(400);
await page.getByRole('button', { name: /^Upload$/i }).first().click().catch(() => {}); await page.waitForTimeout(2500);
console.log('uploaded v05.');

// ---- cascade the resilience filters ----
console.log('\n-- cascading resilience filters --');
await page.getByText(/Select Resilience Filters/i).first().click().catch(() => {});
await page.waitForTimeout(1000);
for (const f of ['Risk Domain', 'Metric', 'Scenario', 'Period', 'Statistic']) {
  await pickFirst(f);
}
await shot('10-filters-selected');
// close the filter panel
await page.getByText(/Select Resilience Filters/i).first().click().catch(() => {});
await page.waitForTimeout(1200);

console.log(`\nAdd to Analysis enabled? ${await enabled(/^Add to Analysis$/i)}`);
console.log(`Save Analysis enabled?  ${await enabled(/^Save Analysis$/i)}`);

// ---- add to analysis (portfolio) ----
if (await enabled(/^Add to Analysis$/i)) {
  await page.getByRole('button', { name: /^Add to Analysis$/i }).first().click().catch((e) => console.log('add err', e.message));
  await page.waitForTimeout(2000);
  console.log('clicked Add to Analysis.');
}
await shot('11-after-add-to-analysis');

// ---- Open My Analysis full-screen modal, drive Compare Portfolio ----
// (Add to Analysis auto-opens the sidebar panel; going full-screen puts all
//  Compare Portfolio controls + the Download Reports tab in reach. Everything
//  below is scoped to the modal root — the collapsed sidebar keeps duplicate
//  copies of the same controls in the DOM behind it.)
console.log('\n-- Compare Portfolio (full-screen modal) --');
await shot('12-portfolio-panel');
const fsBtn = page.getByRole('button', { name: /Open My Analysis in full screen/i });
console.log(`  fullscreen button count=${await fsBtn.count()}`);
if (await fsBtn.count()) { await fsBtn.first().click().catch((e) => console.log('fs err', e.message)); }
// Wait for the modal to actually become visible before driving it.
const modal = page.locator('[data-modal-root="true"]');
await modal.first().waitFor({ state: 'visible', timeout: 8000 }).catch(() => console.log('  WARN: modal did not become visible'));
await page.waitForTimeout(800);
await shot('12b-modal-open');
const M = modal;

// Tick Scenario + Period checkboxes FIRST — the metrics option panel, once
// open, overlays this region and blocks clicks on it.
for (const grp of [/SSP2-4\.5|SSP5-8\.5/, /Early century|Mid-century|End century/]) {
  const cb = M.getByRole('checkbox', { name: grp });
  if (await cb.count()) { await cb.first().check().catch(() => {}); await page.waitForTimeout(400); }
}

// Select Metrics (required — Table/report is empty until a metric is chosen).
// The option list renders as <label>+<input type=checkbox> pairs and its panel
// stays open, overlaying the tab row; we deal with that via dispatchEvent below.
const metricsBtn = M.getByRole('button', { name: /^Select Metrics$/i });
console.log(`  Select Metrics button count=${await metricsBtn.count()}`);
if (await metricsBtn.count()) {
  await metricsBtn.first().click().catch(() => {});
  await page.waitForTimeout(900);
  const picked = await page.evaluate(() => {
    const vis = (el) => { const r = el.getBoundingClientRect(); return r.width > 0 && r.height > 0; };
    const cbs = [...document.querySelectorAll('input[type="checkbox"]')].filter(vis);
    const isMetric = (t) => /°C|#Days|#Events|HWFI|TNx|Composite|Warmest|Amplitude|Index|score/i.test(t) && !/century|SSP|scenario|period/i.test(t);
    const target = cbs.find((cb) => { const lab = cb.closest('label') || cb.parentElement; return lab && isMetric(lab.innerText || ''); });
    if (!target) return null;
    target.click();
    const lab = target.closest('label') || target.parentElement;
    return (lab.innerText || '').trim().slice(0, 40);
  });
  console.log(picked ? `  metric picked: "${picked}"` : '  no metric checkbox found');
}
await page.waitForTimeout(1000);
await shot('13-compare-populated');

// ---- Download Reports tab -> report buttons ----
// The metrics option panel may still overlay the tab row, so switch tabs with a
// synthetic click dispatched straight to the tab element (bypasses hit-testing).
console.log('\n-- Download Reports --');
const dlTab = M.getByRole('button', { name: /^Download Reports$/i });
if (await dlTab.count()) {
  await dlTab.first().dispatchEvent('click').catch((e) => console.log('dl-tab dispatch err', e.message));
  await page.waitForTimeout(1800);
  await shot('14-download-tab');
  // Download Table / Download Visualization / Download All. Use dispatchEvent to
  // be robust against any lingering overlay; each should trigger a download.
  for (const nm of [/^Download Table$/i, /^Download All$/i, /^Download Visualization$/i]) {
    const loc = M.getByRole('button', { name: nm });
    if (await loc.count()) {
      console.log(`  clicking modal button ${nm}`);
      await loc.first().dispatchEvent('click').catch((e) => console.log(`    err ${e.message}`));
      await page.waitForTimeout(3500);
    } else {
      console.log(`  ${nm} absent`);
    }
  }
} else {
  console.log('Download Reports tab absent in modal.');
}
await shot('15-after-download-reports');
await page.waitForTimeout(1500);

// ---- scan downloads ----
console.log('\n================ PAYLOAD SCAN ================');
if (!saved.length) console.log('No downloads captured. (see screenshots 12-15 for why)');
for (const s of saved) {
  console.log(`\nFILE: ${s.suggested}`);
  if (/\.(xlsx|zip)$/i.test(s.suggested)) {
    try {
      const hit = execSync(`unzip -p "${s.file}" 2>/dev/null | grep -aoE "'?=cmd[^<\\"]{0,25}|'?@SUM\\(1\\+9\\)[^<\\"]{0,15}|'?\\+1\\+1|'?-2\\+3" | head -40 || true`, { encoding: 'utf8', shell: '/bin/bash' });
      if (!hit.trim()) { console.log('  payload strings: (none in archive)'); continue; }
      for (const line of hit.trim().split('\n')) {
        const guarded = /^'/.test(line);
        console.log(`  ${JSON.stringify(line)}  -> ${guarded ? 'GUARDED (leading apostrophe)' : 'UNESCAPED (VULNERABLE)'}`);
      }
    } catch (e) { console.log('  scan error:', e.message); }
  } else {
    const txt = readFileSync(s.file, 'utf8');
    for (const l of txt.split(/\r?\n/).filter((x) => PAYLOAD_RE.test(x))) {
      const cell = (l.match(/("?)('?[=@+\-][^",]*)/) || [])[2] || l;
      const guarded = /^'/.test(cell.replace(/^"/, ''));
      console.log(`  RAW: ${JSON.stringify(l.slice(0, 100))} -> ${guarded ? 'guarded' : 'UNESCAPED (VULNERABLE)'}`);
    }
  }
}
console.log(`\nEvidence in: ${OUT}`);
await context.close();
await browser.close();
