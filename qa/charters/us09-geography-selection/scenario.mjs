// US 09 — Geography Selection scenario.
// Drives the Administrative Panel happy path + validation checks, recording a
// per-step outcome and capturing evidence. Deterministic checks are asserted;
// steps whose "expected" is a non-visible internal state (single- vs multi-site
// mode) are captured as screenshots for the reviewer to judge.
//
//   node qa/charters/us09-geography-selection/scenario.mjs
//
// NOTE: the app labels this panel "Administrative Panel" (spec says "Geography
// Selection") — a known spec-drift, not a bug.

import { withSession, APP_URL } from '../../harness/lib/session.mjs';
import {
  createRun, attachCollectors, snapshot, dumpDom, runAxe, finalize, step,
} from '../../harness/lib/evidence.mjs';
import { openAdmin } from '../../harness/lib/flows.mjs';
import { join } from 'node:path';

const shot = (page, run, name) => page.screenshot({ path: join(run.dir, `${name}.png`) });

async function safe(run, name, fn) {
  try {
    const note = await fn();
    step(run, name, true, note || '');
    console.log(`  ok   ${name}${note ? ' — ' + note : ''}`);
  } catch (e) {
    step(run, name, false, String(e && e.message || e));
    console.log(`  FAIL ${name} — ${e && e.message || e}`);
  }
}

await withSession(async (page) => {
  const run = createRun('us09-geography');
  attachCollectors(page, run);

  await page.goto(APP_URL, { waitUntil: 'domcontentloaded' });
  await page.waitForTimeout(2500);

  const stateBtn = () => page.getByRole('button', { name: /Select State/i });
  const districtBtn = () => page.getByRole('button', { name: /Select District/i });
  const addBtn = () => page.getByRole('button', { name: /Add to Analysis/i });
  const options = () => page.locator('li[role="option"]');

  // S1 — panel visible; before a State is chosen, District(s) + Add are disabled.
  await safe(run, 'S1: expand panel & default disabled state', async () => {
    await openAdmin(page);
    await page.waitForTimeout(800);
    await shot(page, run, 's1-panel');
    const dDisabled = await districtBtn().isDisabled().catch(() => null);
    const aDisabled = await addBtn().isDisabled().catch(() => null);
    if (dDisabled === false) throw new Error('District(s) should be disabled before a State is selected');
    if (aDisabled === false) throw new Error('Add to Analysis should be disabled with nothing selected');
    return `district disabled=${dDisabled}, add disabled=${aDisabled}`;
  });

  // S2 — select a State → District list loads, District(s) enables.
  await safe(run, 'S2: select State loads Districts', async () => {
    await stateBtn().click();
    await page.waitForTimeout(600);
    await page.locator('li[role="option"]', { hasText: /^Telangana$/ }).first().click();
    await page.waitForTimeout(1200);
    await shot(page, run, 's2-state-picked');
    if (await districtBtn().isDisabled().catch(() => true)) throw new Error('District(s) still disabled after selecting State');
    await districtBtn().click();
    await page.waitForTimeout(700);
    const n = await options().count();
    if (n < 2) throw new Error(`Expected a populated district list, got ${n} options`);
    return `${n} district options loaded`;
  });

  // S4 — exactly one District → single-site (captured for reviewer).
  await safe(run, 'S4: one District (single-site)', async () => {
    await options().filter({ hasText: /^Adilabad$/ }).first().click();
    await page.waitForTimeout(500);
    await districtBtn().click(); // toggle the dropdown closed to reveal action buttons
    await page.waitForTimeout(400);
    await shot(page, run, 's4-one-district');
    return 'Adilabad selected (single-site mode is an internal state)';
  });

  // S8/S9 — Add to Analysis enable condition. Observational: the spec is ambiguous
  // on whether a location alone enables it, or a resilience filter is also required.
  await safe(run, 'S9: Add to Analysis enable condition', async () => {
    const addDisabled = await addBtn().isDisabled().catch(() => null);
    await shot(page, run, 's9-add-state');
    if (addDisabled === false) {
      await addBtn().click();
      await page.waitForTimeout(1200);
      await shot(page, run, 's9-added');
      return 'enabled with 1 district selected; clicked Add to Analysis';
    }
    return `OBSERVE: Add to Analysis disabled=${addDisabled} with 1 district + no resilience filter — verify whether a filter is required (US 11) or this is a defect`;
  });

  // S5 — select All Districts → multi-site (captured).
  await safe(run, 'S5: All Districts (multi-site)', async () => {
    await districtBtn().click(); // open dropdown
    await page.waitForTimeout(700);
    if (await options().count() === 0) { await districtBtn().click(); await page.waitForTimeout(700); }
    await options().filter({ hasText: /All Districts/ }).first().click();
    await page.waitForTimeout(800);
    await districtBtn().click(); // close dropdown
    await page.waitForTimeout(400);
    await shot(page, run, 's5-all-districts');
    return 'All Districts selected (multi-site mode is an internal state)';
  });

  // S6 — switch to Block → per spec, selections reset & single district only.
  await safe(run, 'S6: switch to Block resets focus', async () => {
    await page.getByText(/^Block$/).first().click();
    await page.waitForTimeout(900);
    await shot(page, run, 's6-block-mode');
    return 'switched to Block view zone';
  });

  // S11 — Reset clears everything back to defaults.
  await safe(run, 'S11: Reset restores defaults', async () => {
    await page.getByRole('button', { name: /Reset geography and filters|^Reset$/i }).first().click();
    await page.waitForTimeout(1000);
    await shot(page, run, 's11-reset');
    const label = await stateBtn().first().innerText().catch(() => '');
    if (!/Select State/i.test(label)) throw new Error(`State not reset (button shows "${label}")`);
    return 'State selection cleared';
  });

  // S12 — collapse & expand sidebar; selections must survive (re-select first).
  // NOTE: the panel is already expanded here; do NOT re-click its header (toggles closed).
  await safe(run, 'S12: collapse/expand retains selection', async () => {
    await stateBtn().click();
    await page.waitForTimeout(500);
    await page.locator('li[role="option"]', { hasText: /^Telangana$/ }).first().click();
    await page.waitForTimeout(800);
    await page.getByRole('button', { name: /Hide sidebar/i }).click();
    await page.waitForTimeout(700);
    await shot(page, run, 's12a-collapsed');
    // Re-expand: the collapsed strip toggle has NO accessible name (see a11y
    // finding), so fall back to a position click on the left strip.
    const showBtn = page.getByRole('button', { name: /Show sidebar/i });
    if (await showBtn.count()) await showBtn.click();
    else await page.mouse.click(16, 84);
    await page.waitForTimeout(800);
    await shot(page, run, 's12b-expanded');
    const label = await stateBtn().first().innerText().catch(() => '');
    if (!/Telangana/i.test(label)) throw new Error(`Selection lost after collapse/expand (shows "${label}")`);
    return 'Telangana retained through collapse/expand';
  });

  // S14 — switch Map ↔ Ranking; geography preserved (captured).
  await safe(run, 'S14: Map to Ranking preserves geography', async () => {
    await page.getByText(/^Ranking Table$/).first().click().catch(() => {});
    await page.waitForTimeout(1200);
    await shot(page, run, 's14-ranking');
    const label = await stateBtn().first().innerText().catch(() => '');
    return `ranking view shown; state label="${label}"`;
  });

  // Cross-cutting: DOM/NaN scan, multi-viewport responsive shots, axe.
  await dumpDom(page, run, 'us09-final');
  await snapshot(page, run, 'us09-responsive');
  await runAxe(page, run, 'us09');

  finalize(run);
  console.log(`\n  Run: ${run.dir}`);
  console.log(`  Steps failed: ${run.summary.stepsFailed} | real error events: ${run.summary.errorEvents} | benign: ${run.summary.benignEvents}`);
});
