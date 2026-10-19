// UX-flow recon, Phase C: the alternate location path and the global chrome.
//   - Coordinate Analysis (manual entry + upload affordances, nothing uploaded)
//   - Block administrative level with a committed filter set
//   - "All Districts" multi-select
//   - header: feedback modal, account menu, quick guide, hover toggle
//   - Reset semantics
// Read-only: nothing is submitted, uploaded, saved or downloaded.
import { withSession } from './lib/session.mjs';
import { createRun, attachCollectors, finalize } from './lib/evidence.mjs';
import {
  attachApiRecorder, captureState, stage, enumerateOptions, selectOption, closeList,
  writeJson, writeApiLog,
} from './lib/recon.mjs';

const trig = (page, aria) => page.getByRole('button', { name: new RegExp(`^${aria}$`, 'i') }).first();
const pick = async (page, aria, text) => {
  const t = trig(page, aria);
  const chosen = await selectOption(page, t, text);
  await closeList(page, t);
  return chosen;
};

/** Toasts mount top-right and intercept clicks on the profile panel header. */
async function dismissToasts(page) {
  for (let i = 0; i < 6; i += 1) {
    const x = page.getByRole('button', { name: /Dismiss (success|error) message/i }).first();
    if (!(await x.isVisible().catch(() => false))) break;
    await x.click({ timeout: 3000 }).catch(() => {});
    await page.waitForTimeout(350);
  }
}

/** Click through a "Switch to …?" / "Clear …?" confirm if one mounts. */
async function acceptConfirm(page, run, label) {
  for (let i = 0; i < 8; i += 1) {
    const modal = page.locator('[data-modal-root]').first();
    if (await modal.isVisible().catch(() => false)) {
      const txt = await modal.innerText().catch(() => '');
      if (/switch|clear|proceed|confirm|continue/i.test(txt)) {
        await captureState(page, run, `${label}-confirm`, { note: 'mode-switch confirm dialog' });
        const btn = modal.getByRole('button', { name: /switch|proceed|continue|yes|confirm|ok/i }).first();
        if (await btn.isVisible().catch(() => false)) { await btn.click(); await page.waitForTimeout(1500); return true; }
      }
    }
    await page.waitForTimeout(500);
  }
  return false;
}

await withSession(async (page) => {
  const run = createRun('ux-flow-C');
  attachCollectors(page, run);
  attachApiRecorder(page, run);
  const found = {};
  page.setDefaultTimeout(9000);

  await page.goto('https://dev.resilience.org.in/', { waitUntil: 'domcontentloaded' });
  await page.waitForLoadState('networkidle', { timeout: 25000 }).catch(() => {});
  await page.waitForTimeout(2500);
  for (const re of [/Expand administrative analysis/i, /Expand resilience filters panel/i]) {
    const b = page.getByRole('button', { name: re }).first();
    if (await b.isVisible().catch(() => false)) { await b.click(); await page.waitForTimeout(800); }
  }

  // ---- C1: block level with a committed cascade --------------------------
  await stage(run, 'C1-block-level', async () => {
    await pick(page, 'Select State', 'Telangana');
    await page.locator('input[type="radio"][value="block"]').first().click();
    await page.waitForTimeout(2000);
    found.blocks = await enumerateOptions(page, trig(page, 'Select Block\\(s\\)'), 'Blocks (TG)');
    if (found.blocks.length > 1) {
      await pick(page, 'Select Block\\(s\\)', found.blocks[1]);
      await page.waitForTimeout(1200);
    }
    await pick(page, 'Risk Domain', 'Heat Risk');
    await pick(page, 'Metric', 'Heat Risk Composite (score)');
    await pick(page, 'Scenario', 'Business as usual');
    await page.waitForTimeout(800);
    await pick(page, 'Period', 'Mid century');
    await pick(page, 'Statistic', 'Mean');
    await page.waitForTimeout(3500);
    await captureState(page, run, 'C1-block-committed',
      { shots: 'all', note: 'Block level + Heat Risk Composite committed' });
  });

  // ---- C2: All Districts --------------------------------------------------
  await stage(run, 'C2-all-districts', async () => {
    await page.locator('input[type="radio"][value="district"]').first().click();
    await page.waitForTimeout(1800);
    await pick(page, 'Select District\\(s\\)', 'All Districts');
    await page.waitForTimeout(3500);
    await captureState(page, run, 'C2-all-districts', { shots: 'all', note: '"All Districts" selected' });
  });

  // ---- C3: historical scenario on a constituent metric -------------------
  await stage(run, 'C3-historical', async () => {
    await pick(page, 'Metric', 'Annual Mean Temperature (TM Mean) (°C)');
    await pick(page, 'Scenario', 'Historical');
    await page.waitForTimeout(900);
    found.historicalPeriods = await enumerateOptions(page, trig(page, 'Period'), 'Periods@Historical');
    if (found.historicalPeriods.length) await pick(page, 'Period', found.historicalPeriods[0]);
    found.historicalStats = await enumerateOptions(page, trig(page, 'Statistic'), 'Stats@Historical');
    if (found.historicalStats.length) await pick(page, 'Statistic', found.historicalStats[0]);
    found.historicalMapModes = await enumerateOptions(page, trig(page, 'Map Mode'), 'MapModes@Historical');
    await page.waitForTimeout(2500);
    await captureState(page, run, 'C3-historical-metric',
      { note: 'constituent metric on Historical scenario — the non-composite path' });
  });

  // ---- C4: coordinate analysis -------------------------------------------
  await stage(run, 'C4-coordinate-panel', async () => {
    await page.getByRole('button', { name: /^Coordinate Analysis$/ }).first().click();
    await page.waitForTimeout(1200);
    await acceptConfirm(page, run, 'C4');
    await page.waitForTimeout(2000);
    await captureState(page, run, 'C4-coordinate-panel',
      { shots: 'all', note: 'Coordinate Analysis panel — manual + upload affordances' });
    const dom = await page.evaluate(() => ({
      inputs: [...document.querySelectorAll('input')].map((i) => ({
        type: i.type, placeholder: i.placeholder, name: i.name, accept: i.accept || null,
      })),
      links: [...document.querySelectorAll('a[download], a[href*="sample" i], a[href*="template" i]')]
        .map((a) => ({ text: a.innerText.trim(), href: a.getAttribute('href') })),
    }));
    found.coordinatePanel = dom;
  });

  await stage(run, 'C5-manual-coordinate', async () => {
    const nums = page.locator('input[type="number"], input[placeholder*="itude" i]');
    const n = await nums.count();
    if (n >= 2) {
      await nums.nth(0).fill('17.9784');
      await nums.nth(1).fill('79.5941');
      const nameBox = page.locator('input[placeholder*="name" i], input[placeholder*="label" i]').first();
      if (await nameBox.isVisible().catch(() => false)) await nameBox.fill('Recon Point');
      await page.waitForTimeout(700);
      await captureState(page, run, 'C5-coordinate-filled', { note: 'manual lat/lon entered, not yet staged' });
      const show = page.getByRole('button', { name: /show on map/i }).first();
      if (await show.isEnabled().catch(() => false)) {
        await show.click();
        await page.waitForTimeout(3000);
        await captureState(page, run, 'C5-coordinate-resolved', { shots: 'all', note: 'Show on Map — resolution result' });
      }
    } else {
      run.steps.push({ name: 'C5-manual-coordinate', ok: false, note: `only ${n} numeric inputs found` });
    }
  });

  // ---- C6: global chrome --------------------------------------------------
  await stage(run, 'C6-account-menu', async () => {
    await dismissToasts(page);
    await page.getByRole('button', { name: /^Welcome,/ }).first().click();
    await page.waitForTimeout(1200);
    await captureState(page, run, 'C6-account-menu', { note: 'account menu open (not logging out)' });
    await page.keyboard.press('Escape').catch(() => {});
    await page.waitForTimeout(800);
  });

  await stage(run, 'C7-feedback-modal', async () => {
    await dismissToasts(page);
    await page.getByRole('button', { name: /^Share Feedback$/ }).first().click();
    await page.waitForTimeout(1800);
    await captureState(page, run, 'C7-feedback-modal', { shots: 'all', note: 'feedback modal — NOT submitted' });
    const close = page.getByRole('button', { name: /close|cancel|×/i }).first();
    if (await close.isVisible().catch(() => false)) await close.click();
    else await page.keyboard.press('Escape').catch(() => {});
    await page.waitForTimeout(1000);
  });

  await stage(run, 'C8-quick-guide', async () => {
    const cbs = page.locator('input[type="checkbox"]');
    found.checkboxCount = await cbs.count();
    // second checkbox in the spatial panel is "Show quick guide on login"
    const guide = page.locator('text=/Show quick guide on login/i').first();
    if (await guide.isVisible().catch(() => false)) {
      await guide.click();
      await page.waitForTimeout(2000);
      await captureState(page, run, 'C8-quick-guide-toggled', { note: 'quick-guide toggle clicked' });
      await guide.click().catch(() => {});
      await page.waitForTimeout(1200);
    }
  });

  await stage(run, 'C9-reset', async () => {
    await page.getByRole('button', { name: /Reset geography and filters/i }).first().click();
    await page.waitForTimeout(2000);
    await acceptConfirm(page, run, 'C9');
    await page.waitForTimeout(2000);
    await captureState(page, run, 'C9-after-reset', { shots: 'all', note: 'after global Reset' });
  });

  writeJson(run, '_found', found);
  const summary = writeApiLog(run);
  finalize(run);
  console.log('\n  Run:', run.dir);
  for (const s of summary) console.log(`    ${s.endpoint}  x${s.calls}  ${JSON.stringify(s.statuses)}`);
  console.log('  Failed stages:', run.steps.filter((s) => s.ok === false).map((s) => s.name).join(', ') || 'none');
}, { viewport: { width: 1600, height: 1000 } });
