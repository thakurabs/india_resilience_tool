// UX-flow recon, Phase B: the analysis-RESULT surface, once a geography and a
// full filter cascade are committed. Documents what the user actually sees:
// map view, ranking table, resilience profile tabs, portfolio + compare,
// coordinate entry, and the header/global chrome.
// Read-only: no saves, no uploads, no downloads, no feedback submits.
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

/** Dismiss the auto "HELP US IMPROVE" survey / any non-action backdrop. */
async function installModalSweeper(page) {
  await page.addInitScript(() => {
    const KEEP = /(confirm|cancel|clear|portfolio|switch|proceed|delete|rename|save)/i;
    const sweep = () => {
      document.querySelectorAll('[data-modal-root]').forEach((m) => {
        const t = (m.innerText || '');
        if (!KEEP.test(t) && /feedback|improve|survey|rate|experience/i.test(t)) m.remove();
      });
    };
    setInterval(sweep, 800);
    new MutationObserver(sweep).observe(document.documentElement, { childList: true, subtree: true });
  });
}

await withSession(async (page) => {
  const run = createRun('ux-flow-B');
  attachCollectors(page, run);
  attachApiRecorder(page, run);
  await installModalSweeper(page);
  const found = {};

  await page.goto('https://dev.resilience.org.in/', { waitUntil: 'domcontentloaded' });
  await page.waitForLoadState('networkidle', { timeout: 25000 }).catch(() => {});
  await page.waitForTimeout(2500);

  // ---- B0: set up a committed analysis -----------------------------------
  await stage(run, 'B0-setup', async () => {
    for (const re of [/Expand administrative analysis/i, /Expand resilience filters panel/i]) {
      const b = page.getByRole('button', { name: re }).first();
      if (await b.isVisible().catch(() => false)) { await b.click(); await page.waitForTimeout(800); }
    }
    await pick(page, 'Select State', 'Telangana');
    await pick(page, 'Select District\\(s\\)', 'Warangal');
    await page.waitForTimeout(1200);
    await pick(page, 'Risk Domain', 'Heat Risk');
    await pick(page, 'Metric', 'Heat Risk Composite (score)');
    await pick(page, 'Scenario', 'Business as usual');
    await page.waitForTimeout(900);
    await pick(page, 'Period', 'Mid century');
    await pick(page, 'Statistic', 'Mean');
    // Map Mode may or may not offer options; record what it holds.
    found.mapModes = await enumerateOptions(page, trig(page, 'Map Mode'), 'Map Mode');
    if (found.mapModes.length) await pick(page, 'Map Mode', found.mapModes[0]).catch(() => {});
    await page.waitForTimeout(3000);
    await captureState(page, run, 'B0-analysis-committed',
      { shots: 'all', note: 'Telangana/Warangal · Heat Risk Composite · BAU · Mid century · Mean' });
  });

  // ---- B1: map view ------------------------------------------------------
  await stage(run, 'B1-map-hover', async () => {
    const box = await page.locator('canvas').first().boundingBox();
    if (box) {
      await page.mouse.move(box.x + box.width * 0.5, box.y + box.height * 0.5);
      await page.waitForTimeout(1500);
      await captureState(page, run, 'B1-map-hover', { note: 'cursor at map centre — hover surface' });
      await page.mouse.click(box.x + box.width * 0.5, box.y + box.height * 0.5);
      await page.waitForTimeout(1800);
      await captureState(page, run, 'B1-map-click', { note: 'map click — info/CTA surface' });
    }
  });

  // ---- B2: ranking table (the old B1 blocker lives here) -----------------
  await stage(run, 'B2-ranking', async () => {
    await page.locator('input[type="radio"][value="ranking"]').first().click();
    await page.waitForTimeout(4000);
    const r = await captureState(page, run, 'B2-ranking-table',
      { shots: 'all', note: 'view mode = Ranking Table' });
    found.rankingTables = r.outline.tables;
  });

  await stage(run, 'B3-back-to-map', async () => {
    await page.locator('input[type="radio"][value="map"]').first().click();
    await page.waitForTimeout(2500);
  });

  // ---- B4: resilience profile -------------------------------------------
  await stage(run, 'B4-resilience-profile', async () => {
    const p = page.getByRole('button', { name: /Expand resilience profile panel/i }).first();
    if (await p.isVisible().catch(() => false)) { await p.click(); await page.waitForTimeout(1500); }
    await page.getByRole('button', { name: /^Resilience Profile$/ }).first().click().catch(() => {});
    await page.waitForTimeout(3000);
    await captureState(page, run, 'B4-profile', { shots: 'all', note: 'Resilience Profile for selected district' });
    // enumerate any tabs inside the profile
    const tabs = await page.locator('[role="tab"]').allInnerTexts().catch(() => []);
    found.profileTabs = tabs.map((t) => t.trim()).filter(Boolean);
    for (const t of found.profileTabs) {
      try {
        await page.locator('[role="tab"]', { hasText: new RegExp(`^${t}$`) }).first().click();
        await page.waitForTimeout(2200);
        await captureState(page, run, `B4-profile-tab-${t.replace(/[^a-z0-9]+/gi, '-').toLowerCase()}`,
          { note: `profile tab: ${t}` });
      } catch { /* tab vanished */ }
    }
  });

  await stage(run, 'B5-profile-fullscreen', async () => {
    const fs = page.getByRole('button', { name: /full screen/i }).first();
    if (await fs.isVisible().catch(() => false)) {
      await fs.click();
      await page.waitForTimeout(2500);
      await captureState(page, run, 'B5-profile-fullscreen', { shots: 'all', note: 'profile full-screen modal' });
      await page.keyboard.press('Escape').catch(() => {});
      await page.waitForTimeout(1200);
    }
  });

  // ---- B6: portfolio -----------------------------------------------------
  await stage(run, 'B6-add-to-analysis', async () => {
    const add = page.getByRole('button', { name: /^Add to Analysis$/ }).first();
    await add.scrollIntoViewIfNeeded().catch(() => {});
    if (await add.isEnabled().catch(() => false)) {
      await add.click();
      await page.waitForTimeout(2500);
      await captureState(page, run, 'B6-portfolio-one', { note: 'one site added to portfolio' });
      // add a second district so Compare has something to compare
      await pick(page, 'Select District\\(s\\)', 'Karimnagar').catch(() => {});
      await page.waitForTimeout(1500);
      const add2 = page.getByRole('button', { name: /^Add to Analysis$/ }).first();
      if (await add2.isEnabled().catch(() => false)) { await add2.click(); await page.waitForTimeout(2500); }
      await captureState(page, run, 'B6-portfolio-two', { note: 'second site added' });
    } else {
      run.steps.push({ name: 'B6-add-disabled', ok: false, note: 'Add to Analysis stayed disabled' });
    }
  });

  await stage(run, 'B7-my-analysis', async () => {
    await page.getByRole('button', { name: /^My Analysis$/ }).first().click().catch(() => {});
    await page.waitForTimeout(2000);
    await captureState(page, run, 'B7-my-analysis', { shots: 'all', note: 'My Analysis panel' });
    const fs = page.getByRole('button', { name: /Open My Analysis in full screen/i }).first();
    if (await fs.isVisible().catch(() => false)) {
      await fs.click();
      await page.waitForTimeout(2500);
      await captureState(page, run, 'B7-my-analysis-fullscreen', { shots: 'all', note: 'My Analysis full-screen modal' });
    }
  });

  // ---- B8: compare portfolio --------------------------------------------
  await stage(run, 'B8-compare', async () => {
    await page.getByRole('button', { name: /^Compare Portfolio$/ }).first().click().catch(() => {});
    await page.waitForTimeout(2000);
    await captureState(page, run, 'B8-compare-open', { note: 'Compare Portfolio opened' });
    found.compareDomains = await enumerateOptions(page, trig(page, 'Select Risk Domain'), 'Compare domains');
    // metric multi-select
    const ms = page.getByRole('button', { name: /^Select Metrics$/ }).first();
    if (await ms.isVisible().catch(() => false)) {
      await ms.click();
      await page.waitForTimeout(1200);
      found.compareMetrics = await page.locator('li[role="option"], label').allInnerTexts()
        .then((a) => a.map((t) => t.trim()).filter(Boolean).slice(0, 40)).catch(() => []);
      await captureState(page, run, 'B8-compare-metric-picker', { note: 'Compare metric multi-select open' });
      const first = page.locator('input[type="checkbox"]').first();
      await first.click().catch(() => {});
      await page.waitForTimeout(2500);
      await ms.click().catch(() => {});
      await page.waitForTimeout(1200);
    }
    for (const tab of ['Table', 'Visualizations', 'Download Reports']) {
      try {
        await page.getByRole('button', { name: new RegExp(`^${tab}$`) }).first()
          .dispatchEvent('click');
        await page.waitForTimeout(2800);
        await captureState(page, run, `B8-compare-${tab.replace(/\s+/g, '-').toLowerCase()}`,
          { shots: 'all', note: `Compare Portfolio tab: ${tab} (no download triggered)` });
      } catch (e) { console.log(`    ! compare tab ${tab}: ${String(e.message).slice(0, 80)}`); }
    }
    await page.keyboard.press('Escape').catch(() => {});
    await page.waitForTimeout(1200);
  });

  writeJson(run, '_found', found);
  const summary = writeApiLog(run);
  finalize(run);
  console.log('\n  Run:', run.dir);
  for (const s of summary) console.log(`    ${s.endpoint}  x${s.calls}  ${JSON.stringify(s.statuses)}`);
  console.log('  Failed stages:', run.steps.filter((s) => s.ok === false).map((s) => s.name).join(', ') || 'none');
}, { viewport: { width: 1600, height: 1000 } });
