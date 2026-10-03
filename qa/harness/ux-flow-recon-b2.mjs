// UX-flow recon, Phase B2: the portfolio + comparison surfaces.
//
// Split out of Phase B because those stages sat on Playwright's 30s default
// timeout. Everything here runs with a 9s default and desktop-only shots.
// Read-only: nothing is saved, uploaded or downloaded — the Download Reports
// tab is opened and photographed, never clicked.
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

/** Success/error toasts mount top-right, directly over the Resilience Profile /
 *  My Analysis panel header, and intercept every click there. Clear them. */
async function dismissToasts(page) {
  for (let i = 0; i < 6; i += 1) {
    const x = page.getByRole('button', { name: /Dismiss (success|error) message/i }).first();
    if (!(await x.isVisible().catch(() => false))) break;
    await x.click({ timeout: 3000 }).catch(() => {});
    await page.waitForTimeout(350);
  }
}

await withSession(async (page) => {
  const run = createRun('ux-flow-B2');
  attachCollectors(page, run);
  attachApiRecorder(page, run);
  page.setDefaultTimeout(9000);
  const found = {};

  await page.goto('https://dev.resilience.org.in/', { waitUntil: 'domcontentloaded' });
  await page.waitForLoadState('networkidle', { timeout: 25000 }).catch(() => {});
  await page.waitForTimeout(2500);

  await stage(run, 'P0-setup', async () => {
    for (const re of [/Expand administrative analysis/i, /Expand resilience filters panel/i]) {
      const b = page.getByRole('button', { name: re }).first();
      if (await b.isVisible().catch(() => false)) { await b.click(); await page.waitForTimeout(800); }
    }
    await pick(page, 'Select State', 'Telangana');
    await pick(page, 'Select District\\(s\\)', 'Warangal');
    await page.waitForTimeout(1000);
    await pick(page, 'Risk Domain', 'Heat Risk');
    await pick(page, 'Metric', 'Heat Risk Composite (score)');
    await pick(page, 'Scenario', 'Business as usual');
    await page.waitForTimeout(800);
    await pick(page, 'Period', 'Mid century');
    await pick(page, 'Statistic', 'Mean');
    await page.waitForTimeout(2500);
    found.addEnabledAfterFilters = await page.getByRole('button', { name: /^Add to Analysis$/ })
      .first().isEnabled().catch(() => null);
  });

  await stage(run, 'P1-add-first', async () => {
    await page.getByRole('button', { name: /^Add to Analysis$/ }).first().click();
    await page.waitForTimeout(2500);
    await captureState(page, run, 'P1-portfolio-one', { note: 'Warangal added to portfolio' });
  });

  await stage(run, 'P2-add-second', async () => {
    await pick(page, 'Select District\\(s\\)', 'Karimnagar');
    await page.waitForTimeout(1500);
    await page.getByRole('button', { name: /^Add to Analysis$/ }).first().click();
    await page.waitForTimeout(2500);
    await captureState(page, run, 'P2-portfolio-two', { note: 'Karimnagar added — 2 sites' });
  });

  await stage(run, 'P3-my-analysis', async () => {
    await dismissToasts(page);
    await page.getByRole('button', { name: /Expand resilience profile panel|Expand My Analysis panel/i })
      .first().click().catch(() => {});
    await page.waitForTimeout(1200);
    await page.getByRole('button', { name: /^My Analysis$/ }).first().click();
    await page.waitForTimeout(2000);
    await captureState(page, run, 'P3-my-analysis', { note: 'My Analysis panel with portfolio' });
  });

  await stage(run, 'P4-fullscreen', async () => {
    await dismissToasts(page);
    await page.getByRole('button', { name: /Open My Analysis in full screen/i }).first().click();
    await page.waitForTimeout(2500);
    await captureState(page, run, 'P4-my-analysis-fullscreen', { note: 'My Analysis full-screen modal' });
  });

  await stage(run, 'P5-compare', async () => {
    await dismissToasts(page);
    await page.getByRole('button', { name: /^Compare Portfolio$/ }).first().click();
    await page.waitForTimeout(2000);
    await captureState(page, run, 'P5-compare-open', { note: 'Compare Portfolio opened' });
    found.compareDomains = await enumerateOptions(page, trig(page, 'Select Risk Domain'), 'Compare domains');
  });

  await stage(run, 'P6-compare-metrics', async () => {
    await dismissToasts(page);
    const ms = page.getByRole('button', { name: /^Select Metrics$/ }).first();
    await ms.click();
    await page.waitForTimeout(1500);
    found.compareMetricPicker = await page.evaluate(() => {
      const boxes = [...document.querySelectorAll('div,ul')]
        .filter((e) => e.querySelectorAll('input[type=checkbox]').length > 2);
      const box = boxes[boxes.length - 1];
      return box ? box.innerText.split('\n').map((s) => s.trim()).filter(Boolean).slice(0, 40) : null;
    });
    await captureState(page, run, 'P6-compare-metric-picker', { note: 'metric multi-select open' });
    const cb = page.locator('input[type="checkbox"]');
    const n = await cb.count();
    for (let i = 0; i < Math.min(n, 3); i += 1) await cb.nth(i).click({ force: true }).catch(() => {});
    await page.waitForTimeout(3000);
    await ms.click().catch(() => {});
    await page.waitForTimeout(1500);
    await captureState(page, run, 'P6-compare-metrics-chosen', { note: 'metrics selected' });
  });

  for (const tab of ['Table', 'Visualizations', 'Download Reports']) {
    await stage(run, `P7-${tab}`, async () => {
      await dismissToasts(page);
      await page.getByRole('button', { name: new RegExp(`^${tab}$`) }).first().dispatchEvent('click');
      await page.waitForTimeout(3200);
      const r = await captureState(page, run, `P7-compare-${tab.replace(/\s+/g, '-').toLowerCase()}`,
        { note: `Compare tab: ${tab} (nothing downloaded)` });
      if (tab === 'Table') found.compareTable = r.outline.tables;
    });
  }

  writeJson(run, '_found', found);
  const summary = writeApiLog(run);
  finalize(run);
  console.log('\n  Run:', run.dir);
  for (const s of summary) console.log(`    ${s.endpoint}  x${s.calls}  ${JSON.stringify(s.statuses)}`);
  console.log('  Failed:', run.steps.filter((s) => s.ok === false).map((s) => `${s.name}(${s.note})`).join('; ') || 'none');
}, { viewport: { width: 1600, height: 1000 } });
