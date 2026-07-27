// US 11 — Resilience Filters scenario (also resolves US 09 Q1).
// Applies the full filter cascade (Risk Domain → Metric → Scenario → Period →
// Statistic → Map Mode), verifies dependent enabling + map legend, and — with a
// district already selected — records whether "Add to Analysis" becomes enabled.
//
//   node qa/charters/us11-resilience-filters/scenario.mjs

import { withSession, APP_URL } from '../../harness/lib/session.mjs';
import { createRun, attachCollectors, snapshot, dumpDom, runAxe, finalize } from '../../harness/lib/evidence.mjs';
import { safe, shot } from '../../harness/lib/runner.mjs';
import { openAdmin } from '../../harness/lib/flows.mjs';

// The 6 filter triggers all start (or become) the placeholder "Select". Because
// we select strictly in DOM order, the first exact-"Select" trigger is always
// the next filter to fill.
async function pickNextFilter(page) {
  await page.getByText('Select', { exact: true }).first().click();
  await page.waitForTimeout(500);
  const chosen = await page.locator('li[role="option"]').first().innerText().catch(() => '');
  await page.locator('li[role="option"]').first().click();
  await page.waitForTimeout(700);
  return chosen.trim();
}

await withSession(async (page) => {
  const run = createRun('us11-filters');
  attachCollectors(page, run);
  await page.goto(APP_URL, { waitUntil: 'domcontentloaded' });
  await page.waitForTimeout(2500);

  const addBtn = () => page.getByRole('button', { name: /Add to Analysis/i });

  // S0 — geography setup (State + one District) for the Q1 check.
  await safe(run, 'S0: select Telangana + Adilabad', async () => {
    await openAdmin(page);
    await page.waitForTimeout(700);
    await page.getByRole('button', { name: /Select State/i }).click();
    await page.waitForTimeout(500);
    await page.locator('li[role="option"]', { hasText: /^Telangana$/ }).first().click();
    await page.waitForTimeout(1000);
    const dBtn = page.getByRole('button', { name: /Select District/i });
    await dBtn.click();
    await page.waitForTimeout(600);
    await page.locator('li[role="option"]').filter({ hasText: /^Adilabad$/ }).first().click();
    await page.waitForTimeout(400);
    await dBtn.click(); // close dropdown
    await page.waitForTimeout(400);
    return 'geography set';
  });

  // S1 — open filters; verify cascade placeholders / disabled downstream.
  await safe(run, 'S1: open filters + cascade gating', async () => {
    await page.getByText(/Select Resilience Filters/i).first().click();
    await page.waitForTimeout(900);
    await shot(page, run, 's1-filters-open');
    const body = await page.locator('body').innerText();
    for (const lbl of ['Risk Domain', 'Metric', 'Scenario', 'Period', 'Statistic', 'Map Mode']) {
      if (!body.includes(lbl)) throw new Error(`Missing filter group: ${lbl}`);
    }
    const gated = /Select a domain first/.test(body) && /Select a metric first/.test(body) && /Select a scenario first/.test(body);
    if (!gated) throw new Error('Cascade placeholders not shown — downstream filters may not be gated');
    return 'all 6 groups present; downstream filters gated';
  });

  // S2 — Risk Domain help tooltip (observational).
  await safe(run, 'S2: Risk Domain help tooltip', async () => {
    await page.getByRole('button', { name: /Risk Domain help/i }).hover();
    await page.waitForTimeout(600);
    await shot(page, run, 's2-help-tooltip');
    return 'hovered help icon (tooltip captured)';
  });

  // S3 — Risk Domain → Metric enables.
  await safe(run, 'S3: pick Risk Domain enables Metric', async () => {
    const rd = await pickNextFilter(page);
    const body = await page.locator('body').innerText();
    if (/Select a domain first/.test(body)) throw new Error('Metric still gated after Risk Domain selected');
    return `Risk Domain = "${rd}"; Metric enabled`;
  });

  // S4 — Metric → Scenario/Statistic enable.
  await safe(run, 'S4: pick Metric enables Scenario', async () => {
    const m = await pickNextFilter(page);
    const body = await page.locator('body').innerText();
    if (/Select a metric first/.test(body)) throw new Error('Scenario/Statistic still gated after Metric selected');
    return `Metric = "${m}"; downstream enabled`;
  });

  // S5 — Scenario + Period require MANUAL selection (Statistic→Mean and Map Mode
  // auto-default). Select in DOM order; Period un-gates after Scenario.
  await safe(run, 'S5: select Scenario + Period', async () => {
    const scenario = await pickNextFilter(page); // first exact "Select" = Scenario
    await page.waitForFunction(() => !document.body.innerText.includes('Select a scenario first'),
      { timeout: 10000 }).catch(() => {});
    const period = await pickNextFilter(page); // next exact "Select" = Period
    await page.waitForTimeout(600);
    await shot(page, run, 's5-filters-applied');
    if (!scenario) throw new Error('Scenario did not offer options');
    return `Scenario="${scenario}", Period="${period}"`;
  });

  // S6 — with all filters set, the map must render data. The reliable signal that
  // data loaded is that "Add to Analysis" becomes enabled (region fill + legend
  // follow). Poll for it; failure here would be a real defect.
  await safe(run, 'S6: map renders data (Add enables)', async () => {
    const enabled = await page.waitForFunction(() => {
      const b = [...document.querySelectorAll('button')].find((x) => /Add to Analysis/i.test(x.textContent));
      return b && !b.disabled;
    }, { timeout: 15000 }).then(() => true).catch(() => false);
    await page.waitForTimeout(800);
    await shot(page, run, 's6-map-legend');
    if (!enabled) throw new Error('Map data did not load: Add to Analysis never enabled after full filter selection');
    return 'map data loaded (Add to Analysis enabled)';
  });

  // S7 — Q1 resolution: Add to Analysis state with district + full filters.
  await safe(run, 'S7 (Q1): Add to Analysis with district + filters', async () => {
    const disabled = await addBtn().isDisabled().catch(() => null);
    await shot(page, run, 's7-add-state');
    return disabled === false
      ? 'ENABLED with district + full filters — US 09 Q1 resolved: a resilience filter IS required (intended)'
      : `STILL DISABLED (disabled=${disabled}) with district + full filters — US 09 Q1 leans DEFECT; verify`;
  });

  await dumpDom(page, run, 'us11-final');
  await snapshot(page, run, 'us11-responsive');
  await runAxe(page, run, 'us11');
  finalize(run);
  console.log(`\n  Run: ${run.dir}`);
  console.log(`  Steps failed: ${run.summary.stepsFailed} | real errors: ${run.summary.errorEvents} | benign: ${run.summary.benignEvents}`);
});
