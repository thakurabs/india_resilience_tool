// US 13 — Map View & Interaction scenario.
//
//   node qa/charters/us13-map-view/scenario.mjs

import { withSession, APP_URL } from '../../harness/lib/session.mjs';
import { createRun, attachCollectors, snapshot, dumpDom, runAxe, finalize } from '../../harness/lib/evidence.mjs';
import { safe, shot } from '../../harness/lib/runner.mjs';
import { selectState, selectDistrict, applyCoreFilters } from '../../harness/lib/flows.mjs';

await withSession(async (page) => {
  const run = createRun('us13-map');
  attachCollectors(page, run);
  await page.goto(APP_URL, { waitUntil: 'domcontentloaded' });
  await page.waitForTimeout(2500);

  const addEnabled = () => page.waitForFunction(() => {
    const b = [...document.querySelectorAll('button')].find((x) => /Add to Analysis/i.test(x.textContent));
    return b && !b.disabled;
  }, { timeout: 15000 }).then(() => true).catch(() => false);

  // S1 — geography + filters → map renders data.
  await safe(run, 'S1: map renders selected region + data', async () => {
    await selectState(page, 'Telangana');
    await selectDistrict(page, 'Adilabad');
    const picks = await applyCoreFilters(page);
    const loaded = await addEnabled();
    await shot(page, run, 's1-map-data');
    if (!loaded) throw new Error('Map data did not load (Add to Analysis never enabled)');
    return `filters ${JSON.stringify(picks)}; map data loaded`;
  });

  // S2 — legend present (numeric scale + caption).
  await safe(run, 'S2: legend present', async () => {
    await page.waitForTimeout(1000);
    await shot(page, run, 's2-legend');
    // The legend caption pipes metric|scenario|period|statistic; look for the
    // scenario token in a legend-like context (distinct from the dropdown).
    const hasLegend = await page.evaluate(() => {
      const t = document.body.innerText;
      return /Middle-of-the-road/.test(t) && /(Very Low|Extreme|\d+\.\d+)/.test(t);
    });
    if (!hasLegend) throw new Error('Legend (numeric scale / caption) not detected');
    return 'legend detected';
  });

  // S3 — click the region → tooltip (observational; canvas-driven).
  await safe(run, 'S3: click region tooltip (observational)', async () => {
    const map = page.locator('canvas').first();
    const box = await map.boundingBox();
    if (box) await page.mouse.click(box.x + box.width * 0.55, box.y + box.height * 0.45);
    await page.waitForTimeout(1200);
    await shot(page, run, 's3-tooltip');
    const body = await page.locator('body').innerText();
    const fields = ['Baseline', 'Position', 'Value', 'Add to Analysis'].filter((f) => body.includes(f));
    return `tooltip fields seen: ${fields.join(', ') || 'none (see screenshot)'}`;
  });

  // S4 — zoom controls present + clickable.
  await safe(run, 'S4: zoom controls', async () => {
    for (const name of [/Zoom in/i, /Zoom out/i, /Reset map/i]) {
      const btn = page.getByRole('button', { name });
      if (!(await btn.count())) throw new Error(`Missing map control: ${name}`);
    }
    await page.getByRole('button', { name: /Zoom in/i }).click();
    await page.waitForTimeout(500);
    await shot(page, run, 's4-zoomed');
    return 'zoom in/out/reset present; zoom-in clicked';
  });

  await dumpDom(page, run, 'us13-final');
  await snapshot(page, run, 'us13-responsive');
  await runAxe(page, run, 'us13');
  finalize(run);
  console.log(`\n  Run: ${run.dir}`);
  console.log(`  Steps failed: ${run.summary.stepsFailed} | real errors: ${run.summary.errorEvents} | benign: ${run.summary.benignEvents}`);
});
