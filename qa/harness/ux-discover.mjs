// Throwaway-ish selector discovery: open the two sidebar panels and dump what
// is actually there in the CURRENT build (July selectors are known stale).
import { withSession } from './lib/session.mjs';
import { createRun, attachCollectors, finalize } from './lib/evidence.mjs';
import { attachApiRecorder, captureState, stage, writeApiLog } from './lib/recon.mjs';

await withSession(async (page) => {
  const run = createRun('ux-discover');
  attachCollectors(page, run);
  attachApiRecorder(page, run);
  await page.goto('https://dev.resilience.org.in/', { waitUntil: 'domcontentloaded' });
  await page.waitForLoadState('networkidle', { timeout: 25000 }).catch(() => {});
  await page.waitForTimeout(2500);

  await stage(run, 'a1-landing', async () => {
    await captureState(page, run, 'a1-landing', { note: 'default state, nothing selected' });
  });

  await stage(run, 'a2-admin-expanded', async () => {
    const exp = page.getByRole('button', { name: /Expand administrative analysis/i }).first();
    if (await exp.isVisible().catch(() => false)) await exp.click();
    await page.waitForTimeout(1200);
    await captureState(page, run, 'a2-admin-expanded', { note: 'administrative analysis panel open' });
  });

  await stage(run, 'a3-filters-expanded', async () => {
    const f = page.getByRole('button', { name: /Expand resilience filters panel/i }).first();
    if (await f.isVisible().catch(() => false)) await f.click();
    await page.waitForTimeout(1500);
    await captureState(page, run, 'a3-filters-expanded', { note: 'resilience filters panel open' });
  });

  await stage(run, 'a4-profile-panel', async () => {
    const p = page.getByRole('button', { name: /Expand resilience profile panel/i }).first();
    if (await p.isVisible().catch(() => false)) await p.click();
    await page.waitForTimeout(1200);
    await captureState(page, run, 'a4-profile-panel', { note: 'resilience profile / my analysis panel open' });
  });

  const summary = writeApiLog(run);
  finalize(run);
  console.log('\n  Run:', run.dir);
  console.log('  API endpoints touched:');
  for (const s of summary) console.log(`    ${s.endpoint}  x${s.calls}  ${JSON.stringify(s.statuses)}`);
}, { viewport: { width: 1600, height: 1000 } });
