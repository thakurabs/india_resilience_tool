// Recon tool: load the saved session, visit a URL, and capture full evidence
// (multi-viewport screenshots, interactive-DOM map, axe, console/network errors)
// without asserting anything. Used to (a) verify the session still works and
// (b) discover selectors before writing per-charter scenarios.
//
// Usage:
//   node qa/harness/explore.mjs [path-or-url] [label]
// Examples:
//   node qa/harness/explore.mjs /                 dashboard-root
//   node qa/harness/explore.mjs /dashboard        dashboard

import { withSession, APP_URL } from './lib/session.mjs';
import { createRun, attachCollectors, snapshot, dumpDom, runAxe, finalize } from './lib/evidence.mjs';

const arg = process.argv[2] || '/';
const label = process.argv[3] || 'explore';
const target = arg.startsWith('http') ? arg : new URL(arg, APP_URL).href;

await withSession(async (page) => {
  const run = createRun(label);
  attachCollectors(page, run);

  await page.goto(target, { waitUntil: 'domcontentloaded' });
  // Let the SPA hydrate; networkidle is best-effort (maps/tiles may keep polling).
  await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => {});
  await page.waitForTimeout(1500);

  const bouncedToLogin = /login|signin|sign-in|auth/i.test(page.url());
  run.landedUrl = page.url();
  run.sessionValid = !bouncedToLogin;

  const dom = await dumpDom(page, run, label);
  await snapshot(page, run, label);
  await runAxe(page, run, label);
  finalize(run);

  console.log(`\n  Run: ${run.dir}`);
  console.log(`  Landed: ${run.landedUrl}`);
  console.log(`  Session valid (not bounced to login): ${run.sessionValid}`);
  console.log(`  Title: ${dom.title}`);
  console.log(`  Interactive elements: ${dom.interactiveCount}`);
  console.log(`  Suspicious (NaN/blank) values: ${dom.suspiciousValues.length}`);
  console.log(`  Error events: ${run.summary.errorEvents} (of ${run.summary.totalEvents} total)`);
  if (!run.sessionValid) {
    console.log('\n  ⚠ Bounced to login — the saved session likely expired.');
    console.log('    Re-run: node qa/harness/capture-session.mjs');
  }
}, { viewport: { width: 1440, height: 900 } });
