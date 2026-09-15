// US 06 — Header & Dropdown Navigation scenario (read-only; NEVER logs out).
//   node qa/charters/us06-header-nav/scenario.mjs
import { withSession, APP_URL } from '../../harness/lib/session.mjs';
import { createRun, attachCollectors, snapshot, dumpDom, runAxe, finalize, step } from '../../harness/lib/evidence.mjs';
import { join } from 'node:path';
const shot = (page, run, name) => page.screenshot({ path: join(run.dir, `${name}.png`) });
const bodyText = (page) => page.evaluate(() => document.body.innerText.replace(/\s+/g, ' ').trim());
async function safe(run, name, fn) {
  try { const n = await fn(); step(run, name, true, n || ''); console.log(`  ok   ${name}${n ? ' — ' + n : ''}`); }
  catch (e) { step(run, name, false, String(e && e.message || e)); console.log(`  FAIL ${name} — ${e && e.message || e}`); }
}

await withSession(async (page) => {
  const run = createRun('us06-header-nav');
  attachCollectors(page, run);
  await page.goto(APP_URL, { waitUntil: 'domcontentloaded' });
  await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => {});
  await page.waitForTimeout(1500);

  await safe(run, 'S1: header chrome (logo/title/welcome/feedback)', async () => {
    await shot(page, run, 's1-header');
    const t = await bodyText(page);
    const has = { title: /India Resilience Tool/i.test(t), welcome: /Welcome,\s*\w/i.test(t), feedback: /Share Feedback/i.test(t) };
    const missing = Object.entries(has).filter(([, v]) => !v).map(([k]) => k);
    return missing.length ? `PARTIAL: missing ${missing.join(',')}` : 'title + Welcome[Name] + Share Feedback present';
  });

  await safe(run, 'S2: Welcome dropdown options', async () => {
    await page.getByRole('button', { name: /Welcome/i }).first().click().catch(() => {});
    await page.waitForTimeout(800);
    await shot(page, run, 's2-dropdown');
    const opts = await page.evaluate(() => {
      const c = (t) => (t || '').trim();
      return [...new Set([...document.querySelectorAll('a,button,[role="menuitem"],span,div')].map((e) => c(e.innerText)).filter((t) => /^(User Profile|My Analysis|Logout)$/i.test(t)))];
    });
    const need = ['User Profile', 'My Analysis', 'Logout'];
    const missing = need.filter((n) => !opts.some((o) => new RegExp(`^${n}$`, 'i').test(o)));
    return missing.length ? `PARTIAL dropdown: found ${JSON.stringify(opts)}; missing ${missing.join(',')}` : `dropdown: User Profile + My Analysis + Logout present (Logout NOT clicked — session-safe)`;
  });

  await safe(run, 'S3: User Profile → /profile', async () => {
    await page.getByText(/^User Profile$/i).first().click().catch(() => {});
    await page.waitForTimeout(2000);
    await shot(page, run, 's3-profile-route');
    const ok = /\/profile/i.test(page.url());
    if (!ok) throw new Error(`did not route to /profile (url=${page.url()})`);
    return `routed to ${page.url()}`;
  });

  await safe(run, 'S4: My Analysis → /my-analysis', async () => {
    await page.goto(APP_URL, { waitUntil: 'domcontentloaded' });
    await page.waitForTimeout(1200);
    await page.getByRole('button', { name: /Welcome/i }).first().click().catch(() => {});
    await page.waitForTimeout(600);
    await page.getByText(/^My Analysis$/i).first().click().catch(() => {});
    await page.waitForTimeout(1800);
    await shot(page, run, 's4-myanalysis-route');
    const ok = /\/my-analysis/i.test(page.url());
    return ok ? `routed to ${page.url()}` : `OBSERVE: url=${page.url()}`;
  });

  await dumpDom(page, run, 'us06-final');
  await snapshot(page, run, 'us06-responsive');
  await runAxe(page, run, 'us06');
  finalize(run);
  console.log(`\n  Run: ${run.dir}`);
  console.log(`  Steps failed: ${run.summary.stepsFailed} | real error events: ${run.summary.errorEvents}`);
});
