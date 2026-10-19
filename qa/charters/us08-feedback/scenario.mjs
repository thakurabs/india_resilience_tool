// US 08 — Feedback Form scenario (read-only; NEVER clicks Submit — it emails admin).
//   node qa/charters/us08-feedback/scenario.mjs
import { withSession, APP_URL } from '../../harness/lib/session.mjs';
import { createRun, attachCollectors, snapshot, dumpDom, runAxe, finalize, step } from '../../harness/lib/evidence.mjs';
import { join } from 'node:path';
const shot = (page, run, name) => page.screenshot({ path: join(run.dir, `${name}.png`) });
async function safe(run, name, fn) {
  try { const n = await fn(); step(run, name, true, n || ''); console.log(`  ok   ${name}${n ? ' — ' + n : ''}`); }
  catch (e) { step(run, name, false, String(e && e.message || e)); console.log(`  FAIL ${name} — ${e && e.message || e}`); }
}

await withSession(async (page) => {
  const run = createRun('us08-feedback');
  attachCollectors(page, run);
  await page.goto(APP_URL, { waitUntil: 'domcontentloaded' });
  await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => {});
  await page.waitForTimeout(1500);

  // If the timed auto-popup is already open, that itself is US 08 evidence — note it.
  const autoOpen = await page.evaluate(() => /HELP US IMPROVE YOUR EXPERIENCE/i.test(document.body.innerText));

  await safe(run, 'S1: Share Feedback opens popup', async () => {
    if (!autoOpen) {
      await page.getByText(/Share Feedback/i).first().click().catch(() => {});
      await page.waitForTimeout(1200);
    }
    await shot(page, run, 's1-feedback');
    const open = await page.evaluate(() => /HELP US IMPROVE YOUR EXPERIENCE|appreciate your quick feedback/i.test(document.body.innerText));
    if (!open) throw new Error('feedback popup did not open');
    return `popup open${autoOpen ? ' (auto-triggered on load)' : ' (via Share Feedback)'}`;
  });

  await safe(run, 'S2: popup structure', async () => {
    const s = await page.evaluate(() => {
      const c = (t) => (t || '').trim();
      const radios = [...document.querySelectorAll('input[type="radio"],[role="radio"]')].length;
      const body = document.body.innerText;
      const options = ['Easy to use', 'Helped me achieve my goal', 'Took too long', 'Confusing navigation', 'Missing features'].filter((o) => new RegExp(o, 'i').test(body));
      return {
        radios, options,
        tellUsMore: /Tell us more/i.test(body),
        rating: !!document.querySelector('svg') && /How was your experience/i.test(body),
        submit: [...document.querySelectorAll('button')].some((b) => /^Submit$/i.test(c(b.innerText))),
      };
    });
    return `radios=${s.radios} options=${JSON.stringify(s.options)}; tellUsMore=${s.tellUsMore}; starRating=${s.rating}; submitBtn=${s.submit} (NOT clicked)`;
  });

  await safe(run, 'S3: close without submitting', async () => {
    // Close via explicit × (never Submit).
    await page.evaluate(() => {
      const m = [...document.querySelectorAll('[data-modal-root],[role="dialog"]')].find((el) => /HELP US IMPROVE/i.test(el.innerText || ''));
      if (!m) return;
      const x = [...m.querySelectorAll('button,[role="button"],[aria-label]')].find((b) => {
        const lbl = (b.getAttribute('aria-label') || b.getAttribute('title') || '').trim();
        const txt = (b.innerText || '').trim();
        if (/submit/i.test(lbl + ' ' + txt)) return false;
        return /close/i.test(lbl) || txt === '×' || txt === '✕' || txt === '';
      });
      if (x) x.click();
    });
    await page.keyboard.press('Escape').catch(() => {});
    await page.waitForTimeout(800);
    await shot(page, run, 's3-closed');
    const stillOpen = await page.evaluate(() => /HELP US IMPROVE YOUR EXPERIENCE/i.test(document.body.innerText));
    return stillOpen ? 'OBSERVE: popup still open after ×/Escape' : 'popup dismissed without submitting';
  });

  await dumpDom(page, run, 'us08-final');
  await snapshot(page, run, 'us08-responsive');
  await runAxe(page, run, 'us08');
  finalize(run);
  console.log(`\n  Run: ${run.dir}`);
  console.log(`  Steps failed: ${run.summary.stepsFailed} | real error events: ${run.summary.errorEvents}`);
});
