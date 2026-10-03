// US 07 — User Profile Management scenario (read-only; NEVER edits/saves/resets).
//   node qa/charters/us07-profile/scenario.mjs
import { withSession, APP_URL } from '../../harness/lib/session.mjs';
import { createRun, attachCollectors, snapshot, dumpDom, runAxe, finalize, step } from '../../harness/lib/evidence.mjs';
import { join } from 'node:path';
const shot = (page, run, name) => page.screenshot({ path: join(run.dir, `${name}.png`) });
async function safe(run, name, fn) {
  try { const n = await fn(); step(run, name, true, n || ''); console.log(`  ok   ${name}${n ? ' — ' + n : ''}`); }
  catch (e) { step(run, name, false, String(e && e.message || e)); console.log(`  FAIL ${name} — ${e && e.message || e}`); }
}

await withSession(async (page) => {
  const run = createRun('us07-profile');
  attachCollectors(page, run);
  await page.goto(new URL('/profile', APP_URL).href, { waitUntil: 'domcontentloaded' });
  await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => {});
  await page.waitForTimeout(1800);

  await safe(run, 'S1: /profile loads', async () => {
    await shot(page, run, 's1-profile');
    const t = await page.evaluate(() => document.body.innerText);
    if (!/User Profile/i.test(t)) throw new Error('User Profile heading not found');
    return `loaded; url=${page.url()}`;
  });

  await safe(run, 'S2: view fields present', async () => {
    const t = await page.evaluate(() => document.body.innerText);
    const fields = {
      name: /\bName\b/i.test(t), email: /\bEmail\b/i.test(t), organization: /Organi[sz]ation/i.test(t),
      designation: /Designation/i.test(t), purpose: /Purpose of use/i.test(t), thematic: /Thematic activity/i.test(t),
      country: /Country/i.test(t) && /India/i.test(t), state: /\bState\b/i.test(t), resetPw: /Reset Password/i.test(t),
    };
    const missing = Object.entries(fields).filter(([, v]) => !v).map(([k]) => k);
    return missing.length ? `fields present except: ${missing.join(',')} [spec 280-289 lists these; note missing]` : 'all spec fields present';
  });

  await safe(run, 'S3: Email locked (not editable)', async () => {
    const info = await page.evaluate(() => {
      const c = (t) => (t || '').trim();
      // Find the input whose value looks like an email.
      const email = [...document.querySelectorAll('input')].find((i) => /@/.test(i.value || ''));
      if (!email) return { found: false };
      const s = getComputedStyle(email);
      return { found: true, disabled: email.disabled || email.readOnly, greyed: /(230|227|229)/.test(s.backgroundColor) || s.backgroundColor !== 'rgb(255, 255, 255)' };
    });
    if (!info.found) return 'OBSERVE: no email input located';
    return `email input disabled/readonly=${info.disabled}; greyed=${info.greyed}`;
  });

  await safe(run, 'S4: Update + Reset Password affordances (not triggered)', async () => {
    const info = await page.evaluate(() => {
      const c = (t) => (t || '').trim();
      const btns = [...document.querySelectorAll('button')].map((b) => c(b.innerText));
      return {
        update: btns.find((b) => /^(Save|Update)$/i.test(b)) || null,
        resetPw: /Reset Password/i.test(document.body.innerText),
        sendOtp: btns.some((b) => /Send OTP/i.test(b)),
      };
    });
    return `save/update button="${info.update}" [spec says "Save"]; resetPassword=${info.resetPw} (Send OTP=${info.sendOtp}); NOT triggered (session-safe)`;
  });

  await dumpDom(page, run, 'us07-final');
  await snapshot(page, run, 'us07-responsive');
  await runAxe(page, run, 'us07');
  finalize(run);
  console.log(`\n  Run: ${run.dir}`);
  console.log(`  Steps failed: ${run.summary.stepsFailed} | real error events: ${run.summary.errorEvents}`);
});
