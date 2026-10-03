// US 15 — My Analysis (Save List & Reload) scenario.
// Drives the full save→list→reload loop:
//   build analysis → Save modal → unique-name save (201) → duplicate guard (409)
//   → blank/default save → open Welcome>My Analysis (/my-analysis) → inspect list
//   → RELOAD a saved row (restore state+district+filters) → 3-dot options
//   → Search filter → auto-trigger-save probe.
// Records a per-step outcome + evidence. Follows us10's SPEC-DRIFT-not-FAIL rules.
//
//   node qa/charters/us15-my-analysis/scenario.mjs
//
// Provokes ONE intentional 409 (duplicate-name guard, step S5) — expected, not a bug.

import { withSession, APP_URL } from '../../harness/lib/session.mjs';
import {
  createRun, attachCollectors, snapshot, dumpDom, runAxe, finalize, step,
} from '../../harness/lib/evidence.mjs';
import {
  openAdmin, selectState, selectDistrict, applyCoreFilters,
} from '../../harness/lib/flows.mjs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const __dirname = dirname(fileURLToPath(import.meta.url));
const shot = (page, run, name) => page.screenshot({ path: join(run.dir, `${name}.png`) });
const bodyText = (page) => page.evaluate(() => document.body.innerText.replace(/\s+/g, ' ').trim());

// Unique per-run name so the first save is a clean 201 and the re-save is a 409.
const RUN_NAME = `QA US15 ${new Date().toISOString().slice(11, 19)}`;

async function safe(run, name, fn) {
  try {
    const note = await fn();
    step(run, name, true, note || '');
    console.log(`  ok   ${name}${note ? ' — ' + note : ''}`);
  } catch (e) {
    step(run, name, false, String(e && e.message || e));
    console.log(`  FAIL ${name} — ${e && e.message || e}`);
  }
}

const TOASTS = [
  /Analysis saved successfully\.?/i,
  /An analysis with this name already exists/i,
  /Analysis name is required/i,
  /could not be saved/i,
];
async function grabToast(page, ms = 4000) {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    const body = await page.evaluate(() => document.body.innerText.replace(/\s+/g, ' '));
    for (const re of TOASTS) { const m = body.match(re); if (m) return m[0].slice(0, 160); }
    await page.waitForTimeout(120);
  }
  return '';
}

await withSession(async (page) => {
  const run = createRun('us15-my-analysis');
  attachCollectors(page, run);

  // Record saved-analyses API traffic (status + method) so save/reload steps can
  // assert on the backend, not just the toast.
  const api = [];
  page.on('response', (res) => {
    const u = res.url();
    if (/saved-analyses|composite-map-data/i.test(u)) {
      api.push({ method: res.request().method(), status: res.status(), url: u.replace(APP_URL, '') });
    }
  });
  const lastApi = (re) => [...api].reverse().find((h) => re.test(h.url));
  // POST-only match: avoids racing against the page-load GET saved-analyses (200)
  // when asserting a just-fired save POST's status (201/409).
  const lastPost = (re) => [...api].reverse().find((h) => h.method === 'POST' && re.test(h.url));

  await page.goto(APP_URL, { waitUntil: 'domcontentloaded' });
  await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => {});
  await page.waitForTimeout(1500);

  const saveBtn = () => page.getByRole('button', { name: /Save Analysis/i }).first();
  const nameInput = () => page.getByPlaceholder('My Analysis');

  // S1 — Save disabled on a clean dashboard.
  await safe(run, 'S1: Save Analysis disabled when empty', async () => {
    await openAdmin(page);
    await shot(page, run, 's1-clean');
    const dis = await saveBtn().isDisabled().catch(() => null);
    if (dis === false) throw new Error('Save Analysis enabled with no location/filters');
    return `save disabled=${dis}`;
  });

  // S2 — Build a real analysis; Save becomes enabled.
  let filters = {};
  await safe(run, 'S2: build analysis → Save enabled', async () => {
    await selectState(page, 'Telangana');
    await selectDistrict(page, 'Warangal');
    filters = await applyCoreFilters(page);
    await page.waitForTimeout(1200);
    await shot(page, run, 's2-analysis-built');
    const dis = await saveBtn().isDisabled().catch(() => null);
    if (dis === true) throw new Error('Save still disabled after building analysis');
    return `filters=${JSON.stringify(filters)}; save disabled=${dis}`;
  });

  // S3 — Save modal structure.
  await safe(run, 'S3: Save modal fields', async () => {
    await saveBtn().click();
    await page.waitForTimeout(900);
    await shot(page, run, 's3-save-modal');
    const title = await page.getByText(/^Save Analysis$/i).first().isVisible().catch(() => false);
    const label = await page.getByText(/Analysis Name/i).first().isVisible().catch(() => false);
    const input = await nameInput().isVisible().catch(() => false);
    const cancel = await page.getByRole('button', { name: /^Cancel$/i }).isVisible().catch(() => false);
    if (!input) throw new Error('Analysis Name input not visible in modal');
    return `title=${title} label=${label} input=${input} cancel=${cancel}`;
  });

  // S4 — Unique custom-name save → expect 201 + success toast + decorated name.
  await safe(run, 'S4: custom-name save (201)', async () => {
    await nameInput().fill(RUN_NAME);
    await page.getByRole('button', { name: /^Save Analysis$/i }).last().click();
    const toast = await grabToast(page, 4000);
    await page.waitForTimeout(1200);
    await shot(page, run, 's4-saved');
    const post = lastPost(/saved-analyses/);
    const ok = /saved successfully/i.test(toast) && post && post.status === 201;
    return ok
      ? `POST ${post.status}; toast="${toast}"`
      : `OBSERVE: expected 201 + success toast; got status=${post && post.status}; toast="${toast || '(none)'}"`;
  });

  // S5 — Duplicate name → expect graceful 409 guard (INTENTIONAL error).
  await safe(run, 'S5: duplicate-name guard (409 expected)', async () => {
    await saveBtn().click();
    await page.waitForTimeout(800);
    await nameInput().fill(RUN_NAME);
    await page.getByRole('button', { name: /^Save Analysis$/i }).last().click();
    const toast = await grabToast(page, 4000);
    await page.waitForTimeout(800);
    await shot(page, run, 's5-duplicate');
    const post = lastPost(/saved-analyses/);
    const guarded = /already exists/i.test(toast) || (post && post.status === 409);
    // Close the modal if it stayed open.
    await page.getByRole('button', { name: /^Cancel$/i }).click().catch(() => {});
    return guarded
      ? `guarded: POST ${post && post.status}; toast="${toast}"`
      : `OBSERVE: expected duplicate guard; status=${post && post.status}; toast="${toast || '(none)'}"`;
  });

  // S6 — Blank name → default label "My Analysis" (or 409 if default already used).
  await safe(run, 'S6: blank name → default label', async () => {
    await saveBtn().click();
    await page.waitForTimeout(800);
    await nameInput().fill('');
    await page.getByRole('button', { name: /^Save Analysis$/i }).last().click();
    const toast = await grabToast(page, 4000);
    await page.waitForTimeout(800);
    await shot(page, run, 's6-default-save');
    const post = lastPost(/saved-analyses/);
    await page.getByRole('button', { name: /^Cancel$/i }).click().catch(() => {});
    if (post && post.status === 201) return `default-label save accepted (201)`;
    if (post && post.status === 409) return `default label already exists (409) — blank⇒default confirmed`;
    return `OBSERVE: blank-name save status=${post && post.status}; toast="${toast || '(none)'}"`;
  });

  // S7 — Reach the listing via Welcome > My Analysis.
  await safe(run, 'S7: Welcome > My Analysis → /my-analysis', async () => {
    await page.getByRole('button', { name: /Welcome/i }).first().click();
    await page.waitForTimeout(700);
    await shot(page, run, 's7-welcome-dropdown');
    const hasProfile = await page.getByText(/^User Profile$/i).first().isVisible().catch(() => false);
    const hasLogout = await page.getByText(/^Logout$/i).first().isVisible().catch(() => false);
    await page.getByText(/^My Analysis$/i).first().click();
    await page.waitForTimeout(1800);
    await shot(page, run, 's7-my-analysis-route');
    const onRoute = /\/my-analysis/i.test(page.url());
    const heading = /My Analysis/i.test(await bodyText(page));
    const search = await page.getByPlaceholder(/Search Analysis/i).isVisible().catch(() => false);
    if (!onRoute) throw new Error(`did not route to /my-analysis (url=${page.url()})`);
    return `route ok; profile=${hasProfile} logout=${hasLogout} heading=${heading} search=${search}`;
  });

  // S8 — Inspect saved-item list; find the just-saved item; check tag/date.
  await safe(run, 'S8: saved-item list structure', async () => {
    const cards = await page.evaluate(() =>
      [...new Set([...document.querySelectorAll('div,li,article')]
        .map((el) => (el.innerText || '').replace(/\s+/g, ' ').trim())
        .filter((t) => /20\d\d/.test(t) && t.length < 120 && /-\s*\d{2}\s*\w{3},?\s*20\d\d/.test(t)))].slice(0, 8));
    const mine = cards.find((c) => c.includes(RUN_NAME));
    const getList = lastApi(/saved-analyses\?/);
    if (!mine) return `OBSERVE: just-saved "${RUN_NAME}" not visible in first page; cards=${JSON.stringify(cards.slice(0, 5))}`;
    return `found "${mine}"; GET ${getList && getList.status}; cards=${cards.length}`;
  });

  // S9 — RELOAD: click the just-saved row (label text, not the 3-dot) → restore.
  await safe(run, 'S9: reload restores state+district+filters', async () => {
    const row = page.getByText(new RegExp(RUN_NAME.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')), { exact: false }).first();
    if (!(await row.count())) throw new Error(`saved row "${RUN_NAME}" not found to reload`);
    await row.scrollIntoViewIfNeeded().catch(() => {});
    await row.click();
    await page.waitForTimeout(3500);
    await shot(page, run, 's9-reloaded');
    const txt = await bodyText(page);
    const restored = {
      backToDash: /\/$|dashboard/i.test(page.url()) || !/my-analysis/i.test(page.url()),
      state: /Telangana/i.test(txt),
      district: /Warangal/i.test(txt),
      heat: /Heat Risk|Heat Risk Composite/i.test(txt),
      scenario: /SSP2-4\.5|ssp245|Middle-of-the-road/i.test(txt),
      period: /2020-2040|Early century/i.test(txt),
    };
    const composite = lastApi(/composite-map-data/);
    const missing = Object.entries(restored).filter(([, v]) => !v).map(([k]) => k);
    const err500 = composite && composite.status >= 500;
    if (err500) return `RELOAD ERROR: ${composite.method} composite-map-data → ${composite.status}; restored=${JSON.stringify(restored)}`;
    return missing.length
      ? `PARTIAL restore — missing: ${missing.join(',')}; restored=${JSON.stringify(restored)}`
      : `full restore: state+district+filters; composite=${composite && composite.status}`;
  });

  // S10 — 3-dot row menu options (rename/delete). Go back to the list.
  await safe(run, 'S10: row 3-dot options (rename/delete)', async () => {
    await page.goto(new URL('/my-analysis', APP_URL).href, { waitUntil: 'domcontentloaded' });
    await page.waitForTimeout(1500);
    // The ⋮ is a per-row button: aria-label "Actions for <name>", aria-haspopup=menu.
    const dot = page.locator('button[aria-haspopup="menu"], button[aria-label^="Actions for"]').first();
    if (!(await dot.count())) return 'OBSERVE: no 3-dot (Actions) button found on rows';
    await dot.click();
    await page.waitForTimeout(700);
    await shot(page, run, 's10-row-menu');
    const opts = await page.evaluate(() =>
      [...new Set([...document.querySelectorAll('[role="menuitem"],button,li,a')]
        .map((el) => (el.innerText || '').trim())
        .filter((t) => /rename|delete|edit|remove/i.test(t) && t.length < 40))]);
    await page.keyboard.press('Escape').catch(() => {});
    return opts.length ? `options=${JSON.stringify(opts)}` : 'OBSERVE: menu opened but no rename/delete text seen';
  });

  // S11 — Search filter narrows the list.
  await safe(run, 'S11: Search Analysis filters list', async () => {
    const search = page.getByPlaceholder(/Search Analysis/i);
    if (!(await search.count())) return 'OBSERVE: no Search Analysis input';
    await search.fill(RUN_NAME.slice(0, 10));
    await page.waitForTimeout(900);
    await shot(page, run, 's11-search');
    const txt = await bodyText(page);
    const hasMine = txt.includes(RUN_NAME.slice(0, 10));
    return hasMine ? 'search matched the saved item' : 'OBSERVE: search did not surface the saved item';
  });

  // S12 — Auto-trigger save: unsaved analysis + change State → Save/Don't Save popup.
  await safe(run, 'S12: auto-trigger save popup on context change', async () => {
    await page.goto(APP_URL, { waitUntil: 'domcontentloaded' });
    await page.waitForTimeout(1500);
    await openAdmin(page);
    await selectState(page, 'Telangana');
    await selectDistrict(page, 'Warangal');
    await applyCoreFilters(page);
    await page.waitForTimeout(1000);
    // Now change State WITHOUT saving → expect an auto-save prompt.
    await page.getByRole('button', { name: /Telangana/i }).first().click().catch(() => {});
    await page.waitForTimeout(500);
    await page.locator('li[role="option"]').filter({ hasText: /Karnataka|Maharashtra|Andhra/i }).first().click().catch(() => {});
    await page.waitForTimeout(1200);
    await shot(page, run, 's12-context-change');
    const txt = await bodyText(page);
    const prompt = /Save.*Don.?t Save|Don.?t Save|save.*before|unsaved|save your analysis/i.test(txt);
    return prompt
      ? 'auto-trigger Save/Don\'t Save prompt appeared'
      : 'OBSERVE: no auto-save prompt on State change (may be gated) — verify with PO';
  });

  // Cross-cutting.
  await dumpDom(page, run, 'us15-final');
  await snapshot(page, run, 'us15-responsive');
  await runAxe(page, run, 'us15');
  finalize(run);

  console.log(`\n  Run: ${run.dir}`);
  console.log(`  saved-analyses/composite API hits: ${JSON.stringify(api)}`);
  console.log(`  Steps failed: ${run.summary.stepsFailed} | real error events: ${run.summary.errorEvents} | benign: ${run.summary.benignEvents}`);
});
