// US 12 — View Mode (Radio Buttons) scenario.
// Verifies the "Select your views" Map View / Ranking Table toggle:
//   default Map View → switch to Ranking Table (map hidden; B1-500 recorded)
//   → back to Map View → geography + filters preserved across the switch.
// Records a per-step outcome + evidence. SPEC-DRIFT-not-FAIL.
//
//   node qa/charters/us12-view-mode/scenario.mjs

import { withSession, APP_URL } from '../../harness/lib/session.mjs';
import {
  createRun, attachCollectors, snapshot, dumpDom, runAxe, finalize, step,
} from '../../harness/lib/evidence.mjs';
import { openAdmin, selectState, selectDistrict, applyCoreFilters } from '../../harness/lib/flows.mjs';
import { join } from 'node:path';

const shot = (page, run, name) => page.screenshot({ path: join(run.dir, `${name}.png`) });
const bodyText = (page) => page.evaluate(() => document.body.innerText.replace(/\s+/g, ' ').trim());

async function safe(run, name, fn) {
  try { const note = await fn(); step(run, name, true, note || ''); console.log(`  ok   ${name}${note ? ' — ' + note : ''}`); }
  catch (e) { step(run, name, false, String(e && e.message || e)); console.log(`  FAIL ${name} — ${e && e.message || e}`); }
}

// Click a view radio by its label text ("Map View" / "Ranking Table").
async function selectView(page, label) {
  return page.evaluate((lbl) => {
    const c = (t) => (t || '').trim().replace(/\s+/g, ' ');
    const el = [...document.querySelectorAll('label,button,[role="radio"],span,div')]
      .find((e) => new RegExp(`^${lbl}$`, 'i').test(c(e.innerText)) && e.closest && e.closest('*'));
    if (!el) return false;
    const clickable = el.closest('label') || el;
    const input = clickable.querySelector && clickable.querySelector('input');
    (input || clickable).click();
    return true;
  }, label);
}
// Which view radio is currently checked?
async function activeView(page) {
  return page.evaluate(() => {
    const c = (t) => (t || '').trim().replace(/\s+/g, ' ');
    const labels = [...document.querySelectorAll('label')].filter((l) => /^(Map View|Ranking Table)$/i.test(c(l.innerText)));
    for (const l of labels) { const i = l.querySelector('input'); if (i && i.checked) return c(l.innerText); }
    return null;
  });
}
const mapVisible = (page) => page.evaluate(() => {
  const cvs = [...document.querySelectorAll('canvas,.maplibregl-canvas')].some((el) => { const r = el.getBoundingClientRect(); return r.width > 200 && r.height > 200; });
  return cvs;
});
const tableVisible = (page) => page.evaluate(() => /rank|ranking table|couldn.?t load the ranking|no data available/i.test(document.body.innerText) && document.querySelectorAll('table,[role="table"],[role="row"]').length > 0);

await withSession(async (page) => {
  const run = createRun('us12-view-mode');
  attachCollectors(page, run);
  const api = [];
  page.on('response', (res) => { const u = res.url(); if (/ranking|parquet/i.test(u) && !/\.(js|css)/.test(u)) api.push({ method: res.request().method(), status: res.status(), url: u.replace(APP_URL, '') }); });

  await page.goto(APP_URL, { waitUntil: 'domcontentloaded' });
  await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => {});
  await page.waitForTimeout(1500);

  // S1 — Build geography + filters; Map View default.
  let filters = {};
  await safe(run, 'S1: build analysis; Map View default', async () => {
    await openAdmin(page);
    await selectState(page, 'Telangana');
    await selectDistrict(page, 'Warangal');
    filters = await applyCoreFilters(page);
    await page.waitForTimeout(1200);
    await shot(page, run, 's1-map-default');
    const active = await activeView(page);
    const map = await mapVisible(page);
    return `default active view=${active}; mapVisible=${map}; filters=${JSON.stringify(filters)}`;
  });

  // S2 — "Select your views" section + both options present.
  await safe(run, 'S2: Select your views section', async () => {
    const info = await page.evaluate(() => {
      const c = (t) => (t || '').trim().replace(/\s+/g, ' ');
      const section = [...document.querySelectorAll('*')].some((e) => /^Select your views$/i.test(c(e.innerText)));
      const opts = [...document.querySelectorAll('label')].map((l) => c(l.innerText)).filter((t) => /^(Map View|Ranking Table)$/i.test(t));
      return { section, opts: [...new Set(opts)] };
    });
    if (!info.opts.length) return `OBSERVE: view options not found; section=${info.section}`;
    return `section=${info.section}; options=${JSON.stringify(info.opts)}`;
  });

  // S3 — Switch to Ranking Table; map hidden; record B1-family 500 if present.
  await safe(run, 'S3: switch to Ranking Table (map hidden)', async () => {
    await selectView(page, 'Ranking Table');
    await page.waitForTimeout(3000);
    await shot(page, run, 's3-ranking');
    const active = await activeView(page);
    const map = await mapVisible(page);
    const txt = await bodyText(page);
    const rankErr = api.filter((h) => /ranking/i.test(h.url) && h.status >= 500);
    const loadFail = /couldn.?t load the ranking|could not load.*ranking|error loading ranking/i.test(txt);
    const noData = /no data available for the selected filters/i.test(txt);
    const mutual = map === false; // map should hide when Ranking active
    return `active=${active}; mapHidden=${mutual}; ranking 5xx=${JSON.stringify(rankErr)}; loadFailMsg=${loadFail}; noDataMsg=${noData}`;
  });

  // S4 — Switch back to Map View; table hidden.
  await safe(run, 'S4: switch back to Map View', async () => {
    await selectView(page, 'Map View');
    await page.waitForTimeout(2500);
    await shot(page, run, 's4-map-again');
    const active = await activeView(page);
    const map = await mapVisible(page);
    return `active=${active}; mapVisible=${map}`;
  });

  // S5 — Geography + filters preserved across the switch.
  await safe(run, 'S5: geography + filters preserved', async () => {
    const txt = await bodyText(page);
    const kept = {
      state: /Telangana/i.test(txt),
      district: /Warangal/i.test(txt),
      metric: filters.metric ? new RegExp(filters.metric.split(' ')[0], 'i').test(txt) : /Heat Risk/i.test(txt),
    };
    const missing = Object.entries(kept).filter(([, v]) => !v).map(([k]) => k);
    return missing.length ? `PARTIAL: missing ${missing.join(',')}; kept=${JSON.stringify(kept)}` : `preserved: state+district+metric intact`;
  });

  // Cross-cutting.
  await dumpDom(page, run, 'us12-final');
  await snapshot(page, run, 'us12-responsive');
  await runAxe(page, run, 'us12');
  finalize(run);

  console.log(`\n  Run: ${run.dir}`);
  console.log(`  ranking API: ${JSON.stringify(api.filter((h) => /ranking/i.test(h.url)))}`);
  console.log(`  Steps failed: ${run.summary.stepsFailed} | real error events: ${run.summary.errorEvents} | benign: ${run.summary.benignEvents}`);
});
