// US 10 — Coordinates Panel scenario.
// Drives the Coordinate Panel: Add (manual entry) happy path + the two spec
// validation strings, Clear, and the Upload sub-flow (sample links + file
// uploads: valid / wrong-structure / unsupported). Records a per-step outcome
// and captures evidence. Steps whose "expected" is a visual/plotted state are
// captured as screenshots for the reviewer to judge.
//
//   node qa/charters/us10-coordinates-panel/scenario.mjs
//
// NOTE: the app labels this "Coordinate Panel" (spec says "Coordinates Panel") —
// a known spec-drift, not a bug.

import { withSession, APP_URL } from '../../harness/lib/session.mjs';
import {
  createRun, attachCollectors, snapshot, dumpDom, runAxe, finalize, step,
} from '../../harness/lib/evidence.mjs';
import { openAdmin } from '../../harness/lib/flows.mjs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const __dirname = dirname(fileURLToPath(import.meta.url));
const fixture = (f) => join(__dirname, 'fixtures', f);
const shot = (page, run, name) => page.screenshot({ path: join(run.dir, `${name}.png`) });

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

/** Read the left Selection Panel's visible text (for locating validation / detail copy). */
async function panelText(page) {
  return page.evaluate(() => {
    // The first element containing "Coordinate Panel" scopes the left column.
    const host = [...document.querySelectorAll('div,section,aside')].find((el) => /Coordinate Panel/i.test(el.textContent || ''));
    return (host || document.body).innerText.replace(/\s+/g, ' ').trim().slice(0, 1200);
  });
}

// Toasts on this app are class-less <div>s appended to <body> (no role, no aria —
// itself an a11y gap), so selector-based capture fails. Instead poll body text
// for a known set of toast / validation phrases and return the first that shows.
const TOAST_PHRASES = [
  /Location could not be resolved[^.]*\./i,
  /Latitude and Longitude are required/i,
  /Enter valid coordinates[^.]*\./i,
  /Invalid file format[^.]*sample/i,
  /Unsupported file format/i,
  /Unable to upload[^.]*\./i,
  /successfully/i,
];

/**
 * Poll body text for a known toast/validation phrase right after an action.
 * Returns the matched phrase, or '' if none appears within `ms`.
 */
async function grabToast(page, ms = 4000) {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    const body = await page.evaluate(() => document.body.innerText.replace(/\s+/g, ' '));
    for (const re of TOAST_PHRASES) {
      const m = body.match(re);
      if (m) return m[0].slice(0, 200);
    }
    await page.waitForTimeout(120);
  }
  return '';
}

/** Wait for any known toast phrase to clear (so the next step reads a fresh state). */
async function waitToastClear(page, ms = 6000) {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    const body = await page.evaluate(() => document.body.innerText.replace(/\s+/g, ' '));
    if (!TOAST_PHRASES.some((re) => re.test(body))) return;
    await page.waitForTimeout(200);
  }
}

await withSession(async (page) => {
  const run = createRun('us10-coordinates');
  attachCollectors(page, run);

  await page.goto(APP_URL, { waitUntil: 'domcontentloaded' });
  await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => {});
  await page.waitForTimeout(1500);

  const latInput = () => page.getByPlaceholder('17.8766');
  const lonInput = () => page.getByPlaceholder('79.2792');
  const nameInput = () => page.getByPlaceholder('Site 1');
  const showBtn = () => page.getByRole('button', { name: /Show on Map/i });
  const clearBtn = () => page.getByRole('button', { name: /^Clear$/i });
  const addBtn = () => page.getByRole('button', { name: /Add to Analysis/i });
  const saveBtn = () => page.getByRole('button', { name: /Save Analysis/i });

  // S1 — open Coordinate Panel; assert inputs present + action buttons disabled by default.
  await safe(run, 'S1: open panel, default disabled state', async () => {
    await page.getByRole('button', { name: /Coordinate Panel/i }).click();
    await page.waitForTimeout(1000);
    await shot(page, run, 's1-panel-open');
    if (!(await latInput().isVisible())) throw new Error('Latitude input not visible');
    if (!(await lonInput().isVisible())) throw new Error('Longitude input not visible');
    const addDis = await addBtn().isDisabled().catch(() => null);
    const saveDis = await saveBtn().isDisabled().catch(() => null);
    if (addDis === false) throw new Error('Add to Analysis should be disabled with no location + no filters');
    return `add disabled=${addDis}, save disabled=${saveDis}`;
  });

  // S2 — valid manual entry → Show on Map; expect Block/State detail + plotted point.
  // Also grabs any toast: a valid, in-coverage point should NOT raise an error toast.
  await safe(run, 'S2: valid coords → Show on Map', async () => {
    await latInput().fill('17.8766');
    await lonInput().fill('79.2792');
    await nameInput().fill('QA Site');
    await showBtn().click();
    const toast = await grabToast(page, 3000);
    await page.waitForTimeout(600);
    await shot(page, run, 's2-show-on-map');
    const txt = await panelText(page);
    const addDis = await addBtn().isDisabled().catch(() => null);
    // Expect a resolved-location line ("This location is <BLOCK>, <STATE>").
    const resolved = /This location is/i.test(txt);
    const detail = (txt.match(/This location is[^]*?(?=Show on Map|$)/i) || [''])[0].slice(0, 90);
    const contradiction = resolved && /could not be resolved/i.test(toast);
    return contradiction
      ? `CONTRADICTION: inline resolved ("${detail}") but error toast="${toast}"`
      : `resolved="${detail}"; toast="${toast || '(none)'}"; add disabled=${addDis}`;
  });

  // S3 — missing lat/long → spec expects "Latitude and Longitude are required".
  await safe(run, 'S3: missing coords validation', async () => {
    await waitToastClear(page); // don't read S2's lingering toast
    await clearBtn().click().catch(() => {});
    await page.waitForTimeout(500);
    await latInput().fill('');
    await lonInput().fill('');
    await showBtn().click();
    const toast = await grabToast(page, 3000);
    await shot(page, run, 's3-missing-validation');
    const match = /Latitude and Longitude are required/i.test(toast);
    return match
      ? 'exact spec string shown'
      : `SPEC-DRIFT: expected "Latitude and Longitude are required"; actual toast="${toast || '(none)'}"`;
  });

  // S4 — invalid coords → spec expects "Enter valid coordinates, Decimal Degrees Only".
  await safe(run, 'S4: invalid coords validation', async () => {
    await waitToastClear(page);
    await latInput().fill('999');
    await lonInput().fill('abc');
    await showBtn().click();
    const toast = await grabToast(page, 3000);
    await shot(page, run, 's4-invalid-validation');
    const match = /Enter valid coordinates, Decimal Degrees Only/i.test(toast);
    return match
      ? 'exact spec string shown'
      : `SPEC-DRIFT: expected "Enter valid coordinates, Decimal Degrees Only"; actual toast="${toast || '(none)'}"`;
  });

  // S5 — Clear resets inputs.
  await safe(run, 'S5: Clear resets inputs', async () => {
    await clearBtn().click();
    await page.waitForTimeout(600);
    await shot(page, run, 's5-clear');
    const lat = await latInput().inputValue().catch(() => '?');
    const lon = await lonInput().inputValue().catch(() => '?');
    if (lat !== '' || lon !== '') throw new Error(`inputs not cleared (lat="${lat}", lon="${lon}")`);
    return 'lat/long cleared';
  });

  // S6 — switch to Upload; assert 3 sample links + import controls present.
  await safe(run, 'S6: Upload sub-flow controls', async () => {
    await page.getByRole('button', { name: /^Upload Coordinates$/i }).click();
    await page.waitForTimeout(900);
    await shot(page, run, 's6-upload-mode');
    const csv = await page.getByText(/Comma separated file \(\.csv\)/i).count();
    const xlsx = await page.getByText(/Spreadsheet \(\.xlsx\)/i).count();
    const zip = await page.getByText(/Zipped shapefile \(\.zip\)/i).count();
    const uploadBtn = await page.getByRole('button', { name: /^Upload$/i }).count();
    if (!csv || !xlsx || !zip) throw new Error(`missing sample links (csv=${csv}, xlsx=${xlsx}, zip=${zip})`);
    return `sample links present (csv/xlsx/zip); Upload button count=${uploadBtn}`;
  });

  // S7–S10 — file uploads. Drive the (possibly hidden) file input directly, then
  // read both the inline panel message and any toast. `errText` returns the first
  // error-ish signal seen in either place.
  const fileInput = () => page.locator('input[type="file"]');
  const doUpload = async (path) => {
    if (await fileInput().count() === 0) throw new Error('no input[type=file] found on page');
    await fileInput().first().setInputFiles(path);
    await page.waitForTimeout(700);
    await page.getByRole('button', { name: /^Upload$/i }).first().click().catch(() => {});
    const toast = await grabToast(page, 2500);
    await page.waitForTimeout(500);
    const panel = await panelText(page);
    return { panel, toast, both: `${panel} || ${toast}` };
  };

  // S7a — documented-schema CSV (Latitude/Longitude/Label, per spec 480). This is
  // what a user following the v1.3 doc would build. Records whether it is accepted.
  await safe(run, 'S7a: documented-schema CSV (Latitude/Longitude/Label)', async () => {
    const { both } = await doUpload(fixture('good.csv'));
    await shot(page, run, 's7a-upload-documented-schema');
    const rejected = /Invalid file format|use the provided sample|Unsupported/i.test(both);
    return rejected
      ? `SPEC-DRIFT: doc columns Latitude/Longitude/Label REJECTED — "${(both.match(/Invalid file format[^]*?sample|Unsupported[^.]*\./i) || [''])[0]}"`
      : `accepted; signal="${both.slice(0, 160)}"`;
  });

  // S7b — app's OWN sample schema (id/custom_name/lat/long). The true valid path.
  await safe(run, 'S7b: app-sample-schema CSV (id/custom_name/lat/long)', async () => {
    const { both } = await doUpload(fixture('app_sample.csv'));
    await shot(page, run, 's7b-upload-app-schema');
    const accepted = /Uploaded|Point 1|Site A|added/i.test(both) && !/Invalid file format|Unsupported/i.test(both);
    return accepted
      ? 'accepted — uploaded coordinates listed'
      : `OBSERVE: app-schema CSV not clearly accepted; signal="${both.slice(0, 200)}"`;
  });

  // S8 — wrong-structure CSV (unknown columns) → expect a structure/invalid error.
  await safe(run, 'S8: wrong-structure CSV rejected', async () => {
    const { both } = await doUpload(fixture('bad_structure.csv'));
    await shot(page, run, 's8-upload-badstructure');
    const match = /Invalid file format|use the provided sample|Incorrect|structure/i.test(both);
    return match
      ? `error shown: "${(both.match(/Invalid file format[^]*?sample/i) || [''])[0]}"`
      : `OBSERVE: expected structure error; signal="${both.slice(0, 200)}"`;
  });

  // S9 — unsupported type (.txt) → spec expects "Unsupported file format".
  await safe(run, 'S9: unsupported file rejected', async () => {
    const { both } = await doUpload(fixture('bad_format.txt'));
    await shot(page, run, 's9-upload-badformat');
    const exact = /Unsupported file format/i.test(both);
    const generic = /Invalid file format|not supported/i.test(both);
    if (exact) return 'exact spec string "Unsupported file format" shown';
    if (generic) return `SPEC-DRIFT: expected "Unsupported file format"; actual="${(both.match(/Invalid file format[^]*?sample/i) || [''])[0]}"`;
    return `OBSERVE: no format error seen; signal="${both.slice(0, 200)}"`;
  });

  // S10 — mode-switch note: with coordinate activity, expand Administrative Panel.
  await safe(run, 'S10: Geography<->Coordinates switch note', async () => {
    await openAdmin(page);
    await page.waitForTimeout(1200);
    await shot(page, run, 's10-mode-switch');
    const txt = await page.evaluate(() => document.body.innerText.replace(/\s+/g, ' '));
    const hasNote = /switch|save or proceed|proceed without saving|location selection mode/i.test(txt);
    return hasNote
      ? 'a mode-switch note/dialog appeared'
      : 'OBSERVE: no switch note seen (may be gated on unsaved analysis) — verify with PO';
  });

  // Cross-cutting: DOM/NaN scan, multi-viewport responsive shots, axe.
  await dumpDom(page, run, 'us10-final');
  await snapshot(page, run, 'us10-responsive');
  await runAxe(page, run, 'us10');

  finalize(run);
  console.log(`\n  Run: ${run.dir}`);
  console.log(`  Steps failed: ${run.summary.stepsFailed} | real error events: ${run.summary.errorEvents} | benign: ${run.summary.benignEvents}`);
});
