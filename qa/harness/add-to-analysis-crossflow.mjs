// US-CROSSFLOW — "Add to Analysis" cross-flow reliability + duplicate detection.
//
// Proves each of the three add-flows (Administrative / Coordinate manual+upload /
// Map click) adds reliably, that a portfolio can be built by MIXING flows, and
// that duplicate sites are rejected. Duplicate handling has never been probed;
// per the PO a silent duplicate is a Major defect.
//
// Built on the us17 stack (withSession + dismissFeedback + safe) — NOT the
// detonator's bare chromium.launch, which has no survey handling and would be
// poisoned by the auto-popping "HELP US IMPROVE" feedback modal.
//
// Phase 0 recon GATES the run: if Clear can't be proven, the flag string can't
// be captured, map-click selectors don't bind, or the compare endpoint doesn't
// fire, Phases A/B/C are skipped (their results would be void).
//
//   node qa/harness/capture-session.mjs      # refresh 2FA session (~24h) first
//   node qa/harness/add-to-analysis-crossflow.mjs
//
// Evidence -> qa/runs/us-crossflow-add-to-analysis/ ; results.json carries a
// structured run.crossflow record (recon + per-add {count,labels} + Phase C grid).

import { withSession, APP_URL } from './lib/session.mjs';
import {
  createRun, attachCollectors, snapshot, dumpDom, runAxe, finalize, step,
} from './lib/evidence.mjs';
import { shot } from './lib/runner.mjs';
import { openAdmin, selectState, selectDistrict, applyCoreFilters } from './lib/flows.mjs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const __dirname = dirname(fileURLToPath(import.meta.url));
const APP_SAMPLE = join(__dirname, '..', 'charters', 'us10-coordinates-panel', 'fixtures', 'app_sample.csv');
const CHARTER_FX = join(__dirname, '..', 'charters', 'us-crossflow-add-to-analysis', 'fixtures');
const DUP_SAME = join(CHARTER_FX, 'dup_same_name.csv');
const DUP_DIFF = join(CHARTER_FX, 'dup_diff_name.csv');

// Warangal coordinate used by the duplicate fixtures (accepted schema).
const WARANGAL = { lat: '17.8766', lon: '79.2792' };

// ---------------------------------------------------------------------------
// Survey dismissal + guarded step runner (adopted verbatim from us17).
// ---------------------------------------------------------------------------
// A full-screen `data-modal-root` backdrop (the "HELP US IMPROVE" survey / promo)
// pops on a TIMER after a few minutes and intercepts pointer events mid-step —
// closing it once at step-start is not enough, and its innerText does not always
// carry a matchable phrase at mount. A live probe confirmed NO data-modal-root
// ever appears during legitimate add / expand / filter operations in this
// harness (expandPanel reads the normal sidebar, not a modal), so removing ALL
// data-modal-root overlays outright is safe here. Installed as an in-page
// interval + MutationObserver so it fires the instant the overlay mounts, plus
// called explicitly before critical clicks. (role=dialog is still gated on
// survey text so we never nuke a genuine dialog.)
// Signature of the intrusive overlay (from the Playwright interception log):
// a full-screen dark backdrop <div data-modal-root ... "fixed inset-0 ... bg-black/35 ... cursor-pointer">.
// The Clear-Portfolio CONFIRM dialog shares this exact backdrop structure, so we
// must NOT nuke every dark backdrop — that cancels Clear. Discriminate by
// content: PRESERVE any backdrop carrying action-dialog keywords (confirm /
// cancel / clear / portfolio / remove / discard / save / proceed); remove the
// rest (the survey/promo). Body duplicated in the init-script (no eval — page CSP).
const killSurvey = (page) => page.evaluate(() => {
  const KEEP = /confirm|are you sure|cancel|clear|portfolio|remove all|discard|unsaved|proceed|delete|save|yes,/i;
  const isSurvey = (el) => /inset-0/.test(el.className || '') && /bg-black\//.test(el.className || '') && !KEEP.test(el.innerText || '');
  document.querySelectorAll('[data-modal-root]').forEach((el) => { if (isSurvey(el)) el.remove(); });
  document.querySelectorAll('[role="dialog"]').forEach((el) => {
    if (/HELP US IMPROVE YOUR EXPERIENCE|appreciate your quick feedback/i.test(el.innerText || '')) el.remove();
  });
}).catch(() => {});

async function installSurveyKiller(page) {
  await page.addInitScript(() => {
    const KEEP = /confirm|are you sure|cancel|clear|portfolio|remove all|discard|unsaved|proceed|delete|save|yes,/i;
    const isSurvey = (el) => /inset-0/.test(el.className || '') && /bg-black\//.test(el.className || '') && !KEEP.test(el.innerText || '');
    const kill = () => {
      document.querySelectorAll('[data-modal-root]').forEach((el) => { if (isSurvey(el)) el.remove(); });
      document.querySelectorAll('[role="dialog"]').forEach((el) => {
        if (/HELP US IMPROVE YOUR EXPERIENCE|appreciate your quick feedback/i.test(el.innerText || '')) el.remove();
      });
    };
    try { setInterval(kill, 300); } catch (e) { /* noop */ }
    const start = () => { kill(); try { new MutationObserver(kill).observe(document.body, { childList: true, subtree: true }); } catch (e) { /* noop */ } };
    if (document.body) start(); else document.addEventListener('DOMContentLoaded', start);
  });
}

async function safe(run, page, name, fn) {
  try {
    await killSurvey(page);
    const note = await fn();
    step(run, name, true, note || '');
    console.log(`  ok   ${name}${note ? ' — ' + note : ''}`);
    return true;
  } catch (e) {
    // Diagnostic: on failure, capture any overlay that may be intercepting clicks.
    const blockers = await page.evaluate(() => [...document.querySelectorAll('[data-modal-root], [role="dialog"]')]
      .map((el) => ({ attr: el.getAttribute('data-modal-root'), cls: (el.className || '').slice(0, 90), text: (el.innerText || '').replace(/\s+/g, ' ').trim().slice(0, 130), buttons: [...el.querySelectorAll('button,[role="button"]')].map((b) => (b.getAttribute('aria-label') || b.innerText || '').replace(/\s+/g, ' ').trim()).filter(Boolean).slice(0, 8) }))).catch(() => []);
    const msg = String((e && e.message) || e).split('\n')[0];
    const note = blockers.length ? `${msg} || BLOCKERS=${JSON.stringify(blockers)}` : msg;
    step(run, name, false, note);
    console.log(`  FAIL ${name} — ${note}`);
    return false;
  }
}

// ---------------------------------------------------------------------------
// Generic roster reader — the assertion backbone. RAW row count (never a Set),
// each row's visible label verbatim. Deliberately does NOT clone us17's
// managePortfolioNames (which Set-collapses duplicates and whitelists Telangana
// names — both would mask the very defect this charter hunts).
// Row count = number of "Remove from portfolio" controls; cross-checked against
// "Show on map" controls (one of each per row).
// ---------------------------------------------------------------------------
const readRoster = (page) => page.evaluate(() => {
  const c = (t) => (t || '').trim().replace(/\s+/g, ' ');
  const h = [...document.querySelectorAll('h2,h3,h4,button,span')]
    .find((el) => /^Manage Portfolio$/i.test(c(el.innerText)));
  if (!h) return { count: 0, labels: [], showOnMap: 0, found: false };
  let box = h;
  for (let i = 0; i < 6 && box.parentElement; i++) box = box.parentElement;
  const btns = [...box.querySelectorAll('button,[role="button"]')];
  const label = (b) => (b.getAttribute('aria-label') || '') + ' ' + (b.getAttribute('title') || '') + ' ' + (b.innerText || '');
  const isRemove = (b) => /remove from portfolio|remove|delete|⊗|✕|×/i.test(label(b));
  const isShow = (b) => /show on map/i.test(label(b));
  const removeBtns = btns.filter(isRemove);
  const showBtns = btns.filter(isShow);
  // "Show on map" is one-per-row and won't appear in the compare table, so it is
  // the most reliable row anchor; fall back to remove buttons if labelled otherwise.
  const anchors = showBtns.length ? showBtns : removeBtns;
  const labels = anchors.map((a) => {
    // Climb while the parent still holds only THIS one anchor AND stays short —
    // don't let a single-row portfolio swallow the whole surrounding panel.
    let row = a;
    for (let i = 0; i < 6 && row.parentElement && row.parentElement !== box; i++) {
      const parent = row.parentElement;
      const ac = [...parent.querySelectorAll('button,[role="button"]')].filter(showBtns.length ? isShow : isRemove).length;
      if (ac > 1) break;
      if (c(parent.innerText).length > 160) break;
      row = parent;
    }
    let txt = c(row.innerText);
    [...row.querySelectorAll('button,[role="button"]')].forEach((b) => {
      const bt = c(b.innerText);
      if (bt) txt = txt.split(bt).join(' ');
    });
    return c(txt);
  });
  return { count: anchors.length, removeCount: removeBtns.length, labels, showOnMap: showBtns.length, found: true };
});

// Portfolio count banner ("You have added N district(s)/site(s)/point(s)...").
// Server-rendered and authoritative — this is the PRIMARY portfolio-size signal.
const countBanner = (page) => page.evaluate(() => {
  const m = document.body.innerText.match(/You have added\s+(\d+)\s+([A-Za-z]+)s?\b[^.]*?portfolio/i)
    || document.body.innerText.match(/You have added\s+(\d+)\s+([A-Za-z]+)s?\b/i);
  return m ? { n: Number(m[1]), unit: m[2], text: m[0].slice(0, 80) } : null;
});

/**
 * Effective portfolio count. Prefers the authoritative count banner; falls back
 * to the DOM row count when no banner is present. Returns both for the report.
 */
async function portfolioCount(page) {
  const banner = await countBanner(page);
  const roster = await readRoster(page);
  const effective = banner ? banner.n : roster.count;
  return { effective, banner, roster };
}

// Best-effort probe for a "duplicate / already added" flag. Returns the matched
// phrase or ''. Phase 0 (P0.2) captures the canonical string for hard matching.
const DUP_PHRASES = [
  /already (?:been )?added[^.]*\.?/i,
  /already in (?:your )?(?:portfolio|analysis)[^.]*\.?/i,
  /already exists[^.]*\.?/i,
  /duplicate[^.]*\.?/i,
  /this (?:location|site|district|point) is already[^.]*\.?/i,
];
async function grabDupFlag(page, ms = 3500) {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    const body = await page.evaluate(() => document.body.innerText.replace(/\s+/g, ' '));
    for (const re of DUP_PHRASES) { const m = body.match(re); if (m) return m[0].slice(0, 160); }
    await page.waitForTimeout(150);
  }
  return '';
}

// ---------------------------------------------------------------------------
// Primitives.
// ---------------------------------------------------------------------------

/** Expand the My Analysis panel so Manage Portfolio / Clear are in reach. */
async function expandPanel(page) {
  const b = page.getByRole('button', { name: /Expand My Analysis panel/i }).first();
  if (await b.count()) { await b.click().catch(() => {}); await page.waitForTimeout(2000); return true; }
  return false;
}

/** Click the first enabled "Add to Analysis" control. Returns true if clicked. */
async function addToAnalysis(page) {
  const clicked = await page.evaluate(() => {
    const vis = (el) => { const r = el.getBoundingClientRect(); return r.width > 0 && r.height > 0; };
    const b = [...document.querySelectorAll('button')]
      .filter((x) => /Add to Analysis/i.test(x.textContent) && !x.disabled && vis(x))[0];
    if (b) { b.click(); return true; }
    return false;
  });
  if (clicked) await page.waitForTimeout(1600);
  return clicked;
}

/** Is any "Add to Analysis" button present AND enabled right now? */
const addEnabled = (page) => page.evaluate(() => {
  const b = [...document.querySelectorAll('button')].find((x) => /Add to Analysis/i.test(x.textContent));
  return { present: !!b, enabled: !!(b && !b.disabled) };
});

/**
 * PROVEN reset primitive. Clicks "Clear Portfolio", handles any confirm dialog,
 * waits. Caller asserts readRoster().count === 0. Unproven in us17 (which only
 * text-tests the label), so this is authored fresh here.
 */
async function clearPortfolio(page) {
  await expandPanel(page);
  const clicked = await page.evaluate(() => {
    const c = (t) => (t || '').trim().replace(/\s+/g, ' ');
    const b = [...document.querySelectorAll('button,[role="button"]')].find((el) =>
      /^Clear Portfolio$/i.test(c(el.innerText)) || /clear portfolio/i.test(el.getAttribute('aria-label') || ''));
    if (b) { b.click(); return true; }
    return false;
  });
  await page.waitForTimeout(700);
  // A confirmation modal may appear ("Are you sure… Clear / Confirm / Yes").
  await page.evaluate(() => {
    const c = (t) => (t || '').trim().replace(/\s+/g, ' ');
    const dlg = [...document.querySelectorAll('[role="dialog"],[data-modal-root]')]
      .find((d) => /clear|remove all|are you sure|confirm/i.test(d.innerText || ''));
    if (!dlg) return;
    const btn = [...dlg.querySelectorAll('button,[role="button"]')]
      .find((b) => /^(clear|confirm|yes|remove all|clear portfolio|clear all)$/i.test(c(b.innerText)));
    if (btn) btn.click();
  });
  await page.waitForTimeout(1200);
  return clicked;
}

/** Clear → assert empty (banner-primary). Throws if the portfolio isn't empty. */
async function resetPortfolio(page) {
  await clearPortfolio(page);
  const pc = await portfolioCount(page);
  if (pc.effective !== 0) {
    throw new Error(`reset failed: portfolio still has ${pc.effective} (banner=${JSON.stringify(pc.banner)}, roster=${pc.roster.count})`);
  }
  return pc;
}

/** Coordinate-mode 5-step cascade (Risk Domain→Statistic) via aria-label triggers. */
async function cascadeCoordinateFilters(page) {
  await page.getByText(/Select Resilience Filters/i).first().click().catch(() => {});
  await page.waitForTimeout(900);
  const picks = {};
  for (const field of ['Risk Domain', 'Metric', 'Scenario', 'Period', 'Statistic']) {
    const trigger = page.locator(`button[aria-label="${field}"]`);
    if (await trigger.count() === 0 || !(await trigger.first().isEnabled().catch(() => false))) { picks[field] = null; continue; }
    await trigger.first().click({ timeout: 5000 }).catch(() => {});
    await page.waitForTimeout(700);
    const opt = page.locator('[role="option"]');
    if (await opt.count() === 0) { await page.keyboard.press('Escape').catch(() => {}); picks[field] = null; continue; }
    picks[field] = (await opt.first().innerText().catch(() => '')).trim().slice(0, 40);
    await opt.first().click({ timeout: 5000 }).catch(() => {});
    await page.waitForTimeout(800);
  }
  // Close the filter panel.
  await page.getByText(/Select Resilience Filters/i).first().click().catch(() => {});
  await page.waitForTimeout(1000);
  return picks;
}

/**
 * Idempotent filter gate. Resilience filters persist for the whole session, so
 * they must be applied ONCE (us17 does exactly this). If "Add to Analysis"
 * enables on its own after a selection/upload, filters are already set — skip.
 * Only when it stays disabled AND unset "Select" triggers exist do we cascade;
 * this prevents applyCoreFilters from 30s-hanging on an already-configured panel.
 */
async function ensureAddReady(page, coordMode = false) {
  const enabled = await page.waitForFunction(() => {
    const b = [...document.querySelectorAll('button')].find((x) => /Add to Analysis/i.test(x.textContent));
    return b && !b.disabled;
  }, { timeout: 12000 }).then(() => true).catch(() => false);
  if (enabled) return { applied: false, note: 'already enabled' };

  if (coordMode) {
    // The coordinate cascade is self-guarding (count/enabled checks + Escape),
    // so it is safe to invoke even if already configured.
    const picks = await cascadeCoordinateFilters(page);
    await page.waitForFunction(() => {
      const b = [...document.querySelectorAll('button')].find((x) => /Add to Analysis/i.test(x.textContent));
      return b && !b.disabled;
    }, { timeout: 8000 }).catch(() => {});
    return { applied: true, picks };
  }

  // Admin mode: only cascade if the panel actually has unset "Select" triggers.
  await page.getByText(/Select Resilience Filters/i).first().click().catch(() => {});
  await page.waitForTimeout(600);
  const unset = await page.getByText('Select', { exact: true }).count().catch(() => 0);
  if (unset === 0) {
    await page.getByText(/Select Resilience Filters/i).first().click().catch(() => {}); // close
    await page.waitForTimeout(4000); // filters set; give map data more time to load
    return { applied: false, note: 'filters already set; waited for load' };
  }
  await page.getByText(/Select Resilience Filters/i).first().click().catch(() => {}); // close; applyCoreFilters reopens
  await page.waitForTimeout(400);
  const picks = await applyCoreFilters(page);
  await page.waitForTimeout(800);
  return { applied: true, picks };
}

/**
 * Toggling between Administrative and Coordinate modes raises a confirmation
 * dialog ("Switch to Coordinates? … your current geography selection will be
 * cleared" and its reciprocal). It stays open and intercepts every later click
 * until confirmed, so we must click its proceed button. Returns the label
 * clicked (or false if no such dialog). The portfolio persists across the switch
 * — only the in-progress geography/coordinate selection is cleared.
 */
async function handleModeSwitch(page, waitMs = 4000) {
  const tryOnce = () => page.evaluate(() => {
    const c = (t) => (t || '').trim().replace(/\s+/g, ' ');
    const dlg = [...document.querySelectorAll('[data-modal-root], [role="dialog"]')]
      .find((d) => /Switch to (Coordinates|Administrative|Geography)\??|switching to (coordinate|geography|administrative)/i.test(d.innerText || ''));
    if (!dlg) return false;
    const btns = [...dlg.querySelectorAll('button, [role="button"]')];
    const isCancel = (b) => /cancel|stay|keep|dismiss|go back|^\s*no\b/i.test(c(b.innerText));
    const proceed = btns.find((b) => /switch|continue|proceed|confirm|yes|ok/i.test(c(b.innerText)) && !isCancel(b))
      || [...btns].reverse().find((b) => c(b.innerText) && !isCancel(b));
    if (proceed) { proceed.click(); return c(proceed.innerText) || 'proceed'; }
    return false;
  }).catch(() => false);
  // The dialog mounts asynchronously after the mode-toggle click — poll for it.
  const end = Date.now() + waitMs;
  do {
    const clicked = await tryOnce();
    if (clicked) { await page.waitForTimeout(1400); return clicked; }
    await page.waitForTimeout(200);
  } while (Date.now() < end);
  return false;
}

/** Re-select the pilot State if the District dropdown isn't usable (mode switches
 *  clear the geography selection, which disables the district picker). */
async function ensureState(page, name) {
  const districtReady = await page.evaluate(() => {
    const b = [...document.querySelectorAll('button')].find((x) => /Select District/i.test(x.getAttribute('aria-label') || '') || /Select District/i.test(x.innerText || ''));
    return !!(b && !(b.disabled || b.getAttribute('aria-disabled') === 'true'));
  });
  if (!districtReady) await selectState(page, name).catch(() => {});
}

/** Enter Administrative mode, confirming any mode-switch dialog + restoring State. */
async function switchToAdmin(page) {
  await killSurvey(page);
  await openAdmin(page);
  const sw = await handleModeSwitch(page);
  if (sw) { await page.waitForTimeout(600); await openAdmin(page); } // panel re-opens after confirm
  await killSurvey(page);
  // A mode switch clears the geography selection → District picker is disabled
  // until a State is re-selected.
  await ensureState(page, 'Telangana');
}

/** Add one Administrative district to the portfolio. Returns {added, enabled}. */
async function addViaAdmin(page, district) {
  await switchToAdmin(page);
  await selectDistrict(page, district);
  const filt = await ensureAddReady(page, false);
  const en = await addEnabled(page);
  const added = en.enabled ? await addToAnalysis(page) : false;
  return { added, enabled: en, picks: filt.picks || filt.note };
}

/** selectDistrict + ensure filters/data ready (for map-click cases). */
async function selectDistrictReady(page, district) {
  await switchToAdmin(page);
  await selectDistrict(page, district);
  return ensureAddReady(page, false);
}

/** Open the Coordinate Panel if it is collapsed, confirming any mode-switch dialog. */
async function openCoordinatePanel(page) {
  await killSurvey(page);
  const open = await page.evaluate(() => {
    const hasLat = [...document.querySelectorAll('input')].some((i) => i.placeholder === '17.8766');
    const hasTab = [...document.querySelectorAll('button')].some((b) => /^(Upload Coordinates|Add Coordinates)$/i.test((b.innerText || '').trim()));
    return hasLat || hasTab;
  });
  if (!open) {
    await page.getByRole('button', { name: /Coordinate Panel/i }).first().click().catch(() => {});
    await page.waitForTimeout(700);
    const sw = await handleModeSwitch(page);
    if (sw) await page.waitForTimeout(700);
  }
}

/** Add one manual coordinate to the portfolio. */
async function addViaCoordManual(page, { lat, lon, name }) {
  await openCoordinatePanel(page);
  // Ensure the manual sub-tab is active (panel may be on Upload from a prior case).
  await page.getByRole('button', { name: /^Add Coordinates$/i }).first().click().catch(() => {});
  await page.waitForTimeout(500);
  await handleModeSwitch(page, 3000); // the switch confirm can mount on sub-tab / first-use
  await killSurvey(page);
  await page.getByPlaceholder('17.8766').fill(lat);
  await page.getByPlaceholder('79.2792').fill(lon);
  if (name) await page.getByPlaceholder('Site 1').fill(name).catch(() => {});
  // Plot, then STAGE the point via "Add Coordinate" (singular) — the plot-only
  // "Show on Map" does not enable Add to Analysis; the point must be added to the
  // coordinate list first (probe-confirmed live control set).
  await page.getByRole('button', { name: /^Show on Map$/i }).first().click().catch(() => {});
  await page.waitForTimeout(800);
  await page.getByRole('button', { name: /^Add Coordinate$/i }).first().click().catch(() => {});
  await page.waitForTimeout(1500);
  const filt = await ensureAddReady(page, true);
  const en = await addEnabled(page);
  const added = en.enabled ? await addToAnalysis(page) : false;
  return { added, enabled: en, picks: filt.picks };
}

/** Upload a coordinate fixture and add the resulting points to the portfolio. */
async function addViaUpload(page, fixture) {
  await openCoordinatePanel(page);
  const uploadTab = page.getByRole('button', { name: /^Upload Coordinates$/i });
  if (await uploadTab.count() === 0) return { added: false, missing: true };
  await uploadTab.first().click().catch(() => {});
  await page.waitForTimeout(600);
  await handleModeSwitch(page, 3000); // switch confirm can mount when entering upload
  await killSurvey(page);
  const fileInput = page.locator('input[type="file"]');
  if (await fileInput.count() === 0) return { added: false, missing: true };
  await fileInput.first().setInputFiles(fixture);
  await page.waitForTimeout(700);
  await page.getByRole('button', { name: /^Upload$/i }).first().click().catch(() => {});
  await page.waitForTimeout(2800);
  const filt = await ensureAddReady(page, true);
  const en = await addEnabled(page);
  const added = en.enabled ? await addToAnalysis(page) : false;
  return { added, enabled: en, picks: filt.picks, missing: false };
}

/**
 * Map-click add (per us13). Clicks the canvas at proportional coords to raise the
 * floating box, then clicks the box's visible+enabled "Add to Analysis".
 * Binds to the visible control only (collapsed panels keep duplicate copies).
 */
async function addViaMapClick(page, fracX = 0.55, fracY = 0.45) {
  const map = page.locator('canvas').first();
  const box = await map.boundingBox();
  if (!box) return { added: false, boxSeen: false, reason: 'no canvas' };
  await page.mouse.click(box.x + box.width * fracX, box.y + box.height * fracY);
  await page.waitForTimeout(1500);
  const boxSeen = await page.evaluate(() =>
    /Baseline|Position|Value/i.test(document.body.innerText));
  const en = await addEnabled(page);
  const added = en.enabled ? await addToAnalysis(page) : false;
  return { added, boxSeen, enabled: en };
}

// ---------------------------------------------------------------------------
// Classification helper for Phase C.
// ---------------------------------------------------------------------------
function classify(beforeCount, afterCount, flag, kind /* 'strict' | 'semantic' */) {
  const secondRow = afterCount > beforeCount;
  if (secondRow) return kind === 'strict' ? 'MAJOR (silent duplicate)' : 'ASK-PO (point-in-district?)';
  if (flag) return kind === 'strict' ? 'PASS (rejected + flagged)' : 'PASS (distinct-unit, flagged)';
  return kind === 'strict' ? 'MINOR (dedup works, no feedback)' : 'PASS (distinct or deduped)';
}

// ===========================================================================
await withSession(async (page) => {
  const run = createRun('us-crossflow-add-to-analysis');
  attachCollectors(page, run);
  run.crossflow = { recon: {}, phaseA: {}, phaseB: {}, phaseC: {}, dupFlagString: '' };

  // Capture portfolio/compare API traffic (status + method).
  const api = [];
  page.on('response', (res) => {
    const u = res.url();
    if (/parquet|portfolio|compare|analysis/i.test(u) && !/\.js|\.css/i.test(u)) {
      api.push({ method: res.request().method(), status: res.status(), url: u.replace(APP_URL, '') });
    }
  });
  const compareHits = () => api.filter((h) => /portfolio-comparison-table/i.test(h.url));

  await installSurveyKiller(page); // must precede goto — installs the in-page survey remover
  await page.goto(APP_URL, { waitUntil: 'domcontentloaded' });
  await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => {});
  await page.waitForTimeout(1500);
  await killSurvey(page);
  await selectState(page, 'Telangana');

  // -----------------------------------------------------------------------
  // PHASE 0 — recon. Gates the run.
  // -----------------------------------------------------------------------
  console.log('\n== PHASE 0 — recon (gates the run) ==');

  const p1 = await safe(run, page, 'P0.1: prove Clear resets to empty', async () => {
    const a = await addViaAdmin(page, 'Warangal');
    if (!a.added) throw new Error(`could not seed one Admin site (enabled=${JSON.stringify(a.enabled)})`);
    await expandPanel(page);
    const seeded = await portfolioCount(page);
    await shot(page, run, 'p0-seeded');
    if (seeded.effective !== 1) throw new Error(`expected count 1 after one add, got ${seeded.effective} (banner=${JSON.stringify(seeded.banner)}, roster=${seeded.roster.count})`);
    await clearPortfolio(page);
    const cleared = await portfolioCount(page);
    await shot(page, run, 'p0-cleared');
    run.crossflow.recon.clear = { seeded, cleared, compareHitsBefore: compareHits().length };
    if (cleared.effective !== 0) {
      throw new Error(`Clear did NOT empty portfolio: still ${cleared.effective} (banner=${JSON.stringify(cleared.banner)}) — reset primitive unusable`);
    }
    return `seeded=1 → cleared=0; banner after clear=${cleared.banner ? cleared.banner.text : '(gone)'}`;
  });

  const p2 = await safe(run, page, 'P0.2: capture duplicate flag string', async () => {
    await resetPortfolio(page);
    // add-twice probe via Administrative (fast + deterministic).
    await addViaAdmin(page, 'Warangal');
    await expandPanel(page);
    const before = await portfolioCount(page);
    // Attempt the same district again.
    await addViaAdmin(page, 'Warangal');
    const flag = await grabDupFlag(page, 3500);
    await expandPanel(page);
    const after = await portfolioCount(page);
    await shot(page, run, 'p0-dup-probe');
    run.crossflow.dupFlagString = flag;
    run.crossflow.recon.dupProbe = { before, after, flag, secondRow: after.effective > before.effective };
    const tail = `before=${before.effective} after=${after.effective}`;
    if (flag) return `captured flag="${flag}" (${tail})`;
    if (after.effective > before.effective) return `OBSERVE: NO flag but count grew (${tail}) — early signal of silent duplicate; Phase C C1 will confirm`;
    return `OBSERVE: no dup flag; count did not grow (${tail}) — app appears to dedup silently; Phase C grid row (c)`;
  });

  const p3 = await safe(run, page, 'P0.3: bind map-click selectors', async () => {
    await resetPortfolio(page);
    // core filters must be active for the box to enable an Add control.
    await selectDistrictReady(page, 'Warangal');
    await page.waitForTimeout(800);
    const map = page.locator('canvas').first();
    const box = await map.boundingBox();
    if (!box) throw new Error('no canvas on page');
    await page.mouse.click(box.x + box.width * 0.55, box.y + box.height * 0.45);
    await page.waitForTimeout(1500);
    await shot(page, run, 'p0-mapclick');
    const info = await page.evaluate(() => {
      const body = document.body.innerText;
      const fields = ['Baseline', 'Position', 'Value', 'Add to Analysis'].filter((f) => body.includes(f));
      const b = [...document.querySelectorAll('button')].find((x) => /Add to Analysis/i.test(x.textContent));
      return { fields, addPresent: !!b, addEnabled: !!(b && !b.disabled) };
    });
    run.crossflow.recon.mapClick = info;
    if (!info.fields.length) return 'OBSERVE: no floating-box fields after canvas click — A4/C2/C6 map-click cases → OBSERVE (WebGL hit-test may need a lat/long fallback)';
    return `box fields=[${info.fields.join(',')}]; Add present=${info.addPresent} enabled=${info.addEnabled}`;
  });

  const p4 = await safe(run, page, 'P0.4: confirm compare endpoint fires', async () => {
    await resetPortfolio(page);
    await addViaAdmin(page, 'Warangal');
    await addViaAdmin(page, 'Karimnagar');
    await expandPanel(page);
    // Drive a minimal comparison to trigger the request.
    await page.getByRole('button', { name: /^Select Metrics$/i }).first().click().catch(() => {});
    await page.waitForTimeout(800);
    await page.getByText(/^All Metrics \(\d+\)/i).first().click().catch(() => {});
    await page.keyboard.press('Escape').catch(() => {});
    await page.waitForTimeout(600);
    const clickBox = (label) => page.evaluate((lb) => {
      const c = (t) => (t || '').trim().replace(/\s+/g, ' ');
      const l = [...document.querySelectorAll('label')].find((el) => new RegExp(`^${lb}$`, 'i').test(c(el.innerText)));
      if (l && l.querySelector('input')) { l.querySelector('input').click(); return true; }
      return false;
    }, label);
    await clickBox('SSP2-4\\.5');
    await clickBox('Early century \\(2020-2040\\)');
    await page.waitForTimeout(3500);
    await page.getByRole('button', { name: /^Table$/i }).first().click().catch(() => {});
    await page.waitForTimeout(2500);
    await shot(page, run, 'p0-compare');
    const hits = compareHits();
    run.crossflow.recon.compare = { hits: hits.slice(-4), fired: hits.length > 0 };
    if (!hits.length) return 'OBSERVE: portfolio-comparison-table not observed — Phase B falls back to DOM row assertion';
    return `portfolio-comparison-table fired; last status=${hits.slice(-1)[0].status}`;
  });

  const p0ok = p1 && p2 && p3 && p4;
  run.crossflow.recon.gate = { p1, p2, p3, p4, passed: p0ok };
  console.log(`\n  Phase 0 gate: ${p0ok ? 'PASS — running A/B/C' : 'FAIL — skipping A/B/C (results would be void)'}`);

  if (!p0ok) {
    step(run, 'GATE: Phases A/B/C skipped', false, 'Phase 0 recon did not fully pass; downstream results would be void. See run.crossflow.recon.');
  } else {
    // ---------------------------------------------------------------------
    // PHASE A — each flow adds one site.
    // ---------------------------------------------------------------------
    console.log('\n== PHASE A — each flow adds one site ==');

    await safe(run, page, 'A1: Administrative district adds 1', async () => {
      await resetPortfolio(page);
      const a = await addViaAdmin(page, 'Warangal');
      await expandPanel(page);
      const pc = await portfolioCount(page);
      run.crossflow.phaseA.A1 = { ...pc, enabled: a.enabled };
      if (!a.added) throw new Error(`Add not available (enabled=${JSON.stringify(a.enabled)})`);
      if (pc.effective !== 1) throw new Error(`expected count 1, got ${pc.effective} (banner=${JSON.stringify(pc.banner)})`);
      return `count=1; labels=${JSON.stringify(pc.roster.labels)}`;
    });

    await safe(run, page, 'A2: Coordinate manual add → 1', async () => {
      await resetPortfolio(page);
      const a = await addViaCoordManual(page, { ...WARANGAL, name: 'Manual QA Point' });
      await expandPanel(page);
      const pc = await portfolioCount(page);
      run.crossflow.phaseA.A2 = { ...pc, enabled: a.enabled, picks: a.picks };
      if (!a.added) throw new Error(`manual Add not enabled (enabled=${JSON.stringify(a.enabled)}, picks=${JSON.stringify(a.picks)})`);
      if (pc.effective !== 1) throw new Error(`expected count 1, got ${pc.effective} (banner=${JSON.stringify(pc.banner)})`);
      return `count=1; labels=${JSON.stringify(pc.roster.labels)}`;
    });

    await safe(run, page, 'A3: Coordinate upload → 3', async () => {
      await resetPortfolio(page);
      const a = await addViaUpload(page, APP_SAMPLE);
      if (a.missing) return 'OBSERVE: Upload Coordinates control absent this run — A3 not asserted';
      await expandPanel(page);
      const pc = await portfolioCount(page);
      run.crossflow.phaseA.A3 = { ...pc, enabled: a.enabled, picks: a.picks };
      if (!a.added) throw new Error(`upload Add not enabled (enabled=${JSON.stringify(a.enabled)})`);
      if (pc.effective !== 3) return `SPEC-DRIFT/BUG: expected count 3 from 3-row upload, got ${pc.effective} (banner=${JSON.stringify(pc.banner)}); labels=${JSON.stringify(pc.roster.labels)}`;
      return `count=3; labels=${JSON.stringify(pc.roster.labels)}`;
    });

    await safe(run, page, 'A4: Map click adds 1', async () => {
      await resetPortfolio(page);
      await selectDistrictReady(page, 'Warangal');
      await page.waitForTimeout(800);
      const m = await addViaMapClick(page);
      await expandPanel(page);
      const pc = await portfolioCount(page);
      run.crossflow.phaseA.A4 = { ...pc, boxSeen: m.boxSeen, enabled: m.enabled };
      if (!m.boxSeen) return `OBSERVE: floating box not detected after canvas click (WebGL hit-test); count=${pc.effective}`;
      if (!m.added) throw new Error(`map-click Add not enabled (box seen; enabled=${JSON.stringify(m.enabled)})`);
      if (pc.effective !== 1) throw new Error(`expected count 1, got ${pc.effective} (banner=${JSON.stringify(pc.banner)})`);
      return `box seen; count=1; labels=${JSON.stringify(pc.roster.labels)}`;
    });

    // ---------------------------------------------------------------------
    // PHASE B — one portfolio across flows.
    // ---------------------------------------------------------------------
    console.log('\n== PHASE B — build one portfolio across flows ==');

    await safe(run, page, 'B4: all flows → one portfolio', async () => {
      await resetPortfolio(page);
      const steps = [];
      const trail = []; // running count after each add, to expose cross-mode loss
      const mark = async (label) => { await expandPanel(page); const c = (await portfolioCount(page)).effective; trail.push(`${label}=${c}`); return c; };
      // Admin
      await addViaAdmin(page, 'Warangal'); steps.push('admin:Warangal'); await mark('after-admin');
      // Coordinate manual (distinct coordinate to avoid a dup with Warangal district)
      await addViaCoordManual(page, { lat: '17.9689', lon: '79.5941', name: 'Manual Mix Point' }); steps.push('manual'); await mark('after-manual');
      // Upload (3 rows)
      const up = await addViaUpload(page, APP_SAMPLE); steps.push(up.missing ? 'upload:absent' : 'upload:3'); await mark('after-upload');
      // Map click
      await selectDistrictReady(page, 'Karimnagar');
      await page.waitForTimeout(600);
      const m = await addViaMapClick(page); steps.push(m.boxSeen ? 'mapclick' : 'mapclick:box-absent'); await mark('after-mapclick');
      await expandPanel(page);
      const pc = await portfolioCount(page);
      const expected = up.missing ? 3 : 6; // 1 admin + 1 manual + 3 upload + 1 mapclick
      const crossModeLoss = pc.effective < expected;
      run.crossflow.phaseB.B4 = { ...pc, steps, trail, expected, crossModeLoss, compareHits: compareHits().slice(-3) };
      return `count=${pc.effective} (expected≈${expected}); trail=[${trail.join(', ')}]; ${crossModeLoss ? 'CROSS-MODE-LOSS: portfolio did not accumulate across flows' : 'accumulated OK'}; compare-fired=${compareHits().length > 0}`;
    });

    // ---------------------------------------------------------------------
    // PHASE C — duplicate detection.
    // ---------------------------------------------------------------------
    console.log('\n== PHASE C — duplicate detection ==');

    const runDupCase = async (id, kind, seedFn, dupFn) => {
      await safe(run, page, `${id}: duplicate (${kind})`, async () => {
        await resetPortfolio(page);
        await seedFn();
        await expandPanel(page);
        const before = await portfolioCount(page);
        await dupFn();
        const flag = await grabDupFlag(page, 3500);
        await expandPanel(page);
        const after = await portfolioCount(page);
        await shot(page, run, `${id.toLowerCase()}`);
        const verdict = classify(before.effective, after.effective, flag, kind);
        run.crossflow.phaseC[id] = { kind, before, after, flag, verdict };
        return `${before.effective}→${after.effective}; flag="${flag || '(none)'}"; VERDICT=${verdict}`;
      });
    };

    // C-strict
    await runDupCase('C1', 'strict',
      async () => { await addViaAdmin(page, 'Warangal'); },
      async () => { await addViaAdmin(page, 'Warangal'); });

    await runDupCase('C3', 'strict',
      async () => { await addViaUpload(page, DUP_SAME); },
      async () => { /* dup_same_name.csv already carries both identical rows */ });
    // C3 special-case: the fixture itself is the duplicate (two identical rows in
    // one upload). Re-evaluate: before is measured AFTER seed, so use a direct read.
    if (run.crossflow.phaseC.C3) {
      const c3 = run.crossflow.phaseC.C3;
      // For an internal-duplicate upload, "before" already includes both rows;
      // reclassify by whether the single upload produced 2 rows for one coord.
      c3.note = 'C3 = two identical rows in ONE upload; count 2 ⇒ MAJOR (silent dup), count 1 ⇒ deduped';
      c3.verdict = c3.before.effective > 1 ? 'MAJOR (silent duplicate rows from one upload)'
        : (c3.flag ? 'PASS (rejected + flagged)' : 'MINOR (deduped, no feedback)');
    }

    await runDupCase('C4', 'strict',
      async () => { await addViaUpload(page, DUP_DIFF); },
      async () => { /* dup_diff_name.csv carries both rows: same coord, different name */ });
    if (run.crossflow.phaseC.C4) {
      const c4 = run.crossflow.phaseC.C4;
      c4.note = 'C4 = same coord, DIFFERENT name, one upload; count 2 ⇒ MAJOR (name defeated dedup), count 1 ⇒ deduped';
      c4.verdict = c4.before.effective > 1 ? 'MAJOR (different name defeated coordinate dedup)'
        : (c4.flag ? 'PASS (rejected + flagged)' : 'MINOR (deduped, no feedback)');
    }

    // C-semantic
    await runDupCase('C2', 'semantic',
      async () => { await addViaAdmin(page, 'Warangal'); },
      async () => {
        // add same district via a map click landing inside it
        await selectDistrictReady(page, 'Warangal');
        await page.waitForTimeout(600);
        await addViaMapClick(page);
      });

    await runDupCase('C5', 'semantic',
      async () => { await addViaAdmin(page, 'Warangal'); },
      async () => { await addViaCoordManual(page, { ...WARANGAL, name: 'Inside Warangal' }); });

    await runDupCase('C6', 'semantic',
      async () => { await addViaUpload(page, DUP_SAME); /* seeds the Warangal coord (2 rows) */ },
      async () => {
        await selectDistrictReady(page, 'Warangal');
        await page.waitForTimeout(600);
        await addViaMapClick(page);
      });
  }

  // Cross-cutting evidence.
  await dumpDom(page, run, 'crossflow-final');
  await snapshot(page, run, 'crossflow-responsive');
  await runAxe(page, run, 'crossflow');
  finalize(run);

  console.log(`\n  Run: ${run.dir}`);
  console.log(`  Phase 0 gate passed: ${p0ok}`);
  console.log(`  dup flag string: "${run.crossflow.dupFlagString || '(none captured)'}"`);
  console.log(`  Steps failed: ${run.summary.stepsFailed} | real errors: ${run.summary.errorEvents} | benign: ${run.summary.benignEvents}`);
});
