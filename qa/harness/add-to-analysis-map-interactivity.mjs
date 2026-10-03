// US-MAP-INTERACTIVITY (CHG-0225) — probe: does the district DROPDOWN gate map
// interactivity, and is the portfolio order-dependent (non-commutative)?
//
// Two user-reported claims, establishable ONLY by driving the live vendor app
// (dev.resilience.org.in — frontend source is not in this repo):
//   Claim 1 (gating): after a State is picked the whole map is live; once a
//     district is chosen in the DROPDOWN, hover/click only respond over that
//     district.
//   Claim 2 (order-dependent portfolio): map-add A,B,C then dropdown-add D,E,F
//     -> 6; but dropdown-first appears to lock out A,B,C -> stuck at 3. A
//     portfolio is a set union, so an order-dependent final count is a defect.
//
// This probe emits NO Claim verdict unless THREE hard gates pass; otherwise it
// records BLOCKED (<gate>). It never PASS/FAILs on a gate miss.
//   Gate 1 stateNormalized   — geography normalized + filters ready + no stale
//                              popover + PORTFOLIO EMPTY (effective === 0).
//   Gate 2 interactionCalibrated — empirically learn how the app surfaces a
//                              district under the pointer (hover-tooltip OR
//                              click-info-box). Neither -> abort (GL/headless).
//   Gate 3 mapPopoverScopedAdd — a map add is counted only when an Add control
//                              INSIDE the freshly-opened map popover is clicked,
//                              proven by a positive smoke test whose one item is
//                              then removed (no residue leaks into Claims).
//
// Standalone by design — duplicates the full self-contained portfolio closure
// from add-to-analysis-crossflow.mjs (isolated per prior session decision, not a
// shared lib/portfolio.mjs). Imports only stable lib/ helpers. Testing tier:
// QA tooling, no pytest (AGENTS.md §3, mirror CLAUDE.md §3).
//
//   node --check qa/harness/add-to-analysis-map-interactivity.mjs   # syntax first
//   node qa/harness/capture-session.mjs                             # refresh ~24h 2FA
//   QA_SOFTWARE_GL=1 node qa/harness/add-to-analysis-map-interactivity.mjs
//
// Evidence -> qa/runs/<id>_us-map-interactivity/ ; results.json carries a
// serializable run.probe record; automated-summary.md is a machine-generated
// digest (NOT a vendor report — CHG-0226 is authored by hand after triage).

import { withSession, APP_URL } from './lib/session.mjs';
import {
  createRun, attachCollectors, snapshot, dumpDom, runAxe, finalize, step,
} from './lib/evidence.mjs';
import { shot } from './lib/runner.mjs';
import { openAdmin, selectState, applyCoreFilters } from './lib/flows.mjs';
import { writeFileSync } from 'node:fs';
import { join } from 'node:path';

// Named verdict thresholds (no magic numbers in the phases).
const TH = {
  L0_LIVE_MIN: 8,          // Claim 1 baseline: >= this many live grid points
  D0_DISTINCT_MIN: 3,      // Claim 1 baseline: >= this many distinct districts
  CLAIM2_DISTINCT_MIN: 6,  // Claim 2 full coverage needs A,B,C + D,E,F distinct
  OUTSIDE_SELECTED_MAX: 1, // gating: at most this many live points outside the pick
  OVERLAP_MIN: 0.8,        // "not gated": L1 overlaps L0 by >= 80%
  GRID_N: 6,               // sweep grid resolution (n x n) — 6x6 lands enough
                           // on-polygon points over Telangana's irregular shape.
};

const W = (page, ms) => page.waitForTimeout(ms);

// Field vocabulary of the ACTUAL map info surfaces (confirmed live + us13 report):
//   district hover/click tooltip -> "District", "State", "Composite Score", "Rank in state"
//   coordinate-point popover     -> "Latitude"/"Longitude"/"Value"/"Position"
// (Baseline / Δ-vs-baseline are omitted by the app per us13.) A surfaced info box
// is identified by these fields AND/OR a dropdown-roster district-name match — never
// by the presence of the bare word "District" (which also appears in filter chrome).
const INFO_FIELD_RE = 'Composite Score|Rank in state|Latitude|Longitude|Position in state';

// ===========================================================================
// Duplicated closure from add-to-analysis-crossflow.mjs (COMPLETE) — bodies are
// copied verbatim; addViaAdmin/addViaMapClick are intentionally NOT copied
// (rewritten as addViaAdminExact / mapAddScoped). selectDistrict is not reused
// (no clear, no assert) — replaced by selectDistrictExact.
// ===========================================================================

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

// 4-arg guarded step runner (crossflow variant — distinct from lib/runner.mjs's 3-arg safe).
async function safe(run, page, name, fn) {
  try {
    await killSurvey(page);
    const note = await fn();
    step(run, name, true, note || '');
    console.log(`  ok   ${name}${note ? ' — ' + note : ''}`);
    return true;
  } catch (e) {
    const blockers = await page.evaluate(() => [...document.querySelectorAll('[data-modal-root], [role="dialog"]')]
      .map((el) => ({ attr: el.getAttribute('data-modal-root'), cls: (el.className || '').slice(0, 90), text: (el.innerText || '').replace(/\s+/g, ' ').trim().slice(0, 130), buttons: [...el.querySelectorAll('button,[role="button"]')].map((b) => (b.getAttribute('aria-label') || b.innerText || '').replace(/\s+/g, ' ').trim()).filter(Boolean).slice(0, 8) }))).catch(() => []);
    const msg = String((e && e.message) || e).split('\n')[0];
    const note = blockers.length ? `${msg} || BLOCKERS=${JSON.stringify(blockers)}` : msg;
    step(run, name, false, note);
    console.log(`  FAIL ${name} — ${note}`);
    return false;
  }
}

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
  const anchors = showBtns.length ? showBtns : removeBtns;
  const labels = anchors.map((a) => {
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

const countBanner = (page) => page.evaluate(() => {
  const m = document.body.innerText.match(/You have added\s+(\d+)\s+([A-Za-z]+)s?\b[^.]*?portfolio/i)
    || document.body.innerText.match(/You have added\s+(\d+)\s+([A-Za-z]+)s?\b/i);
  return m ? { n: Number(m[1]), unit: m[2], text: m[0].slice(0, 80) } : null;
});

async function portfolioCount(page) {
  const banner = await countBanner(page);
  const roster = await readRoster(page);
  const effective = banner ? banner.n : roster.count;
  return { effective, banner, roster };
}

async function expandPanel(page) {
  const b = page.getByRole('button', { name: /Expand My Analysis panel/i }).first();
  if (await b.count()) { await b.click().catch(() => {}); await W(page, 2000); return true; }
  return false;
}

async function addToAnalysis(page) {
  const clicked = await page.evaluate(() => {
    const vis = (el) => { const r = el.getBoundingClientRect(); return r.width > 0 && r.height > 0; };
    const b = [...document.querySelectorAll('button')]
      .filter((x) => /Add to Analysis/i.test(x.textContent) && !x.disabled && vis(x))[0];
    if (b) { b.click(); return true; }
    return false;
  });
  if (clicked) await W(page, 1600);
  return clicked;
}

const addEnabled = (page) => page.evaluate(() => {
  const b = [...document.querySelectorAll('button')].find((x) => /Add to Analysis/i.test(x.textContent));
  return { present: !!b, enabled: !!(b && !b.disabled) };
});

async function clearPortfolio(page) {
  await expandPanel(page);
  const clicked = await page.evaluate(() => {
    const c = (t) => (t || '').trim().replace(/\s+/g, ' ');
    const b = [...document.querySelectorAll('button,[role="button"]')].find((el) =>
      /^Clear Portfolio$/i.test(c(el.innerText)) || /clear portfolio/i.test(el.getAttribute('aria-label') || ''));
    if (b) { b.click(); return true; }
    return false;
  });
  await W(page, 700);
  await page.evaluate(() => {
    const c = (t) => (t || '').trim().replace(/\s+/g, ' ');
    const dlg = [...document.querySelectorAll('[role="dialog"],[data-modal-root]')]
      .find((d) => /clear|remove all|are you sure|confirm/i.test(d.innerText || ''));
    if (!dlg) return;
    const btn = [...dlg.querySelectorAll('button,[role="button"]')]
      .find((b) => /^(clear|confirm|yes|remove all|clear portfolio|clear all)$/i.test(c(b.innerText)));
    if (btn) btn.click();
  });
  await W(page, 1200);
  return clicked;
}

async function resetPortfolio(page) {
  await clearPortfolio(page);
  const pc = await portfolioCount(page);
  if (pc.effective !== 0) {
    throw new Error(`reset failed: portfolio still has ${pc.effective} (banner=${JSON.stringify(pc.banner)}, roster=${pc.roster.count})`);
  }
  return pc;
}

async function cascadeCoordinateFilters(page) {
  await page.getByText(/Select Resilience Filters/i).first().click().catch(() => {});
  await W(page, 900);
  const picks = {};
  for (const field of ['Risk Domain', 'Metric', 'Scenario', 'Period', 'Statistic']) {
    const trigger = page.locator(`button[aria-label="${field}"]`);
    if (await trigger.count() === 0 || !(await trigger.first().isEnabled().catch(() => false))) { picks[field] = null; continue; }
    await trigger.first().click({ timeout: 5000 }).catch(() => {});
    await W(page, 700);
    const opt = page.locator('[role="option"]');
    if (await opt.count() === 0) { await page.keyboard.press('Escape').catch(() => {}); picks[field] = null; continue; }
    picks[field] = (await opt.first().innerText().catch(() => '')).trim().slice(0, 40);
    await opt.first().click({ timeout: 5000 }).catch(() => {});
    await W(page, 800);
  }
  await page.getByText(/Select Resilience Filters/i).first().click().catch(() => {});
  await W(page, 1000);
  return picks;
}

async function ensureAddReady(page, coordMode = false) {
  const enabled = await page.waitForFunction(() => {
    const b = [...document.querySelectorAll('button')].find((x) => /Add to Analysis/i.test(x.textContent));
    return b && !b.disabled;
  }, { timeout: 12000 }).then(() => true).catch(() => false);
  if (enabled) return { applied: false, note: 'already enabled' };

  if (coordMode) {
    const picks = await cascadeCoordinateFilters(page);
    await page.waitForFunction(() => {
      const b = [...document.querySelectorAll('button')].find((x) => /Add to Analysis/i.test(x.textContent));
      return b && !b.disabled;
    }, { timeout: 8000 }).catch(() => {});
    return { applied: true, picks };
  }

  await page.getByText(/Select Resilience Filters/i).first().click().catch(() => {});
  await W(page, 600);
  const unset = await page.getByText('Select', { exact: true }).count().catch(() => 0);
  if (unset === 0) {
    await page.getByText(/Select Resilience Filters/i).first().click().catch(() => {});
    await W(page, 4000);
    return { applied: false, note: 'filters already set; waited for load' };
  }
  await page.getByText(/Select Resilience Filters/i).first().click().catch(() => {});
  await W(page, 400);
  const picks = await applyCoreFilters(page);
  await W(page, 800);
  return { applied: true, picks };
}

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
  const end = Date.now() + waitMs;
  do {
    const clicked = await tryOnce();
    if (clicked) { await W(page, 1400); return clicked; }
    await W(page, 200);
  } while (Date.now() < end);
  return false;
}

async function ensureState(page, name) {
  const districtReady = await page.evaluate(() => {
    const b = [...document.querySelectorAll('button')].find((x) => /Select District/i.test(x.getAttribute('aria-label') || '') || /Select District/i.test(x.innerText || ''));
    return !!(b && !(b.disabled || b.getAttribute('aria-disabled') === 'true'));
  });
  if (!districtReady) await selectState(page, name).catch(() => {});
}

async function switchToAdmin(page) {
  await killSurvey(page);
  await openAdmin(page);
  const sw = await handleModeSwitch(page);
  if (sw) { await W(page, 600); await openAdmin(page); }
  await killSurvey(page);
  await ensureState(page, 'Telangana');
}

// ===========================================================================
// New / hardened primitives (CHG-0225).
// ===========================================================================

/**
 * Dismiss any open map info-box / popover so it can't fool ensureAddReady's
 * page-wide Add check or contaminate the next interaction sample. Scoped to
 * transient map tooltips/popovers — the survey killer handles [data-modal-root].
 * (#3, #5)
 */
async function clearMapPopover(page) {
  // Non-destructive: never remove app-managed nodes (that corrupts React's map
  // reconciliation). Dismiss the click tooltip via Escape / its own close control,
  // and move the pointer off the canvas so any hover tooltip fades naturally.
  await page.keyboard.press('Escape').catch(() => {});
  await page.evaluate(() => {
    const c = (t) => (t || '').trim().replace(/\s+/g, ' ');
    const hints = '[role="tooltip"],[class*="tooltip"],[class*="Tooltip"],[class*="popup"],[class*="Popup"],[class*="popover"],[class*="Popover"],[class*="deck-tooltip"]';
    document.querySelectorAll(hints).forEach((el) => {
      const close = [...el.querySelectorAll('button,[role="button"]')].find((b) => /^(close|dismiss)$|×|✕|⊗/i.test(c(b.innerText) + ' ' + (b.getAttribute('aria-label') || '')));
      if (close) { try { close.click(); } catch (e) { /* noop */ } }
    });
  }).catch(() => {});
  await page.mouse.move(3, 3).catch(() => {}); // off-canvas → hover tooltip clears
  await W(page, 200);
  // Report how many map info-boxes remain visible (0 = clean).
  return page.evaluate((fieldReSrc) => {
    const fieldRe = new RegExp(fieldReSrc, 'i');
    return [...document.querySelectorAll('div,section,aside,[role="tooltip"]')].filter((el) => {
      const r = el.getBoundingClientRect();
      return r.width > 0 && r.height > 0 && r.width < 640 && fieldRe.test(el.innerText || '');
    }).length;
  }, INFO_FIELD_RE).catch(() => 0);
}

/**
 * Locate a Reset / clear-geography control WITHIN the Administrative Panel /
 * geography container (scoped to the admin root so it can't hit a portfolio
 * "clear" or unrelated action). Returns {click, text, aria} or null. (#4)
 */
async function discoverResetControl(page) {
  const meta = await page.evaluate(() => {
    const c = (t) => (t || '').trim().replace(/\s+/g, ' ');
    // Find the admin/geography container: the ancestor that holds the State picker.
    const stateBtn = [...document.querySelectorAll('button')].find((b) => /Select State|Administrative Panel/i.test(c(b.innerText) + ' ' + (b.getAttribute('aria-label') || '')));
    let root = stateBtn;
    for (let i = 0; i < 8 && root && root.parentElement; i++) root = root.parentElement;
    const scope = root || document.body;
    const re = /reset|clear (all|filters|geography)/i;
    const btn = [...scope.querySelectorAll('button,[role="button"]')].find((b) => {
      const label = c(b.innerText) + ' ' + (b.getAttribute('aria-label') || '');
      // Guard: never a portfolio-clear.
      if (/portfolio/i.test(label)) return false;
      return re.test(label);
    });
    if (!btn) return null;
    // Tag it so the caller can click via a stable selector.
    btn.setAttribute('data-qa-reset', '1');
    return { text: c(btn.innerText).slice(0, 60), aria: (btn.getAttribute('aria-label') || '').slice(0, 60) };
  });
  if (!meta) return null;
  return {
    click: () => page.locator('[data-qa-reset="1"]').first().click().catch(() => {}),
    text: meta.text,
    aria: meta.aria,
  };
}

/** Read the current district-picker display mode ('empty-placeholder' | 'all-districts' | 'district-selected'). */
const readDistrictMode = (page) => page.evaluate(() => {
  const c = (t) => (t || '').trim().replace(/\s+/g, ' ');
  const b = [...document.querySelectorAll('button')].find((x) => /Select District|District/i.test(x.getAttribute('aria-label') || '') || /Select District|All Districts|District/i.test(c(x.innerText)));
  if (!b) return 'unknown';
  const t = c(b.innerText);
  if (/^Select District/i.test(t) || t === '') return 'empty-placeholder';
  if (/All Districts/i.test(t)) return 'all-districts';
  return 'district-selected';
});

/** Read the district dropdown option roster (used to parse surfaced names). */
async function readDistrictOptions(page) {
  const dBtn = page.getByRole('button', { name: /Select District/i }).first();
  if (!(await dBtn.count())) return [];
  await dBtn.click().catch(() => {});
  await W(page, 600);
  const opts = await page.locator('li[role="option"]').allInnerTexts().catch(() => []);
  await dBtn.click().catch(() => {}); // close
  await W(page, 300);
  return [...new Set(opts.map((t) => t.trim()).filter(Boolean))];
}

/**
 * Normalize to a clean whole-state baseline (Claim 1 precondition + Gate 1):
 * clear popover -> reset (scoped) -> Admin -> Telangana -> leave district empty
 * -> check filter-control state directly, then ensureAddReady -> resetPortfolio
 * + assert effective===0 -> killSurvey -> assert no popover. (#1, #3, #4)
 */
async function clearGeographyToStateWide(page) {
  await clearMapPopover(page);
  const reset = await discoverResetControl(page);
  if (reset) { await reset.click(); await W(page, 800); }
  await switchToAdmin(page);
  await selectState(page, 'Telangana');
  await W(page, 800);
  // Do NOT select "All Districts"; prefer an empty district placeholder.
  const districtMode = await readDistrictMode(page);
  // Check filter-control state directly (are filters set?) before relying on Add.
  await clearMapPopover(page);
  await ensureAddReady(page, false);
  await resetPortfolio(page);
  await killSurvey(page);
  const popoverLeft = await clearMapPopover(page);
  const pc = await portfolioCount(page);
  const stateNormalized = pc.effective === 0 && popoverLeft === 0;
  return {
    stateNormalized,
    normalizedDistrictMode: districtMode,
    portfolioEmpty: pc.effective === 0,
    popoverLeft,
    resetControl: reset ? { text: reset.text, aria: reset.aria } : null,
  };
}

/**
 * Select exactly ONE district by name and verify the selection resolves to
 * exactly {name}. Clears any prior selection first; throws if the resulting
 * district display does not reflect name.
 */
async function selectDistrictExact(page, name) {
  const dBtn = page.getByRole('button', { name: /Select District/i }).first();
  await dBtn.click().catch(() => {});
  await W(page, 600);
  // Deselect any already-checked options so each admin add is exactly one district.
  await page.evaluate(() => {
    [...document.querySelectorAll('li[role="option"][aria-selected="true"]')].forEach((o) => { try { o.click(); } catch (e) { /* noop */ } });
  }).catch(() => {});
  await W(page, 300);
  await page.locator('li[role="option"]').filter({ hasText: new RegExp(`^${name}$`) }).first().click();
  await W(page, 500);
  // Verify against the option's SELECTED state (robust to button-label truncation
  // — the multi-select "Select District(s)" collapses long names like
  // "Bhadradri Kothagudem"). Fall back to the collapsed button text only if the
  // control does not expose aria-selected.
  const check = await page.evaluate(() => {
    const c = (t) => (t || '').trim().replace(/\s+/g, ' ');
    const selOpts = [...document.querySelectorAll('li[role="option"][aria-selected="true"]')].map((o) => c(o.innerText));
    const b = [...document.querySelectorAll('button')].find((x) => /Select District|District/i.test(x.getAttribute('aria-label') || '') || /District/i.test(c(x.innerText)));
    return { selOpts, btnText: b ? c(b.innerText) : '' };
  });
  await dBtn.click().catch(() => {}); // close dropdown
  await W(page, 400);
  const rx = new RegExp(name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i');
  const rxFirst = new RegExp('\\b' + name.split(/\s+/)[0].replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i');
  const ok = check.selOpts.length
    ? check.selOpts.some((s) => rx.test(s))
    : (rx.test(check.btnText) || rxFirst.test(check.btnText));
  if (!ok) {
    throw new Error(`selectDistrictExact(${name}): not confirmed (selOpts=${JSON.stringify(check.selOpts)}, btn="${check.btnText}")`);
  }
  return check.selOpts.length ? check.selOpts.join(', ') : check.btnText;
}

/**
 * Add one Administrative district with a hard +1 assertion. (#1 carry-forward)
 * switchToAdmin -> preCount -> selectDistrictExact -> ensureAddReady ->
 * addToAnalysis -> postCount; throws unless postCount === preCount + 1.
 */
async function addViaAdminExact(page, name) {
  await switchToAdmin(page);
  const pre = (await portfolioCount(page)).effective;
  await selectDistrictExact(page, name);
  await clearMapPopover(page);
  await ensureAddReady(page, false);
  const added = await addToAnalysis(page);
  await expandPanel(page);
  const pc = await portfolioCount(page);
  const post = pc.effective;
  if (post !== pre + 1) {
    throw new Error(`addViaAdminExact(${name}): expected ${pre + 1}, got ${post} (pre=${pre}, added=${added})`);
  }
  // Soft scoped proof: the new roster entry should mention this district (labels
  // can truncate, so this is recorded, not asserted).
  const rxFirst = new RegExp('\\b' + name.split(/\s+/)[0].replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i');
  const labelMatch = (pc.roster.labels || []).some((l) => rxFirst.test(l));
  return { added, preCount: pre, postCount: post, labelMatch };
}

/** Cheap positive-only map view signature — a change is proof of a viewport/transform shift (#2). */
const viewSignature = (page) => page.evaluate(() => {
  const canvas = document.querySelector('canvas');
  const t = canvas ? getComputedStyle(canvas.parentElement || canvas).transform : '';
  const zoom = (document.body.innerText.match(/zoom[:\s]+([\d.]+)/i) || [])[1] || '';
  const url = location.href;
  return `${t}|z=${zoom}|${url}`;
});

/**
 * Read whatever the app surfaced under the pointer, matched against the district
 * dropdown roster (Baseline omitted per us13 — click tooltip only). Returns
 * SERIALIZABLE metadata only, never a DOM handle. (#5)
 */
async function readSurfaced(page, roster, fieldRe) {
  return page.evaluate(({ names, fieldReSrc }) => {
    const c = (t) => (t || '').trim().replace(/\s+/g, ' ');
    const fieldRe = new RegExp(fieldReSrc, 'i');
    const vis = (el) => {
      const r = el.getBoundingClientRect();
      const s = getComputedStyle(el);
      // NB: do NOT exclude pointer-events:none — the hover tooltip is click-through.
      // The field-vocabulary filter below (Composite Score / Rank in state) already
      // excludes the filter/selection chrome, which lacks those labels.
      return r.width > 0 && r.height > 0 && s.visibility !== 'hidden' && s.display !== 'none';
    };
    const esc = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    // An info surface is a SMALL box that carries the map-tooltip fields; the
    // filter panel / selection chrome is large and lacks "Composite Score"/"Rank".
    const cands = [...document.querySelectorAll('div,section,aside,[role="tooltip"]')].filter((el) => {
      const r = el.getBoundingClientRect();
      if (!vis(el) || r.width < 60 || r.width > 640 || r.height < 40 || r.height > 640) return false;
      return fieldRe.test(el.innerText || '');
    });
    // Prefer the smallest candidate that also matches a roster district name.
    cands.sort((a, b) => {
      const ra = a.getBoundingClientRect(); const rb = b.getBoundingClientRect();
      return (ra.width * ra.height) - (rb.width * rb.height);
    });
    let best = null;
    for (const el of cands) {
      const t = c(el.innerText);
      if (!t) continue;
      const hit = (names || []).find((nm) => nm && new RegExp('\\b' + esc(nm) + '\\b', 'i').test(t));
      const r = el.getBoundingClientRect();
      const rec = {
        surfaced: true,
        district: hit || null,
        text: t.slice(0, 300),
        rect: { x: Math.round(r.x), y: Math.round(r.y), w: Math.round(r.width), h: Math.round(r.height) },
        hint: (el.className || el.tagName || '').toString().slice(0, 60),
      };
      if (hit) return rec;            // roster-name match is the strongest signal
      if (!best) best = rec;          // else remember the smallest field-y box
    }
    return best || { surfaced: false, district: null, text: '', rect: null, hint: '' };
  }, { names: roster, fieldReSrc: fieldRe || 'Composite Score|Rank in state|Latitude|Longitude' });
}

/**
 * Calibrated "what district is here" reader. `mode` is decided in Phase 1.
 * Returns serializable-only metadata; clears any popover afterward. (#5)
 */
async function interactionProbe(page, fracX, fracY, mode, roster) {
  const canvas = page.locator('canvas').first();
  const box = await canvas.boundingBox();
  if (!box) return { mode, surfaced: false, district: null, popoverText: '', rect: null, selectorHint: 'no-canvas' };
  const x = box.x + box.width * fracX;
  const y = box.y + box.height * fracY;
  if (mode === 'hover') {
    // deck.gl hover needs a genuine pointermove delta ending on the target — a
    // single teleport move is unreliable, so approach in two steps then settle.
    await page.mouse.move(x - 6, y - 6);
    await page.mouse.move(x, y, { steps: 3 });
    await W(page, 650);
  } else {
    await page.mouse.click(x, y);
    await W(page, 900);
  }
  const res = await readSurfaced(page, roster, INFO_FIELD_RE);
  return { mode, surfaced: res.surfaced, district: res.district, popoverText: res.text, rect: res.rect, selectorHint: res.hint };
}

/**
 * Classify a post-selection sample into FOUR states (#2):
 *   sameDistrict | differentDistrictOrBlock | inert | offCanvasOrViewportShift
 * `inert` (nothing surfaced) is Claim-1 gating evidence — never viewChanged.
 * viewChanged requires a positive different-district/block surface OR a PROVEN
 * view-signature delta.
 */
function classifyTarget(sample, selectedDistrict, knownDistricts, blocks, viewDelta) {
  const sel = (selectedDistrict || '').toLowerCase();
  if (!sample.surfaced) {
    return viewDelta ? 'offCanvasOrViewportShift' : 'inert';
  }
  const d = (sample.district || '').toLowerCase();
  if (d && d === sel) return 'sameDistrict';
  const isBlock = d && (blocks || []).some((b) => b.toLowerCase() === d);
  const isOtherDistrict = d && (knownDistricts || []).some((k) => k.toLowerCase() === d && k.toLowerCase() !== sel);
  if (isBlock || isOtherDistrict) return 'differentDistrictOrBlock';
  if (viewDelta) return 'offCanvasOrViewportShift';
  // Surfaced coordinate-fields box with no recognizable name — treat as inert
  // for gating unless a proven view change accompanies it.
  return sample.district ? 'differentDistrictOrBlock' : 'inert';
}

/** After a district is selected, read active Block controls/options (drill-down classifier feed). */
async function discoverBlockRoster(page) {
  const meta = await page.evaluate(() => {
    const c = (t) => (t || '').trim().replace(/\s+/g, ' ');
    const btn = [...document.querySelectorAll('button')].find((b) => /Select Block|Block/i.test(b.getAttribute('aria-label') || '') || /^Select Block/i.test(c(b.innerText)));
    return { blockMode: !!btn, label: btn ? c(btn.innerText).slice(0, 40) : null };
  });
  const blocks = [];
  if (meta.blockMode) {
    await page.getByRole('button', { name: /Block/i }).first().click().catch(() => {});
    await W(page, 500);
    const opts = await page.locator('li[role="option"]').allInnerTexts().catch(() => []);
    blocks.push(...opts.map((t) => t.trim()).filter(Boolean));
    await page.keyboard.press('Escape').catch(() => {});
    await W(page, 300);
  }
  return { blockMode: meta.blockMode, blocks: [...new Set(blocks)] };
}

/**
 * Map-add that ONLY clicks an Add control found INSIDE the freshly-opened map
 * popover near the click point — never the global panel Add. ElementHandle
 * (if any) stays local; returns serializable record only. (#3, #5)
 */
async function mapAddScoped(page, fracX, fracY) {
  const canvas = page.locator('canvas').first();
  const box = await canvas.boundingBox();
  if (!box) return { added: false, reason: 'no-canvas', boxSeen: false, preCount: null, postCount: null };
  const pre = (await portfolioCount(page)).effective;
  const preGlobalAddEnabled = (await addEnabled(page)).enabled;
  const x = box.x + box.width * fracX;
  const y = box.y + box.height * fracY;
  await page.mouse.click(x, y);
  await W(page, 1400);
  const result = await page.evaluate(({ px, py, fieldReSrc }) => {
    const c = (t) => (t || '').trim().replace(/\s+/g, ' ');
    const fieldRe = new RegExp(fieldReSrc, 'i');
    const vis = (el) => {
      const r = el.getBoundingClientRect();
      const s = getComputedStyle(el);
      return r.width > 0 && r.height > 0 && s.pointerEvents !== 'none';
    };
    // A map popover = a SMALL floating box carrying the district/coordinate tooltip
    // fields AND an "Add to Analysis" control INSIDE it (the click tooltip's CTA,
    // per us13). Pick the smallest such box; record its District field for scoping.
    const cands = [...document.querySelectorAll('div,section,aside,[role="tooltip"]')].filter((el) => {
      const r = el.getBoundingClientRect();
      if (!vis(el) || r.width < 60 || r.width > 640 || r.height < 40 || r.height > 640) return false;
      const t = el.innerText || '';
      return fieldRe.test(t) && /Add to Analysis/i.test(t);
    });
    if (!cands.length) return { boxSeen: false, added: false, popoverText: '' };
    cands.sort((a, b) => { const ra = a.getBoundingClientRect(); const rb = b.getBoundingClientRect(); return (ra.width * ra.height) - (rb.width * rb.height); });
    const pop = cands[0];
    const addBtn = [...pop.querySelectorAll('button,[role="button"]')].find((b) => /Add to Analysis/i.test(b.textContent) && !b.disabled && vis(b));
    const popoverText = c(pop.innerText).slice(0, 300);
    const dm = (popoverText.match(/District\s+([A-Za-z .()'-]+?)\s+State/i) || [])[1] || null;
    if (addBtn) { addBtn.click(); return { boxSeen: true, added: true, popoverText, popoverDistrict: dm }; }
    return { boxSeen: true, added: false, popoverText, popoverDistrict: dm };
  }, { px: x, py: y, fieldReSrc: INFO_FIELD_RE });
  if (result.added) await W(page, 1600);
  await expandPanel(page);
  const post = (await portfolioCount(page)).effective;
  return {
    added: result.added && post === pre + 1,
    reason: result.boxSeen ? (result.added ? 'scoped-add' : 'no-add-in-popover') : 'no-map-popover',
    boxSeen: result.boxSeen,
    popoverText: result.popoverText,
    popoverDistrict: result.popoverDistrict || null,
    preCount: pre,
    postCount: post,
    preGlobalAddEnabled,
  };
}

/**
 * Sweep an n x n canvas grid via interactionProbe. Returns per-point serializable
 * records, the live-point set, and distinct reachable districts.
 */
async function sweepGrid(page, n, mode, roster, bbox = { x0: 0, y0: 0, x1: 1, y1: 1 }) {
  const points = [];
  const livePoints = [];
  const districts = new Set();
  const { x0, y0, x1, y1 } = bbox;
  for (let i = 0; i < n; i++) {
    for (let j = 0; j < n; j++) {
      const fracX = x0 + ((i + 0.5) / n) * (x1 - x0);
      const fracY = y0 + ((j + 0.5) / n) * (y1 - y0);
      const s = await interactionProbe(page, fracX, fracY, mode, roster);
      const rec = { fracX: Number(fracX.toFixed(3)), fracY: Number(fracY.toFixed(3)), surfaced: s.surfaced, district: s.district, parentDistrict: s.district };
      points.push(rec);
      if (s.surfaced) { livePoints.push({ fracX: rec.fracX, fracY: rec.fracY }); if (s.district) districts.add(s.district); }
      await clearMapPopover(page);
    }
  }
  return { points, livePoints, distinctDistricts: [...districts] };
}

/**
 * Derive the interactive (state) bounding box in canvas fracs from a coarse
 * sweep's live points, padded and clamped. Focusing subsequent sweeps here (vs.
 * the full canvas, ~75% of which is off-state basemap) concentrates samples on
 * the choropleth so live-point counts reflect real interactivity, not geometry.
 */
function deriveLiveBBox(coarse, pad = 0.06) {
  const lp = coarse.livePoints;
  if (lp.length < 3) return { x0: 0, y0: 0, x1: 1, y1: 1, focused: false };
  const xs = lp.map((p) => p.fracX); const ys = lp.map((p) => p.fracY);
  const clamp = (v) => Math.max(0, Math.min(1, v));
  return {
    x0: clamp(Math.min(...xs) - pad), y0: clamp(Math.min(...ys) - pad),
    x1: clamp(Math.max(...xs) + pad), y1: clamp(Math.max(...ys) + pad),
    focused: true,
  };
}

/** Overlap fraction of two live-point sets (by frac coordinate key). */
function overlapFraction(a, b) {
  if (!a.length) return 0;
  const key = (p) => `${p.fracX},${p.fracY}`;
  const bset = new Set(b.map(key));
  const hit = a.filter((p) => bset.has(key(p))).length;
  return hit / a.length;
}

// ===========================================================================
// Automated summary writer (part of CHG-0225 — NOT the vendor report CHG-0226).
// ===========================================================================
function writeAutomatedSummary(run) {
  const p = run.probe || {};
  const g = p.gates || {};
  const lines = [];
  lines.push('# Automated summary — map interactivity probe (machine-generated)');
  lines.push('');
  lines.push('> NOT a vendor report. CHG-0226 (`qa/reports/us-map-interactivity.md`) is authored by hand');
  lines.push('> after a human triages this evidence. Do not forward this file as findings.');
  lines.push('');
  lines.push(`- Run: \`${run.id}\``);
  lines.push(`- App: ${run.appUrl}`);
  lines.push(`- QA_SOFTWARE_GL: ${process.env.QA_SOFTWARE_GL === '1' ? 'on' : 'off'}`);
  lines.push('');
  lines.push('## Gates');
  for (const [k, v] of Object.entries(g)) {
    lines.push(`- **${k}**: ${v && v.passed ? 'PASS' : `BLOCKED — ${v && v.reason ? v.reason : 'n/a'}`}${v && v.detail ? ` _(${typeof v.detail === 'string' ? v.detail : JSON.stringify(v.detail)})_` : ''}`);
  }
  lines.push('');
  lines.push('## Calibration');
  lines.push('```json');
  lines.push(JSON.stringify(p.calibration || {}, null, 2));
  lines.push('```');
  lines.push('');
  lines.push('## Claim 1 (dropdown gates interactivity)');
  lines.push('```json');
  lines.push(JSON.stringify(p.claim1 || { status: 'not-run' }, null, 2));
  lines.push('```');
  lines.push('');
  lines.push(`## Claim 2 (commutativity) — coverage: ${p.claim2Coverage || 'n/a'}`);
  lines.push('```json');
  lines.push(JSON.stringify(p.claim2 || { status: 'not-run' }, null, 2));
  lines.push('```');
  lines.push('');
  writeFileSync(join(run.dir, 'automated-summary.md'), lines.join('\n'));
  run.artifacts.push('automated-summary.md');
}

// ===========================================================================
await withSession(async (page) => {
  const run = createRun('us-map-interactivity');
  attachCollectors(page, run);
  run.probe = {
    gates: {},
    calibration: {},
    normalizedDistrictMode: null,
    claim1: { status: 'not-run' },
    claim2: { status: 'not-run' },
    claim2Coverage: 'n/a',
  };
  const P = run.probe;

  await installSurveyKiller(page);
  await page.goto(APP_URL, { waitUntil: 'domcontentloaded' });
  await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => {});
  await W(page, 1500);
  await killSurvey(page);
  await selectState(page, 'Telangana');

  // -----------------------------------------------------------------------
  // PHASE 0 — Gate stateNormalized.
  // -----------------------------------------------------------------------
  console.log('\n== PHASE 0 — normalize + gate stateNormalized ==');
  let roster = [];
  const g0 = await safe(run, page, 'Gate stateNormalized', async () => {
    const norm = await clearGeographyToStateWide(page);
    await shot(page, run, 'p0-normalized');
    P.normalizedDistrictMode = norm.normalizedDistrictMode;
    roster = await readDistrictOptions(page);
    P.gates.stateNormalized = {
      passed: norm.stateNormalized,
      detail: { districtMode: norm.normalizedDistrictMode, portfolioEmpty: norm.portfolioEmpty, popoverLeft: norm.popoverLeft, resetControl: norm.resetControl, rosterSize: roster.length },
    };
    if (!norm.stateNormalized) throw new Error(`stateNormalized failed: ${JSON.stringify(P.gates.stateNormalized.detail)}`);
    return `mode=${norm.normalizedDistrictMode}, portfolioEmpty=${norm.portfolioEmpty}, roster=${roster.length}, reset=${norm.resetControl ? norm.resetControl.text : '(none)'}`;
  });

  // -----------------------------------------------------------------------
  // PHASE 1 — Gate interactionCalibrated (hover first, then click).
  // -----------------------------------------------------------------------
  console.log('\n== PHASE 1 — calibrate interaction (hover -> click) ==');
  let CALIB = { mode: null };
  let STATE_BBOX = { x0: 0, y0: 0, x1: 1, y1: 1, focused: false };
  const g1 = g0 && await safe(run, page, 'Gate interactionCalibrated', async () => {
    const probeCorners = [[0.5, 0.5], [0.35, 0.45], [0.6, 0.55], [0.45, 0.6], [0.55, 0.4]];
    for (const mode of ['hover', 'click']) {
      for (const [fx, fy] of probeCorners) {
        const s = await interactionProbe(page, fx, fy, mode, roster);
        await clearMapPopover(page);
        if (s.surfaced) { CALIB = { mode, sample: { fracX: fx, fracY: fy, district: s.district, hint: s.selectorHint } }; break; }
      }
      if (CALIB.mode) break;
    }
    P.calibration = CALIB;
    P.gates.interactionCalibrated = { passed: !!CALIB.mode, reason: CALIB.mode ? null : 'no district surfaced via hover OR click (GL/headless — try headed/xvfb)', detail: CALIB.sample || null };
    await shot(page, run, 'p1-calibrate');
    if (!CALIB.mode) throw new Error('interactionCalibrated failed: neither hover nor click surfaced a district');
    return `mode=${CALIB.mode}; sample=${JSON.stringify(CALIB.sample)}`;
  });

  // -----------------------------------------------------------------------
  // PHASE 1.5 — Gate mapPopoverScopedAdd (positive smoke test + cleanup).
  // -----------------------------------------------------------------------
  console.log('\n== PHASE 1.5 — scoped map-add smoke test (+ cleanup) ==');
  const g15 = g1 && await safe(run, page, 'Gate mapPopoverScopedAdd', async () => {
    // Pick a calibrated live baseline point.
    const base = CALIB.sample ? [CALIB.sample.fracX, CALIB.sample.fracY] : [0.5, 0.5];
    const before = (await portfolioCount(page)).effective;
    const add = await mapAddScoped(page, base[0], base[1]);
    await shot(page, run, 'p15-scoped-add');
    P.gates.mapPopoverScopedAdd = { passed: add.added && add.postCount === before + 1, reason: add.added ? null : add.reason, detail: { before, ...add, popoverText: undefined } };
    if (!(add.added && add.postCount === before + 1)) {
      throw new Error(`mapPopoverScopedAdd failed: reason=${add.reason}, ${before}->${add.postCount}`);
    }
    // Cleanup: remove the smoke-test item + clear popover + re-assert empty (#1).
    await resetPortfolio(page);
    await clearMapPopover(page);
    const after = (await portfolioCount(page)).effective;
    if (after !== 0) throw new Error(`smoke-test cleanup failed: portfolio not empty (effective=${after})`);
    return `scoped add proved (${before}->${add.postCount}); cleaned back to 0`;
  });

  // -----------------------------------------------------------------------
  // PHASE 2 — Claim 1: dropdown gates interactivity.
  // -----------------------------------------------------------------------
  if (g15) {
    console.log('\n== PHASE 2 — Claim 1 (dropdown gates interactivity) ==');
    await safe(run, page, 'Claim 1', async () => {
      // 2a baseline sweep (state-wide): coarse full-canvas pass -> focus on the
      // state's live extent -> dense baseline sweep there.
      await clearGeographyToStateWide(page);
      const coarse = await sweepGrid(page, TH.GRID_N, CALIB.mode, roster);
      STATE_BBOX = deriveLiveBBox(coarse);
      const base = await sweepGrid(page, TH.GRID_N, CALIB.mode, roster, STATE_BBOX);
      await shot(page, run, 'p2a-baseline-sweep');
      const L0 = base.livePoints; const D0 = base.distinctDistricts;
      if (L0.length < TH.L0_LIVE_MIN || D0.length < TH.D0_DISTINCT_MIN) {
        P.claim1 = { status: 'BLOCKED', reason: `baseline too sparse (live=${L0.length}<${TH.L0_LIVE_MIN} or distinct=${D0.length}<${TH.D0_DISTINCT_MIN})`, L0: L0.length, D0 };
        return `BLOCKED — baseline live=${L0.length}, distinct=${D0.length}`;
      }
      // 2b select one district, re-sweep, classify.
      const pick = D0[0];
      const sigBefore = await viewSignature(page);
      await selectDistrictExact(page, pick);
      const blockRoster = await discoverBlockRoster(page);
      const sigAfter = await viewSignature(page);
      const viewDelta = sigBefore !== sigAfter;
      const after = await sweepGrid(page, TH.GRID_N, CALIB.mode, roster, STATE_BBOX);
      await shot(page, run, 'p2b-selected-sweep');
      const L1 = after.livePoints; const D1 = after.distinctDistricts;
      // Classify each post-selection point.
      const classes = { sameDistrict: 0, differentDistrictOrBlock: 0, inert: 0, offCanvasOrViewportShift: 0 };
      let outsideSelectedLive = 0;
      for (const pt of after.points) {
        const cls = classifyTarget(pt, pick, D0, blockRoster.blocks, viewDelta);
        classes[cls]++;
        if (pt.surfaced && (pt.district || '').toLowerCase() !== pick.toLowerCase()) outsideSelectedLive++;
      }
      const viewChanged = classes.differentDistrictOrBlock > 0 || classes.offCanvasOrViewportShift > 0;
      const overlap = overlapFraction(L0, L1);
      const gating = outsideSelectedLive <= TH.OUTSIDE_SELECTED_MAX && !viewChanged;
      // 2c click analogue on a non-selected point (best-effort).
      let clickAnalogue = null;
      const nonSel = base.points.find((pt) => pt.surfaced && (pt.district || '').toLowerCase() !== pick.toLowerCase());
      if (nonSel) {
        const s = await interactionProbe(page, nonSel.fracX, nonSel.fracY, 'click', roster);
        clickAnalogue = { fracX: nonSel.fracX, fracY: nonSel.fracY, surfaced: s.surfaced, district: s.district, class: classifyTarget(s, pick, D0, blockRoster.blocks, viewDelta) };
        await clearMapPopover(page);
      }
      // 2d reversibility.
      await clearGeographyToStateWide(page);
      const rev = await sweepGrid(page, TH.GRID_N, CALIB.mode, roster, STATE_BBOX);
      await shot(page, run, 'p2d-reversibility');
      const reverted = overlapFraction(L0, rev.livePoints) >= TH.OVERLAP_MIN;

      let verdict;
      if (gating && reverted && !viewChanged) verdict = 'CONFIRMED (interactivity-only gating, reversible; no view change)';
      else if (gating && reverted && viewChanged) verdict = 'CONFIRMED (gating via reversible drill-down; view changed)';
      else if (gating && !reverted) verdict = 'CONFIRMED (gating, HARD lock-out — not reversible)';
      else if (viewChanged) verdict = 'NOT-REPRODUCED (view-change/drill-down, not interactivity lock)';
      else verdict = 'NOT-REPRODUCED (map stays broadly live after district pick)';

      P.claim1 = {
        status: 'RESULT',
        verdict,
        selectedDistrict: pick,
        L0: L0.length, D0, L1: L1.length, D1,
        overlapL0L1: Number(overlap.toFixed(2)),
        classes, outsideSelectedLive, viewChanged, viewDelta,
        blockRoster, clickAnalogue,
        reversible: reverted,
      };
      return `${verdict}; outsideLive=${outsideSelectedLive}, viewChanged=${viewChanged}, overlap=${overlap.toFixed(2)}, reversible=${reverted}`;
    });

    // ---------------------------------------------------------------------
    // PHASE 3 — Claim 2: commutativity.
    // ---------------------------------------------------------------------
    console.log('\n== PHASE 3 — Claim 2 (commutativity) ==');
    await safe(run, page, 'Claim 2', async () => {
      await clearGeographyToStateWide(page);
      if (!STATE_BBOX || !STATE_BBOX.focused) {
        const coarse = await sweepGrid(page, TH.GRID_N, CALIB.mode, roster);
        STATE_BBOX = deriveLiveBBox(coarse);
      }
      const base = await sweepGrid(page, TH.GRID_N, CALIB.mode, roster, STATE_BBOX);
      const D0 = base.distinctDistricts;
      const full = D0.length >= TH.CLAIM2_DISTINCT_MIN;
      P.claim2Coverage = full ? 'full' : 'reduced';
      const N = full ? 3 : Math.max(1, Math.floor(D0.length / 2));
      if (D0.length < 2 || base.livePoints.length < 2) {
        P.claim2 = { status: 'BLOCKED', reason: `too few reachable districts/points (distinct=${D0.length})`, coverage: P.claim2Coverage };
        return `BLOCKED — distinct=${D0.length}`;
      }
      // Map-side points (A,B,C) = first N live points in distinct districts.
      const mapPts = [];
      const seen = new Set();
      for (const pt of base.points) {
        if (pt.surfaced && pt.district && !seen.has(pt.district)) { seen.add(pt.district); mapPts.push(pt); }
        if (mapPts.length >= N) break;
      }
      // Admin-side districts (D,E,F) = distinct districts NOT used by mapPts.
      const adminDistricts = D0.filter((d) => !seen.has(d)).slice(0, N);

      // --- P1 map-first ---
      await clearGeographyToStateWide(page);
      const p1trail = [];
      let p1MapAdds = 0;
      for (const pt of mapPts) {
        const a = await mapAddScoped(page, pt.fracX, pt.fracY);
        if (a.added) p1MapAdds++;
        p1trail.push(`map(${pt.district})=${a.postCount}`);
        await clearMapPopover(page);
      }
      const p1AfterMap = (await portfolioCount(page)).effective;
      for (const d of adminDistricts) {
        try { const r = await addViaAdminExact(page, d); p1trail.push(`admin(${d})=${r.postCount}`); } catch (e) { p1trail.push(`admin(${d})=ERR:${String(e.message).slice(0, 60)}`); }
      }
      const p1Final = (await portfolioCount(page)).effective;
      await shot(page, run, 'p3-p1-map-first');

      // --- P2 dropdown-first ---
      await clearGeographyToStateWide(page);
      const p2trail = [];
      for (const d of adminDistricts) {
        try { const r = await addViaAdminExact(page, d); p2trail.push(`admin(${d})=${r.postCount}`); } catch (e) { p2trail.push(`admin(${d})=ERR:${String(e.message).slice(0, 60)}`); }
      }
      const p2AfterAdmin = (await portfolioCount(page)).effective;
      let p2MapAdds = 0;
      const p2MapClasses = [];
      const sigBefore = await viewSignature(page);
      for (const pt of mapPts) {
        // Validate the target still surfaces the intended district (not view-changed).
        const s = await interactionProbe(page, pt.fracX, pt.fracY, CALIB.mode, roster);
        const cls = classifyTarget(s, adminDistricts[0] || '', D0, [], sigBefore !== (await viewSignature(page)));
        p2MapClasses.push({ district: pt.district, surfaced: s.surfaced, class: cls });
        await clearMapPopover(page);
        const a = await mapAddScoped(page, pt.fracX, pt.fracY);
        if (a.added) p2MapAdds++;
        p2trail.push(`map(${pt.district})=${a.postCount}[${a.reason}]`);
        await clearMapPopover(page);
      }
      const p2Final = (await portfolioCount(page)).effective;
      await shot(page, run, 'p3-p2-dropdown-first');

      const commutative = p1Final === p2Final;
      let mechanism;
      if (commutative) mechanism = 'commutative (no defect: same final count both orders)';
      else if (p2MapClasses.some((c) => c.class === 'differentDistrictOrBlock' || c.class === 'offCanvasOrViewportShift')) mechanism = 'targetInvalidatedByViewChange (dropdown drill-down moved/hid the map targets — not a hard lock-out)';
      else if (p2MapAdds === 0 && p1MapAdds > 0) mechanism = 'dropdown-gating / lock-out (map adds succeed map-first, blocked dropdown-first with targets still inert)';
      else mechanism = 'mode-switch-wipe or partial loss (per-step trail disambiguates owner)';

      P.claim2 = {
        status: 'RESULT',
        coverage: P.claim2Coverage,
        N,
        mapDistricts: mapPts.map((p) => p.district),
        adminDistricts,
        p1: { afterMap: p1AfterMap, mapAdds: p1MapAdds, final: p1Final, trail: p1trail },
        p2: { afterAdmin: p2AfterAdmin, mapAdds: p2MapAdds, final: p2Final, trail: p2trail, mapTargetClasses: p2MapClasses },
        commutative,
        mechanism,
      };
      return `coverage=${P.claim2Coverage}; P1final=${p1Final} vs P2final=${p2Final}; ${mechanism}`;
    });
  } else {
    step(run, 'GATE: Claims skipped', false, 'One of stateNormalized / interactionCalibrated / mapPopoverScopedAdd did not pass; Claim verdicts would be unsound. See run.probe.gates.');
  }

  // Cross-cutting evidence + summary.
  await dumpDom(page, run, 'map-interactivity-final');
  await snapshot(page, run, 'map-interactivity-responsive');
  await runAxe(page, run, 'map-interactivity');
  writeAutomatedSummary(run);
  finalize(run);

  console.log(`\n  Run: ${run.dir}`);
  console.log(`  Gates: ${JSON.stringify(Object.fromEntries(Object.entries(P.gates).map(([k, v]) => [k, v && v.passed])))}`);
  console.log(`  Claim 1: ${P.claim1.verdict || P.claim1.status}`);
  console.log(`  Claim 2: ${P.claim2.mechanism || P.claim2.status} (coverage=${P.claim2Coverage})`);
  console.log(`  Steps failed: ${run.summary.stepsFailed} | real errors: ${run.summary.errorEvents} | benign: ${run.summary.benignEvents}`);
});
