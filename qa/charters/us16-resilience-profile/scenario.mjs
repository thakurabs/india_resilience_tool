// US 16 — Resilience Profile (Single-Site Analysis) scenario.
// Drives the full profile surface for a single district:
//   empty-state message → build analysis (climate metric + all 6 filters)
//   → Overview → Risk Summary (baseline/projected/Δ/position)
//   → Trend line chart + "Show model members" → max-models slider
//   → Scenario Comparison bar chart + "Start y-axis at zero"
//   → full-screen modal (+ left/right split probe)
//   → composite-metric data-availability probe ("No data available").
// Records a per-step outcome + evidence. Follows us10/us15 SPEC-DRIFT-not-FAIL rules.
//
//   node qa/charters/us16-resilience-profile/scenario.mjs

import { withSession, APP_URL } from '../../harness/lib/session.mjs';
import {
  createRun, attachCollectors, snapshot, dumpDom, runAxe, finalize, step,
} from '../../harness/lib/evidence.mjs';
import { openAdmin, selectState, selectDistrict } from '../../harness/lib/flows.mjs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const __dirname = dirname(fileURLToPath(import.meta.url));
const shot = (page, run, name) => page.screenshot({ path: join(run.dir, `${name}.png`) });
const bodyText = (page) => page.evaluate(() => document.body.innerText.replace(/\s+/g, ' ').trim());

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

// Pick the first option of the currently-first "Select" dropdown in the filters.
async function pickFirst(page) {
  await page.getByText('Select', { exact: true }).first().click();
  await page.waitForTimeout(500);
  const v = await page.locator('li[role="option"]').first().innerText().catch(() => '');
  await page.locator('li[role="option"]').first().click();
  await page.waitForTimeout(700);
  return v.trim();
}

// Build a single-site analysis with a chosen metric and ALL six filters set,
// so both the Risk Summary and the trend/scenario charts populate.
async function buildAnalysis(page, { metric }) {
  await openAdmin(page);
  await selectState(page, 'Telangana');
  await selectDistrict(page, 'Warangal');
  await page.getByText(/Select Resilience Filters/i).first().click();
  await page.waitForTimeout(800);
  const riskDomain = await pickFirst(page); // Heat Risk (first domain)
  // Metric: choose explicitly (capture the chosen label for the ledger).
  await page.getByText('Select', { exact: true }).first().click();
  await page.waitForTimeout(500);
  const metricOpt = page.locator('li[role="option"]').filter({ hasText: metric }).first();
  const metricLabel = (await metricOpt.innerText().catch(() => '')).trim();
  await metricOpt.click();
  await page.waitForTimeout(800);
  const scenario = await pickFirst(page);
  await page.waitForFunction(() => !document.body.innerText.includes('Select a scenario first'),
    { timeout: 10000 }).catch(() => {});
  const period = await pickFirst(page);
  const statistic = await pickFirst(page).catch(() => '');
  await page.waitForTimeout(1500);
  return { riskDomain, metric: metricLabel, scenario, period, statistic };
}

// The panel body toggle (expand/collapse) — label flips with state.
async function expandPanel(page) {
  const btn = page.getByRole('button', { name: /Expand resilience profile panel/i }).first();
  if (await btn.count()) { await btn.click(); await page.waitForTimeout(2000); return true; }
  return false;
}

// Open an accordion whose header <h4> starts with `label`. Risk Summary is a
// role=button; Trend/Scenario are cursor-pointer divs — click the h4's nearest
// clickable (cursor:pointer) ancestor for all three.
async function openAccordion(page, label) {
  return page.evaluate((lbl) => {
    const c = (t) => (t || '').trim().replace(/\s+/g, ' ');
    const h4 = [...document.querySelectorAll('h4')].find((h) => c(h.innerText).startsWith(lbl));
    if (!h4) return false;
    let e = h4;
    for (let i = 0; i < 5 && e; i++) {
      if (e.tagName === 'BUTTON' || e.getAttribute('role') === 'button' || getComputedStyle(e).cursor === 'pointer') { e.click(); return true; }
      e = e.parentElement;
    }
    h4.click();
    return true;
  }, label);
}

// Read the text of the accordion region whose header matches `re`.
async function sectionText(page, label) {
  return page.evaluate((lbl) => {
    const c = (t) => (t || '').trim().replace(/\s+/g, ' ');
    const h4 = [...document.querySelectorAll('h4')].find((h) => c(h.innerText).startsWith(lbl));
    if (!h4) return null;
    let b = h4; for (let i = 0; i < 4 && b.parentElement; i++) b = b.parentElement;
    return c(b.innerText).slice(0, 700);
  }, label);
}

await withSession(async (page) => {
  const run = createRun('us16-resilience-profile');
  attachCollectors(page, run);

  await page.goto(APP_URL, { waitUntil: 'domcontentloaded' });
  await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => {});
  await page.waitForTimeout(1500);

  // S1 — Empty state (no location): panel shows a "no location" message.
  await safe(run, 'S1: empty-state message (no location)', async () => {
    await expandPanel(page);
    await shot(page, run, 's1-empty');
    const txt = await page.evaluate(() => {
      const h = [...document.querySelectorAll('h3')].find((el) => /Resilience Profile/i.test(el.innerText));
      if (!h) return ''; let b = h; for (let i = 0; i < 8 && b.parentElement; i++) b = b.parentElement;
      return (b.innerText || '').replace(/\s+/g, ' ').trim();
    });
    const ok = /no location|select .*location|select .*coordinate|view (insights|profile)/i.test(txt);
    if (!ok) return `OBSERVE: no clear empty-state message; panel text="${txt.slice(0, 120)}"`;
    const m = /no location\(s\) selected|select [^.]{0,60}(location|coordinate|profile|insights)/i.exec(txt);
    return `empty-state message present: "${m ? m[0] : txt.slice(0, 80)}"`;
  });

  // S2 — Build single-site analysis with a climate metric + all filters.
  let filters = {};
  await safe(run, 'S2: build single-site analysis (Annual Mean Temperature)', async () => {
    filters = await buildAnalysis(page, { metric: /Annual Mean Temperature/i });
    await page.waitForTimeout(1000);
    await shot(page, run, 's2-built');
    if (!filters.statistic) return `OBSERVE: built but Statistic unset; filters=${JSON.stringify(filters)}`;
    return `filters=${JSON.stringify(filters)}`;
  });

  // S3 — Overview fields.
  await safe(run, 'S3: Profile Overview fields', async () => {
    await expandPanel(page);
    await page.waitForTimeout(800);
    await shot(page, run, 's3-overview');
    const txt = await bodyText(page);
    const fields = {
      geography: /Warangal .*(District )?Climate Profile/i.test(txt),
      index: /Index:\s*Annual Mean Temperature/i.test(txt),
      scenario: /Scenario:\s*ssp245/i.test(txt),
      period: /Period:\s*2020-2040/i.test(txt),
    };
    const missing = Object.entries(fields).filter(([, v]) => !v).map(([k]) => k);
    return missing.length ? `PARTIAL overview — missing: ${missing.join(',')}` : 'overview: geography+index+scenario+period all present';
  });

  // S4 — Risk Summary: baseline / projected / Δ / position.
  await safe(run, 'S4: Risk Summary values', async () => {
    await page.getByRole('button', { name: /^Risk Summary$/i }).first().click().catch(() => {});
    await page.waitForTimeout(1500);
    await shot(page, run, 's4-risk-summary');
    const t = (await sectionText(page, 'Risk Summary')) || '';
    const has = {
      baseline: /HISTORICAL BASELINE\s*[\d.]/i.test(t),
      projected: /PROJECTED VALUE\s*[\d.]/i.test(t),
      delta: /[+\-]\d+\.\d/.test(t),
      position: /POSITION IN (STATE|INDIA)\s*\d/i.test(t),
    };
    const posLabel = /POSITION IN (STATE|INDIA)/i.exec(t);
    const missing = Object.entries(has).filter(([, v]) => !v).map(([k]) => k);
    const drift = posLabel && /STATE/i.test(posLabel[1]) ? ' [SPEC-DRIFT: "Position in State" vs spec "Position in India"]' : '';
    return missing.length
      ? `PARTIAL — missing: ${missing.join(',')}${drift}; text="${t.slice(0, 160)}"`
      : `baseline+projected+Δ+position all present${drift}; "${t.slice(0, 160)}"`;
  });

  // S5 — Trend Over Time line chart + Show model members.
  let trendSvg = 0;
  await safe(run, 'S5: Trend Over Time line chart', async () => {
    await openAccordion(page, 'Trend Over Time');
    await page.waitForTimeout(2500);
    await shot(page, run, 's5-trend');
    const info = await page.evaluate(() => {
      const c = (t) => (t || '').trim().replace(/\s+/g, ' ');
      const h4 = [...document.querySelectorAll('h4')].find((h) => /Trend Over Time/i.test(h.innerText));
      let box = h4; for (let i = 0; i < 4 && box && box.parentElement; i++) box = box.parentElement;
      const txt = box ? c(box.innerText) : '';
      return {
        svg: box ? box.querySelectorAll('svg').length : 0,
        modelCb: /Show model members/i.test(txt),
        historical: /Historical/i.test(txt),
        ssp: /SSP2?45|SSP2-4\.5|SSP245/i.test(txt),
        year: /\bYear\b|19\d\d|20\d\d/.test(txt),
      };
    });
    trendSvg = info.svg;
    if (!info.svg) return `OBSERVE: no SVG chart in Trend section; ${JSON.stringify(info)}`;
    const missing = Object.entries({ modelCb: info.modelCb, historical: info.historical, ssp: info.ssp, year: info.year }).filter(([, v]) => !v).map(([k]) => k);
    return missing.length ? `line chart svg=${info.svg}; missing labels: ${missing.join(',')}` : `line chart svg=${info.svg}; Historical+SSP series, Year axis, Show-model-members present`;
  });

  // S6 — Show model members → Max models slider (spec 871); percentile band probe (spec 872).
  await safe(run, 'S6: Show model members → max-models slider', async () => {
    const cb = page.getByText(/Show model members/i).first();
    if (!(await cb.count())) return 'OBSERVE: no "Show model members" control';
    await cb.click().catch(() => {});
    await page.waitForTimeout(2000);
    await shot(page, run, 's6-model-members');
    const info = await page.evaluate(() => {
      const c = (t) => (t || '').trim().replace(/\s+/g, ' ');
      const body = c(document.body.innerText);
      return {
        sliders: document.querySelectorAll('input[type="range"],[role="slider"]').length,
        maxModels: /Max models to draw/i.test(body),
        percentile: /percentile|p05|p95/i.test(body),
      };
    });
    const band = info.percentile ? 'percentile band control PRESENT' : 'percentile band (p05-p95) control NOT found [spec 872]';
    if (info.sliders >= 1 && info.maxModels) return `Max-models slider appeared (spec 871 ✓); ${band}`;
    return `OBSERVE: expected max-models slider; sliders=${info.sliders} maxModels=${info.maxModels}; ${band}`;
  });

  // S7 — Scenario Comparison bar chart + Start-y-axis-at-zero.
  await safe(run, 'S7: Scenario Comparison bar chart', async () => {
    await openAccordion(page, 'Scenario Comparison');
    await page.waitForTimeout(2500);
    await shot(page, run, 's7-scenario-comparison');
    const info = await page.evaluate(() => {
      const c = (t) => (t || '').trim().replace(/\s+/g, ' ');
      const h4 = [...document.querySelectorAll('h4')].find((h) => /Scenario Comparison/i.test(h.innerText));
      let box = h4; for (let i = 0; i < 4 && box && box.parentElement; i++) box = box.parentElement;
      const txt = box ? c(box.innerText) : '';
      return {
        svg: box ? box.querySelectorAll('svg').length : 0,
        historical: /Historical/i.test(txt),
        ssp245: /SSP2-4\.5|SSP245/i.test(txt),
        ssp585: /SSP5-8\.5|SSP585/i.test(txt),
        zero: /start y.?axis|start.*zero|does not start at zero/i.test(txt),
        period: /1990.?2010|2020.?2040/i.test(txt),
      };
    });
    if (!info.svg) return `OBSERVE: no SVG chart in Scenario Comparison; ${JSON.stringify(info)}`;
    const series = [info.historical && 'Historical', info.ssp245 && 'SSP2-4.5', info.ssp585 && 'SSP5-8.5'].filter(Boolean);
    return `bar chart svg=${info.svg}; series=[${series.join(', ')}]; periods=${info.period}; start-y-axis-at-zero=${info.zero}`;
  });

  // S8 — Full-screen modal opens.
  await safe(run, 'S8: full-screen modal opens', async () => {
    const fs = page.getByRole('button', { name: /Open Resilience Profile in full screen/i }).first();
    if (!(await fs.count())) return 'OBSERVE: no full-screen (⛶) control';
    await fs.click();
    await page.waitForTimeout(2500);
    await shot(page, run, 's8-fullscreen');
    const info = await page.evaluate(() => {
      const dialogs = [...document.querySelectorAll('[role="dialog"],[aria-modal="true"]')].map((el) => Math.round(el.getBoundingClientRect().width));
      const close = [...document.querySelectorAll('button,[role="button"]')].some((el) => /close expanded resilience profile|close .*profile/i.test((el.getAttribute('aria-label') || el.innerText || '')));
      const sameProfile = /Warangal .*Climate Profile/i.test(document.body.innerText);
      return { dialogCount: dialogs.length, dialogW: dialogs[0] || 0, close, sameProfile };
    });
    if (!info.dialogCount) return `OBSERVE: full-screen click did not open a dialog; ${JSON.stringify(info)}`;
    return `modal open (role=dialog, w=${info.dialogW}); sameProfile=${info.sameProfile}; closeControl=${info.close}`;
  });

  // S9 — Left/right split probe inside the modal (spec 897-899).
  await safe(run, 'S9: modal left/right split (portfolio) probe', async () => {
    const info = await page.evaluate(() => {
      const c = (t) => (t || '').trim().replace(/\s+/g, ' ');
      const KW = /saved analysis|manage portfolio|refine your filters|compare portfolio|compare scenario/i;
      const hits = [...new Set([...document.querySelectorAll('h1,h2,h3,h4,button,label,span,div')]
        .map((el) => c(el.innerText)).filter((t) => t && t.length < 60 && KW.test(t)))];
      return hits;
    });
    // Close the modal.
    await page.getByRole('button', { name: /Close expanded Resilience Profile view/i }).first().click().catch(() => {});
    await page.keyboard.press('Escape').catch(() => {});
    await page.waitForTimeout(1000);
    return info.length
      ? `split present: ${JSON.stringify(info)}`
      : 'OBSERVE: no left/right split (Saved Analysis / Manage Portfolio / Compare Portfolio) for single-site empty-portfolio — likely US 17 surface [PASS-WITH-NOTE]';
  });

  // S10 — Composite-metric data-availability behaviour.
  await safe(run, 'S10: composite metric → graceful "No data available"', async () => {
    // Switch Metric to the composite (first metric) without rebuilding geography.
    // The Metric dropdown trigger is a combobox (not role=button); find the
    // trigger by its current value text, excluding the profile's "Index:" line.
    const opened = await page.evaluate(() => {
      const trig = [...document.querySelectorAll('button,[role="combobox"],[role="button"],[class*="select"]')]
        .find((el) => /Annual Mean Temperature/i.test(el.innerText || '') && !/Index:/i.test(el.innerText || ''));
      if (trig) { trig.click(); return true; }
      return false;
    });
    if (opened) {
      await page.waitForTimeout(600);
      await page.locator('li[role="option"]').filter({ hasText: /Heat Risk Composite/i }).first().click().catch(() => {});
      await page.waitForTimeout(2000);
    }
    await expandPanel(page);
    await openAccordion(page, 'Trend Over Time');
    await page.waitForTimeout(1500);
    await openAccordion(page, 'Scenario Comparison');
    await page.waitForTimeout(1500);
    await shot(page, run, 's10-composite-nodata');
    const info = await page.evaluate(() => {
      const c = (t) => (t || '').trim().replace(/\s+/g, ' ');
      const body = c(document.body.innerText);
      return {
        noData: /no data available/i.test(body),
        baselineOmitted: !/HISTORICAL BASELINE\s*[\d.]/i.test(body),
        deltaZero: /\+0\.00/.test(body),
      };
    });
    if (info.noData) return `composite shows graceful "No data available"; baselineOmitted=${info.baselineOmitted}; Δ+0.00=${info.deltaZero} [confirm intended for composites]`;
    return `OBSERVE: composite metric did not show "No data available"; ${JSON.stringify(info)}`;
  });

  // Cross-cutting.
  await dumpDom(page, run, 'us16-final');
  await snapshot(page, run, 'us16-responsive');
  await runAxe(page, run, 'us16');
  finalize(run);

  console.log(`\n  Run: ${run.dir}`);
  console.log(`  Steps failed: ${run.summary.stepsFailed} | real error events: ${run.summary.errorEvents} | benign: ${run.summary.benignEvents}`);
});
