// Evidence-capture helpers shared by every QA charter scenario.
//
// The harness is deliberately "dumb but thorough": it drives the browser and
// records everything a downstream reviewer (a Haiku subagent) or a human might
// need to judge pass/fail — console errors, failed/HTTP-error requests,
// multi-viewport screenshots, a distilled interactive-DOM map, and axe-core
// accessibility violations. It makes no judgement itself.

import { fileURLToPath } from 'node:url';
import { dirname, resolve, join } from 'node:path';
import { mkdirSync, writeFileSync } from 'node:fs';

const __dirname = dirname(fileURLToPath(import.meta.url));

/** Absolute path to the qa/ root, regardless of where node is invoked from. */
export const QA_ROOT = resolve(__dirname, '..', '..');
export const AUTH_STATE = join(QA_ROOT, '.auth', 'storageState.json');
export const RUNS_DIR = join(QA_ROOT, 'runs');
export const APP_URL = process.env.IRT_QA_URL || 'https://dev.resilience.org.in';

/** Pinned so runs are reproducible; loaded into the page from CDN at runtime. */
const AXE_CDN = 'https://cdnjs.cloudflare.com/ajax/libs/axe-core/4.10.2/axe.min.js';

export const VIEWPORTS = [
  { name: 'desktop', width: 1440, height: 900 },
  { name: 'tablet', width: 768, height: 1024 },
  { name: 'mobile', width: 375, height: 812 },
];

function ts() {
  return new Date().toISOString().replace(/[:.]/g, '-');
}

/**
 * Create a run directory and an in-memory record that collectors append to.
 * @param {string} label short charter id, e.g. "us09-geography".
 */
export function createRun(label) {
  const id = `${ts()}_${label}`;
  const dir = join(RUNS_DIR, id);
  mkdirSync(dir, { recursive: true });
  const run = {
    id,
    dir,
    label,
    startedAt: new Date().toISOString(),
    appUrl: APP_URL,
    events: [], // { type, ... } console/pageerror/requestfailed/httperror
    artifacts: [], // relative filenames written into dir
    steps: [], // { name, ok, note } — filled in by the scenario
  };
  return run;
}

// Known-benign console noise on this app: MapLibre/WebGL renderer chatter that
// fires on every run regardless of app health. Tagged `.benign` and excluded
// from the error count so real errors are not buried.
const BENIGN_CONSOLE = [
  /GL Driver Message/i,
  /Max vertices per segment/i,
  /WebGL/i,
  /\[DOM\].*autocomplete/i,
];

/** Attach passive collectors that record errors/failures for the run. */
export function attachCollectors(page, run) {
  page.on('console', (msg) => {
    const type = msg.type();
    if (type !== 'error' && type !== 'warning') return;
    const text = msg.text();
    const benign = BENIGN_CONSOLE.some((re) => re.test(text));
    run.events.push({ type: `console.${type}${benign ? '.benign' : ''}`, text, url: page.url() });
  });
  page.on('pageerror', (err) => {
    run.events.push({ type: 'pageerror', text: String(err && err.message || err), url: page.url() });
  });
  page.on('requestfailed', (req) => {
    const failure = req.failure() ? req.failure().errorText : null;
    // ERR_ABORTED is normal for cancelled/streamed range requests (e.g. pmtiles);
    // it does not indicate a broken resource, so classify it separately.
    const aborted = failure === 'net::ERR_ABORTED';
    run.events.push({
      type: aborted ? 'requestaborted' : 'requestfailed',
      url: req.url(),
      method: req.method(),
      failure,
    });
  });
  page.on('response', (res) => {
    const status = res.status();
    if (status >= 400) {
      run.events.push({ type: 'httperror', status, url: res.url(), method: res.request().method() });
    }
  });
}

/** Save a screenshot at every viewport. Restores the original size afterwards. */
export async function snapshot(page, run, name, { full = true } = {}) {
  const original = page.viewportSize();
  for (const vp of VIEWPORTS) {
    await page.setViewportSize({ width: vp.width, height: vp.height });
    // Streamlit/SPA content reflows async; give layout a beat to settle.
    await page.waitForTimeout(400);
    const file = `${name}__${vp.name}.png`;
    await page.screenshot({ path: join(run.dir, file), fullPage: full });
    run.artifacts.push(file);
  }
  if (original) await page.setViewportSize(original);
}

/**
 * Distil the page into a machine-readable map of interactive elements +
 * a NaN/blank scan. This is what the reviewer and I use to find selectors and
 * spot missing data without eyeballing raw HTML.
 */
export async function dumpDom(page, run, name) {
  const data = await page.evaluate(() => {
    const vis = (el) => {
      const r = el.getBoundingClientRect();
      const s = getComputedStyle(el);
      return r.width > 0 && r.height > 0 && s.visibility !== 'hidden' && s.display !== 'none';
    };
    const clip = (t) => (t || '').trim().replace(/\s+/g, ' ').slice(0, 120);
    const sel = 'a,button,input,select,textarea,[role="button"],[role="tab"],[role="radio"],[role="checkbox"],[onclick]';
    const interactive = [...document.querySelectorAll(sel)]
      .filter(vis)
      .slice(0, 400)
      .map((el) => ({
        tag: el.tagName.toLowerCase(),
        type: el.getAttribute('type') || null,
        role: el.getAttribute('role') || null,
        id: el.id || null,
        name: el.getAttribute('name') || null,
        testid: el.getAttribute('data-testid') || null,
        aria: el.getAttribute('aria-label') || null,
        text: clip(el.innerText || el.value || el.getAttribute('placeholder')),
        href: el.getAttribute('href') || null,
        disabled: el.disabled === true || el.getAttribute('aria-disabled') === 'true',
      }));
    // Scan visible text for suspicious empty/NaN data cells.
    const suspicious = [];
    const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
    let n;
    while ((n = walker.nextNode())) {
      const t = (n.textContent || '').trim();
      if (/^(NaN|nan|null|undefined|None|-)$/.test(t) && n.parentElement && vis(n.parentElement)) {
        suspicious.push({ text: t, parent: clip(n.parentElement.outerHTML) });
        if (suspicious.length >= 50) break;
      }
    }
    return {
      title: document.title,
      url: location.href,
      interactiveCount: interactive.length,
      interactive,
      suspiciousValues: suspicious,
    };
  });
  const file = `${name}__dom.json`;
  writeFileSync(join(run.dir, file), JSON.stringify(data, null, 2));
  run.artifacts.push(file);
  return data;
}

/** Inject axe-core from CDN and record violations. Skips gracefully offline. */
export async function runAxe(page, run, name) {
  try {
    await page.addScriptTag({ url: AXE_CDN });
    const result = await page.evaluate(async () => {
      // eslint-disable-next-line no-undef
      const r = await axe.run(document, { resultTypes: ['violations'] });
      return r.violations.map((v) => ({
        id: v.id,
        impact: v.impact,
        help: v.help,
        nodes: v.nodes.length,
        sample: v.nodes.slice(0, 3).map((x) => x.target.join(' ')),
      }));
    });
    const file = `${name}__axe.json`;
    writeFileSync(join(run.dir, file), JSON.stringify(result, null, 2));
    run.artifacts.push(file);
    return result;
  } catch (e) {
    run.events.push({ type: 'axe.skipped', text: String(e && e.message || e) });
    return null;
  }
}

/** Write results.json summarising the run. Call once at the end. */
export function finalize(run) {
  run.finishedAt = new Date().toISOString();
  const errorCount = run.events.filter((e) =>
    ['console.error', 'pageerror', 'requestfailed', 'httperror'].includes(e.type)).length;
  run.summary = {
    errorEvents: errorCount,
    benignEvents: run.events.filter((e) => e.type.endsWith('.benign') || e.type === 'requestaborted').length,
    totalEvents: run.events.length,
    artifacts: run.artifacts.length,
    stepsFailed: run.steps.filter((s) => s.ok === false).length,
  };
  writeFileSync(join(run.dir, 'results.json'), JSON.stringify(run, null, 2));
  return run;
}

/** Record a scenario step outcome (used by charter scenarios). */
export function step(run, name, ok, note = '') {
  run.steps.push({ name, ok, note });
  return ok;
}
