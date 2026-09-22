// Recon helpers for the as-built UX-flow documentation pass (CHG-0470).
//
// Different goal from the charter harness: charters ASSERT against a spec, this
// one DESCRIBES. For every state we reach we record (a) the full visible text,
// (b) the interactive-DOM map, (c) a screenshot, and (d) every /api/ call the
// state fired, with request payload and response shape. The API log is what
// lets us tell the vendor "this screen is driven by that endpoint".

import { writeFileSync, appendFileSync } from 'node:fs';
import { join } from 'node:path';
import { dumpDom, snapshot } from './evidence.mjs';

const MAX_BODY = 6000;

/**
 * Record every API request/response on the page into run.api.
 * Bodies are captured for JSON responses only and truncated.
 */
export function attachApiRecorder(page, run) {
  run.api = [];
  page.on('request', (req) => {
    const url = req.url();
    if (!/\/api\//.test(url)) return;
    run.api.push({
      t: Date.now(),
      phase: run.currentPhase || null,
      method: req.method(),
      url,
      postData: (req.postData() || '').slice(0, MAX_BODY) || null,
      status: null,
      contentType: null,
      body: null,
    });
  });
  page.on('response', async (res) => {
    const url = res.url();
    if (!/\/api\//.test(url)) return;
    const rec = [...run.api].reverse().find((r) => r.url === url && r.status === null);
    if (!rec) return;
    rec.status = res.status();
    const ct = res.headers()['content-type'] || '';
    rec.contentType = ct;
    if (/json/i.test(ct)) {
      try { rec.body = (await res.text()).slice(0, MAX_BODY); } catch { /* stream gone */ }
    }
  });
}

/** Full visible text of the page, normalised — the primary documentation record. */
export async function textSnapshot(page, run, name) {
  const txt = await page.evaluate(() => document.body.innerText.replace(/\n{3,}/g, '\n\n'));
  const file = `${name}__text.txt`;
  writeFileSync(join(run.dir, file), txt);
  run.artifacts.push(file);
  return txt;
}

/** Structural outline: headings, landmarks, panel containers. */
export async function outline(page, run, name) {
  const data = await page.evaluate(() => {
    const vis = (el) => {
      const r = el.getBoundingClientRect();
      const s = getComputedStyle(el);
      return r.width > 0 && r.height > 0 && s.visibility !== 'hidden' && s.display !== 'none';
    };
    const clip = (t) => (t || '').trim().replace(/\s+/g, ' ').slice(0, 160);
    return {
      headings: [...document.querySelectorAll('h1,h2,h3,h4,h5,h6')]
        .filter(vis).map((el) => ({ level: el.tagName, text: clip(el.innerText) })),
      landmarks: [...document.querySelectorAll('main,nav,header,footer,aside,[role="main"],[role="navigation"],[role="dialog"],[data-modal-root]')]
        .filter(vis).map((el) => ({ tag: el.tagName.toLowerCase(), role: el.getAttribute('role'), text: clip(el.innerText) })),
      tabs: [...document.querySelectorAll('[role="tab"],[role="tablist"]')]
        .filter(vis).map((el) => ({ role: el.getAttribute('role'), selected: el.getAttribute('aria-selected'), text: clip(el.innerText) })),
      canvases: [...document.querySelectorAll('canvas,svg')].filter(vis)
        .map((el) => ({ tag: el.tagName.toLowerCase(), cls: clip(el.getAttribute('class')), w: Math.round(el.getBoundingClientRect().width), h: Math.round(el.getBoundingClientRect().height) })),
      tables: [...document.querySelectorAll('table')].filter(vis).map((t) => ({
        headers: [...t.querySelectorAll('th')].map((th) => clip(th.innerText)),
        rows: t.querySelectorAll('tbody tr').length,
        firstRows: [...t.querySelectorAll('tbody tr')].slice(0, 5)
          .map((tr) => [...tr.querySelectorAll('td')].map((td) => clip(td.innerText))),
      })),
    };
  });
  const file = `${name}__outline.json`;
  writeFileSync(join(run.dir, file), JSON.stringify(data, null, 2));
  run.artifacts.push(file);
  return data;
}

/**
 * Capture one UI state completely. `shots` = 'desktop' (default) or 'all'.
 */
export async function captureState(page, run, name, { shots = 'desktop', note = '' } = {}) {
  run.currentPhase = name;
  await page.waitForTimeout(600);
  const dom = await dumpDom(page, run, name);
  const out = await outline(page, run, name);
  const txt = await textSnapshot(page, run, name);
  if (shots === 'all') {
    await snapshot(page, run, name, { full: true });
  } else {
    const file = `${name}__desktop.png`;
    await page.screenshot({ path: join(run.dir, file), fullPage: true });
    run.artifacts.push(file);
  }
  run.steps.push({ name, ok: true, note });
  appendFileSync(join(run.dir, '_trace.log'),
    `[${new Date().toISOString()}] ${name} :: interactive=${dom.interactiveCount} headings=${out.headings.length} tables=${out.tables.length} textLen=${txt.length} :: ${note}\n`);
  console.log(`  ✓ ${name}  (interactive ${dom.interactiveCount}, tables ${out.tables.length}, text ${txt.length}b) ${note}`);
  return { dom, outline: out, text: txt };
}

/** Run a stage, never letting one failure kill the sweep. */
export async function stage(run, name, fn) {
  try {
    await fn();
  } catch (e) {
    const msg = String(e && e.message || e).split('\n')[0].slice(0, 300);
    run.steps.push({ name, ok: false, note: msg });
    appendFileSync(join(run.dir, '_trace.log'), `[${new Date().toISOString()}] STAGE-FAIL ${name} :: ${msg}\n`);
    console.log(`  ✗ ${name}  FAILED: ${msg}`);
  }
}

/** Wait until the option listbox is actually open (li[role=option] present). */
async function listOpen(page) {
  return (await page.locator('li[role="option"]').count()) > 0;
}

/** Click a trigger and guarantee the listbox ends up OPEN (toggle-safe). */
export async function openList(page, trigger) {
  for (let i = 0; i < 3; i += 1) {
    if (await listOpen(page)) return true;
    await trigger.click({ timeout: 8000 }).catch(() => {});
    await page.waitForTimeout(650);
    if (await listOpen(page)) return true;
  }
  return false;
}

/** Guarantee the listbox ends up CLOSED without pressing Escape. */
export async function closeList(page, trigger) {
  for (let i = 0; i < 3; i += 1) {
    if (!(await listOpen(page))) return true;
    await trigger.click({ timeout: 8000 }).catch(() => {});
    await page.waitForTimeout(450);
  }
  await page.keyboard.press('Escape').catch(() => {});
  await page.waitForTimeout(300);
  return !(await listOpen(page));
}

/**
 * Open a dropdown trigger, read every option, then close without selecting.
 */
export async function enumerateOptions(page, trigger, label) {
  const ok = await openList(page, trigger);
  const opts = ok ? await page.locator('li[role="option"]').allInnerTexts().catch(() => []) : [];
  const cleaned = opts.map((t) => t.trim().replace(/\s+/g, ' ')).filter(Boolean);
  await closeList(page, trigger);
  console.log(`    \u00b7 ${label}: ${cleaned.length} options${ok ? '' : ' (LIST DID NOT OPEN)'}`);
  return cleaned;
}

/**
 * Open a dropdown and click the option whose text matches exactly. Returns the
 * text actually chosen, or throws with the available options for diagnosis.
 */
export async function selectOption(page, trigger, text, { exact = true } = {}) {
  const ok = await openList(page, trigger);
  if (!ok) throw new Error(`listbox would not open for "${text}"`);
  const items = page.locator('li[role="option"]');
  const n = await items.count();
  let idx = -1;
  const texts = [];
  for (let i = 0; i < n; i += 1) {
    const t = (await items.nth(i).innerText()).trim().replace(/\s+/g, ' ');
    texts.push(t);
    if (idx === -1 && (exact ? t === text : t.includes(text))) idx = i;
  }
  if (idx === -1 && exact) {
    for (let i = 0; i < texts.length; i += 1) if (texts[i].includes(text)) { idx = i; break; }
  }
  if (idx === -1) throw new Error(`option "${text}" not among [${texts.slice(0, 12).join(' | ')}]`);
  await items.nth(idx).click({ timeout: 10000 });
  await page.waitForTimeout(1100);
  return texts[idx];
}

/** Persist an arbitrary structured finding alongside the run evidence. */
export function writeJson(run, name, data) {
  const file = `${name}.json`;
  writeFileSync(join(run.dir, file), JSON.stringify(data, null, 2));
  run.artifacts.push(file);
}

/** Write the API log; call before finalize(). */
export function writeApiLog(run) {
  writeJson(run, '_api_log', run.api || []);
  const uniq = new Map();
  for (const r of (run.api || [])) {
    const key = `${r.method} ${r.url.replace(/\?.*$/, '')}`;
    const e = uniq.get(key) || { key, count: 0, statuses: new Set(), phases: new Set() };
    e.count += 1;
    if (r.status !== null) e.statuses.add(r.status);
    if (r.phase) e.phases.add(r.phase);
    uniq.set(key, e);
  }
  const summary = [...uniq.values()].map((e) => ({
    endpoint: e.key, calls: e.count,
    statuses: [...e.statuses], phases: [...e.phases],
  })).sort((a, b) => a.endpoint.localeCompare(b.endpoint));
  writeJson(run, '_api_summary', summary);
  return summary;
}
