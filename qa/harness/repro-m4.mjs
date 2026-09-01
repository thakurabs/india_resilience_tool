// M4 repro — records a video of the "Show on Map" success + contradictory
// "Location could not be resolved" error toast on a cold page load.
//
// Faithfully mirrors us10-coordinates-panel scenario.mjs S1->S2:
//   cold goto -> open Coordinate Panel -> fill 17.8766 / 79.2792 -> Show on Map.
//
// Adds: video capture, network trace for resolve/geocode calls, and a timed
// toast poll so we can see WHEN the toast appears relative to the click.
//
//   node qa/harness/repro-m4.mjs            # single cold-load attempt
//   node qa/harness/repro-m4.mjs 5          # 5 cold-load attempts (fresh context each)
//
// Videos + log land in qa/runs/repro-m4/.

import { existsSync, mkdirSync, writeFileSync, renameSync } from 'node:fs';
import { join } from 'node:path';
import { chromium } from 'playwright';
import { AUTH_STATE, APP_URL, QA_ROOT } from './lib/evidence.mjs';

const OUT = join(QA_ROOT, 'runs', 'repro-m4');
mkdirSync(OUT, { recursive: true });

const TOAST_RE = /Location could not be resolved[^.]*\./i;
const RESOLVED_RE = /This location is/i;

/** Poll body text; return {found, ms} for the first time the toast phrase shows. */
async function pollToast(page, ms, t0) {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    const body = await page.evaluate(() => document.body.innerText.replace(/\s+/g, ' '));
    if (TOAST_RE.test(body)) return { toast: true, atMs: Date.now() - t0 };
    await page.waitForTimeout(80);
  }
  return { toast: false, atMs: null };
}

async function attempt(i) {
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({
    storageState: AUTH_STATE,
    viewport: { width: 1440, height: 900 },
    recordVideo: { dir: OUT, size: { width: 1440, height: 900 } },
  });
  const page = await context.newPage();

  const net = [];
  page.on('requestfinished', async (req) => {
    const url = req.url();
    if (/resolve|geocode|coordinate|reverse|point/i.test(url)) {
      const res = await req.response();
      net.push({ url, status: res ? res.status() : null, method: req.method() });
    }
  });
  page.on('requestfailed', (req) => {
    const url = req.url();
    if (/resolve|geocode|coordinate|reverse|point/i.test(url)) {
      net.push({ url, status: 'FAILED', failure: req.failure()?.errorText, method: req.method() });
    }
  });

  const log = { attempt: i };
  await page.goto(APP_URL, { waitUntil: 'domcontentloaded' });
  await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => {});
  await page.waitForTimeout(1500);

  // S1 — open Coordinate Panel
  await page.getByRole('button', { name: /Coordinate Panel/i }).click();
  await page.waitForTimeout(1000);

  // S2 — fill + Show on Map (this is the exact action that produced the bug)
  await page.getByPlaceholder('17.8766').fill('17.8766');
  await page.getByPlaceholder('79.2792').fill('79.2792');
  await page.getByPlaceholder('Site 1').fill('QA Site');

  const t0 = Date.now();
  await page.getByRole('button', { name: /Show on Map/i }).click();
  const poll = await pollToast(page, 3000, t0);
  await page.waitForTimeout(700);
  await page.screenshot({ path: join(OUT, `attempt-${i}.png`) });

  const bodyTxt = await page.evaluate(() => document.body.innerText.replace(/\s+/g, ' '));
  log.resolvedInline = RESOLVED_RE.test(bodyTxt);
  log.errorToast = poll.toast;
  log.toastAtMs = poll.atMs;
  log.contradiction = log.resolvedInline && log.errorToast;
  log.resolveCalls = net;

  // Capture the video path before closing, then give it a stable name.
  const vidPath = await page.video()?.path();
  await context.close(); // flushes the video to disk
  await browser.close();
  if (vidPath && existsSync(vidPath)) {
    const dest = join(OUT, `attempt-${i}.webm`);
    try { renameSync(vidPath, dest); log.video = dest; } catch { log.video = vidPath; }
  }
  return log;
}

// Simple sequential runner
const n = parseInt(process.argv[2] || '1', 10);
const results = [];
for (let i = 1; i <= n; i++) {
  process.stdout.write(`attempt ${i}/${n} ... `);
  const r = await attempt(i);
  console.log(r.contradiction ? 'REPRODUCED (resolved+toast)' : `no repro (resolved=${r.resolvedInline}, toast=${r.errorToast})`);
  results.push(r);
}
writeFileSync(join(OUT, 'repro-log.json'), JSON.stringify(results, null, 2));
console.log(`\nlog + videos + screenshots in: ${OUT}`);
const hit = results.filter((r) => r.contradiction).length;
console.log(`reproduced ${hit}/${n} attempt(s).`);
