import { mkdirSync } from 'node:fs';
import { resolve } from 'node:path';
import { createInterface } from 'node:readline/promises';
import { stdin as input, stdout as output } from 'node:process';
import { assertContainedPath } from './paths.mjs';
import { installRequestPolicy, validateTargetUrl } from './policy.mjs';

export async function captureSession({ root, targetUrl, approvedOrigins }) {
  const authRoot = resolve(root, '.auth');
  mkdirSync(authRoot, { recursive: true, mode: 0o700 });
  const statePath = assertContainedPath(authRoot, resolve(authRoot, 'cravis-state.json'));
  validateTargetUrl(targetUrl, approvedOrigins);
  const { chromium } = await import('playwright');
  const browser = await chromium.launch({ headless: false });
  const context = await browser.newContext();
  const page = await context.newPage();
  await installRequestPolicy(page, approvedOrigins);
  try {
    await page.goto(targetUrl, { waitUntil: 'domcontentloaded' });
    const readline = createInterface({ input, output });
    try { await readline.question('Complete login in the browser, then press Enter here to save the session. '); } finally { readline.close(); }
    await context.storageState({ path: statePath });
    return statePath;
  } finally {
    await context.close();
    await browser.close();
  }
}

