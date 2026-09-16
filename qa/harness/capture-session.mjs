// One-time (per session lifetime) helper: opens a REAL visible browser so you
// can log in and clear 2FA by hand, then saves the resulting cookies/localStorage
// to qa/.auth/storageState.json for all automated runs to reuse.
//
// Usage:
//   node qa/harness/capture-session.mjs
// Then: log in + complete 2FA in the window, land on the dashboard, come back
// to the terminal and press ENTER. The window closes and the session is saved.
//
// The saved file is credentials-equivalent and is gitignored.

import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import { createInterface } from 'node:readline';
import { chromium } from 'playwright';
import { AUTH_STATE, APP_URL } from './lib/evidence.mjs';

function waitForEnter(prompt) {
  const rl = createInterface({ input: process.stdin, output: process.stdout });
  return new Promise((res) => rl.question(prompt, () => { rl.close(); res(); }));
}

const browser = await chromium.launch({ headless: false });
const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
const page = await context.newPage();
await page.goto(APP_URL, { waitUntil: 'domcontentloaded' });

console.log('\n  A browser window is open.');
console.log('  → Log in and complete 2FA until you are on the dashboard.');
console.log('  → Then return here and press ENTER to save the session.\n');

await waitForEnter('  Press ENTER once logged in… ');

mkdirSync(dirname(AUTH_STATE), { recursive: true });
await context.storageState({ path: AUTH_STATE });
console.log(`\n  Saved session → ${AUTH_STATE}`);

await context.close();
await browser.close();
