// Browser/session helpers: one place that knows how to launch chromium and
// reuse the saved (post-2FA) login session.

import { existsSync } from 'node:fs';
import { chromium } from 'playwright';
import { AUTH_STATE, APP_URL } from './evidence.mjs';

/**
 * Launch a headless browser with the saved login session and hand a page to
 * `fn`. Throws a clear error if the session hasn't been captured yet.
 * @param {(page: import('playwright').Page, ctx: import('playwright').BrowserContext) => Promise<void>} fn
 */
export async function withSession(fn, { viewport } = {}) {
  if (!existsSync(AUTH_STATE)) {
    throw new Error(
      `No saved session at ${AUTH_STATE}. Run: node qa/harness/capture-session.mjs first.`,
    );
  }
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({
    storageState: AUTH_STATE,
    viewport: viewport || { width: 1440, height: 900 },
  });
  const page = await context.newPage();
  try {
    await fn(page, context);
  } finally {
    await context.close();
    await browser.close();
  }
}

export { APP_URL };
