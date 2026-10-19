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
  // Opt-in software GL (SwiftShader) so deck.gl WebGL hit-testing works headless,
  // enabling map-click probes. Default path is unchanged (no args) when unset.
  const useSoftwareGL = process.env.QA_SOFTWARE_GL === '1';
  const browser = await chromium.launch({
    headless: true,
    args: useSoftwareGL
      ? [
          '--use-gl=angle',
          '--use-angle=swiftshader',
          '--enable-unsafe-swiftshader',
          '--ignore-gpu-blocklist',
        ]
      : [],
  });
  const context = await browser.newContext({
    storageState: AUTH_STATE,
    viewport: viewport || { width: 1440, height: 900 },
  });
  const page = await context.newPage();
  try {
    await fn(page, context);
  } finally {
    // The app ROTATES refresh tokens: any run that triggers
    // POST /api/api/auth/refresh invalidates the refresh token saved on disk and
    // issues a new pair into THIS context only. Discarding the context therefore
    // burned the saved session after exactly one refresh — which is what the old
    // "session expires in ~24h" note was actually observing. Persist the rotated
    // cookies back so the session survives across runs.
    if (process.env.QA_NO_PERSIST !== '1') {
      try {
        // Guard: never overwrite a good saved session with a logged-out one.
        const state = await context.storageState();
        const hasAuth = state.cookies.some((c) => /refresh_token|access_token/.test(c.name));
        if (hasAuth) await context.storageState({ path: AUTH_STATE });
        else console.warn('  ! context holds no auth cookies — saved session left untouched');
      } catch (e) {
        console.warn(`  ! could not persist rotated session: ${e && e.message}`);
      }
    }
    await context.close();
    await browser.close();
  }
}

export { APP_URL };
