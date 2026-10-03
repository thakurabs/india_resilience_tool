// Run metadata helpers for the data-coverage harness.

import { existsSync, statSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { execFileSync } from 'node:child_process';
import { AUTH_STATE } from '../evidence.mjs';

function git(args) {
  try {
    return execFileSync('git', args, { cwd: process.cwd(), encoding: 'utf8', timeout: 5000 }).trim();
  } catch (e) {
    return null;
  }
}

function gitDirtySummary() {
  const porcelain = git(['status', '--short', '--untracked-files=no']);
  if (porcelain === null) return { available: false, dirty: null, changedFiles: [] };
  const changedFiles = porcelain
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => line.replace(/^.. /, ''));
  return { available: true, dirty: changedFiles.length > 0, changedFiles };
}

function findVisibleBuildText(text) {
  const compact = String(text || '').replace(/\s+/g, ' ');
  const match = compact.match(/(?:version|build|commit|hash)\s*[:#]?\s*([A-Za-z0-9._-]{6,40})/i);
  return match ? match[0] : null;
}

/** Build the Phase 0 metadata record. */
export async function collectRunMetadata({ page, context, targetUrl, viewport }) {
  const browser = context.browser();
  const appText = await page.locator('body').innerText({ timeout: 5000 }).catch(() => '');
  return {
    targetUrl,
    timestamp: new Date().toISOString(),
    authStatePath: AUTH_STATE,
    authStateMtime: existsSync(AUTH_STATE) ? statSync(AUTH_STATE).mtime.toISOString() : null,
    browserVersion: browser ? browser.version() : null,
    userAgent: await page.evaluate(() => navigator.userAgent).catch(() => null),
    viewport: page.viewportSize() || viewport || null,
    git: {
      branch: git(['branch', '--show-current']),
      shortSha: git(['rev-parse', '--short', 'HEAD']),
      dirtyStatus: gitDirtySummary(),
    },
    app: {
      title: await page.title().catch(() => ''),
      landedUrl: page.url(),
      visibleVersionOrBuild: findVisibleBuildText(appText),
    },
  };
}

/** Write `run_metadata.json` into the run directory. */
export function writeRunMetadata(runDir, metadata) {
  const path = join(runDir, 'run_metadata.json');
  writeFileSync(path, JSON.stringify(metadata, null, 2));
  return path;
}
