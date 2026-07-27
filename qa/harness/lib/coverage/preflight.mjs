// Phase 0 selector and local-data preflight checks.

import { existsSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { execFileSync } from 'node:child_process';
import { APP_URL } from '../evidence.mjs';
import { openAdmin } from '../flows.mjs';
import { dismissCoverageOverlays } from './overlays.mjs';

function normalize(text) {
  return String(text || '').replace(/\s+/g, ' ').trim();
}

export function resolveDataDir() {
  if (process.env.IRT_DATA_DIR) return resolve(process.env.IRT_DATA_DIR);
  try {
    const script = "from paths import get_paths_config; print(get_paths_config().data_dir)";
    const out = execFileSync(process.env.PYTHON || 'python', ['-c', script], { encoding: 'utf8' }).trim();
    return out ? resolve(out) : null;
  } catch (e) {
    return resolve(dirname(process.cwd()), 'irt_data');
  }
}

async function locatorCount(locator) {
  return locator.count().catch(() => 0);
}

async function clickIfVisible(locator, timeout = 3000) {
  const first = locator.first();
  if (!(await first.isVisible({ timeout }).catch(() => false))) return false;
  await first.click({ timeout }).catch(() => false);
  return true;
}

async function checkControl(page, name, locator, { required = true } = {}) {
  const count = await locatorCount(locator);
  const first = locator.first();
  const visible = count > 0 ? await first.isVisible().catch(() => false) : false;
  const disabled = count > 0 ? await first.isDisabled().catch(() => null) : null;
  return {
    name,
    required,
    ok: required ? count > 0 && visible : count === 0 || visible,
    count,
    visible,
    disabled,
    sampleText: count > 0 ? normalize(await first.innerText({ timeout: 1000 }).catch(() => '')) : '',
  };
}

async function bodyContains(page, name, pattern, { required = true } = {}) {
  const body = await page.locator('body').innerText({ timeout: 5000 }).catch(() => '');
  const matched = pattern.test(body);
  return { name, required, ok: required ? matched : true, count: matched ? 1 : 0, visible: matched, disabled: null, sampleText: matched ? pattern.source : '' };
}

async function runSelectorChecks(page) {
  const checks = [];

  await openAdmin(page).catch(() => {});
  await dismissCoverageOverlays(page);
  checks.push(await checkControl(page, 'state_selector', page.getByRole('button', { name: /Select State/i })));

  const stateButton = page.getByRole('button', { name: /Select State/i }).first();
  if (await stateButton.isVisible().catch(() => false)) {
    await stateButton.click({ timeout: 3000 }).catch(() => {});
    await page.waitForTimeout(500);
    const telangana = page.locator('li[role="option"]', { hasText: /^Telangana$/ }).first();
    if (await telangana.isVisible().catch(() => false)) {
      await telangana.click({ timeout: 3000 }).catch(() => {});
      await page.waitForTimeout(900);
    } else {
      await page.keyboard.press('Escape').catch(() => {});
    }
  }

  const body = await page.locator('body').innerText({ timeout: 5000 }).catch(() => '');
  checks.push({
    name: 'district_level_control',
    required: true,
    ok: /District/i.test(body),
    count: /District/i.test(body) ? 1 : 0,
    visible: /District/i.test(body),
    disabled: null,
    sampleText: 'District',
  });
  checks.push({
    name: 'block_level_control',
    required: true,
    ok: /Block/i.test(body),
    count: /Block/i.test(body) ? 1 : 0,
    visible: /Block/i.test(body),
    disabled: null,
    sampleText: 'Block',
  });

  checks.push(await checkControl(page, 'resilience_filter_panel', page.getByText(/Select Resilience Filters/i).first()));
  await clickIfVisible(page.getByText(/Select Resilience Filters/i), 3000);
  await page.waitForTimeout(700).catch(() => {});
  for (const label of ['Risk Domain', 'Metric', 'Scenario', 'Period', 'Statistic', 'Map Mode']) {
    checks.push(await bodyContains(page, `filter_stage_${label.toLowerCase().replace(/\s+/g, '_')}`, new RegExp(label, 'i')));
  }

  checks.push(await bodyContains(page, 'map_view_control', /^Map View$|Map View/i));
  checks.push(await bodyContains(page, 'ranking_view_control', /^Ranking Table$|Ranking Table/i));
  checks.push(await bodyContains(page, 'profile_panel_control', /Resilience Profile|Open Resilience Profile|Profile/i, { required: true }));

  return checks;
}

export function checkRosters() {
  const dataDir = resolveDataDir();
  const files = [
    { logicalName: 'districts', path: dataDir ? join(dataDir, 'districts_4326.geojson') : null, required: true },
    { logicalName: 'blocks', path: dataDir ? join(dataDir, 'blocks_4326.geojson') : null, required: true },
  ];
  return {
    dataDir,
    ok: Boolean(dataDir) && files.every((f) => f.path && existsSync(f.path)),
    files: files.map((f) => ({ ...f, exists: Boolean(f.path && existsSync(f.path)) })),
  };
}

/** Run Phase 0 checks and write `selector_preflight.json`. */
export async function runPhase0Preflight(page, runDir) {
  const landedUrl = page.url();
  const body = await page.locator('body').innerText({ timeout: 5000 }).catch(() => '');
  const authBounce = /login|signin|sign-in|auth/i.test(landedUrl) || /sign in|log in|two-factor|password/i.test(body);
  const overlayDismissal = await dismissCoverageOverlays(page);
  const rosters = checkRosters();
  let selectorChecks = [];
  let selectorError = null;
  if (!authBounce) {
    try {
      selectorChecks = await runSelectorChecks(page);
    } catch (e) {
      selectorError = String((e && e.message) || e);
    }
  }
  const ok = !authBounce && !selectorError && rosters.ok && selectorChecks.every((check) => check.ok);
  const result = {
    targetUrl: APP_URL,
    timestamp: new Date().toISOString(),
    landedUrl,
    auth: {
      ok: !authBounce,
      blockedReason: authBounce ? 'AUTH_BOUNCE' : null,
    },
    overlayDismissal,
    rosters,
    selectorChecks,
    selectorError,
    ok,
  };
  writeFileSync(join(runDir, 'selector_preflight.json'), JSON.stringify(result, null, 2));
  return result;
}
