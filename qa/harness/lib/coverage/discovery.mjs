// Deterministic resilience-filter cascade discovery.

import { existsSync, mkdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { APP_URL } from '../evidence.mjs';
import { openAdmin, selectState } from '../flows.mjs';
import { appendJsonl, writeCsv } from './io.mjs';
import { dismissCoverageOverlays } from './overlays.mjs';

const STAGES = ['risk_domain', 'metric', 'scenario', 'period', 'statistic', 'map_mode'];
const STAGE_LABELS = {
  risk_domain: 'Risk Domain',
  metric: 'Metric',
  scenario: 'Scenario',
  period: 'Period',
  statistic: 'Statistic',
  map_mode: 'Map Mode',
};

function normalizeText(value) {
  return String(value ?? '').replace(/\s+/g, ' ').trim();
}

function slug(value) {
  return normalizeText(value)
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '') || 'blank';
}

function stableKey(row) {
  return [
    slug(row.state_name),
    row.admin_level,
    slug(row.risk_domain),
    slug(row.metric),
    slug(row.scenario),
    slug(row.period),
    slug(row.statistic),
    slug(row.map_mode),
  ].join('|');
}

async function visibleText(page) {
  return page.locator('body').innerText({ timeout: 5000 }).catch(() => '');
}

async function selectLevel(page, level) {
  const label = level === 'block' ? /Block/i : /District/i;
  const exact = level === 'block' ? /^Block$/i : /^District$/i;
  const candidates = [
    page.getByRole('radio', { name: label }),
    page.getByRole('button', { name: exact }),
    page.getByText(exact),
  ];
  for (const locator of candidates) {
    const first = locator.first();
    if (await first.isVisible({ timeout: 1500 }).catch(() => false)) {
      await first.click({ timeout: 3000 }).catch(() => {});
      await page.waitForTimeout(700);
      return true;
    }
  }
  return level === 'district';
}

async function openFilters(page) {
  const body = await visibleText(page);
  if (/Risk Domain/i.test(body) && /Map Mode/i.test(body)) return true;
  const trigger = page.getByText(/Select Resilience Filters/i).first();
  if (!(await trigger.isVisible({ timeout: 3000 }).catch(() => false))) return false;
  await trigger.click({ timeout: 3000 }).catch(() => {});
  await page.waitForTimeout(700);
  return /Risk Domain/i.test(await visibleText(page));
}

export async function setupPath(page, targetUrl, stateName, level, priorLabels) {
  await page.goto(targetUrl, { waitUntil: 'domcontentloaded' });
  await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => {});
  await page.waitForTimeout(1200);
  await dismissCoverageOverlays(page);
  await openAdmin(page).catch(() => {});
  await selectState(page, stateName);
  await selectLevel(page, level);
  await openFilters(page);
  for (const label of priorLabels) {
    await chooseNextSelectOption(page, label);
  }
}

async function dropdownOptions(page) {
  return page.locator('li[role="option"]').evaluateAll((nodes) => nodes
    .map((node) => {
      const text = (node.innerText || node.textContent || '').replace(/\s+/g, ' ').trim();
      const ariaDisabled = node.getAttribute('aria-disabled') === 'true';
      const disabled = ariaDisabled || node.hasAttribute('disabled') || /disabled/i.test(node.className || '');
      return { label: text, normalized_label: text.toLowerCase(), disabled };
    })
    .filter((row) => row.label));
}

async function collectAutoSelectedOption(page, stage) {
  const label = STAGE_LABELS[stage];
  return page.evaluate((stageLabel) => {
    const clean = (value) => String(value || '').replace(/\s+/g, ' ').trim();
    const labels = [...document.querySelectorAll('label,span,div,p,h1,h2,h3,h4,h5,h6')]
      .filter((el) => clean(el.innerText) === stageLabel);
    for (const labelEl of labels) {
      let box = labelEl;
      for (let depth = 0; depth < 5 && box; depth += 1, box = box.parentElement) {
        const buttons = [...box.querySelectorAll('button,[role="button"]')]
          .map((el) => clean(el.innerText || el.getAttribute('aria-label') || el.getAttribute('title')))
          .filter(Boolean)
          .filter((text) => !/^select$/i.test(text))
          .filter((text) => !new RegExp(stageLabel, 'i').test(text))
          .filter((text) => !/help/i.test(text));
        if (buttons.length) return buttons[0];
      }
    }
    return '';
  }, label).then((labelText) => (labelText ? [{
    label: labelText,
    normalized_label: labelText.toLowerCase(),
    disabled: false,
    auto_selected: true,
  }] : []));
}

async function collectNextOptions(page, stage) {
  const trigger = page.getByText('Select', { exact: true }).first();
  if (await trigger.isVisible({ timeout: 2500 }).catch(() => false)) {
    await trigger.click({ timeout: 3000 }).catch(() => {});
    await page.waitForTimeout(500);
    const options = await dropdownOptions(page);
    await page.keyboard.press('Escape').catch(() => {});
    return { options, source: 'dropdown' };
  }
  const options = await collectAutoSelectedOption(page, stage);
  return { options, source: options.length ? 'auto_selected' : 'missing_control' };
}

async function chooseNextSelectOption(page, label) {
  const trigger = page.getByText('Select', { exact: true }).first();
  if (!(await trigger.isVisible({ timeout: 4000 }).catch(() => false))) {
    return false;
  }
  await trigger.click({ timeout: 3000 });
  await page.waitForTimeout(400);
  const option = page.locator('li[role="option"]').filter({ hasText: new RegExp(`^${escapeRegex(label)}$`) }).first();
  if (!(await option.isVisible({ timeout: 3000 }).catch(() => false))) {
    await page.keyboard.press('Escape').catch(() => {});
    throw new Error(`Could not select cascade option "${label}"`);
  }
  await option.click({ timeout: 3000 });
  await page.waitForTimeout(800);
  return true;
}

function escapeRegex(value) {
  return String(value).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function makeOptionRecord({ stateName, level, stage, priorPath, option, source, error = null }) {
  return {
    row_type: 'option',
    timestamp: new Date().toISOString(),
    state_name: stateName,
    admin_level: level,
    cascade_stage: stage,
    cascade_stage_label: STAGE_LABELS[stage],
    selected_prior_path: priorPath,
    label: option?.label || '',
    normalized_label: option?.normalized_label || '',
    disabled: option?.disabled ?? null,
    auto_selected: option?.auto_selected === true,
    source,
    error,
  };
}

function makeUniverseRecord({ stateName, level, path }) {
  const row = {
    row_type: 'universe',
    timestamp: new Date().toISOString(),
    state_name: stateName,
    admin_level: level,
    risk_domain: path.risk_domain,
    metric: path.metric,
    scenario: path.scenario,
    period: path.period,
    statistic: path.statistic,
    map_mode: path.map_mode,
    status: 'discovered',
  };
  return { ...row, stable_key: stableKey(row) };
}

async function discoverForStateLevel(page, opts, stateName, level, jsonlPath, universeRows) {
  const terminalLimit = opts.maxDiscoveryPaths;
  async function visit(stageIndex, path) {
    if (terminalLimit !== null && universeRows.length >= terminalLimit) return;
    if (stageIndex >= STAGES.length) {
      const record = makeUniverseRecord({ stateName, level, path });
      appendJsonl(jsonlPath, record);
      universeRows.push(record);
      console.log(`    discovered ${universeRows.length}: ${record.stable_key}`);
      return;
    }
    const stage = STAGES[stageIndex];
    const priorLabels = STAGES.slice(0, stageIndex).map((key) => path[key]);
    try {
      await setupPath(page, opts.targetUrl, stateName, level, priorLabels);
      const { options, source } = await collectNextOptions(page, stage);
      if (!options.length) {
        appendJsonl(jsonlPath, makeOptionRecord({
          stateName,
          level,
          stage,
          priorPath: path,
          source,
          error: `No options discovered for ${stage}`,
        }));
        return;
      }
      for (const option of options) {
        if (terminalLimit !== null && universeRows.length >= terminalLimit) break;
        appendJsonl(jsonlPath, makeOptionRecord({ stateName, level, stage, priorPath: path, option, source }));
        if (!option.disabled) {
          await visit(stageIndex + 1, { ...path, [stage]: option.label });
        }
      }
    } catch (e) {
      appendJsonl(jsonlPath, makeOptionRecord({
        stateName,
        level,
        stage,
        priorPath: path,
        source: 'error',
        error: String((e && e.message) || e),
      }));
    }
  }
  await visit(0, {});
}

/** Run Phase 2 discovery and write `filter_universe.jsonl/csv`. */
export async function runCascadeDiscovery(page, runDir, opts) {
  mkdirSync(runDir, { recursive: true });
  const jsonlPath = join(runDir, 'filter_universe.jsonl');
  const csvPath = join(runDir, 'filter_universe.csv');
  const summaryPath = join(runDir, 'filter_universe_summary.json');
  if (existsSync(jsonlPath)) writeFileSync(jsonlPath, '');

  const universeRows = [];
  for (const stateName of opts.states) {
    for (const level of opts.levels) {
      if (opts.maxDiscoveryPaths !== null && universeRows.length >= opts.maxDiscoveryPaths) break;
      console.log(`  Discovering ${stateName} / ${level}`);
      await discoverForStateLevel(page, opts, stateName, level, jsonlPath, universeRows);
    }
    if (opts.maxDiscoveryPaths !== null && universeRows.length >= opts.maxDiscoveryPaths) break;
  }

  writeCsv(csvPath, universeRows, [
    'stable_key',
    'state_name',
    'admin_level',
    'risk_domain',
    'metric',
    'scenario',
    'period',
    'statistic',
    'map_mode',
    'status',
  ]);

  const summary = {
    timestamp: new Date().toISOString(),
    targetUrl: opts.targetUrl || APP_URL,
    states: opts.states,
    levels: opts.levels,
    maxDiscoveryPaths: opts.maxDiscoveryPaths,
    universeRows: universeRows.length,
    outputs: {
      jsonl: jsonlPath,
      csv: csvPath,
      summary: summaryPath,
    },
  };
  writeFileSync(summaryPath, JSON.stringify(summary, null, 2));
  return summary;
}
