// Phase 3A pilot probe scaffolding.
//
// This module intentionally stops after deterministic cascade replay. Map,
// ranking, profile, and network assertions are Phase 3B work.

import { existsSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { readCsv, writeCsv, appendJsonl } from './io.mjs';
import { setupPath } from './discovery.mjs';
import { dismissCoverageOverlays } from './overlays.mjs';

const CASCADE_FIELDS = [
  'risk_domain',
  'metric',
  'scenario',
  'period',
  'statistic',
  'map_mode',
];

function nowCompact() {
  return new Date().toISOString().replace(/[-:.TZ]/g, '').slice(0, 14);
}

function selectedPilotRows(rows, opts) {
  const pilotRows = rows.filter((row) => (
    /^Telangana$/i.test(row.state_name)
    && ['district', 'block'].includes(row.admin_level)
    && /Heat Risk/i.test(row.risk_domain)
  ));
  const source = pilotRows.length ? pilotRows : rows;
  return source.slice(0, opts.maxUnits || 1);
}

function attemptId(row, attemptNumber) {
  return `att_${nowCompact()}_${attemptNumber}_${row.stable_key.replace(/[^a-zA-Z0-9]+/g, '_').slice(0, 120)}`;
}

function observationBase(row) {
  return {
    stable_key: row.stable_key,
    state_name: row.state_name,
    admin_level: row.admin_level,
    risk_domain: row.risk_domain,
    metric: row.metric,
    scenario: row.scenario,
    period: row.period,
    statistic: row.statistic,
    map_mode: row.map_mode,
  };
}

function observationFromAttempt(row, attempt) {
  const selected = attempt.selection_status === 'selected';
  return {
    ...observationBase(row),
    terminal_status: selected ? 'needs_triage' : 'fail',
    blocked_reason: '',
    selection_status: attempt.selection_status,
    map_status: 'not_checked',
    ranking_status: 'not_checked',
    profile_status: 'not_checked',
    api_status_summary: 'not_checked',
    visible_error_summary: selected ? '' : attempt.error_summary,
    observed_count_source: 'not_checked',
    retry_count: 0,
    attempt_ids: attempt.attempt_id,
    evidence_path: '',
  };
}

async function replayCascade(page, targetUrl, row) {
  await dismissCoverageOverlays(page);
  const labels = CASCADE_FIELDS.map((field) => row[field]);
  await setupPath(page, targetUrl, row.state_name, row.admin_level, labels);
  await dismissCoverageOverlays(page);
  return page.locator('body').innerText({ timeout: 5000 }).catch(() => '');
}

/** Run Phase 3A pilot scaffolding and write attempts/terminal observations. */
export async function runPilotProbeScaffold(page, runDir, opts) {
  const universePath = join(runDir, 'filter_universe.csv');
  if (!existsSync(universePath)) {
    throw new Error(`Missing filter universe CSV: ${universePath}. Run --discover-only first.`);
  }
  const rows = readCsv(universePath).filter((row) => row.stable_key);
  const selectedRows = selectedPilotRows(rows, opts);
  const attemptsPath = join(runDir, 'coverage_attempts.jsonl');
  const observationsJsonlPath = join(runDir, 'coverage_observations.jsonl');
  const observationsCsvPath = join(runDir, 'coverage_observations.csv');
  const summaryPath = join(runDir, 'pilot_probe_summary.json');
  writeFileSync(attemptsPath, '');
  writeFileSync(observationsJsonlPath, '');

  const attempts = [];
  const observations = [];
  let index = 0;
  for (const row of selectedRows) {
    index += 1;
    console.log(`  Pilot replay ${index}/${selectedRows.length}: ${row.stable_key}`);
    const startedAt = new Date().toISOString();
    const id = attemptId(row, 1);
    const attempt = {
      attempt_id: id,
      attempt_number: 1,
      started_at: startedAt,
      finished_at: null,
      ...observationBase(row),
      selection_status: 'selected',
      error_summary: '',
      note: 'Phase 3A only: cascade replay complete; map/ranking/profile checks deferred.',
    };
    try {
      const body = await replayCascade(page, opts.targetUrl, row);
      attempt.body_text_sample = body.replace(/\s+/g, ' ').slice(0, 240);
    } catch (e) {
      attempt.selection_status = 'selection_failed';
      attempt.error_summary = String((e && e.message) || e);
      attempt.body_text_sample = '';
    }
    attempt.finished_at = new Date().toISOString();
    appendJsonl(attemptsPath, attempt);
    attempts.push(attempt);

    const observation = observationFromAttempt(row, attempt);
    appendJsonl(observationsJsonlPath, observation);
    observations.push(observation);
  }

  writeCsv(observationsCsvPath, observations, [
    'stable_key',
    'state_name',
    'admin_level',
    'risk_domain',
    'metric',
    'scenario',
    'period',
    'statistic',
    'map_mode',
    'terminal_status',
    'blocked_reason',
    'selection_status',
    'map_status',
    'ranking_status',
    'profile_status',
    'api_status_summary',
    'visible_error_summary',
    'observed_count_source',
    'retry_count',
    'attempt_ids',
    'evidence_path',
  ]);

  const summary = {
    timestamp: new Date().toISOString(),
    sourceUniverse: universePath,
    selectedRows: selectedRows.length,
    attempts: attempts.length,
    observations: observations.length,
    selectionFailures: attempts.filter((attempt) => attempt.selection_status !== 'selected').length,
    outputs: {
      attemptsJsonl: attemptsPath,
      observationsJsonl: observationsJsonlPath,
      observationsCsv: observationsCsvPath,
      summary: summaryPath,
    },
  };
  writeFileSync(summaryPath, JSON.stringify(summary, null, 2));
  return summary;
}
