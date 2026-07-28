// Phase 3.5 observation integrity audit for data coverage probe artifacts.

import { existsSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { readCsv, readJsonl } from './io.mjs';
import { selectedPilotRows } from './probe.mjs';

const TERMINAL_STATUSES = new Set(['pass', 'fail', 'needs_triage', 'skipped', 'blocked']);
const BLOCKED_OR_SKIPPED = new Set(['blocked', 'skipped']);

function countBy(items, keyFn) {
  const counts = new Map();
  for (const item of items) {
    const key = keyFn(item);
    counts.set(key, (counts.get(key) || 0) + 1);
  }
  return counts;
}

function addIssue(issues, code, severity, detail) {
  issues.push({ code, severity, ...detail });
}

function attemptIdsFromObservation(observation) {
  return String(observation.attempt_ids || '')
    .split(/[;,]/)
    .map((value) => value.trim())
    .filter(Boolean);
}

function selectedRowsForAudit(universeRows, attempts, opts) {
  if (opts.maxUnits !== null) {
    return {
      source: 'pilot_selector',
      rows: selectedPilotRows(universeRows, opts),
    };
  }
  const attemptedKeys = new Set(attempts.map((attempt) => attempt.stable_key).filter(Boolean));
  return {
    source: 'attempted_stable_keys',
    rows: universeRows.filter((row) => attemptedKeys.has(row.stable_key)),
  };
}

/** Audit attempts/observations consistency and write `coverage_run_audit.json`. */
export function auditCoverageRun(runDir, opts) {
  const universePath = join(runDir, 'filter_universe.csv');
  const attemptsPath = join(runDir, 'coverage_attempts.jsonl');
  const observationsPath = join(runDir, 'coverage_observations.jsonl');
  const outputPath = join(runDir, 'coverage_run_audit.json');
  for (const path of [universePath, attemptsPath, observationsPath]) {
    if (!existsSync(path)) throw new Error(`Missing required audit input: ${path}`);
  }

  const universeRows = readCsv(universePath).filter((row) => row.stable_key);
  const attempts = readJsonl(attemptsPath);
  const observations = readJsonl(observationsPath);
  const selected = selectedRowsForAudit(universeRows, attempts, opts);
  const selectedKeys = selected.rows.map((row) => row.stable_key);
  const selectedKeySet = new Set(selectedKeys);
  const attemptIds = new Set(attempts.map((attempt) => attempt.attempt_id).filter(Boolean));
  const attemptCountsByKey = countBy(attempts, (attempt) => attempt.stable_key || '');
  const observationCountsByKey = countBy(observations, (observation) => observation.stable_key || '');
  const observationCountsByCompositeKey = countBy(observations, (observation) => [
    observation.stable_key || '',
    observation.state_name || '',
    observation.admin_level || '',
    observation.risk_domain || '',
    observation.metric || '',
    observation.scenario || '',
    observation.period || '',
    observation.statistic || '',
    observation.map_mode || '',
  ].join('\u001f'));

  const issues = [];
  for (const key of selectedKeys) {
    const count = observationCountsByKey.get(key) || 0;
    if (count !== 1) {
      addIssue(issues, 'selected_row_terminal_observation_count', 'error', {
        stable_key: key,
        expected: 1,
        actual: count,
      });
    }
  }

  for (const [key, count] of observationCountsByKey.entries()) {
    if (key && !selectedKeySet.has(key)) {
      addIssue(issues, 'observation_outside_selected_scope', 'warning', {
        stable_key: key,
        actual: count,
      });
    }
  }

  for (const [key, count] of observationCountsByCompositeKey.entries()) {
    if (key && count > 1) {
      addIssue(issues, 'duplicate_observation_key', 'error', {
        observation_key: key,
        actual: count,
      });
    }
  }

  for (const observation of observations) {
    if (!TERMINAL_STATUSES.has(observation.terminal_status)) {
      addIssue(issues, 'non_terminal_observation_status', 'error', {
        stable_key: observation.stable_key || '',
        terminal_status: observation.terminal_status || '',
      });
    }
    const linkedAttemptIds = attemptIdsFromObservation(observation);
    if (!linkedAttemptIds.length) {
      addIssue(issues, 'terminal_observation_missing_attempt_link', 'error', {
        stable_key: observation.stable_key || '',
      });
    }
    for (const attemptId of linkedAttemptIds) {
      if (!attemptIds.has(attemptId)) {
        addIssue(issues, 'terminal_observation_unknown_attempt_link', 'error', {
          stable_key: observation.stable_key || '',
          attempt_id: attemptId,
        });
      }
    }
    if (BLOCKED_OR_SKIPPED.has(observation.terminal_status) && !String(observation.blocked_reason || '').trim()) {
      addIssue(issues, 'skipped_or_blocked_missing_reason', 'error', {
        stable_key: observation.stable_key || '',
        terminal_status: observation.terminal_status,
      });
    }
  }

  for (const row of selected.rows) {
    if ((attemptCountsByKey.get(row.stable_key) || 0) < 1) {
      addIssue(issues, 'selected_row_missing_attempt', 'error', {
        stable_key: row.stable_key,
      });
    }
  }

  const errorCount = issues.filter((issue) => issue.severity === 'error').length;
  const warningCount = issues.filter((issue) => issue.severity === 'warning').length;
  const audit = {
    timestamp: new Date().toISOString(),
    ok: errorCount === 0,
    status: errorCount === 0 ? 'pass' : 'fail',
    selectedRows: selected.rows.length,
    selectionSource: selected.source,
    attempts: attempts.length,
    observations: observations.length,
    duplicateObservationKeys: issues.filter((issue) => issue.code === 'duplicate_observation_key').length,
    missingTerminalObservations: issues.filter((issue) => issue.code === 'selected_row_terminal_observation_count' && issue.actual === 0).length,
    missingAttemptLinks: issues.filter((issue) => issue.code === 'terminal_observation_missing_attempt_link').length,
    missingBlockedOrSkippedReasons: issues.filter((issue) => issue.code === 'skipped_or_blocked_missing_reason').length,
    errorCount,
    warningCount,
    issues,
    inputs: {
      universeCsv: universePath,
      attemptsJsonl: attemptsPath,
      observationsJsonl: observationsPath,
    },
    outputs: {
      auditJson: outputPath,
    },
  };
  writeFileSync(outputPath, JSON.stringify(audit, null, 2));
  return audit;
}
