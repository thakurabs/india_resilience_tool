// Scope estimation and sharding helpers for discovered coverage universes.

import { existsSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { readCsv, writeCsv } from './io.mjs';

function groupKey(row) {
  return [row.state_name, row.admin_level, row.risk_domain, row.metric].join('\u001f');
}

function parseGroupKey(key) {
  const [stateName, adminLevel, riskDomain, metric] = key.split('\u001f');
  return { state_name: stateName, admin_level: adminLevel, risk_domain: riskDomain, metric };
}

function sortRows(rows) {
  return [...rows].sort((a, b) => (
    a.state_name.localeCompare(b.state_name)
    || a.admin_level.localeCompare(b.admin_level)
    || a.risk_domain.localeCompare(b.risk_domain)
    || a.metric.localeCompare(b.metric)
  ));
}

function applyShard(rows, shard) {
  if (!shard) return rows;
  return rows.filter((row, idx) => (idx % shard.total) === (shard.index - 1));
}

function applyMaxUnits(rows, maxUnits) {
  return maxUnits === null ? rows : rows.slice(0, maxUnits);
}

function summarize(rows) {
  const groups = new Map();
  for (const row of rows) {
    const key = groupKey(row);
    groups.set(key, (groups.get(key) || 0) + 1);
  }
  return sortRows([...groups.entries()].map(([key, count]) => ({
    ...parseGroupKey(key),
    universe_count: count,
  })));
}

function executionGate(opts) {
  const gates = [];
  if (opts.maxUnits !== null) gates.push('max_units');
  if (opts.shard) gates.push('shard');
  if (opts.confirmLargeRun) gates.push('confirm_large_run');
  return {
    ok_for_probe: gates.length > 0,
    gates,
    required_for_future_probe: ['max_units', 'shard', 'confirm_large_run'],
  };
}

/** Write Phase 2.5 plan summaries from an existing discovery run directory. */
export function writeCoveragePlanSummary(runDir, opts) {
  const universePath = join(runDir, 'filter_universe.csv');
  if (!existsSync(universePath)) {
    throw new Error(`Missing filter universe CSV: ${universePath}. Run --discover-only first.`);
  }
  const rows = readCsv(universePath).filter((row) => row.stable_key);
  const sortedUniverse = [...rows].sort((a, b) => a.stable_key.localeCompare(b.stable_key));
  const shardedRows = applyShard(sortedUniverse, opts.shard);
  const selectedRows = applyMaxUnits(shardedRows, opts.maxUnits);
  const summaryRows = summarize(selectedRows);
  const csvPath = join(runDir, 'coverage_plan_summary.csv');
  const jsonPath = join(runDir, 'coverage_plan_summary.json');

  writeCsv(csvPath, summaryRows, [
    'state_name',
    'admin_level',
    'risk_domain',
    'metric',
    'universe_count',
  ]);

  const summary = {
    timestamp: new Date().toISOString(),
    sourceUniverse: universePath,
    totalUniverseRows: rows.length,
    selectedUniverseRows: selectedRows.length,
    summaryGroups: summaryRows.length,
    scope: {
      states: [...new Set(selectedRows.map((row) => row.state_name))].sort((a, b) => a.localeCompare(b)),
      levels: [...new Set(selectedRows.map((row) => row.admin_level))].sort((a, b) => a.localeCompare(b)),
      shard: opts.shard,
      maxUnits: opts.maxUnits,
    },
    executionGate: executionGate(opts),
    outputs: {
      csv: csvPath,
      json: jsonPath,
    },
  };
  writeFileSync(jsonPath, JSON.stringify(summary, null, 2));
  return summary;
}
