// Phase 3 pilot probe scaffolding and first-pass surface checks.

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

export function selectedPilotRows(rows, opts) {
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
  const surfaceStatuses = attempt.surface_statuses || {
    map_status: 'not_checked',
    ranking_status: 'not_checked',
    profile_status: 'not_checked',
  };
  return {
    ...observationBase(row),
    terminal_status: terminalStatus(attempt, surfaceStatuses),
    blocked_reason: '',
    selection_status: attempt.selection_status,
    map_status: surfaceStatuses.map_status,
    ranking_status: surfaceStatuses.ranking_status,
    profile_status: surfaceStatuses.profile_status,
    api_status_summary: attempt.api_status_summary || 'not_checked',
    visible_error_summary: selected ? surfaceStatuses.visible_error_summary || '' : attempt.error_summary,
    observed_count_source: surfaceStatuses.observed_count_source || 'not_checked',
    observed_count: surfaceStatuses.observed_count ?? '',
    retry_count: 0,
    attempt_ids: attempt.attempt_id,
    evidence_path: '',
  };
}

function terminalStatus(attempt, surfaceStatuses) {
  if (attempt.selection_status !== 'selected') return 'fail';
  const statuses = [
    surfaceStatuses.map_status,
    surfaceStatuses.ranking_status,
    surfaceStatuses.profile_status,
  ];
  if (statuses.some((status) => /error|empty|missing|failed/i.test(status))) return 'fail';
  if (surfaceStatuses.observed_count_source === 'dom_rows_triage') return 'needs_triage';
  if (statuses.some((status) => /needs_triage|not_checked/i.test(status))) return 'needs_triage';
  return 'pass';
}

function relevantNetwork(url) {
  return /\/api\/|\/parquet\/|ranking|map|trend|scenario-comparison|profile|chart|table/i.test(url)
    && !/analytics|audit\/event|fonts|favicon|telemetry|\.css\b|\.js\b|cdn/i.test(url);
}

function responseCountCandidates(value, path = '$') {
  if (Array.isArray(value)) {
    const objectRows = value.filter((item) => item && typeof item === 'object' && !Array.isArray(item));
    const candidates = objectRows.length ? [{ source: `${path}[]`, count: objectRows.length }] : [];
    return candidates.concat(value.flatMap((item, idx) => responseCountCandidates(item, `${path}[${idx}]`)));
  }
  if (!value || typeof value !== 'object') return [];
  const candidates = [];
  for (const [key, child] of Object.entries(value)) {
    const childPath = `${path}.${key}`;
    if (/^(total|total_count|totalCount|count|row_count|rowCount|recordsTotal)$/i.test(key) && Number.isFinite(Number(child))) {
      candidates.push({ source: childPath, count: Number(child) });
    } else if (Array.isArray(child)) {
      const objectRows = child.filter((item) => item && typeof item === 'object' && !Array.isArray(item));
      if (objectRows.length) candidates.push({ source: childPath, count: objectRows.length });
      candidates.push(...responseCountCandidates(child, childPath));
    } else {
      candidates.push(...responseCountCandidates(child, childPath));
    }
  }
  return candidates;
}

function bestResponseCountCandidate(events) {
  const candidates = events
    .filter((event) => event.status === 200 && event.response_body_summary)
    .flatMap((event) => (event.response_body_summary.count_candidates || []).map((candidate) => ({
      ...candidate,
      url: event.url,
    })))
    .filter((candidate) => Number.isFinite(candidate.count) && candidate.count > 0);
  const rankingPayloads = candidates.filter((candidate) => /\/parquet\/.*(?:map-data|ranking|table|data)|ranking|table/i.test(candidate.url));
  const sourcePool = rankingPayloads.length ? rankingPayloads : candidates;
  const preferred = sourcePool.find((candidate) => /total|count|recordsTotal/i.test(candidate.source))
    || sourcePool.find((candidate) => /data|rows|features|records|results|items/i.test(candidate.source))
    || sourcePool[0];
  return preferred || null;
}

function summarizeNetwork(events) {
  const relevant = events.filter((event) => relevantNetwork(event.url || ''));
  const errors = relevant.filter((event) => event.status >= 400 || event.failure);
  const statuses = [...new Set(relevant.map((event) => event.status || event.failure || 'unknown'))];
  return {
    relevant_count: relevant.length,
    error_count: errors.length,
    statuses,
    errors: errors.slice(0, 12),
  };
}

function networkSummaryText(summary) {
  if (!summary.relevant_count) return 'no_relevant_calls';
  const statusText = summary.statuses.length ? `statuses=${summary.statuses.join('|')}` : 'statuses=none';
  return `relevant=${summary.relevant_count}; errors=${summary.error_count}; ${statusText}`;
}

function attachObservationNetwork(page) {
  const events = [];
  const pending = [];
  async function captureResponseBody(res, event) {
    const contentType = await res.headerValue('content-type').catch(() => '');
    if (!/json|text/i.test(contentType || '')) {
      event.response_body_summary = { content_type: contentType || '', skipped: 'non_text_response' };
      return;
    }
    const text = await res.text().catch(() => '');
    if (!text) {
      event.response_body_summary = { content_type: contentType || '', skipped: 'empty_body' };
      return;
    }
    let parsed = null;
    try {
      parsed = JSON.parse(text);
    } catch {
      event.response_body_summary = {
        content_type: contentType || '',
        text_sample: text.replace(/\s+/g, ' ').slice(0, 240),
        count_candidates: [],
      };
      return;
    }
    const countCandidates = responseCountCandidates(parsed)
      .filter((candidate) => candidate.count > 0)
      .slice(0, 20);
    event.response_body_summary = {
      content_type: contentType || '',
      root_type: Array.isArray(parsed) ? 'array' : typeof parsed,
      root_keys: parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? Object.keys(parsed).slice(0, 20) : [],
      count_candidates: countCandidates,
    };
  }
  const onResponse = (res) => {
    const url = res.url();
    if (relevantNetwork(url)) {
      const event = { type: 'response', status: res.status(), url, method: res.request().method() };
      events.push(event);
      pending.push(captureResponseBody(res, event).catch((e) => {
        event.response_body_summary = { error: String((e && e.message) || e) };
      }));
    }
  };
  const onFailed = (req) => {
    const url = req.url();
    if (relevantNetwork(url)) {
      events.push({
        type: 'requestfailed',
        url,
        method: req.method(),
        failure: req.failure() ? req.failure().errorText : null,
      });
    }
  };
  page.on('response', onResponse);
  page.on('requestfailed', onFailed);
  return {
    events,
    async flush() {
      await Promise.allSettled(pending);
    },
    detach() {
      page.off('response', onResponse);
      page.off('requestfailed', onFailed);
    },
  };
}

async function replayCascade(page, targetUrl, row) {
  await dismissCoverageOverlays(page);
  const labels = CASCADE_FIELDS.map((field) => row[field]);
  await setupPath(page, targetUrl, row.state_name, row.admin_level, labels);
  await dismissCoverageOverlays(page);
  return page.locator('body').innerText({ timeout: 5000 }).catch(() => '');
}

async function checkMapSurface(page) {
  await page.waitForTimeout(1500);
  const result = await page.evaluate(() => {
    const text = document.body.innerText || '';
    const canvasCount = document.querySelectorAll('canvas').length;
    const svgCount = document.querySelectorAll('svg').length;
    const noData = /no data|couldn.?t load|unable to load|error loading|something went wrong/i.test(text);
    const mapText = /Map View|Add to Analysis|Very Low|Low|Moderate|High|Extreme|Absolute value|Mean/i.test(text);
    return { canvasCount, svgCount, noData, mapText };
  });
  if (result.noData) return { status: 'map_error', detail: result };
  if (result.canvasCount || result.svgCount || result.mapText) return { status: 'pass', detail: result };
  return { status: 'needs_triage', detail: result };
}

async function checkRankingSurface(page, network) {
  await page.getByText(/^Ranking Table$/i).first().click({ timeout: 4000 }).catch(() => {});
  await page.waitForTimeout(3500);
  await network.flush();
  const apiCount = bestResponseCountCandidate(network.events);
  const result = await page.evaluate(() => {
    const text = document.body.innerText || '';
    const rows = document.querySelectorAll('table tr, [role="row"]').length;
    const errored = /couldn.?t load the ranking data|ranking data failed|error loading|something went wrong/i.test(text);
    const empty = /no data|no records|no results/i.test(text);
    const totalMatch = text.match(/(?:total|showing)\D{0,20}(\d+)/i);
    return {
      rows,
      errored,
      empty,
      visibleTotal: totalMatch ? Number(totalMatch[1]) : null,
    };
  });
  if (result.errored) return { status: 'ranking_api_error', observed_count_source: 'ui_error', detail: result };
  if (apiCount) {
    return {
      status: 'pass',
      observed_count_source: 'api_payload',
      observed_count: apiCount.count,
      detail: { ...result, apiCount },
    };
  }
  if (result.visibleTotal !== null && result.visibleTotal > 0) return { status: 'pass', observed_count_source: 'visible_total', detail: result };
  if (result.rows >= 2) return { status: 'pass', observed_count_source: 'dom_rows_triage', detail: result };
  if (result.empty) return { status: 'ranking_empty', observed_count_source: 'visible_empty', detail: result };
  return { status: 'needs_triage', observed_count_source: 'unresolved', detail: result };
}

async function checkProfileSurface(page) {
  await page.waitForTimeout(700);
  const result = await page.evaluate(() => {
    const text = document.body.innerText || '';
    const present = /Resilience Profile|Open Resilience Profile|Profile/i.test(text);
    const errored = /profile.*(error|failed|couldn.?t load)|something went wrong/i.test(text);
    const empty = /profile.*(no data|empty|not available)/i.test(text);
    return { present, errored, empty };
  });
  if (result.errored) return { status: 'profile_api_error', detail: result };
  if (result.empty) return { status: 'profile_empty', detail: result };
  if (result.present) return { status: 'pass', detail: result };
  return { status: 'profile_missing', detail: result };
}

async function checkSurfaces(page, network) {
  const map = await checkMapSurface(page);
  const ranking = await checkRankingSurface(page, network);
  const profile = await checkProfileSurface(page);
  const visibleErrors = [map, ranking, profile]
    .filter((item) => /error|empty|missing/i.test(item.status))
    .map((item) => item.status)
    .join('; ');
  return {
    map_status: map.status,
    ranking_status: ranking.status,
    profile_status: profile.status,
    observed_count_source: ranking.observed_count_source || 'not_checked',
    observed_count: ranking.observed_count ?? '',
    visible_error_summary: visibleErrors,
    details: { map, ranking, profile },
  };
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
      note: 'Phase 3B pilot: cascade replay plus first-pass map/ranking/profile checks.',
    };
    try {
      const network = attachObservationNetwork(page);
      const body = await replayCascade(page, opts.targetUrl, row);
      await network.flush();
      const surfaces = await checkSurfaces(page, network);
      await network.flush();
      network.detach();
      const networkSummary = summarizeNetwork(network.events);
      attempt.surface_statuses = surfaces;
      attempt.network_events = network.events;
      attempt.network_summary = networkSummary;
      attempt.api_status_summary = networkSummaryText(networkSummary);
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
    'observed_count',
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
    terminalStatusCounts: observations.reduce((counts, observation) => {
      counts[observation.terminal_status] = (counts[observation.terminal_status] || 0) + 1;
      return counts;
    }, {}),
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
