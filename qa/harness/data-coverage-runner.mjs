// Data coverage QA runner for the vendor resilience-filter cascade.
//
// Implemented coverage foundation:
// - Phase 0: saved-auth preflight, overlay dismissal, local roster file
//   validation, metadata capture, and required selector audit.
// - Phase 1: local expected district/block roster extraction.
// - Phase 2: deterministic exposed cascade discovery.
// - Phase 2.5: scope estimation and sharding summaries.
// - Phase 3: pilot attempt/observation scaffolding plus first-pass surface checks.
//
// Usage:
//   node qa/harness/data-coverage-runner.mjs --check-selectors
//   node qa/harness/data-coverage-runner.mjs --dry-run
//   node qa/harness/data-coverage-runner.mjs --discover-only --states Telangana --levels district,block
//   node qa/harness/data-coverage-runner.mjs --estimate-only --run-dir qa/runs/<id>_data-coverage
//   node qa/harness/data-coverage-runner.mjs --pilot --run-dir qa/runs/<id>_data-coverage --max-units 1

import { mkdirSync } from 'node:fs';
import { join } from 'node:path';
import { withSession, APP_URL } from './lib/session.mjs';
import { RUNS_DIR } from './lib/evidence.mjs';
import { installCoverageOverlayDismissal, dismissCoverageOverlays } from './lib/coverage/overlays.mjs';
import { collectRunMetadata, writeRunMetadata } from './lib/coverage/metadata.mjs';
import { runPhase0Preflight } from './lib/coverage/preflight.mjs';
import { writeExpectedRosters } from './lib/coverage/rosters.mjs';
import { runCascadeDiscovery } from './lib/coverage/discovery.mjs';
import { writeCoveragePlanSummary } from './lib/coverage/estimation.mjs';
import { runPilotProbeScaffold } from './lib/coverage/probe.mjs';

function timestampForPath() {
  return new Date().toISOString().replace(/[:.]/g, '-');
}

function parseArgs(argv) {
  const opts = {
    checkSelectors: false,
    dryRun: false,
    discoverOnly: false,
    estimateOnly: false,
    pilot: false,
    targetUrl: APP_URL,
    runDir: null,
    states: ['Telangana'],
    levels: ['district', 'block'],
    maxDiscoveryPaths: null,
    maxUnits: null,
    shard: null,
    confirmLargeRun: false,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === '--check-selectors') opts.checkSelectors = true;
    else if (arg === '--dry-run') opts.dryRun = true;
    else if (arg === '--discover-only') opts.discoverOnly = true;
    else if (arg === '--estimate-only') opts.estimateOnly = true;
    else if (arg === '--pilot') opts.pilot = true;
    else if (arg === '--run-dir') opts.runDir = argv[++i];
    else if (arg === '--target-url') opts.targetUrl = argv[++i];
    else if (arg === '--states') opts.states = splitList(argv[++i]);
    else if (arg === '--levels') opts.levels = splitList(argv[++i]).map((level) => level.toLowerCase());
    else if (arg === '--max-discovery-paths') opts.maxDiscoveryPaths = parsePositiveInt(argv[++i], '--max-discovery-paths');
    else if (arg === '--max-units') opts.maxUnits = parsePositiveInt(argv[++i], '--max-units');
    else if (arg === '--shard') opts.shard = parseShard(argv[++i]);
    else if (arg === '--confirm-large-run') opts.confirmLargeRun = true;
    else if (arg === '--help' || arg === '-h') opts.help = true;
    else throw new Error(`Unknown argument: ${arg}`);
  }
  validateOpts(opts);
  return opts;
}

function splitList(value) {
  return String(value || '').split(',').map((item) => item.trim()).filter(Boolean);
}

function parsePositiveInt(value, flag) {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isInteger(parsed) || parsed <= 0) throw new Error(`${flag} must be a positive integer`);
  return parsed;
}

function parseShard(value) {
  const match = String(value || '').match(/^(\d+)\/(\d+)$/);
  if (!match) throw new Error('--shard must use N/M format, for example 1/10');
  const index = Number.parseInt(match[1], 10);
  const total = Number.parseInt(match[2], 10);
  if (index < 1 || total < 1 || index > total) {
    throw new Error('--shard must satisfy 1 <= N <= M');
  }
  return { index, total, label: `${index}/${total}` };
}

function validateOpts(opts) {
  const badLevels = opts.levels.filter((level) => !['district', 'block'].includes(level));
  if (badLevels.length) throw new Error(`Unsupported --levels value(s): ${badLevels.join(', ')}`);
  if (!opts.states.length) throw new Error('--states must include at least one state');
  if (!opts.levels.length) throw new Error('--levels must include district and/or block');
  if (opts.estimateOnly && !opts.runDir) throw new Error('--estimate-only requires --run-dir from a discovery run');
  if (opts.pilot && !opts.runDir) throw new Error('--pilot requires --run-dir from a discovery run');
}

function printHelp() {
  console.log(`
Data coverage runner

Implemented commands:
  node qa/harness/data-coverage-runner.mjs --check-selectors
  node qa/harness/data-coverage-runner.mjs --dry-run
  node qa/harness/data-coverage-runner.mjs --discover-only --states Telangana --levels district,block
  node qa/harness/data-coverage-runner.mjs --estimate-only --run-dir qa/runs/<id>_data-coverage
  node qa/harness/data-coverage-runner.mjs --pilot --run-dir qa/runs/<id>_data-coverage --max-units 1

Options:
  --target-url <url>             Override the target URL for this run
  --run-dir <path>               Write artifacts into an existing/new run dir
  --states <A,B>                 State names to discover (default: Telangana)
  --levels <district,block>      Admin levels to discover (default: district,block)
  --max-discovery-paths <N>      Stop after N terminal universe rows
  --max-units <N>                Future probe safety gate / selected-row cap
  --shard <N/M>                  Future probe safety gate / deterministic shard
  --confirm-large-run            Future probe safety gate for exhaustive runs
  --help                         Show this message

Planned later-phase flags from DATA_COVERAGE_PLAN.md are intentionally not
accepted yet, so accidental exhaustive runs cannot start from this foundation.
`);
}

function makeRunDir(runDir) {
  const dir = runDir || join(RUNS_DIR, `${timestampForPath()}_data-coverage`);
  mkdirSync(dir, { recursive: true });
  return dir;
}

const opts = parseArgs(process.argv.slice(2));
if (opts.help) {
  printHelp();
  process.exit(0);
}
if (!opts.checkSelectors && !opts.dryRun && !opts.discoverOnly && !opts.estimateOnly && !opts.pilot) {
  printHelp();
  throw new Error('Use --check-selectors, --dry-run, --discover-only, --estimate-only, or --pilot. Later coverage modes are not implemented yet.');
}

const runDir = makeRunDir(opts.runDir);
const viewport = { width: 1440, height: 900 };

if (opts.dryRun) {
  console.log('  Building local expected rosters');
  const summary = writeExpectedRosters(runDir);
  console.log(`\n  Run dir: ${runDir}`);
  console.log(`  Expected states: ${summary.outputs.expectedStates}`);
  console.log(`  Expected districts: ${summary.outputs.expectedDistricts}`);
  console.log(`  Expected blocks: ${summary.outputs.expectedBlocks}`);
  console.log(`  Counts: ${summary.outputs.expectedCountsByStateLevel}`);
  console.log(`  Totals: states=${summary.totals.states}, districts=${summary.totals.districts}, blocks=${summary.totals.blocks}`);
  console.log(`  Duplicate checks: district=${summary.duplicateChecks.districtsByStateDistrict.length}, block_key=${summary.duplicateChecks.blocksByBlockKey.length}, block_label=${summary.duplicateChecks.blocksByStateDistrictBlock.length}`);
  process.exit(0);
}

if (opts.estimateOnly) {
  console.log('  Building coverage plan summary');
  const summary = writeCoveragePlanSummary(runDir, {
    maxUnits: opts.maxUnits,
    shard: opts.shard,
    confirmLargeRun: opts.confirmLargeRun,
  });
  console.log(`\n  Run dir: ${runDir}`);
  console.log(`  Source universe rows: ${summary.totalUniverseRows}`);
  console.log(`  Selected universe rows: ${summary.selectedUniverseRows}`);
  console.log(`  Summary groups: ${summary.summaryGroups}`);
  console.log(`  Coverage plan CSV: ${summary.outputs.csv}`);
  console.log(`  Coverage plan JSON: ${summary.outputs.json}`);
  console.log(`  Future probe gate: ${summary.executionGate.ok_for_probe ? summary.executionGate.gates.join(', ') : 'missing'}`);
  process.exit(0);
}

let exitCode = 0;
await withSession(async (page, context) => {
  page.setDefaultTimeout(8000);
  await installCoverageOverlayDismissal(page);
  console.log(`  Loading ${opts.targetUrl}`);
  await page.goto(opts.targetUrl, { waitUntil: 'domcontentloaded' });
  await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => {});
  await page.waitForTimeout(1500);
  await dismissCoverageOverlays(page);

  console.log('  Writing run metadata');
  const metadata = await collectRunMetadata({ page, context, targetUrl: opts.targetUrl, viewport });
  writeRunMetadata(runDir, metadata);

  console.log('  Running selector preflight');
  const preflight = await runPhase0Preflight(page, runDir);
  if (!preflight.ok) {
    exitCode = 2;
  } else if (opts.discoverOnly) {
    console.log('  Running cascade discovery');
    const discovery = await runCascadeDiscovery(page, runDir, {
      targetUrl: opts.targetUrl,
      states: opts.states,
      levels: opts.levels,
      maxDiscoveryPaths: opts.maxDiscoveryPaths,
    });
    console.log(`  Filter universe JSONL: ${discovery.outputs.jsonl}`);
    console.log(`  Filter universe CSV: ${discovery.outputs.csv}`);
    console.log(`  Universe rows: ${discovery.universeRows}`);
    exitCode = 0;
  } else if (opts.pilot) {
    console.log('  Running pilot probe scaffold');
    const pilot = await runPilotProbeScaffold(page, runDir, {
      targetUrl: opts.targetUrl,
      maxUnits: opts.maxUnits,
    });
    console.log(`  Coverage attempts: ${pilot.outputs.attemptsJsonl}`);
    console.log(`  Coverage observations JSONL: ${pilot.outputs.observationsJsonl}`);
    console.log(`  Coverage observations CSV: ${pilot.outputs.observationsCsv}`);
    console.log(`  Pilot observations: ${pilot.observations}`);
    console.log(`  Selection failures: ${pilot.selectionFailures}`);
    exitCode = pilot.selectionFailures ? 2 : 0;
  } else {
    exitCode = 0;
  }

  console.log(`\n  Run dir: ${runDir}`);
  console.log(`  Metadata: ${join(runDir, 'run_metadata.json')}`);
  console.log(`  Selector preflight: ${join(runDir, 'selector_preflight.json')}`);
  console.log(`  Auth: ${preflight.auth.ok ? 'ok' : `blocked (${preflight.auth.blockedReason})`}`);
  console.log(`  Rosters: ${preflight.rosters.ok ? 'ok' : 'missing/blocked'}`);
  console.log(`  Selector checks: ${preflight.selectorChecks.filter((c) => c.ok).length}/${preflight.selectorChecks.length} ok`);
}, { viewport });

process.exit(exitCode);
