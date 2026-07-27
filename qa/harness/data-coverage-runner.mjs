// Data coverage QA runner for the vendor resilience-filter cascade.
//
// Implemented coverage foundation:
// - Phase 0: saved-auth preflight, overlay dismissal, local roster file
//   validation, metadata capture, and required selector audit.
// - Phase 1: local expected district/block roster extraction.
//
// Usage:
//   node qa/harness/data-coverage-runner.mjs --check-selectors
//   node qa/harness/data-coverage-runner.mjs --dry-run

import { mkdirSync } from 'node:fs';
import { join } from 'node:path';
import { withSession, APP_URL } from './lib/session.mjs';
import { RUNS_DIR } from './lib/evidence.mjs';
import { installCoverageOverlayDismissal, dismissCoverageOverlays } from './lib/coverage/overlays.mjs';
import { collectRunMetadata, writeRunMetadata } from './lib/coverage/metadata.mjs';
import { runPhase0Preflight } from './lib/coverage/preflight.mjs';
import { writeExpectedRosters } from './lib/coverage/rosters.mjs';

function timestampForPath() {
  return new Date().toISOString().replace(/[:.]/g, '-');
}

function parseArgs(argv) {
  const opts = {
    checkSelectors: false,
    dryRun: false,
    targetUrl: APP_URL,
    runDir: null,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === '--check-selectors') opts.checkSelectors = true;
    else if (arg === '--dry-run') opts.dryRun = true;
    else if (arg === '--run-dir') opts.runDir = argv[++i];
    else if (arg === '--target-url') opts.targetUrl = argv[++i];
    else if (arg === '--help' || arg === '-h') opts.help = true;
    else throw new Error(`Unknown argument: ${arg}`);
  }
  return opts;
}

function printHelp() {
  console.log(`
Data coverage runner

Implemented commands:
  node qa/harness/data-coverage-runner.mjs --check-selectors
  node qa/harness/data-coverage-runner.mjs --dry-run

Options:
  --target-url <url>   Override the target URL for this run
  --run-dir <path>     Write artifacts into an existing/new run dir
  --help               Show this message

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
if (!opts.checkSelectors && !opts.dryRun) {
  printHelp();
  throw new Error('Use --check-selectors or --dry-run. Later coverage modes are not implemented yet.');
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
  exitCode = preflight.ok ? 0 : 2;

  console.log(`\n  Run dir: ${runDir}`);
  console.log(`  Metadata: ${join(runDir, 'run_metadata.json')}`);
  console.log(`  Selector preflight: ${join(runDir, 'selector_preflight.json')}`);
  console.log(`  Auth: ${preflight.auth.ok ? 'ok' : `blocked (${preflight.auth.blockedReason})`}`);
  console.log(`  Rosters: ${preflight.rosters.ok ? 'ok' : 'missing/blocked'}`);
  console.log(`  Selector checks: ${preflight.selectorChecks.filter((c) => c.ok).length}/${preflight.selectorChecks.length} ok`);
}, { viewport });

process.exit(exitCode);
