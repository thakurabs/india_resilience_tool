#!/usr/bin/env node
import { existsSync, mkdirSync } from 'node:fs';
import { basename, dirname, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { createInterface } from 'node:readline/promises';
import { stdin as input, stdout as output } from 'node:process';

function evaluatorRoot() { return dirname(fileURLToPath(import.meta.url)); }

function parseArgs(argv) {
  const [command, ...rest] = argv;
  const options = {};
  for (let index = 0; index < rest.length; index += 1) {
    const token = rest[index];
    if (!token.startsWith('--')) throw new Error(`Unexpected argument: ${token}`);
    const key = token.slice(2);
    const next = rest[index + 1];
    options[key] = next && !next.startsWith('--') ? rest[++index] : true;
  }
  return { command, options };
}

function campaignId(options, prefix = 'cravis') {
  return options.campaign ?? `${prefix}-${new Date().toISOString().replace(/[:.]/g, '-')}`;
}

async function terminalConfirmer(promptId, text) {
  output.write(`\n${promptId}\n${text}\n\n`);
  const readline = createInterface({ input, output });
  try { return await readline.question(`Type exactly SEND ${promptId}: `); } finally { readline.close(); }
}

async function terminalAdjudicator() {
  const readline = createInterface({ input, output });
  try {
    const value = await readline.question('Automatic completion was not proven. Type one of COMPLETED MANUAL, PARTIAL MANUAL, TIMED OUT, or UNCERTAIN: ');
    return ({ 'COMPLETED MANUAL':'completed_manual', 'PARTIAL MANUAL':'partial_manual', 'TIMED OUT':'timed_out', 'UNCERTAIN':'uncertain' })[value] ?? 'uncertain';
  } finally { readline.close(); }
}

export async function main(argv = process.argv.slice(2)) {
  const { command, options } = parseArgs(argv);
  const root = evaluatorRoot();
  if (!command || command === 'help' || options.help) {
    output.write('Usage: node assistant/evaluations/cravis/cli.mjs <simulate|capture-session|recon|campaign|review|report> [--campaign <id>]\n');
    return 0;
  }
  const { loadConfiguration } = await import('./lib/config.mjs');
  const config = loadConfiguration(root);

  if (command === 'capture-session') {
    const { readOriginApprovals } = await import('./lib/config.mjs');
    const { captureSession } = await import('./lib/auth.mjs');
    const approvals = [...new Set([...config.targets.approvedOrigins, ...readOriginApprovals(resolve(root, '.auth/origin-approvals.json'))])];
    const path = await captureSession({ root, targetUrl: options.target ?? config.targets.defaultOrigin, approvedOrigins: approvals });
    output.write(`Session state saved beneath the evaluator auth directory: ${basename(path)}\n`);
    return 0;
  }

  if (command === 'simulate') {
    const { startSimulator } = await import('./lib/simulator.mjs');
    const { createCampaign, runPrompt, runZeroSendRecon } = await import('./lib/campaign.mjs');
    const simulator = await startSimulator();
    const id = campaignId(options, 'simulation');
    try {
      await runZeroSendRecon({ root, simulation: true, simulatorOrigin: simulator.origin });
      createCampaign(root, id, config, { simulation: true, targetUrl: simulator.origin });
      const result = await runPrompt({ root, campaignId: id, promptId: options.prompt ?? 'P01', confirmer: async (promptId) => `SEND ${promptId}`, browserOptions: { observation: { timeoutMs: 10000, stableMs: 500, pollMs: 50 } } });
      output.write(`${JSON.stringify(result, null, 2)}\n`);
    } finally { await simulator.close(); }
    return 0;
  }

  if (command === 'recon') {
    const { runZeroSendRecon } = await import('./lib/campaign.mjs');
    const result = await runZeroSendRecon({ root, targetUrl: options.target });
    output.write(`${JSON.stringify(result, null, 2)}\n`);
    return 0;
  }

  if (command === 'campaign') {
    const { createCampaign, runPrompt, campaignPaths } = await import('./lib/campaign.mjs');
    const id = campaignId(options);
    const predeclaredNaDimensions = options['na-dimensions'] ? String(options['na-dimensions']).split(',').map((value) => value.trim()).filter(Boolean) : [];
    if (!existsSync(campaignPaths(root, id).campaign)) createCampaign(root, id, config, { targetUrl: options.target ?? null, predeclaredNaDimensions });
    const promptId = options.prompt ?? 'P01';
    const result = await runPrompt({ root, campaignId: id, promptId, confirmer: terminalConfirmer, browserOptions: { headless: false, adjudicator: terminalAdjudicator } });
    output.write(`${JSON.stringify(result, null, 2)}\n`);
    return 0;
  }

  if (command === 'review') {
    if (!options.campaign) throw new Error('review requires --campaign <id>');
    const { campaignPaths } = await import('./lib/campaign.mjs');
    const { createReviewLock, writeReviewLock } = await import('./lib/review.mjs');
    const { readJson } = await import('./lib/config.mjs');
    const paths = campaignPaths(root, options.campaign);
    const review = readJson(paths.reviewInput);
    const campaign = readJson(paths.campaign);
    const scopePromptIds = review.scopePromptIds ?? ['P01','P02','P03','P04','P05','P06','P07','P08'];
    const lock = createReviewLock({ campaignId: campaign.id, reviewer: review.reviewer, scores: review.scores, overrideNotes: review.overrideNotes ?? [], scopePromptIds, predeclaredNaDimensions: campaign.predeclaredNaDimensions ?? [], rubric: config.rubric, paths: { promptConfigPath: config.paths.promptPath, rubricPath: config.paths.rubricPath, ledgerPath: paths.ledger, evidenceManifestPath: paths.evidenceManifest, promptResultsPath: paths.promptResults } });
    writeReviewLock(paths.reviewLock, lock);
    output.write(`Review lock created: ${paths.reviewLock}\nDigest: ${lock.digest}\nScope: ${scopePromptIds.join(', ')}\n`);
    return 0;
  }

  if (command === 'report') {
    if (!options.campaign) throw new Error('report requires --campaign <id>');
    const { campaignPaths } = await import('./lib/campaign.mjs');
    const { verifyReviewLock } = await import('./lib/review.mjs');
    const { generateReports, readCampaignReportInputs } = await import('./lib/reporting.mjs');
    const paths = campaignPaths(root, options.campaign);
    const reviewPaths = { promptConfigPath: config.paths.promptPath, rubricPath: config.paths.rubricPath, ledgerPath: paths.ledger, evidenceManifestPath: paths.evidenceManifest, promptResultsPath: paths.promptResults };
    const lock = verifyReviewLock(paths.reviewLock, { campaignId: options.campaign, paths: reviewPaths, rubric: config.rubric, requireComplete: true });
    const reportInputs = readCampaignReportInputs(paths.root);
    const outputRoot = resolve(root, 'reports', options.campaign); mkdirSync(outputRoot, { recursive: true });
    const files = generateReports({ outputRoot, rubric: config.rubric, lock, predeclaredNaDimensions: reportInputs.campaign.predeclaredNaDimensions, ...reportInputs });
    output.write(`${JSON.stringify(files, null, 2)}\n`);
    return 0;
  }
  throw new Error(`Unknown command: ${command}`);
}

if (import.meta.url === pathToFileURL(resolve(process.argv[1] ?? '')).href) {
  main().then((code) => { process.exitCode = code; }).catch((error) => { console.error(error.message); process.exitCode = 1; });
}
