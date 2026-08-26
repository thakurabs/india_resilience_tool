import { existsSync, mkdirSync, readFileSync } from 'node:fs';
import { basename, join, resolve } from 'node:path';
import { performance } from 'node:perf_hooks';
import { atomicWrite, canonicalJson, hashFile, hashJson, sha256 } from './canonical.mjs';
import { activeConversationIdentity, applyManualTerminalOutcome, lastUserIdentity, launchCampaignBrowser, observeResponse, promptIdentity, runRecon, segmentOffsets, uniqueVisible } from './browser.mjs';
import { loadConfiguration, readJson, readOriginApprovals, validateAdaptivePrompt } from './config.mjs';
import { ledgerDigest, persistTransition, recoverLedger } from './ledger.mjs';
import { assertContainedPath, assertNonSymlinkFile } from './paths.mjs';
import { confirmQuotaDecrement, parseQuota } from './quota.mjs';

export const EVALUATOR_ROOT = resolve(new URL('..', import.meta.url).pathname);

function safeCampaignId(value) {
  if (!/^[A-Za-z0-9][A-Za-z0-9._-]{0,79}$/.test(value)) throw new Error('Campaign ID contains unsafe characters');
  return value;
}

export function campaignPaths(root, campaignId) {
  const id = safeCampaignId(campaignId);
  const campaignRoot = resolve(root, 'runs', id);
  return {
    root: campaignRoot, campaign: join(campaignRoot, 'campaign.json'), ledger: join(campaignRoot, 'send-ledger.jsonl'),
    state: join(campaignRoot, 'campaign-state.json'), promptResults: join(campaignRoot, 'prompt-results.json'),
    timings: join(campaignRoot, 'timings.json'), evidenceManifest: join(campaignRoot, 'evidence-manifest.json'),
    classifications: join(campaignRoot, 'classifications.json'), reviewInput: join(campaignRoot, 'human-review.json'),
    reviewLock: join(campaignRoot, 'review-lock.json'), evidence: join(campaignRoot, 'evidence'), videos: join(campaignRoot, 'videos')
  };
}

function writeJson(path, value) { atomicWrite(path, `${canonicalJson(value)}\n`); }
function readJsonOr(path, fallback) { return existsSync(path) ? readJson(path) : fallback; }

export function createCampaign(root, campaignId, config, { simulation = false, targetUrl = null, predeclaredNaDimensions = [] } = {}) {
  const paths = campaignPaths(root, campaignId);
  if (existsSync(paths.campaign)) throw new Error(`Campaign already exists: ${campaignId}`);
  mkdirSync(paths.evidence, { recursive: true, mode: 0o700 });
  mkdirSync(paths.videos, { recursive: true, mode: 0o700 });
  const knownDimensions = new Set(config.rubric.dimensions.map((dimension) => dimension.id));
  if (predeclaredNaDimensions.some((id) => !knownDimensions.has(id))) throw new Error('Unknown predeclared N/A dimension');
  const campaign = {
    schemaVersion: 1, id: campaignId, scope: 'n=1 case study; not a general performance estimate',
    createdAt: new Date().toISOString(), simulation, targetUrl: targetUrl ?? config.targets.defaultOrigin,
    configHashes: config.hashes, predeclaredNaDimensions: [...new Set(predeclaredNaDimensions)].sort()
  };
  campaign.campaignHash = hashJson(campaign);
  writeJson(paths.campaign, campaign);
  writeJson(paths.promptResults, []); writeJson(paths.timings, []); writeJson(paths.evidenceManifest, []); writeJson(paths.classifications, []);
  writeJson(paths.state, { campaignHash: campaign.campaignHash, configHashes: config.hashes, ledgerHash: null, activePromptId: null });
  return { campaign, paths };
}

function requireOperatorAcknowledgement(root) {
  const path = assertNonSymlinkFile(resolve(root, '.auth'), resolve(root, '.auth/operator-ack.json'));
  const acknowledgement = readJson(path);
  for (const field of ['authorized', 'termsCompliant', 'quotaUnderstood', 'accountPolicyCompliant']) {
    if (acknowledgement[field] !== true) throw new Error(`Operator acknowledgement requires ${field}=true`);
  }
  return acknowledgement;
}

export function validateCampaign(root, campaignId, config) {
  const paths = campaignPaths(root, campaignId);
  const campaign = readJson(paths.campaign);
  const hashInput = { ...campaign }; delete hashInput.campaignHash;
  if (hashJson(hashInput) !== campaign.campaignHash) throw new Error('Campaign metadata hash mismatch');
  for (const [name, hash] of Object.entries(config.hashes)) if (campaign.configHashes[name] !== hash) throw new Error(`${name} configuration hash mismatch`);
  const state = readJson(paths.state);
  if (state.campaignHash !== campaign.campaignHash || JSON.stringify(state.configHashes) !== JSON.stringify(config.hashes)) throw new Error('Campaign state hash mismatch');
  const recovery = recoverLedger(paths.ledger);
  if (recovery.truncatedTail) {
    const validLedger = recovery.records.map((record) => `${canonicalJson(record)}\n`).join('');
    atomicWrite(paths.ledger, validLedger);
    state.activePromptId = recovery.records.at(-1)?.promptId ?? state.activePromptId;
    state.activePromptOutcome = 'uncertain'; state.blocked = true; state.blockReason = 'truncated_ledger_tail'; state.ledgerHash = ledgerDigest(paths.ledger);
    writeJson(paths.state, state);
    return { campaign, paths, state, recovery: { ...recovery, blocked: true } };
  }
  const currentLedgerHash = existsSync(paths.ledger) ? ledgerDigest(paths.ledger) : null;
  if (state.ledgerHash !== currentLedgerHash) throw new Error('Ledger hash mismatch');
  return { campaign, paths, state, recovery };
}

export function persistAdaptivePrompt(root, campaignId, record) {
  const config = loadConfiguration(root);
  validateAdaptivePrompt(record, config.prompts);
  const paths = campaignPaths(root, campaignId);
  const path = join(paths.root, `${record.id}.json`);
  if (existsSync(path)) throw new Error(`${record.id} is immutable once written`);
  writeJson(path, record);
  return path;
}

function persist(paths, state, event) {
  const record = persistTransition(paths.ledger, event);
  state.ledgerHash = ledgerDigest(paths.ledger); state.activePromptId = record.promptId;
  writeJson(paths.state, state);
  return record;
}

async function waitForExactLastMessage(page, selectors, identity, timeoutMs = 10000) {
  const start = performance.now();
  while (performance.now() - start < timeoutMs) {
    const observed = await lastUserIdentity(page, selectors);
    if (observed?.sha256 === identity.sha256 && observed.normalizedText === identity.normalizedText) return observed;
    await page.waitForTimeout(100);
  }
  return null;
}

export async function runPrompt({ root, campaignId, promptId, confirmer, browserOptions = {} }) {
  const config = loadConfiguration(root);
  const validated = validateCampaign(root, campaignId, config);
  const { campaign, paths, state, recovery } = validated;
  if (recovery.blocked || state.blocked) throw new Error('Further sends are blocked because prior submission safety is unresolved');
  if (!campaign.simulation) requireOperatorAcknowledgement(root);
  const prompt = config.prompts.fixedPrompts.find((item) => item.id === promptId);
  if (!prompt) throw new Error(`Unknown fixed prompt: ${promptId}`);
  if (readJson(paths.promptResults).some((result) => result.promptId === promptId)) throw new Error(`${promptId} already has a result; automatic retry is forbidden`);
  if (recovery.records.some((record) => record.promptId === promptId && ['click_attempt_persisted','click_dispatched','prompt_observed','quota_confirmed','response_started','completed_automatic','completed_manual','partial_manual','timed_out','uncertain'].includes(record.state))) {
    throw new Error(`${promptId} has already reached click_attempt_persisted; retry or resubmission is forbidden`);
  }
  const identity = promptIdentity(prompt.text);
  const approvalsPath = resolve(root, '.auth/origin-approvals.json');
  const approvedOrigins = [...new Set([...config.targets.approvedOrigins, ...readOriginApprovals(approvalsPath)])];
  const storageState = campaign.simulation ? undefined : assertNonSymlinkFile(resolve(root, '.auth'), resolve(root, '.auth/cravis-state.json'));
  const session = await launchCampaignBrowser({
    targetUrl: campaign.targetUrl, approvedOrigins, storageState, videoDir: paths.videos,
    selectors: config.targets.selectors, simulation: campaign.simulation, headless: campaign.simulation || browserOptions.headless,
    excludedPathPrefixes: config.targets.authExcludedPathPrefixes
  });
  const { page, context, browser, selectors } = session;
  try {
    const previousConfirmed = [...recovery.records].reverse().find((record) => record.state === 'quota_confirmed');
    if (previousConfirmed) {
      const last = await lastUserIdentity(page, selectors);
      if (!last || last.sha256 !== previousConfirmed.promptHash || last.normalizedText !== previousConfirmed.promptText) throw new Error('Active conversation does not match the last confirmed prompt');
    }
    const conversationId = await activeConversationIdentity(page, selectors);
    const quotaNode = await uniqueVisible(page, selectors.quota, { label: 'quota text' });
    const currentQuota = parseQuota(await quotaNode.innerText());
    if (!currentQuota.ok) throw new Error(`Cannot arm with invalid quota: ${currentQuota.reason}`);
    const lastRecord = recovery.records.at(-1);
    let quotaBefore = currentQuota;
    if (lastRecord?.promptId === promptId && lastRecord.state === 'armed_persisted') {
      if (lastRecord.promptHash !== identity.sha256 || lastRecord.promptText !== identity.normalizedText) throw new Error('Armed prompt identity mismatch');
      if (lastRecord.conversationId !== conversationId) throw new Error('Armed conversation identity mismatch');
      quotaBefore = lastRecord.quotaBefore;
      if (!quotaBefore?.ok || quotaBefore.current !== currentQuota.current || quotaBefore.maximum !== currentQuota.maximum) throw new Error('Quota changed while prompt was armed; send is blocked');
    } else if (lastRecord?.promptId === promptId && lastRecord.state === 'prepared') {
      persist(paths, state, { campaignId, promptId, state: 'armed_persisted', conversationId, promptHash: identity.sha256, promptText: identity.normalizedText, quotaBefore });
    } else {
      persist(paths, state, { campaignId, promptId, state: 'prepared', conversationId });
      persist(paths, state, { campaignId, promptId, state: 'armed_persisted', conversationId, promptHash: identity.sha256, promptText: identity.normalizedText, quotaBefore });
    }
    const confirmation = await confirmer(promptId, prompt.text);
    if (confirmation !== `SEND ${promptId}`) throw new Error(`Confirmation must be exactly SEND ${promptId}`);
    const input = await uniqueVisible(page, selectors.promptInput, { label: 'active prompt input' });
    const submit = await uniqueVisible(page, selectors.submit, { enabled: true, label: 'submit control' });
    let uploadFixture = null;
    if (prompt.fixture) {
      const fixturePath = assertNonSymlinkFile(root, resolve(root, prompt.fixture));
      const upload = await uniqueVisible(page, selectors.upload, { label: 'upload control' });
      await upload.setInputFiles(fixturePath);
      uploadFixture = { path: prompt.fixture, sha256: hashFile(fixturePath) };
    }
    await input.fill(prompt.text);
    const activeConversation = await uniqueVisible(page, selectors.activeConversation, { label: 'active conversation' });
    const baselineResponseCount = await activeConversation.locator(selectors.response).count();
    persist(paths, state, { campaignId, promptId, state: 'click_attempt_persisted', conversationId, promptHash: identity.sha256, promptText: identity.normalizedText });
    const t0 = performance.now();
    await submit.click();
    persist(paths, state, { campaignId, promptId, state: 'click_dispatched', conversationId, promptHash: identity.sha256, promptText: identity.normalizedText, T0: t0 });
    const responsePromise = observeResponse(page, selectors, t0, { ...browserOptions.observation, baselineResponseCount });
    const observed = await waitForExactLastMessage(page, selectors, identity);
    if (!observed) {
      persist(paths, state, { campaignId, promptId, state: 'uncertain', reason: 'prompt_not_observed', promptHash: identity.sha256, promptText: identity.normalizedText });
      throw new Error('Submission uncertain: exact prompt was not observed in the active conversation');
    }
    persist(paths, state, { campaignId, promptId, state: 'prompt_observed', promptHash: identity.sha256, promptText: identity.normalizedText });
    const quotaResult = await confirmQuotaDecrement(quotaBefore, () => quotaNode.innerText(), { selector: selectors.quota });
    if (!quotaResult.ok) {
      persist(paths, state, { campaignId, promptId, state: 'uncertain', reason: `quota_${quotaResult.reason}`, promptHash: identity.sha256, promptText: identity.normalizedText, quotaObservations: quotaResult.observations });
      throw new Error(`Submission uncertain: quota confirmation failed (${quotaResult.reason})`);
    }
    persist(paths, state, { campaignId, promptId, state: 'quota_confirmed', promptHash: identity.sha256, promptText: identity.normalizedText, quota: quotaResult.after });
    let responseResult = await responsePromise;
    if (responseResult.outcome === 'timed_out' && browserOptions.adjudicator) {
      responseResult = applyManualTerminalOutcome(responseResult, await browserOptions.adjudicator(responseResult));
    }
    if (responseResult.timing.T1 != null) persist(paths, state, { campaignId, promptId, state: 'response_started', promptHash: identity.sha256, promptText: identity.normalizedText, T1: responseResult.timing.T1 });
    persist(paths, state, { campaignId, promptId, state: responseResult.outcome, promptHash: identity.sha256, promptText: identity.normalizedText, timing: responseResult.timing, completionSignals: responseResult.signals });
    const video = page.video();
    await context.close();
    const videoFile = video ? basename(await video.path()) : null;
    const offsets = segmentOffsets(responseResult.timing, session.segmentStartedAt);
    const evidence = {
      promptId, promptHash: identity.sha256, responseHash: responseResult.responseHash, responseText: responseResult.responseText, uploadFixture,
      quota: { before: quotaBefore, after: quotaResult.after }, conversationId, blockedOrigins: [...new Set(session.blockedOrigins)],
      network: session.collectors.network, errors: session.collectors.errors, videoSegment: {
        id: session.segmentId, file: videoFile ? `videos/${videoFile}` : null,
        startedAtMonotonicMs: session.segmentStartedAt, ...offsets
      }
    };
    const evidencePath = join(paths.evidence, `${promptId}.json`); writeJson(evidencePath, evidence);
    const results = readJson(paths.promptResults); results.push({ promptId, outcome: responseResult.outcome, responseHash: responseResult.responseHash, evidenceRef: `evidence/${promptId}.json`, expectedEvidenceIds: [`${promptId}:response`, `${promptId}:ledger`, `${promptId}:quota`] }); writeJson(paths.promptResults, results);
    const timings = readJson(paths.timings); timings.push({
      promptId, ...responseResult.timing, videoSegmentId: session.segmentId,
      ...offsets
    }); writeJson(paths.timings, timings);
    const finalLedgerHash = ledgerDigest(paths.ledger);
    const manifest = readJson(paths.evidenceManifest).map((item) => item.id.endsWith(':ledger') ? { ...item, sha256: finalLedgerHash } : item);
    manifest.push({ id: `${promptId}:response`, present: true, path: `evidence/${promptId}.json`, sha256: sha256(readFileSync(evidencePath)) }, { id: `${promptId}:ledger`, present: true, path: 'send-ledger.jsonl', sha256: finalLedgerHash }, { id: `${promptId}:quota`, present: true, path: `evidence/${promptId}.json`, sha256: sha256(canonicalJson(quotaResult.after)) }); writeJson(paths.evidenceManifest, manifest);
    return { campaignId, promptId, outcome: responseResult.outcome, quota: quotaResult.after, timing: responseResult.timing };
  } finally {
    await context.close().catch(() => {});
    await browser.close().catch(() => {});
  }
}

export async function runZeroSendRecon({ root, targetUrl = null, simulation = false, simulatorOrigin = null }) {
  const config = loadConfiguration(root);
  const approvalsPath = resolve(root, '.auth/origin-approvals.json');
  const approvedOrigins = [...new Set([...config.targets.approvedOrigins, ...readOriginApprovals(approvalsPath)])];
  const actualTarget = simulatorOrigin ?? targetUrl ?? config.targets.defaultOrigin;
  const storageState = simulation ? undefined : assertNonSymlinkFile(resolve(root, '.auth'), resolve(root, '.auth/cravis-state.json'));
  const reconRoot = resolve(root, 'runs', 'recon'); mkdirSync(reconRoot, { recursive: true, mode: 0o700 });
  const session = await launchCampaignBrowser({ targetUrl: actualTarget, approvedOrigins, storageState, videoDir: join(reconRoot, 'videos'), selectors: config.targets.selectors, simulation, headless: simulation, excludedPathPrefixes: config.targets.authExcludedPathPrefixes });
  try {
    const result = await runRecon(session.page, config.targets.selectors);
    const path = join(reconRoot, `recon-${Date.now()}.json`); writeJson(path, { ...result, targetOrigin: new URL(actualTarget).origin, discoveredBlockedOrigins: [...new Set(session.blockedOrigins)], timestamp: new Date().toISOString() });
    return { ...result, path };
  } finally { await session.context.close().catch(() => {}); await session.browser.close().catch(() => {}); }
}
