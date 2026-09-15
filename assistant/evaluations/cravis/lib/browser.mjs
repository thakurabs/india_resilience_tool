import { mkdirSync } from 'node:fs';
import { join } from 'node:path';
import { performance } from 'node:perf_hooks';
import { sha256 } from './canonical.mjs';
import { confirmQuotaDecrement, parseQuota } from './quota.mjs';
import { installRequestPolicy, validateTargetUrl } from './policy.mjs';
import { redactText, sanitizeNetworkRecord } from './redaction.mjs';

export function normalizePromptText(value) {
  return String(value ?? '').normalize('NFKC').replace(/\s+/g, ' ').trim();
}

export function promptIdentity(value) {
  const normalizedText = normalizePromptText(value);
  return { normalizedText, sha256: sha256(normalizedText) };
}

async function visibleElements(locator) {
  const count = await locator.count();
  const visible = [];
  for (let index = 0; index < count; index += 1) if (await locator.nth(index).isVisible()) visible.push(locator.nth(index));
  return visible;
}

export async function uniqueVisible(page, selector, { enabled = false, label = selector } = {}) {
  const visible = await visibleElements(page.locator(selector));
  const candidates = enabled ? await Promise.all(visible.map(async (item) => ({ item, enabled: await item.isEnabled() }))) : visible.map((item) => ({ item, enabled: true }));
  const matching = candidates.filter((candidate) => candidate.enabled).map((candidate) => candidate.item);
  if (matching.length !== 1) throw new Error(`Expected exactly one visible${enabled ? ' enabled' : ''} ${label}; found ${matching.length}`);
  return matching[0];
}

export async function lastUserIdentity(page, selectors) {
  const active = await uniqueVisible(page, selectors.activeConversation, { label: 'active conversation' });
  const messages = active.locator(selectors.lastUserMessage);
  const count = await messages.count();
  if (!count) return null;
  return promptIdentity(await messages.nth(count - 1).innerText());
}

export async function activeConversationIdentity(page, selectors) {
  const active = await uniqueVisible(page, selectors.activeConversation, { label: 'active conversation' });
  return await active.getAttribute('data-conversation-id') ?? await active.getAttribute('id') ?? sha256(await active.innerText());
}

async function status(locator) {
  try { return (await visibleElements(locator)).length ? 'found' : 'not_observable'; } catch { return 'ambiguous'; }
}

export async function runRecon(page, selectors, { settleMs = 500 } = {}) {
  const input = await uniqueVisible(page, selectors.promptInput, { label: 'active prompt input' });
  const submit = await uniqueVisible(page, selectors.submit, { enabled: true, label: 'submit control' });
  const quotaNode = await uniqueVisible(page, selectors.quota, { label: 'quota text' });
  const beforeQuota = parseQuota(await quotaNode.innerText());
  if (!beforeQuota.ok) throw new Error(`Quota is not unambiguously parseable: ${beforeQuota.reason}`);
  const beforeLast = await lastUserIdentity(page, selectors);
  const conversationId = await activeConversationIdentity(page, selectors);
  const beforeActivations = await page.evaluate(() => Number(globalThis.__cravisDriverSendActivations ?? 0));
  await page.waitForTimeout(settleMs);
  const afterQuota = parseQuota(await quotaNode.innerText());
  const afterLast = await lastUserIdentity(page, selectors);
  const afterActivations = await page.evaluate(() => Number(globalThis.__cravisDriverSendActivations ?? 0));
  const unchanged = beforeQuota.ok && afterQuota.ok && beforeQuota.current === afterQuota.current && beforeQuota.maximum === afterQuota.maximum;
  if (!unchanged || JSON.stringify(beforeLast) !== JSON.stringify(afterLast) || beforeActivations !== afterActivations) throw new Error('Recon zero-send proof failed');
  return {
    zeroSendProven: true, quota: beforeQuota, lastUserMessage: beforeLast, conversationId,
    controls: { promptInput: 'found', submit: 'found', idleStop: await status(page.locator(selectors.busy)), upload: await status(page.locator(selectors.upload)) },
    optional: {
      table: await status(page.locator(selectors.table)), chart: await status(page.locator(selectors.chart)), map: await status(page.locator(selectors.map)),
      sources: await status(page.locator(selectors.sources)), export: await status(page.locator(selectors.export)), followUps: 'not_observable', workflowStatuses: 'not_observable'
    },
    proof: { quotaUnchanged: true, lastUserMessageUnchanged: true, driverSendActivations: afterActivations - beforeActivations, sendLedgerRecords: 0 },
    inputTag: await input.evaluate((node) => node.tagName), submitTag: await submit.evaluate((node) => node.tagName)
  };
}

export function attachSanitizedCollectors(page, { excludedPathPrefixes = [] } = {}) {
  const network = [];
  const errors = [];
  const starts = new WeakMap();
  page.on('request', (request) => starts.set(request, performance.now()));
  page.on('response', async (response) => {
    const request = response.request();
    const record = sanitizeNetworkRecord({ url: request.url(), method: request.method(), resourceType: request.resourceType(), status: response.status(), durationMs: Math.round(performance.now() - (starts.get(request) ?? performance.now())) }, excludedPathPrefixes);
    if (record) network.push(record);
  });
  page.on('requestfailed', (request) => {
    const record = sanitizeNetworkRecord({ url: request.url(), method: request.method(), resourceType: request.resourceType(), durationMs: Math.round(performance.now() - (starts.get(request) ?? performance.now())), failureClass: request.failure()?.errorText ?? 'request_failed' }, excludedPathPrefixes);
    if (record) network.push(record);
  });
  page.on('console', (message) => errors.push({ type: 'console', level: message.type(), text: redactText(message.text()) }));
  page.on('pageerror', (error) => errors.push({ type: 'pageerror', text: redactText(error.message) }));
  return { network, errors };
}

export async function observeResponse(page, selectors, t0, { timeoutMs = 300000, stableMs = 3000, pollMs = 250, baselineResponseCount = 0 } = {}) {
  const active = await uniqueVisible(page, selectors.activeConversation, { label: 'active conversation' });
  const response = active.locator(selectors.response);
  const started = performance.now();
  let lastSignature = '';
  let lastChange = performance.now();
  let busySeen = false;
  const timing = { T0: t0, T1: null, T2: null, T_visual: null, T3: null, T4: null };
  while (performance.now() - started < timeoutMs) {
    const count = await response.count();
    const hasCurrentResponse = count > baselineResponseCount;
    const current = hasCurrentResponse ? response.last() : null;
    const text = current ? normalizePromptText(await current.innerText().catch(() => '')) : '';
    const html = current ? await current.innerHTML().catch(() => '') : '';
    const signature = sha256(`${text}\0${html}`);
    const busy = (await visibleElements(page.locator(selectors.busy))).length > 0;
    if (busy) busySeen = true;
    if ((text || html) && timing.T1 == null) timing.T1 = performance.now();
    if (text.length >= 20 && timing.T2 == null) timing.T2 = performance.now();
    if (current && timing.T_visual == null) {
      const visualCount = await current.locator(`${selectors.table}, ${selectors.chart}, ${selectors.map}`).count().catch(() => 0);
      if (visualCount) timing.T_visual = performance.now();
    }
    if (signature !== lastSignature) { lastSignature = signature; lastChange = performance.now(); }
    if (current && timing.T4 == null && await current.locator(selectors.export).count().catch(() => 0)) timing.T4 = performance.now();
    if (busySeen && !busy && timing.T2 != null && performance.now() - lastChange >= stableMs) {
      timing.T3 = performance.now();
      return { outcome: 'completed_automatic', timing, signals: { busySeen, idleObserved: true, stableMs }, responseText: text, responseHash: sha256(text) };
    }
    await page.waitForTimeout(pollMs);
  }
  timing.T3 = performance.now();
  return { outcome: 'timed_out', timing, signals: { busySeen, idleObserved: false, stableMs: 0 }, responseText: '', responseHash: null };
}

export function applyManualTerminalOutcome(observation, outcome, at = performance.now()) {
  if (!['completed_manual', 'partial_manual', 'timed_out', 'uncertain'].includes(outcome)) throw new Error(`Invalid manual terminal outcome: ${outcome}`);
  return { ...observation, outcome, timing: { ...observation.timing, T3: at }, manualAdjudication: true, evidenceConfidenceUpgraded: false };
}

export function segmentOffsets(timing, segmentStartedAt) {
  return {
    T0OffsetMs: timing.T0 == null ? null : timing.T0 - segmentStartedAt,
    T3OffsetMs: timing.T3 == null ? null : timing.T3 - segmentStartedAt
  };
}

export async function launchCampaignBrowser({ targetUrl, approvedOrigins, storageState, videoDir, selectors, simulation = false, headless = false, excludedPathPrefixes = [] }) {
  validateTargetUrl(targetUrl, approvedOrigins, { simulation });
  const { chromium } = await import('playwright');
  const browser = await chromium.launch({ headless });
  mkdirSync(videoDir, { recursive: true, mode: 0o700 });
  const segmentStartedAt = performance.now();
  const context = await browser.newContext({ storageState, recordVideo: { dir: videoDir } });
  const page = await context.newPage();
  const blockedOrigins = [];
  await installRequestPolicy(page, approvedOrigins, { simulation, onBlocked: (origin) => blockedOrigins.push(origin) });
  const collectors = attachSanitizedCollectors(page, { excludedPathPrefixes });
  await page.goto(targetUrl, { waitUntil: 'domcontentloaded' });
  return { browser, context, page, blockedOrigins, collectors, selectors, segmentId: `segment-${Date.now()}`, segmentStartedAt };
}
