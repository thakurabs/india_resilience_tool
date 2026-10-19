import { closeSync, existsSync, fsyncSync, openSync, readFileSync, writeSync } from 'node:fs';
import { atomicWrite, canonicalJson, sha256 } from './canonical.mjs';

export const SEND_STATES = Object.freeze([
  'prepared', 'armed_persisted', 'click_attempt_persisted', 'click_dispatched',
  'prompt_observed', 'quota_confirmed', 'response_started', 'completed_automatic',
  'completed_manual', 'partial_manual', 'timed_out', 'uncertain'
]);

const TERMINAL = new Set(['completed_automatic', 'completed_manual', 'partial_manual', 'timed_out', 'uncertain']);
const NEXT = new Map([
  ['prepared', new Set(['armed_persisted', 'uncertain'])],
  ['armed_persisted', new Set(['click_attempt_persisted', 'uncertain'])],
  ['click_attempt_persisted', new Set(['click_dispatched', 'uncertain'])],
  ['click_dispatched', new Set(['prompt_observed', 'uncertain'])],
  ['prompt_observed', new Set(['quota_confirmed', 'uncertain'])],
  ['quota_confirmed', new Set(['response_started', 'completed_manual', 'partial_manual', 'timed_out', 'uncertain'])],
  ['response_started', new Set(['completed_automatic', 'completed_manual', 'partial_manual', 'timed_out', 'uncertain'])]
]);

export function appendLedgerEvent(path, event) {
  if (!SEND_STATES.includes(event.state)) throw new Error(`Unknown send state: ${event.state}`);
  const record = { schemaVersion: 1, ...event, timestamp: event.timestamp ?? new Date().toISOString() };
  const line = `${canonicalJson(record)}\n`;
  const descriptor = openSync(path, 'a', 0o600);
  try {
    const bytes = Buffer.from(line, 'utf8');
    let offset = 0;
    while (offset < bytes.length) offset += writeSync(descriptor, bytes, offset, bytes.length - offset);
    fsyncSync(descriptor);
  } finally {
    closeSync(descriptor);
  }
  return record;
}

export function validateTransition(previous, next) {
  if (!previous) {
    if (next.state !== 'prepared') throw new Error('First ledger state must be prepared');
    return;
  }
  if (previous.campaignId !== next.campaignId || previous.promptId !== next.promptId) {
    if (!TERMINAL.has(previous.state) || next.state !== 'prepared') throw new Error('Cannot change prompt before a terminal state');
    return;
  }
  if (!NEXT.get(previous.state)?.has(next.state)) throw new Error(`Invalid transition ${previous.state} -> ${next.state}`);
  if (previous.promptHash && next.promptHash && previous.promptHash !== next.promptHash) throw new Error('Prompt hash changed after arming');
}

export function persistTransition(path, event) {
  const recovered = recoverLedger(path, { quarantine: false });
  if (recovered.truncatedTail) throw new Error('Ledger has a truncated tail; further sends are blocked');
  validateTransition(recovered.records.at(-1), event);
  return appendLedgerEvent(path, event);
}

export function recoverLedger(path, { quarantine = true } = {}) {
  if (!existsSync(path)) return { records: [], truncatedTail: null, blocked: false };
  const raw = readFileSync(path);
  const finalNewline = raw.length === 0 || raw.at(-1) === 0x0a;
  const parts = raw.toString('utf8').split('\n');
  if (finalNewline) parts.pop();
  const records = [];
  let tailStart = 0;
  for (const part of parts) {
    const byteLength = Buffer.byteLength(part, 'utf8') + 1;
    try {
      const record = JSON.parse(part);
      validateTransition(records.at(-1), record);
      records.push(record);
      tailStart += byteLength;
    } catch {
      break;
    }
  }
  const truncatedTail = tailStart < raw.length ? raw.subarray(tailStart) : null;
  let quarantinePath = null;
  if (truncatedTail?.length && quarantine) {
    quarantinePath = `${path}.quarantine-${sha256(truncatedTail).slice(0, 12)}`;
    atomicWrite(quarantinePath, truncatedTail);
  }
  const last = records.at(-1);
  const submissionStarted = last && SEND_STATES.indexOf(last.state) >= SEND_STATES.indexOf('click_attempt_persisted');
  const proven = records.some((r) => r.promptId === last?.promptId && r.state === 'prompt_observed') &&
    records.some((r) => r.promptId === last?.promptId && r.state === 'quota_confirmed');
  return { records, truncatedTail, quarantinePath, blocked: Boolean(truncatedTail?.length || (submissionStarted && !proven)) };
}

export function ledgerDigest(path) {
  return sha256(readFileSync(path));
}

