import test from 'node:test';
import assert from 'node:assert/strict';
import { appendFileSync, mkdtempSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { appendLedgerEvent, persistTransition, recoverLedger, SEND_STATES } from '../lib/ledger.mjs';
import { confirmQuotaDecrement, parseQuota, quotaPollDelays } from '../lib/quota.mjs';

function tempPath(name = 'ledger.jsonl') { return join(mkdtempSync(join(tmpdir(), 'cravis-ledger-')), name); }
function event(state) { return { campaignId: 'C1', promptId: 'P01', state, promptHash: state === 'prepared' ? undefined : 'abc', promptText: state === 'prepared' ? undefined : 'prompt' }; }

test('crash injection at each send transition produces the required block state', () => {
  const sequence = ['prepared','armed_persisted','click_attempt_persisted','click_dispatched','prompt_observed','quota_confirmed','response_started','completed_automatic'];
  for (let stop = 0; stop < sequence.length; stop += 1) {
    const path = tempPath();
    for (const state of sequence.slice(0, stop + 1)) persistTransition(path, event(state));
    const recovered = recoverLedger(path, { quarantine: false });
    assert.equal(recovered.records.at(-1).state, sequence[stop]);
    const shouldBlock = ['click_attempt_persisted','click_dispatched','prompt_observed'].includes(sequence[stop]);
    assert.equal(recovered.blocked, shouldBlock, `unexpected crash block for ${sequence[stop]}`);
  }
});

test('all terminal outcomes are accepted after response_started', () => {
  for (const terminal of ['completed_automatic','completed_manual','partial_manual','timed_out','uncertain']) {
    const path = tempPath();
    for (const state of ['prepared','armed_persisted','click_attempt_persisted','click_dispatched','prompt_observed','quota_confirmed','response_started',terminal]) persistTransition(path, event(state));
    assert.equal(recoverLedger(path).records.at(-1).state, terminal);
  }
});

test('invalid transitions, prompt mutation, and automatic resubmission are rejected', () => {
  const path = tempPath();
  persistTransition(path, event('prepared'));
  persistTransition(path, event('armed_persisted'));
  assert.throws(() => persistTransition(path, { ...event('click_attempt_persisted'), promptHash: 'changed' }), /Prompt hash changed/);
  assert.throws(() => persistTransition(path, { ...event('prepared'), promptId: 'P02' }), /terminal state/);
});

test('an armed prompt can remain immutable without being classified as a send attempt', () => {
  const path = tempPath();
  persistTransition(path, event('prepared'));
  persistTransition(path, { ...event('armed_persisted'), quotaBefore: parseQuota('10/10'), conversationId:'c1' });
  const recovered = recoverLedger(path);
  assert.equal(recovered.blocked,false);
  assert.equal(recovered.records.at(-1).promptHash,'abc');
});

test('truncated final ledger tail preserves valid records and quarantines raw bytes', () => {
  const path = tempPath();
  persistTransition(path, event('prepared'));
  appendFileSync(path, '{"campaignId":"C1","promptId":"P01","state":"armed_');
  const recovery = recoverLedger(path);
  assert.equal(recovery.records.length, 1);
  assert.equal(recovery.blocked, true);
  assert.ok(recovery.quarantinePath);
  assert.match(readFileSync(recovery.quarantinePath, 'utf8'), /armed_/);
});

test('quota parser is unambiguous and poll schedule spans thirty seconds', () => {
  assert.deepEqual(parseQuota('Quota 9 / 10'), { ok: true, raw: 'Quota 9 / 10', current: 9, maximum: 10, parserVersion: 'quota-v1' });
  assert.equal(parseQuota('9/10 and 8/10').reason, 'ambiguous');
  assert.equal(parseQuota('9/10 8/10').reason, 'ambiguous');
  assert.equal(parseQuota('unknown').reason, 'malformed');
  assert.equal(quotaPollDelays().reduce((a,b) => a+b, 0), 30000);
});

test('quota confirmation handles delayed, malformed, unchanged, and multi-decrement observations', async () => {
  const before = parseQuota('10 / 10');
  let values = ['bad', '10 / 10', '9 / 10'];
  const delayed = await confirmQuotaDecrement(before, async () => values.shift(), { sleep: async () => {}, now: () => 'now' });
  assert.equal(delayed.ok, true);
  values = ['8 / 10'];
  const multi = await confirmQuotaDecrement(before, async () => values[0], { sleep: async () => {} });
  assert.equal(multi.reason, 'multi_decrement');
  const unchanged = await confirmQuotaDecrement(before, async () => '10 / 10', { sleep: async () => {} });
  assert.equal(unchanged.reason, 'unchanged');
  const malformed = await confirmQuotaDecrement(before, async () => 'none', { sleep: async () => {} });
  assert.equal(malformed.reason, 'missing_or_malformed');
});
