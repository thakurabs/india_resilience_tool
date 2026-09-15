import test from 'node:test';
import assert from 'node:assert/strict';
import { appendFileSync, mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { tmpdir } from 'node:os';
import { createCampaign, validateCampaign } from '../lib/campaign.mjs';
import { loadConfiguration } from '../lib/config.mjs';
import { ledgerDigest, persistTransition } from '../lib/ledger.mjs';

const evaluatorRoot = resolve(new URL('..', import.meta.url).pathname);
function fixture() {
  const root = mkdtempSync(join(tmpdir(), 'cravis-campaign-'));
  const source = loadConfiguration(evaluatorRoot);
  const config = { ...source, paths: source.paths };
  const created = createCampaign(root, 'C1', config, { simulation:true, targetUrl:'http://127.0.0.1:1' });
  return { root, config, ...created };
}

test('campaign and prompt/rubric/target config hashes are validated on resume', () => {
  const { root, config } = fixture();
  assert.equal(validateCampaign(root,'C1',config).campaign.id,'C1');
  for (const key of ['prompts','rubric','targets']) {
    const changed={...config,hashes:{...config.hashes,[key]:'changed'}};
    assert.throws(()=>validateCampaign(root,'C1',changed),new RegExp(`${key} configuration hash mismatch`));
  }
});

test('ledger hash mutation is rejected', () => {
  const { root, config, paths } = fixture();
  persistTransition(paths.ledger,{campaignId:'C1',promptId:'P01',state:'prepared'});
  const state=JSON.parse(readFileSync(paths.state,'utf8')); state.ledgerHash=ledgerDigest(paths.ledger); writeFileSync(paths.state,JSON.stringify(state));
  writeFileSync(paths.ledger,readFileSync(paths.ledger,'utf8').replace('"C1"','"D1"'));
  assert.throws(()=>validateCampaign(root,'C1',config),/Ledger hash mismatch/);
});

test('truncated campaign ledger is quarantined, repaired to valid lines, marked uncertain, and blocked', () => {
  const { root, config, paths } = fixture();
  persistTransition(paths.ledger,{campaignId:'C1',promptId:'P01',state:'prepared'});
  const state=JSON.parse(readFileSync(paths.state,'utf8')); state.ledgerHash=ledgerDigest(paths.ledger); writeFileSync(paths.state,JSON.stringify(state));
  appendFileSync(paths.ledger,'{"campaignId":"C1","promptId":"P01","state":"armed_');
  const resumed=validateCampaign(root,'C1',config);
  assert.equal(resumed.state.blocked,true);
  assert.equal(resumed.state.activePromptOutcome,'uncertain');
  assert.doesNotThrow(()=>JSON.parse(readFileSync(paths.ledger,'utf8').trim()));
  assert.ok(resumed.recovery.quarantinePath);
});
