import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { spawnSync } from 'node:child_process';
import { hashJson } from '../lib/canonical.mjs';
import { loadConfiguration, validateAdaptivePrompt } from '../lib/config.mjs';

const root = resolve(new URL('..', import.meta.url).pathname);

test('configuration fixes eight prompts, fourteen dimensions, and weights totaling 100', () => {
  const config = loadConfiguration(root);
  assert.equal(config.prompts.fixedPrompts.length,8);
  assert.equal(config.rubric.dimensions.length,14);
  assert.equal(config.rubric.dimensions.reduce((sum,item)=>sum+item.weight,0),100);
});

test('adaptive prompt requires immutable metadata, hash, and two A09 weaknesses', () => {
  const config = loadConfiguration(root);
  const record = { id:'A09',text:'test',author:'RT',rationale:'observed',intendedWeaknesses:['missing data','ambiguous geography'],dimensions:['failure_honesty'],createdAt:'2026-01-01T00:00:00Z' };
  record.sha256=hashJson(record);
  assert.equal(validateAdaptivePrompt(record,config.prompts).id,'A09');
  const bad={...record,intendedWeaknesses:['missing data']}; const noHash={...bad}; delete noHash.sha256; bad.sha256=hashJson(noHash);
  assert.throws(()=>validateAdaptivePrompt(bad,config.prompts),/at least two/);
});

test('CLI import has no browser, filesystem, or network side effects', () => {
  const cwd=mkdtempSync(`${tmpdir()}/cravis-import-`);
  const cli=pathToFileURL(resolve(root,'cli.mjs')).href;
  const result=spawnSync(process.execPath,['--input-type=module','-e',`await import(${JSON.stringify(cli)})`],{cwd,encoding:'utf8'});
  assert.equal(result.status,0,result.stderr);
  assert.deepEqual(readdirSync(cwd),[]);
});

