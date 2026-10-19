import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { tmpdir } from 'node:os';
import { hashFile } from '../lib/canonical.mjs';
import { loadConfiguration } from '../lib/config.mjs';
import { aggregateScores, expectedCells, validateHumanScores } from '../lib/scoring.mjs';
import { createReviewLock, verifyReviewLock, writeReviewLock } from '../lib/review.mjs';
import { generateReports } from '../lib/reporting.mjs';

const root = resolve(new URL('..', import.meta.url).pathname);
const config = loadConfiguration(root);
function completeScores(value = 3) { return expectedCells(config.rubric).map((cell) => ({ ...cell, score: value, confirmedBy: 'RT' })); }

test('rubric applicability, aggregation, N/A coverage, and adaptive separation are deterministic', () => {
  const scores = completeScores(3);
  const aggregate = aggregateScores(config.rubric, scores);
  assert.equal(aggregate.headlineScore, 100);
  assert.equal(aggregate.applicableWeightCoverage, 1);
  assert.equal(aggregate.adaptiveExcluded, true);
  const naDimensions = ['correctness','spatial_temporal'];
  const lowCoverage = aggregateScores(config.rubric, scores.filter((score) => !naDimensions.includes(score.dimensionId)), { predeclaredNaDimensions: naDimensions });
  assert.equal(lowCoverage.applicableWeightCoverage, 0.75);
  assert.equal(lowCoverage.headlineScore, null);
  assert.throws(() => validateHumanScores(config.rubric, [...scores, { promptId:'A09', dimensionId:'interpretation', score:3, confirmedBy:'RT' }]), /outside the review scope|Duplicate/);
  const failed = completeScores(3); failed.find((score) => score.promptId === 'P01').score = 0;
  assert.ok(aggregateScores(config.rubric, failed).headlineScore < 100, 'failed runs score zero rather than N/A');
});

function reviewFixture() {
  const dir = mkdtempSync(join(tmpdir(), 'cravis-review-'));
  const paths = {};
  for (const name of ['promptConfig','rubric','ledger','evidenceManifest','promptResults']) {
    paths[`${name}Path`] = join(dir, `${name}.json`); writeFileSync(paths[`${name}Path`], `${name}\n`);
  }
  return { dir, paths };
}

test('review lock validates hashes and is invalidated by evidence changes', () => {
  const { dir, paths } = reviewFixture();
  const lock = createReviewLock({ campaignId:'C1', reviewer:'RT', scores:completeScores(), paths, rubric:config.rubric, timestamp:'2026-01-01T00:00:00Z' });
  const lockPath = join(dir, 'review-lock.json'); writeReviewLock(lockPath, lock);
  assert.equal(verifyReviewLock(lockPath, { campaignId:'C1', paths, rubric:config.rubric }).digest, lock.digest);
  writeFileSync(paths.evidenceManifestPath, 'mutated\n');
  assert.throws(() => verifyReviewLock(lockPath, { campaignId:'C1', paths, rubric:config.rubric }), /evidenceManifest change/);
});

test('pilot P01 lock is allowed for review but rejected for final report', () => {
  const { dir, paths } = reviewFixture();
  const scores = expectedCells(config.rubric, ['P01']).map((cell) => ({ ...cell, score:2, confirmedBy:'RT' }));
  const lock = createReviewLock({ campaignId:'C1', reviewer:'RT', scores, scopePromptIds:['P01'], paths, rubric:config.rubric });
  const lockPath = join(dir, 'pilot.json'); writeReviewLock(lockPath, lock);
  assert.equal(verifyReviewLock(lockPath, { campaignId:'C1', paths, rubric:config.rubric, requireComplete:false }).scopePromptIds[0], 'P01');
  assert.throws(() => verifyReviewLock(lockPath, { campaignId:'C1', paths, rubric:config.rubric, requireComplete:true }), /complete P01-P08/);
});

test('reports are deterministic, ordered, atomic, and emit all durable files', () => {
  const dir = mkdtempSync(join(tmpdir(), 'cravis-report-'));
  const lock = { digest:'lock', scores:completeScores() };
  const args = {
    outputRoot:dir, campaign:{id:'C1'}, rubric:config.rubric, lock,
    promptResults:[{promptId:'P02',outcome:'completed_automatic'},{promptId:'P01',outcome:'completed_automatic'}],
    timings:[{promptId:'P02',T0:2,T3:4},{promptId:'P01',T0:1,T3:2}], evidenceManifest:[],
    classifications:[{capability:'Tables',claim_status:'advertised',evidence_status:'demonstrated',irt_mapping:'planned',irt_disposition:'match',evidence_refs:['P01:response'],reviewer_rationale:'Observed in the locked response.'}]
  };
  const files = generateReports(args);
  const first = Object.fromEntries(Object.entries(files).map(([name,path]) => [name, hashFile(path)]));
  generateReports(args);
  const second = Object.fromEntries(Object.entries(files).map(([name,path]) => [name, hashFile(path)]));
  assert.deepEqual(first, second);
  assert.deepEqual(Object.keys(files).sort(), ['assessment','gapMatrix','promptResults','scores','timings']);
  assert.ok(readFileSync(files.promptResults,'utf8').indexOf('P01') < readFileSync(files.promptResults,'utf8').indexOf('P02'));
});
