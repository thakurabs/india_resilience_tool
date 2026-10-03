import { existsSync, readFileSync } from 'node:fs';
import { basename, resolve } from 'node:path';
import { atomicWrite, canonicalJson, hashFile, hashJson } from './canonical.mjs';
import { validateHumanScores } from './scoring.mjs';

export function reviewedHashes({ promptConfigPath, rubricPath, ledgerPath, evidenceManifestPath, promptResultsPath }) {
  return {
    promptConfig: hashFile(promptConfigPath),
    rubric: hashFile(rubricPath),
    ledger: hashFile(ledgerPath),
    evidenceManifest: hashFile(evidenceManifestPath),
    promptResults: hashFile(promptResultsPath)
  };
}

export function createReviewLock({ campaignId, reviewer, scores, overrideNotes = [], paths, rubric, scopePromptIds = ['P01','P02','P03','P04','P05','P06','P07','P08'], predeclaredNaDimensions = [], timestamp = new Date().toISOString() }) {
  if (!campaignId || !reviewer) throw new Error('Campaign ID and reviewer identifier are required');
  validateHumanScores(rubric, scores, scopePromptIds, predeclaredNaDimensions);
  const lock = { schemaVersion: 1, campaignId, reviewer, timestamp, scopePromptIds, predeclaredNaDimensions, scores, overrideNotes, hashes: reviewedHashes(paths) };
  lock.digest = hashJson(lock);
  return lock;
}

export function writeReviewLock(path, lock) {
  atomicWrite(path, `${canonicalJson(lock)}\n`);
}

export function verifyReviewLock(path, { campaignId, paths, rubric, requireComplete = true }) {
  if (!existsSync(path)) throw new Error('Campaign is not review-locked');
  const lock = JSON.parse(readFileSync(path, 'utf8'));
  const digest = lock.digest;
  const digestInput = { ...lock };
  delete digestInput.digest;
  if (hashJson(digestInput) !== digest) throw new Error('Review lock digest mismatch');
  if (lock.campaignId !== campaignId) throw new Error('Review lock campaign mismatch');
  const requiredScope = ['P01','P02','P03','P04','P05','P06','P07','P08'];
  if (requireComplete && JSON.stringify(lock.scopePromptIds) !== JSON.stringify(requiredScope)) throw new Error('Final report requires a complete P01-P08 review lock');
  validateHumanScores(rubric, lock.scores, lock.scopePromptIds, lock.predeclaredNaDimensions ?? []);
  const current = reviewedHashes(paths);
  for (const [name, hash] of Object.entries(lock.hashes)) if (current[name] !== hash) throw new Error(`Review lock invalidated by ${name} change`);
  return lock;
}

export function lockPathFor(campaignRoot) {
  return resolve(campaignRoot, 'review-lock.json');
}

export function describeReviewPaths(paths) {
  return Object.fromEntries(Object.entries(paths).map(([key, value]) => [key, basename(value)]));
}
