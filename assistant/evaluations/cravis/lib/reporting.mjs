import { mkdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { atomicWrite, canonicalJson } from './canonical.mjs';
import { rowsToCsv } from './csv.mjs';
import { aggregateScores, evidenceCompleteness } from './scoring.mjs';

export const CAPABILITY_AXES = Object.freeze({
  claim_status: ['advertised','not_advertised','unknown'],
  evidence_status: ['verified','demonstrated','not_demonstrated','failed','unsupported'],
  irt_mapping: ['already_supported','planned','gap','not_relevant','methodology_conflict'],
  irt_disposition: ['match','exceed','do_not_copy','defer']
});

function validateClassifications(rows) {
  for (const row of rows) {
    for (const [axis, values] of Object.entries(CAPABILITY_AXES)) if (!values.includes(row[axis])) throw new Error(`Invalid ${axis}: ${row[axis]}`);
    if (!row.evidence_refs?.length || !row.reviewer_rationale?.trim()) throw new Error(`Classification ${row.capability ?? '<unknown>'} lacks evidence or rationale`);
  }
}

function markdownReport(campaign, aggregate, completeness, classifications) {
  const score = aggregate.headlineScore == null ? 'Not published (coverage below threshold)' : aggregate.headlineScore.toFixed(2);
  const lines = [
    '# CRAVIS Capability Assessment', '',
    `Campaign: \`${campaign.id}\``, '',
    'This is a human-gated `n=1` case study and is not a general performance estimate.', '',
    `Fixed P01-P08 headline score: **${score}**`, '',
    `Applicable-weight coverage: ${(aggregate.applicableWeightCoverage * 100).toFixed(1)}%`, '',
    `Evidence completeness: ${(completeness * 100).toFixed(1)}%`, '',
    'Adaptive prompts A09-A10 are reported separately and do not change the fixed headline score.', '',
    '## Dimension scores', '',
    '| Dimension | Mean (0-3) | Weight |', '|---|---:|---:|',
    ...aggregate.dimensions.map((item) => `| ${item.id} | ${item.mean == null ? 'N/A' : item.mean.toFixed(2)} | ${item.weight} |`), '',
    '## Capability mapping', '',
    '| Capability | Claim | Evidence | IRT mapping | Disposition | Evidence | Rationale |', '|---|---|---|---|---|---|---|',
    ...classifications.map((row) => `| ${row.capability} | ${row.claim_status} | ${row.evidence_status} | ${row.irt_mapping} | ${row.irt_disposition} | ${row.evidence_refs.join('; ')} | ${row.reviewer_rationale.replaceAll('|', '\\|')} |`), ''
  ];
  return `${lines.join('\n')}\n`;
}

export function generateReports({ outputRoot, campaign, rubric, lock, promptResults, timings, evidenceManifest, classifications, predeclaredNaDimensions = [] }) {
  validateClassifications(classifications);
  mkdirSync(outputRoot, { recursive: true });
  const aggregate = aggregateScores(rubric, lock.scores, { predeclaredNaDimensions });
  const expectedEvidence = promptResults.flatMap((result) => result.expectedEvidenceIds ?? []);
  const completeness = evidenceCompleteness(expectedEvidence, evidenceManifest);
  const scores = { campaignId: campaign.id, scope: 'n=1 case study', ...aggregate, evidenceCompleteness: completeness, reviewLockDigest: lock.digest };
  const gapRows = classifications.map((row) => ({ ...row, evidence_refs: row.evidence_refs.join(';') }));
  const files = {
    assessment: join(outputRoot, 'CRAVIS_CAPABILITY_ASSESSMENT.md'),
    scores: join(outputRoot, 'cravis_scores.json'),
    promptResults: join(outputRoot, 'cravis_prompt_results.csv'),
    timings: join(outputRoot, 'cravis_timings.csv'),
    gapMatrix: join(outputRoot, 'irt_gap_matrix.csv')
  };
  atomicWrite(files.assessment, markdownReport(campaign, aggregate, completeness, classifications));
  atomicWrite(files.scores, `${canonicalJson(scores)}\n`);
  atomicWrite(files.promptResults, rowsToCsv([...promptResults].sort((a,b) => a.promptId.localeCompare(b.promptId))));
  atomicWrite(files.timings, rowsToCsv([...timings].sort((a,b) => a.promptId.localeCompare(b.promptId))));
  atomicWrite(files.gapMatrix, rowsToCsv([...gapRows].sort((a,b) => a.capability.localeCompare(b.capability))));
  return files;
}

export function readCampaignReportInputs(campaignRoot) {
  const read = (name) => JSON.parse(readFileSync(join(campaignRoot, name), 'utf8'));
  return {
    campaign: read('campaign.json'), promptResults: read('prompt-results.json'), timings: read('timings.json'),
    evidenceManifest: read('evidence-manifest.json'), classifications: read('classifications.json')
  };
}

