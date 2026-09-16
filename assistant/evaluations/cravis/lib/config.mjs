import { existsSync, readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { hashJson } from './canonical.mjs';

export function readJson(path) {
  return JSON.parse(readFileSync(path, 'utf8'));
}

export function validatePromptConfig(config) {
  const prompts = config.fixedPrompts ?? [];
  if (prompts.length !== 8 || prompts.map((p) => p.id).join(',') !== 'P01,P02,P03,P04,P05,P06,P07,P08') throw new Error('Fixed prompt IDs must be exactly P01-P08');
  for (const prompt of prompts) {
    if (!prompt.text?.trim() || !prompt.oracle?.dimensions?.length) throw new Error(`Prompt ${prompt.id} is incomplete`);
  }
  return config;
}

export function validateRubric(rubric, promptConfig) {
  if (rubric.dimensions?.length !== 14) throw new Error('Rubric must contain fourteen dimensions');
  const total = rubric.dimensions.reduce((sum, dimension) => sum + dimension.weight, 0);
  if (total !== 100) throw new Error(`Rubric weights total ${total}, expected 100`);
  const known = new Set(promptConfig.fixedPrompts.map((prompt) => prompt.id));
  for (const dimension of rubric.dimensions) {
    if (!dimension.id || !dimension.requiredEvidence?.length || Object.keys(dimension.anchors ?? {}).join(',') !== '0,1,2,3') throw new Error(`Incomplete rubric dimension: ${dimension.id}`);
    if (dimension.appliesTo.some((id) => !known.has(id))) throw new Error(`Unknown prompt in ${dimension.id} applicability`);
  }
  for (const prompt of promptConfig.fixedPrompts) {
    const matrix = rubric.dimensions.filter((dimension) => dimension.appliesTo.includes(prompt.id)).map((dimension) => dimension.id).sort();
    const oracle = [...prompt.oracle.dimensions].sort();
    if (JSON.stringify(matrix) !== JSON.stringify(oracle)) throw new Error(`Rubric applicability differs from ${prompt.id} oracle`);
  }
  return rubric;
}

export function loadConfiguration(root) {
  const promptPath = resolve(root, 'config/prompts.json');
  const rubricPath = resolve(root, 'config/rubric.json');
  const targetPath = resolve(root, 'config/targets.json');
  const prompts = validatePromptConfig(readJson(promptPath));
  const rubric = validateRubric(readJson(rubricPath), prompts);
  const targets = readJson(targetPath);
  return {
    prompts, rubric, targets,
    hashes: { prompts: hashJson(prompts), rubric: hashJson(rubric), targets: hashJson(targets) },
    paths: { promptPath, rubricPath, targetPath }
  };
}

export function validateAdaptivePrompt(record, promptConfig) {
  if (!['A09', 'A10'].includes(record.id)) throw new Error('Adaptive prompt ID must be A09 or A10');
  const required = promptConfig.adaptivePolicy.requiredFields.filter((field) => field !== 'sha256');
  for (const field of required) if (record[field] == null || record[field] === '') throw new Error(`Adaptive prompt is missing ${field}`);
  if (record.id === 'A09' && new Set(record.intendedWeaknesses).size < 2) throw new Error('A09 must target at least two observed weaknesses');
  const hashable = { ...record };
  delete hashable.sha256;
  const expected = hashJson(hashable);
  if (record.sha256 !== expected) throw new Error('Adaptive prompt hash mismatch');
  return record;
}

export function readOriginApprovals(path) {
  if (!existsSync(path)) return [];
  const data = readJson(path);
  if (!Array.isArray(data.approvedOrigins)) throw new Error('Origin approvals must contain approvedOrigins[]');
  return data.approvedOrigins;
}
