export function expectedCells(rubric, promptIds = ['P01','P02','P03','P04','P05','P06','P07','P08'], excludedDimensionIds = []) {
  const excluded = new Set(excludedDimensionIds);
  return rubric.dimensions.filter((dimension) => !excluded.has(dimension.id)).flatMap((dimension) => dimension.appliesTo.filter((id) => promptIds.includes(id)).map((promptId) => ({ promptId, dimensionId: dimension.id })));
}

export function validateHumanScores(rubric, scores, promptIds = ['P01','P02','P03','P04','P05','P06','P07','P08'], excludedDimensionIds = []) {
  const expected = expectedCells(rubric, promptIds, excludedDimensionIds);
  const excluded = new Set(excludedDimensionIds);
  const keys = new Set();
  for (const score of scores) {
    const key = `${score.promptId}:${score.dimensionId}`;
    if (keys.has(key)) throw new Error(`Duplicate human score: ${key}`);
    keys.add(key);
    if (!promptIds.includes(score.promptId)) throw new Error(`Score is outside the review scope: ${key}`);
    if (excluded.has(score.dimensionId)) throw new Error(`Score supplied for predeclared N/A dimension: ${key}`);
    if (!Number.isInteger(score.score) || score.score < 0 || score.score > 3) throw new Error(`Invalid human score: ${key}`);
    if (!score.confirmedBy) throw new Error(`Unconfirmed human score: ${key}`);
  }
  const missing = expected.filter(({ promptId, dimensionId }) => !keys.has(`${promptId}:${dimensionId}`));
  if (missing.length) throw new Error(`Missing human scores: ${missing.map((cell) => `${cell.promptId}:${cell.dimensionId}`).join(', ')}`);
  return scores;
}

export function aggregateScores(rubric, scores, { predeclaredNaDimensions = [] } = {}) {
  validateHumanScores(rubric, scores, ['P01','P02','P03','P04','P05','P06','P07','P08'], predeclaredNaDimensions);
  const na = new Set(predeclaredNaDimensions);
  const dimensions = rubric.dimensions.map((dimension) => {
    if (na.has(dimension.id)) return { id: dimension.id, weight: dimension.weight, applicable: false, mean: null, weighted: null };
    const values = scores.filter((score) => score.dimensionId === dimension.id && /^P0[1-8]$/.test(score.promptId)).map((score) => score.score);
    const mean = values.reduce((sum, score) => sum + score, 0) / values.length;
    return { id: dimension.id, weight: dimension.weight, applicable: true, mean, weighted: (mean / 3) * dimension.weight };
  });
  const applicableWeight = dimensions.filter((d) => d.applicable).reduce((sum, d) => sum + d.weight, 0);
  const coverage = applicableWeight / rubric.dimensions.reduce((sum, d) => sum + d.weight, 0);
  const weightedTotal = dimensions.filter((d) => d.applicable).reduce((sum, d) => sum + d.weighted, 0);
  return {
    dimensions,
    applicableWeight,
    applicableWeightCoverage: coverage,
    headlineScore: coverage >= rubric.coverageThreshold ? (weightedTotal / applicableWeight) * 100 : null,
    adaptiveExcluded: true
  };
}

export function evidenceCompleteness(expectedArtifacts, evidenceManifest) {
  if (!expectedArtifacts.length) return 1;
  const present = new Set(evidenceManifest.filter((item) => item.present !== false).map((item) => item.id));
  return expectedArtifacts.filter((id) => present.has(id)).length / expectedArtifacts.length;
}
