// US 14 — Ranking Table scenario. Currently guards a confirmed Blocker: the
// ranking data endpoint returns HTTP 500. This scenario captures that endpoint's
// status and the UI state; it FAILS while the 500 persists and will PASS (and can
// be extended to assert columns/rows/ranking) once the endpoint is fixed.
//
//   node qa/charters/us14-ranking-table/scenario.mjs

import { withSession, APP_URL } from '../../harness/lib/session.mjs';
import { createRun, attachCollectors, snapshot, dumpDom, runAxe, finalize } from '../../harness/lib/evidence.mjs';
import { safe, shot } from '../../harness/lib/runner.mjs';
import { selectState, applyCoreFilters } from '../../harness/lib/flows.mjs';

await withSession(async (page) => {
  const run = createRun('us14-ranking');
  attachCollectors(page, run);

  // Capture the status of the ranking data endpoint specifically.
  const rankingResponses = [];
  page.on('response', (res) => {
    if (/\/ranking(\b|\?|$)/i.test(res.url()) || /parquet\/ranking/i.test(res.url())) {
      rankingResponses.push({ status: res.status(), url: res.url() });
    }
  });

  await page.goto(APP_URL, { waitUntil: 'domcontentloaded' });
  await page.waitForTimeout(2500);

  await safe(run, 'S0: geography + filters', async () => {
    await selectState(page, 'Telangana');
    const picks = await applyCoreFilters(page);
    return `filters: ${JSON.stringify(picks)}`;
  });

  await safe(run, 'S1: switch to Ranking Table', async () => {
    await page.getByText(/^Ranking Table$/).first().click();
    await page.waitForTimeout(4000);
    await shot(page, run, 's1-ranking');
    const body = await page.locator('body').innerText();
    if (!/Ranking Table/i.test(body)) throw new Error('Ranking Table view did not render its heading');
    return 'view switched to Ranking Table';
  });

  await safe(run, 'S2: ranking data loads (guards 500 blocker)', async () => {
    const body = await page.locator('body').innerText();
    const errored = /We couldn.t load the ranking data/i.test(body);
    const rows = await page.locator('table tr, [role=row]').count();
    const statuses = rankingResponses.map((r) => r.status);
    run.rankingResponses = rankingResponses;
    if (errored || statuses.some((s) => s >= 400)) {
      throw new Error(`BLOCKER: ranking data failed — endpoint status ${statuses.join(',') || 'n/a'}; UI shows error banner; rows=${rows}. Endpoint: ${rankingResponses[0] && rankingResponses[0].url}`);
    }
    if (rows < 2) throw new Error(`No ranking rows rendered (rows=${rows})`);
    return `ranking table loaded with ${rows} rows`;
  });

  await dumpDom(page, run, 'us14-final');
  await snapshot(page, run, 'us14-responsive');
  await runAxe(page, run, 'us14');
  finalize(run);
  console.log(`\n  Run: ${run.dir}`);
  console.log(`  Ranking endpoint responses: ${JSON.stringify(rankingResponses)}`);
  console.log(`  Steps failed: ${run.summary.stepsFailed} | real errors: ${run.summary.errorEvents} | benign: ${run.summary.benignEvents}`);
});
