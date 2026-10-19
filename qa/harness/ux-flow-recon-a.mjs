// UX-flow recon, Phase A: the analysis-setup surface.
//   - shell / panels / view controls
//   - geography cascade (State -> District|Block) + the new Search Geography box
//   - the COMPLETE Risk Domain x Metric x Scenario x Period x Statistic x Map Mode
//     taxonomy, enumerated without committing to a selection where possible
// Read-only: no adds, no saves, no uploads, no submits.
import { withSession } from './lib/session.mjs';
import { createRun, attachCollectors, finalize } from './lib/evidence.mjs';
import {
  attachApiRecorder, captureState, stage, enumerateOptions, selectOption, closeList,
  writeJson, writeApiLog,
} from './lib/recon.mjs';

const trig = (page, aria) => page.getByRole('button', { name: new RegExp(`^${aria}$`, 'i') }).first();

async function pickOption(page, aria, text) {
  const t = trig(page, aria);
  const chosen = await selectOption(page, t, text);
  await closeList(page, t);
  return chosen;
}

await withSession(async (page) => {
  const run = createRun('ux-flow-A');
  attachCollectors(page, run);
  attachApiRecorder(page, run);
  const taxonomy = { domains: [], geography: {}, viewControls: {} };

  await page.goto('https://dev.resilience.org.in/', { waitUntil: 'domcontentloaded' });
  await page.waitForLoadState('networkidle', { timeout: 25000 }).catch(() => {});
  await page.waitForTimeout(2500);

  // ---- open both panels -------------------------------------------------
  await stage(run, 'A0-open-panels', async () => {
    for (const re of [/Expand administrative analysis/i, /Expand resilience filters panel/i]) {
      const b = page.getByRole('button', { name: re }).first();
      if (await b.isVisible().catch(() => false)) { await b.click(); await page.waitForTimeout(900); }
    }
    await captureState(page, run, 'A0-panels-open', { note: 'admin + filters panels open, no selection' });
  });

  // ---- geography --------------------------------------------------------
  await stage(run, 'A1-state-list', async () => {
    taxonomy.geography.states = await enumerateOptions(page, trig(page, 'Select State'), 'States');
  });

  await stage(run, 'A2-select-state', async () => {
    await pickOption(page, 'Select State', 'Telangana');
    await page.waitForTimeout(1500);
    await captureState(page, run, 'A2-state-selected', { note: 'State = Telangana, district level' });
  });

  await stage(run, 'A3-district-list', async () => {
    taxonomy.geography.districts_telangana =
      await enumerateOptions(page, trig(page, 'Select District\\(s\\)'), 'Districts (TG)');
  });

  await stage(run, 'A4-block-level', async () => {
    await page.locator('input[type="radio"][value="block"]').first().click();
    await page.waitForTimeout(1600);
    await captureState(page, run, 'A4-block-level', { note: 'admin level switched to Block' });
    const blkTrig = page.locator('button[aria-label*="Block"]').first();
    if (await blkTrig.isVisible().catch(() => false)) {
      taxonomy.geography.blocks_trigger_aria = await blkTrig.getAttribute('aria-label');
      taxonomy.geography.blocks_sample = await enumerateOptions(page, blkTrig, 'Blocks');
    }
    await page.locator('input[type="radio"][value="district"]').first().click();
    await page.waitForTimeout(1400);
  });

  await stage(run, 'A5-geo-search', async () => {
    const box = page.locator('input[placeholder*="Search Geography" i]').first();
    await box.scrollIntoViewIfNeeded().catch(() => {});
    await box.click();
    await box.fill('Warangal');
    await page.waitForTimeout(1600);
    await captureState(page, run, 'A5-geo-search', { note: 'Search Geography = "Warangal"' });
    await box.fill('');
    await page.keyboard.press('Escape').catch(() => {});
    await page.waitForTimeout(700);
  });

  await stage(run, 'A6-select-district', async () => {
    await pickOption(page, 'Select District\\(s\\)', 'Warangal');
    await page.keyboard.press('Escape').catch(() => {});
    await page.waitForTimeout(1400);
    await captureState(page, run, 'A6-district-selected', { note: 'District = Warangal selected' });
  });

  // ---- the filter taxonomy ---------------------------------------------
  await stage(run, 'A7-domain-list', async () => {
    taxonomy.domainList = await enumerateOptions(page, trig(page, 'Risk Domain'), 'Risk Domains');
  });

  await stage(run, 'A8-cascade-sweep', async () => {
    for (const domain of taxonomy.domainList) {
      const entry = { domain, metrics: [], deep: null };
      try {
        await pickOption(page, 'Risk Domain', domain);
        entry.metrics = await enumerateOptions(page, trig(page, 'Metric'), `  ${domain} metrics`);
        // Deep-drill the FIRST metric of every domain: scenarios/periods/statistics/map modes
        if (entry.metrics.length) {
          const m = entry.metrics[0];
          await pickOption(page, 'Metric', m);
          const scenarios = await enumerateOptions(page, trig(page, 'Scenario'), `    ${m} scenarios`);
          let periods = [], statistics = [], mapModes = [];
          if (scenarios.length) {
            await pickOption(page, 'Scenario', scenarios[0]);
            await page.waitForTimeout(900);
            periods = await enumerateOptions(page, trig(page, 'Period'), `    ${m} periods`);
            if (periods.length) {
              await pickOption(page, 'Period', periods[0]);
              statistics = await enumerateOptions(page, trig(page, 'Statistic'), `    ${m} statistics`);
              if (statistics.length) {
                await pickOption(page, 'Statistic', statistics[0]);
                mapModes = await enumerateOptions(page, trig(page, 'Map Mode'), `    ${m} map modes`);
              }
            }
          }
          entry.deep = { metric: m, scenarios, periods, statistics, mapModes };
        }
      } catch (e) {
        entry.error = String(e && e.message || e).split('\n')[0].slice(0, 200);
        console.log(`    ! ${domain}: ${entry.error}`);
      }
      taxonomy.domains.push(entry);
      writeJson(run, '_taxonomy', taxonomy); // checkpoint every domain
    }
  });

  // ---- per-metric scenario/period matrix for the default domain ---------
  await stage(run, 'A9-per-metric-matrix', async () => {
    const target = taxonomy.domains.find((d) => /heat/i.test(d.domain)) || taxonomy.domains[0];
    if (!target) return;
    const matrix = [];
    await pickOption(page, 'Risk Domain', target.domain);
    for (const m of target.metrics) {
      try {
        await pickOption(page, 'Metric', m);
        const scenarios = await enumerateOptions(page, trig(page, 'Scenario'), `    ${m}`);
        let periods = [];
        if (scenarios.length) {
          await pickOption(page, 'Scenario', scenarios[0]);
          await page.waitForTimeout(800);
          periods = await enumerateOptions(page, trig(page, 'Period'), `      periods`);
        }
        matrix.push({ metric: m, scenarios, periodsForFirstScenario: periods });
      } catch (e) {
        matrix.push({ metric: m, error: String(e && e.message || e).split('\n')[0].slice(0, 160) });
      }
      writeJson(run, '_metric_matrix', { domain: target.domain, matrix });
    }
    taxonomy.metricMatrix = { domain: target.domain, matrix };
  });

  // ---- help tooltips: what the app tells the user each control means ----
  await stage(run, 'A10-help-copy', async () => {
    const help = {};
    for (const label of ['Risk Domain', 'Metric', 'Scenario', 'Period', 'Statistic', 'Map Mode']) {
      try {
        const b = page.getByRole('button', { name: new RegExp(`^${label} help$`, 'i') }).first();
        await b.hover();
        await page.waitForTimeout(900);
        help[label] = await page.evaluate(() => {
          const t = [...document.querySelectorAll('[role="tooltip"],.tooltip,[data-tooltip]')]
            .map((e) => e.innerText.trim()).filter(Boolean);
          return t.length ? t : null;
        });
        if (!help[label]) { await b.click(); await page.waitForTimeout(800);
          help[label] = await page.evaluate(() => {
            const t = [...document.querySelectorAll('[role="tooltip"],.tooltip,[data-tooltip]')]
              .map((e) => e.innerText.trim()).filter(Boolean);
            return t.length ? t : null; }); }
      } catch (e) { help[label] = `ERR ${String(e.message).slice(0, 100)}`; }
      console.log(`    help ${label}: ${JSON.stringify(help[label])?.slice(0, 120)}`);
    }
    taxonomy.helpCopy = help;
    await page.keyboard.press('Escape').catch(() => {});
  });

  writeJson(run, '_taxonomy', taxonomy);
  const summary = writeApiLog(run);
  finalize(run);
  console.log('\n  Run:', run.dir);
  console.log('  Endpoints:');
  for (const s of summary) console.log(`    ${s.endpoint}  x${s.calls}  ${JSON.stringify(s.statuses)}`);
  console.log('  Failed stages:', run.steps.filter((s) => s.ok === false).map((s) => s.name).join(', ') || 'none');
}, { viewport: { width: 1600, height: 1000 } });
