// UX-flow recon, Phase D: read-only API capability probe.
//
// The Overview we want to specify needs a NATIONAL district payload. The UI only
// ever asks for one State at a time, so the question "can the deployed backend
// already serve national, or is that new vendor work?" cannot be answered from
// the UI alone. This probes the SAME endpoints the app itself calls, from inside
// an authenticated page context, with read-only queries only. No writes.
import { withSession } from './lib/session.mjs';
import { createRun, attachCollectors, finalize } from './lib/evidence.mjs';
import { attachApiRecorder, writeJson, writeApiLog, stage } from './lib/recon.mjs';

const PROBES = [
  // 1. the exact call the UI makes (control)
  { name: 'control-state-district', path: '/api/api/parquet/composite-map-data',
    body: { riskDomain: 'composite_heat_risk', state: 'Telangana', level: 'district', scenario: 'ssp585', period: '2040-2060' } },
  // 2. same, block level
  { name: 'control-state-block', path: '/api/api/parquet/composite-map-data',
    body: { riskDomain: 'composite_heat_risk', state: 'Telangana', level: 'block', scenario: 'ssp585', period: '2040-2060' } },
  // 3. national: omit state entirely
  { name: 'national-no-state', path: '/api/api/parquet/composite-map-data',
    body: { riskDomain: 'composite_heat_risk', level: 'district', scenario: 'ssp585', period: '2040-2060' } },
  // 4. national: explicit nulls / wildcards
  { name: 'national-state-null', path: '/api/api/parquet/composite-map-data',
    body: { riskDomain: 'composite_heat_risk', state: null, level: 'district', scenario: 'ssp585', period: '2040-2060' } },
  { name: 'national-state-all', path: '/api/api/parquet/composite-map-data',
    body: { riskDomain: 'composite_heat_risk', state: 'All', level: 'district', scenario: 'ssp585', period: '2040-2060' } },
  // 5. is there a state-level aggregate at all?
  { name: 'level-state', path: '/api/api/parquet/composite-map-data',
    body: { riskDomain: 'composite_heat_risk', state: 'Telangana', level: 'state', scenario: 'ssp585', period: '2040-2060' } },
  // 6. a second State, to see whether scores are comparable across States
  { name: 'control-kerala', path: '/api/api/parquet/composite-map-data',
    body: { riskDomain: 'composite_heat_risk', state: 'Kerala', level: 'district', scenario: 'ssp585', period: '2040-2060' } },
  { name: 'control-rajasthan', path: '/api/api/parquet/composite-map-data',
    body: { riskDomain: 'composite_heat_risk', state: 'Rajasthan', level: 'district', scenario: 'ssp585', period: '2040-2060' } },
  { name: 'control-goa', path: '/api/api/parquet/composite-map-data',
    body: { riskDomain: 'composite_heat_risk', state: 'Goa', level: 'district', scenario: 'ssp585', period: '2040-2060' } },
  // 7. the endpoints the July report logged as 500s
  { name: 'ranking', path: '/api/api/parquet/ranking',
    body: { riskDomain: 'composite_heat_risk', state: 'Telangana', level: 'district', scenario: 'ssp585', period: '2040-2060' } },
  { name: 'trend', path: '/api/api/parquet/trend',
    body: { riskDomain: 'composite_heat_risk', state: 'Telangana', level: 'district', districtNames: ['Warangal'], scenario: 'ssp585', period: '2040-2060' } },
  { name: 'scenario-comparison', path: '/api/api/parquet/scenario-comparison',
    body: { riskDomain: 'composite_heat_risk', state: 'Telangana', level: 'district', districtNames: ['Warangal'], scenario: 'ssp585', period: '2040-2060' } },
];

await withSession(async (page) => {
  const run = createRun('ux-flow-D-api');
  attachCollectors(page, run);
  attachApiRecorder(page, run);
  await page.goto('https://dev.resilience.org.in/', { waitUntil: 'domcontentloaded' });
  await page.waitForLoadState('networkidle', { timeout: 25000 }).catch(() => {});
  await page.waitForTimeout(2500);

  const results = [];
  for (const p of PROBES) {
    await stage(run, p.name, async () => {
      const r = await page.evaluate(async ({ path, body }) => {
        // The API enforces CSRF: the XSRF-TOKEN cookie must be echoed as a header.
        const xsrf = decodeURIComponent((document.cookie.match(/XSRF-TOKEN=([^;]+)/) || [])[1] || '');
        const res = await fetch(path, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'X-XSRF-TOKEN': xsrf, 'x-csrf-token': xsrf },
          credentials: 'include',
          body: JSON.stringify(body),
        });
        const text = await res.text();
        return { status: res.status, len: text.length, head: text.slice(0, 1500) };
      }, p);
      let parsed = null;
      try { parsed = JSON.parse(r.head.length === r.len ? r.head : `${r.head}`); } catch { /* truncated */ }
      const rec = {
        probe: p.name, path: p.path, request: p.body, status: r.status, bytes: r.len,
        count: parsed && parsed.count !== undefined ? parsed.count : null,
        sample: r.head.slice(0, 700),
      };
      results.push(rec);
      console.log(`  ${String(r.status).padEnd(4)} ${p.name.padEnd(26)} ${String(r.len).padStart(9)}b  count=${rec.count}`);
      writeJson(run, '_probe_results', results);
    });
  }

  // Value-range summary per State probe: the per-State min-max fingerprint test.
  const ranges = {};
  for (const r of results) {
    if (!/control-(kerala|rajasthan|goa|state-district)/.test(r.probe) || r.status !== 200) continue;
    try {
      const full = await page.evaluate(async ({ path, body }) => {
        const xsrf = decodeURIComponent((document.cookie.match(/XSRF-TOKEN=([^;]+)/) || [])[1] || '');
        const res = await fetch(path, { method: 'POST',
          headers: { 'Content-Type': 'application/json', 'X-XSRF-TOKEN': xsrf, 'x-csrf-token': xsrf },
          credentials: 'include', body: JSON.stringify(body) });
        const j = await res.json();
        const vals = (j.data || []).map((d) => d.value).filter((v) => typeof v === 'number');
        return { n: vals.length, min: Math.min(...vals), max: Math.max(...vals) };
      }, { path: r.path, body: r.request });
      ranges[r.request.state] = full;
      console.log(`    range ${r.request.state}: n=${full.n} min=${full.min} max=${full.max}`);
    } catch (e) { ranges[r.request.state] = `ERR ${e.message}`; }
  }
  writeJson(run, '_state_value_ranges', ranges);

  writeApiLog(run);
  finalize(run);
  console.log('\n  Run:', run.dir);
}, { viewport: { width: 1280, height: 800 } });
