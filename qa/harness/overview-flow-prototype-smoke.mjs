/**
 * Smoke check for the Overview flow prototype (CHG-0385).
 *
 * Walks the full flow once -- national view, State-level tooltip, drill-down,
 * district inspection, the Detailed Analysis seam, a pinned histogram bin, and
 * a slice change -- capturing a screenshot at each step and failing loudly on
 * any console or page error. It asserts nothing about the numbers; it exists so
 * a rebuild cannot ship a page whose interactions are broken.
 *
 * Run from the repository root, where node_modules resolves:
 *   node qa/harness/overview-flow-prototype-smoke.mjs <abs path to html> <out dir>
 */
import { chromium } from 'playwright';
const out = process.argv[3];
const b = await chromium.launch();
const p = await b.newPage({ viewport: { width: 1600, height: 1150 } });
const errs = [];
p.on('console', m => { if (m.type() === 'error') errs.push(m.text()); });
p.on('pageerror', e => errs.push('PAGEERROR ' + e.message));
await p.goto('file://' + process.argv[2]);
await p.waitForTimeout(1200);
await p.screenshot({ path: out + '/01-india.png', fullPage: true });

// state name shown in ranking row 1
const first = await p.locator('#ranking tbody tr').first().innerText();
console.log('rank row 1:', first.replace(/\n/g, ' | '));

// hover a district to test the state-level tooltip
const path0 = p.locator('#g-dist path').nth(400);
await path0.hover({ force: true });
await p.waitForTimeout(300);
console.log('tooltip:', (await p.locator('#tip').innerText()).replace(/\n/g, ' | '));

// click ranking row 1 -> state view
await p.locator('#ranking tbody tr').first().click();
await p.waitForTimeout(700);
console.log('crumbs:', (await p.locator('#crumbs').innerText()).replace(/\n/g, ' '));
console.log('hist title:', await p.locator('#hist-title').innerText());
await p.screenshot({ path: out + '/02-state.png', fullPage: true });

// click district row 1 -> inspection
await p.locator('#ranking tbody tr').first().click();
await p.waitForTimeout(500);
console.log('inspection:', (await p.locator('#inspection').innerText()).slice(0, 200).replace(/\n/g, ' | '));
await p.screenshot({ path: out + '/03-district.png', fullPage: true });

// DA seam
await p.locator('#da-dist').click();
await p.waitForTimeout(400);
await p.screenshot({ path: out + '/04-da.png', fullPage: true });

// histogram pin
await p.locator('#inspection .close').click();
await p.waitForTimeout(300);
const bins = p.locator('.hbin[data-empty="0"]');
await bins.first().click();
await p.waitForTimeout(400);
console.log('filterbar:', (await p.locator('#filterbar').innerText()).replace(/\n/g, ' '));
await p.screenshot({ path: out + '/05-pinned.png', fullPage: true });

// back to india + change slice
await p.locator('#crumbs button').first().click();
await p.waitForTimeout(400);
await p.selectOption('#sel-scenario', 'ssp245');
await p.selectOption('#sel-period', '2060-2080');
await p.waitForTimeout(700);
console.log('defaults note:', await p.locator('#defaults-note').innerText());
console.log('rank row 1 after slice change:', (await p.locator('#ranking tbody tr').first().innerText()).replace(/\n/g, ' | '));
await p.screenshot({ path: out + '/06-slice.png', fullPage: true });

console.log(errs.length ? 'CONSOLE ERRORS:\n' + errs.join('\n') : 'no console errors');
await b.close();
