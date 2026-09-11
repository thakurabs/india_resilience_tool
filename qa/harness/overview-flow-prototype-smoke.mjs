/**
 * Smoke check for the Overview flow prototype (CHG-0385/0392/0393).
 *
 * Walks the full three-level flow once -- national view, State-level tooltip,
 * the locked-State tooltip, drill to the live State, drill to a district, block
 * inspection, a driver route into Detailed Analysis, the local-contrast view, a
 * pinned histogram bin, and a slice change -- screenshotting each step and failing loudly on any console
 * or page error. It asserts nothing about the numbers; it exists so a rebuild
 * cannot ship a page whose interactions are broken.
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
const line = (s) => String(s).replace(/\n/g, ' | ');
await p.goto('file://' + process.argv[2]);
await p.waitForTimeout(1200);
await p.screenshot({ path: out + '/01-india.png', fullPage: true });
console.log('rank row 1     :', line(await p.locator('#ranking tbody tr').first().innerText()));


// Context and Evidence: present from the State view onward, collapsed, scoped to
// the State/UT, and honest about the basin context it does not carry there
console.log('ctx hidden India:', await p.locator('#context').isHidden());
// a non-live State must hover but refuse selection
await p.locator('#g-dist path').nth(400).hover({ force: true });
await p.waitForTimeout(300);
console.log('locked tooltip :', line(await p.locator('#tip').innerText()));
await p.locator('#g-dist path').nth(400).click({ force: true });
await p.waitForTimeout(300);
console.log('after locked click, crumbs:', line(await p.locator('#crumbs').innerText()));

// Context layer: additive over the risk choropleth, never a replacement for it
const fillBefore = await p.locator('#g-dist path').nth(300).getAttribute('fill');
console.log('ctxl row in India:', !(await p.locator('#ctxl-row').isHidden()));
await p.selectOption('#ctx-layer', 'pop');
await p.waitForTimeout(400);
console.log('ctx circles    :', await p.locator('#g-ctx circle').count());
console.log('risk fill kept :', (await p.locator('#g-dist path').nth(300).getAttribute('fill')) === fillBefore);
console.log('ctx key        :', line(await p.locator('#ctx-key').innerText()));
await p.screenshot({ path: out + '/01b-ctx-pop.png', fullPage: true });
await p.selectOption('#ctx-layer', '');
await p.waitForTimeout(300);
console.log('ctx circles off:', await p.locator('#g-ctx circle').count());

// drill to the live State from the ranking
await p.locator('#ranking tbody tr').first().click();
await p.waitForTimeout(800);
console.log('state crumbs   :', line(await p.locator('#crumbs').innerText()));
console.log('state headline :', line(await p.locator('#headline').innerText()).slice(0, 160));
console.log('state painted  :', await p.locator('#painted').innerText());
await p.screenshot({ path: out + '/02-state.png', fullPage: true });
console.log('ctxl row in State (must be false):', await p.locator('#ctxl-row').isHidden());

// the overlay follows the painted unit: districts nationally, blocks here
await p.selectOption('#ctx-layer', 'pop');
await p.waitForTimeout(400);
console.log('state ctx circles:', await p.locator('#g-ctx circle').count());
console.log('state ctx key    :', line(await p.locator('#ctx-key').innerText()));
await p.screenshot({ path: out + '/02d-ctx-blocks.png', fullPage: true });
await p.selectOption('#ctx-layer', '');
await p.waitForTimeout(300);

// State-view hover must report the parent DISTRICT and highlight all its blocks,
// mirroring the national view's State/UT hover over painted districts
await p.locator('#g-block path').nth(120).hover({ force: true });
await p.waitForTimeout(350);
console.log('state hover    :', line(await p.locator('#tip').innerText()));
console.log('blocks lit     :', await p.locator('#g-block path.hl').count());


console.log('ctx summary     :', line(await p.locator('#context summary').innerText()));
console.log('ctx open by dflt:', await p.locator('#context details').evaluate(d => d.open));
await p.locator('#context summary').click();
await p.waitForTimeout(300);
console.log('ctx state expo  :', line(await p.locator('#context .ctx-sec').first().innerText()).slice(0, 200));
console.log('ctx state hydro :', line(await p.locator('#context .ctx-sec').nth(1).innerText()).slice(0, 150));
await p.screenshot({ path: out + '/02c-context-state.png', fullPage: true });
// local contrast: map fill stretches, ticks leave 0-100, the frozen state returns
const tick0 = () => p.locator('#cbar-ticks span').first().innerText();
const fill0 = () => p.locator('#g-block path').first().getAttribute('fill');
console.log('frozen tick/fill:', await tick0(), await fill0());
await p.locator('#lc-toggle').check();
await p.waitForTimeout(500);
console.log('local tick/fill :', await tick0(), await fill0());
console.log('local warn      :', line(await p.locator('#cbar-warn').innerText()).slice(0, 140));
await p.screenshot({ path: out + '/02b-local-contrast.png', fullPage: true });
await p.locator('#lc-toggle').uncheck();
await p.waitForTimeout(400);
console.log('back to frozen  :', await tick0(), await fill0());

// drill to a district
await p.locator('#ranking tbody tr').first().click();
await p.waitForTimeout(700);
console.log('district crumbs:', line(await p.locator('#crumbs').innerText()));
await p.selectOption('#ctx-layer', 'pop');
await p.waitForTimeout(400);
console.log('dist ctx circles :', await p.locator('#g-ctx circle').count());
await p.selectOption('#ctx-layer', '');
await p.waitForTimeout(300);
console.log('dist headline  :', line(await p.locator('#headline').innerText()).slice(0, 200));
console.log('dist painted   :', await p.locator('#painted').innerText());
await p.screenshot({ path: out + '/03-district.png', fullPage: true });


// context follows the selection down: district scope gains real basin context
console.log('ctx dist scope  :', line(await p.locator('#context summary').innerText()));
console.log('ctx dist hydro  :', line(await p.locator('#context .ctx-sec').nth(1).innerText()).slice(0, 170));
// select a block inside it -> inspection + Detailed Analysis
const shown = p.locator('#g-block path:not(.muted)');
console.log('blocks in view :', await shown.count());
await shown.first().hover({ force: true });
await p.waitForTimeout(350);
console.log('dist hover     :', line(await p.locator('#tip').innerText()));
await shown.first().click({ force: true });
await p.waitForTimeout(600);
console.log('inspection     :', line(await p.locator('#inspection').innerText()).slice(0, 260));
await p.screenshot({ path: out + '/04-block-da.png', fullPage: true });
console.log('ctx block scope :', line(await p.locator('#context summary').innerText()));

// a driver must route into Detailed Analysis carrying that metric
await p.locator('#headline ul.drv button').first().click();
await p.waitForTimeout(500);
console.log('driver route   :', line(await p.locator('#inspection .stub').innerText()).slice(0, 300));
await p.screenshot({ path: out + '/05-driver-da.png', fullPage: true });

// histogram pin still filters the district ranking
await p.locator('#crumbs button').nth(1).click();
await p.waitForTimeout(500);
await p.locator('.hbin[data-empty="0"]').first().click();
await p.waitForTimeout(500);
console.log('filterbar      :', line(await p.locator('#filterbar').innerText()));
await p.screenshot({ path: out + '/06-pinned.png', fullPage: true });

// returning to India must clear local contrast: it is never the landing state
await p.locator('#lc-toggle').check();
await p.waitForTimeout(400);
await p.locator('#crumbs button').first().click();
await p.waitForTimeout(500);
console.log('india lc state :', await p.locator('#lc-toggle').isChecked(),
            '| domain:', line(await p.locator('#cbar-domain').innerText()));

// change slice

await p.selectOption('#sel-scenario', 'ssp245');
await p.selectOption('#sel-period', '2060-2080');
await p.waitForTimeout(700);
console.log('after slice    :', line(await p.locator('#ranking tbody tr').first().innerText()));
await p.screenshot({ path: out + '/07-slice.png', fullPage: true });

console.log(errs.length ? 'CONSOLE ERRORS:\n' + errs.join('\n') : 'no console errors');
await b.close();
