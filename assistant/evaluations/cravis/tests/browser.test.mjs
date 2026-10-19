import test from 'node:test';
import assert from 'node:assert/strict';
import { chromium } from 'playwright';
import { applyManualTerminalOutcome, lastUserIdentity, observeResponse, promptIdentity, runRecon, segmentOffsets, uniqueVisible } from '../lib/browser.mjs';
import { installRequestPolicy } from '../lib/policy.mjs';

const selectors = {
  promptInput:'textarea', submit:'button[type=submit]', quota:'[data-testid=quota]', activeConversation:'[data-testid=active-conversation]',
  lastUserMessage:'[data-role=user]', response:'[data-role=assistant]', busy:'[aria-busy=true]', table:'table', chart:'svg', map:'.map',
  sources:'a.source', export:'a[download]', upload:'input[type=file]'
};

async function withPage(t, html, fn) {
  let browser;
  try { browser = await chromium.launch({ headless:true }); } catch (error) { t.skip(`Playwright browser unavailable: ${error.message}`); return; }
  const page = await browser.newPage();
  try { await page.setContent(html); await fn(page); } finally { await browser.close(); }
}

test('active last-message matching ignores duplicate sidebar/history text', async (t) => withPage(t, `
  <aside><div data-role=user>target prompt</div></aside>
  <main data-testid=active-conversation><div data-role=user>older</div><div data-role=user>active prompt</div></main>
  <textarea></textarea><button type=submit>Send</button><div data-testid=quota>10/10</div>`, async (page) => {
    assert.deepEqual(await lastUserIdentity(page, selectors), promptIdentity('active prompt'));
    assert.notDeepEqual(await lastUserIdentity(page, selectors), promptIdentity('target prompt'));
  }));

test('multiple visible controls fail while hidden duplicates do not', async (t) => withPage(t, `
  <textarea></textarea><textarea style="display:none"></textarea><button type=submit>Send</button>`, async (page) => {
    assert.ok(await uniqueVisible(page, 'textarea'));
    await page.locator('textarea').nth(1).evaluate((node) => node.style.display='block');
    await assert.rejects(() => uniqueVisible(page, 'textarea'), /found 2/);
  }));

test('zero-send recon proves quota/transcript stability and classifies optional features', async (t) => withPage(t, `
  <main data-testid=active-conversation data-conversation-id=c1><div data-role=user>last</div></main>
  <textarea></textarea><button type=submit>Send</button><div data-testid=quota>10 / 10</div><input type=file>`, async (page) => {
    const result = await runRecon(page, selectors, { settleMs:10 });
    assert.equal(result.zeroSendProven, true);
    assert.equal(result.proof.driverSendActivations, 0);
    assert.equal(result.optional.table, 'not_observable');
  }));

test('response observation ignores unrelated mutations, supports pauses, and keeps nullable visual/export timings', async (t) => withPage(t, `
  <div id=unrelated></div><main data-testid=active-conversation><div data-role=assistant>Starting substantive response</div></main><div aria-busy=true></div>`, async (page) => {
    await page.evaluate(() => {
      let n=0; globalThis.noise=setInterval(()=>document.querySelector('#unrelated').textContent=String(n++),5);
      setTimeout(()=>document.querySelector('[data-role=assistant]').textContent='Substantive response after a streaming pause',30);
      setTimeout(()=>document.querySelector('[aria-busy]').remove(),80);
    });
    const observed = await observeResponse(page, selectors, 0, { timeoutMs:1000,stableMs:60,pollMs:10 });
    await page.evaluate(() => clearInterval(globalThis.noise));
    assert.equal(observed.outcome, 'completed_automatic');
    assert.equal(observed.timing.T_visual, null);
    assert.equal(observed.timing.T4, null);
    for (const outcome of ['completed_manual','partial_manual','timed_out','uncertain']) assert.equal(applyManualTerminalOutcome(observed,outcome,123).timing.T3,123);
    assert.equal(applyManualTerminalOutcome(observed,'completed_manual').evidenceConfidenceUpgraded,false);
  }));

test('response observation ignores the prior assistant subtree until a new response appears', async (t) => withPage(t, `
  <main data-testid=active-conversation><div data-role=assistant>Old response must not set current timing</div></main><div aria-busy=true></div>`, async (page) => {
    await page.evaluate(() => {
      setTimeout(()=>{ const node=document.createElement('div'); node.dataset.role='assistant'; node.textContent='New substantive response content'; document.querySelector('main').append(node); },60);
      setTimeout(()=>document.querySelector('[aria-busy]').remove(),100);
    });
    const observed=await observeResponse(page,selectors,0,{timeoutMs:1000,stableMs:40,pollMs:10,baselineResponseCount:1});
    assert.equal(observed.responseText,'New substantive response content');
    assert.ok(observed.timing.T1 >= 50);
  }));

test('test browser aborts every non-loopback request', async (t) => withPage(t, '<p>safe</p>', async (page) => {
    const blocked=[]; await installRequestPolicy(page, ['https://cravis.ai'], { simulation:true,onBlocked:(origin)=>blocked.push(origin) });
    await assert.rejects(() => page.goto('http://example.com/non-loopback'));
    assert.deepEqual(blocked, ['http://example.com']);
  }));

test('campaign video segment references use deterministic T0-T3 offsets', () => {
  assert.deepEqual(segmentOffsets({T0:1250,T3:4600},1000),{T0OffsetMs:250,T3OffsetMs:3600});
  assert.deepEqual(segmentOffsets({T0:1250,T3:null},1000),{T0OffsetMs:250,T3OffsetMs:null});
});
