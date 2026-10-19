import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdirSync, mkdtempSync, symlinkSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { assertContainedPath } from '../lib/paths.mjs';
import { isLoopbackHostname, validateTargetUrl } from '../lib/policy.mjs';
import { redactText, sanitizeNetworkRecord, sanitizePathname } from '../lib/redaction.mjs';
import { MAX_DOWNLOAD_BYTES, saveInertDownload } from '../lib/downloads.mjs';
import { rowsToCsv } from '../lib/csv.mjs';

test('origin allowlist permits HTTPS approval and loopback-only simulation HTTP', () => {
  assert.equal(validateTargetUrl('https://cravis.ai/path', ['https://cravis.ai']).origin, 'https://cravis.ai');
  assert.throws(() => validateTargetUrl('http://cravis.ai', ['https://cravis.ai']), /HTTPS/);
  assert.equal(validateTargetUrl('http://127.0.0.1:1234', [], { simulation: true }).hostname, '127.0.0.1');
  assert.throws(() => validateTargetUrl('http://example.com', [], { simulation: true }), /loopback/);
  assert.equal(isLoopbackHostname('localhost'), true);
});

test('filesystem containment rejects traversal and symlink descendants', () => {
  const root = mkdtempSync(join(tmpdir(), 'cravis-path-'));
  mkdirSync(join(root, 'safe'));
  assert.equal(assertContainedPath(root, join(root, 'safe', 'state.json')).startsWith(root), true);
  assert.throws(() => assertContainedPath(root, join(root, '..', 'escape')), /outside/);
  symlinkSync(tmpdir(), join(root, 'link'));
  assert.throws(() => assertContainedPath(root, join(root, 'link', 'state.json')), /Symlinks/);
});

test('network and error evidence strips secrets, queries, fragments, and identifier-like paths', () => {
  const record = sanitizeNetworkRecord({ url: 'https://cravis.ai/api/users/123e4567-e89b-12d3-a456-426614174000/data?token=secret#x', method: 'get', resourceType: 'xhr', status: 200, durationMs: 4 });
  assert.equal(record.pathname, '/api/users/:redacted/data');
  assert.equal(JSON.stringify(record).includes('token'), false);
  assert.equal(sanitizeNetworkRecord({ url: 'https://cravis.ai/auth/callback?code=x', method: 'GET', resourceType: 'document' }, ['/auth']), null);
  const redacted = redactText('Bearer abc.def.ghi user@example.com api_key=abcdef');
  assert.equal(redacted.includes('user@example.com'), false);
  assert.equal(redacted.includes('abcdef'), false);
  assert.equal(sanitizePathname('/opaque/abcdefghijklmnopqrstuvwxyz012345'), '/opaque/:redacted');
});

test('downloads use generated names, reject collisions and oversize, and ignore malicious suggested names', () => {
  const root = mkdtempSync(join(tmpdir(), 'cravis-download-'));
  const saved = saveInertDownload(root, Buffer.from('a,b\n1,2\n'), { suggestedName: '../../evil.exe', sequence: 1 });
  assert.match(saved.generatedName, /^download-001-[a-f0-9]{12}\.csv$/);
  assert.equal(saved.suggestedName, '../../evil.exe');
  assert.throws(() => saveInertDownload(root, Buffer.from('a,b\n1,2\n'), { sequence: 1 }), /collision/);
  assert.throws(() => saveInertDownload(root, Buffer.alloc(MAX_DOWNLOAD_BYTES + 1)), /exceeds/);
});

test('CSV preserves typed numbers and neutralizes formula-leading string cells', () => {
  const csv = rowsToCsv([{ n: 12.5, formula: '=2+2', padded: '  -9', normal: 'text' }], ['n','formula','padded','normal']);
  assert.match(csv, /^n,formula,padded,normal\r\n12\.5,'=2\+2,'  -9,text\r\n$/);
});

