import { existsSync, mkdirSync } from 'node:fs';
import { extname, join } from 'node:path';
import { atomicWrite, sha256 } from './canonical.mjs';
import { assertContainedPath } from './paths.mjs';

export const MAX_DOWNLOAD_BYTES = 25 * 1024 * 1024;
const SIGNATURES = [
  { bytes: Buffer.from('%PDF-'), mediaType: 'application/pdf', extension: '.pdf' },
  { bytes: Buffer.from('PK\x03\x04', 'binary'), mediaType: 'application/zip', extension: '.zip' },
  { bytes: Buffer.from('\x89PNG\r\n\x1a\n', 'binary'), mediaType: 'image/png', extension: '.png' },
  { bytes: Buffer.from('GIF8'), mediaType: 'image/gif', extension: '.gif' }
];

export function detectMediaType(buffer) {
  for (const signature of SIGNATURES) if (buffer.subarray(0, signature.bytes.length).equals(signature.bytes)) return signature;
  const prefix = buffer.subarray(0, 4096).toString('utf8');
  if (/^\s*[\[{]/.test(prefix)) return { mediaType: 'application/json', extension: '.json' };
  if (prefix.includes(',') && !prefix.includes('\0')) return { mediaType: 'text/csv', extension: '.csv' };
  if (!prefix.includes('\0')) return { mediaType: 'text/plain', extension: '.txt' };
  return { mediaType: 'application/octet-stream', extension: '.bin' };
}

export function saveInertDownload(root, buffer, { suggestedName = null, sequence = 1 } = {}) {
  if (!Buffer.isBuffer(buffer)) throw new TypeError('Download body must be a Buffer');
  if (buffer.length > MAX_DOWNLOAD_BYTES) throw new Error(`Download exceeds ${MAX_DOWNLOAD_BYTES} bytes`);
  const digest = sha256(buffer);
  const detected = detectMediaType(buffer);
  const name = `download-${String(sequence).padStart(3, '0')}-${digest.slice(0, 12)}${detected.extension}`;
  if (!existsSync(root)) mkdirSync(root, { recursive: true, mode: 0o700 });
  const path = assertContainedPath(root, join(root, name));
  if (existsSync(path)) throw new Error(`Download collision: ${name}`);
  atomicWrite(path, buffer);
  return { generatedName: name, suggestedName: suggestedName == null ? null : String(suggestedName), sha256: digest, size: buffer.length, mediaType: detected.mediaType, originalExtension: suggestedName ? extname(String(suggestedName)) : '' };
}

