import { createHash } from 'node:crypto';
import { closeSync, fsyncSync, openSync, readFileSync, renameSync, unlinkSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';

function normalize(value) {
  if (Array.isArray(value)) return value.map(normalize);
  if (value && typeof value === 'object') {
    return Object.fromEntries(Object.keys(value).sort().map((key) => [key, normalize(value[key])]));
  }
  if (typeof value === 'number' && !Number.isFinite(value)) throw new TypeError('Canonical JSON forbids non-finite numbers');
  return value;
}

export function canonicalJson(value) {
  return JSON.stringify(normalize(value));
}

export function sha256(value) {
  const bytes = Buffer.isBuffer(value) ? value : Buffer.from(String(value), 'utf8');
  return createHash('sha256').update(bytes).digest('hex');
}

export function hashJson(value) {
  return sha256(canonicalJson(value));
}

export function hashFile(path) {
  return sha256(readFileSync(path));
}

export function atomicWrite(path, data) {
  const temp = join(dirname(path), `.${process.pid}.${Date.now()}.${sha256(path).slice(0, 8)}.tmp`);
  const descriptor = openSync(temp, 'wx', 0o600);
  try {
    writeFileSync(descriptor, data);
    fsyncSync(descriptor);
  } finally {
    closeSync(descriptor);
  }
  try {
    renameSync(temp, path);
  } catch (error) {
    try { unlinkSync(temp); } catch {}
    throw error;
  }
  const directory = openSync(dirname(path), 'r');
  try { fsyncSync(directory); } finally { closeSync(directory); }
}

