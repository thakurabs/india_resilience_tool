import { existsSync, mkdirSync, readFileSync } from 'node:fs';
import { basename, join, resolve } from 'node:path';
import { atomicWrite, canonicalJson, hashFile } from './canonical.mjs';
import { assertNonSymlinkFile } from './paths.mjs';

export function registerReferenceData(root, { dataPath, sourceUrl, retrievalDate, schema, worksheet }) {
  const referenceRoot = resolve(root, 'reference-data');
  mkdirSync(referenceRoot, { recursive: true, mode: 0o700 });
  const source = assertNonSymlinkFile(referenceRoot, resolve(referenceRoot, dataPath));
  if (new URL(sourceUrl).protocol !== 'https:') throw new Error('Reference source URL must use HTTPS');
  if (!/^\d{4}-\d{2}-\d{2}$/.test(retrievalDate)) throw new Error('retrievalDate must be YYYY-MM-DD');
  if (!schema || !worksheet) throw new Error('Reference schema and calculation worksheet are required');
  const record = { file: basename(source), sha256: hashFile(source), sourceUrl, retrievalDate, schema, worksheet };
  const manifestPath = join(referenceRoot, 'manifest.json');
  const manifest = existsSync(manifestPath) ? JSON.parse(readFileSync(manifestPath, 'utf8')) : { version: 1, references: [] };
  if (manifest.references.some((item) => item.file === record.file)) throw new Error(`Reference already registered: ${record.file}`);
  manifest.references.push(record); manifest.references.sort((a, b) => a.file.localeCompare(b.file));
  atomicWrite(manifestPath, `${canonicalJson(manifest)}\n`);
  return record;
}
