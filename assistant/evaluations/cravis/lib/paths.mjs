import { lstatSync, realpathSync } from 'node:fs';
import { dirname, isAbsolute, relative, resolve, sep } from 'node:path';

function inside(base, candidate) {
  const rel = relative(base, candidate);
  return rel !== '' && !rel.startsWith(`..${sep}`) && rel !== '..' && !isAbsolute(rel);
}

export function assertContainedPath(basePath, candidatePath, { allowBase = false, mustExist = false } = {}) {
  const base = realpathSync(resolve(basePath));
  const candidate = resolve(candidatePath);
  const rel = relative(base, candidate);
  if ((!allowBase && rel === '') || rel === '..' || rel.startsWith(`..${sep}`) || isAbsolute(rel)) {
    throw new Error(`Path is outside the allowed root: ${candidate}`);
  }

  let probe = candidate;
  const missing = [];
  while (probe !== base) {
    try {
      const stat = lstatSync(probe);
      if (stat.isSymbolicLink()) throw new Error(`Symlinks are forbidden in contained paths: ${probe}`);
      break;
    } catch (error) {
      if (error.code !== 'ENOENT') throw error;
      missing.push(probe);
      probe = dirname(probe);
    }
  }
  if (mustExist && missing.length) throw new Error(`Required path does not exist: ${candidate}`);
  const existingReal = realpathSync(probe);
  if (existingReal !== base && !inside(base, existingReal)) throw new Error(`Resolved path escapes allowed root: ${candidate}`);
  return candidate;
}

export function assertNonSymlinkFile(basePath, candidatePath) {
  const resolved = assertContainedPath(basePath, candidatePath, { mustExist: true });
  const stat = lstatSync(resolved);
  if (!stat.isFile() || stat.isSymbolicLink()) throw new Error(`Expected a non-symlink file: ${resolved}`);
  return resolved;
}

