import { randomUUID } from 'node:crypto';
import { closeSync, fstatSync, linkSync, lstatSync, mkdirSync, openSync, readFileSync, realpathSync,
  renameSync, rmSync, unlinkSync, writeFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { basename, dirname, isAbsolute, join, relative, resolve, sep } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';

export function resolvePath(path) {
  if (path === '~' || /^~[/\\]/.test(path)) path = join(homedir(), path.slice(2));
  const absolute = resolve(path);
  return resolveExistingPrefix(absolute);
}

function resolveExistingPrefix(path) {
  try {
    return process.platform === 'win32' ? realpathSync.native(path) : realpathSync(path);
  } catch (error) {
    if (error.code !== 'ENOENT' && error.code !== 'ENOTDIR') throw error;
    const parent = dirname(path);
    return parent === path ? path : join(resolveExistingPrefix(parent), basename(path));
  }
}

export function withinPath(path, root) {
  const rel = relative(root, path);
  return !isAbsolute(rel) && rel !== '..' && !rel.startsWith(`..${sep}`);
}

export function isPlainObject(value) {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

export function parseToolArguments(raw) {
  if (raw == null) return {};
  let args = raw;
  if (typeof raw === 'string') {
    const text = raw.trim();
    if (!text) return {};
    try {
      args = JSON.parse(text);
    } catch {
      throw new Error('Tool arguments must be a JSON object.');
    }
  }
  if (!isPlainObject(args)) throw new Error('Tool arguments must be a JSON object.');
  return args;
}

export const SESSION_ID_MAX_LEN = 128;
export const DEFAULT_LOCK_STALE_MS = 300_000;
const SESSION_ID_RE = /^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$/;
const RESERVED_WIN_NAME = /^(?:con|prn|aux|nul|com[1-9]|lpt[1-9])(?:\.|$)/i;

export function isValidSessionId(sessionId) {
  return typeof sessionId === 'string'
    && SESSION_ID_RE.test(sessionId)
    && !sessionId.includes('..')
    && !sessionId.endsWith('.')
    && !RESERVED_WIN_NAME.test(sessionId);
}

export function validateSessionId(sessionId) {
  const id = typeof sessionId === 'string' ? sessionId.trim() : '';
  if (!isValidSessionId(id)) {
    throw new Error(`Invalid session_id '${sessionId}'`);
  }
  return id;
}

export function lockStaleMsForBudget(timeoutSec) {
  const budgetMs = Number.isFinite(timeoutSec) && timeoutSec > 0 ? Math.ceil(timeoutSec * 1000) + 60_000 : 0;
  return Math.max(DEFAULT_LOCK_STALE_MS, budgetMs);
}

export function atomicJson(path, data, { exclusive = false } = {}) {
  const parent = dirname(path);
  mkdirSync(parent, { recursive: true });
  const temp = join(parent, `.gempt-${randomUUID()}.tmp`);
  try {
    writeFileSync(temp, JSON.stringify(data) + '\n', { encoding: 'utf8', mode: 0o600, flag: 'wx' });
    if (exclusive) linkSync(temp, path);
    else renameSync(temp, path);
  } finally {
    rmSync(temp, { force: true });
  }
}

function unlinkOwnedLock(path, owner) {
  try {
    const current = lstatSync(path);
    if (current.dev !== owner.dev || current.ino !== owner.ino) return false;
    unlinkSync(path);
    return true;
  } catch (error) {
    if (error.code !== 'ENOENT') throw error;
    return true;
  }
}

function staleLock(fd, info, staleMs) {
  const record = /^([1-9]\d*):\d+$/.exec(readFileSync(fd, 'utf8').trim());
  const pid = Number(record?.[1]);
  if (!record || pid > 0x7fffffff) return true;
  try {
    process.kill(pid, 0);
    return false;
  } catch (error) {
    if (error.code === 'ESRCH') return true;
    return false;
  }
}

function reclaimLock(path, staleMs) {
  let fd;
  try {
    fd = openSync(path, 'r');
    const info = fstatSync(fd);
    return staleLock(fd, info, staleMs) && unlinkOwnedLock(path, info);
  } catch (error) {
    if (error.code === 'ENOENT') return true;
    if (error.code === 'EPERM' || error.code === 'EACCES') return false;
    throw error;
  } finally {
    if (fd !== undefined) closeSync(fd);
  }
}

const syncWaitBuf = new Int32Array(new SharedArrayBuffer(4));

export function withLockSync(path, operation, { attempts = 80, delayMs = 25, staleMs = DEFAULT_LOCK_STALE_MS } = {}) {
  const parent = dirname(path);
  mkdirSync(parent, { recursive: true });
  const temp = join(parent, `.gempt-${randomUUID()}.tmp`);
  const fd = openSync(temp, 'wx', 0o600);
  let owner;
  try {
    writeFileSync(fd, `${process.pid}:${Date.now()}\n`);
    const info = fstatSync(fd);
    for (let attempt = 1; ; attempt++) {
      try {
        linkSync(temp, path);
        owner = info;
        break;
      } catch (error) {
        if (attempt >= attempts || !['EEXIST', 'EPERM', 'EACCES'].includes(error.code)) throw error;
        if (error.code === 'EEXIST' && reclaimLock(path, staleMs)) continue;
        Atomics.wait(syncWaitBuf, 0, 0, delayMs);
      }
    }
    unlinkSync(temp);
    return operation();
  } finally {
    try {
      if (owner) unlinkOwnedLock(path, owner);
    } finally {
      closeSync(fd);
      rmSync(temp, { force: true });
    }
  }
}

export async function withLock(path, operation, { attempts = 25, delayMs = 25, staleMs = DEFAULT_LOCK_STALE_MS } = {}) {
  const parent = dirname(path);
  mkdirSync(parent, { recursive: true });
  const temp = join(parent, `.gempt-${randomUUID()}.tmp`);
  const fd = openSync(temp, 'wx', 0o600);
  let owner;
  try {
    writeFileSync(fd, `${process.pid}:${Date.now()}\n`);
    const info = fstatSync(fd);
    for (let attempt = 1; ; attempt++) {
      try {
        linkSync(temp, path);
        owner = info;
        break;
      } catch (error) {
        if (attempt >= attempts || !['EEXIST', 'EPERM', 'EACCES'].includes(error.code)) throw error;
        if (error.code === 'EEXIST' && reclaimLock(path, staleMs)) continue;
        await sleep(delayMs);
      }
    }
    unlinkSync(temp);
    return await operation();
  } finally {
    try {
      if (owner) unlinkOwnedLock(path, owner);
    } finally {
      closeSync(fd);
      rmSync(temp, { force: true });
    }
  }
}
