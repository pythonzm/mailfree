/**
 * 邮箱生命周期配置与时间计算工具
 * @module utils/mailboxLifecycle
 */

export const DEFAULT_MAILBOX_EXPIRE_HOURS = 1;
export const DEFAULT_MAILBOX_TOMBSTONE_HOURS = 24;
export const DEFAULT_MAILBOX_CLEANUP_BATCH_SIZE = 100;

function parsePositiveNumber(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

export function resolveMailboxExpireHours(env = {}) {
  return parsePositiveNumber(env.MAILBOX_EXPIRE_HOURS, DEFAULT_MAILBOX_EXPIRE_HOURS);
}

export function resolveMailboxTombstoneHours(env = {}) {
  return parsePositiveNumber(env.MAILBOX_TOMBSTONE_HOURS, DEFAULT_MAILBOX_TOMBSTONE_HOURS);
}

export function resolveMailboxCleanupBatchSize(env = {}) {
  const parsed = parsePositiveNumber(env.MAILBOX_CLEANUP_BATCH_SIZE, DEFAULT_MAILBOX_CLEANUP_BATCH_SIZE);
  return Math.max(1, Math.min(500, Math.floor(parsed)));
}

export function resolveMailboxExpireMs(env = {}) {
  return Math.round(resolveMailboxExpireHours(env) * 60 * 60 * 1000);
}

export function resolveMailboxTombstoneMs(env = {}) {
  return Math.round(resolveMailboxTombstoneHours(env) * 60 * 60 * 1000);
}

export function createMailboxExpiresAt(ttlMs, now = Date.now()) {
  const ttl = parsePositiveNumber(ttlMs, DEFAULT_MAILBOX_EXPIRE_HOURS * 60 * 60 * 1000);
  return new Date(now + ttl).toISOString();
}

export function nowIso(now = Date.now()) {
  return new Date(now).toISOString();
}

export function isExpiredAt(expiresAt, currentIso = nowIso()) {
  if (!expiresAt) return false;
  return String(expiresAt) <= currentIso;
}

export function buildMailboxLifecycleOptions(env = {}) {
  const ttlMs = resolveMailboxExpireMs(env);
  return {
    ttlMs,
    expiresAt: createMailboxExpiresAt(ttlMs),
    nowIso: nowIso(),
    tombstoneMs: resolveMailboxTombstoneMs(env),
    cleanupBatchSize: resolveMailboxCleanupBatchSize(env)
  };
}
