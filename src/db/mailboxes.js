/**
 * 邮箱数据库操作模块
 * @module db/mailboxes
 */

import {
  getCachedMailboxId,
  updateMailboxIdCache,
  invalidateMailboxCache,
  invalidateSystemStatCache,
  getCachedSystemStat
} from '../utils/cache.js';
import {
  createMailboxExpiresAt,
  isExpiredAt,
  nowIso
} from '../utils/mailboxLifecycle.js';
import { deleteR2Objects } from '../utils/r2.js';

function parseMailboxAddress(address) {
  const normalized = String(address || '').trim().toLowerCase();
  const at = normalized.indexOf('@');
  if (!normalized || at <= 0 || at >= normalized.length - 1) {
    return null;
  }

  return {
    normalized,
    localPart: normalized.slice(0, at),
    domain: normalized.slice(at + 1)
  };
}

function buildMailboxOptions(options = {}) {
  const currentIso = options.nowIso || nowIso();
  return {
    allowAutoCreate: options.allowAutoCreate !== false,
    allowReviveExpired: options.allowReviveExpired !== false,
    clearTombstone: !!options.clearTombstone,
    touchExisting: options.touchExisting !== false,
    currentIso,
    expiresAt: options.expiresAt || createMailboxExpiresAt(options.ttlMs),
    tombstoneBlockedUntil: options.tombstoneBlockedUntil || null
  };
}

async function getMailboxRow(db, normalized) {
  return await db.prepare(`
    SELECT id, address, local_part, domain, expires_at
    FROM mailboxes
    WHERE address = ?
    LIMIT 1
  `).bind(normalized).first();
}

export async function clearMailboxTombstone(db, address) {
  const normalized = String(address || '').trim().toLowerCase();
  if (!normalized) return;
  await db.prepare('DELETE FROM mailbox_tombstones WHERE address = ?').bind(normalized).run();
}

export async function blockMailboxReuse(db, address, { expiredAt, blockedUntil }) {
  const normalized = String(address || '').trim().toLowerCase();
  if (!normalized || !expiredAt || !blockedUntil) return;

  await db.prepare(`
    INSERT INTO mailbox_tombstones (address, expired_at, blocked_until)
    VALUES (?, ?, ?)
    ON CONFLICT(address) DO UPDATE SET
      expired_at = excluded.expired_at,
      blocked_until = excluded.blocked_until
  `).bind(normalized, expiredAt, blockedUntil).run();
}

export async function isMailboxBlocked(db, address, currentTimeIso = nowIso()) {
  const normalized = String(address || '').trim().toLowerCase();
  if (!normalized) return false;

  const row = await db.prepare(`
    SELECT blocked_until
    FROM mailbox_tombstones
    WHERE address = ?
    LIMIT 1
  `).bind(normalized).first();

  if (!row?.blocked_until) return false;
  if (String(row.blocked_until) > currentTimeIso) return true;

  await clearMailboxTombstone(db, normalized);
  return false;
}

export async function touchMailboxActivity(db, mailboxId, expiresAt) {
  if (!mailboxId) return;

  if (expiresAt) {
    await db.prepare(`
      UPDATE mailboxes
      SET last_accessed_at = CURRENT_TIMESTAMP, expires_at = ?
      WHERE id = ?
    `).bind(expiresAt, mailboxId).run();
    return;
  }

  await db.prepare('UPDATE mailboxes SET last_accessed_at = CURRENT_TIMESTAMP WHERE id = ?')
    .bind(mailboxId)
    .run();
}

/**
 * 获取或创建邮箱ID，如果邮箱不存在则自动创建
 * @param {object} db - 数据库连接对象
 * @param {string} address - 邮箱地址
 * @returns {Promise<number>} 邮箱ID
 * @throws {Error} 当邮箱地址无效时抛出异常
 */
export async function getOrCreateMailboxId(db, address, options = {}) {
  const parsed = parseMailboxAddress(address);
  const normalized = parsed?.normalized || '';
  const mailboxOptions = buildMailboxOptions(options);
  if (mailboxOptions.clearTombstone) {
    await clearMailboxTombstone(db, normalized);
  } else if (await isMailboxBlocked(db, normalized, mailboxOptions.currentIso)) {
    return null;
  }
  if (!normalized) throw new Error('无效的邮箱地址');
  
  // 先检查缓存
  const cachedId = await getCachedMailboxId(db, normalized);
  if (cachedId) {
    if (mailboxOptions.touchExisting) {
    // 更新访问时间（使用后台任务，不阻塞主流程）
      db.prepare(`
        UPDATE mailboxes
        SET last_accessed_at = CURRENT_TIMESTAMP, expires_at = ?
        WHERE id = ?
      `).bind(mailboxOptions.expiresAt, cachedId).run().catch(() => {});
      updateMailboxIdCache(normalized, cachedId, mailboxOptions.expiresAt);
    }
    return cachedId;
  }
  
  const existingRow = await getMailboxRow(db, normalized);
  if (existingRow?.id) {
    if (isExpiredAt(existingRow.expires_at, mailboxOptions.currentIso) && !mailboxOptions.allowReviveExpired) {
      return null;
    }

    const nextExpiresAt = mailboxOptions.touchExisting ? mailboxOptions.expiresAt : existingRow.expires_at;
    if (mailboxOptions.touchExisting) {
      await touchMailboxActivity(db, existingRow.id, nextExpiresAt);
    }
    updateMailboxIdCache(normalized, existingRow.id, nextExpiresAt || existingRow.expires_at || null);
    return existingRow.id;
  }
  
  // 解析邮箱地址
  let local_part = '';
  let domain = '';
  const at = normalized.indexOf('@');
  if (at > 0 && at < normalized.length - 1) {
    local_part = normalized.slice(0, at);
    domain = normalized.slice(at + 1);
  }
  if (!local_part || !domain) throw new Error('无效的邮箱地址');
  
  // 再次查询数据库（避免并发创建）
  const existing = await db.prepare('SELECT id FROM mailboxes WHERE address = ? LIMIT 1').bind(normalized).all();
  if (existing.results && existing.results.length > 0) {
    const id = existing.results[0].id;
    updateMailboxIdCache(normalized, id, mailboxOptions.expiresAt);
    await touchMailboxActivity(db, id, mailboxOptions.expiresAt);
    return id;
  }
  
  // 创建新邮箱
  await db.prepare(
    'INSERT INTO mailboxes (address, local_part, domain, password_hash, last_accessed_at, expires_at) VALUES (?, ?, ?, NULL, CURRENT_TIMESTAMP, ?)'
  ).bind(normalized, local_part, domain, mailboxOptions.expiresAt).run();
  
  // 查询新创建的ID
  const created = await db.prepare('SELECT id FROM mailboxes WHERE address = ? LIMIT 1').bind(normalized).all();
  const newId = created.results[0].id;
  
  // 更新缓存
  updateMailboxIdCache(normalized, newId, mailboxOptions.expiresAt);
  
  // 使系统统计缓存失效（邮箱数量变化）
  invalidateSystemStatCache('total_mailboxes');
  
  return newId;
}

/**
 * 根据邮箱地址获取邮箱ID
 * @param {object} db - 数据库连接对象
 * @param {string} address - 邮箱地址
 * @returns {Promise<number|null>} 邮箱ID，如果不存在返回null
 */
export async function getMailboxIdByAddress(db, address) {
  const normalized = String(address || '').trim().toLowerCase();
  if (!normalized) return null;
  
  // 使用缓存
  return await getCachedMailboxId(db, normalized);
}

/**
 * 检查邮箱是否存在以及是否属于特定用户
 * @param {object} db - 数据库连接对象
 * @param {string} address - 邮箱地址
 * @param {number} userId - 用户ID（可选）
 * @returns {Promise<object>} 包含exists(是否存在)、ownedByUser(是否属于该用户)、mailboxId的对象
 */
export async function checkMailboxOwnership(db, address, userId = null) {
  const normalized = String(address || '').trim().toLowerCase();
  if (!normalized) return { exists: false, ownedByUser: false, mailboxId: null };
  
  // 检查邮箱是否存在
  const mailboxId = await getMailboxIdByAddress(db, normalized);
  if (!mailboxId) {
    return { exists: false, ownedByUser: false, mailboxId: null };
  }
  
  // 如果没有提供用户ID，只返回存在性检查结果
  if (!userId) {
    return { exists: true, ownedByUser: false, mailboxId };
  }
  
  // 检查邮箱是否属于该用户
  const ownerRes = await db.prepare(
    'SELECT id FROM user_mailboxes WHERE user_id = ? AND mailbox_id = ? LIMIT 1'
  ).bind(userId, mailboxId).all();
  
  const ownedByUser = ownerRes.results && ownerRes.results.length > 0;
  
  return { exists: true, ownedByUser, mailboxId };
}

/**
 * 切换邮箱的置顶状态
 * @param {object} db - 数据库连接对象
 * @param {string} address - 邮箱地址
 * @param {number} userId - 用户ID
 * @returns {Promise<object>} 包含is_pinned状态的对象
 * @throws {Error} 当邮箱地址无效、用户未登录或邮箱不存在时抛出异常
 */
export async function toggleMailboxPin(db, address, userId) {
  const normalized = String(address || '').trim().toLowerCase();
  if (!normalized) throw new Error('无效的邮箱地址');
  const uid = Number(userId || 0);
  if (!uid) throw new Error('未登录');

  // 获取邮箱 ID
  const mailboxId = await getMailboxIdByAddress(db, normalized);
  if (!mailboxId){
    throw new Error('邮箱不存在');
  }

  // 检查该邮箱是否属于该用户
  const umRes = await db.prepare('SELECT id, is_pinned FROM user_mailboxes WHERE user_id = ? AND mailbox_id = ? LIMIT 1')
    .bind(uid, mailboxId).all();
  if (!umRes.results || umRes.results.length === 0){
    // 若尚未存在关联记录（例如严格管理员未分配该邮箱），则创建一条仅用于个人置顶的关联
    await db.prepare('INSERT INTO user_mailboxes (user_id, mailbox_id, is_pinned) VALUES (?, ?, 1)')
      .bind(uid, mailboxId).run();
    return { is_pinned: 1 };
  }

  const currentPin = umRes.results[0].is_pinned ? 1 : 0;
  const newPin = currentPin ? 0 : 1;
  await db.prepare('UPDATE user_mailboxes SET is_pinned = ? WHERE user_id = ? AND mailbox_id = ?')
    .bind(newPin, uid, mailboxId).run();
  return { is_pinned: newPin };
}

/**
 * 获取系统中所有邮箱的总数量
 * @param {object} db - 数据库连接对象
 * @returns {Promise<number>} 系统中所有邮箱的总数量
 */
export async function getTotalMailboxCount(db) {
  try {
    // 使用缓存避免频繁的 COUNT 全表扫描
    return await getCachedSystemStat(db, 'total_mailboxes', async (db) => {
      const result = await db.prepare(`
        SELECT COUNT(1) AS count
        FROM mailboxes
        WHERE expires_at IS NULL OR expires_at > ?
      `).bind(nowIso()).all();
      return result?.results?.[0]?.count || 0;
    });
  } catch (error) {
    console.error('获取系统邮箱总数失败:', error);
    return 0;
  }
}

/**
 * 获取邮箱的转发目标
 * @param {object} db - 数据库连接对象
 * @param {string} address - 邮箱地址
 * @returns {Promise<string|null>} 转发目标地址，无配置返回 null
 */
export async function getForwardTarget(db, address) {
  const normalized = String(address || '').trim().toLowerCase();
  if (!normalized) return null;
  
  const result = await db.prepare(
    'SELECT forward_to FROM mailboxes WHERE address = ? AND (expires_at IS NULL OR expires_at > ?) LIMIT 1'
  ).bind(normalized, nowIso()).first();
  
  return result?.forward_to || null;
}

export async function cleanupExpiredMailboxes(db, {
  r2 = null,
  currentTimeIso = nowIso(),
  tombstoneBlockedUntil = null,
  batchSize = 100
} = {}) {
  const limit = Math.max(1, Math.min(500, Number(batchSize) || 100));
  const expiredRows = await db.prepare(`
    SELECT id, address, expires_at
    FROM mailboxes
    WHERE expires_at IS NOT NULL
      AND expires_at <= ?
    ORDER BY expires_at ASC
    LIMIT ?
  `).bind(currentTimeIso, limit).all();

  const mailboxes = expiredRows?.results || [];
  if (!mailboxes.length) {
    await db.prepare('DELETE FROM mailbox_tombstones WHERE blocked_until <= ?').bind(currentTimeIso).run();
    return {
      expiredCount: 0,
      deletedMessages: 0,
      deletedMailboxes: 0,
      deletedR2Objects: 0,
      failedR2Objects: 0,
      deletedTombstones: 0
    };
  }

  const mailboxIds = mailboxes.map(row => row.id);
  const placeholders = mailboxIds.map(() => '?').join(',');
  const keyRows = await db.prepare(`
    SELECT r2_object_key
    FROM messages
    WHERE mailbox_id IN (${placeholders})
      AND r2_object_key IS NOT NULL
      AND r2_object_key != ''
  `).bind(...mailboxIds).all();
  const objectKeys = (keyRows?.results || []).map(row => row.r2_object_key);

  if (tombstoneBlockedUntil) {
    const tombstoneSql = mailboxes.map(() => '(?, ?, ?)').join(',');
    const tombstoneParams = [];
    for (const row of mailboxes) {
      tombstoneParams.push(row.address, row.expires_at || currentTimeIso, tombstoneBlockedUntil);
    }
    await db.prepare(`
      INSERT INTO mailbox_tombstones (address, expired_at, blocked_until)
      VALUES ${tombstoneSql}
      ON CONFLICT(address) DO UPDATE SET
        expired_at = excluded.expired_at,
        blocked_until = excluded.blocked_until
    `).bind(...tombstoneParams).run();
  }

  const deleteMessagesResult = await db.prepare(`
    DELETE FROM messages
    WHERE mailbox_id IN (${placeholders})
  `).bind(...mailboxIds).run();

  await db.prepare(`
    DELETE FROM user_mailboxes
    WHERE mailbox_id IN (${placeholders})
  `).bind(...mailboxIds).run();

  const deleteMailboxesResult = await db.prepare(`
    DELETE FROM mailboxes
    WHERE id IN (${placeholders})
  `).bind(...mailboxIds).run();

  for (const row of mailboxes) {
    invalidateMailboxCache(row.address);
  }
  invalidateSystemStatCache('total_mailboxes');

  const r2Result = await deleteR2Objects(r2, objectKeys);
  const tombstoneCleanup = await db.prepare('DELETE FROM mailbox_tombstones WHERE blocked_until <= ?')
    .bind(currentTimeIso)
    .run();

  return {
    expiredCount: mailboxes.length,
    deletedMessages: deleteMessagesResult?.meta?.changes || 0,
    deletedMailboxes: deleteMailboxesResult?.meta?.changes || 0,
    deletedR2Objects: r2Result.deleted,
    failedR2Objects: r2Result.failed,
    deletedTombstones: tombstoneCleanup?.meta?.changes || 0
  };
}
