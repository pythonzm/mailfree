/**
 * 数据库初始化模块
 * @module db/init
 */

import { clearExpiredCache } from '../utils/cache.js';
import { resolveMailboxExpireHours } from '../utils/mailboxLifecycle.js';

const CORE_TABLES = ['mailboxes', 'messages', 'users', 'user_mailboxes', 'sent_emails'];

const MAILBOXES_TABLE_SQL = `
  CREATE TABLE IF NOT EXISTS mailboxes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    address TEXT NOT NULL UNIQUE,
    local_part TEXT NOT NULL,
    domain TEXT NOT NULL,
    password_hash TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    last_accessed_at TEXT,
    expires_at TEXT,
    is_pinned INTEGER DEFAULT 0,
    can_login INTEGER DEFAULT 0,
    forward_to TEXT DEFAULT NULL,
    is_favorite INTEGER DEFAULT 0
  );
`;

const MESSAGES_TABLE_SQL = `
  CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    mailbox_id INTEGER NOT NULL,
    sender TEXT NOT NULL,
    to_addrs TEXT NOT NULL DEFAULT '',
    subject TEXT NOT NULL,
    verification_code TEXT,
    preview TEXT,
    r2_bucket TEXT NOT NULL DEFAULT 'mail-eml',
    r2_object_key TEXT NOT NULL DEFAULT '',
    received_at TEXT DEFAULT CURRENT_TIMESTAMP,
    is_read INTEGER DEFAULT 0,
    FOREIGN KEY(mailbox_id) REFERENCES mailboxes(id)
  );
`;

const USERS_TABLE_SQL = `
  CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT,
    role TEXT NOT NULL DEFAULT 'user',
    can_send INTEGER NOT NULL DEFAULT 0,
    mailbox_limit INTEGER NOT NULL DEFAULT 10,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
  );
`;

const USER_MAILBOXES_TABLE_SQL = `
  CREATE TABLE IF NOT EXISTS user_mailboxes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    mailbox_id INTEGER NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    is_pinned INTEGER NOT NULL DEFAULT 0,
    UNIQUE(user_id, mailbox_id),
    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY(mailbox_id) REFERENCES mailboxes(id) ON DELETE CASCADE
  );
`;

const SENT_EMAILS_TABLE_SQL = `
  CREATE TABLE IF NOT EXISTS sent_emails (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    resend_id TEXT,
    from_name TEXT,
    from_addr TEXT NOT NULL,
    to_addrs TEXT NOT NULL,
    subject TEXT NOT NULL,
    html_content TEXT,
    text_content TEXT,
    status TEXT DEFAULT 'queued',
    scheduled_at TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
  );
`;

const MAILBOX_TOMBSTONES_TABLE_SQL = `
  CREATE TABLE IF NOT EXISTS mailbox_tombstones (
    address TEXT PRIMARY KEY,
    expired_at TEXT NOT NULL,
    blocked_until TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
  );
`;

// Worker 生命周期内复用的初始化标记
let _isFirstInit = true;

/**
 * 轻量级数据库初始化。
 * 仅在 Worker 首次启动时执行结构检查，后续请求只做必要兜底。
 * @param {object} db - 数据库连接对象
 * @returns {Promise<void>}
 */
export async function initDatabase(db, env = {}) {
  try {
    clearExpiredCache();
    await enableForeignKeys(db);

    if (_isFirstInit) {
      await performFirstTimeSetup(db, env);
      _isFirstInit = false;
      return;
    }

    await ensureMailboxTombstonesTable(db);
  } catch (error) {
    console.error('数据库初始化失败:', error);
    throw error;
  }
}

/**
 * 首次启动时检查表结构并补齐缺失结构。
 * @param {object} db - 数据库连接对象
 * @returns {Promise<void>}
 */
async function performFirstTimeSetup(db, env = {}) {
  const hasAllCoreTables = await checkCoreTables(db);

  if (!hasAllCoreTables) {
    console.log('检测到数据库表不完整，开始初始化...');
    await ensureBaseTables(db);
  }

  await migrateMailboxesFields(db, env);
  await migrateLegacyTables(db);
  await ensureMailboxTombstonesTable(db);
  await createIndexes(db);
}

/**
 * 检查核心业务表是否都已存在。
 * @param {object} db - 数据库连接对象
 * @returns {Promise<boolean>}
 */
async function checkCoreTables(db) {
  try {
    for (const tableName of CORE_TABLES) {
      await db.prepare(`SELECT 1 FROM ${tableName} LIMIT 1`).all();
    }
    return true;
  } catch (_) {
    return false;
  }
}

/**
 * 打开 SQLite 外键约束。
 * @param {object} db - 数据库连接对象
 * @returns {Promise<void>}
 */
async function enableForeignKeys(db) {
  await db.exec('PRAGMA foreign_keys = ON;');
}

/**
 * 创建完整业务表结构。
 * @param {object} db - 数据库连接对象
 * @returns {Promise<void>}
 */
async function ensureBaseTables(db) {
  await db.exec(MAILBOXES_TABLE_SQL);
  await db.exec(MESSAGES_TABLE_SQL);
  await db.exec(USERS_TABLE_SQL);
  await db.exec(USER_MAILBOXES_TABLE_SQL);
  await db.exec(SENT_EMAILS_TABLE_SQL);
  await ensureMailboxTombstonesTable(db);
}

/**
 * 创建邮箱墓碑表，用于阻止过期地址被自动复活。
 * @param {object} db - 数据库连接对象
 * @returns {Promise<void>}
 */
async function ensureMailboxTombstonesTable(db) {
  await db.exec(MAILBOX_TOMBSTONES_TABLE_SQL);
}

/**
 * 创建数据库索引。
 * @param {object} db - 数据库连接对象
 * @returns {Promise<void>}
 */
async function createIndexes(db) {
  await createIndexIfColumnsExist(db, 'mailboxes', ['address'], 'CREATE INDEX IF NOT EXISTS idx_mailboxes_address ON mailboxes(address);');
  await createIndexIfColumnsExist(db, 'mailboxes', ['expires_at'], 'CREATE INDEX IF NOT EXISTS idx_mailboxes_expires_at ON mailboxes(expires_at);');
  await createIndexIfColumnsExist(db, 'mailboxes', ['is_pinned'], 'CREATE INDEX IF NOT EXISTS idx_mailboxes_is_pinned ON mailboxes(is_pinned DESC);');
  await createIndexIfColumnsExist(db, 'mailboxes', ['address', 'created_at'], 'CREATE INDEX IF NOT EXISTS idx_mailboxes_address_created ON mailboxes(address, created_at DESC);');
  await createIndexIfColumnsExist(db, 'mailboxes', ['is_favorite'], 'CREATE INDEX IF NOT EXISTS idx_mailboxes_is_favorite ON mailboxes(is_favorite DESC);');
  await createIndexIfColumnsExist(db, 'messages', ['mailbox_id'], 'CREATE INDEX IF NOT EXISTS idx_messages_mailbox_id ON messages(mailbox_id);');
  await createIndexIfColumnsExist(db, 'messages', ['received_at'], 'CREATE INDEX IF NOT EXISTS idx_messages_received_at ON messages(received_at DESC);');
  await createIndexIfColumnsExist(db, 'messages', ['r2_object_key'], 'CREATE INDEX IF NOT EXISTS idx_messages_r2_object_key ON messages(r2_object_key);');
  await createIndexIfColumnsExist(db, 'messages', ['mailbox_id', 'received_at'], 'CREATE INDEX IF NOT EXISTS idx_messages_mailbox_received ON messages(mailbox_id, received_at DESC);');
  await createIndexIfColumnsExist(db, 'messages', ['mailbox_id', 'received_at', 'is_read'], 'CREATE INDEX IF NOT EXISTS idx_messages_mailbox_received_read ON messages(mailbox_id, received_at DESC, is_read);');
  await createIndexIfColumnsExist(db, 'users', ['username'], 'CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);');
  await createIndexIfColumnsExist(db, 'user_mailboxes', ['user_id'], 'CREATE INDEX IF NOT EXISTS idx_user_mailboxes_user ON user_mailboxes(user_id);');
  await createIndexIfColumnsExist(db, 'user_mailboxes', ['mailbox_id'], 'CREATE INDEX IF NOT EXISTS idx_user_mailboxes_mailbox ON user_mailboxes(mailbox_id);');
  await createIndexIfColumnsExist(db, 'user_mailboxes', ['user_id', 'is_pinned'], 'CREATE INDEX IF NOT EXISTS idx_user_mailboxes_user_pinned ON user_mailboxes(user_id, is_pinned DESC);');
  await createIndexIfColumnsExist(db, 'user_mailboxes', ['user_id', 'mailbox_id', 'is_pinned'], 'CREATE INDEX IF NOT EXISTS idx_user_mailboxes_composite ON user_mailboxes(user_id, mailbox_id, is_pinned);');
  await createIndexIfColumnsExist(db, 'sent_emails', ['resend_id'], 'CREATE INDEX IF NOT EXISTS idx_sent_emails_resend_id ON sent_emails(resend_id);');
  await createIndexIfColumnsExist(db, 'sent_emails', ['status', 'created_at'], 'CREATE INDEX IF NOT EXISTS idx_sent_emails_status_created ON sent_emails(status, created_at DESC);');
  await createIndexIfColumnsExist(db, 'sent_emails', ['from_addr'], 'CREATE INDEX IF NOT EXISTS idx_sent_emails_from_addr ON sent_emails(from_addr);');
  await createIndexIfColumnsExist(db, 'mailbox_tombstones', ['blocked_until'], 'CREATE INDEX IF NOT EXISTS idx_mailbox_tombstones_blocked_until ON mailbox_tombstones(blocked_until);');
}

/**
 * 迁移 mailboxes 表缺失字段，并补齐墓碑表。
 * @param {object} db - 数据库连接对象
 * @returns {Promise<void>}
 */
async function migrateMailboxesFields(db, env = {}) {
  try {
    await ensureTableColumn(db, 'mailboxes', 'last_accessed_at', 'ALTER TABLE mailboxes ADD COLUMN last_accessed_at TEXT;', '已补齐 mailboxes.last_accessed_at 字段');
    await ensureTableColumn(db, 'mailboxes', 'expires_at', 'ALTER TABLE mailboxes ADD COLUMN expires_at TEXT;', '已补齐 mailboxes.expires_at 字段');
    await ensureTableColumn(db, 'mailboxes', 'is_pinned', 'ALTER TABLE mailboxes ADD COLUMN is_pinned INTEGER DEFAULT 0;', '已补齐 mailboxes.is_pinned 字段');
    await ensureTableColumn(db, 'mailboxes', 'can_login', 'ALTER TABLE mailboxes ADD COLUMN can_login INTEGER DEFAULT 0;', '已补齐 mailboxes.can_login 字段');
    await ensureTableColumn(db, 'mailboxes', 'forward_to', 'ALTER TABLE mailboxes ADD COLUMN forward_to TEXT DEFAULT NULL;', '已补齐 mailboxes.forward_to 字段');
    await ensureTableColumn(db, 'mailboxes', 'is_favorite', 'ALTER TABLE mailboxes ADD COLUMN is_favorite INTEGER DEFAULT 0;', '已补齐 mailboxes.is_favorite 字段');

    await backfillLegacyMailboxExpiresAt(db, env);
    await ensureMailboxTombstonesTable(db);
  } catch (error) {
    console.error('mailboxes 字段迁移失败:', error);
  }
}

/**
 * 迁移旧版本其他业务表缺失字段，避免旧库升级时索引或查询失败。
 * @param {object} db - 数据库连接对象
 * @returns {Promise<void>}
 */
async function migrateLegacyTables(db) {
  try {
    await ensureTableColumn(db, 'users', 'role', "ALTER TABLE users ADD COLUMN role TEXT NOT NULL DEFAULT 'user';", '已补齐 users.role 字段');
    await ensureTableColumn(db, 'users', 'can_send', 'ALTER TABLE users ADD COLUMN can_send INTEGER NOT NULL DEFAULT 0;', '已补齐 users.can_send 字段');
    await ensureTableColumn(db, 'users', 'mailbox_limit', 'ALTER TABLE users ADD COLUMN mailbox_limit INTEGER NOT NULL DEFAULT 10;', '已补齐 users.mailbox_limit 字段');

    await ensureTableColumn(db, 'user_mailboxes', 'created_at', 'ALTER TABLE user_mailboxes ADD COLUMN created_at TEXT;', '已补齐 user_mailboxes.created_at 字段');
    await ensureTableColumn(db, 'user_mailboxes', 'is_pinned', 'ALTER TABLE user_mailboxes ADD COLUMN is_pinned INTEGER NOT NULL DEFAULT 0;', '已补齐 user_mailboxes.is_pinned 字段');

    await ensureTableColumn(db, 'sent_emails', 'from_name', 'ALTER TABLE sent_emails ADD COLUMN from_name TEXT;', '已补齐 sent_emails.from_name 字段');
    await ensureTableColumn(db, 'sent_emails', 'status', "ALTER TABLE sent_emails ADD COLUMN status TEXT DEFAULT 'queued';", '已补齐 sent_emails.status 字段');
    await ensureTableColumn(db, 'sent_emails', 'scheduled_at', 'ALTER TABLE sent_emails ADD COLUMN scheduled_at TEXT;', '已补齐 sent_emails.scheduled_at 字段');
    await ensureTableColumn(db, 'sent_emails', 'created_at', 'ALTER TABLE sent_emails ADD COLUMN created_at TEXT;', '已补齐 sent_emails.created_at 字段');
    await ensureTableColumn(db, 'sent_emails', 'updated_at', 'ALTER TABLE sent_emails ADD COLUMN updated_at TEXT;', '已补齐 sent_emails.updated_at 字段');
  } catch (error) {
    console.error('旧表结构迁移失败:', error);
  }
}

/**
 * 读取指定表的字段集合。
 * @param {object} db - 数据库连接对象
 * @param {string} tableName - 表名
 * @returns {Promise<string[]>}
 */
async function getTableColumnNames(db, tableName) {
  try {
    const columns = await db.prepare(`PRAGMA table_info(${tableName})`).all();
    return (columns.results || []).map((column) => column.name);
  } catch (_) {
    return [];
  }
}

/**
 * 确保指定表包含某个字段，不存在时自动补齐。
 * @param {object} db - 数据库连接对象
 * @param {string} tableName - 表名
 * @param {string} columnName - 字段名
 * @param {string} sql - 补字段 SQL
 * @param {string} successLog - 成功日志
 * @returns {Promise<void>}
 */
async function ensureTableColumn(db, tableName, columnName, sql, successLog) {
  const columnNames = await getTableColumnNames(db, tableName);
  if (columnNames.includes(columnName)) {
    return;
  }

  await db.exec(sql);
  if (successLog) {
    console.log(successLog);
  }
}

/**
 * 仅在索引依赖字段齐全时创建索引，避免旧库升级阶段因缺字段直接失败。
 * @param {object} db - 数据库连接对象
 * @param {string} tableName - 表名
 * @param {string[]} requiredColumns - 依赖字段
 * @param {string} sql - 建索引 SQL
 * @returns {Promise<void>}
 */
async function createIndexIfColumnsExist(db, tableName, requiredColumns, sql) {
  const columnNames = await getTableColumnNames(db, tableName);
  if (!requiredColumns.every((columnName) => columnNames.includes(columnName))) {
    return;
  }

  await db.exec(sql);
}

/**
 * 为旧版本遗留邮箱回填过期时间，使其纳入自动清理。
 * 只处理 expires_at 为空的历史数据，回填基准优先使用最后访问时间。
 * @param {object} db - 数据库连接对象
 * @param {object} env - 环境变量对象
 * @returns {Promise<void>}
 */
async function backfillLegacyMailboxExpiresAt(db, env = {}) {
  const ttlHours = resolveMailboxExpireHours(env);
  const ttlModifier = `+${ttlHours} hours`;

  const result = await db.prepare(`
    UPDATE mailboxes
    SET expires_at = strftime(
      '%Y-%m-%dT%H:%M:%fZ',
      COALESCE(last_accessed_at, created_at, CURRENT_TIMESTAMP),
      ?
    )
    WHERE expires_at IS NULL
  `).bind(ttlModifier).run();

  const changedRows = result?.meta?.changes || 0;
  if (changedRows > 0) {
    console.log(`已为 ${changedRows} 个旧邮箱回填 expires_at，旧数据将纳入自动清理`);
  }
}

/**
 * 完整数据库初始化脚本，用于首次部署或人工初始化。
 * @param {object} db - 数据库连接对象
 * @returns {Promise<void>}
 */
export async function setupDatabase(db) {
  await enableForeignKeys(db);
  await ensureBaseTables(db);
  await createIndexes(db);
}
