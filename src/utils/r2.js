/**
 * R2 对象批量删除工具
 * @module utils/r2
 */

export async function deleteR2Objects(r2, keys = []) {
  if (!r2 || !Array.isArray(keys) || keys.length === 0) {
    return { deleted: 0, failed: 0 };
  }

  const uniqueKeys = [...new Set(
    keys
      .map(key => String(key || '').trim())
      .filter(Boolean)
  )];

  let deleted = 0;
  let failed = 0;

  for (const key of uniqueKeys) {
    try {
      await r2.delete(key);
      deleted++;
    } catch (error) {
      failed++;
      console.error('删除 R2 对象失败:', key, error);
    }
  }

  return { deleted, failed };
}
