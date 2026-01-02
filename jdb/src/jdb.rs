//! Jdb - Main database entry point
//! Jdb - 数据库主入口
//!
//! High-performance embedded KV-separated database.
//! 高性能嵌入式 KV 分离数据库。

use std::path::{Path, PathBuf};

use jdb_ckp::Ckp;
use jdb_val::{Conf as WalConf, Wal};

use crate::{Batch, Conf, ConfItem, Entry, Error, Ns, NsId, NsMgr, Result, Site};

/// Main database entry point
/// 数据库主入口
pub struct Jdb {
  /// Data directory
  /// 数据目录
  dir: PathBuf,
  /// Shared WAL (jdb_val)
  /// 共享 WAL
  wal: Wal,
  /// Checkpoint manager
  /// 检查点管理器
  ckp: Option<Ckp>,
  /// Namespace manager with LRU
  /// 命名空间管理器（LRU）
  ns_mgr: NsMgr,
  /// Configuration
  /// 配置
  conf: Conf,
  /// Database open state
  /// 数据库打开状态
  opened: bool,
}

impl Jdb {
  /// Create new Jdb instance (not opened yet)
  /// 创建新 Jdb 实例（尚未打开）
  ///
  /// # Arguments
  /// - `dir`: Data directory path / 数据目录路径
  /// - `conf`: Configuration items / 配置项
  pub fn new(dir: impl Into<PathBuf>, conf: &[ConfItem]) -> Self {
    let dir = dir.into();
    let conf = Conf::from_items(conf);

    // Create WAL with default config
    // 使用默认配置创建 WAL
    let wal = Wal::new(&dir, &[]);

    // Create namespace manager
    // 创建命名空间管理器
    let ns_mgr = NsMgr::new(dir.clone(), conf.clone());

    Self {
      dir,
      wal,
      ckp: None,
      ns_mgr,
      conf,
      opened: false,
    }
  }

  /// Create with custom WAL configuration
  /// 使用自定义 WAL 配置创建
  pub fn with_wal_conf(dir: impl Into<PathBuf>, conf: &[ConfItem], wal_conf: &[WalConf]) -> Self {
    let dir = dir.into();
    let conf = Conf::from_items(conf);

    // Create WAL with custom config
    // 使用自定义配置创建 WAL
    let wal = Wal::new(&dir, wal_conf);

    // Create namespace manager
    // 创建命名空间管理器
    let ns_mgr = NsMgr::new(dir.clone(), conf.clone());

    Self {
      dir,
      wal,
      ckp: None,
      ns_mgr,
      conf,
      opened: false,
    }
  }

  /// Get data directory
  /// 获取数据目录
  #[inline]
  pub fn dir(&self) -> &Path {
    &self.dir
  }

  /// Get configuration
  /// 获取配置
  #[inline]
  pub fn conf(&self) -> &Conf {
    &self.conf
  }

  /// Check if database is opened
  /// 检查数据库是否已打开
  #[inline]
  pub fn is_opened(&self) -> bool {
    self.opened
  }

  /// Get WAL reference (for batch operations)
  /// 获取 WAL 引用（用于批量操作）
  #[inline]
  pub fn wal(&mut self) -> &mut Wal {
    &mut self.wal
  }

  /// Get namespace manager reference
  /// 获取命名空间管理器引用
  #[inline]
  pub fn ns_mgr(&mut self) -> &mut NsMgr {
    &mut self.ns_mgr
  }

  /// Get namespace handle
  /// 获取命名空间句柄
  #[inline]
  pub fn ns(&mut self, site_id: u64, user_id: u64) -> Ns<'_> {
    Ns::new(&mut self.ns_mgr, NsId::new(site_id, user_id))
  }

  /// Get site handle
  /// 获取站点句柄
  #[inline]
  pub fn site(&mut self, site_id: u64) -> Site<'_> {
    Site::new(&mut self.ns_mgr, site_id)
  }

  /// Create a batch for namespace
  /// 为命名空间创建批次
  #[inline]
  pub fn batch(&self, site_id: u64, user_id: u64) -> Batch {
    Batch::new(NsId::new(site_id, user_id))
  }

  /// Commit a batch atomically
  /// 原子提交批次
  pub async fn commit(&mut self, batch: Batch) -> Result<()> {
    batch.commit(&mut self.wal, &mut self.ns_mgr).await
  }

  /// Drop namespace (delete directory)
  /// 删除命名空间（删除目录）
  pub async fn drop_ns(&mut self, site_id: u64, user_id: u64) -> Result<()> {
    self.ns_mgr.drop(NsId::new(site_id, user_id)).await
  }

  /// Get current WAL position (id, offset)
  /// 获取当前 WAL 位置
  #[inline]
  pub fn wal_pos(&self) -> (u64, u64) {
    (self.wal.cur_id(), self.wal.cur_pos())
  }

  /// Check if WAL has pending writes
  /// 检查 WAL 是否有待写入数据
  #[inline]
  pub fn has_pending(&self) -> bool {
    self.wal.has_pending()
  }

  /// Open database with recovery
  /// 打开数据库并恢复
  ///
  /// 1. Open checkpoint and get recovery info
  /// 2. Open WAL with checkpoint info
  /// 3. Replay WAL entries to rebuild index
  ///
  /// 1. 打开检查点并获取恢复信息
  /// 2. 使用检查点信息打开 WAL
  /// 3. 回放 WAL 条目重建索引
  pub async fn open(&mut self) -> Result<()> {
    if self.opened {
      return Err(Error::AlreadyOpen);
    }

    // Create directory if not exists
    // 如果目录不存在则创建
    if !self.dir.exists() {
      std::fs::create_dir_all(&self.dir)?;
    }

    // Open checkpoint
    // 打开检查点
    let (ckp, after) = jdb_ckp::open(&self.dir, &[]).await?;
    self.ckp = Some(ckp);

    // Open WAL with checkpoint info
    // 使用检查点信息打开 WAL
    let _stream = self.wal.open(after.as_ref()).await?;

    // TODO: replay stream to rebuild index
    // TODO: 回放流重建索引

    self.opened = true;
    Ok(())
  }

  /// Flush all dirty namespaces and write checkpoint
  /// Flush 所有脏命名空间并写入检查点
  pub async fn flush(&mut self) -> Result<()> {
    // Flush WAL first
    // 先 flush WAL
    self.wal.flush().await?;

    // Flush all dirty NsIndex
    // Flush 所有脏 NsIndex
    self.ns_mgr.flush_all().await?;

    // Save checkpoint
    // 保存检查点
    if let Some(ckp) = &mut self.ckp {
      let id = self.wal.cur_id();
      let offset = self.wal.cur_pos();
      ckp.set_wal_ptr(id, offset).await?;
    }

    Ok(())
  }

  /// Sync all data to disk
  /// 同步所有数据到磁盘
  pub async fn sync_all(&mut self) -> Result<()> {
    // Flush first
    // 先 flush
    self.flush().await?;

    // Sync WAL to disk
    // 同步 WAL 到磁盘
    self.wal.sync_all().await?;

    Ok(())
  }

  /// Put key-value to namespace (with NsId prefix in WAL)
  /// 写入键值到命名空间（WAL 中带 NsId 前缀）
  pub async fn put(&mut self, ns_id: NsId, key: &[u8], val: &[u8]) -> Result<()> {
    // Encode key with NsId prefix for WAL
    // 为 WAL 编码带 NsId 前缀的 key
    let wal_key = encode_ns_key(ns_id, key);

    // Write to WAL
    // 写入 WAL
    let pos = self.wal.put(&wal_key, val).await?;

    // Update index (user key without prefix)
    // 更新索引（不带前缀的用户 key）
    let ns_index = self.ns_mgr.get(ns_id).await?;
    ns_index.index.put(key.into(), pos);
    ns_index.dirty = true;

    Ok(())
  }

  /// Delete key from namespace (with NsId prefix in WAL)
  /// 从命名空间删除键（WAL 中带 NsId 前缀）
  pub async fn del(&mut self, ns_id: NsId, key: &[u8]) -> Result<()> {
    // Encode key with NsId prefix for WAL
    // 为 WAL 编码带 NsId 前缀的 key
    let wal_key = encode_ns_key(ns_id, key);

    // Write tombstone to WAL
    // 写入删除标记到 WAL
    let pos = self.wal.del(&wal_key).await?;

    // Update index (user key without prefix)
    // 更新索引（不带前缀的用户 key）
    let ns_index = self.ns_mgr.get(ns_id).await?;
    ns_index.index.del(key.into());
    ns_index.dirty = true;

    // Suppress unused variable warning
    let _ = pos;

    Ok(())
  }

  /// Get value from namespace
  /// 从命名空间获取值
  pub async fn get(&mut self, ns_id: NsId, key: &[u8]) -> Result<Option<Vec<u8>>> {
    // Get from index first
    // 先从索引获取
    let ns_index = self.ns_mgr.get(ns_id).await?;
    let entry = ns_index.index.get(key).await?;

    match entry {
      Some(Entry::Value(pos)) => {
        // Read value from WAL
        // 从 WAL 读取值
        let val = self.wal.val(pos).await?;
        Ok(Some(val.to_vec()))
      }
      Some(Entry::Tombstone) | None => Ok(None),
    }
  }
}

/// Encode NsId + user key for WAL storage
/// 编码 NsId + 用户 key 用于 WAL 存储
#[inline]
fn encode_ns_key(ns_id: NsId, key: &[u8]) -> Vec<u8> {
  let mut buf = Vec::with_capacity(16 + key.len());
  buf.extend_from_slice(&ns_id.site_id.to_le_bytes());
  buf.extend_from_slice(&ns_id.user_id.to_le_bytes());
  buf.extend_from_slice(key);
  buf
}
