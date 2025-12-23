//! Tablet implementation Tablet 实现

use jdb_comm::{JdbResult, TableID, Timestamp, VNodeID};
use jdb_index::BTree;
use jdb_layout::WalEntry;
use jdb_tag::TagIndex;
use jdb_vlog::VlogWriter;
use jdb_wal::{WalReader, WalWriter};
use std::path::{Path, PathBuf};

// File names 文件名常量
const WAL_FILE: &str = "wal.log";
const VLOG_FILE: &str = "vlog.dat";

/// Tablet: a VNode storage unit Tablet：VNode 存储单元
pub struct Tablet {
  vnode: VNodeID,
  #[allow(dead_code)]
  dir: PathBuf,
  wal: Option<WalWriter>,
  vlog: Option<VlogWriter>,
  index: BTree,
  tags: TagIndex,
}

impl Tablet {
  /// Create new tablet 创建新 tablet
  pub async fn create(dir: impl AsRef<Path>, vnode: VNodeID) -> JdbResult<Self> {
    let dir = dir.as_ref().to_path_buf();
    std::fs::create_dir_all(&dir).ok();

    let wal = WalWriter::create(dir.join(WAL_FILE)).await?;
    let vlog = VlogWriter::create(dir.join(VLOG_FILE), u32::from(vnode.0)).await?;

    Ok(Self {
      vnode,
      dir,
      wal: Some(wal),
      vlog: Some(vlog),
      index: BTree::new(),
      tags: TagIndex::new(),
    })
  }

  /// Open existing tablet 打开已有 tablet
  pub async fn open(dir: impl AsRef<Path>, vnode: VNodeID) -> JdbResult<Self> {
    let dir = dir.as_ref().to_path_buf();
    let wal_path = dir.join(WAL_FILE);
    let vlog_path = dir.join(VLOG_FILE);

    // Recover from WAL 从 WAL 恢复
    let mut index = BTree::new();
    let tags = TagIndex::new();

    if wal_path.exists() {
      let mut reader = WalReader::open(&wal_path).await?;
      while let Some(entry) = reader.next()? {
        match entry {
          WalEntry::Put { key, val, .. } => index.insert(key, val),
          WalEntry::Delete { key, .. } => {
            index.delete(&key);
          }
          WalEntry::Barrier { .. } => {}
        }
      }
    }

    let wal = WalWriter::open(&wal_path).await?;
    let vlog = VlogWriter::open(&vlog_path, u32::from(vnode.0)).await?;

    Ok(Self {
      vnode,
      dir,
      wal: Some(wal),
      vlog: Some(vlog),
      index,
      tags,
    })
  }

  #[inline]
  pub fn vnode(&self) -> VNodeID {
    self.vnode
  }

  /// Put key-value (no fsync) 写入键值（不 fsync）
  pub async fn put(&mut self, table: TableID, key: Vec<u8>, val: Vec<u8>) -> JdbResult<()> {
    self.put_inner(table, key, val, false).await
  }

  /// Put key-value with sync 写入键值并同步
  pub async fn put_sync(&mut self, table: TableID, key: Vec<u8>, val: Vec<u8>) -> JdbResult<()> {
    self.put_inner(table, key, val, true).await
  }

  /// Internal put 内部写入
  async fn put_inner(
    &mut self,
    table: TableID,
    key: Vec<u8>,
    val: Vec<u8>,
    sync: bool,
  ) -> JdbResult<()> {
    if let Some(wal) = &mut self.wal {
      let entry = WalEntry::Put {
        table,
        ts: Timestamp::now(),
        key: key.clone(),
        val: val.clone(),
      };
      if sync {
        wal.append_sync(&entry).await?;
      } else {
        wal.append(&entry)?;
      }
    }
    self.index.insert(key, val);
    Ok(())
  }

  pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
    self.index.get(key)
  }

  /// Delete key (no fsync) 删除键（不 fsync）
  pub async fn delete(&mut self, table: TableID, key: &[u8]) -> JdbResult<bool> {
    self.delete_inner(table, key, false).await
  }

  /// Delete key with sync 删除键并同步
  pub async fn delete_sync(&mut self, table: TableID, key: &[u8]) -> JdbResult<bool> {
    self.delete_inner(table, key, true).await
  }

  /// Internal delete 内部删除
  async fn delete_inner(&mut self, table: TableID, key: &[u8], sync: bool) -> JdbResult<bool> {
    if let Some(wal) = &mut self.wal {
      let entry = WalEntry::Delete {
        table,
        ts: Timestamp::now(),
        key: key.to_vec(),
      };
      if sync {
        wal.append_sync(&entry).await?;
      } else {
        wal.append(&entry)?;
      }
    }
    Ok(self.index.delete(key))
  }

  pub fn range(&self, start: &[u8], end: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
    self.index.range(start, end)
  }

  pub fn add_tag(&mut self, id: u32, key: &[u8], val: &[u8]) {
    self.tags.add(id, key, val);
  }

  pub fn query_tags(&self, tags: &[(&[u8], &[u8])]) -> Vec<u32> {
    self.tags.and(tags).iter().collect()
  }

  pub async fn flush(&mut self) -> JdbResult<()> {
    if let Some(wal) = &mut self.wal {
      wal.flush().await?;
    }
    if let Some(vlog) = &mut self.vlog {
      vlog.sync().await?;
    }
    Ok(())
  }
}
