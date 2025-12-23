//! JDB Client implementation JDB 客户端实现

use crate::error::Result;
use jdb_comm::TableID;
use jdb_runtime::{Runtime, RuntimeConfig};
use std::path::Path;

/// JDB Client JDB 客户端
pub struct JdbClient {
  rt: Runtime,
}

impl JdbClient {
  /// Open database at path 在指定路径打开数据库
  pub fn open(path: impl AsRef<Path>) -> Result<Self> {
    Self::open_with_workers(path, 1)
  }

  /// Open with specified worker count 指定 worker 数量打开
  pub fn open_with_workers(path: impl AsRef<Path>, workers: usize) -> Result<Self> {
    let mut rt = Runtime::new();
    let cfg = RuntimeConfig {
      workers,
      bind_cores: false,
      data_dir: path.as_ref().to_path_buf(),
    };
    rt.start(cfg)?;
    Ok(Self { rt })
  }

  /// Put key-value to table 写入键值到表
  pub async fn put(&self, table: &[u8], key: &[u8], val: &[u8]) -> Result<()> {
    let tid = TableID::from_name(table);
    self.rt.put(tid, key.to_vec(), val.to_vec()).await?;
    Ok(())
  }

  /// Get value by key 通过键获取值
  pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
    Ok(self.rt.get(key).await?)
  }

  /// Delete key from table 从表中删除键
  pub async fn delete(&self, table: &[u8], key: &[u8]) -> Result<bool> {
    let tid = TableID::from_name(table);
    Ok(self.rt.delete(tid, key).await?)
  }

  /// Range scan 范围扫描
  pub async fn range(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    Ok(self.rt.range(start, end).await?)
  }

  /// Flush to disk 刷新到磁盘
  pub async fn flush(&self) -> Result<()> {
    Ok(self.rt.flush().await?)
  }

  /// Close client 关闭客户端
  pub fn close(mut self) {
    self.rt.shutdown();
  }
}
