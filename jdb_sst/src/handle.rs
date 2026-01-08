//! SSTable handle with auto-cleanup
//! 带自动清理的 SSTable 句柄

use std::{cell::{Cell, RefCell}, ops::Deref, path::PathBuf, rc::Rc};

use jdb_fs::{FileLru, fs_id::id_path};
use log::error;

use crate::Table;

type Lru = Rc<RefCell<FileLru>>;

/// SSTable handle with reference-counted cleanup
/// 带引用计数清理的 SSTable 句柄
pub struct Handle {
  table: Table,
  dir: Rc<PathBuf>,
  lru: Lru,
  rm_on_drop: Cell<bool>,
}

impl Handle {
  /// Create handle
  /// 创建句柄
  #[inline]
  pub fn new(table: Table, dir: Rc<PathBuf>, lru: Lru) -> Self {
    Self {
      table,
      dir,
      lru,
      rm_on_drop: Cell::new(false),
    }
  }

  /// Mark for deletion on drop
  /// 标记 drop 时删除
  #[inline]
  pub fn mark_rm(&self) {
    self.rm_on_drop.set(true);
  }

  /// Get inner table reference
  /// 获取内部表引用
  #[inline]
  pub fn table(&self) -> &Table {
    &self.table
  }

  /// Get LRU reference
  /// 获取 LRU 引用
  #[inline]
  pub fn lru(&self) -> &Lru {
    &self.lru
  }
}

impl Deref for Handle {
  type Target = Table;

  #[inline]
  fn deref(&self) -> &Self::Target {
    &self.table
  }
}

impl Drop for Handle {
  fn drop(&mut self) {
    if self.rm_on_drop.get() {
      let id = self.table.meta().id;
      let dir = Rc::clone(&self.dir);

      // Evict from LRU cache
      // 从 LRU 缓存移除
      self.lru.borrow_mut().evict(id);

      // Spawn background task to delete file
      // 启动后台任务删除文件
      compio::runtime::spawn(async move {
        let path = id_path(&dir, id);
        if let Err(e) = compio_fs::remove_file(&path).await {
          error!("rm sst {id}: {e}");
        }
      })
      .detach();
    }
  }
}


impl jdb_base::table::Meta for Handle {
  #[inline]
  fn id(&self) -> u64 {
    self.table.meta().id
  }

  #[inline]
  fn min_key(&self) -> &[u8] {
    self.table.min_key()
  }

  #[inline]
  fn max_key(&self) -> &[u8] {
    self.table.max_key()
  }

  #[inline]
  fn size(&self) -> u64 {
    self.table.size()
  }

  #[inline]
  fn count(&self) -> u64 {
    self.table.count()
  }

  #[inline]
  fn rm_count(&self) -> u64 {
    self.table.rm_count()
  }
}
