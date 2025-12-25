//! Jdb implementation / Jdb 实现

use std::{
  collections::BTreeMap,
  path::{Path, PathBuf},
  rc::Rc,
};

use jdb_page::PageStore;
use jdb_table::{Commit, Conf, Keep, Table};
use jdb_trait::{Order, TableId};
use jdb_tree::BTree;
use jdb_vlog::VLog;

use crate::Result;

/// Meta file name / 元数据文件名
const META_FILE: &str = "meta.jdb";

/// Jdb - top level database / 顶层数据库
pub struct Jdb {
  dir: PathBuf,
  vlog: Rc<VLog>,
  tables: BTreeMap<TableId, TableMeta>,
  next_id: TableId,
}

/// Table metadata / 表元数据
#[derive(Debug, Clone, Copy)]
struct TableMeta {
  root: u64,
  rev: u64,
  keep: Keep,
}

impl Jdb {
  /// Open or create Jdb / 打开或创建 Jdb
  pub async fn open(dir: impl AsRef<Path>) -> Result<Self> {
    let dir = dir.as_ref().to_path_buf();

    // Create dir / 创建目录
    if !dir.exists() {
      std::fs::create_dir_all(&dir)?;
    }

    // Open shared vlog / 打开共享值日志
    let vlog_dir = dir.join("vlog");
    let vlog = Rc::new(VLog::open(&vlog_dir).await?);

    // Load meta / 加载元数据
    let meta_path = dir.join(META_FILE);
    let (tables, next_id) = if meta_path.exists() {
      Self::load_meta(&meta_path)?
    } else {
      (BTreeMap::new(), 1)
    };

    Ok(Self {
      dir,
      vlog,
      tables,
      next_id,
    })
  }

  fn load_meta(_path: &Path) -> Result<(BTreeMap<TableId, TableMeta>, TableId)> {
    // TODO: implement meta persistence / 实现元数据持久化
    Ok((BTreeMap::new(), 1))
  }

  fn save_meta(&self) -> Result<()> {
    // TODO: implement meta persistence / 实现元数据持久化
    Ok(())
  }

  /// Get or create table / 获取或创建表
  pub async fn table(&mut self, id: TableId, conf: &[Conf]) -> Result<Table> {
    if let Some(meta) = self.tables.get(&id).copied() {
      // Open existing / 打开已有
      let page_path = self.table_page_path(id);
      let store = PageStore::open(&page_path).await?;
      let tree = BTree::open(store, meta.root);
      let commit = Commit {
        rev: meta.rev,
        root: meta.root,
      };
      let mut tbl = Table::with_keep(tree, Rc::clone(&self.vlog), commit, meta.keep);
      tbl.conf(conf);
      Ok(tbl)
    } else {
      self.create_table(id, conf).await
    }
  }

  async fn create_table(&mut self, id: TableId, conf: &[Conf]) -> Result<Table> {
    let page_path = self.table_page_path(id);
    let store = PageStore::open(&page_path).await?;
    let tree = BTree::new(store).await?;

    // Parse keep from conf / 从配置解析保留策略
    let mut keep = Keep::default();
    for c in conf {
      let Conf::Keep(k) = c;
      keep = *k;
    }

    let meta = TableMeta {
      root: tree.root(),
      rev: 0,
      keep,
    };
    self.tables.insert(id, meta);

    if id >= self.next_id {
      self.next_id = id + 1;
    }

    self.save_meta()?;

    let commit = Commit {
      rev: meta.rev,
      root: meta.root,
    };
    Ok(Table::with_keep(tree, Rc::clone(&self.vlog), commit, keep))
  }

  fn table_page_path(&self, id: TableId) -> PathBuf {
    self.dir.join(format!("tbl_{id:08}.jdb"))
  }

  /// Fork table / Fork 表
  pub async fn fork(&mut self, id: TableId) -> Result<Option<Table>> {
    let meta = match self.tables.get(&id).copied() {
      Some(m) => m,
      None => return Ok(None),
    };

    // Allocate new id / 分配新 ID
    let new_id = self.next_id;
    self.next_id += 1;

    // Copy page file / 复制页文件
    let src_path = self.table_page_path(id);
    let dst_path = self.table_page_path(new_id);
    std::fs::copy(&src_path, &dst_path)?;

    // Create new meta / 创建新元数据
    let new_meta = TableMeta {
      root: meta.root,
      rev: meta.rev,
      keep: meta.keep,
    };
    self.tables.insert(new_id, new_meta);
    self.save_meta()?;

    // Open new table / 打开新表
    let store = PageStore::open(&dst_path).await?;
    let tree = BTree::open(store, new_meta.root);
    let commit = Commit {
      rev: new_meta.rev,
      root: new_meta.root,
    };
    Ok(Some(Table::with_keep(
      tree,
      Rc::clone(&self.vlog),
      commit,
      new_meta.keep,
    )))
  }

  /// Scan tables / 扫描表
  pub fn scan(&self, start: TableId, order: Order) -> Vec<TableId> {
    match order {
      Order::Asc => self
        .tables
        .keys()
        .filter(|&&id| id >= start)
        .copied()
        .collect(),
      Order::Desc => self
        .tables
        .keys()
        .rev()
        .filter(|&&id| id <= start)
        .copied()
        .collect(),
    }
  }

  /// Get next available id / 获取下一个可用 ID
  pub fn next_id(&self) -> TableId {
    self.next_id
  }

  /// Sync all / 同步全部
  pub async fn sync(&self) -> Result<()> {
    self.vlog.sync().await?;
    Ok(())
  }

  /// Commit table changes / 提交表变更
  pub fn commit_table(&mut self, id: TableId, tbl: &Table) {
    self.tables.insert(
      id,
      TableMeta {
        root: tbl.root(),
        rev: tbl.rev(),
        keep: tbl.keep(),
      },
    );
    let _ = self.save_meta();
  }
}
