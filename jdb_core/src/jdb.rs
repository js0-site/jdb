//! Jdb implementation / Jdb 实现

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use jdb_db::{Commit, Db, DbConf, Retention};
use jdb_page::PageStore;
use jdb_trait::{DbId, Order};
use jdb_tree::BTree;
use jdb_vlog::VLog;

use crate::Result;

/// Meta file name / 元数据文件名
const META_FILE: &str = "meta.jdb";

/// Jdb - top level database / 顶层数据库
pub struct Jdb {
  dir: PathBuf,
  vlog: Rc<VLog>,
  dbs: BTreeMap<DbId, DbMeta>,
  next_id: DbId,
}

/// Database metadata / 数据库元数据
#[derive(Debug, Clone, Copy)]
struct DbMeta {
  root: u64,
  rev: u64,
  retention: Retention,
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
    let (dbs, next_id) = if meta_path.exists() {
      Self::load_meta(&meta_path)?
    } else {
      (BTreeMap::new(), 1)
    };

    Ok(Self {
      dir,
      vlog,
      dbs,
      next_id,
    })
  }

  fn load_meta(_path: &Path) -> Result<(BTreeMap<DbId, DbMeta>, DbId)> {
    // TODO: implement meta persistence / 实现元数据持久化
    Ok((BTreeMap::new(), 1))
  }

  fn save_meta(&self) -> Result<()> {
    // TODO: implement meta persistence / 实现元数据持久化
    Ok(())
  }

  /// Get or create database / 获取或创建数据库
  pub async fn db(&mut self, id: DbId, conf: &[DbConf]) -> Result<Db> {
    if let Some(meta) = self.dbs.get(&id).copied() {
      // Open existing / 打开已有
      let page_path = self.db_page_path(id);
      let store = PageStore::open(&page_path).await?;
      let tree = BTree::open(store, meta.root);
      let commit = Commit {
        rev: meta.rev,
        root: meta.root,
      };
      let mut db = Db::from_parts_with_retention(tree, Rc::clone(&self.vlog), commit, meta.retention);
      db.conf(conf); // apply new conf / 应用新配置
      Ok(db)
    } else {
      // Create new / 创建新的
      self.create_db(id, conf).await
    }
  }

  async fn create_db(&mut self, id: DbId, conf: &[DbConf]) -> Result<Db> {
    let page_path = self.db_page_path(id);
    let store = PageStore::open(&page_path).await?;
    let tree = BTree::new(store).await?;

    // Parse retention from conf / 从配置解析保留策略
    let mut retention = Retention::default();
    for c in conf {
      if let DbConf::Retention(r) = c {
        retention = *r;
      }
    }

    let meta = DbMeta {
      root: tree.root(),
      rev: 0,
      retention,
    };
    self.dbs.insert(id, meta);

    if id >= self.next_id {
      self.next_id = id + 1;
    }

    self.save_meta()?;

    let commit = Commit {
      rev: meta.rev,
      root: meta.root,
    };
    Ok(Db::from_parts_with_retention(tree, Rc::clone(&self.vlog), commit, retention))
  }

  fn db_page_path(&self, id: DbId) -> PathBuf {
    self.dir.join(format!("db_{id:08}.jdb"))
  }

  /// Fork database / Fork 数据库
  pub async fn fork(&mut self, id: DbId) -> Result<Option<Db>> {
    let meta = match self.dbs.get(&id) {
      Some(m) => m.clone(),
      None => return Ok(None),
    };

    // Allocate new id / 分配新 ID
    let new_id = self.next_id;
    self.next_id += 1;

    // Copy page file / 复制页文件
    let src_path = self.db_page_path(id);
    let dst_path = self.db_page_path(new_id);
    std::fs::copy(&src_path, &dst_path)?;

    // Create new meta / 创建新元数据
    let new_meta = DbMeta {
      root: meta.root,
      rev: meta.rev,
    };
    self.dbs.insert(new_id, new_meta.clone());
    self.save_meta()?;

    // Open new db / 打开新数据库
    let store = PageStore::open(&dst_path).await?;
    let tree = BTree::open(store, new_meta.root);
    let commit = Commit {
      rev: new_meta.rev,
      root: new_meta.root,
    };
    Ok(Some(Db::from_parts(tree, Rc::clone(&self.vlog), commit)))
  }

  /// Scan databases / 扫描数据库
  pub fn scan(&self, start: DbId, order: Order) -> Vec<DbId> {
    let ids: Vec<DbId> = match order {
      Order::Asc => self.dbs.keys().filter(|&&id| id >= start).copied().collect(),
      Order::Desc => self
        .dbs
        .keys()
        .rev()
        .filter(|&&id| id <= start)
        .copied()
        .collect(),
    };
    ids
  }

  /// Get next available id / 获取下一个可用 ID
  pub fn next_id(&self) -> DbId {
    self.next_id
  }

  /// Sync all / 同步全部
  pub async fn sync(&self) -> Result<()> {
    self.vlog.sync().await?;
    Ok(())
  }

  /// Commit db changes / 提交数据库变更
  pub fn commit_db(&mut self, id: DbId, db: &Db) {
    self.dbs.insert(
      id,
      DbMeta {
        root: db.root(),
        rev: db.rev(),
      },
    );
    let _ = self.save_meta();
  }
}
