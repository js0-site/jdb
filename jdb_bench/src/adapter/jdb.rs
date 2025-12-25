// JdbSlab adapter / JdbSlab 适配器

use std::{
  collections::HashMap,
  path::{Path, PathBuf},
};

use jdb_slab::{Engine, SlabConfig, SlotId};

use crate::{BenchEngine, Result};

const ENGINE_NAME: &str = "jdb_slab";

/// JdbSlab adapter / JdbSlab 适配器
pub struct JdbSlabAdapter {
  engine: Engine,
  /// key -> (class_idx, slot_id) mapping / 键到槽位的映射
  index: HashMap<Vec<u8>, (usize, SlotId)>,
  /// Data directory / 数据目录
  path: PathBuf,
}

impl JdbSlabAdapter {
  /// Create new adapter / 创建新适配器
  pub async fn new(path: &Path) -> Result<Self> {
    let config = SlabConfig::new(path);
    let engine = Engine::new(config).await?;
    Ok(Self {
      engine,
      index: HashMap::new(),
      path: path.to_path_buf(),
    })
  }

  /// Create with custom config / 使用自定义配置创建
  pub async fn with_config(config: SlabConfig) -> Result<Self> {
    let path = config.base_path.clone();
    let engine = Engine::new(config).await?;
    Ok(Self {
      engine,
      index: HashMap::new(),
      path,
    })
  }
}

impl BenchEngine for JdbSlabAdapter {
  fn name(&self) -> &str {
    ENGINE_NAME
  }

  fn data_path(&self) -> &Path {
    &self.path
  }

  async fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
    let (class_idx, slot_id) = self.engine.put(val).await?;
    self.index.insert(key.to_vec(), (class_idx, slot_id));
    Ok(())
  }

  async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
    let Some(&(class_idx, slot_id)) = self.index.get(key) else {
      return Ok(None);
    };
    let data = self.engine.get(class_idx, slot_id).await?;
    Ok(Some(data))
  }

  async fn del(&mut self, key: &[u8]) -> Result<()> {
    if let Some((class_idx, slot_id)) = self.index.remove(key) {
      self.engine.del(class_idx, slot_id);
    }
    Ok(())
  }

  async fn sync(&self) -> Result<()> {
    self.engine.sync_all().await?;
    Ok(())
  }
}
