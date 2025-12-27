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
  /// key -> slot_id mapping / 键到槽位的映射
  index: HashMap<Vec<u8>, SlotId>,
  /// Data directory / 数据目录
  path: PathBuf,
}

impl JdbSlabAdapter {
  /// Create new adapter / 创建新适配器
  pub async fn new(path: &Path) -> Result<Self> {
    let config = SlabConfig::new(path);
    Self::with_config(config).await
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
    let slot_id = self.engine.put(val).await?;
    self.index.insert(key.to_vec(), slot_id);
    Ok(())
  }

  async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
    let Some(&slot_id) = self.index.get(key) else {
      return Ok(None);
    };
    let data = self.engine.get(slot_id).await?;
    Ok(Some(data))
  }

  async fn del(&mut self, key: &[u8]) -> Result<()> {
    if let Some(slot_id) = self.index.remove(key) {
      self.engine.del(slot_id);
    }
    Ok(())
  }

  async fn sync(&self) -> Result<()> {
    Ok(())
  }
}
