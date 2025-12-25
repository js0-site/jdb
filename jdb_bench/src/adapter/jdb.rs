// JdbSlab adapter / JdbSlab 适配器

use std::{
  collections::HashMap,
  future::Future,
  path::{Path, PathBuf},
  pin::Pin,
  rc::Rc,
};

use jdb_flush::{AsyncFn, Flush, SecItem};
use jdb_slab::{Engine, SlabConfig, SlotId};

use crate::{BenchEngine, Result};

const ENGINE_NAME: &str = "jdb_slab";

/// Redis-style flush thresholds / Redis 风格刷新阈值
/// 900s/1, 300s/10, 60s/10000
fn redis_flush() -> Vec<SecItem> {
  vec![SecItem(900, 1), SecItem(300, 10), SecItem(60, 10000)]
}

/// JdbSlab adapter / JdbSlab 适配器
pub struct JdbSlabAdapter {
  engine: Rc<std::cell::UnsafeCell<Engine>>,
  /// key -> (class_idx, slot_id) mapping / 键到槽位的映射
  index: HashMap<Vec<u8>, (usize, SlotId)>,
  /// Data directory / 数据目录
  path: PathBuf,
  /// Flush policy / 刷新策略
  flush: Rc<Flush>,
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
    let engine = Rc::new(std::cell::UnsafeCell::new(engine));

    // Setup flush with Redis-style thresholds / 使用 Redis 风格阈值设置刷新
    let flush = Flush::new(redis_flush());
    let engine_ref = engine.clone();
    let hook: AsyncFn = Rc::new(move || {
      let e = engine_ref.clone();
      Box::pin(async move {
        // Safe: single-threaded / 安全：单线程
        let engine = unsafe { &mut *e.get() };
        let _ = engine.flush().await;
      }) as Pin<Box<dyn Future<Output = ()>>>
    });
    flush.hook(hook);

    Ok(Self {
      engine,
      index: HashMap::new(),
      path,
      flush,
    })
  }

  /// Get engine mut / 获取可变引擎引用
  fn engine_mut(&mut self) -> &mut Engine {
    // Safe: single-threaded / 安全：单线程
    unsafe { &mut *self.engine.get() }
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
    let (class_idx, slot_id) = self.engine_mut().put(val).await?;
    self.index.insert(key.to_vec(), (class_idx, slot_id));
    self.flush.incr();
    Ok(())
  }

  async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
    let Some(&(class_idx, slot_id)) = self.index.get(key) else {
      return Ok(None);
    };
    let data = self.engine_mut().get(class_idx, slot_id).await?;
    Ok(Some(data))
  }

  async fn del(&mut self, key: &[u8]) -> Result<()> {
    if let Some((class_idx, slot_id)) = self.index.remove(key) {
      self.engine_mut().del(class_idx, slot_id);
    }
    Ok(())
  }

  async fn sync(&self) -> Result<()> {
    // Safe: single-threaded / 安全：单线程
    let engine = unsafe { &mut *self.engine.get() };
    engine.flush().await?;
    Ok(())
  }
}
