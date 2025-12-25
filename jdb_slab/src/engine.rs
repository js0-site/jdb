//! Top-level slab engine / 顶层 Slab 引擎
//!
//! Manages multiple SlabClasses with auto-routing and blob storage.
//! 管理多个 SlabClass，自动路由，支持大文件存储。

use std::path::{Path, PathBuf};

use crate::{
  BlobStore, Compress, Error, Header, Result, SlabClass, SlabReader, SlabWriter, SlotId, blob_id,
  decode_slab, encode_slab, is_blob, make_blob,
};

/// Slab configuration / Slab 配置
#[derive(Debug, Clone)]
pub struct SlabConfig {
  /// Size classes [16KB, 64KB, ...] / 大小层级
  pub class_sizes: Vec<usize>,
  /// Data directory / 数据目录
  pub base_path: PathBuf,
}

impl SlabConfig {
  /// Create with default sizes / 使用默认大小创建
  pub fn new(base_path: impl Into<PathBuf>) -> Self {
    Self {
      class_sizes: vec![
        16384,   // 16KB (min for Direct I/O alignment)
        65536,   // 64KB
        262144,  // 256KB
        1048576, // 1MB
        4194304, // 4MB
      ],
      base_path: base_path.into(),
    }
  }

  /// Add custom size class / 添加自定义大小层级
  pub fn with_class(mut self, size: usize) -> Self {
    self.class_sizes.push(size);
    self.class_sizes.sort_unstable();
    self
  }

  /// Get max class size / 获取最大层级大小
  #[inline]
  pub fn max_class_size(&self) -> usize {
    self.class_sizes.last().copied().unwrap_or(0)
  }
}

/// Top-level slab engine / 顶层 Slab 引擎
pub struct Engine {
  /// SlabClasses by size / 按大小排序的 SlabClass
  pub(crate) classes: Vec<SlabClass>,
  /// Blob storage for large files / 大文件存储
  blob: BlobStore,
  /// Configuration / 配置
  config: SlabConfig,
}

impl Engine {
  /// Create engine with config / 使用配置创建引擎
  pub async fn new(config: SlabConfig) -> Result<Self> {
    // Ensure base_path exists / 确保目录存在
    std::fs::create_dir_all(&config.base_path).map_err(Error::Io)?;

    let mut classes = Vec::with_capacity(config.class_sizes.len());
    for &size in &config.class_sizes {
      let slab = SlabClass::open(&config.base_path, size).await?;
      classes.push(slab);
    }

    let blob = BlobStore::new(&config.base_path)?;

    Ok(Self {
      classes,
      blob,
      config,
    })
  }

  /// Find smallest class that fits data / 查找能容纳数据的最小层级
  fn select_class(&self, data_len: usize) -> Option<usize> {
    let needed = data_len + Header::SIZE;
    self
      .config
      .class_sizes
      .iter()
      .position(|&size| size >= needed)
  }

  /// Write data, auto-select class or blob / 写入数据，自动选择层级或 blob
  pub async fn put(&mut self, data: &[u8]) -> Result<SlotId> {
    self.put_with(data, Compress::None).await
  }

  /// Write data with compression / 写入数据（带压缩）
  pub async fn put_with(&mut self, data: &[u8], compress: Compress) -> Result<SlotId> {
    // Try slab first / 先尝试 slab
    if let Some(class_idx) = self.select_class(data.len()) {
      let inner_id = self.classes[class_idx].put_with(data, compress).await?;
      return Ok(encode_slab(class_idx, inner_id));
    }

    // Fall back to blob / 回退到 blob
    let id = self.blob.put(data, compress).await?;
    Ok(make_blob(id))
  }

  /// Read data by slot id / 按槽位 ID 读取
  pub async fn get(&mut self, slot_id: SlotId) -> Result<Vec<u8>> {
    if is_blob(slot_id) {
      return self.blob.get(blob_id(slot_id)).await;
    }

    let (class_idx, inner_id) = decode_slab(slot_id);
    let slab = self
      .classes
      .get_mut(class_idx)
      .ok_or(Error::InvalidClass(class_idx))?;
    slab.get(inner_id).await
  }

  /// Delete slot / 删除槽位
  pub fn del(&mut self, slot_id: SlotId) {
    if is_blob(slot_id) {
      self.blob.del(blob_id(slot_id));
      return;
    }

    let (class_idx, inner_id) = decode_slab(slot_id);
    if let Some(slab) = self.classes.get_mut(class_idx) {
      slab.del(inner_id);
    }
  }

  /// Flush all classes and blob / 刷新所有层级和 blob
  pub async fn flush(&mut self) -> Result<()> {
    for slab in &mut self.classes {
      slab.flush().await?;
    }
    self.blob.flush()?;
    Ok(())
  }

  /// Get class count / 获取层级数量
  #[inline]
  pub fn class_count(&self) -> usize {
    self.classes.len()
  }

  /// Get class by index / 按索引获取层级
  #[inline]
  pub fn class(&self, idx: usize) -> Option<&SlabClass> {
    self.classes.get(idx)
  }

  /// Get mutable class by index / 按索引获取可变层级
  #[inline]
  pub fn class_mut(&mut self, idx: usize) -> Option<&mut SlabClass> {
    self.classes.get_mut(idx)
  }

  /// Get config / 获取配置
  #[inline]
  pub fn config(&self) -> &SlabConfig {
    &self.config
  }

  /// Get base path / 获取基础路径
  #[inline]
  pub fn base_path(&self) -> &Path {
    &self.config.base_path
  }

  /// Get blob store ref / 获取 blob 存储引用
  #[inline]
  pub fn blob(&self) -> &BlobStore {
    &self.blob
  }

  /// Get mutable blob store / 获取可变 blob 存储
  #[inline]
  pub fn blob_mut(&mut self) -> &mut BlobStore {
    &mut self.blob
  }
}

impl Engine {
  /// Get streaming reader (slab only) / 获取流式读取器（仅 slab）
  pub fn reader(&self, slot_id: SlotId, len: u64) -> Result<SlabReader<'_>> {
    if is_blob(slot_id) {
      return Err(Error::Serialize(
        "blob does not support streaming read".into(),
      ));
    }

    let (class_idx, inner_id) = decode_slab(slot_id);
    let slab = self
      .classes
      .get(class_idx)
      .ok_or(Error::InvalidClass(class_idx))?;
    Ok(slab.reader(inner_id, len))
  }

  /// Get streaming writer for specific class / 获取指定层级的流式写入器
  pub async fn writer(&mut self, class_idx: usize) -> Result<SlabWriter<'_>> {
    let slab = self
      .classes
      .get_mut(class_idx)
      .ok_or(Error::InvalidClass(class_idx))?;
    slab.writer().await
  }

  /// Recovery from metadata / 从元数据恢复
  pub async fn recovery(&mut self) -> Result<()> {
    for slab in &mut self.classes {
      slab.recovery().await?;
    }
    self.blob.recovery()?;
    Ok(())
  }
}
