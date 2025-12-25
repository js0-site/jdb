//! Top-level slab engine / 顶层 Slab 引擎
//!
//! Manages multiple SlabClasses with auto-routing.
//! 管理多个 SlabClass，自动路由。

use std::path::{Path, PathBuf};

use crate::{Compress, Error, Header, Result, SlabClass, SlotId};

/// Slab configuration / Slab 配置
#[derive(Debug, Clone)]
pub struct SlabConfig {
  /// Size classes [4KB, 16KB, 64KB, ...] / 大小层级
  pub class_sizes: Vec<usize>,
  /// Data directory / 数据目录
  pub base_path: PathBuf,
}

impl SlabConfig {
  /// Create with default sizes / 使用默认大小创建
  pub fn new(base_path: impl Into<PathBuf>) -> Self {
    Self {
      class_sizes: vec![
        16384,   // 16KB
        65536,   // 64KB
        262144,  // 256KB
        1048576, // 1MB
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
}

/// Top-level slab engine / 顶层 Slab 引擎
pub struct Engine {
  /// SlabClasses by size / 按大小排序的 SlabClass
  pub(crate) classes: Vec<SlabClass>,
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

    Ok(Self { classes, config })
  }

  /// Find smallest class that fits data / 查找能容纳数据的最小层级
  pub fn select_class(&self, data_len: usize) -> Option<usize> {
    let needed = data_len + Header::SIZE;
    self
      .config
      .class_sizes
      .iter()
      .position(|&size| size >= needed)
  }

  /// Write data, auto-select class / 写入数据，自动选择层级
  pub async fn put(&mut self, data: &[u8]) -> Result<(usize, SlotId)> {
    self.put_with(data, Compress::None).await
  }

  /// Write data with compression / 写入数据（带压缩）
  pub async fn put_with(&mut self, data: &[u8], compress: Compress) -> Result<(usize, SlotId)> {
    let class_idx = self
      .select_class(data.len())
      .ok_or(Error::NoFittingClass(data.len()))?;
    let slot_id = self.classes[class_idx].put_with(data, compress).await?;
    Ok((class_idx, slot_id))
  }

  /// Read data from specific class / 从指定层级读取
  pub async fn get(&mut self, class_idx: usize, slot_id: SlotId) -> Result<Vec<u8>> {
    let slab = self
      .classes
      .get_mut(class_idx)
      .ok_or(Error::InvalidClass(class_idx))?;
    slab.get(slot_id).await
  }

  /// Delete slot / 删除槽位
  pub fn del(&mut self, class_idx: usize, slot_id: SlotId) {
    if let Some(slab) = self.classes.get_mut(class_idx) {
      slab.del(slot_id);
    }
  }

  /// Flush all classes / 刷新所有层级
  pub async fn flush(&mut self) -> Result<()> {
    for slab in &mut self.classes {
      slab.flush().await?;
    }
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
}

use crate::{SlabReader, SlabWriter};

impl Engine {
  /// Get streaming reader / 获取流式读取器
  pub fn reader(&self, class_idx: usize, slot_id: SlotId, len: u64) -> Result<SlabReader<'_>> {
    let slab = self
      .classes
      .get(class_idx)
      .ok_or(Error::InvalidClass(class_idx))?;
    Ok(slab.reader(slot_id, len))
  }

  /// Get streaming writer for specific class / 获取指定层级的流式写入器
  pub async fn writer(&mut self, class_idx: usize) -> Result<SlabWriter<'_>> {
    let slab = self
      .classes
      .get_mut(class_idx)
      .ok_or(Error::InvalidClass(class_idx))?;
    slab.writer().await
  }
}
