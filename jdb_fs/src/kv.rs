//! Persistent key-value map with CRC
//! 带 CRC 的持久化键值映射
//!
//! Disk format: magic(1) + kind(1) + data + crc32(4)
//! 磁盘格式：magic(1) + kind(1) + data + crc32(4)

use std::{collections::HashMap, hash::Hash, io, path::Path};

use compio::{
  buf::{IntoInner, IoBuf},
  io::{AsyncReadAtExt, AsyncWriteExt},
};
use log::warn;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::atom_write::AtomWrite;

/// Magic byte
/// 魔数
pub const MAGIC: u8 = 0x42;

/// Header size (magic + kind)
/// 头部大小
pub const HEAD_SIZE: usize = 2;

/// CRC size
/// CRC 大小
pub const CRC_SIZE: usize = 4;

/// Serialize data to disk format: magic(1) + kind(1) + data + crc32(4)
/// 序列化数据为磁盘格式
#[inline]
pub fn to_disk<const N: usize, T: IntoBytes + Immutable>(kind: u8, data: &T) -> [u8; N] {
  let mut buf = [0u8; N];
  buf[0] = MAGIC;
  buf[1] = kind;
  let bytes = data.as_bytes();
  buf[HEAD_SIZE..HEAD_SIZE + bytes.len()].copy_from_slice(bytes);
  // CRC covers kind + data (skip magic)
  // CRC 覆盖 kind + data（跳过 magic）
  let crc = crc32fast::hash(&buf[1..N - CRC_SIZE]);
  buf[N - CRC_SIZE..].copy_from_slice(&crc.to_le_bytes());
  buf
}

/// Serialize data to disk format (Vec version)
/// 序列化数据为磁盘格式（Vec 版本）
#[inline]
pub fn to_disk_vec<T: IntoBytes + Immutable>(kind: u8, data: &T) -> Vec<u8> {
  let size = HEAD_SIZE + size_of::<T>() + CRC_SIZE;
  let mut buf = vec![0u8; size];
  buf[0] = MAGIC;
  buf[1] = kind;
  let bytes = data.as_bytes();
  buf[HEAD_SIZE..HEAD_SIZE + bytes.len()].copy_from_slice(bytes);
  let crc = crc32fast::hash(&buf[1..size - CRC_SIZE]);
  buf[size - CRC_SIZE..].copy_from_slice(&crc.to_le_bytes());
  buf
}

/// Verify and parse disk format, returns None if invalid
/// 校验并解析磁盘格式，无效返回 None
#[inline]
pub fn from_disk<T: FromBytes + KnownLayout + Immutable>(kind: u8, buf: &[u8]) -> Option<T> {
  let size = HEAD_SIZE + size_of::<T>() + CRC_SIZE;
  if buf.len() < size {
    return None;
  }
  if buf[0] != MAGIC || buf[1] != kind {
    return None;
  }
  // Verify CRC
  // 校验 CRC
  let crc_stored = u32::from_le_bytes(buf[size - CRC_SIZE..size].try_into().ok()?);
  let crc_calc = crc32fast::hash(&buf[1..size - CRC_SIZE]);
  if crc_stored != crc_calc {
    return None;
  }
  T::read_from_bytes(&buf[HEAD_SIZE..HEAD_SIZE + size_of::<T>()]).ok()
}

/// Disk entry trait for key-value persistence
/// 磁盘条目 trait，用于键值持久化
pub trait Entry: FromBytes + IntoBytes + Immutable + KnownLayout + Copy {
  type Key: Eq + Hash + Copy;
  type Val: Copy;

  /// Entry kind byte
  /// 条目类型字节
  const KIND: u8;

  fn new(key: Self::Key, val: Self::Val) -> Self;
  fn key(&self) -> Self::Key;
  fn val(&self) -> Self::Val;

  /// Check if entry should be removed (e.g. val == 0)
  /// 检查条目是否应被移除（如 val == 0）
  fn is_remove(&self) -> bool {
    false
  }

  /// Total size on disk
  /// 磁盘总大小
  #[inline]
  fn disk_size() -> usize {
    HEAD_SIZE + size_of::<Self>() + CRC_SIZE
  }
}

/// Load key-value map from file
/// 从文件加载键值映射
pub async fn load<E: Entry>(path: &Path) -> io::Result<HashMap<E::Key, E::Val>> {
  let file = match compio::fs::OpenOptions::new().read(true).open(path).await {
    Ok(f) => f,
    Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(HashMap::new()),
    Err(e) => return Err(e),
  };

  let meta = file.metadata().await?;
  let len = meta.len();
  if len == 0 {
    return Ok(HashMap::new());
  }

  // Optimization: Read in chunks to ensure O(1) memory usage instead of O(N)
  // 优化：分块读取以确保 O(1) 的内存使用，而不是 O(N)
  let mut map = HashMap::new();
  let entry_size = E::disk_size();
  // 64KB chunk size
  let chunk_capacity = (65536 / entry_size).max(1) * entry_size;
  let mut buf = vec![0u8; chunk_capacity];
  let mut pos = 0;

  while pos < len {
    let read_len = ((len - pos) as usize).min(chunk_capacity);
    let slice = buf.slice(0..read_len);
    let res = file.read_exact_at(slice, pos).await;
    res.0?;
    buf = res.1.into_inner();

    for chunk in buf[..read_len].chunks_exact(entry_size) {
      if let Some(entry) = from_disk::<E>(E::KIND, chunk) {
        if entry.is_remove() {
          map.remove(&entry.key());
        } else {
          map.insert(entry.key(), entry.val());
        }
      } else {
        warn!("kv load: CRC failed at pos {pos}, path={path:?}");
      }
    }
    pos += read_len as u64;
  }

  Ok(map)
}

/// Rewrite file from byte chunks (atomic)
/// 从字节块重写文件（原子）
pub async fn rewrite_iter<I, B>(path: &Path, iter: I) -> io::Result<()>
where
  I: IntoIterator<Item = B>,
  B: AsRef<[u8]>,
{
  let mut file = AtomWrite::new(path.to_path_buf()).await?;
  let mut has_content = false;

  for chunk in iter {
    let data = chunk.as_ref();
    if !data.is_empty() {
      file.write_all(data.to_vec()).await.0?;
      has_content = true;
    }
  }

  if !has_content {
    let _ = compio::fs::remove_file(path).await;
    return Ok(());
  }

  file.rename().await?;
  Ok(())
}

/// Rewrite key-value map to file (atomic)
/// 重写键值映射到文件（原子）
pub async fn rewrite<E: Entry>(path: &Path, map: &HashMap<E::Key, E::Val>) -> io::Result<()>
where
  E::Key: Ord,
{
  if map.is_empty() {
    let _ = compio::fs::remove_file(path).await;
    return Ok(());
  }

  // Sort for deterministic output
  // 排序以保证确定性输出
  let mut entries: Vec<_> = map.iter().collect();
  entries.sort_unstable_by_key(|(k, _)| *k);

  rewrite_iter(
    path,
    entries
      .into_iter()
      .map(|(&key, &val)| to_disk_vec(E::KIND, &E::new(key, val))),
  )
  .await
}
