//! Persistent key-value map with CRC
//! 带 CRC 的持久化键值映射
//!
//! Disk format: magic(1) + kind(1) + data + crc32(4)
//! 磁盘格式：magic(1) + kind(1) + data + crc32(4)

use std::{collections::HashMap, hash::Hash, io, io::Cursor, path::Path};

use compio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use log::warn;
use zbin::Bin;
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

/// Serialize data to disk format into buffer (reusable)
/// 序列化数据为磁盘格式到缓冲区（可复用）
#[inline]
pub fn to_disk_buf<T: IntoBytes + Immutable>(kind: u8, data: &T, buf: &mut Vec<u8>) {
  let data_bytes = data.as_bytes();
  let start = buf.len();

  buf.push(MAGIC);
  buf.push(kind);
  buf.extend_from_slice(data_bytes);
  // CRC covers kind + data (skip magic)
  // CRC 覆盖 kind + data（跳过 magic）
  let crc = crc32fast::hash(&buf[start + 1..]);
  buf.extend_from_slice(&crc.to_le_bytes());
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

  let mut map = HashMap::new();
  let entry_size = E::disk_size();
  let mut reader = BufReader::new(Cursor::new(file));
  let mut buf = vec![0u8; entry_size];
  let mut pos = 0u64;

  loop {
    let res = reader.read_exact(buf).await;
    if let Err(ref e) = res.0 {
      if e.kind() == io::ErrorKind::UnexpectedEof {
        break;
      }
      return Err(res.0.unwrap_err());
    }
    buf = res.1;

    if let Some(entry) = from_disk::<E>(E::KIND, &buf) {
      if entry.is_remove() {
        map.remove(&entry.key());
      } else {
        map.insert(entry.key(), entry.val());
      }
    } else {
      warn!("kv load: CRC failed at pos {pos}, path={path:?}");
    }
    pos += entry_size as u64;
  }

  Ok(map)
}

/// Rewrite file from byte chunks (atomic)
/// 从字节块重写文件（原子）
pub async fn rewrite_iter<I, B>(path: &Path, iter: I) -> io::Result<()>
where
  I: IntoIterator<Item = B>,
  B: for<'a> Bin<'a>,
{
  let mut file = AtomWrite::new(path.to_path_buf()).await?;
  let mut has_content = false;

  for chunk in iter {
    if !chunk.is_empty() {
      file.write_all(chunk.io()).await.0?;
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

/// Rewrite key-value map to file (atomic, O(1) extra alloc)
/// 重写键值映射到文件（原子，O(1) 额外分配）
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

  let mut file = AtomWrite::new(path.to_path_buf()).await?;

  // Reuse buffer to avoid heap alloc per entry
  // 复用缓冲区避免每条目堆分配
  let mut buf = vec![0u8; E::disk_size()];
  for (&key, &val) in entries {
    buf.clear();
    to_disk_buf(E::KIND, &E::new(key, val), &mut buf);
    // Take ownership, write, then restore
    // 取得所有权，写入，然后恢复
    let b = std::mem::take(&mut buf);
    let res = file.write_all(b).await;
    res.0?;
    buf = res.1;
  }

  file.rename().await
}
