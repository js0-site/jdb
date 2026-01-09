//! SSTable write functions
//! SSTable 写入函数

use std::path::Path;

use compio::{
  buf::IoBuf,
  fs::{self, File},
  io::AsyncWriteAtExt,
};
use defer_lite::defer;
use futures::StreamExt;
use jdb_base::{Kv, Pos};
use jdb_fs::fs_id::id_path;
use jdb_sst::{Error, Meta, Result, TMP_DIR};
use zbin::Bin;

use crate::{Conf, id, state::State};

/// Create SSTable from iterator
/// 从迭代器创建 SSTable
pub async fn write<'a, I>(dir: &Path, level: u8, iter: I, conf_li: &[Conf]) -> Result<Meta>
where
  I: Iterator<Item = (&'a Box<[u8]>, &'a Pos)>,
{
  write_id(dir, level, iter, conf_li, id::new(dir)).await
}

/// Create SSTable from iterator with specified id
/// 从迭代器创建 SSTable（指定 id）
pub async fn write_id<'a, I>(
  dir: &Path,
  level: u8,
  iter: I,
  conf_li: &[Conf],
  id: u64,
) -> Result<Meta>
where
  I: Iterator<Item = (&'a Box<[u8]>, &'a Pos)>,
{
  let mut w = State::new(level, conf_li, id);
  let tmp_dir = dir.join(TMP_DIR);
  fs::create_dir_all(&tmp_dir).await?;

  let temp_path = id_path(&tmp_dir, id);
  let mut file = File::create(&temp_path).await?;
  defer! { let _ = std::fs::remove_file(&temp_path); }

  let mut last_key: Box<[u8]> = Box::default();
  for (key, pos) in iter {
    if key.len() > u16::MAX as usize {
      return Err(Error::KeyTooLarge(key.len()));
    }
    w.add(key, pos, &mut file).await?;
    last_key = key.clone();
  }

  w.finish(&mut file, last_key, dir, id).await
}

/// Create SSTable from async stream
/// 从异步流创建 SSTable
pub async fn write_stream<S>(dir: &Path, level: u8, mut stream: S, conf_li: &[Conf]) -> Result<Meta>
where
  S: futures::Stream<Item = Kv> + Unpin,
{
  let id = id::new(dir);
  let mut w = State::new(level, conf_li, id);
  let tmp_dir = dir.join(TMP_DIR);
  fs::create_dir_all(&tmp_dir).await?;

  let temp_path = id_path(&tmp_dir, id);
  let mut file = File::create(&temp_path).await?;
  defer! { let _ = std::fs::remove_file(&temp_path); }

  let mut last_key: Box<[u8]> = Box::default();
  while let Some((key, pos)) = stream.next().await {
    if key.len() > u16::MAX as usize {
      return Err(Error::KeyTooLarge(key.len()));
    }
    w.add(&key, &pos, &mut file).await?;
    last_key = key;
  }

  w.finish(&mut file, last_key, dir, id).await
}

/// Write data to file at offset (zero-copy for owned types)
/// 写入数据到文件指定偏移（拥有所有权类型零拷贝）
#[inline]
pub async fn write_at<'a>(file: &mut File, data: impl Bin<'a>, offset: u64) -> Result<u64> {
  let len = data.len() as u64;
  if len == 0 {
    return Ok(0);
  }
  let buf = data.io();
  let slice = buf.slice(..);
  let res = file.write_all_at(slice, offset).await;
  res.0?;
  Ok(len)
}
