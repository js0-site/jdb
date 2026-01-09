//! SSTable write functions
//! SSTable 写入函数

use std::path::Path;

use compio::io::{AsyncWrite, AsyncWriteExt};
use futures::StreamExt;
use jdb_base::{Kv, Pos, sst::Meta};
use jdb_fs::{AtomWrite, fs_id::id_path};
use jdb_sst::{Conf, Error, Result};
use zbin::Bin;

use crate::{id, state::State};

/// Create SSTable from iterator
/// 从迭代器创建 SSTable
pub async fn write<'a, I>(dir: &Path, level: u8, iter: I, conf_li: &[Conf]) -> Result<Meta>
where
  I: Iterator<Item = (&'a Box<[u8]>, &'a Pos)>,
{
  write_id(dir, level, iter, conf_li, id::new(dir).await).await
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
  let dst = id_path(dir, id);
  let mut atom = AtomWrite::new(&dst).await?;

  let mut last_key: Box<[u8]> = Box::default();
  for (key, pos) in iter {
    if key.len() > u16::MAX as usize {
      return Err(Error::KeyTooLarge(key.len()));
    }
    w.add(key, pos, &mut *atom).await?;
    last_key = key.clone();
  }

  w.finish(atom, last_key).await
}

/// Create SSTable from async stream
/// 从异步流创建 SSTable
pub async fn write_stream<S>(dir: &Path, level: u8, mut stream: S, conf_li: &[Conf]) -> Result<Meta>
where
  S: futures::Stream<Item = Kv> + Unpin,
{
  let id = id::new(dir).await;
  let mut w = State::new(level, conf_li, id);
  let dst = id_path(dir, id);
  let mut atom = AtomWrite::new(&dst).await?;

  let mut last_key: Box<[u8]> = Box::default();
  while let Some((key, pos)) = stream.next().await {
    if key.len() > u16::MAX as usize {
      return Err(Error::KeyTooLarge(key.len()));
    }
    w.add(&key, &pos, &mut *atom).await?;
    last_key = key;
  }

  w.finish(atom, last_key).await
}

/// Sequential write data to writer
/// 顺序写入数据
#[inline]
pub async fn push<'a>(w: &mut (impl AsyncWrite + Unpin), data: impl Bin<'a>) -> Result<u64> {
  let len = data.len() as u64;
  if len == 0 {
    return Ok(0);
  }
  let buf = data.io();
  w.write_all(buf).await.0?;
  Ok(len)
}
