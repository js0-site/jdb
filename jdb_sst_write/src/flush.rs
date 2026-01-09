//! Flush trait implementation
//! Flush trait 实现

use std::path::PathBuf;

use jdb_base::{Pos, sst::Flush};
use jdb_sst::{Conf, Error, Meta};
use oneshot::Receiver;

use crate::{id, write_id};

/// SSTable writer implementing Flush trait
/// 实现 Flush trait 的 SSTable 写入器
pub struct Write {
  dir: PathBuf,
  level: u8,
  conf: Box<[Conf]>,
}

impl Write {
  pub fn new(dir: impl Into<PathBuf>, level: u8, conf: impl Into<Box<[Conf]>>) -> Self {
    Self {
      dir: dir.into(),
      level,
      conf: conf.into(),
    }
  }
}

impl Flush for Write {
  type Error = Error;

  fn flush<'a, I>(&mut self, iter: I) -> Receiver<std::result::Result<Meta, Self::Error>>
  where
    I: Iterator<Item = (&'a Box<[u8]>, &'a Pos)>,
  {
    let (tx, rx) = oneshot::channel();
    let dir = self.dir.clone();
    let level = self.level;
    let conf = self.conf.clone();

    let sst_id = id::new(&dir);

    let data: Vec<(Box<[u8]>, Pos)> = iter.map(|(k, v)| (k.clone(), *v)).collect();

    cpu_bind::spawn(move |rt| {
      let iter = data.iter().map(|(k, v)| (k, v));
      let result = rt.block_on(write_id(&dir, level, iter, &conf, sst_id));
      let _ = tx.send(result);
    });

    rx
  }
}
