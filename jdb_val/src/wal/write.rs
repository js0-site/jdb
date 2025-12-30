//! WAL write operations
//! WAL 写入操作

use std::mem;

use compio::{buf::IoBuf, io::AsyncWriteAtExt};
use compio_fs::OpenOptions;
use size_lru::SizeLru;

use super::{CachedData, MAX_SLOT_SIZE, WalConf, WalInner};
use crate::{
  Bin, FilePos, INFILE_MAX, Pos, Store,
  error::{Error, Result},
};

impl<C: WalConf> WalInner<C> {
  #[inline]
  pub(crate) async fn reserve(&mut self, len: u64) -> Result<()> {
    if self.cur_pos + len > self.max_size {
      self.rotate().await?;
    }
    Ok(())
  }

  async fn write_file(&mut self, data: &[u8]) -> Result<FilePos> {
    let id = self.ider.get();
    let path = self.bin_path(id);
    let mut file = OpenOptions::new()
      .write(true)
      .create(true)
      .open(&path)
      .await?;
    file.write_all_at(data.to_vec(), 0).await.0?;
    Ok(FilePos::with_hash(id, 0, data))
  }

  pub(crate) async fn write_file_io<T: IoBuf>(&mut self, id: u64, data: T) -> Result<T> {
    let path = self.bin_path(id);
    let mut file = OpenOptions::new()
      .write(true)
      .create(true)
      .open(&path)
      .await?;
    let res = file.write_all_at(data, 0).await;
    res.0?;
    Ok(res.1)
  }

  pub async fn put<'a, 'b>(&mut self, key: impl Bin<'a>, val: impl Bin<'b>) -> Result<Pos> {
    let k_len = key.len();
    let v_len = val.len();

    if k_len > u32::MAX as usize {
      return Err(Error::DataTooLong(k_len, u32::MAX as usize));
    }
    if v_len > u32::MAX as usize {
      return Err(Error::DataTooLong(v_len, u32::MAX as usize));
    }

    let id = self.ider.get();

    let (k_store, k_pos) = if k_len <= INFILE_MAX {
      (Store::Infile, None)
    } else {
      let pos = self.write_file(key.as_slice()).await?;
      (Store::File, Some(pos))
    };

    // Cache val for INFILE mode
    // 对 INFILE 模式缓存 val
    let val_infile = v_len <= INFILE_MAX;

    let (v_store, v_pos) = if val_infile {
      (Store::Infile, None)
    } else {
      let pos = self.write_file(val.as_slice()).await?;
      (Store::File, Some(pos))
    };

    let head_bytes = match (k_pos, v_pos) {
      (None, None) => self
        .head_builder
        .infile_infile(id, k_store, key.as_slice(), v_store, val.as_slice())
        .to_vec(),
      (None, Some(vp)) => self
        .head_builder
        .infile_file(id, k_store, key.as_slice(), v_store, &vp, v_len as u64)
        .to_vec(),
      (Some(kp), None) => self
        .head_builder
        .file_infile(id, k_store, &kp, k_len as u64, v_store, val.as_slice())
        .to_vec(),
      (Some(kp), Some(vp)) => self
        .head_builder
        .file_file(id, k_store, &kp, k_len as u64, v_store, &vp, v_len as u64)
        .to_vec(),
    };

    let total = head_bytes.len() as u64;
    self.reserve(total).await?;

    let start = self.cur_pos;
    self.write_combined(&[&head_bytes], start).await?;
    self.cur_pos += total;

    let pos = Pos::new(self.cur_id(), start);

    // Update val_cache for INFILE mode (like memtable)
    // 对 INFILE 模式更新 val_cache（类似 memtable）
    if val_infile {
      let data: CachedData = val.as_slice().into();
      self.val_cache.set(pos, data, v_len as u32);
    }

    Ok(pos)
  }

  pub async fn put_with_file<'a>(
    &mut self,
    key: impl Bin<'a>,
    val_store: Store,
    val_file_id: u64,
    val_len: u64,
    val_hash: u128,
  ) -> Result<Pos> {
    let k_len = key.len();
    if k_len > u32::MAX as usize {
      return Err(Error::DataTooLong(k_len, u32::MAX as usize));
    }

    let id = self.ider.get();
    let val_pos = FilePos {
      file_id: val_file_id,
      offset: 0,
      hash: val_hash,
    };

    let (k_store, k_pos) = if k_len <= INFILE_MAX {
      (Store::Infile, None)
    } else {
      let pos = self.write_file(key.as_slice()).await?;
      (Store::File, Some(pos))
    };

    let head_bytes = if let Some(kp) = k_pos {
      self
        .head_builder
        .file_file(id, k_store, &kp, k_len as u64, val_store, &val_pos, val_len)
        .to_vec()
    } else {
      self
        .head_builder
        .infile_file(id, k_store, key.as_slice(), val_store, &val_pos, val_len)
        .to_vec()
    };

    let total = head_bytes.len() as u64;
    self.reserve(total).await?;

    let start = self.cur_pos;
    self.write_combined(&[&head_bytes], start).await?;
    self.cur_pos += total;

    Ok(Pos::new(self.cur_id(), start))
  }

  pub(crate) async fn write_combined(&mut self, parts: &[&[u8]], pos: u64) -> Result<()> {
    let total_len: usize = parts.iter().map(|p| p.len()).sum();

    if total_len > MAX_SLOT_SIZE {
      self.flush().await?;
      if let Some(f) = self.shared.file() {
        self.data_buf.clear();
        self.data_buf.reserve(total_len);
        for p in parts {
          self.data_buf.extend_from_slice(p);
        }
        let buf = mem::take(&mut self.data_buf);
        let res = f.write_all_at(buf, pos).await;
        self.data_buf = res.1;
        res.0?;
      }
      return Ok(());
    }

    self.shared.push_slices(pos, parts, total_len);
    self.maybe_spawn_writer();
    Ok(())
  }

  pub async fn del<'a>(&mut self, key: impl Bin<'a>) -> Result<Pos> {
    let k_len = key.len();
    if k_len > u32::MAX as usize {
      return Err(Error::DataTooLong(k_len, u32::MAX as usize));
    }

    let id = self.ider.get();

    let head_bytes = if k_len <= INFILE_MAX {
      self
        .head_builder
        .tombstone_infile(id, Store::Infile, key.as_slice())
        .to_vec()
    } else {
      let kp = self.write_file(key.as_slice()).await?;
      self
        .head_builder
        .tombstone_file(id, Store::File, &kp, k_len as u64)
        .to_vec()
    };

    let total = head_bytes.len() as u64;
    self.reserve(total).await?;

    let start = self.cur_pos;
    self.write_combined(&[&head_bytes], start).await?;
    self.cur_pos += total;

    Ok(Pos::new(self.cur_id(), start))
  }

  pub async fn put_infile_lz4<'a>(
    &mut self,
    key: impl Bin<'a>,
    compressed: &[u8],
    _original_len: u64,
  ) -> Result<Pos> {
    let k_len = key.len();
    let c_len = compressed.len();
    if k_len > u32::MAX as usize {
      return Err(Error::DataTooLong(k_len, u32::MAX as usize));
    }
    if c_len > INFILE_MAX {
      return Err(Error::DataTooLong(c_len, INFILE_MAX));
    }

    let id = self.ider.get();

    let (k_store, k_pos) = if k_len <= INFILE_MAX {
      (Store::Infile, None)
    } else {
      let pos = self.write_file(key.as_slice()).await?;
      (Store::File, Some(pos))
    };

    let head_bytes = if let Some(kp) = k_pos {
      self
        .head_builder
        .file_infile(id, k_store, &kp, k_len as u64, Store::InfileLz4, compressed)
        .to_vec()
    } else {
      self
        .head_builder
        .infile_infile(id, k_store, key.as_slice(), Store::InfileLz4, compressed)
        .to_vec()
    };

    let total = head_bytes.len() as u64;
    self.reserve(total).await?;

    let start = self.cur_pos;
    self.write_combined(&[&head_bytes], start).await?;
    self.cur_pos += total;

    Ok(Pos::new(self.cur_id(), start))
  }

  pub async fn put_with_store<'a>(
    &mut self,
    key: impl Bin<'a>,
    val: &[u8],
    val_store: Store,
  ) -> Result<Pos> {
    let k_len = key.len();
    let v_len = val.len();
    if k_len > u32::MAX as usize {
      return Err(Error::DataTooLong(k_len, u32::MAX as usize));
    }
    if v_len > INFILE_MAX {
      return Err(Error::DataTooLong(v_len, INFILE_MAX));
    }

    let id = self.ider.get();

    let (k_store, k_pos) = if k_len <= INFILE_MAX {
      (Store::Infile, None)
    } else {
      let pos = self.write_file(key.as_slice()).await?;
      (Store::File, Some(pos))
    };

    let head_bytes = if let Some(kp) = k_pos {
      self
        .head_builder
        .file_infile(id, k_store, &kp, k_len as u64, val_store, val)
        .to_vec()
    } else {
      self
        .head_builder
        .infile_infile(id, k_store, key.as_slice(), val_store, val)
        .to_vec()
    };

    let total = head_bytes.len() as u64;
    self.reserve(total).await?;

    let start = self.cur_pos;
    self.write_combined(&[&head_bytes], start).await?;
    self.cur_pos += total;

    Ok(Pos::new(self.cur_id(), start))
  }
}
