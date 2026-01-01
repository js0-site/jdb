//! WAL write operations
//! WAL 写入操作

use compio::{buf::IoBuf, io::AsyncWriteAtExt};
use jdb_base::{Flag, HEAD_CRC, INFILE_MAX, KEY_MAX, Pos, open_write_create, write_file};
use size_lru::SizeLru;
use zbin::Bin;

use super::{Val, WalConf, WalInner};
use crate::error::{Error, Result};

impl<C: WalConf> WalInner<C> {
  async fn write_val_file(&mut self, data: &[u8]) -> Result<(u64, u32)> {
    let id = self.ider.get();
    let path = self.bin_path(id);
    write_file(&path, data).await?;
    Ok((id, data.len() as u32))
  }

  pub(crate) async fn write_file_io<T: IoBuf>(&mut self, id: u64, data: T) -> Result<T> {
    let path = self.bin_path(id);
    let mut file = open_write_create(&path).await?;
    let res = file.write_all_at(data, 0).await;
    res.0?;
    Ok(res.1)
  }

  /// Write record, return start position
  /// 写入记录，返回起始位置
  #[inline(always)]
  async fn write_record(&mut self, record: &[u8]) -> u64 {
    self.wait_if_full().await;
    let start = self.cur_pos;
    self.cur_pos += record.len() as u64;
    self.shared.push(start, record);
    self.maybe_spawn_writer();
    start
  }

  pub async fn put<'a, 'b>(&mut self, key: impl Bin<'a>, val: impl Bin<'b>) -> Result<Pos> {
    let k = key.as_slice();
    let v = val.as_slice();

    if k.len() > KEY_MAX {
      return Err(Error::DataTooLong(k.len(), KEY_MAX));
    }

    let id = self.ider.get();
    let val_infile = v.len() <= INFILE_MAX;
    let wal_id = self.cur_id();

    let pos = if val_infile {
      let record = self.head_builder.infile(id, Flag::Infile, v, k).to_vec();
      let start = self.write_record(&record).await;
      // val offset = magic(1) + HEAD_CRC
      // val 偏移 = magic(1) + HEAD_CRC
      let val_pos = start + 1 + HEAD_CRC as u64;
      Pos::infile_with_flag(Flag::Infile, wal_id, val_pos, v.len() as u32)
    } else {
      let (file_id, len) = self.write_val_file(v).await?;
      let record = self
        .head_builder
        .file(id, Flag::File, file_id, len, k)
        .to_vec();
      self.write_record(&record).await;
      Pos::file_with_flag(Flag::File, wal_id, file_id, len)
    };

    if val_infile {
      let data: Val = v.into();
      self.val_cache.set(pos, data, v.len() as u32);
    }

    Ok(pos)
  }

  pub async fn put_with_file<'a>(
    &mut self,
    key: impl Bin<'a>,
    val_store: Flag,
    val_file_id: u64,
    val_len: u32,
  ) -> Result<Pos> {
    let k = key.as_slice();
    if k.len() > KEY_MAX {
      return Err(Error::DataTooLong(k.len(), KEY_MAX));
    }

    let id = self.ider.get();
    let wal_id = self.cur_id();
    let record = self
      .head_builder
      .file(id, val_store, val_file_id, val_len, k)
      .to_vec();
    self.write_record(&record).await;
    Ok(Pos::file_with_flag(val_store, wal_id, val_file_id, val_len))
  }

  pub async fn del<'a>(&mut self, key: impl Bin<'a>) -> Result<Pos> {
    let k = key.as_slice();
    if k.len() > KEY_MAX {
      return Err(Error::DataTooLong(k.len(), KEY_MAX));
    }

    let id = self.ider.get();
    let wal_id = self.cur_id();
    let record = self.head_builder.tombstone(id, k).to_vec();
    let start = self.write_record(&record).await;
    // Tombstone: len=0, pos points to head
    // 删除标记：len=0，pos 指向 head
    Ok(Pos::tombstone(wal_id, start + 1))
  }

  pub async fn put_with_store<'a>(
    &mut self,
    key: impl Bin<'a>,
    val: &[u8],
    val_store: Flag,
  ) -> Result<Pos> {
    let k = key.as_slice();
    if k.len() > KEY_MAX {
      return Err(Error::DataTooLong(k.len(), KEY_MAX));
    }
    if val.len() > INFILE_MAX {
      return Err(Error::DataTooLong(val.len(), INFILE_MAX));
    }

    let id = self.ider.get();
    let wal_id = self.cur_id();
    let record = self.head_builder.infile(id, val_store, val, k).to_vec();
    let start = self.write_record(&record).await;
    let val_pos = start + 1 + HEAD_CRC as u64;
    Ok(Pos::infile_with_flag(
      val_store,
      wal_id,
      val_pos,
      val.len() as u32,
    ))
  }
}
