//! Write-Ahead Log for crash recovery
//! 用于崩溃恢复的预写日志

#![cfg_attr(docsrs, feature(doc_cfg))]
#![allow(clippy::await_holding_refcell_ref)] // compio single-thread runtime / compio 单线程运行时

mod error;

use std::{
  cell::RefCell,
  path::{Path, PathBuf},
};

use bytes::Bytes;
pub use error::{Error, Result};
use jdb_alloc::{AlignedBuf, PAGE_SIZE};
use jdb_fs::File;

/// WAL file extension / WAL 文件扩展名
const EXT: &str = "wal";

/// Max WAL file size (64MB) / 最大 WAL 文件大小
const MAX_SIZE: u64 = 64 * 1024 * 1024;

/// Record type / 记录类型
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordType {
  /// Put operation / 写入操作
  Put = 1,
  /// Delete operation / 删除操作
  Del = 2,
  /// Commit marker / 提交标记
  Commit = 3,
}

impl TryFrom<u8> for RecordType {
  type Error = Error;
  fn try_from(v: u8) -> Result<Self> {
    match v {
      1 => Ok(Self::Put),
      2 => Ok(Self::Del),
      3 => Ok(Self::Commit),
      _ => Err(Error::InvalidRecord),
    }
  }
}

/// WAL record / WAL 记录
#[derive(Debug, Clone)]
pub struct Record {
  pub typ: RecordType,
  pub db_id: u64,
  pub key: Bytes,
  pub val: Bytes,
}

impl Record {
  pub fn put(db_id: u64, key: impl Into<Bytes>, val: impl Into<Bytes>) -> Self {
    Self {
      typ: RecordType::Put,
      db_id,
      key: key.into(),
      val: val.into(),
    }
  }

  pub fn del(db_id: u64, key: impl Into<Bytes>) -> Self {
    Self {
      typ: RecordType::Del,
      db_id,
      key: key.into(),
      val: Bytes::new(),
    }
  }

  pub fn commit(db_id: u64) -> Self {
    Self {
      typ: RecordType::Commit,
      db_id,
      key: Bytes::new(),
      val: Bytes::new(),
    }
  }
}

struct WalState {
  active_id: u64,
  active: File,
  size: u64,
}

/// Write-Ahead Log / 预写日志
pub struct Wal {
  dir: PathBuf,
  state: RefCell<WalState>,
}

impl Wal {
  /// Open or create WAL / 打开或创建 WAL
  pub async fn open(dir: impl AsRef<Path>) -> Result<Self> {
    let dir = dir.as_ref().to_path_buf();
    jdb_fs::mkdir(&dir).await?;

    // Find max file id / 查找最大文件 ID
    let files = jdb_fs::ls(&dir).await?;
    let mut max_id = 0u64;
    for f in &files {
      if let Some(name) = f.file_name().and_then(|n| n.to_str())
        && let Some(id_str) = name.strip_suffix(&format!(".{EXT}"))
        && let Ok(id) = id_str.parse::<u64>()
      {
        max_id = max_id.max(id);
      }
    }

    let active_id = if max_id == 0 { 1 } else { max_id };
    let path = Self::file_path(&dir, active_id);
    let (active, size) = if jdb_fs::exists(&path) {
      let f = File::open_rw(&path).await?;
      let size = f.size().await?;
      (f, size)
    } else {
      let f = File::create(&path).await?;
      (f, 0)
    };

    Ok(Self {
      dir,
      state: RefCell::new(WalState {
        active_id,
        active,
        size,
      }),
    })
  }

  fn file_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{id:08}.{EXT}"))
  }

  /// Append record / 追加记录
  pub async fn append(&self, rec: &Record) -> Result<()> {
    let mut state = self.state.borrow_mut();

    // Check rotation / 检查轮转
    if state.size >= MAX_SIZE {
      drop(state);
      self.rotate().await?;
      state = self.state.borrow_mut();
    }

    // Encode: len(4) + crc(4) + type(1) + db_id(8) + key_len(4) + key + val_len(4) + val
    let record_len = 1 + 8 + 4 + rec.key.len() + 4 + rec.val.len();
    let total_len = 4 + 4 + record_len;
    let aligned_len = align_up(total_len);

    let mut buf = AlignedBuf::zeroed(aligned_len)?;

    // len (4B)
    buf[0..4].copy_from_slice(&(record_len as u32).to_le_bytes());

    // type (1B)
    buf[8] = rec.typ as u8;

    // db_id (8B)
    buf[9..17].copy_from_slice(&rec.db_id.to_le_bytes());

    // key_len (4B) + key
    buf[17..21].copy_from_slice(&(rec.key.len() as u32).to_le_bytes());
    buf[21..21 + rec.key.len()].copy_from_slice(&rec.key);

    // val_len (4B) + val
    let val_pos = 21 + rec.key.len();
    buf[val_pos..val_pos + 4].copy_from_slice(&(rec.val.len() as u32).to_le_bytes());
    buf[val_pos + 4..val_pos + 4 + rec.val.len()].copy_from_slice(&rec.val);

    // crc (4B)
    let crc = crc32(&buf[8..8 + record_len]);
    buf[4..8].copy_from_slice(&crc.to_le_bytes());

    // Write
    let offset = state.size;
    state.active.write_at(buf, offset).await?;
    state.size += aligned_len as u64;

    Ok(())
  }

  /// Rotate to new file / 轮转到新文件
  pub async fn rotate(&self) -> Result<()> {
    let mut state = self.state.borrow_mut();
    state.active.sync_data().await?;
    state.active_id += 1;
    let path = Self::file_path(&self.dir, state.active_id);
    state.active = File::create(&path).await?;
    state.size = 0;
    Ok(())
  }

  /// Sync to disk / 同步到磁盘
  pub async fn sync(&self) -> Result<()> {
    let state = self.state.borrow();
    state.active.sync_data().await?;
    Ok(())
  }

  /// Recover records from WAL / 从 WAL 恢复记录
  pub async fn recover(&self) -> Result<Vec<Record>> {
    let mut records = Vec::new();

    // Read all WAL files / 读取所有 WAL 文件
    let files = jdb_fs::ls(&self.dir).await?;
    let mut ids: Vec<u64> = files
      .iter()
      .filter_map(|f| {
        f.file_name()
          .and_then(|n| n.to_str())
          .and_then(|n| n.strip_suffix(&format!(".{EXT}")))
          .and_then(|s| s.parse().ok())
      })
      .collect();
    ids.sort();

    for id in ids {
      let path = Self::file_path(&self.dir, id);
      let file = File::open(&path).await?;
      let size = file.size().await?;

      let mut offset = 0u64;
      while offset < size {
        match self.read_record(&file, offset).await {
          Ok((rec, len)) => {
            records.push(rec);
            offset += len as u64;
          }
          Err(Error::Incomplete) => break,
          Err(e) => return Err(e),
        }
      }
    }

    Ok(records)
  }

  async fn read_record(&self, file: &File, offset: u64) -> Result<(Record, usize)> {
    // Read header / 读取头部
    let header = AlignedBuf::zeroed(PAGE_SIZE)?;
    let header = file.read_at(header, offset).await?;

    let record_len = u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
    if record_len == 0 {
      return Err(Error::Incomplete);
    }

    let crc_stored = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
    let crc_calc = crc32(&header[8..8 + record_len]);
    if crc_stored != crc_calc {
      return Err(Error::Crc {
        expected: crc_stored,
        got: crc_calc,
      });
    }

    let typ = RecordType::try_from(header[8])?;
    let db_id = u64::from_le_bytes([
      header[9], header[10], header[11], header[12], header[13], header[14], header[15], header[16],
    ]);

    let key_len = u32::from_le_bytes([header[17], header[18], header[19], header[20]]) as usize;
    let key = Bytes::copy_from_slice(&header[21..21 + key_len]);

    let val_pos = 21 + key_len;
    let val_len = u32::from_le_bytes([
      header[val_pos],
      header[val_pos + 1],
      header[val_pos + 2],
      header[val_pos + 3],
    ]) as usize;
    let val = Bytes::copy_from_slice(&header[val_pos + 4..val_pos + 4 + val_len]);

    let total_len = align_up(4 + 4 + record_len);

    Ok((
      Record {
        typ,
        db_id,
        key,
        val,
      },
      total_len,
    ))
  }

  /// Clear WAL files / 清除 WAL 文件
  pub async fn clear(&self) -> Result<()> {
    let files = jdb_fs::ls(&self.dir).await?;
    for f in files {
      if f
        .file_name()
        .and_then(|n| n.to_str())
        .is_some_and(|n| n.ends_with(&format!(".{EXT}")))
      {
        let _ = std::fs::remove_file(&f);
      }
    }

    // Create new active file / 创建新活跃文件
    let mut state = self.state.borrow_mut();
    state.active_id = 1;
    let path = Self::file_path(&self.dir, 1);
    state.active = File::create(&path).await?;
    state.size = 0;

    Ok(())
  }

  /// Get active file id / 获取活跃文件 ID
  pub fn active_id(&self) -> u64 {
    self.state.borrow().active_id
  }

  /// Get active file size / 获取活跃文件大小
  pub fn size(&self) -> u64 {
    self.state.borrow().size
  }
}

#[inline]
fn align_up(n: usize) -> usize {
  (n + PAGE_SIZE - 1) & !(PAGE_SIZE - 1)
}

fn crc32(data: &[u8]) -> u32 {
  let mut crc = 0xFFFF_FFFFu32;
  for &byte in data {
    crc ^= byte as u32;
    for _ in 0..8 {
      crc = if crc & 1 != 0 {
        (crc >> 1) ^ 0xEDB8_8320
      } else {
        crc >> 1
      };
    }
  }
  !crc
}
