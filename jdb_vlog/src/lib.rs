//! Value Log for KV separation
//! KV 分离的值日志

#![cfg_attr(docsrs, feature(doc_cfg))]
#![allow(clippy::await_holding_refcell_ref)] // compio single-thread runtime / compio 单线程运行时

mod error;

use std::{
  cell::RefCell,
  fs::File as StdFile,
  path::{Path, PathBuf},
};

use bytes::Bytes;
use coarsetime::Clock;
use compio::{
  fs::AsyncFd,
  io::{AsyncWrite, AsyncWriteExt, BufWriter},
};
pub use error::{Error, Result};
use jdb_alloc::{AlignedBuf, PAGE_SIZE};
use jdb_fs::File;
use jdb_trait::ValRef;

/// Tombstone flag in offset / offset 中的 tombstone 标记
const TOMBSTONE_FLAG: u64 = 1 << 63;

/// External file flag in offset / offset 中的外部文件标记
const EXTERNAL_FLAG: u64 = 1 << 62;

/// Record flag: inline value / 记录标记：内联值
const FLAG_INLINE: u8 = 0;

/// Record flag: tombstone / 记录标记：墓碑
const FLAG_TOMBSTONE: u8 = 1;

/// Record flag: external file / 记录标记：外部文件
const FLAG_EXTERNAL: u8 = 2;

/// Max file size before rotation (256MB) / 轮转前最大文件大小
const MAX_FILE_SIZE: u64 = 256 * 1024 * 1024;

/// Large value threshold (1MB) / 大值阈值
const LARGE_THRESHOLD: usize = 1024 * 1024;

/// File extension / 文件扩展名
const EXT: &str = "vlog";

/// Blob extension / Blob 扩展名
const BLOB_EXT: &str = "blob";

/// Body CRC size / Body CRC 大小
const CRC_B_SIZE: usize = 4;

/// Record layout (Header CRC + Body CRC):
/// 记录布局（头部 CRC + 体部 CRC）:
/// ```text
/// Inline (flag=0):
/// [0..4]     crc_h (u32)       - header CRC, covers [4..37]
/// [4..12]    len (u64)         - record length
/// [12]       flag (u8)         - 0=inline
/// [13..21]   ts (u64)          - timestamp seconds
/// [21..29]   prev_file_id (u64)
/// [29..37]   prev_offset (u64)
/// [37..]     value
/// [len-4..len] crc_b (u32)     - body CRC
///
/// External (flag=2):
/// [0..4]     crc_h (u32)       - header CRC, covers [4..45]
/// [4..12]    len (u64)         - record length (= 49)
/// [12]       flag (u8)         - 2=external
/// [13..21]   ts (u64)          - timestamp seconds
/// [21..29]   prev_file_id (u64)
/// [29..37]   prev_offset (u64)
/// [37..45]   blob_id (u64)     - blob file id
/// [45..49]   crc_b (u32)       - body CRC (covers [37..45])
/// ```
const HEADER_SIZE: usize = 37;
const EXTERNAL_HEADER_SIZE: usize = 45;
const EXTERNAL_RECORD_LEN: usize = 49;

/// Value Log / 值日志
pub struct VLog {
  dir: PathBuf,
  blob_dir: PathBuf,
  state: RefCell<VLogState>,
}

struct VLogState {
  active_id: u64,
  active: File,
  active_size: u64,
  next_blob_id: u64,
}

/// Get current timestamp seconds / 获取当前时间戳秒
#[inline]
fn now_secs() -> u64 {
  Clock::recent_since_epoch().as_secs()
}

/// Generate blob path from id / 根据 id 生成 blob 路径
/// Format: ab/cd/ef.blob (3 levels, 2 chars each)
fn blob_path(dir: &Path, id: u64) -> PathBuf {
  let b64 = ub64::u64_b64(id);
  let chars: Vec<char> = b64.chars().collect();
  let len = chars.len();

  // Pad to at least 6 chars / 至少 6 字符
  let (l1, l2, rest) = if len >= 6 {
    let l1: String = chars[..2].iter().collect();
    let l2: String = chars[2..4].iter().collect();
    let rest: String = chars[4..].iter().collect();
    (l1, l2, rest)
  } else {
    // Short id, use padding / 短 id 用填充
    let padded = format!("{:0>6}", b64);
    let chars: Vec<char> = padded.chars().collect();
    let l1: String = chars[..2].iter().collect();
    let l2: String = chars[2..4].iter().collect();
    let rest: String = chars[4..].iter().collect();
    (l1, l2, rest)
  };

  dir.join(l1).join(l2).join(format!("{rest}.{BLOB_EXT}"))
}

impl VLog {
  /// Open or create VLog / 打开或创建 VLog
  pub async fn open(dir: impl AsRef<Path>) -> Result<Self> {
    Clock::update();

    let dir = dir.as_ref().to_path_buf();
    jdb_fs::mkdir(&dir).await?;

    let blob_dir = dir.join("blob");
    jdb_fs::mkdir(&blob_dir).await?;

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
    let active_path = Self::file_path(&dir, active_id);
    let (active, active_size) = if jdb_fs::exists(&active_path) {
      let f = File::open_rw(&active_path).await?;
      let size = f.size().await?;
      (f, size)
    } else {
      let f = File::create(&active_path).await?;
      (f, 0)
    };

    // Find max blob id / 查找最大 blob id
    let next_blob_id = Self::scan_max_blob_id(&blob_dir).await + 1;

    Ok(Self {
      dir,
      blob_dir,
      state: RefCell::new(VLogState {
        active_id,
        active,
        active_size,
        next_blob_id,
      }),
    })
  }

  async fn scan_max_blob_id(blob_dir: &Path) -> u64 {
    // Simple scan, could be optimized / 简单扫描，可优化
    let mut max_id = 0u64;
    if let Ok(l1_dirs) = jdb_fs::ls(blob_dir).await {
      for l1 in l1_dirs {
        if let Ok(l2_dirs) = jdb_fs::ls(&l1).await {
          for l2 in l2_dirs {
            if let Ok(files) = jdb_fs::ls(&l2).await {
              for f in files {
                if let Some(name) = f.file_name().and_then(|n| n.to_str())
                  && let Some(rest) = name.strip_suffix(&format!(".{BLOB_EXT}"))
                {
                  // Reconstruct full b64 / 重建完整 b64
                  let l1_name = l1.file_name().and_then(|n| n.to_str()).unwrap_or("");
                  let l2_name = l2.file_name().and_then(|n| n.to_str()).unwrap_or("");
                  let full_b64 = format!("{l1_name}{l2_name}{rest}");
                  let id = ub64::b64_u64(&full_b64);
                  max_id = max_id.max(id);
                }
              }
            }
          }
        }
      }
    }
    max_id
  }

  fn file_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{id:08}.{EXT}"))
  }

  /// Append value / 追加值
  pub async fn append(&self, val: &[u8], prev: Option<&ValRef>) -> Result<ValRef> {
    if val.len() >= LARGE_THRESHOLD {
      self.append_external(val, prev).await
    } else {
      self.append_inline(val, prev).await
    }
  }

  /// Append tombstone / 追加墓碑
  pub async fn append_tombstone(&self, prev: Option<&ValRef>) -> Result<ValRef> {
    self.append_inline_inner(None, prev, FLAG_TOMBSTONE).await
  }

  /// Append inline value / 追加内联值
  async fn append_inline(&self, val: &[u8], prev: Option<&ValRef>) -> Result<ValRef> {
    self.append_inline_inner(Some(val), prev, FLAG_INLINE).await
  }

  async fn append_inline_inner(
    &self,
    val: Option<&[u8]>,
    prev: Option<&ValRef>,
    flag: u8,
  ) -> Result<ValRef> {
    let mut state = self.state.borrow_mut();

    if state.active_size >= MAX_FILE_SIZE {
      self.rotate_inner(&mut state).await?;
    }

    let val_len = val.map(|v| v.len()).unwrap_or(0);
    let record_len = HEADER_SIZE + val_len + CRC_B_SIZE;
    let aligned_len = align_up(record_len);

    let mut buf = AlignedBuf::zeroed(aligned_len)?;

    // len (8B) at [4..12]
    buf[4..12].copy_from_slice(&(record_len as u64).to_le_bytes());

    // flag (1B) at [12]
    buf[12] = flag;

    // ts (8B) at [13..21]
    let ts = now_secs();
    buf[13..21].copy_from_slice(&ts.to_le_bytes());

    // prev_file_id (8B) + prev_offset (8B) at [21..37]
    let (pfid, poff) = prev.map(|p| (p.file_id, p.offset)).unwrap_or((0, 0));
    buf[21..29].copy_from_slice(&pfid.to_le_bytes());
    buf[29..37].copy_from_slice(&poff.to_le_bytes());

    // crc_h (4B) at [0..4] - covers [4..37]
    let crc_h = crc32(&buf[4..HEADER_SIZE]);
    buf[0..4].copy_from_slice(&crc_h.to_le_bytes());

    // value at [37..]
    if let Some(v) = val {
      buf[HEADER_SIZE..HEADER_SIZE + v.len()].copy_from_slice(v);
    }

    // crc_b (4B) at [len-4..len] - covers [37..len-4]
    let body_end = record_len - CRC_B_SIZE;
    let crc_b = crc32(&buf[HEADER_SIZE..body_end]);
    buf[body_end..record_len].copy_from_slice(&crc_b.to_le_bytes());

    // Write
    let offset = state.active_size;
    state.active.write_at(buf, offset).await?;
    state.active_size += aligned_len as u64;

    let mut result_offset = offset;
    if flag == FLAG_TOMBSTONE {
      result_offset |= TOMBSTONE_FLAG;
    }

    Ok(ValRef {
      file_id: state.active_id,
      offset: result_offset,
      prev_file_id: prev.map(|p| p.file_id).unwrap_or(0),
      prev_offset: prev.map(|p| p.offset).unwrap_or(0),
    })
  }

  /// Append external (large) value / 追加外部（大）值
  async fn append_external(&self, val: &[u8], prev: Option<&ValRef>) -> Result<ValRef> {
    let blob_id = {
      let mut state = self.state.borrow_mut();
      let id = state.next_blob_id;
      state.next_blob_id += 1;
      id
    };

    // Prepare blob path / 准备 blob 路径
    let blob_path = blob_path(&self.blob_dir, blob_id);
    if let Some(parent) = blob_path.parent() {
      jdb_fs::mkdir(parent).await?;
    }

    // Write blob file with compio BufWriter / 用 compio BufWriter 写入 blob 文件
    let std_file = StdFile::create(&blob_path)?;
    let async_fd = AsyncFd::new(std_file)?;
    let mut writer = BufWriter::new(async_fd);
    let compio::buf::BufResult(res, _) = writer.write_all(val.to_vec()).await;
    res?;
    AsyncWrite::flush(&mut writer).await?;

    // Write vlog record / 写入 vlog 记录
    let mut state = self.state.borrow_mut();

    if state.active_size >= MAX_FILE_SIZE {
      self.rotate_inner(&mut state).await?;
    }

    let aligned_len = align_up(EXTERNAL_RECORD_LEN);
    let mut buf = AlignedBuf::zeroed(aligned_len)?;

    // len (8B) at [4..12]
    buf[4..12].copy_from_slice(&(EXTERNAL_RECORD_LEN as u64).to_le_bytes());

    // flag (1B) at [12]
    buf[12] = FLAG_EXTERNAL;

    // ts (8B) at [13..21]
    let ts = now_secs();
    buf[13..21].copy_from_slice(&ts.to_le_bytes());

    // prev_file_id (8B) + prev_offset (8B) at [21..37]
    let (pfid, poff) = prev.map(|p| (p.file_id, p.offset)).unwrap_or((0, 0));
    buf[21..29].copy_from_slice(&pfid.to_le_bytes());
    buf[29..37].copy_from_slice(&poff.to_le_bytes());

    // blob_id (8B) at [37..45]
    buf[37..45].copy_from_slice(&blob_id.to_le_bytes());

    // crc_h (4B) at [0..4] - covers [4..45]
    let crc_h = crc32(&buf[4..EXTERNAL_HEADER_SIZE]);
    buf[0..4].copy_from_slice(&crc_h.to_le_bytes());

    // crc_b (4B) at [45..49] - covers [37..45]
    let crc_b = crc32(&buf[HEADER_SIZE..EXTERNAL_HEADER_SIZE]);
    buf[EXTERNAL_HEADER_SIZE..EXTERNAL_RECORD_LEN].copy_from_slice(&crc_b.to_le_bytes());

    // Write
    let offset = state.active_size;
    state.active.write_at(buf, offset).await?;
    state.active_size += aligned_len as u64;

    // Mark as external / 标记为外部
    let result_offset = offset | EXTERNAL_FLAG;

    Ok(ValRef {
      file_id: state.active_id,
      offset: result_offset,
      prev_file_id: prev.map(|p| p.file_id).unwrap_or(0),
      prev_offset: prev.map(|p| p.offset).unwrap_or(0),
    })
  }

  /// Get value / 获取值
  pub async fn get(&self, vref: &ValRef) -> Result<Option<Bytes>> {
    if vref.is_tombstone() {
      return Ok(None);
    }
    self
      .get_full(vref)
      .await
      .map(|res| res.map(|(val, ..)| val))
  }

  /// Check if external / 检查是否外部文件
  #[inline]
  fn is_external(offset: u64) -> bool {
    offset & EXTERNAL_FLAG != 0
  }

  /// Get real offset (strip flags) / 获取实际偏移（去除标记）
  #[inline]
  fn real_offset(offset: u64) -> u64 {
    offset & !(TOMBSTONE_FLAG | EXTERNAL_FLAG)
  }

  /// Get value, full ValRef, and timestamp / 获取值、完整引用和时间戳
  pub async fn get_full(&self, vref: &ValRef) -> Result<Option<(Bytes, ValRef, u64)>> {
    if vref.is_tombstone() && Self::real_offset(vref.offset) == 0 {
      return Ok(None);
    }

    let file = self.open_file(vref.file_id).await?;
    let offset = Self::real_offset(vref.offset);
    let is_external = Self::is_external(vref.offset);

    // Read header
    let header_buf = AlignedBuf::zeroed(PAGE_SIZE)?;
    let header_buf = file.read_at(header_buf, offset).await?;

    let header_size = if is_external {
      EXTERNAL_HEADER_SIZE
    } else {
      HEADER_SIZE
    };

    // Verify header CRC / 校验头部 CRC
    let crc_h_stored =
      u32::from_le_bytes([header_buf[0], header_buf[1], header_buf[2], header_buf[3]]);
    let crc_h_calc = crc32(&header_buf[4..header_size]);
    if crc_h_stored != crc_h_calc {
      return Err(Error::HeaderCrc {
        expected: crc_h_stored,
        got: crc_h_calc,
      });
    }

    // Parse header / 解析头部
    let len = u64::from_le_bytes([
      header_buf[4],
      header_buf[5],
      header_buf[6],
      header_buf[7],
      header_buf[8],
      header_buf[9],
      header_buf[10],
      header_buf[11],
    ]) as usize;

    let flag = header_buf[12];

    let ts = u64::from_le_bytes([
      header_buf[13],
      header_buf[14],
      header_buf[15],
      header_buf[16],
      header_buf[17],
      header_buf[18],
      header_buf[19],
      header_buf[20],
    ]);

    let prev_file_id = u64::from_le_bytes([
      header_buf[21],
      header_buf[22],
      header_buf[23],
      header_buf[24],
      header_buf[25],
      header_buf[26],
      header_buf[27],
      header_buf[28],
    ]);

    let prev_offset = u64::from_le_bytes([
      header_buf[29],
      header_buf[30],
      header_buf[31],
      header_buf[32],
      header_buf[33],
      header_buf[34],
      header_buf[35],
      header_buf[36],
    ]);

    let full_vref = ValRef {
      file_id: vref.file_id,
      offset: vref.offset,
      prev_file_id,
      prev_offset,
    };

    if flag == FLAG_TOMBSTONE {
      return Ok(Some((Bytes::new(), full_vref, ts)));
    }

    if flag == FLAG_EXTERNAL {
      // Read blob_id / 读取 blob_id
      let blob_id = u64::from_le_bytes([
        header_buf[37],
        header_buf[38],
        header_buf[39],
        header_buf[40],
        header_buf[41],
        header_buf[42],
        header_buf[43],
        header_buf[44],
      ]);

      // Verify body CRC / 校验 body CRC
      let crc_b_stored = u32::from_le_bytes([
        header_buf[45],
        header_buf[46],
        header_buf[47],
        header_buf[48],
      ]);
      let crc_b_calc = crc32(&header_buf[HEADER_SIZE..EXTERNAL_HEADER_SIZE]);
      if crc_b_stored != crc_b_calc {
        return Err(Error::BodyCrc {
          expected: crc_b_stored,
          got: crc_b_calc,
        });
      }

      // Read blob file / 读取 blob 文件
      let val = self.read_blob(blob_id).await?;
      return Ok(Some((val, full_vref, ts)));
    }

    // Inline value / 内联值
    self
      .read_inline_value(&header_buf, len, &full_vref, ts, &file, offset)
      .await
  }

  async fn read_inline_value(
    &self,
    header_buf: &AlignedBuf,
    len: usize,
    full_vref: &ValRef,
    ts: u64,
    file: &File,
    offset: u64,
  ) -> Result<Option<(Bytes, ValRef, u64)>> {
    let body_start = HEADER_SIZE;
    let body_end = len - CRC_B_SIZE;

    // Small record in header_buf / 小记录在 header_buf 中
    if len <= PAGE_SIZE {
      let body_data = &header_buf[body_start..body_end];

      // Verify body CRC / 校验 body CRC
      let crc_b_stored = u32::from_le_bytes([
        header_buf[body_end],
        header_buf[body_end + 1],
        header_buf[body_end + 2],
        header_buf[body_end + 3],
      ]);
      let crc_b_calc = crc32(body_data);
      if crc_b_stored != crc_b_calc {
        return Err(Error::BodyCrc {
          expected: crc_b_stored,
          got: crc_b_calc,
        });
      }

      return Ok(Some((Bytes::copy_from_slice(body_data), *full_vref, ts)));
    }

    // Large inline (shouldn't happen with LARGE_THRESHOLD, but handle it)
    // 大内联（有 LARGE_THRESHOLD 不应发生，但处理它）
    let aligned_len = align_up(len);
    let full_buf = AlignedBuf::zeroed(aligned_len)?;
    let full_buf = file.read_at(full_buf, offset).await?;

    let body_data = &full_buf[body_start..body_end];

    let crc_b_stored = u32::from_le_bytes([
      full_buf[body_end],
      full_buf[body_end + 1],
      full_buf[body_end + 2],
      full_buf[body_end + 3],
    ]);
    let crc_b_calc = crc32(body_data);
    if crc_b_stored != crc_b_calc {
      return Err(Error::BodyCrc {
        expected: crc_b_stored,
        got: crc_b_calc,
      });
    }

    Ok(Some((Bytes::copy_from_slice(body_data), *full_vref, ts)))
  }

  /// Read blob file / 读取 blob 文件
  async fn read_blob(&self, blob_id: u64) -> Result<Bytes> {
    use compio::io::AsyncRead;

    let path = blob_path(&self.blob_dir, blob_id);
    if !jdb_fs::exists(&path) {
      return Err(Error::BlobNotFound(blob_id));
    }

    // Read with compio AsyncFd / 用 compio AsyncFd 读取
    let std_file = StdFile::open(&path)?;
    let len = std_file.metadata()?.len() as usize;
    let mut async_fd = AsyncFd::new(std_file)?;
    let buf = vec![0u8; len];
    let compio::buf::BufResult(res, buf) = async_fd.read(buf).await;
    let n = res?;
    if n != len {
      return Err(Error::Io(std::io::Error::new(
        std::io::ErrorKind::UnexpectedEof,
        "incomplete read",
      )));
    }
    Ok(Bytes::from(buf))
  }

  async fn open_file(&self, file_id: u64) -> Result<File> {
    let path = Self::file_path(&self.dir, file_id);
    if !jdb_fs::exists(&path) {
      return Err(Error::FileNotFound(file_id));
    }
    Ok(File::open(&path).await?)
  }

  /// Rotate to new file / 轮转到新文件
  pub async fn rotate(&self) -> Result<()> {
    let mut state = self.state.borrow_mut();
    self.rotate_inner(&mut state).await
  }

  async fn rotate_inner(&self, state: &mut VLogState) -> Result<()> {
    state.active.sync_data().await?;
    state.active_id += 1;
    let path = Self::file_path(&self.dir, state.active_id);
    state.active = File::create(&path).await?;
    state.active_size = 0;
    Ok(())
  }

  /// Sync to disk / 同步到磁盘
  pub async fn sync(&self) -> Result<()> {
    let state = self.state.borrow();
    state.active.sync_data().await?;
    Ok(())
  }

  /// Get active file id / 获取活跃文件 ID
  pub fn active_id(&self) -> u64 {
    self.state.borrow().active_id
  }

  /// Get active file size / 获取活跃文件大小
  pub fn active_size(&self) -> u64 {
    self.state.borrow().active_size
  }

  /// Update time cache / 更新时间缓存
  #[inline]
  pub fn update_time() {
    Clock::update();
  }

  /// Delete blob file (for GC) / 删除 blob 文件（用于 GC）
  pub async fn delete_blob(&self, blob_id: u64) -> Result<()> {
    let path = blob_path(&self.blob_dir, blob_id);
    if jdb_fs::exists(&path) {
      jdb_fs::remove(&path).await?;
    }
    Ok(())
  }
}

#[inline]
fn align_up(n: usize) -> usize {
  (n + PAGE_SIZE - 1) & !(PAGE_SIZE - 1)
}

fn crc32(data: &[u8]) -> u32 {
  let mut hasher = crc32fast::Hasher::new();
  hasher.update(data);
  hasher.finalize()
}
