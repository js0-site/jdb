//! Manifest - LSM-Tree level metadata persistence
//! 清单 - LSM-Tree 层级元数据持久化
//!
//! Tracks SSTable files per level and supports atomic updates.
//! 跟踪每个层级的 SSTable 文件并支持原子更新。

use std::path::{Path, PathBuf};

use crc32fast::Hasher;
use jdb_base::{open_read, read_all, write_file};

use crate::Result;

/// Manifest file magic number
/// 清单文件魔数
const MANIFEST_MAGIC: u32 = 0x4A44424D; // "JDBM"

/// Manifest format version
/// 清单格式版本
const MANIFEST_VERSION: u8 = 1;

/// SSTable entry in manifest
/// 清单中的 SSTable 条目
#[derive(Debug, Clone)]
pub struct TableEntry {
  /// Table ID
  /// 表 ID
  pub id: u64,
  /// Minimum key
  /// 最小键
  pub min_key: Box<[u8]>,
  /// Maximum key
  /// 最大键
  pub max_key: Box<[u8]>,
  /// Item count
  /// 条目数量
  pub item_count: u64,
  /// File size
  /// 文件大小
  pub file_size: u64,
}

/// Level metadata in manifest
/// 清单中的层级元数据
#[derive(Debug, Clone, Default)]
pub struct LevelMeta {
  /// Level number
  /// 层级编号
  pub level: usize,
  /// Tables in this level
  /// 此层级的表
  pub tables: Vec<TableEntry>,
}

/// Manifest for LSM-Tree metadata
/// LSM-Tree 元数据清单
#[derive(Debug, Clone)]
pub struct Manifest {
  /// Manifest version (monotonically increasing)
  /// 清单版本（单调递增）
  pub version: u64,
  /// Current sequence number
  /// 当前序列号
  pub seqno: u64,
  /// Next table ID
  /// 下一个表 ID
  pub next_table_id: u64,
  /// Level metadata
  /// 层级元数据
  pub levels: Vec<LevelMeta>,
}

impl Default for Manifest {
  fn default() -> Self {
    Self::new()
  }
}

impl Manifest {
  /// Create new empty manifest
  /// 创建新的空清单
  pub fn new() -> Self {
    Self {
      version: 0,
      seqno: 0,
      next_table_id: 1,
      levels: vec![LevelMeta::default()], // Start with L0
    }
  }

  /// Get level count
  /// 获取层级数量
  #[inline]
  pub fn level_count(&self) -> usize {
    self.levels.len()
  }

  /// Get level metadata
  /// 获取层级元数据
  #[inline]
  pub fn level(&self, n: usize) -> Option<&LevelMeta> {
    self.levels.get(n)
  }

  /// Get mutable level metadata
  /// 获取可变层级元数据
  #[inline]
  pub fn level_mut(&mut self, n: usize) -> Option<&mut LevelMeta> {
    self.levels.get_mut(n)
  }

  /// Ensure level exists
  /// 确保层级存在
  pub fn ensure_level(&mut self, n: usize) {
    while self.levels.len() <= n {
      let level = self.levels.len();
      self.levels.push(LevelMeta {
        level,
        tables: Vec::new(),
      });
    }
  }

  /// Add table to level
  /// 添加表到层级
  pub fn add_table(&mut self, level: usize, entry: TableEntry) {
    self.ensure_level(level);
    self.levels[level].tables.push(entry);
    self.version += 1;
  }

  /// Remove table from level
  /// 从层级移除表
  pub fn remove_table(&mut self, level: usize, table_id: u64) -> bool {
    if let Some(level_meta) = self.levels.get_mut(level) {
      let len_before = level_meta.tables.len();
      level_meta.tables.retain(|t| t.id != table_id);
      if level_meta.tables.len() < len_before {
        self.version += 1;
        return true;
      }
    }
    false
  }

  /// Get all table IDs
  /// 获取所有表 ID
  pub fn all_table_ids(&self) -> Vec<u64> {
    self
      .levels
      .iter()
      .flat_map(|l| l.tables.iter().map(|t| t.id))
      .collect()
  }

  /// Encode manifest to bytes
  /// 将清单编码为字节
  pub fn encode(&self) -> Vec<u8> {
    let mut buf = Vec::new();

    // Header: magic (4) + version (1) + reserved (3)
    // 头部：魔数 (4) + 版本 (1) + 保留 (3)
    buf.extend_from_slice(&MANIFEST_MAGIC.to_le_bytes());
    buf.push(MANIFEST_VERSION);
    buf.extend_from_slice(&[0u8; 3]); // Reserved

    // Manifest version (8)
    // 清单版本 (8)
    buf.extend_from_slice(&self.version.to_le_bytes());

    // Seqno (8)
    // 序列号 (8)
    buf.extend_from_slice(&self.seqno.to_le_bytes());

    // Next table ID (8)
    // 下一个表 ID (8)
    buf.extend_from_slice(&self.next_table_id.to_le_bytes());

    // Level count (1)
    // 层级数量 (1)
    buf.push(self.levels.len() as u8);

    // Levels
    // 层级
    for level in &self.levels {
      // Table count (4)
      // 表数量 (4)
      buf.extend_from_slice(&(level.tables.len() as u32).to_le_bytes());

      // Tables
      // 表
      for table in &level.tables {
        // Table ID (8)
        // 表 ID (8)
        buf.extend_from_slice(&table.id.to_le_bytes());

        // Min key length (2) + min key
        // 最小键长度 (2) + 最小键
        buf.extend_from_slice(&(table.min_key.len() as u16).to_le_bytes());
        buf.extend_from_slice(&table.min_key);

        // Max key length (2) + max key
        // 最大键长度 (2) + 最大键
        buf.extend_from_slice(&(table.max_key.len() as u16).to_le_bytes());
        buf.extend_from_slice(&table.max_key);

        // Item count (8)
        // 条目数量 (8)
        buf.extend_from_slice(&table.item_count.to_le_bytes());

        // File size (8)
        // 文件大小 (8)
        buf.extend_from_slice(&table.file_size.to_le_bytes());
      }
    }

    // Checksum (4)
    // 校验和 (4)
    let mut hasher = Hasher::new();
    hasher.update(&buf);
    let checksum = hasher.finalize();
    buf.extend_from_slice(&checksum.to_le_bytes());

    buf
  }

  /// Decode manifest from bytes
  /// 从字节解码清单
  pub fn decode(data: &[u8]) -> Result<Self> {
    if data.len() < 36 {
      // Minimum: header(8) + version(8) + seqno(8) + next_id(8) + level_count(1) + checksum(4)
      return Err(crate::Error::Corruption {
        msg: "Manifest too small".into(),
      });
    }

    // Verify checksum first
    // 先验证校验和
    let checksum_offset = data.len() - 4;
    let stored_checksum = u32::from_le_bytes([
      data[checksum_offset],
      data[checksum_offset + 1],
      data[checksum_offset + 2],
      data[checksum_offset + 3],
    ]);

    let mut hasher = Hasher::new();
    hasher.update(&data[..checksum_offset]);
    let computed_checksum = hasher.finalize();

    if stored_checksum != computed_checksum {
      return Err(crate::Error::Corruption {
        msg: format!(
          "Manifest checksum mismatch: expected {stored_checksum}, got {computed_checksum}"
        ),
      });
    }

    let mut pos = 0;

    // Header
    // 头部
    let magic = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
    pos += 4;

    if magic != MANIFEST_MAGIC {
      return Err(crate::Error::Corruption {
        msg: format!("Invalid manifest magic: {magic:#x}"),
      });
    }

    let format_version = data[pos];
    pos += 1;

    if format_version != MANIFEST_VERSION {
      return Err(crate::Error::Corruption {
        msg: format!("Unsupported manifest version: {format_version}"),
      });
    }

    pos += 3; // Skip reserved

    // Manifest version
    // 清单版本
    let version = u64::from_le_bytes([
      data[pos],
      data[pos + 1],
      data[pos + 2],
      data[pos + 3],
      data[pos + 4],
      data[pos + 5],
      data[pos + 6],
      data[pos + 7],
    ]);
    pos += 8;

    // Seqno
    // 序列号
    let seqno = u64::from_le_bytes([
      data[pos],
      data[pos + 1],
      data[pos + 2],
      data[pos + 3],
      data[pos + 4],
      data[pos + 5],
      data[pos + 6],
      data[pos + 7],
    ]);
    pos += 8;

    // Next table ID
    // 下一个表 ID
    let next_table_id = u64::from_le_bytes([
      data[pos],
      data[pos + 1],
      data[pos + 2],
      data[pos + 3],
      data[pos + 4],
      data[pos + 5],
      data[pos + 6],
      data[pos + 7],
    ]);
    pos += 8;

    // Level count
    // 层级数量
    let level_count = data[pos] as usize;
    pos += 1;

    // Levels
    // 层级
    let mut levels = Vec::with_capacity(level_count);
    for level_idx in 0..level_count {
      if pos + 4 > checksum_offset {
        return Err(crate::Error::Corruption {
          msg: "Manifest truncated at level".into(),
        });
      }

      let table_count =
        u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
      pos += 4;

      let mut tables = Vec::with_capacity(table_count);
      for _ in 0..table_count {
        if pos + 8 > checksum_offset {
          return Err(crate::Error::Corruption {
            msg: "Manifest truncated at table".into(),
          });
        }

        // Table ID
        // 表 ID
        let id = u64::from_le_bytes([
          data[pos],
          data[pos + 1],
          data[pos + 2],
          data[pos + 3],
          data[pos + 4],
          data[pos + 5],
          data[pos + 6],
          data[pos + 7],
        ]);
        pos += 8;

        // Min key
        // 最小键
        if pos + 2 > checksum_offset {
          return Err(crate::Error::Corruption {
            msg: "Manifest truncated at min_key length".into(),
          });
        }
        let min_key_len = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;

        if pos + min_key_len > checksum_offset {
          return Err(crate::Error::Corruption {
            msg: "Manifest truncated at min_key".into(),
          });
        }
        let min_key: Box<[u8]> = data[pos..pos + min_key_len].into();
        pos += min_key_len;

        // Max key
        // 最大键
        if pos + 2 > checksum_offset {
          return Err(crate::Error::Corruption {
            msg: "Manifest truncated at max_key length".into(),
          });
        }
        let max_key_len = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;

        if pos + max_key_len > checksum_offset {
          return Err(crate::Error::Corruption {
            msg: "Manifest truncated at max_key".into(),
          });
        }
        let max_key: Box<[u8]> = data[pos..pos + max_key_len].into();
        pos += max_key_len;

        // Item count
        // 条目数量
        if pos + 8 > checksum_offset {
          return Err(crate::Error::Corruption {
            msg: "Manifest truncated at item_count".into(),
          });
        }
        let item_count = u64::from_le_bytes([
          data[pos],
          data[pos + 1],
          data[pos + 2],
          data[pos + 3],
          data[pos + 4],
          data[pos + 5],
          data[pos + 6],
          data[pos + 7],
        ]);
        pos += 8;

        // File size
        // 文件大小
        if pos + 8 > checksum_offset {
          return Err(crate::Error::Corruption {
            msg: "Manifest truncated at file_size".into(),
          });
        }
        let file_size = u64::from_le_bytes([
          data[pos],
          data[pos + 1],
          data[pos + 2],
          data[pos + 3],
          data[pos + 4],
          data[pos + 5],
          data[pos + 6],
          data[pos + 7],
        ]);
        pos += 8;

        tables.push(TableEntry {
          id,
          min_key,
          max_key,
          item_count,
          file_size,
        });
      }

      levels.push(LevelMeta {
        level: level_idx,
        tables,
      });
    }

    Ok(Self {
      version,
      seqno,
      next_table_id,
      levels,
    })
  }
}

/// Manifest file name
/// 清单文件名
const MANIFEST_FILE: &str = "MANIFEST";

/// Temporary manifest file name
/// 临时清单文件名
const MANIFEST_TMP: &str = "MANIFEST.tmp";

/// Load manifest from directory
/// 从目录加载清单
///
/// Returns None if manifest doesn't exist.
/// 如果清单不存在则返回 None。
pub async fn load_manifest(dir: &Path) -> Result<Option<Manifest>> {
  let path = dir.join(MANIFEST_FILE);

  // Check if file exists
  // 检查文件是否存在
  match compio::fs::metadata(&path).await {
    Ok(_) => {}
    Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
    Err(e) => return Err(e.into()),
  }

  // Read file
  // 读取文件
  let file = open_read(&path).await?;
  let meta = file.metadata().await?;
  let size = meta.len();

  let data = read_all(&file, size).await?;

  // Decode
  // 解码
  let manifest = Manifest::decode(&data)?;
  Ok(Some(manifest))
}

/// Save manifest to directory atomically
/// 原子地保存清单到目录
///
/// Writes to temporary file first, then renames.
/// 先写入临时文件，然后重命名。
pub async fn save_manifest(dir: &Path, manifest: &Manifest) -> Result<()> {
  let tmp_path = dir.join(MANIFEST_TMP);
  let final_path = dir.join(MANIFEST_FILE);

  // Encode and write to temporary file
  // 编码并写入临时文件
  let data = manifest.encode();
  write_file(&tmp_path, &data).await?;

  // Atomic rename
  // 原子重命名
  compio::fs::rename(&tmp_path, &final_path).await?;

  // Sync directory (important for durability on Unix)
  // 同步目录（在 Unix 上对持久性很重要）
  #[cfg(unix)]
  {
    if let Ok(dir_file) = compio::fs::File::open(dir).await {
      let _ = dir_file.sync_all().await;
    }
  }

  Ok(())
}

/// Get manifest file path
/// 获取清单文件路径
#[inline]
#[allow(dead_code)]
pub fn manifest_path(dir: &Path) -> PathBuf {
  dir.join(MANIFEST_FILE)
}
