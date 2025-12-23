#![cfg_attr(docsrs, feature(doc_cfg))]

use std::io::Read;

use thiserror::Error;

/// 压缩错误 Compression error
#[derive(Error, Debug)]
pub enum Error {
  #[error("lz4: {0}")]
  Lz4(#[from] lz4_flex::block::DecompressError),

  #[error("zstd: {0}")]
  Zstd(#[from] std::io::Error),

  #[error("unknown codec: {0}")]
  UnknownCodec(u8),
}

/// 压缩算法 Compression codec
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum Codec {
  #[default]
  None = 0,
  Lz4 = 1,
  Zstd = 2,
}

impl Codec {
  /// 从 u8 转换 Convert from u8
  #[inline]
  pub fn from_u8(v: u8) -> Result<Self, Error> {
    match v {
      0 => Ok(Self::None),
      1 => Ok(Self::Lz4),
      2 => Ok(Self::Zstd),
      _ => Err(Error::UnknownCodec(v)),
    }
  }
}

/// 压缩 Compress
#[inline]
pub fn enc(codec: Codec, src: &[u8]) -> Vec<u8> {
  match codec {
    Codec::None => src.to_vec(),
    Codec::Lz4 => lz4_flex::compress_prepend_size(src),
    Codec::Zstd => zstd::encode_all(src, 3).unwrap_or_default(),
  }
}

/// 解压 Decompress
#[inline]
pub fn dec(codec: Codec, src: &[u8]) -> Result<Vec<u8>, Error> {
  match codec {
    Codec::None => Ok(src.to_vec()),
    Codec::Lz4 => Ok(lz4_flex::decompress_size_prepended(src)?),
    Codec::Zstd => {
      let mut out = Vec::new();
      zstd::Decoder::new(src)?.read_to_end(&mut out)?;
      Ok(out)
    }
  }
}
