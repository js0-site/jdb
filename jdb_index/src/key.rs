//! 索引键编码 Index key encoding
//!
//! 支持可比较字节序的键编码
//! Supports comparable byte order key encoding

use std::cmp::Ordering;

use jdb_trait::Val;

/// 编码后的键 Encoded key
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Key(Vec<u8>);

impl Key {
  /// 从 Val 切片编码 Encode from Val slice
  pub fn encode(vals: &[Val]) -> Self {
    let mut buf = Vec::new();
    for val in vals {
      encode_val(val, &mut buf);
    }
    Self(buf)
  }

  /// 解码为 Val 向量 Decode to Val vector
  pub fn decode(&self) -> Vec<Val> {
    let mut vals = Vec::new();
    let mut pos = 0;
    while pos < self.0.len() {
      let (val, n) = decode_val(&self.0[pos..]);
      vals.push(val);
      pos += n;
    }
    vals
  }

  #[inline]
  pub fn as_bytes(&self) -> &[u8] {
    &self.0
  }

  #[inline]
  pub fn from_bytes(bytes: Vec<u8>) -> Self {
    Self(bytes)
  }

  #[inline]
  pub fn len(&self) -> usize {
    self.0.len()
  }

  #[inline]
  pub fn is_empty(&self) -> bool {
    self.0.is_empty()
  }
}

impl Ord for Key {
  fn cmp(&self, other: &Self) -> Ordering {
    self.0.cmp(&other.0)
  }
}

impl PartialOrd for Key {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

/// 类型标签 Type tags
mod tag {
  pub const BOOL_FALSE: u8 = 0x01;
  pub const BOOL_TRUE: u8 = 0x02;
  pub const I8: u8 = 0x10;
  pub const I16: u8 = 0x11;
  pub const I32: u8 = 0x12;
  pub const I64: u8 = 0x13;
  pub const I128: u8 = 0x14;
  pub const U8: u8 = 0x20;
  pub const U16: u8 = 0x21;
  pub const U32: u8 = 0x22;
  pub const U64: u8 = 0x23;
  pub const U128: u8 = 0x24;
  pub const F32: u8 = 0x30;
  pub const F64: u8 = 0x31;
  pub const STR: u8 = 0x40;
  pub const BIN: u8 = 0x50;
}

/// 编码单个 Val Encode single Val
fn encode_val(val: &Val, buf: &mut Vec<u8>) {
  match val {
    Val::Bool(false) => buf.push(tag::BOOL_FALSE),
    Val::Bool(true) => buf.push(tag::BOOL_TRUE),
    Val::I8(v) => {
      buf.push(tag::I8);
      buf.push((*v as u8) ^ 0x80);
    }
    Val::I16(v) => {
      buf.push(tag::I16);
      buf.extend_from_slice(&((*v as u16) ^ 0x8000).to_be_bytes());
    }
    Val::I32(v) => {
      buf.push(tag::I32);
      buf.extend_from_slice(&((*v as u32) ^ 0x8000_0000).to_be_bytes());
    }
    Val::I64(v) => {
      buf.push(tag::I64);
      buf.extend_from_slice(&((*v as u64) ^ 0x8000_0000_0000_0000).to_be_bytes());
    }
    Val::I128(v) => {
      buf.push(tag::I128);
      buf.extend_from_slice(&((*v as u128) ^ (1u128 << 127)).to_be_bytes());
    }
    Val::U8(v) => {
      buf.push(tag::U8);
      buf.push(*v);
    }
    Val::U16(v) => {
      buf.push(tag::U16);
      buf.extend_from_slice(&v.to_be_bytes());
    }
    Val::U32(v) => {
      buf.push(tag::U32);
      buf.extend_from_slice(&v.to_be_bytes());
    }
    Val::U64(v) => {
      buf.push(tag::U64);
      buf.extend_from_slice(&v.to_be_bytes());
    }
    Val::U128(v) => {
      buf.push(tag::U128);
      buf.extend_from_slice(&v.to_be_bytes());
    }
    Val::F32(v) => {
      buf.push(tag::F32);
      let bits = v.0.to_bits();
      let cmp = if bits & 0x8000_0000 != 0 { !bits } else { bits ^ 0x8000_0000 };
      buf.extend_from_slice(&cmp.to_be_bytes());
    }
    Val::F64(v) => {
      buf.push(tag::F64);
      let bits = v.0.to_bits();
      let cmp = if bits & 0x8000_0000_0000_0000 != 0 { !bits } else { bits ^ 0x8000_0000_0000_0000 };
      buf.extend_from_slice(&cmp.to_be_bytes());
    }
    Val::Str(s) => {
      buf.push(tag::STR);
      let bytes = s.as_bytes();
      buf.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
      buf.extend_from_slice(bytes);
    }
    Val::Bin(b) => {
      buf.push(tag::BIN);
      buf.extend_from_slice(&(b.len() as u32).to_be_bytes());
      buf.extend_from_slice(b);
    }
  }
}

/// 安全读取字节数组 Safe read bytes
#[inline]
fn read_bytes<const N: usize>(buf: &[u8], off: usize) -> [u8; N] {
  let mut arr = [0u8; N];
  let end = (off + N).min(buf.len());
  let len = end.saturating_sub(off);
  arr[..len].copy_from_slice(&buf[off..end]);
  arr
}

/// 解码单个 Val Decode single Val
fn decode_val(buf: &[u8]) -> (Val, usize) {
  if buf.is_empty() {
    return (Val::Bool(false), 0);
  }

  let t = buf[0];
  match t {
    tag::BOOL_FALSE => (Val::Bool(false), 1),
    tag::BOOL_TRUE => (Val::Bool(true), 1),
    tag::I8 => {
      let v = buf.get(1).copied().unwrap_or(0) ^ 0x80;
      (Val::I8(v as i8), 2)
    }
    tag::I16 => {
      let v = u16::from_be_bytes(read_bytes(buf, 1)) ^ 0x8000;
      (Val::I16(v as i16), 3)
    }
    tag::I32 => {
      let v = u32::from_be_bytes(read_bytes(buf, 1)) ^ 0x8000_0000;
      (Val::I32(v as i32), 5)
    }
    tag::I64 => {
      let v = u64::from_be_bytes(read_bytes(buf, 1)) ^ 0x8000_0000_0000_0000;
      (Val::I64(v as i64), 9)
    }
    tag::I128 => {
      let v = u128::from_be_bytes(read_bytes(buf, 1)) ^ (1u128 << 127);
      (Val::I128(v as i128), 17)
    }
    tag::U8 => {
      let v = buf.get(1).copied().unwrap_or(0);
      (Val::U8(v), 2)
    }
    tag::U16 => {
      let v = u16::from_be_bytes(read_bytes(buf, 1));
      (Val::U16(v), 3)
    }
    tag::U32 => {
      let v = u32::from_be_bytes(read_bytes(buf, 1));
      (Val::U32(v), 5)
    }
    tag::U64 => {
      let v = u64::from_be_bytes(read_bytes(buf, 1));
      (Val::U64(v), 9)
    }
    tag::U128 => {
      let v = u128::from_be_bytes(read_bytes(buf, 1));
      (Val::U128(v), 17)
    }
    tag::F32 => {
      let cmp = u32::from_be_bytes(read_bytes(buf, 1));
      let bits = if cmp & 0x8000_0000 != 0 { cmp ^ 0x8000_0000 } else { !cmp };
      (Val::F32(f32::from_bits(bits).into()), 5)
    }
    tag::F64 => {
      let cmp = u64::from_be_bytes(read_bytes(buf, 1));
      let bits = if cmp & 0x8000_0000_0000_0000 != 0 { cmp ^ 0x8000_0000_0000_0000 } else { !cmp };
      (Val::F64(f64::from_bits(bits).into()), 9)
    }
    tag::STR => {
      let len = u32::from_be_bytes(read_bytes(buf, 1)) as usize;
      let end = (5 + len).min(buf.len());
      let s = std::str::from_utf8(&buf[5..end]).unwrap_or_default();
      (Val::Str(s.into()), 5 + len)
    }
    tag::BIN => {
      let len = u32::from_be_bytes(read_bytes(buf, 1)) as usize;
      let end = (5 + len).min(buf.len());
      let b = &buf[5..end];
      (Val::Bin(b.into()), 5 + len)
    }
    _ => (Val::Bool(false), 1),
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_encode_decode_i64() {
    let vals = vec![Val::I64(-100), Val::I64(0), Val::I64(100)];
    let key = Key::encode(&vals);
    let decoded = key.decode();
    assert_eq!(vals, decoded);
  }

  #[test]
  fn test_encode_decode_str() {
    let vals = vec![Val::Str("hello".into()), Val::U32(42)];
    let key = Key::encode(&vals);
    let decoded = key.decode();
    assert_eq!(vals, decoded);
  }

  #[test]
  fn test_key_ordering() {
    let k1 = Key::encode(&[Val::I64(-10)]);
    let k2 = Key::encode(&[Val::I64(0)]);
    let k3 = Key::encode(&[Val::I64(10)]);

    assert!(k1 < k2);
    assert!(k2 < k3);
  }

  #[test]
  fn test_string_ordering() {
    let k1 = Key::encode(&[Val::Str("aaa".into())]);
    let k2 = Key::encode(&[Val::Str("bbb".into())]);
    let k3 = Key::encode(&[Val::Str("ccc".into())]);

    assert!(k1 < k2);
    assert!(k2 < k3);
  }

  #[test]
  fn test_empty_key() {
    let key = Key::encode(&[]);
    assert!(key.is_empty());
    assert_eq!(key.decode(), vec![]);
  }

  #[test]
  fn test_all_types() {
    let vals = vec![
      Val::Bool(false),
      Val::Bool(true),
      Val::I8(-128),
      Val::I8(127),
      Val::I16(-32768),
      Val::I16(32767),
      Val::I32(i32::MIN),
      Val::I32(i32::MAX),
      Val::I64(i64::MIN),
      Val::I64(i64::MAX),
      Val::U8(0),
      Val::U8(255),
      Val::U16(0),
      Val::U16(65535),
      Val::U32(0),
      Val::U32(u32::MAX),
      Val::U64(0),
      Val::U64(u64::MAX),
      Val::F32((-1.5f32).into()),
      Val::F32((0.0f32).into()),
      Val::F32((1.5f32).into()),
      Val::F64((-1.5f64).into()),
      Val::F64((0.0f64).into()),
      Val::F64((1.5f64).into()),
      Val::Str("".into()),
      Val::Str("test".into()),
      Val::Bin(vec![].into()),
      Val::Bin(vec![1, 2, 3].into()),
    ];
    let key = Key::encode(&vals);
    let decoded = key.decode();
    assert_eq!(vals, decoded);
  }
}
