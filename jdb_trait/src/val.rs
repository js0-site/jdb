use hipstr::{HipByt, HipStr};
use ordered_float::OrderedFloat;

/// 数据库原子值 Database atomic value
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Val {
  Bool(bool),
  I8(i8),
  I16(i16),
  I32(i32),
  I64(i64),
  I128(i128),
  U8(u8),
  U16(u16),
  U32(u32),
  U64(u64),
  U128(u128),
  F32(OrderedFloat<f32>),
  F64(OrderedFloat<f64>),
  Str(HipStr<'static>),
  Bin(HipByt<'static>),
}

// From trait implementations for convenient conversions
// From trait 实现以便于转换

macro_rules! impl_from_val {
  ($($src:ty => $variant:ident),+ $(,)?) => {
    $(
      impl From<$src> for Val {
        #[inline]
        fn from(v: $src) -> Self {
          Val::$variant(v.into())
        }
      }
    )+
  };
}

// Implement From for numeric types
// 为数值类型实现 From
impl_from_val! {
  bool => Bool,
  i8 => I8,
  i16 => I16,
  i32 => I32,
  i64 => I64,
  i128 => I128,
  u8 => U8,
  u16 => U16,
  u32 => U32,
  u64 => U64,
  u128 => U128,
}

// Implement From for float types with OrderedFloat wrapper
// 为浮点类型实现 From，使用 OrderedFloat 包装
impl From<f32> for Val {
  #[inline]
  fn from(v: f32) -> Self {
    Val::F32(OrderedFloat(v))
  }
}

impl From<f64> for Val {
  #[inline]
  fn from(v: f64) -> Self {
    Val::F64(OrderedFloat(v))
  }
}

// Implement From for string types
// 为字符串类型实现 From
impl From<&str> for Val {
  #[inline]
  fn from(v: &str) -> Self {
    Val::Str(v.into())
  }
}

impl From<String> for Val {
  #[inline]
  fn from(v: String) -> Self {
    Val::Str(v.into())
  }
}

impl From<HipStr<'static>> for Val {
  #[inline]
  fn from(v: HipStr<'static>) -> Self {
    Val::Str(v)
  }
}

// Implement From for binary types
// 为二进制类型实现 From
impl From<&[u8]> for Val {
  #[inline]
  fn from(v: &[u8]) -> Self {
    Val::Bin(v.into())
  }
}

impl From<Vec<u8>> for Val {
  #[inline]
  fn from(v: Vec<u8>) -> Self {
    Val::Bin(v.into())
  }
}

impl From<HipByt<'static>> for Val {
  #[inline]
  fn from(v: HipByt<'static>) -> Self {
    Val::Bin(v)
  }
}
