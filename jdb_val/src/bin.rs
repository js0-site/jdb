//! Binary data wrapper for zero-copy optimization
//! 二进制数据包装器，用于零拷贝优化

use std::borrow::Cow;
use std::rc::Rc;
use std::sync::Arc;

use compio::buf::IoBuf;
use hipstr::{HipByt, HipStr};

#[cfg(feature = "bytes")]
use bytes::Bytes;

/// Trait for binary data input / 二进制数据输入 trait
///
/// `Io` is the owned type for async IO (must impl IoBuf for compio)
/// `Io` 是用于异步 IO 的拥有所有权类型（必须实现 IoBuf）
pub trait Bin<'a> {
  /// Owned type for IO operations / IO 操作的拥有所有权类型
  type Io: IoBuf;

  /// Get slice reference / 获取切片引用
  fn as_slice(&self) -> &[u8];

  /// Convert to owned IO buffer (zero-copy if possible)
  /// 转换为拥有所有权的 IO 缓冲区（尽可能零拷贝）
  fn into_io(self) -> Self::Io;

  #[inline(always)]
  fn len(&self) -> usize {
    self.as_slice().len()
  }

  #[inline(always)]
  fn is_empty(&self) -> bool {
    self.as_slice().is_empty()
  }
}

/// Impl Bin for multiple types / 为多个类型实现 Bin
macro_rules! impl_into_bin {
  ($($ty:ty, $io:ty, $slice:ident, $into:expr);* $(;)?) => {
    $(
      impl<'a> Bin<'a> for $ty {
        type Io = $io;
        #[inline(always)]
        fn as_slice(&self) -> &[u8] { self.$slice() }
        #[inline(always)]
        fn into_io(self) -> $io { $into(self) }
      }
    )*
  };
}

impl_into_bin!(
  // Owned types (zero-copy) / 拥有所有权类型（零拷贝）
  Vec<u8>, Vec<u8>, as_ref, |s| s;
  Box<[u8]>, Box<[u8]>, as_ref, |s| s;
  Rc<[u8]>, Rc<[u8]>, as_ref, |s| s;
  Arc<[u8]>, Arc<[u8]>, as_ref, |s| s;
  String, Vec<u8>, as_bytes, String::into_bytes;
  HipByt<'static>, Vec<u8>, as_ref, |s: HipByt<'static>| s.into_vec().unwrap_or_else(|h| h.to_vec());
  HipStr<'static>, Vec<u8>, as_bytes, |s: HipStr<'static>| s.into_bytes().into_vec().unwrap_or_else(|h| h.to_vec());

  // Reference to owned types / 拥有所有权类型的引用
  &'a Vec<u8>, Vec<u8>, as_ref, |s: &Vec<u8>| s.clone();
  &'a Box<[u8]>, Box<[u8]>, as_ref, |s: &Box<[u8]>| s.clone();
  &'a Rc<[u8]>, Rc<[u8]>, as_ref, |s: &Rc<[u8]>| s.clone();
  &'a Arc<[u8]>, Arc<[u8]>, as_ref, |s: &Arc<[u8]>| s.clone();
  &'a String, Box<[u8]>, as_bytes, |s: &String| s.as_bytes().into();
  &'a HipByt<'static>, Box<[u8]>, as_ref, |s: &HipByt<'static>| s.as_slice().into();
  &'a HipStr<'static>, Box<[u8]>, as_bytes, |s: &HipStr<'static>| s.as_bytes().into();

  // Primitive references / 原始引用类型
  &'a [u8], Box<[u8]>, as_ref, |s: &[u8]| s.into();
  &'a str, Box<[u8]>, as_bytes, |s: &str| s.as_bytes().into();

  // Cow types / Cow 类型
  Cow<'a, [u8]>, Vec<u8>, as_ref, Cow::into_owned;
  Cow<'a, str>, Vec<u8>, as_bytes, |s: Cow<'_, str>| s.into_owned().into_bytes();
);

#[cfg(feature = "bytes")]
impl_into_bin!(
  Bytes, Bytes, as_ref, |s| s;
  &'a Bytes, Bytes, as_ref, |s: &Bytes| s.clone();
);

// Array reference (const generic) / 数组引用（常量泛型）
impl<'a, const N: usize> Bin<'a> for &'a [u8; N] {
  type Io = Box<[u8]>;
  #[inline(always)]
  fn as_slice(&self) -> &[u8] { *self }
  #[inline(always)]
  fn into_io(self) -> Box<[u8]> { self.as_slice().into() }
}
