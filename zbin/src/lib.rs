//! Binary data trait for compio async IO.
//!
//! compio uses io_uring/IOCP for async IO, which requires buffer ownership transfer.
//! The kernel holds the buffer during IO, so we must pass owned data, not references.
//!
//! # compio IoBuf constraint
//!
//! compio requires [`IoBuf`] trait for async write. Types that impl `IoBuf`:
//! - `Vec<u8>`, `Box<[u8]>`, `&'static [u8]`, `&'static str`
//! - `Rc<[u8]>`, `Arc<[u8]>` (zero-copy, compio native support)
//! - `Bytes` (with `bytes` feature)
//!
//! # Bin trait
//!
//! This trait bridges various input types to compio's ownership model:
//! - `as_slice()`: for sync operations (CRC calc, inline write)
//! - `io()`: transfer ownership to kernel, returns `Self::Io: IoBuf`
//!
//! # Zero-copy Table
//!
//! | Type | Io | Note |
//! |------|-----|------|
//! | `Vec<u8>`, `Box<[u8]>` | self | zero-copy |
//! | `Rc<[u8]>`, `Arc<[u8]>` | self | zero-copy (compio impl) |
//! | `Bytes` | self | zero-copy |
//! | `&Rc`, `&Arc`, `&Bytes` | clone | O(1) ref-count +1 |
//! | `String` | `Vec<u8>` | into_bytes() |
//! | `&[u8]`, `&str`, `&[u8; N]` | `Box<[u8]>` | copy |
//! | `Cow<[u8]>`, `Cow<str>` | `Vec<u8>` | into_owned |
//!
//! ---
//!
//! 用于 compio 异步 IO 的二进制数据 trait。
//!
//! compio 使用 io_uring/IOCP 实现异步 IO，需要转移缓冲区所有权。
//! 内核在 IO 期间持有缓冲区，因此必须传递拥有所有权的数据，而非引用。
//!
//! # compio IoBuf 约束
//!
//! compio 异步写入需要 [`IoBuf`] trait。实现了 `IoBuf` 的类型：
//! - `Vec<u8>`, `Box<[u8]>`, `&'static [u8]`, `&'static str`
//! - `Rc<[u8]>`, `Arc<[u8]>`（零拷贝，compio 原生支持）
//! - `Bytes`（需 `bytes` feature）
//!
//! # Bin trait
//!
//! 此 trait 将各种输入类型桥接到 compio 的所有权模型：
//! - `as_slice()`: 用于同步操作（CRC 计算、内联写入）
//! - `io()`: 将所有权转移给内核，返回 `Self::Io: IoBuf`
//!
//! # 零拷贝表
//!
//! | 类型 | Io | 说明 |
//! |------|-----|------|
//! | `Vec<u8>`, `Box<[u8]>` | self | 零拷贝 |
//! | `Rc<[u8]>`, `Arc<[u8]>` | self | 零拷贝（compio 实现）|
//! | `Bytes` | self | 零拷贝 |
//! | `&Rc`, `&Arc`, `&Bytes` | clone | O(1) 引用计数 +1 |
//! | `String` | `Vec<u8>` | into_bytes() |
//! | `&[u8]`, `&str`, `&[u8; N]` | `Box<[u8]>` | 拷贝 |
//! | `Cow<[u8]>`, `Cow<str>` | `Vec<u8>` | into_owned |

use std::{borrow::Cow, rc::Rc, sync::Arc};

#[cfg(feature = "bytes")]
use bytes::Bytes;
use compio::buf::IoBuf;

/// Binary data trait for async IO / 异步 IO 二进制数据 trait
///
/// `Io` is the owned type for compio async write (must impl [`IoBuf`])
/// `Io` 是用于 compio 异步写入的拥有所有权类型（必须实现 [`IoBuf`]）
pub trait Bin<'a> {
  /// Owned type for IO operations / IO 操作的拥有所有权类型
  type Io: IoBuf;

  /// Get slice reference / 获取切片引用
  fn as_slice(&self) -> &[u8];

  /// Convert to owned IO buffer (zero-copy if possible)
  /// 转换为拥有所有权的 IO 缓冲区（尽可能零拷贝）
  fn io(self) -> Self::Io;

  #[inline(always)]
  fn len(&self) -> usize {
    self.as_slice().len()
  }

  #[inline(always)]
  fn is_empty(&self) -> bool {
    self.as_slice().is_empty()
  }
}

macro_rules! impl_bin {
  ($($ty:ty, $io:ty, $slice:ident, $into:expr);* $(;)?) => {
    $(
      #[allow(clippy::borrowed_box)]
      impl<'a> Bin<'a> for $ty {
        type Io = $io;
        #[inline(always)]
        fn as_slice(&self) -> &[u8] { self.$slice() }
        #[inline(always)]
        fn io(self) -> $io { $into(self) }
      }
    )*
  };
}

impl_bin!(
  // Owned types (zero-copy) / 拥有所有权类型（零拷贝）
  Vec<u8>, Vec<u8>, as_ref, |s| s;
  Box<[u8]>, Box<[u8]>, as_ref, |s| s;
  Rc<[u8]>, Rc<[u8]>, as_ref, |s| s;
  Arc<[u8]>, Arc<[u8]>, as_ref, |s| s;
  String, Vec<u8>, as_bytes, String::into_bytes;

  // Reference to owned types / 拥有所有权类型的引用
  &'a Vec<u8>, Box<[u8]>, as_ref, |s: &Vec<u8>| s.as_slice().into();
  &'a Box<[u8]>, Box<[u8]>, as_ref, |s: &Box<[u8]>| (*s).clone();
  &'a Rc<[u8]>, Rc<[u8]>, as_ref, |s: &Rc<[u8]>| s.clone();
  &'a Arc<[u8]>, Arc<[u8]>, as_ref, |s: &Arc<[u8]>| s.clone();
  &'a String, Box<[u8]>, as_bytes, |s: &String| s.as_bytes().into();

  // Primitive references / 原始引用类型
  &'a [u8], Box<[u8]>, as_ref, |s: &[u8]| s.into();
  &'a str, Box<[u8]>, as_bytes, |s: &str| s.as_bytes().into();

  // Cow types / Cow 类型
  Cow<'a, [u8]>, Vec<u8>, as_ref, Cow::into_owned;
  Cow<'a, str>, Vec<u8>, as_bytes, |s: Cow<'_, str>| s.into_owned().into_bytes();
);

#[cfg(feature = "bytes")]
impl_bin!(
  Bytes, Bytes, as_ref, |s| s;
  &'a Bytes, Bytes, as_ref, |s: &Bytes| s.clone();
);

// Array reference (const generic) / 数组引用（常量泛型）
impl<'a, const N: usize> Bin<'a> for &'a [u8; N] {
  type Io = Box<[u8]>;
  #[inline(always)]
  fn as_slice(&self) -> &[u8] {
    *self
  }
  #[inline(always)]
  fn io(self) -> Box<[u8]> {
    self.as_slice().into()
  }
}

// Array reference (const generic) / 数组引用（常量泛型）
impl<'a, const N: usize> Bin<'a> for [u8; N] {
  type Io = Box<[u8]>;
  #[inline(always)]
  fn as_slice(&self) -> &[u8] {
    &self[..]
  }
  #[inline(always)]
  fn io(self) -> Box<[u8]> {
    self.into()
  }
}
