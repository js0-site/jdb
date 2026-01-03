//! # jdb_lock - File locking for JDB / JDB 文件锁
//!
//! Cross-process file locking using flock/fcntl.
//! 使用 flock/fcntl 实现跨进程文件锁。

pub mod error;
pub mod gc;
pub mod w;

use std::{fs, path::Path};

pub use error::{Error, Result};
use fd_lock::RwLock;

type Guard = fd_lock::RwLockWriteGuard<'static, fs::File>;

/// Write lock holder / 写锁持有者
pub(crate) struct FileLock {
  // SAFETY: _guard must be dropped before _lock. Rust drops fields in declaration order.
  // 安全：_guard 必须在 _lock 之前释放。Rust 按字段声明顺序 Drop。
  _guard: Guard,
  _lock: Box<RwLock<fs::File>>,
}

impl FileLock {
  /// Acquire write lock on file / 获取文件写锁
  pub fn try_new(file: fs::File) -> Result<Self> {
    let lock = Box::new(RwLock::new(file));
    // SAFETY: Box provides stable address, guard lives shorter than lock
    // 安全：Box 提供稳定地址，guard 生命周期短于 lock
    let lock_ptr: *mut RwLock<fs::File> = Box::into_raw(lock);
    let guard = unsafe { (*lock_ptr).try_write().map_err(|_| Error::Locked)? };
    // Transmute to 'static, safe because we control lifetime via struct
    // 转换为 'static，安全因为我们通过结构体控制生命周期
    let guard: Guard = unsafe { std::mem::transmute(guard) };
    let _lock = unsafe { Box::from_raw(lock_ptr) };
    Ok(Self {
      _guard: guard,
      _lock,
    })
  }
}

/// WAL file lock trait / WAL 文件锁 trait
pub trait WalLock: Default {
  /// Try acquire lock on WAL file / 尝试获取 WAL 文件锁
  fn try_lock(&mut self, path: &Path) -> Result<()>;
}

/// No-op lock (for GC WAL) / 空锁（用于 GC WAL）
#[derive(Default)]
pub struct NoLock;

impl WalLock for NoLock {
  #[inline(always)]
  fn try_lock(&mut self, _: &Path) -> Result<()> {
    Ok(())
  }
}
