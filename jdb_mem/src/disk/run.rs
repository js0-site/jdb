use std::{cell::RefCell, rc::Rc};

use futures::channel::oneshot::{Receiver, channel};
use jdb_base::{
  Discard,
  sst::{Meta, Sst},
};

use super::Disk;
use crate::Map;

#[cold]
fn log_err(msg: &str, err: impl std::fmt::Debug) {
  log::error!("{}: {:?}", msg, err);
}

/// Start background flush operations
/// 开始后台刷盘操作
#[allow(clippy::await_holding_refcell_ref)]
pub fn run<S, D>(disk: Rc<RefCell<Disk<S, D>>>, map: Rc<Map>) -> Receiver<Result<Meta, S::Error>>
where
  S: Sst,
  D: Discard,
{
  let (tx, rx) = channel();
  compio::runtime::spawn(async move {
    // Keep the mutable borrow within a constrained scope
    // 将可变借用保持在受限范围内
    let res = {
      // Safety: We hold the lock across the await point.
      // This is safe because:
      // 1. The task is spawned on a runtime that does not migrate tasks between threads (compio).
      // 2. The `State` machine ensures no other access to `disk` occurs while the flush task is running.
      // 安全性：我们在 await 点持有锁。
      // 这是安全的，因为：
      // 1. 任务是在不跨线程迁移任务的运行时 (compio) 上生成的。
      // 2. `State` 状态机确保在刷盘任务运行时不会发生对 `disk` 的其他访问。
      let mut disk = disk.borrow_mut();
      let Disk { sst, discard } = &mut *disk;

      // Process discarded entries before flushing
      // 在刷盘前处理丢弃的条目
      for (k, p) in &map.discard_li {
        discard.discard(k, p);
      }
      futures::join!(sst.write(map.iter()), discard.flush())
    };

    let (sst_res, discard_res) = res;

    if let Err(err) = discard_res {
      log_err("discard flush error", err);
    }

    // sst.push is done by caller synchronously with old.remove to ensure atomicity
    // sst.push 由调用方与 old.remove 同步执行以确保原子性

    if let Err(res) = tx.send(sst_res)
      && let Err(err) = res
    {
      log_err("mem send sst write error (receiver dropped)", err);
    }
  })
  .detach();
  rx
}
