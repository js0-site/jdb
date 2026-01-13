use std::{cell::RefCell, rc::Rc};

use futures::channel::oneshot::{Receiver, channel};
use jdb_base::{Discard, sst::MemToSst};
use log::error;

use super::{Disk, FlushResult};
use crate::Map;

/// Start background flush operations.
/// Note on Design (Rc/RefCell + compio):
/// We use `Rc<RefCell<Disk>>` because `compio` is a thread-per-core runtime where tasks stay on the same thread.
/// This allows us to share mutable state without `Arc<Mutex>` overhead.
///
/// Safety:
/// We explicitly allow `await_holding_refcell_ref` because this function effectively acts as a "Logical Lock" on the Disk.
/// While the flush is running (awaiting I/O), we hold a mutable borrow of Disk.
/// This ensures exclusive access: no other task can modify Disk concurrently.
/// The `State` machine guarantees that we do not attempt to start another flush or access Disk while `Step::Ing`.
///
/// 开始后台刷盘操作。
/// 关于设计（Rc/RefCell + compio）的说明：
/// 我们使用 `Rc<RefCell<Disk>>`，因为 `compio` 是 thread-per-core 运行时，任务保留在同一线程上。
/// 这允许我们在没有 `Arc<Mutex>` 开销的情况下共享可变状态。
///
/// 安全性：
/// 我们显式允许 `await_holding_refcell_ref`，因为此函数实际上充当 Disk 的“逻辑锁”。
/// 当刷盘正在运行（等待 I/O）时，我们持有 Disk 的可变借用。
/// 这确保了独占访问：没有其他任务可以并发修改 Disk。
/// `State` 状态机保证我们在 `Step::Ing` 时不会尝试启动另一个刷盘或访问 Disk。
pub fn run<S, D>(disk: Rc<RefCell<Disk<S, D>>>, map: Rc<Map>) -> Receiver<FlushResult<S>>
where
  S: MemToSst,
  D: Discard,
{
  let (tx, rx) = channel();
  compio::runtime::spawn(
    async move {
      // Suppress clippy warning because we intentionally hold the borrow across await
      // to enforce exclusive access during the flush operation.
      // 抑制 clippy 警告，因为我们有意在 await 期间持有借用，以在刷盘操作期间强制执行独占访问。
      #![allow(clippy::await_holding_refcell_ref)]
      let res = {
        let mut disk = disk.borrow_mut();
        let Disk { sst, discard } = &mut *disk;

        // Process discarded entries before flushing
        // 在刷盘前处理丢弃的条目
        for (k, p) in &map.discards {
          discard.discard(k, p);
        }
        futures::join!(sst.write(map.iter()), discard.flush())
      };

      let (sst_res, discard_res) = res;

      if let Err(err) = discard_res {
        error!("discard flush error: {:?}", err);
      }

      // sst.push is done by caller synchronously with old.remove to ensure atomicity
      // sst.push 由调用方与 old.remove 同步执行以确保原子性

      // Ensure RefMut is dropped before sending (which wakes up the waiter)
      // 确保 RefMut 在发送（唤醒等待者）之前被丢弃
      if let Err(Err(err)) = tx.send(sst_res) {
        error!("mem send sst write error (receiver dropped): {:?}", err);
      }
    },
  )
  .detach();
  rx
}
