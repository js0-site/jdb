use std::{cell::RefCell, rc::Rc};

use futures::channel::oneshot::Receiver;
use jdb_base::{Discard, ckp::sst::Meta, sst::MemToSst};
use log::error;

use super::{Disk, FlushResult, run::run};
use crate::{Error, Map};

/// Flush task state enum
/// 刷盘任务状态枚举
enum Step<S: MemToSst> {
  /// Idle state, no task running
  /// 空闲状态，无任务运行
  Idle,
  /// Task is running with a receiver for the result
  /// 任务运行中，持有结果接收器
  Ing(Receiver<FlushResult<S>>),
}

/// State of the background flush task.
///
/// Design:
/// 1. `Rc<RefCell<Disk>>`: Shared ownership tailored for single-threaded async (compio).
///    Allows `State` to retain ownership of `Disk` even if the background task panics or fails, avoiding "lost disk" scenarios.
/// 2. `Step`: Tracks whether the flush is Idle or Running (Ing).
///
/// 后台刷盘任务的状态。
///
/// 设计：
/// 1. `Rc<RefCell<Disk>>`：专为单线程异步（compio）定制的共享所有权。
///    允许 `State` 在后台任务 panic 或失败时保留 `Disk` 所有权，避免“丢失磁盘”的情况。
/// 2. `Step`：跟踪刷盘是空闲（Idle）还是正在运行（Ing）。
pub struct State<S, D>
where
  S: MemToSst,
  D: Discard,
{
  disk: Rc<RefCell<Disk<S, D>>>,
  step: Step<S>,
}

impl<S, D> State<S, D>
where
  S: MemToSst,
  D: Discard,
{
  pub fn new(disk: Disk<S, D>) -> Self {
    Self {
      disk: Rc::new(RefCell::new(disk)),
      step: Step::<S>::Idle,
    }
  }

  /// Complete flush: push sst then remove freeze (sync, no await)
  /// 完成刷盘：先 push sst 再删除 freeze（同步，无 await）
  #[inline]
  fn done(&mut self, meta: Meta, freeze: &mut Option<Rc<Map>>) {
    self.disk.borrow_mut().sst.push(meta);
    *freeze = None;
    self.step = Step::<S>::Idle;
  }

  /// Handle error and transition to Idle
  /// 处理错误并切换到 Idle
  #[cold]
  fn fail(&mut self, err: &Error<S::Error>) {
    error!("{:?}", err);
    // Note: We do not remove the map from `freeze` on failure, so it will retry on next flush.
    // 注意：失败时我们不从 `freeze` 中移除 map，因此它会在下次 flush 时重试。
    // Disk logic is safe because RefCell borrow is dropped when task panics/finishes.
    self.step = Step::<S>::Idle;
  }

  /// Check task status and trigger new flush if needed
  /// 检查任务状态并在需要时触发新的刷盘
  pub fn flush(&mut self, freeze: &mut Option<Rc<Map>>) -> Result<(), Error<S::Error>> {
    loop {
      match &mut self.step {
        Step::Ing(rx) => {
          match rx.try_recv() {
            Ok(Some(res)) => match res {
              Ok(meta) => self.done(meta, freeze),
              Err(err) => {
                let e = Error::Sst(err);
                self.fail(&e);
              }
            },
            // Still running
            // 仍在运行
            Ok(None) => return Ok(()),
            // Channel closed (task panicked or dropped sender)
            // 通道关闭（任务 panic 或发送端被丢弃）
            Err(_) => {
              let e = Error::Disconnect;
              self.fail(&e);
              return Err(e);
            }
          }
        }
        Step::Idle => {
          if let Some(map) = freeze {
            let map = map.clone();
            let rx = run(self.disk.clone(), map);
            self.step = Step::Ing(rx);
          } else {
            return Ok(());
          }
        }
      }
    }
  }

  /// Block and wait for active flush to complete
  /// 阻塞并等待当前刷盘完成
  pub async fn wait(&mut self, freeze: &mut Option<Rc<Map>>) -> Result<(), Error<S::Error>> {
    // Ensure task is running
    // 确保任务正在运行
    self.flush(freeze)?;

    if let Step::Ing(rx) = &mut self.step {
      // Await until task completes
      // 等待任务完成
      match rx.await {
        Ok(Ok(meta)) => {
          self.done(meta, freeze);
          Ok(())
        }
        Ok(Err(err)) => {
          let e = Error::Sst(err);
          self.fail(&e);
          // Return the SST error up
          Err(e)
        }
        Err(_) => {
          let e = Error::Disconnect;
          self.fail(&e);
          Err(e)
        }
      }
    } else {
      Ok(())
    }
  }
}
