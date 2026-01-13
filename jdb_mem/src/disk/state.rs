use std::{cell::RefCell, rc::Rc};

use futures::channel::oneshot::Receiver;
use jdb_base::{
  Discard,
  sst::{Meta, Sst},
};

use super::{Disk, ERR_DISCONN, ERR_SST, FlushResult, run::run};
use crate::{Map, log_err};

/// Task details for a running flush
/// 运行中的刷盘任务详情
pub struct Ing<S>
where
  S: Sst,
{
  /// Receiver for Meta after flush
  /// 刷盘完成后返回元数据的接收器
  pub rx: Receiver<FlushResult<S>>,
}

enum Step<S: Sst> {
  Idle,
  Ing(Ing<S>),
}

/// State of the background flush task
/// 后台刷盘任务的状态
pub struct State<S, D>
where
  S: Sst,
  D: Discard,
{
  disk: Rc<RefCell<Disk<S, D>>>,
  step: Step<S>,
}

impl<S, D> State<S, D>
where
  S: Sst,
  D: Discard,
{
  pub fn new(disk: Disk<S, D>) -> Self {
    Self {
      disk: Rc::new(RefCell::new(disk)),
      step: Step::<S>::Idle,
    }
  }

  /// Complete flush: push sst then remove freeze2 (sync, no await)
  /// 完成刷盘：先 push sst 再删除 freeze2（同步，无 await）
  #[inline]
  fn done(&mut self, meta: Meta, freeze2: &mut Option<Rc<Map>>) {
    self.disk.borrow_mut().sst.push(meta);
    *freeze2 = None;
    self.step = Step::<S>::Idle;
  }

  /// Handle error and transition to Idle
  /// 处理错误并切换到 Idle
  #[cold]
  fn fail(&mut self, msg: &str, err: Option<&dyn std::fmt::Debug>) {
    if let Some(e) = err {
      log_err(msg, e);
    } else {
      log::error!("{}", msg);
    }
    // Note: We do not remove the map from `freeze2` on failure, so it will retry on next flush.
    // 注意：失败时我们不从 `freeze2` 中移除 map，因此它会在下次 flush 时重试。
    self.step = Step::<S>::Idle;
  }

  /// Check task status and trigger new flush if needed
  /// 检查任务状态并在需要时触发新的刷盘
  pub fn flush(&mut self, freeze2: &mut Option<Rc<Map>>) -> Result<(), super::Error<S::Error>> {
    loop {
      match &mut self.step {
        Step::Ing(ing) => {
          match ing.rx.try_recv() {
            Ok(Some(res)) => match res {
              Ok(meta) => self.done(meta, freeze2),
              Err(ref err) => self.fail(ERR_SST, Some(err)),
            },
            // Still running
            // 仍在运行
            Ok(None) => return Ok(()),
            // Channel closed (task panicked or dropped sender)
            // 通道关闭（任务 panic 或发送端被丢弃）
            Err(_) => {
              self.fail(ERR_DISCONN, None);
              return Err(super::Error::Disconnect);
            }
          }
        }
        Step::Idle => {
          if let Some(map) = freeze2 {
            let map = map.clone();
            let rx = run(self.disk.clone(), map);
            self.step = Step::Ing(Ing { rx });
          } else {
            return Ok(());
          }
        }
      }
    }
  }

  /// Block and wait for active flush to complete
  /// 阻塞并等待当前刷盘完成
  pub async fn wait(
    &mut self,
    freeze2: &mut Option<Rc<Map>>,
  ) -> Result<(), super::Error<S::Error>> {
    // Ensure task is running
    // 确保任务正在运行
    self.flush(freeze2)?;

    if let Step::Ing(ing) = &mut self.step {
      // Blocks current thread until task completes
      // 阻塞当前线程直到任务完成
      match (&mut ing.rx).await {
        Ok(Ok(meta)) => {
          self.done(meta, freeze2);
          Ok(())
        }
        Ok(Err(err)) => {
          self.fail(ERR_SST, Some(&err));
          // Return the SST error up
          Err(super::Error::Sst(err))
        }
        Err(_) => {
          self.fail(ERR_DISCONN, None);
          Err(super::Error::Disconnect)
        }
      }
    } else {
      Ok(())
    }
  }
}
