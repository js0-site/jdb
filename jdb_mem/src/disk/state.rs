use std::rc::Rc;

use futures::channel::oneshot::Receiver;
use jdb_base::{
  Discard,
  sst::{Meta, Sst},
};

use super::{Disk, ERR_DISCONN, ERR_SST, FlushRes, run::run};
use crate::{Map, log_err};

/// Task details for a running flush
/// 运行中的刷盘任务详情
pub struct Ing<S, D>
where
  S: Sst,
  D: Discard,
{
  /// Receiver for Disk and Meta after flush
  /// 刷盘完成后返回 Disk 和 元数据的接收器
  pub rx: Receiver<FlushRes<S, D>>,
}

/// State of the background flush task
/// 后台刷盘任务的状态
pub enum State<S, D>
where
  S: Sst,
  D: Discard,
{
  /// No task is running
  /// 没有任务在运行
  Idle(Disk<S, D>),
  /// A task is currently running
  /// 任务正在运行中
  Ing(Ing<S, D>),
  /// State is poisoned (task panic or moved out)
  /// 状态已中毒（任务恐慌或移出）
  Poisoned,
}

impl<S, D> State<S, D>
where
  S: Sst,
  D: Discard,
{
  /// Complete flush: push sst then remove freeze2 (sync, no await)
  /// 完成刷盘：先 push sst 再删除 freeze2（同步，无 await）
  #[inline]
  fn done(&mut self, mut disk: Disk<S, D>, meta: Meta, freeze2: &mut Option<Rc<Map>>) {
    disk.sst.push(meta);
    *freeze2 = None;
    *self = State::Idle(disk);
  }

  /// Handle error and transition to Idle
  /// 处理错误并切换到 Idle
  #[cold]
  fn fail(&mut self, disk: Disk<S, D>, msg: &str, err: Option<&dyn std::fmt::Debug>) {
    if let Some(e) = err {
      log_err(msg, e);
    } else {
      log::error!("{}", msg);
    }
    // Note: We do not remove the map from `freeze2` on failure, so it will retry on next flush.
    // 注意：失败时我们不从 `freeze2` 中移除 map，因此它会在下次 flush 时重试。
    *self = State::Idle(disk);
  }

  /// Check task status and trigger new flush if needed
  /// 检查任务状态并在需要时触发新的刷盘
  pub fn flush(&mut self, freeze2: &mut Option<Rc<Map>>) {
    loop {
      match self {
        State::Ing(ing) => {
          match ing.rx.try_recv() {
            Ok(Some((disk, res))) => match res {
              Ok(meta) => self.done(disk, meta, freeze2),
              Err(ref err) => self.fail(disk, ERR_SST, Some(err)),
            },
            // Still running
            // 仍在运行
            Ok(None) => return,
            // Channel closed (task panicked or dropped sender)
            // 通道关闭（任务 panic 或发送端被丢弃）
            Err(_) => {
              log::error!("{}", ERR_DISCONN);
              *self = State::Poisoned;
              return;
            }
          }
        }
        State::Idle(_) => {
          if let Some(map) = freeze2 {
            // Take disk out of State (replace with Poisoned temporarily)
            // 从 State 中取出 disk（暂时替换为 Poisoned）
            let old_state = std::mem::replace(self, State::Poisoned);
            if let State::Idle(disk) = old_state {
              let map = map.clone();
              let rx = run(disk, map);
              *self = State::Ing(Ing { rx });
            } else {
              unreachable!("state must be Idle");
            }
          } else {
            return;
          }
        }
        State::Poisoned => {
          // If we reach here, previous flush task died and lost the disk.
          // We cannot recover safely.
          // 如果到达这里，说明之前的刷盘任务挂了并且丢失了 disk。
          // 我们无法安全恢复。
          panic!("Disk state is poisoned");
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
    self.flush(freeze2);

    if let State::Ing(ing) = self {
      // Blocks current thread until task completes
      // 阻塞当前线程直到任务完成
      match (&mut ing.rx).await {
        Ok((disk, Ok(meta))) => {
          self.done(disk, meta, freeze2);
          Ok(())
        }
        Ok((disk, Err(err))) => {
          self.fail(disk, ERR_SST, Some(&err));
          // Return the SST error up
          Err(super::Error::Sst(err))
        }
        Err(_) => {
          log::error!("{}", ERR_DISCONN);
          *self = State::Poisoned;
          Err(super::Error::Disconnect)
        }
      }
    } else if let State::Poisoned = self {
      Err(super::Error::Poisoned)
    } else {
      Ok(())
    }
  }
}
