use std::{cell::RefCell, rc::Rc};

use futures::channel::oneshot::Receiver;
use jdb_base::{
  Discard,
  sst::{Meta, Sst},
};

use super::{Disk, ERR_DISCONN, ERR_SST, run::run};
use crate::Map;

/// Task details for a running flush
/// 运行中的刷盘任务详情
pub struct Ing<S, D>
where
  S: Sst,
  D: Discard,
{
  /// Shared disk instance
  /// 共享的磁盘实例
  pub disk: Rc<RefCell<Disk<S, D>>>,
  /// Receiver for Meta after flush
  /// 刷盘完成后返回元数据的接收器
  pub rx: Receiver<Result<Meta, S::Error>>,
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
  Idle(Rc<RefCell<Disk<S, D>>>),
  /// A task is currently running
  /// 任务正在运行中
  Ing(Ing<S, D>),
}

impl<S, D> State<S, D>
where
  S: Sst,
  D: Discard,
{
  /// Complete flush: push sst then remove old (sync, no await)
  /// 完成刷盘：先 push sst 再删除 old（同步，无 await）
  #[inline]
  fn done(&mut self, disk: Rc<RefCell<Disk<S, D>>>, meta: Meta, old: &mut [Option<Rc<Map>>; 2]) {
    disk.borrow_mut().sst.push(meta);
    old[0] = old[1].take();
    *self = State::Idle(disk);
  }

  /// Handle error and transition to Idle
  /// 处理错误并切换到 Idle
  #[cold]
  fn fail(&mut self, disk: Rc<RefCell<Disk<S, D>>>, msg: &str, err: Option<&dyn std::fmt::Debug>) {
    if let Some(e) = err {
      log::error!("{}: {:?}", msg, e);
    } else {
      log::error!("{}", msg);
    }
    *self = State::Idle(disk);
  }

  /// Check task status and trigger new flush if needed
  /// 检查任务状态并在需要时触发新的刷盘
  pub fn flush(&mut self, old: &mut [Option<Rc<Map>>; 2]) {
    loop {
      match self {
        State::Ing(ing) => {
          match ing.rx.try_recv() {
            Ok(Some(res)) => {
              let disk = ing.disk.clone();
              match res {
                Ok(meta) => self.done(disk, meta, old),
                Err(ref err) => self.fail(disk, ERR_SST, Some(err)),
              }
            }
            // Still running
            Ok(None) => return,
            // Channel closed (task panicked or dropped sender)
            Err(_) => {
              let disk = ing.disk.clone();
              self.fail(disk, ERR_DISCONN, None);
              return;
            }
          }
        }
        State::Idle(disk) => {
          if let Some(map) = &old[0] {
            // Get the OLDEST map WITHOUT removing it yet (to keep it queryable)
            // 获取最旧的 map 但不立即移除（保证其可查询性）
            let map = map.clone();
            let disk = disk.clone();
            let rx = run(disk.clone(), map);
            *self = State::Ing(Ing { disk, rx });
          }
          return;
        }
      }
    }
  }

  /// Block and wait for active flush to complete
  /// 阻塞并等待当前刷盘完成
  pub async fn wait(&mut self, old: &mut [Option<Rc<Map>>; 2]) {
    // Ensure task is running
    // 确保任务正在运行
    self.flush(old);

    if let State::Ing(ing) = self {
      let disk = ing.disk.clone();

      // Blocks current thread until task completes
      // 阻塞当前线程直到任务完成
      match (&mut ing.rx).await {
        Ok(Ok(meta)) => self.done(disk, meta, old),
        Ok(Err(ref err)) => self.fail(disk, ERR_SST, Some(err)),
        Err(_) => self.fail(disk, ERR_DISCONN, None),
      }
    }
  }
}
