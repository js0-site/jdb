use std::{cell::RefCell, rc::Rc};

use jdb_base::{
  Discard,
  sst::{Meta, Sst},
};
use oneshot::{Receiver, TryRecvError, channel};

use crate::Map;

/// Disk handler for SST and Discard operations
/// SST 和 Discard 操作的磁盘处理器
pub struct Disk<S, D> {
  /// SST writer implementation
  /// SST 写入器实现
  pub sst: S,
  /// Discard manager implementation
  /// 丢弃管理器实现
  pub discard: D,
}

impl<S: Sst, D: Discard> Disk<S, D> {
  /// Create a new Disk handler
  /// 创建新的磁盘处理器
  pub const fn new(sst: S, discard: D) -> Self {
    Self { sst, discard }
  }

  /// Start background flush operations
  /// 开始后台刷盘操作
  pub fn run(this: Rc<RefCell<Self>>, map: Rc<Map>) -> Receiver<Meta> {
    let (tx, rx) = channel();
    compio::spawn(async move {
      // Use a block to constrain the lifetime of the mutable borrow
      // 使用代码块限制可变借用的生命周期
      let res = {
        let mut disk = this.borrow_mut();
        // Assume compio::task::spawn supports !Send futures (since Rc is used)
        // Holding RefMut across await is safe here as no one else borrows it
        futures::join!(disk.sst.write(map.iter()), disk.discard.flush())
      };
      
      let (sst_res, discard_res) = res;
      let mut meta = Meta::default();

      if let Err(err) = sst_res {
        log::error!("sst write error: {:?}", err);
      } else if let Ok(m) = sst_res {
        meta = m;
        // Re-borrow mutably to update internal state
        // 重新获取可变借用以更新内部状态
        this.borrow_mut().sst.push(meta.clone());
      }

      if let Err(err) = discard_res {
        log::error!("discard flush error: {:?}", err);
      }

      let _ = tx.send(meta);
    });
    rx
  }
}

/// Task details for a running flush
/// 运行中的刷盘任务详情
pub struct Ing<S, D> {
  /// Shared disk instance
  /// 共享的磁盘实例
  pub disk: Rc<RefCell<Disk<S, D>>>,
  /// Receiver for Meta after flush
  /// 刷盘完成后返回元数据的接收器
  pub rx: Receiver<Meta>,
}

/// State of the background flush task
/// 后台刷盘任务的状态
pub enum State<S, D> {
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
  /// Check task status and trigger new flush if needed
  /// 检查任务状态并在需要时触发新的刷盘
  pub fn flush(&mut self, old: &mut Vec<Rc<Map>>) {
    loop {
      match self {
        State::Ing(ing) => match ing.rx.try_recv() {
          Ok(_meta) => {
            // Task finished, remove the flushed map from old and return to Idle
            // 任务完成，从 old 中移除已刷盘的 map 并切回 Idle
            if !old.is_empty() {
              old.remove(0);
            }
            let disk = ing.disk.clone();
            *self = State::Idle(disk);
          }
          Err(TryRecvError::Empty) => return,
          Err(TryRecvError::Disconnected) => {
            log::error!("flush task disconnected unexpectedly");
            // Recover Disk from Ing back to Idle
            let disk = ing.disk.clone();
            *self = State::Idle(disk);
            return;
          }
        },
        State::Idle(disk) => {
          if !old.is_empty() {
            // Get the OLDEST map WITHOUT removing it yet (to keep it queryable)
            // 获取最旧的 map 但不立即移除（保证其可查询性）
            let map = old[0].clone();
            let disk = disk.clone();
            let rx = Disk::run(disk.clone(), map);
            *self = State::Ing(Ing { disk, rx });
          }
          return;
        }
      }
    }
  }
}
