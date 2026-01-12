use std::{cell::RefCell, rc::Rc};

use futures::channel::oneshot::{Receiver, channel};
use jdb_base::{
  Discard,
  sst::{Meta, Sst},
};

use super::Disk;
use crate::Map;

/// Start background flush operations
/// 开始后台刷盘操作
#[allow(clippy::await_holding_refcell_ref)]
pub fn run<S, D>(this: Rc<RefCell<Disk<S, D>>>, map: Rc<Map>) -> Receiver<Result<Meta, S::Error>>
where
  S: Sst,
  D: Discard,
{
  let (tx, rx) = channel();
  compio::runtime::spawn(async move {
    // Use a block to constrain the lifetime of the mutable borrow
    // 使用代码块限制可变借用的生命周期
    let res = {
      let mut disk = this.borrow_mut();
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
      log::error!("discard flush error: {:?}", err);
    }

    // sst.push is done by caller synchronously with old.remove
    // sst.push 由调用方同步执行，与 old.remove 一起
    let result = match sst_res {
      Ok(meta) => Ok(meta),
      Err(err) => {
        log::error!("sst write error: {:?}", err);
        Err(err)
      }
    };

    let _ = tx.send(result);
  })
  .detach();
  rx
}
